import argparse
import fnmatch
import os
import subprocess
from pathlib import Path

from rich.console import Console
from rich.table import Table
from ruamel.yaml import YAML

yaml = YAML(typ="safe", pure=True)


def _converted_string_to_list(full_str: str) -> list:
    """
    Take a SPACE-DELIMITED string and split it into a list.

    This function is used by the generate-helm-upgrade-jobs subcommand to ensure that
    the list os added or modified files parsed from the command line is transformed
    into a list of strings instead of one long string with spaces between the elements
    """
    return full_str.split(" ")


def discover_modified_common_files(modified_paths: list) -> (bool, bool):
    """There are certain common files which, if modified, we should upgrade all hubs
    and/or all clusters appropriately. These common files include the helm charts we
    deploy, as well as the GitHub Actions and deployer package we use to deploy them.

    Args:
        modified_paths (list[str]): The list of files that have been added or modified
            in a given GitHub Pull Request.

    Returns:
        upgrade_support_on_all_clusters (bool): Whether or not all clusters should have
            their support chart upgraded since has changes
        upgrade_all_hubs_on_all_clusters (bool): Whether or not all hubs on all clusters
            should be upgraded since a core piece of infrastructure has changed
    """
    # If any of the following filepaths have changed, we should update all hubs on all clusters
    common_filepaths = [
        # Filepaths related to the deployer infrastructure
        "mymodule/*",
        # Filepaths related to GitHub Actions infrastructure
        ".github/workflows/*",
        # Filepaths related to helm chart infrastructure
        "helm-charts/basehub/*",
        "helm-charts/daskhub/*",
    ]
    # If this filepath has changes, we should update the support chart on all clusters
    support_chart_filepath = "helm-charts/support/*"

    # Discover if the support chart has been modified
    upgrade_support_on_all_clusters = bool(
        fnmatch.filter(modified_paths, support_chart_filepath)
    )

    # Discover if any common config has been modified
    upgrade_all_hubs_on_all_clusters = False
    for common_filepath_pattern in common_filepaths:
        upgrade_all_hubs_on_all_clusters = bool(
            fnmatch.filter(modified_paths, common_filepath_pattern)
        )
        if upgrade_all_hubs_on_all_clusters:
            break

    return upgrade_support_on_all_clusters, upgrade_all_hubs_on_all_clusters


def get_all_cluster_yaml_files(test_env: bool = False) -> set:
    """Get a set of absolute paths to all cluster.yaml files in the repository

    Args:
        test_env (bool, optional): A flag to determine whether we are running a test
            suite or not. If True, only return the paths to cluster.yaml files under the
            'tests/' directory. If False, explicitly exclude the cluster.yaml files
            nested under the 'tests/' directory. Defaults to False.

    Returns:
        set[path obj]: A set of absolute paths to all cluster.yaml files in the repo
    """
    # Get absolute paths
    if test_env:
        # We are running a test via pytest. We only want to focus on the cluster
        # folders nested under the `tests/` folder.
        cluster_files = [
            filepath
            for filepath in Path(os.getcwd()).glob("**/cluster.yaml")
            if "tests" in str(filepath)
        ]
    else:
        # We are NOT running a test via pytest. We want to explicitly ignore the
        # cluster folders nested under the `tests/` folder.
        cluster_files = [
            filepath
            for filepath in Path(os.getcwd()).glob("**/cluster.yaml")
            if "tests" not in str(filepath)
        ]

    # Return unique absolute paths
    return set(cluster_files)


def generate_hub_matrix_jobs(
    cluster_file: Path,
    cluster_config: dict,
    cluster_info: dict,
    added_or_modified_files: set,
    upgrade_all_hubs_on_this_cluster: bool = False,
    upgrade_all_hubs_on_all_clusters: bool = False,
) -> list:
    """Generate a list of dictionaries describing which hubs on a given cluster need
    to undergo a helm upgrade based on whether their associated helm chart values
    files have been modified. To be parsed to GitHub Actions in order to generate
    parallel jobs in a matrix.

    Args:
        cluster_file (path obj): The absolute path to the cluster.yaml file of a given
            cluster
        cluster_config (dict): The cluster-wide config for a given cluster in
            dictionary format
        cluster_info (dict): A template dictionary for defining matrix jobs prepopulated
            with some info. "cluster_name": The name of the given cluster; "provider":
            the cloud provider the given cluster runs on; "reason_for_redeploy":
            what has changed in the repository to prompt a hub on this cluster to be
            redeployed.
        added_or_modified_files (set[str]): A set of all added or modified files
            provided in a GitHub Pull Requests
        upgrade_all_hubs_on_this_cluster (bool, optional): If True, generates jobs to
            upgrade all hubs on the given cluster. This is triggered when the
            cluster.yaml file itself has been modified. Defaults to False.
        upgrade_all_hubs_on_all_clusters (bool, optional): If True, generates jobs to
            upgrade all hubs on all clusters. This is triggered when common config has
            been modified, such as the basehub or daskhub helm charts. Defaults to False.

    Returns:
        list[dict]: A list of dictionaries. Each dictionary contains: the name of a
            cluster, the cloud provider that cluster runs on, the name of a hub
            deployed to that cluster, and the reason that hub needs to be redeployed.
    """
    # Empty list to store all the matrix job definitions in
    matrix_jobs = []

    # Loop over each hub on this cluster
    for hub in cluster_config.get("hubs", {}):
        if upgrade_all_hubs_on_all_clusters or upgrade_all_hubs_on_this_cluster:
            # We know we're upgrading all hubs, so just add the hub name to the list
            # of matrix jobs and move on
            matrix_job = cluster_info.copy()
            matrix_job["hub_name"] = hub["name"]
            matrix_jobs.append(matrix_job)

        else:
            # Read in this hub's helm chart values files from the cluster.yaml file
            values_files = [
                cluster_file.parent.joinpath(values_file)
                for values_file in hub.get("helm_chart_values_files", {})
            ]
            # Establish if any of this hub's helm chart values files have been
            # modified
            intersection = added_or_modified_files.intersection(values_files)

            if intersection:
                # If at least one of the helm chart values files associated with
                # this hub has been modified, add it to list of matrix jobs to be
                # upgraded
                matrix_job = cluster_info.copy()
                matrix_job["hub_name"] = hub["name"]
                matrix_job["reason_for_redeploy"] = (
                    "Following helm chart values files were modified:\n- "
                    + "\n- ".join([path.name for path in intersection])
                )
                matrix_jobs.append(matrix_job)

    return matrix_jobs


def generate_support_matrix_jobs(
    cluster_file: Path,
    cluster_config: dict,
    cluster_info: dict,
    added_or_modified_files: set,
    upgrade_support_on_this_cluster: bool = False,
    upgrade_support_on_all_clusters: bool = False,
) -> list:
    """Generate a list of dictionaries describing which clusters need to undergo a helm
    upgrade of their support chart based on whether their associated support chart
    values files have been modified. To be parsed to GitHub Actions in order to generate
    jobs in a matrix.

    Args:
        cluster_file (path obj): The absolute path to the cluster.yaml file of a given
            cluster
        cluster_config (dict): The cluster-wide config for a given cluster in
            dictionary format
        cluster_info (dict): A template dictionary for defining matrix jobs prepopulated
            with some info. "cluster_name": The name of the given cluster; "provider":
            the cloud provider the given cluster runs on; "reason_for_redeploy":
            what has changed in the repository to prompt the support chart for this
            cluster to be redeployed.
        added_or_modified_files (set[str]): A set of all added or modified files
            provided in a GitHub Pull Requests
        upgrade_support_on_this_cluster (bool, optional): If True, generates jobs to
            update the support chart on the given cluster. This is triggered when the
            cluster.yaml file itself is modified. Defaults to False.
        upgrade_support_on_all_clusters (bool, optional): If True, generates jobs to
            update the support chart on all clusters. This is triggered when common
            config has been modified in the support helm chart. Defaults to False.

    Returns:
        list[dict]: A list of dictionaries. Each dictionary contains: the name of a
            cluster, the cloud provider that cluster runs on, a Boolean indicating if
            the support chart should be upgraded, and a reason why the support chart
            needs upgrading.
    """
    cluster_info["reason_for_support_redeploy"] = cluster_info.pop(
        "reason_for_redeploy"
    )

    # Empty list to store the matrix definitions in
    matrix_jobs = []

    # Double-check that support is defined for this cluster.
    support_config = cluster_config.get("support", {})
    if support_config:
        if upgrade_support_on_all_clusters or upgrade_support_on_this_cluster:
            # We know we're upgrading support on all clusters, so just add the cluster
            # name to the list of matrix jobs and move on
            matrix_job = cluster_info.copy()
            matrix_job["upgrade_support"] = True
            matrix_job["reason_for_support_redeploy"] = "Support helm chart has been modified"
            matrix_jobs.append(matrix_job)

        else:
            # Have the related support values files for this cluster been modified?
            values_files = [
                cluster_file.parent.joinpath(values_file)
                for values_file in support_config.get("helm_chart_values_files", {})
            ]
            intersection = added_or_modified_files.intersection(values_files)

            if intersection:
                matrix_job = cluster_info.copy()
                matrix_job["upgrade_support"] = True
                matrix_job["reason_for_support_redeploy"] = (
                    "Following helm chart values files were modified:\n- "
                    + "\n- ".join([path.name for path in intersection])
                )
                matrix_jobs.append(matrix_job)

    else:
        print(f"No support defined for cluster: {cluster_info['cluster_name']}")

    return matrix_jobs


def assign_staging_matrix_jobs(
    hub_matrix_jobs: list,
    staging_matrix_jobs: list,
) -> (list, list):
    """This function ensures that all prod hub jobs will have an associated staging hub
    job. It also ensures that each job in staging_matrix_jobs takes the following form:

    {
        "cluster_name": str                 # Name of the cluster
        "provider": str                     # Name of the cloud provider the cluster runs on
        "upgrade_support": bool             # Whether to upgrade the support chart for this cluster
        "reason_for_support_redeploy": str  # Why the support chart needs to be upgraded
        "upgrade_staging": bool             # Whether the upgrade the staging deployment on this cluster
        "reason_for_staging)redeploy": str  # Why the staging hub needs to be upgraded
    }

    Args:
        hub_matrix_jobs (list[dict]): A list of dictionaries describing matrix jobs to
            upgrade all hubs regardless of staging/prod status
        staging_matrix_jobs (list[dict]): A list of dictionaries describing matrix jobs
            to upgrade just the support chart if necessary

    Returns:
        hub_matrix_jobs (list[dict]): Transformed list of dictionaries describing matrix
            jobs to upgrade only production hubs
        staging_matrix_jobs (list[dict]): Transformed list of dictionaries describing
            matrix jobs to upgrade staging hubs and the support chart if necessary
    """
    # We want to move the jobs updating a staging hub from the prod hub matrix job list
    # into the support and staging matrix job list. The job_ids_to_remove list will
    # track the indexes of the jobs that need to be deleted from hub_matrix_jobs.
    job_ids_to_remove = []

    # Loop over each matrix job in hub_matrix_jobs. If the name of the hub contains
    # "staging" (including "dask-staging"), update a job in staging_matrix_jobs
    # to hold a Boolean value to upgrade the staging deployment.
    for hub_job_id, hub_job in enumerate(hub_matrix_jobs):
        if "staging" in hub_job["hub_name"]:
            # Find the index of a job in staging_matrix_jobs that has the
            # same cluster name as the current hub job.
            job_idx = next(
                (
                    idx
                    for (idx, staging_job) in enumerate(staging_matrix_jobs)
                    if staging_job["cluster_name"] == hub_job["cluster_name"]
                ),
                None,
            )

            if job_idx is not None:
                # Add information to the matching staging_matrix_jobs entry
                # to upgrade the staging deployment
                staging_matrix_jobs[job_idx]["upgrade_staging"] = True
                staging_matrix_jobs[job_idx]["reason_for_staging_redeploy"] = hub_job[
                    "reason_for_redeploy"
                ]

                # Mark the current hub job for deletion from hub_matrix_jobs
                job_ids_to_remove.append(hub_job_id)

            else:
                # A job with a matching cluster name doesn't exist, this is because its
                # support chart doesn't need upgrading. We create a new job in that will
                # upgrade the staging deployment for this cluster, but not the support
                # chart.
                new_job = {
                    "cluster_name": hub_job["cluster_name"],
                    "provider": hub_job["provider"],
                    "upgrade_staging": True,
                    "reason_for_staging_redeploy": hub_job["reason_for_redeploy"],
                    "upgrade_support": False,
                    "reason_for_support_redeploy": "",
                }
                staging_matrix_jobs.append(new_job)

    # Remove all the jobs for staging hubs from hub_matrix_jobs, they are now tracked
    # in staging_matrix_jobs
    for job_id in job_ids_to_remove:
        del hub_matrix_jobs[job_id]

    # For each job listed in staging_matrix_jobs, ensure it has the
    # upgrade_staging key present, even if we just set it to False
    for staging_job in staging_matrix_jobs:
        if "upgrade_staging" not in staging_job.keys():
            # Get a list of prod hubs running on the same cluster this staging job will
            # run on
            hubs_on_this_cluster = [
                hub["hub_name"]
                for hub in hub_matrix_jobs
                if hub["cluster_name"] == staging_job["cluster_name"]
            ]

            if hubs_on_this_cluster:
                # There are prod hubs on this cluster that require an upgrade, and so we
                # also upgrade staging
                staging_job["upgrade_staging"] = True
                staging_job[
                    "reason_for_staging_redeploy"
                ] = "Following prod hubs require redeploy:\n- " + "\n- ".join(
                    hubs_on_this_cluster
                )
            else:
                # There are no prod hubs on this cluster that require an upgrade, so we
                # do not upgrade staging
                staging_job["upgrade_staging"] = False
                staging_job["reason_for_staging_redeploy"] = ""

    # Ensure that for each cluster listed in hub_matrix_jobs, there is an associated job
    # in staging_matrix_jobs. This is our last-hope catch-all to ensure there are no
    # prod hub jobs trying to run without an associated support/staging job
    prod_hub_clusters = {job["cluster_name"] for job in hub_matrix_jobs}
    support_staging_clusters = {job["cluster_name"] for job in staging_matrix_jobs}
    missing_clusters = prod_hub_clusters.difference(support_staging_clusters)

    if missing_clusters:
        # Generate support/staging jobs for clusters that don't have them but do have
        # prod hub jobs. We assume they are mising because neither the support chart
        # nor staging hub needed an upgrade. We set upgrade_support to False. However,
        # if prod hubs need upgrading, then we should upgrade staging so set that to
        # True.
        for missing_cluster in missing_clusters:
            provider = next(
                (
                    hub["provider"]
                    for hub in hub_matrix_jobs
                    if hub["cluster_name"] == missing_cluster
                ),
                None,
            )
            prod_hubs = [
                hub["hub_name"]
                for hub in hub_matrix_jobs
                if hub["cluster_name"] == missing_cluster
            ]

            new_job = {
                "cluster_name": missing_cluster,
                "provider": provider,
                "upgrade_support": False,
                "reason_for_support_redeploy": "",
                "upgrade_staging": True,
                "reason_for_staging_redeploy": (
                    "Following prod hubs require redeploy:\n- " + "\n- ".join(prod_hubs)
                ),
            }

            staging_matrix_jobs.append(new_job)

    return hub_matrix_jobs, staging_matrix_jobs


def pretty_print_matrix_jobs(
    prod_hub_matrix_jobs: list, support_and_staging_matrix_jobs: list
) -> None:
    # Construct table for support chart upgrades
    support_table = Table(title="Support chart and Staging hub upgrades")
    support_table.add_column("Cloud Provider")
    support_table.add_column("Cluster Name")
    support_table.add_column("Upgrade Support?")
    support_table.add_column("Reason for Support Redeploy")
    support_table.add_column("Upgrade Staging?")
    support_table.add_column("Reason for Staging Redeploy")

    # Add rows
    for job in support_and_staging_matrix_jobs:
        support_table.add_row(
            job["provider"],
            job["cluster_name"],
            str(job["upgrade_support"]),
            job["reason_for_support_redeploy"],
            str(job["upgrade_staging"]),
            job["reason_for_staging_redeploy"],
        )

    # Construct table for prod hub upgrades
    hub_table = Table(title="Prod hub upgrades")
    hub_table.add_column("Cloud Provider")
    hub_table.add_column("Cluster Name")
    hub_table.add_column("Hub Name")
    hub_table.add_column("Reason for Redeploy")

    # Add rows
    for job in prod_hub_matrix_jobs:
        hub_table.add_row(
            job["provider"],
            job["cluster_name"],
            job["hub_name"],
            job["reason_for_redeploy"],
        )

    console = Console()
    console.print(support_table)
    console.print(hub_table)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "filepaths",
        nargs="?",
        type=_converted_string_to_list,
        help="A singular or space-delimited list of newly added or modified filepaths in the repo",
    )
    parser.add_argument(
        "--pretty-print",
        action="store_true",
        help="Pretty print the calculated matrix jobs as tables using rich",
    )

    args = parser.parse_args()

    (
        upgrade_support_on_all_clusters,
        upgrade_all_hubs_on_all_clusters,
    ) = discover_modified_common_files(args.filepaths)

    # Get a list of filepaths to target cluster folders
    cluster_files = get_all_cluster_yaml_files(args.filepaths)

    # Empty lists to store job definitions in
    prod_hub_matrix_jobs = []
    support_and_staging_matrix_jobs = []

    for cluster_file in cluster_files:
        # Read in the cluster.yaml file
        with open(cluster_file) as f:
            cluster_config = yaml.load(f)

        # Get cluster's name and its cloud provider
        cluster_name = cluster_config.get("name", {})
        provider = cluster_config.get("provider", {})

        # Generate template dictionary for all jobs associated with this cluster
        cluster_info = {
            "cluster_name": cluster_name,
            "provider": provider,
            "reason_for_redeploy": "",
        }

        # Check if this cluster file has been modified. If so, set boolean flags to True
        intersection = set(args.filepaths).intersection([str(cluster_file)])
        if intersection:
            print(
                f"This cluster.yaml file has been modified. Generating jobs to upgrade all hubs and the support chart on THIS cluster: {cluster_name}"
            )
            upgrade_all_hubs_on_this_cluster = True
            upgrade_support_on_this_cluster = True
            cluster_info["reason_for_redeploy"] = "cluster.yaml file was modified"
        else:
            upgrade_all_hubs_on_this_cluster = False
            upgrade_support_on_this_cluster = False

        # Generate a job matrix of all hubs that need upgrading on this cluster
        prod_hub_matrix_jobs.extend(
            generate_hub_matrix_jobs(
                cluster_file,
                cluster_config,
                cluster_info,
                set(args.filepaths),
                upgrade_all_hubs_on_this_cluster=upgrade_all_hubs_on_this_cluster,
                upgrade_all_hubs_on_all_clusters=upgrade_all_hubs_on_all_clusters,
            )
        )

        # Generate a job matrix for support chart upgrades
        support_and_staging_matrix_jobs.extend(
            generate_support_matrix_jobs(
                cluster_file,
                cluster_config,
                cluster_info,
                set(args.filepaths),
                upgrade_support_on_this_cluster=upgrade_support_on_this_cluster,
                upgrade_support_on_all_clusters=upgrade_support_on_all_clusters,
            )
        )

    # this needs to be a better comment v
    # Ensure that the matrix job definitions conform to schema
    prod_hub_matrix_jobs, support_and_staging_matrix_jobs = assign_staging_matrix_jobs(
        prod_hub_matrix_jobs, support_and_staging_matrix_jobs
    )

    # The existence of the CI environment variable is an indication that we are running
    # in an GitHub Actions workflow
    # https://docs.github.com/en/actions/learn-github-actions/environment-variables#default-environment-variables
    # We should always default to pretty printing the results of the decision logic
    # if we are not running in GitHub Actions, even when the --pretty-print flag has
    # not been parsed on the command line. This will avoid errors trying to set CI
    # output variables in an environment that doesn't exist.
    ci_env = os.environ.get("CI", False)
    if args.pretty_print or not ci_env:
        pretty_print_matrix_jobs(prod_hub_matrix_jobs, support_and_staging_matrix_jobs)
    else:
        # Add these matrix jobs as output variables for use in another job
        # https://docs.github.com/en/github-ae@latest/actions/using-workflows/workflow-syntax-for-github-actions#jobsjob_idoutputs
        subprocess.check_call(
            [
                "echo",
                f'"::set-output name=PROD_HUB_MATRIX_JOBS::{prod_hub_matrix_jobs}"',
            ]
        )
        subprocess.check_call(
            [
                "echo",
                f'"::set-output name=SUPPORT_AND_STAGING_MATRIX_JOBS::{support_and_staging_matrix_jobs}"',
            ]
        )


if __name__ == "__main__":
    main()
