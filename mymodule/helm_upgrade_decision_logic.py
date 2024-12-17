import argparse
import fnmatch
import json
import os
import warnings
from pathlib import Path

from rich.console import Console
from rich.table import Table
from ruamel.yaml import YAML

yaml = YAML(typ="safe", pure=True)
REPO_ROOT_PATH = Path(__file__).parent.parent
CONFIG_CLUSTERS_PATH = REPO_ROOT_PATH.joinpath("config/clusters")


def _converted_string_to_list(full_str: str) -> list:
    """
    Take a SPACE-DELIMITED string and split it into a list.

    This function is used by the generate-helm-upgrade-jobs subcommand to ensure that
    the list os added or modified files parsed from the command line is transformed
    into a list of strings instead of one long string with spaces between the elements
    """
    return full_str.split(" ")


def find_absolute_path_to_cluster_file(cluster_name: str):
    """Find the absolute path to a cluster.yaml file for a named cluster

    Args:
        cluster_name (str): The name of the cluster we wish to perform actions on.
            This corresponds to a folder name, and that folder should contain a
            cluster.yaml file.

    Returns:
        Path object: The absolute path to the cluster.yaml file for the named cluster
    """
    cluster_yaml_path = CONFIG_CLUSTERS_PATH.joinpath(f"{cluster_name}/cluster.yaml")
    if not cluster_yaml_path.exists():
        raise FileNotFoundError(
            f"No cluster.yaml file exists for cluster {cluster_name}. "
            + "Please create one and then continue."
        )

    with open(cluster_yaml_path) as cf:
        cluster_config = yaml.load(cf)

    if cluster_yaml_path.parent.name != cluster_config["name"]:
        warnings.warn(
            "Cluster Name Mismatch: It is convention that the cluster name defined "
            + "in cluster.yaml matches the name of the parent directory. "
            + "Deployment won't be halted but please update this for consistency!"
        )

    return cluster_yaml_path


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


def get_all_cluster_yaml_files() -> set:
    """Get a set of absolute paths to all cluster.yaml files in the repository

    Returns:
        set[path obj]: A set of absolute paths to all cluster.yaml files in the repo
    """
    return {
        path
        for path in CONFIG_CLUSTERS_PATH.glob("**/cluster.yaml")
        if "templates" not in path.as_posix()
    }


def filter_out_staging_hubs(all_hub_matrix_jobs):
    """Separate staging hubs from prod hubs in hub matrix jobs.

    Args:
        all_hub_matrix_jobs (list[dict]): A list of dictionaries representing matrix
            jobs to upgrade deployed hubs as identified by the generate_hub_matrix_jobs
            function.

    Returns:
        staging_matrix_jobs (list[dict]): A list of dictionaries representing
            matrix jobs to upgrade staging hubs on clusters that require it.
        prod_hub_matrix_jobs (list[dict]): A list of dictionaries representing matrix
            jobs to upgrade all production hubs, i.e., those without "staging" in their
            name.
    """
    # Separate the jobs for hubs with "staging" in their name (including "dask-staging")
    # from those without staging in their name
    staging_hub_matrix_jobs = [
        job for job in all_hub_matrix_jobs if "staging" in job["hub_name"]
    ]
    prod_hub_matrix_jobs = [
        job for job in all_hub_matrix_jobs if "staging" not in job["hub_name"]
    ]

    return staging_hub_matrix_jobs, prod_hub_matrix_jobs


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

            if upgrade_all_hubs_on_all_clusters:
                matrix_job["reason_for_redeploy"] = (
                    "Core infrastructure has been modified"
                )

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
                    "Following helm chart values files were modified: "
                    + ", ".join([path.name for path in intersection])
                )
                matrix_jobs.append(matrix_job)

    staging_hub_matrix_jobs, prod_hub_matrix_jobs = filter_out_staging_hubs(matrix_jobs)

    return staging_hub_matrix_jobs, prod_hub_matrix_jobs


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
    # Empty list to store the matrix definitions in
    matrix_jobs = []

    # Double-check that support is defined for this cluster.
    support_config = cluster_config.get("support", {})
    if support_config:
        if upgrade_support_on_all_clusters or upgrade_support_on_this_cluster:
            # We know we're upgrading support on all clusters, so just add the cluster
            # name to the list of matrix jobs and move on
            matrix_job = cluster_info.copy()

            if upgrade_support_on_all_clusters:
                matrix_job["reason_for_redeploy"] = (
                    "Support helm chart has been modified"
                )

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
                matrix_job["reason_for_redeploy"] = (
                    "Following helm chart values files were modified: "
                    + ", ".join([path.name for path in intersection])
                )
                matrix_jobs.append(matrix_job)

    else:
        print(f"No support defined for cluster: {cluster_info['cluster_name']}")

    return matrix_jobs


def assign_staging_jobs_for_missing_clusters(
    staging_hub_matrix_jobs: list, prod_hub_matrix_jobs: list
) -> list:
    """Ensure that for each cluster listed in prod_hub_matrix_jobs, there is an
    associated job in staging_hub_matrix_jobs. This is our last-hope catch-all
    to ensure there are no prod hub jobs trying to run without an associated
    staging job.

    Args:
        staging_hub_matrix_jobs (list[dict]): A list of dictionaries representing jobs
            to upgrade staging hubs on clusters that require it.
        prod_hub_matrix_jobs (list[dict]): A list of dictionaries representing jobs to
            upgrade production hubs that require it.

    Returns:
        staging_hub_matrix_jobs (list[dict]): Updated to ensure any clusters missing
            present in prod_hub_matrix_jobs but missing from staging_hub_matrix_jobs
            now have an associated staging job.
    """
    prod_hub_clusters = {job["cluster_name"] for job in prod_hub_matrix_jobs}
    staging_hub_clusters = {job["cluster_name"] for job in staging_hub_matrix_jobs}
    missing_clusters = prod_hub_clusters.difference(staging_hub_clusters)

    if missing_clusters:
        # Generate staging jobs for clusters that don't have them but do have
        # prod hub jobs. We assume they are missing because the staging hub
        # didn't need an upgrade.
        for missing_cluster in missing_clusters:
            provider = next(
                (
                    hub["provider"]
                    for hub in prod_hub_matrix_jobs
                    if hub["cluster_name"] == missing_cluster
                ),
                None,
            )
            prod_hubs = [
                hub["hub_name"]
                for hub in prod_hub_matrix_jobs
                if hub["cluster_name"] == missing_cluster
            ]

            cluster_file = find_absolute_path_to_cluster_file(missing_cluster)
            with open(cluster_file) as f:
                cluster_config = yaml.load(f)

            staging_hubs = [
                hub["name"]
                for hub in cluster_config.get("hubs")
                if "staging" in hub["name"]
            ]

            for staging_hub in staging_hubs:
                new_job = {
                    "cluster_name": missing_cluster,
                    "provider": provider,
                    "hub_name": staging_hub,
                    "reason_for_redeploy": (
                        "Following prod hubs require redeploy: " + ", ".join(prod_hubs)
                    ),
                }
                staging_hub_matrix_jobs.append(new_job)

    return staging_hub_matrix_jobs


def pretty_print_matrix_jobs(
    support_matrix_jobs: list,
    staging_hub_matrix_jobs: list,
    prod_hub_matrix_jobs: list,
) -> None:
    # Construct table for support chart upgrades
    support_table = Table(title="Support chart upgrades")
    support_table.add_column("Cloud Provider")
    support_table.add_column("Cluster Name")
    support_table.add_column("Reason for Redeploy")

    # Add rows
    for job in support_matrix_jobs:
        support_table.add_row(
            job["provider"],
            job["cluster_name"],
            job["reason_for_redeploy"],
            end_section=True,
        )

    # Construct table for staging hub upgrades
    staging_hub_table = Table(title="Staging hub upgrades")
    staging_hub_table.add_column("Cloud Provider")
    staging_hub_table.add_column("Cluster Name")
    staging_hub_table.add_column("Hub Name")
    staging_hub_table.add_column("Reason for Redeploy")

    # Add rows
    for job in staging_hub_matrix_jobs:
        staging_hub_table.add_row(
            job["provider"],
            job["cluster_name"],
            job["hub_name"],
            job["reason_for_redeploy"],
            end_section=True,
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
            end_section=True,
        )

    console = Console()
    console.print(support_table)
    console.print(staging_hub_table)
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

    args.filepaths = [
        REPO_ROOT_PATH.joinpath(filepath) for filepath in args.filepaths
    ]

    # Get a list of filepaths to target cluster folders
    cluster_files = get_all_cluster_yaml_files()

    # Empty lists to store job definitions in
    support_matrix_jobs = []
    staging_hub_matrix_jobs = []
    prod_hub_matrix_jobs = []

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
        staging_hubs, prod_hubs = generate_hub_matrix_jobs(
            cluster_file,
            cluster_config,
            cluster_info,
            set(args.filepaths),
            upgrade_all_hubs_on_this_cluster=upgrade_all_hubs_on_this_cluster,
            upgrade_all_hubs_on_all_clusters=upgrade_all_hubs_on_all_clusters,
        )
        staging_hub_matrix_jobs.extend(staging_hubs)
        prod_hub_matrix_jobs.extend(prod_hubs)

        # Generate a job matrix for support chart upgrades
        support_matrix_jobs.extend(
            generate_support_matrix_jobs(
                cluster_file,
                cluster_config,
                cluster_info,
                set(args.filepaths),
                upgrade_support_on_this_cluster=upgrade_support_on_this_cluster,
                upgrade_support_on_all_clusters=upgrade_support_on_all_clusters,
            )
        )

    # Clean up the matrix jobs
    staging_hub_matrix_jobs = assign_staging_jobs_for_missing_clusters(
        staging_hub_matrix_jobs, prod_hub_matrix_jobs
    )

    # The existence of the CI environment variable is an indication that we are running
    # in an GitHub Actions workflow
    # https://docs.github.com/en/actions/learn-github-actions/environment-variables#default-environment-variables
    # We should always default to pretty printing the results of the decision logic
    # if we are not running in GitHub Actions, even when the --pretty-print flag has
    # not been parsed on the command line. This will avoid errors trying to set CI
    # output variables in an environment that doesn't exist.
    ci_env = os.environ.get("CI", False)
    env_file = os.environ.get("GITHUB_ENV")
    if args.pretty_print or not ci_env:
        pretty_print_matrix_jobs(
            support_matrix_jobs, staging_hub_matrix_jobs, prod_hub_matrix_jobs
        )
    else:
        # Add these matrix jobs as output variables for use in another job
        # https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions
        with open(env_file, "a") as f:
            f.write(f"support-matrix-jobs={json.dumps(support_matrix_jobs)}\n")
            f.write(f"staging-hub-matrix-jobs={json.dumps(staging_hub_matrix_jobs)}\n")
            f.write(f"prod-hub-matrix-jobs={json.dumps(prod_hub_matrix_jobs)}")


if __name__ == "__main__":
    main()
