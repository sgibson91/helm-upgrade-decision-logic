import argparse
import fnmatch
import os
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


def discover_modified_common_files(modified_paths: list):
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
    cluster_filepaths,
    added_or_modified_files,
    upgrade_all_hubs_on_all_clusters=False,
):
    """Generate a list of dictionaries describing which hubs on which clusters need
    to undergo a helm upgrade based on whether their associated helm chart values
    files have been modified. To be parsed to GitHub Actions in order to generate
    parallel jobs in a matrix.

    Note: "cluster folders" are those that contain a cluster.yaml file.

    Args:
        cluster_filepaths (list[path obj]): List of absolute paths to cluster folders
        added_or_modified_files (set[str]): A set of all added or modified files
            provided in a GitHub Pull Requests
        upgrade_all_hubs_on_all_clusters (bool, optional): If True, generates jobs to
            upgrade all hubs on all clusters. This is triggered when common config has
            been modified, such as basehub or daskhub helm charts. Defaults to False.

    Returns:
        list[dict]: A list of dictionaries. Each dictionary contains: the name of a
            cluster, the cloud provider that cluster runs on, and the name of a hub
            deployed to that cluster.
    """
    # Empty list to store the matrix job definitions in
    matrix_jobs = []

    # This flag will allow us to establish when a cluster.yaml file has been updated
    # and all hubs on that cluster should be upgraded, without also upgrading all hubs
    # on all other clusters
    upgrade_all_hubs_on_this_cluster = False

    if upgrade_all_hubs_on_all_clusters:
        print(
            "Core infrastrucure has been modified. Generating jobs to upgrade all hubs on ALL clusters."
        )
        reason_for_redeploy = "Core infrastructure has changed"

        # Overwrite cluster_filepaths to contain paths to all clusters
        if test_env == "test":
            # We are running a test via pytest. We only want to focus on the cluster
            # folders nested under the `tests/` folder.
            cluster_filepaths = [
                filepath.parent
                for filepath in Path(os.getcwd()).glob("**/cluster.yaml")
                if "tests/" in str(filepath)
            ]
        else:
            # We are NOT running a test via pytest. We want to explicitly ignore the
            # cluster folders nested under the `tests/` folder.
            cluster_filepaths = [
                filepath.parent
                for filepath in Path(os.getcwd()).glob("**/cluster.yaml")
                if "tests/" not in str(filepath)
            ]

    for cluster_filepath in cluster_filepaths:
        # Read in the cluster.yaml file
        with open(cluster_filepath.joinpath("cluster.yaml")) as f:
            cluster_config = yaml.load(f)

        if not upgrade_all_hubs_on_all_clusters:
            # Check if this cluster file has been modified. If so, set
            # upgrade_all_hubs_on_this_cluster to True
            intersection = added_or_modified_files.intersection(
                [str(cluster_filepath.joinpath("cluster.yaml"))]
            )
            if len(intersection) > 0:
                print(
                    f"This cluster.yaml file has been modified. Generating jobs to upgrade all hubs on THIS cluster: {cluster_config.get('name', {})}"
                )
                upgrade_all_hubs_on_this_cluster = True
                reason_for_redeploy = "cluster.yaml file was modified"

        # Generate template dictionary for all jobs associated with this cluster
        cluster_info = {
            "cluster_name": cluster_config.get("name", {}),
            "provider": cluster_config.get("provider", {}),
        }

        # Loop over each hub on this cluster
        for hub in cluster_config.get("hubs", {}):
            if upgrade_all_hubs_on_all_clusters or upgrade_all_hubs_on_this_cluster:
                # We know we're upgrading all hubs, so just add the hub name to the list
                # of matrix jobs and move on
                matrix_job = cluster_info.copy()
                matrix_job["hub_name"] = hub["name"]
                matrix_job["reason_for_redeploy"] = reason_for_redeploy
                matrix_jobs.append(matrix_job)

            else:
                # Read in this hub's helm chart values files from the cluster.yaml file
                helm_chart_values_files = [
                    str(cluster_filepath.joinpath(values_file))
                    for values_file in hub.get("helm_chart_values_files", {})
                ]
                # Establish if any of this hub's helm chart values files have been
                # modified
                intersection = list(
                    added_or_modified_files.intersection(helm_chart_values_files)
                )

                if len(intersection) > 0:
                    # If at least one of the helm chart values files associated with
                    # this hub has been modified, add it to list of matrix jobs to be
                    # upgraded
                    reason_for_redeploy = (
                        "Following helm chart values files were modified:\n"
                        + "\n- ".join(intersection)
                    )

                    matrix_job = cluster_info.copy()
                    matrix_job["hub_name"] = hub["name"]
                    matrix_job["reason_for_redeploy"] = reason_for_redeploy
                    matrix_jobs.append(matrix_job)

        # Reset upgrade_all_hubs_on_this_cluster for the next iteration
        upgrade_all_hubs_on_this_cluster = False

    return matrix_jobs


def generate_support_matrix_jobs(
    cluster_filepaths, added_or_modified_files, upgrade_support_on_all_clusters=False
):
    """Generate a list of dictionaries describing which clusters need to undergo a helm
    upgrade of their support chart based on whether their cluster.yaml file or
    associated support chart values files have been modified. To be parsed to GitHub
    Actions in order to generate parallel jobs in a matrix.

    Note: "cluster folders" are those that contain a cluster.yaml file.

    Args:
        cluster_filepaths (list[path obj]): List of absolute paths to cluster folders
            that contain added or modified files from the input of a GitHub Pull
            Request
        added_or_modified_files (set): A set of all added or modified files from the
            input of a GitHub Pull Request
        upgrade_support_on_all_clusters (bool, optional): If True, generates jobs to
            update the support chart on all clusters. This is triggered when common
            config has been modified in the support helm chart. Defaults to False.

    Returns:
        list[dict]: A list of dictionaries. Each dictionary contains: the name of a
            cluster and the cloud provider that cluster runs on.
    """
    # Empty list to store the matrix definitions in
    matrix_jobs = []

    if upgrade_support_on_all_clusters:
        print(
            "Support helm chart has been modified. Generating jobs to upgrade support chart on ALL clusters."
        )

        # Overwrite cluster_filepaths to contain paths to all clusters
        if test_env == "test":
            # We are running a test via pytest. We only want to focus on the cluster
            # folders nested under the `tests/` folder.
            cluster_filepaths = [
                filepath.parent
                for filepath in Path(os.getcwd()).glob("**/cluster.yaml")
                if "tests/" in str(filepath)
            ]
        else:
            # We are NOT running a test via pytest. We want to explicitly ignore the
            # cluster folders nested under the `tests/` folder.
            cluster_filepaths = [
                filepath.parent
                for filepath in Path(os.getcwd()).glob("**/cluster.yaml")
                if "tests/" not in str(filepath)
            ]

    for cluster_filepath in cluster_filepaths:
        # Read in the cluster.yaml file
        with open(cluster_filepath.joinpath("cluster.yaml")) as f:
            cluster_config = yaml.load(f)

        # Generate a dictionary-style job entry for this cluster
        cluster_info = {
            "cluster_name": cluster_config.get("name", {}),
            "provider": cluster_config.get("provider", {}),
        }

        # Double-check that support is defined for this cluster.
        support_config = cluster_config.get("support", {})
        if support_config:
            if upgrade_support_on_all_clusters:
                # We know we're upgrading support on all clusters, so just add the cluster name to the list
                # of matrix jobs and move on
                matrix_job = cluster_info.copy()
                matrix_job[
                    "reason_for_redeploy"
                ] = "Support helm chart has been modified"
                matrix_jobs.append(matrix_job)
            else:
                # Has the cluster.yaml file for this cluster folder been modified?
                cluster_yaml_intersection = added_or_modified_files.intersection(
                    [str(cluster_filepath.joinpath("cluster.yaml"))]
                )

                # Have the related support values files for this cluster been modified?
                support_values_files = [
                    str(cluster_filepath.joinpath(values_file))
                    for values_file in support_config.get("helm_chart_values_files", {})
                ]
                support_values_intersection = added_or_modified_files.intersection(
                    support_values_files
                )

                # If either of the intersections have a length greater than zero, append
                # the job definition to the list of matrix jobs
                if (len(cluster_yaml_intersection) > 0) or (
                    len(support_values_intersection) > 0
                ):
                    matrix_job = cluster_info.copy()

                    if len(support_values_intersection) > 0:
                        matrix_job["reason_for_redeploy"] = (
                            "Following helm chart values files were modified:\n"
                            + "\n- ".join(support_values_intersection)
                        )
                    elif len(cluster_yaml_intersection) > 0:
                        matrix_job[
                            "reason_for_redeploy"
                        ] = "cluster.yaml file was modified"

                    matrix_jobs.append(matrix_job)
        else:
            print(
                f"No support defined for cluster: {cluster_config.get('name', {})}"
            )

    return matrix_jobs


def update_github_env(hub_matrix_jobs, support_matrix_jobs):
    """Update the GITHUB_ENV environment with two new variables describing the matrix
    jobs that need to be run in order to update the support charts and hubs that have
    been modified.

    Args:
        hub_matrix_jobs (list[dict]): A list of dictionaries which describe the set of
            matrix jobs required to update only the hubs on clusters whose config has
            been modified.
        support_matrix_jobs (list[dict]):  A list of dictionaries which describe the
            set of matrix jobs required to update only the support chart on clusters
            whose config has been modified.
    """
    # In GitHub Actions, the environment a workflow/job/step executes in can be
    # influenced by the contents of the `GITHUB_ENV` file.
    #
    # For more information, see:
    # https://docs.github.com/en/actions/using-workflows/workflow-commands-for-github-actions#setting-an-environment-variable
    with open(os.getenv("GITHUB_ENV"), "a") as f:
        f.write(
            "\n".join(
                [
                    f"HUB_MATRIX_JOBS={hub_matrix_jobs}",
                    f"SUPPORT_MATRIX_JOBS={support_matrix_jobs}",
                ]
            )
        )


def pretty_print_matrix_jobs(hub_matrix_jobs, support_matrix_jobs):
    # Construct table for support chart upgrades
    support_table = Table(title="Support chart upgrades")
    support_table.add_column("Cloud Provider")
    support_table.add_column("Cluster Name")
    support_table.add_column("Reason for Redeploy")

    # Add rows
    for job in support_matrix_jobs:
        support_table.add_row(
            job["provider"], job["cluster_name"], job["reason_for_redeploy"]
        )

    # Construct table for hub helm chart upgrades
    hub_table = Table(title="Hub helm chart upgrades")
    hub_table.add_column("Cloud Provider")
    hub_table.add_column("Cluster Name")
    hub_table.add_column("Hub Name")
    hub_table.add_column("Reason for Redeploy")

    # Add rows
    for job in hub_matrix_jobs:
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
    cluster_filepaths = get_unique_cluster_filepaths(args.filepaths)

    # Generate a job matrix of all hubs that need upgrading
    hub_matrix_jobs = generate_hub_matrix_jobs(
        cluster_filepaths,
        set(args.filepaths),
        upgrade_all_hubs_on_all_clusters=upgrade_all_hubs_on_all_clusters,
    )

    # Generate a job matrix of all clusters that need their support chart upgrading
    support_matrix_jobs = generate_support_matrix_jobs(
        cluster_filepaths,
        set(args.filepaths),
        upgrade_support_on_all_clusters=upgrade_support_on_all_clusters,
    )

    # The existence of the GITHUB_ENV environment variable is an indication that
    # we are running in an GitHub Actions workflow
    # https://docs.github.com/en/actions/learn-github-actions/environment-variables#default-environment-variables
    # We should always default to pretty printing the results of the decision logic
    # if we are not running in GitHub Actions, even when the --pretty-print flag has
    # not been parsed on the command line. This will avoid errors trying to write to
    # a GITHUB_ENV file that does not exist in the update_github_env function
    env = os.environ.get("GITHUB_ENV", {})
    if args.pretty_print or not env:
        pretty_print_matrix_jobs(hub_matrix_jobs, support_matrix_jobs)
    else:
        # Add these matrix jobs to the GitHub environment for use in another job
        update_github_env(hub_matrix_jobs, support_matrix_jobs)


if __name__ == "__main__":
    main()
