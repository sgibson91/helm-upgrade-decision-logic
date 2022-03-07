import argparse
import fnmatch
import os
from pathlib import Path

import yaml
from rich.console import Console
from rich.table import Table

# If any of the following filepaths have changed, we should update all hubs on all clusters
common_filepaths = [
    "deployer/*",
    "helm-charts/basehub/*",
    "helm-charts/daskhub/*",
    ".github/actions/deploy/*",
    "requirements.txt",
    ".github/workflows/deploy-hubs.yaml",
]
# If this filepath has changes, we should update the support chart on all clusters
support_chart_filepath = "helm-charts/support/*"


def convert_string_to_list(full_str: str) -> list:
    """
    Take a SPACE-DELIMITED string and split it into a list
    """
    return full_str.split(" ")


def generate_lists_of_filepaths_and_filenames(input_file_list: list):
    """For a list of added and modified files, generate the following:
    - A list of unique filepaths to cluster folders containing added/modified files
    - A set of all added/modified files matching the pattern "*/cluster.yaml"
    - A set of all added/modified files matching the pattern "*/*.values.yaml"
    - A set of all added/modified files matching the pattern "*/support.values.yaml"

    Args:
        input_file_list (list[str]): A list of files that have been added or modified
            in a GitHub Pull Request

    Returns:
        list[str]: A list of unique filepaths to cluster folders
        set[str]: A set of all files matching the pattern "*/cluster.yaml"
        set[str]: A set of all files matching the pattern "*/*.values.yaml"
        set[str]: A set of all files matching the pattern "*/support.values.yaml"
    """
    patterns_to_match = ["*/cluster.yaml", "*/*.values.yaml", "*/support.values.yaml"]
    cluster_filepaths = []

    # Identify cluster paths amongst target paths depending on the files they contain
    for pattern in patterns_to_match:
        cluster_filepaths.extend(fnmatch.filter(input_file_list, pattern))

    # Get absolute paths
    cluster_filepaths = [Path(filepath).parent for filepath in cluster_filepaths]
    # Get unique absolute paths
    cluster_filepaths = list(set(cluster_filepaths))

    # Filter for all added/modified cluster config files
    cluster_files = set(fnmatch.filter(input_file_list, "*/cluster.yaml"))

    # Filter for all added/modified helm chart values files
    values_files = set(fnmatch.filter(input_file_list, "*/*.values.yaml"))

    # Filter for all add/modified support chart values files
    support_files = set(fnmatch.filter(input_file_list, "*/support.values.yaml"))

    return cluster_filepaths, cluster_files, values_files, support_files


def generate_hub_matrix_jobs(
    cluster_filepaths,
    modified_cluster_files,
    modified_values_files,
    upgrade_all_hubs=False,
):
    """Generate a list of dictionaries describing which hubs on which clusters need
    to undergo a helm upgrade based on whether their associated helm chart values
    files have been modified. To be parsed to GitHub Actions in order to generate
    parallel jobs in a matrix.

    Args:
        cluster_filepaths (list[path obj]): List of absolute paths to cluster folders
        modified_cluster_files (set[list]): A set of all */cluster.yaml files that have
            been added or modified
        modified_values_files (set[list]): A set of all */*.values.yaml files that have
            been added or modified
        upgrade_all_hubs (bool, optional): If True, generates jobs to upgrade all hubs
            on all clusters. This is triggered when common config has been modified,
            such as basehub or daskhub helm charts. Defaults to False.

    Returns:
        list[dict]: A list of dictionaries. Each dictionary contains: the name of a
            cluster, the cloud provider that cluster runs on, and the name of a hub
            deployed to that cluster.
    """
    # Empty list to store the matrix job definitions in
    matrix_jobs = []
    copy_input = cluster_filepaths.copy()

    # This flag will allow us to establish when a cluster.yaml file has been updated
    # and all hubs on that cluster should be upgraded, without also upgrading all hubs
    # on all other clusters
    upgrade_all_hubs_on_this_cluster = False

    if upgrade_all_hubs:
        print(
            "Common config has been updated. Generating jobs to upgrade all hubs on ALL clusters."
        )
        # Overwrite cluster_filepaths to contain paths to all clusters
        cluster_filepaths = Path(os.getcwd()).glob("*/cluster.yaml")

    for cluster_filepath in cluster_filepaths:
        if not upgrade_all_hubs:
            # Check if this cluster file has been modified. If so, set
            # upgrade_all_hubs_on_this_cluster to True
            intersection = modified_cluster_files.intersection(
                [cluster_filepath.joinpath("cluster.yaml")]
            )
            if len(intersection) > 0:
                print(
                    "This cluster.yaml file has been modified. Generating jobs to upgrade all hubs on THIS cluster."
                )
                upgrade_all_hubs_on_this_cluster = True

        # Read in the cluster.yaml file
        with open(cluster_filepath.joinpath("cluster.yaml")) as f:
            cluster_config = yaml.safe_load(f)

        # Generate template dictionary for all jobs associated with this cluster
        cluster_info = {
            "cluster_name": cluster_config.get("name", {}),
            "provider": cluster_config.get("provider", {}),
        }

        # Loop over each hub on this cluster
        for hub in cluster_config.get("hubs", {}):
            if upgrade_all_hubs or upgrade_all_hubs_on_this_cluster:
                # We know we're upgrading all hubs, so just add the hub name to the list
                # of matrix jobs and move on
                matrix_job = cluster_info.copy()
                matrix_job["hub_name"] = hub["name"]
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
                    modified_values_files.intersection(helm_chart_values_files)
                )

                if len(intersection) > 0:
                    # If at least one of the helm chart values files associated with
                    # this hub has been modified, add it to list of matrix jobs to be
                    # upgraded
                    matrix_job = cluster_info.copy()
                    matrix_job["hub_name"] = hub["name"]
                    matrix_jobs.append(matrix_job)

            # Reset upgrade_all_hubs_on_this_cluster for the next iteration
            upgrade_all_hubs_on_this_cluster = False

    return matrix_jobs


def generate_support_matrix_jobs(modified_dirpaths, upgrade_all_clusters=False):
    """Generate a list of dictionaries describing which clusters need to undergo a helm
    upgrade of their support chart based on whether their cluster.yaml file or
    associated support chart values files have been modified. To be parsed to GitHub
    Actions in order to generate parallel jobs in a matrix.

    Args:
        modified_dirpaths (list[path obj]): List of absolute paths to cluster folders
            that contain EITHER a modified cluster.yaml OR a modified support.values.yaml
            file
        upgrade_all_clusters (bool, optional): If True, generates jobs to update the
            support chart on all clusters. This is triggered when common config has been
            modified in the support helm chart. Defaults to False.

    Returns:
        list[dict]: A list of dictionaries. Each dictionary contains: the name of a
            cluster and the cloud provider that cluster runs on.
    """
    # Empty list to store the matrix definitions in
    matrix_jobs = []

    if upgrade_all_clusters:
        print(
            "Common config has been updated. Generating jobs to upgrade all hubs on all clusters."
        )
        # Overwrite modified_dirpaths to contain paths to all clusters
        modified_dirpaths = Path(os.getcwd()).glob("*/cluster.yaml")

    for cluster_filepath in modified_dirpaths:
        # Read in the cluster.yaml file
        with open(cluster_filepath.joinpath("cluster.yaml")) as f:
            cluster_config = yaml.safe_load(f)

        # Generate a dictionary-style job entry for this cluster
        cluster_info = {
            "cluster_name": cluster_config.get("name", {}),
            "provider": cluster_config.get("provider", {}),
        }

        # Double-check that support is defined for this cluster. If it is, append the
        # job definition to the list of matrix jobs.
        support_config = cluster_config.get("support", {})
        if support_config:
            matrix_jobs.append(cluster_info)
        else:
            print(f"No support defined for cluster: {cluster_config.get('name', {})}")

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

    # Add rows
    for job in support_matrix_jobs:
        support_table.add_row(job["provider"], job["cluster_name"])

    # Construct table for hub helm chart upgrades
    hub_table = Table(title="Hub helm chart upgrades")
    hub_table.add_column("Cloud Provider")
    hub_table.add_column("Cluster Name")
    hub_table.add_column("Hub Name")

    # Add rows
    for job in hub_matrix_jobs:
        hub_table.add_row(job["provider"], job["cluster_name"], job["hub_name"])

    console = Console()
    console.print(support_table)
    console.print(hub_table)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "filepaths",
        nargs="?",
        type=convert_string_to_list,
        help="A singular or space-delimited list of newly added or modified filepaths in the repo",
    )
    parser.add_argument(
        "--pretty-print",
        action="store_true",
        help="Pretty print the calculated matrix jobs as tables using rich",
    )

    args = parser.parse_args()

    # Discover if the support chart has been modified and all clusters should be upgraded
    support_matches = []
    support_matches.extend(fnmatch.filter(args.filepaths, support_chart_filepath))
    upgrade_all_clusters = len(support_matches) > 0

    # Discover if any common config has been modified and all hubs on all clusters should
    # be upgraded
    common_config_matches = []
    for common_filepath_pattern in common_filepaths:
        common_config_matches.extend(
            fnmatch.filter(args.filepaths, common_filepath_pattern)
        )
    upgrade_all_hubs = len(common_config_matches) > 0

    # Generate a list of filepaths to target cluster folders, and sets of affected
    # cluster.yaml files, hub helm chart values files, and support helm chart values
    # files
    (
        target_cluster_filepaths,
        target_cluster_files,
        target_values_files,
        target_support_files,
    ) = generate_lists_of_filepaths_and_filenames(args.filepaths)

    # Generate a job matrix of all hubs that need upgrading
    hub_matrix_jobs = generate_hub_matrix_jobs(
        target_cluster_filepaths,
        target_cluster_files,
        target_values_files,
        upgrade_all_hubs=upgrade_all_hubs,
    )

    # We want to upgrade the support chart on a cluster that has a modified cluster.yaml
    # file or a modified associated support.values.yaml file. We calculate this by
    # taking the Union of the folder paths of both modified_cluster_files and
    # modified_support_files and generating jobs to upgrade all clusters in the
    # resulting set.
    modified_cluster_filepaths = {
        Path(filepath).parent for filepath in target_cluster_files
    }
    modified_support_filepaths = {
        Path(filepath).parent for filepath in target_support_files
    }
    modified_paths_for_support_upgrade = list(
        modified_cluster_filepaths.union(modified_support_filepaths)
    )

    # Generate a job matrix of all clusters that need their support chart upgrading
    support_matrix_jobs = generate_support_matrix_jobs(
        modified_paths_for_support_upgrade,
        upgrade_all_clusters=upgrade_all_clusters,
    )

    if args.pretty_print:
        pretty_print_matrix_jobs(hub_matrix_jobs, support_matrix_jobs)
    else:
        # Add these matrix jobs to the GitHub environment for use in another job
        update_github_env(hub_matrix_jobs, support_matrix_jobs)


if __name__ == "__main__":
    main()
