import os
from pathlib import Path

from helm_upgrade_decision_logic import (
    generate_hub_matrix_jobs,
    generate_lists_of_filepaths_and_filenames,
)


def test_generate_lists_of_filepaths_and_filenames():
    input_filepaths = [
        os.path.join("config", "clusters", "cluster1", "cluster.yaml"),
        os.path.join("config", "clusters", "cluster1", "hub1.values.yaml"),
        os.path.join("config", "clusters", "cluster1", "support.values.yaml"),
    ]

    # Expected returns
    expected_cluster_filepaths = [Path("config/clusters/cluster1")]
    expected_cluster_files = {
        os.path.join("config", "clusters", "cluster1", "cluster.yaml")
    }
    expected_values_files = {
        os.path.join("config", "clusters", "cluster1", "hub1.values.yaml"),
        os.path.join("config", "clusters", "cluster1", "support.values.yaml"),
    }
    expected_support_files = {
        os.path.join("config", "clusters", "cluster1", "support.values.yaml")
    }

    (
        target_cluster_filepaths,
        target_cluster_files,
        target_values_files,
        target_support_files,
    ) = generate_lists_of_filepaths_and_filenames(input_filepaths)

    assert target_cluster_filepaths == expected_cluster_filepaths
    assert target_cluster_files == expected_cluster_files
    assert target_values_files == expected_values_files
    assert target_support_files == expected_support_files

    assert isinstance(target_cluster_filepaths, list)
    assert isinstance(target_cluster_files, set)
    assert isinstance(target_values_files, set)
    assert isinstance(target_support_files, set)
