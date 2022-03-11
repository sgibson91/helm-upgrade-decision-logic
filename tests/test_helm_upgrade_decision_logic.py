import os
from pathlib import Path

from .helm_upgrade_decision_logic import (
    generate_hub_matrix_jobs,
    generate_lists_of_filepaths_and_filenames,
    generate_support_matrix_jobs,
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


def test_generate_hub_matrix_jobs_one_cluster_one_hub():
    input_cluster_filepaths = [Path("tests/config/clusters/cluster1")]
    input_cluster_files = set()
    input_values_files = {
        os.path.join("tests", "config", "clusters", "cluster1", "hub1.values.yaml")
    }

    expected_matrix_jobs = [
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub1"}
    ]

    result_matrix_jobs = generate_hub_matrix_jobs(
        input_cluster_filepaths, input_cluster_files, input_values_files
    )

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "hub_name" in result_matrix_jobs[0].keys()


def test_generate_hub_matrix_jobs_one_cluster_many_hubs():
    input_cluster_filepaths = [Path("tests/config/clusters/cluster1")]
    input_cluster_files = set()
    input_values_files = {
        os.path.join("tests", "config", "clusters", "cluster1", "hub1.values.yaml"),
        os.path.join("tests", "config", "clusters", "cluster1", "hub2.values.yaml"),
    }

    expected_matrix_jobs = [
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub1"},
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub2"},
    ]

    result_matrix_jobs = generate_hub_matrix_jobs(
        input_cluster_filepaths, input_cluster_files, input_values_files
    )

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "hub_name" in result_matrix_jobs[0].keys()


def test_generate_hub_matrix_jobs_one_cluster_all_hubs():
    input_cluster_filepaths = [Path("tests/config/clusters/cluster1")]
    input_cluster_files = {
        os.path.join("tests", "config", "clusters", "cluster1", "cluster.yaml")
    }
    input_values_files = {
        os.path.join("tests", "config", "clusters", "cluster1", "hub1.values.yaml"),
    }

    expected_matrix_jobs = [
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub1"},
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub2"},
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub3"},
    ]

    result_matrix_jobs = generate_hub_matrix_jobs(
        input_cluster_filepaths, input_cluster_files, input_values_files
    )

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "hub_name" in result_matrix_jobs[0].keys()


def test_generate_hub_matrix_jobs_many_clusters_one_hub():
    input_cluster_filepaths = [
        Path("tests/config/clusters/cluster1"),
        Path("tests/config/clusters/cluster2"),
    ]
    input_cluster_files = set()
    input_values_files = {
        os.path.join("tests", "config", "clusters", "cluster1", "hub1.values.yaml"),
        os.path.join("tests", "config", "clusters", "cluster2", "hub1.values.yaml"),
    }

    expected_matrix_jobs = [
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub1"},
        {"provider": "aws", "cluster_name": "cluster2", "hub_name": "hub1"},
    ]

    result_matrix_jobs = generate_hub_matrix_jobs(
        input_cluster_filepaths, input_cluster_files, input_values_files
    )

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "hub_name" in result_matrix_jobs[0].keys()


def test_generate_hub_matrix_jobs_many_clusters_many_hubs():
    input_cluster_filepaths = [
        Path("tests/config/clusters/cluster1"),
        Path("tests/config/clusters/cluster2"),
    ]
    input_cluster_files = set()
    input_values_files = {
        os.path.join("tests", "config", "clusters", "cluster1", "hub1.values.yaml"),
        os.path.join("tests", "config", "clusters", "cluster1", "hub2.values.yaml"),
        os.path.join("tests", "config", "clusters", "cluster2", "hub1.values.yaml"),
        os.path.join("tests", "config", "clusters", "cluster2", "hub2.values.yaml"),
    }

    expected_matrix_jobs = [
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub1"},
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub2"},
        {"provider": "aws", "cluster_name": "cluster2", "hub_name": "hub1"},
        {"provider": "aws", "cluster_name": "cluster2", "hub_name": "hub2"},
    ]

    result_matrix_jobs = generate_hub_matrix_jobs(
        input_cluster_filepaths, input_cluster_files, input_values_files
    )

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "hub_name" in result_matrix_jobs[0].keys()


def test_generate_hub_matrix_jobs_all_clusters_all_hubs():
    input_cluster_filepaths = [Path("tests/config/clusters/cluster1")]
    input_cluster_files = set()
    input_values_files = {}

    expected_matrix_jobs = [
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub1"},
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub2"},
        {"provider": "gcp", "cluster_name": "cluster1", "hub_name": "hub3"},
        {"provider": "aws", "cluster_name": "cluster2", "hub_name": "hub1"},
        {"provider": "aws", "cluster_name": "cluster2", "hub_name": "hub2"},
        {"provider": "aws", "cluster_name": "cluster2", "hub_name": "hub3"},
    ]

    result_matrix_jobs = generate_hub_matrix_jobs(
        input_cluster_filepaths,
        input_cluster_files,
        input_values_files,
        upgrade_all_hubs=True,
    )

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "hub_name" in result_matrix_jobs[0].keys()


def test_generate_support_matrix_jobs_one_cluster():
    input_dirpaths = [Path("tests/config/clusters/cluster1")]

    expected_matrix_jobs = [{"provider": "gcp", "cluster_name": "cluster1"}]

    result_matrix_jobs = generate_support_matrix_jobs(input_dirpaths)

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()


def test_generate_support_matrix_jobs_many_clusters():
    input_dirpaths = [
        Path("tests/config/clusters/cluster1"),
        Path("tests/config/clusters/cluster2"),
    ]

    expected_matrix_jobs = [
        {"provider": "gcp", "cluster_name": "cluster1"},
        {"provider": "aws", "cluster_name": "cluster2"},
    ]

    result_matrix_jobs = generate_support_matrix_jobs(input_dirpaths)

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()


def test_generate_support_matrix_jobs_all_clusters():
    input_dirpaths = [Path("tests/config/clusters/cluster1")]

    expected_matrix_jobs = [
        {"provider": "gcp", "cluster_name": "cluster1"},
        {"provider": "aws", "cluster_name": "cluster2"},
    ]

    result_matrix_jobs = generate_support_matrix_jobs(
        input_dirpaths, upgrade_all_clusters=True
    )

    assert result_matrix_jobs == expected_matrix_jobs
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
