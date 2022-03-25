import os
from pathlib import Path
from unittest import TestCase

from ruamel.yaml import YAML

from mymodule.helm_upgrade_decision_logic import (
    assign_staging_matrix_jobs,
    discover_modified_common_files,
    generate_hub_matrix_jobs,
    generate_support_matrix_jobs,
    get_all_cluster_yaml_files,
)

yaml = YAML(typ="safe", pure=True)
case = TestCase()


def test_get_all_cluster_yaml_files():
    expected_cluster_files = {
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/cluster.yaml"),
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster2/cluster.yaml"),
    }

    result_cluster_files = get_all_cluster_yaml_files(test_env=True)

    assert result_cluster_files == expected_cluster_files
    assert isinstance(result_cluster_files, set)


def test_generate_hub_matrix_jobs_one_hub():
    cluster_file = Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/cluster.yaml")
    with open(cluster_file) as f:
        cluster_config = yaml.load(f)

    cluster_info = {
        "cluster_name": cluster_config.get("name", {}),
        "provider": cluster_config.get("provider", {}),
        "reason_for_redeploy": "",
    }

    modified_file = {
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/hub1.values.yaml"),
    }

    expected_matrix_jobs = [
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "hub_name": "hub1",
            "reason_for_redeploy": "Following helm chart values files were modified:\n- hub1.values.yaml",
        }
    ]

    result_matrix_jobs = generate_hub_matrix_jobs(cluster_file, cluster_config, cluster_info, modified_file)

    case.assertCountEqual(result_matrix_jobs, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "hub_name" in result_matrix_jobs[0].keys()
    assert "reason_for_redeploy" in result_matrix_jobs[0].keys()


def test_generate_hub_matrix_jobs_many_hubs():
    cluster_file = Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/cluster.yaml")
    with open(cluster_file) as f:
        cluster_config = yaml.load(f)

    cluster_info = {
        "cluster_name": cluster_config.get("name", {}),
        "provider": cluster_config.get("provider", {}),
        "reason_for_redeploy": "",
    }

    modified_files = {
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/hub1.values.yaml"),
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/hub2.values.yaml"),
    }

    expected_matrix_jobs = [
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "hub_name": "hub1",
            "reason_for_redeploy": "Following helm chart values files were modified:\n- hub1.values.yaml",
        },
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "hub_name": "hub2",
            "reason_for_redeploy": "Following helm chart values files were modified:\n- hub2.values.yaml",
        },
    ]

    result_matrix_jobs = generate_hub_matrix_jobs(
        cluster_file,
        cluster_config,
        cluster_info,
        modified_files,
    )

    case.assertCountEqual(result_matrix_jobs, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "hub_name" in result_matrix_jobs[0].keys()
    assert "reason_for_redeploy" in result_matrix_jobs[0].keys()


def test_generate_hub_matrix_jobs_all_hubs():
    cluster_file = Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/cluster.yaml")
    with open(cluster_file) as f:
        cluster_config = yaml.load(f)

    cluster_info = {
        "cluster_name": cluster_config.get("name", {}),
        "provider": cluster_config.get("provider", {}),
        "reason_for_redeploy": "cluster.yaml file was modified",
    }

    modified_files = {
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/hub1.values.yaml"),
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/cluster.yaml"),
    }

    expected_matrix_jobs = [
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "hub_name": "staging",
            "reason_for_redeploy": "cluster.yaml file was modified",
        },
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "hub_name": "hub1",
            "reason_for_redeploy": "cluster.yaml file was modified",
        },
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "hub_name": "hub2",
            "reason_for_redeploy": "cluster.yaml file was modified",
        },
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "hub_name": "hub3",
            "reason_for_redeploy": "cluster.yaml file was modified",
        },
    ]

    result_matrix_jobs_1 = generate_hub_matrix_jobs(
        cluster_file,
        cluster_config,
        cluster_info,
        modified_files,
        upgrade_all_hubs_on_this_cluster=True,
    )
    result_matrix_jobs_2 = generate_hub_matrix_jobs(
        cluster_file,
        cluster_config,
        cluster_info,
        modified_files,
        upgrade_all_hubs_on_all_clusters=True,
    )
    result_matrix_jobs_3 = generate_hub_matrix_jobs(
        cluster_file,
        cluster_config,
        cluster_info,
        modified_files,
        upgrade_all_hubs_on_this_cluster=True,
        upgrade_all_hubs_on_all_clusters=True,
    )

    case.assertCountEqual(result_matrix_jobs_1, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs_1, list)
    assert isinstance(result_matrix_jobs_1[0], dict)

    case.assertCountEqual(result_matrix_jobs_2, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs_2, list)
    assert isinstance(result_matrix_jobs_2[0], dict)

    case.assertCountEqual(result_matrix_jobs_3, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs_3, list)
    assert isinstance(result_matrix_jobs_3[0], dict)

    assert "provider" in result_matrix_jobs_1[0].keys()
    assert "cluster_name" in result_matrix_jobs_1[0].keys()
    assert "hub_name" in result_matrix_jobs_1[0].keys()
    assert "reason_for_redeploy" in result_matrix_jobs_1[0].keys()

    assert "provider" in result_matrix_jobs_2[0].keys()
    assert "cluster_name" in result_matrix_jobs_2[0].keys()
    assert "hub_name" in result_matrix_jobs_2[0].keys()
    assert "reason_for_redeploy" in result_matrix_jobs_2[0].keys()

    assert "provider" in result_matrix_jobs_3[0].keys()
    assert "cluster_name" in result_matrix_jobs_3[0].keys()
    assert "hub_name" in result_matrix_jobs_3[0].keys()
    assert "reason_for_redeploy" in result_matrix_jobs_3[0].keys()


def test_generate_support_matrix_jobs_one_cluster():
    cluster_file = Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/cluster.yaml")
    with open(cluster_file) as f:
        cluster_config = yaml.load(f)

    cluster_info = {
        "cluster_name": cluster_config.get("name", {}),
        "provider": cluster_config.get("provider", {}),
        "reason_for_redeploy": "",
    }

    modified_file = {
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/support.values.yaml"),
    }

    expected_matrix_jobs = [
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "upgrade_support": True,
            "reason_for_support_redeploy": "Following helm chart values files were modified:\n- support.values.yaml",
        }
    ]

    result_matrix_jobs = generate_support_matrix_jobs(cluster_file, cluster_config, cluster_info, modified_file)

    case.assertCountEqual(result_matrix_jobs, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs, list)
    assert isinstance(result_matrix_jobs[0], dict)

    assert "provider" in result_matrix_jobs[0].keys()
    assert "cluster_name" in result_matrix_jobs[0].keys()
    assert "upgrade_support" in result_matrix_jobs[0].keys()
    assert "reason_for_support_redeploy" in result_matrix_jobs[0].keys()
    assert result_matrix_jobs[0]["upgrade_support"]


def test_generate_support_matrix_jobs_all_clusters():
    cluster_file = Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/cluster.yaml")
    with open(cluster_file) as f:
        cluster_config = yaml.load(f)

    cluster_info = {
        "cluster_name": cluster_config.get("name", {}),
        "provider": cluster_config.get("provider", {}),
        "reason_for_redeploy": "",
    }

    modified_file = {
        Path(os.getcwd()).joinpath("tests/test-clusters/cluster1/support.values.yaml"),
    }

    expected_matrix_jobs = [
        {
            "provider": "gcp",
            "cluster_name": "cluster1",
            "upgrade_support": True,
            "reason_for_support_redeploy": "Support helm chart has been modified",
        }
    ]

    result_matrix_jobs_1 = generate_support_matrix_jobs(cluster_file, cluster_config, cluster_info.copy(), modified_file, upgrade_support_on_this_cluster=True)
    result_matrix_jobs_2 = generate_support_matrix_jobs(cluster_file, cluster_config, cluster_info.copy(), modified_file, upgrade_support_on_all_clusters=True)
    result_matrix_jobs_3 = generate_support_matrix_jobs(cluster_file, cluster_config, cluster_info.copy(), modified_file, upgrade_support_on_this_cluster=True, upgrade_support_on_all_clusters=True)

    case.assertCountEqual(result_matrix_jobs_1, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs_1, list)
    assert isinstance(result_matrix_jobs_1[0], dict)

    case.assertCountEqual(result_matrix_jobs_2, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs_2, list)
    assert isinstance(result_matrix_jobs_2[0], dict)

    case.assertCountEqual(result_matrix_jobs_3, expected_matrix_jobs)
    assert isinstance(result_matrix_jobs_3, list)
    assert isinstance(result_matrix_jobs_3[0], dict)

    assert "provider" in result_matrix_jobs_1[0].keys()
    assert "cluster_name" in result_matrix_jobs_1[0].keys()
    assert "upgrade_support" in result_matrix_jobs_1[0].keys()
    assert "reason_for_support_redeploy" in result_matrix_jobs_1[0].keys()
    assert result_matrix_jobs_1[0]["upgrade_support"]

    assert "provider" in result_matrix_jobs_2[0].keys()
    assert "cluster_name" in result_matrix_jobs_2[0].keys()
    assert "upgrade_support" in result_matrix_jobs_2[0].keys()
    assert "reason_for_support_redeploy" in result_matrix_jobs_2[0].keys()
    assert result_matrix_jobs_2[0]["upgrade_support"]

    assert "provider" in result_matrix_jobs_3[0].keys()
    assert "cluster_name" in result_matrix_jobs_3[0].keys()
    assert "upgrade_support" in result_matrix_jobs_3[0].keys()
    assert "reason_for_support_redeploy" in result_matrix_jobs_3[0].keys()
    assert result_matrix_jobs_3[0]["upgrade_support"]


def test_discover_modified_common_files_hub_helm_charts():
    input_path_basehub = [os.path.join("helm-charts", "basehub", "Chart.yaml")]
    input_path_daskhub = [os.path.join("helm-charts", "daskhub", "Chart.yaml")]

    (
        basehub_upgrade_all_clusters,
        basehub_upgrade_all_hubs,
    ) = discover_modified_common_files(input_path_basehub)
    (
        daskhub_upgrade_all_clusters,
        daskhub_upgrade_all_hubs,
    ) = discover_modified_common_files(input_path_daskhub)

    assert not basehub_upgrade_all_clusters
    assert basehub_upgrade_all_hubs
    assert not daskhub_upgrade_all_clusters
    assert daskhub_upgrade_all_hubs


def test_discover_modified_common_files_support_helm_chart():
    modified_files = [os.path.join("helm-charts", "support", "Chart.yaml")]

    upgrade_all_clusters, upgrade_all_hubs = discover_modified_common_files(modified_files)

    assert upgrade_all_clusters
    assert not upgrade_all_hubs
