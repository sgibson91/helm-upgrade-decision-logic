# Testing decision logic for helm upgrade

This repository is a sandbox for testing the logic required for deciding which hubs on which clusters should have a helm upgrade based on changed filepaths.

**What it contains:**

- `helm-upgrade-decision-logic.py`: A Python script that analyses a list of filenames and decides which clusters/hubs need a helm upgrade, for their hub helm chart and the support helm chart. It adds a list of dictionaries to the GitHub Actions environment that can be used to configure matrix jobs in a later job. There is also a `--pretty-print` option that prints a human-readable table of jobs that will be run using the `rich` package.
- `.github/workflows/helm-upgrade-decision-logic.yaml`: A GitHub Action workflow that uses [`jitterbit/get-changed-files`](https://github.com/jitterbit/get-changed-files) to establish which files have been changed and pass this list to the Python script.

There is a `config/clusters` folder and `helm-charts` folder as we would see in the `2i2c-org/infrastructure repo`.

Please make as many Pull Requests as you like to test the workflow!
