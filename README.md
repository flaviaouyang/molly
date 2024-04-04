# Molly
[![Molly Pipeline Test](https://github.com/flaviaouyang/molly/actions/workflows/pipeline-test.yml/badge.svg?branch=master)](https://github.com/flaviaouyang/molly/actions/workflows/pipeline-test.yml)

Molly is a data quality monitoring library specifically designed for time series data written in pure Python.

## Description

Molly is designed to be used in conjunction with a SQL database such as MySQL or PostgreSQL. Currently under development. Pending first release.

The following features will be included in the first release:

- data staleness monitoring
- data completeness monitoring

The following messaging services will be supported in the first release:

- Slack

## Getting Started

### Usage

#### Installation

We strongly recommend using a virtual environment to install Molly.

Standard set up using pip:

```bash
cd molly
conda create -n "molly" python=3.12
conda activate molly
pip install .
```

Development set up using pip:

```bash
cd molly
conda create -n "molly" python=3.12
conda activate molly
pip install -e ".[dev]"
```

<!-- TODO: using molly as a TUI or as an airflow DAG -->
<!-- TODO: scheduling -->

## License

This project is licensed under the MIT License - see the LICENSE.md file for details
