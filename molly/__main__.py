import argparse
import json
import logging
import os

from molly.core import monitor_data_quality

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "configuration_file",
        help="Path to the configuration file.",
        type=str,
    )
    args = parser.parse_args()

    assert os.path.exists(args.configuration_file), "Configuration file does not exist."
    assert args.configuration_file.endswith(
        ".json"
    ), "Configuration file should be a JSON file."
    with open(args.configuration_file, "r") as f:
        config = json.load(f)
    for required_key in [
        "data_quality_rules",
        "sql_credentials",
        "monitor_platform_config",
    ]:
        assert (
            required_key in config
        ), f"{required_key} is required in the configuration."

    monitor_data_quality(
        data_quality_rules=config["data_quality_rules"],
        sql_credentials=config["sql_credentials"],
        monitor_platform_config=config["monitor_platform_config"],
    )
