import argparse
import datetime
import json
import logging
import os
from molly.coordinator import Coordinator
from molly.messengers.factory import messenger_factory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def monitor_data_quality(
    data_quality_rules: dict, sql_credentials: dict, monitor_platform_config: dict
) -> None:
    utc_now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    output_message = f"Data Quality Report\nExecution time (UTC): {utc_now}.\n"
    logger.info("Starting data quality monitoring.")

    are_all_passed = True

    dq_coordinator = Coordinator(data_quality_rules, sql_credentials)
    for result, description in dq_coordinator.execute():
        output_message += f"{description}\n"
        are_all_passed = are_all_passed and result

    output_message += f"\nReport Result: {'passed' if are_all_passed else 'at least one check has failed'}"

    messenger = messenger_factory(
        messenger_name=monitor_platform_config["messenger_name"],
        credentials=monitor_platform_config["messenger_credentials"],
    )
    logger.info(f"Sending report via {messenger.messenger_name}")
    messenger.send(
        message=f"```{output_message}```",
        destination=monitor_platform_config["destination"],
    )
    logger.info("Data quality monitoring completed.")


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
