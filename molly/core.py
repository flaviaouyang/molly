import datetime
import logging

from molly.coordinator import Coordinator
from molly.messengers.factory import messenger_factory

logger = logging.getLogger(__name__)


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
