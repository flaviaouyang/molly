import logging
from typing import Any, Dict, Tuple, Union

from dateutil import parser

from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def parse_date(
    utc_value: Union[str, int, datetime, Tuple[str, int], Dict[str, Any]],
) -> datetime:
    """
    Parse the given value to a datetime object.
    Note: This function expects the value to be in UTC timezone.
    """
    if isinstance(utc_value, datetime):
        logger.debug(
            f"Value {utc_value} is already a datetime object. Returning the same."
        )
        return utc_value

    if isinstance(utc_value, str):
        parsed_value = parser.parse(utc_value)
        logger.debug(f"Parsed {utc_value} to {parsed_value}")
        return parsed_value

    utc_now = datetime.now(tz=timezone.utc)
    if isinstance(utc_value, int):
        processed_value = ("days", utc_value)
    elif isinstance(utc_value, dict):
        processed_value = (utc_value["unit"], utc_value["value"])
    elif isinstance(utc_value, tuple):
        processed_value = utc_value
    else:
        raise TypeError(
            f"Unsupported value type {type(utc_value)}. Supported types are str, int, dict, Tuple[str, int]"
        )

    unit, numeric_value = processed_value
    assert unit in [
        "days",
        "seconds",
        "microseconds",
        "milliseconds",
        "minutes",
        "hours",
        "weeks",
    ], (
        f"Unsupported unit: {unit}. Supported units are days, seconds, microseconds, milliseconds, minutes, hours, "
        f"weeks."
    )
    assert isinstance(
        numeric_value, int
    ), f"If value is a tuple, it should be (str, int) but got (str, {type(numeric_value)})"
    parsed_value = utc_now + timedelta(**{unit: numeric_value})

    logger.debug(f"Parsed {utc_value} to {parsed_value}")
    return parsed_value
