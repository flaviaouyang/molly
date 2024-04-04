import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Tuple, Union
from dateutil import parser

logger = logging.getLogger(__name__)


def parse_date(value: Union[str, int, datetime, Tuple[str, int], Dict[str, Any]]) -> datetime:
    if isinstance(value, datetime):
        logger.debug(f"Value {value} is already a datetime object. Returning as is.")
        return value

    utc_now = datetime.now(tz=timezone.utc)
    if isinstance(value, int):
        value = ("days", value)
    elif isinstance(value, str):
        parsed_value = parser.parse(value)
    elif isinstance(value, dict):
        value = (value["unit"], value["value"])

    if isinstance(value, Tuple):
        unit, numeric_value = value
        assert unit in [
            "days",
            "seconds",
            "microseconds",
            "milliseconds",
            "minutes",
            "hours",
            "weeks",
        ], (f"Unsupported unit: {unit}. Supported units are days, seconds, microseconds, milliseconds, minutes, hours, "
            f"weeks.")
        assert isinstance(
            numeric_value, int
        ), f"If value is a tuple, it should be (str, int) but got (str, {type(numeric_value)})"
        parsed_value = utc_now + timedelta(**{unit: numeric_value})

    logger.debug(f"Parsed {value} to {parsed_value}")
    return parsed_value
