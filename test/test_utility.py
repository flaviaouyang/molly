from datetime import datetime, timedelta, timezone

import pytest

from molly.utility.datetime import parse_date


@pytest.mark.parametrize(
    "value, expected_value",
    [
        (datetime(2021, 1, 1), datetime(2021, 1, 1)),
        (-1, -1),
        (10, 10),
        (("minutes", -30), None),
    ],
)
def test_parse_date(value, expected_value):
    now = datetime.now(tz=timezone.utc)
    if isinstance(expected_value, int):
        expected_value = now + timedelta(days=expected_value)
    if not expected_value:
        assert isinstance(parse_date(value), datetime)
    else:
        assert (
            parse_date(value).date() == expected_value.date()
        ), f"Expected {expected_value.date()} but got {parse_date(value).date()}"
