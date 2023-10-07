import os
import pytest
from utility.extract_data import (
    download_stock_data_from_yahoo_finance,
    extract_stock_data_from_yahoo_finance,
)
import logging
from contextlib import nullcontext as does_not_raise

logging.basicConfig(level=logging.INFO)

TEST_DATA_LOCATION = "test/data/"


@pytest.mark.parametrize(
    "tickers, interval, inclusive_start, inclusive_end, expectation",
    [
        ("AAPL", "1d", "2023-10-05 00:00:00", None, does_not_raise()),
        (
            "AAPL MSFT",
            "5m",
            "2023-10-01 00:00:00",
            "2023-10-05 00:00:00",
            does_not_raise(),
        ),
        (
            "DNE",
            "1m",
            "2023-10-01 00:00:00",
            "2023-10-05 00:00:00",
            pytest.raises(ValueError),
        ),
        (
            "AAPL",
            "1m",
            "1023-10-01 00:00:00",
            "1023-10-05 00:00:00",
            pytest.raises(ValueError),
        ),
    ],
)
def test_extract_stock_data_from_yahoo_finance(
    tickers, interval, inclusive_start, inclusive_end, expectation
):
    with expectation:
        stock_data = extract_stock_data_from_yahoo_finance(
            tickers=tickers,
            interval=interval,
            inclusive_start=inclusive_start,
            inclusive_end=inclusive_end,
        )
        assert stock_data.columns.tolist() == [
            "Ticker",
            "UTCDatetime",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
        ]
        assert stock_data["Ticker"].unique().tolist() == tickers.split(" ")


@pytest.mark.parametrize(
    "tickers, interval, inclusive_start, inclusive_end, path, expectation",
    [
        ("AAPL", "1d", "2023-10-05 00:00:00", None, None, does_not_raise()),
        (
            "AAPL MSFT",
            "5m",
            "2023-10-01 00:00:00",
            "2023-10-05 00:00:00",
            "test/data/test.csv",
            does_not_raise(),
        ),
        (
            "AAPL",
            "1m",
            "1023-10-01 00:00:00",
            "1023-10-05 00:00:00",
            None,
            pytest.raises(ValueError),
        ),
    ],
)
def test_download_stock_data_from_yahoo_finance(
    tickers, interval, inclusive_start, inclusive_end, path, expectation
):
    with expectation:
        filepath = download_stock_data_from_yahoo_finance(
            tickers=tickers,
            interval=interval,
            inclusive_start=inclusive_start,
            inclusive_end=inclusive_end,
            path=path,
        )
        assert os.path.isfile(filepath)
