from datetime import datetime, timedelta
from typing import Optional
import yfinance as yf
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def extract_stock_data_from_yahoo_finance(
    tickers: str,
    interval: str,
    inclusive_start: str,
    inclusive_end: Optional[str] = None,
) -> pd.DataFrame:
    """Download stock data from yahoo finance and save it to a csv file.

    Args:
        ticker_names (str): Name or a list of names of the tickers to download.
        interval(str): For which interval to download the data.
            Accepts: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
        inclusive_start (str): Start date of the data to download.
            Format: YYYY-MM-DD HH:MM:SS
        inclusive_end (str): End date of the data to download.
            Format: YYYY-MM-DD HH:MM:SS
            Default: now

    Returns:
        pd.DataFrame: Dataframe with the downloaded data.
        Columns: Ticker, UTCDatetime, Open, High, Low, Close, Volume
    """
    # Convert start and end to datetime
    inclusive_start = datetime.strptime(inclusive_start, "%Y-%m-%d %H:%M:%S")
    if inclusive_end is None:
        inclusive_end = datetime.today() + timedelta(days=1)
    else:
        inclusive_end = datetime.strptime(
            inclusive_end, "%Y-%m-%d %H:%M:%S"
        ) + timedelta(days=1)

    # Download from yahoo finance
    logger.info(f"Downloading data from yahoo finance for {tickers}...")
    logger.info(f"Start: {inclusive_start}")
    logger.info(f"End: {inclusive_end}")
    logger.info(f"Interval: {interval}")
    data = yf.download(
        tickers=tickers,
        interval=interval,
        start=inclusive_start,
        end=inclusive_end,
        group_by="ticker",
        ignore_tz=False,
        threads=True,
        keepna=True,
    )
    if data.empty:
        raise ValueError(
            f"No data found for {tickers} from {inclusive_start} to {inclusive_end} at"
            f" interval {interval}"
        )
    logger.info(f"Downloaded data: \n{data.head(3)}")

    # Unstack data and add Ticker column
    if isinstance(data.columns, pd.MultiIndex):
        data = (
            data.stack(level=0).rename_axis(["Datetime", "Ticker"]).reset_index(level=1)
        )
    else:
        data["Ticker"] = tickers
    data = data.reset_index().rename(columns={data.index.name: "Datetime"})

    # Convert Datetime column to UTCDatetime
    data["UTCDatetime"] = pd.to_datetime(data["Datetime"], utc=True).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    data = data.drop(columns=["Datetime"])

    # Order by Ticker and UTCDatetime
    data = data.sort_values(by=["Ticker", "UTCDatetime"])
    data = data.reset_index(drop=True)

    # Order columns
    data = data[["Ticker", "UTCDatetime", "Open", "High", "Low", "Close", "Volume"]]
    logger.info(f"Transformed data:\n{data.head(6).to_markdown()}")
    return data


def download_stock_data_from_yahoo_finance(
    tickers: str,
    interval: str,
    inclusive_start: str,
    inclusive_end: Optional[str] = None,
    path: Optional[str] = None,
) -> str:
    """Download stock data from yahoo finance and save it to a csv file

    Args:
        ticker_names (str): Name or a list of names of the tickers to download.
        interval(str): For which interval to download the data.
            Accepts: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
        inclusive_start (str): Start date of the data to download.
            Format: YYYY-MM-DD HH:MM:SS
        inclusive_end (str): End date of the data to download.
            Format: YYYY-MM-DD HH:MM:SS
            Default: now
        path (str): Path to save the csv file.
            Default: test/data/<ticker_names>.csv

    Returns:
        str: Path to the saved csv file.
    """
    # Set default path
    if path is None:
        ticker_names = tickers.replace(" ", "_")
        path = f"test/data/{ticker_names}.csv"

    # Download data and save to csv
    extract_stock_data_from_yahoo_finance(
        tickers=tickers,
        interval=interval,
        inclusive_start=inclusive_start,
        inclusive_end=inclusive_end,
    ).to_csv(path_or_buf=path, index=False)

    logger.info(f"Saved data to {path}")
    return path
