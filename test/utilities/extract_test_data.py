from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert


def extract_content_from_url(url: str) -> bytes:
    response = requests.get(url)
    return response.content


if __name__ == "__main__":
    date = datetime.now().strftime("%Y%m%d")
    ieso_url = (
        f"http://reports.ieso.ca/public/DispUnconsHOEP/PUB_DispUnconsHOEP_{date}.csv"
    )
    raw_data = extract_content_from_url(ieso_url)

    # Clean up data
    df = pd.read_csv(
        StringIO(raw_data.decode("utf-8")),
        skiprows=[0, 1, 2, 3],
        header=None,
        names=["hour", "price"],
        usecols=[0, 1],
    )
    df["local_timestamp"] = pd.Timestamp(date) + pd.to_timedelta(df["hour"], unit="h")
    df["utc_timestamp"] = (
        df["local_timestamp"]
        .dt.tz_localize("EST")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )
    df = df[["utc_timestamp", "local_timestamp", "price"]]

    # Upsert data to database
    engine = create_engine("postgresql://localhost:5432/test")
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = Table("hourly_energy_price", metadata, schema="ieso", autoload_with=engine)
    statement = insert(table).values(df.to_dict(orient="records"))
    statement = statement.on_conflict_do_update(
        index_elements=["utc_timestamp"],
        set_={
            "local_timestamp": statement.excluded.local_timestamp,
            "price": statement.excluded.price,
        },
    )
    with engine.connect() as connection:
        connection.execute(statement)
        connection.commit()
