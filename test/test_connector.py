import pandas as pd
import pytest
from sqlalchemy import select

from molly.connector import SQLConnector


@pytest.mark.skip(
    reason="Should be tested locally. Remote connection to database is yet to be implemented"
)
def test_sql_connector():
    sql = SQLConnector("postgresql://localhost:5432/test")
    fx = sql.read_table(
        "bank_of_canada", "foreign_exchange_rates", {"date": "%Y-%m-%d"}
    )
    assert isinstance(fx, pd.DataFrame)

    table_info = sql.construct_table("bank_of_canada", "foreign_exchange_rates")
    assert (
        table_info.name == "foreign_exchange_rates"
        and table_info.schema == "bank_of_canada"
    )

    data = sql.execute_query("SELECT * FROM bank_of_canada.foreign_exchange_rates")
    assert isinstance(next(data), pd.DataFrame)

    queries = (
        select(table_info).where(table_info.c.country == country)
        for country in ["USD", "EUR", "GBP"]
    )

    data_generator = sql.execute_query(queries)
    for data in data_generator:
        print(data.head(5))
        assert isinstance(data, pd.DataFrame)
    print(data_generator)
