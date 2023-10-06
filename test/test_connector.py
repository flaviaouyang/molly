from molly.connector import SQLConnector


def test_sql_connector():
    sql = SQLConnector(
        drivername="postgresql",
        username="flavia",
        password="VHbhugNIbqBv0haBb9",
        host="localhost",
        port=5432,
        database="molly",
    )
    # data = sql.select(schema="stock", table="bank_of_america")
    # data = sql.select(schema="stock", table="bank_of_america", limit=10)
    # data = sql.select(
    #     schema="stock", table="bank_of_america", where={"Date": "2022-10-11"}
    # )
    data = sql.select(
        schema="stock",
        table="bank_of_america",
        where=[{"column": "Date", "value": "2022-11-10", "operator": ">"}],
        limit=15,
    )
