import pytest
from molly.connector import SQLConnector


# @pytest.mark.skip(
#     reason="Should be tested locally. Remote connection to database is yet to be implemented"
# )
def test_sql_connector():
    sql = SQLConnector(
        db_url="postgresql://localhost:5432/test"
    )
    print(sql.read_table("bank_of_canada", "foreign_exchange_rates", {"date": "%Y-%m-%d"}))
