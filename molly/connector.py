import logging
from dataclasses import dataclass
from typing import Iterable, overload

import pandas as pd
from sqlalchemy import create_engine, MetaData, Select, Table

logger = logging.getLogger(__name__)


@dataclass
class SQLConnector(object):
    __db_url: str

    def __post_init__(self):
        self.__engine = create_engine(self.__db_url)

    def read_table(
        self, schema_name: str, table_name: str, parse_dates: dict, **kwargs
    ) -> pd.DataFrame:
        return pd.read_sql_table(
            table_name=table_name,
            con=self.__engine,
            schema=schema_name,
            parse_dates=parse_dates,
            **kwargs,
        )

    def construct_table(self, schema_name: str, table_name: str) -> Table:
        metadata = MetaData()
        metadata.reflect(bind=self.__engine)
        return Table(
            table_name,
            metadata,
            schema=schema_name,
            autoload_with=self.__engine,
        )

    # TODO: what???????
    @overload
    def execute_query(self, query: str) -> pd.DataFrame: ...

    @overload
    def execute_query(self, query: Iterable[Select]) -> pd.DataFrame: ...

    @overload
    def execute_query(self, query: Select) -> Iterable[pd.DataFrame]: ...

    def execute_query(self, query) -> Iterable[pd.DataFrame]:
        if isinstance(query, str) or isinstance(query, Select):
            return pd.read_sql_query(query, self.__engine)
        elif isinstance(query, Iterable):
            return (pd.read_sql_query(q, self.__engine) for q in query)
        else:
            raise ValueError(f"Unsupported query type: {type(query)}")
