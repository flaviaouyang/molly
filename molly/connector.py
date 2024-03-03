from dataclasses import dataclass
from typing import List, Mapping, Sequence
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dataclass
class SQLConnector:
    db_url: str

    def __post_init__(self):
        self.engine = create_engine(self.db_url)

    def read_table(
        self, schema_name: str, table_name: str, parse_dates: dict, **kwargs
    ) -> pd.DataFrame:
        return pd.read_sql_table(
            table_name=table_name,
            con=self.engine,
            schema=schema_name,
            parse_dates=parse_dates,
            **kwargs,
        )
