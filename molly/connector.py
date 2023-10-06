from typing import Dict, List, Mapping, Sequence
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class SQLConnector:
    def __init__(
        self,
        drivername: str,
        username: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        query: Mapping[str, Sequence[str] | str] = {},
    ):
        self.drivername = drivername
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.query = query

    @property
    def url(self):
        return URL.create(
            drivername=self.drivername,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    @property
    def engine(self):
        return create_engine(self.url)

    def select(
        self,
        schema: str,
        table: str,
        where: List[dict] | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        query = f"SELECT * FROM {schema}.{table}"
        if where:
            query += " WHERE"
            for condition in where:
                column = condition["column"]
                value = condition["value"]
                operator = condition["operator"]
                query += f" '{column}' {operator} '{value}' AND"
            query = query[:-4]
        if limit:
            query += f" LIMIT {limit}"
        logger.info(f"Executing query: {query}")
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            result = pd.DataFrame(result.fetchall())
        return result
