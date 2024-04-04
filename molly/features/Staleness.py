import logging
from dataclasses import dataclass
from typing import Iterable, Union

import pandas as pd
from sqlalchemy import Select, select, text

from molly.features.feature import Feature
from molly.utility.datetime import parse_date

logger = logging.getLogger(__name__)


@dataclass
class Staleness(Feature):
    @property
    def feature_name(self) -> str:
        return "staleness"

    def construct_query(self) -> Select:
        availability_column = self.configurations["availability_column"]
        group_by_columns = self.configurations.get("group_by_columns", [])
        complex_filters = self.configurations.get("complex_filters", [])

        # Select the availability column and the group by columns
        select_columns = [self.subject_table.c[availability_column]]
        select_columns.extend([self.subject_table.c[col] for col in group_by_columns])
        query = select(self.subject_table).with_only_columns(*select_columns)

        # Add complex filters if any
        for complex_filter in complex_filters:
            query = query.where(text(complex_filter))

        # Group by the group by columns
        for column in group_by_columns:
            query = query.group_by(self.subject_table.c[column])

        logger.debug(f"Built Staleness Query:\n{query}")
        self.__query_info = query
        return query

    def validate(
        self, retrieved_data: Union[pd.DataFrame, Iterable[pd.DataFrame]]
    ) -> bool:
        # TODO: retrieved_data can also be a generator??? maybe not
        maxmimum_latency = parse_date(["maximum_latency"])

    def describe(self) -> str:
        pass
