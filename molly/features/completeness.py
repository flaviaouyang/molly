import logging
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Select, and_, select, text

from molly.features.feature import Feature
from molly.utility.datetime import parse_date

logger = logging.getLogger(__name__)


@dataclass
class Completeness(Feature):
    def construct_query(self) -> Select:
        # Add time constraints
        time_series_index_column = self.configurations["time_series_index_column"]
        start_time = parse_date(self.configurations["delta_start_time"])
        end_time = parse_date(self.configurations["delta_end_time"])
        query = select(self.subject_table).where(
            and_(
                self.subject_table.c[time_series_index_column] >= start_time,
                self.subject_table.c[time_series_index_column] <= end_time,
            )
        )

        # Add attribute constraints if any
        for attribute_column, attribute_values in self.configurations.get(
            "attribute_map", {}
        ).items():
            query = query.where(
                self.subject_table.c[attribute_column].in_(attribute_values)
            )

        # Add complex filter if any
        for complex_filter in self.configurations.get("complex_filters", []):
            query = query.where(text(complex_filter))
        logger.debug(f"Built Completeness Query:\n{query}")
        return query

    def validate(self, retrieved_data: pd.DataFrame) -> bool:
        pass
