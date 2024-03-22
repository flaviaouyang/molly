import logging
from dataclasses import dataclass
from typing import ClassVar, Iterable, Union

import pandas as pd
from sqlalchemy import Select, and_, select, text, func

from molly.features.feature import Feature
from molly.utility.datetime import parse_date

logger = logging.getLogger(__name__)


@dataclass
class Completeness(Feature):
    @property
    def feature_name(self) -> str:
        return "completeness"

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

        # Since it's completeness, we only care about the number of the records
        query = select(func.count("*")).select_from(query)
        logger.debug(f"Built Completeness Query:\n{query}")
        return query

    def validate(
        self, retrieved_data: Union[pd.DataFrame, Iterable[pd.DataFrame]]
    ) -> bool:
        required_ticks = self.requirements["required_ticks"]
        actual_ticks = retrieved_data.iloc[0, 0]
        result = actual_ticks >= required_ticks
        logger.debug(
            f"Validating Completeness: Required Ticks: {required_ticks}, Actual Ticks: {actual_ticks}.\nResult: {result}"
        )
        return result

    def describe(self) -> str:
        query_info = self.construct_query()
        validation_info = self.requirements
        description = f"Feature: {self.feature_name}\n\nQuery executed: {query_info}\n\nRequirements: {validation_info}"
        description = f"====================\n{description}\n===================="
        description = f"\nTable: {self.subject_table.schema}.{self.subject_table.name}\n{description}"
        return description
