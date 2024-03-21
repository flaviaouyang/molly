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
    def __post_init__(self):
        self.__query_complete = False
        self.__validation_complete = False

    @property
    def feature_name(self) -> str:
        return "completeness"

    @property
    def query_info(self) -> str:
        if not self.__query_complete:
            raise ValueError(
                "Query info is not set. Try calling construct_query() first."
            )
        return f"Query info:\n{self.__query_info}"

    @property
    def validation_info(self) -> str:
        if not self.__validation_complete:
            raise ValueError(
                "Validation info is not set. Try calling validate() first."
            )
        return f"Validation info:\n{self.__validation_info}"

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
        self.__query_info = f"Time series index column: {time_series_index_column}\n"
        self.__query_info += (
            f"Searching for records between {start_time} and {end_time}\n"
        )

        # Add attribute constraints if any
        for attribute_column, attribute_values in self.configurations.get(
            "attribute_map", {}
        ).items():
            query = query.where(
                self.subject_table.c[attribute_column].in_(attribute_values)
            )
            self.__query_info += (
                f"Attribute: {attribute_column} in {attribute_values}\n"
            )

        # Add complex filter if any
        for complex_filter in self.configurations.get("complex_filters", []):
            query = query.where(text(complex_filter))
            self.__query_info += f"Complex Filter: {complex_filter}\n"

        # Since it's completeness, we only care about the count of the records
        query = select(func.count("*")).select_from(query)
        self.__query_info += (
            "Only query for the count of records satisfying the conditions"
        )
        self.__query_complete = True
        logger.debug(f"Built Completeness Query:\n{query}")
        return query

    def validate(
        self, retrieved_data: Union[pd.DataFrame, Iterable[pd.DataFrame]]
    ) -> bool:
        required_ticks = self.requirements["required_ticks"]
        actual_ticks = retrieved_data.iloc[0, 0]
        self.__validation_info = f"Maximum Ticks: {self.requirements['maximum_ticks']}\nRequired Ticks: {required_ticks}\nFound {actual_ticks} ticks\n"
        result = actual_ticks >= required_ticks
        self.__validation_info += f"Completeness Check result: {result}"
        self.__validation_complete = True
        logger.debug(
            f"Validating Completeness: Required Ticks: {required_ticks}, Actual Ticks: {actual_ticks}.\nResult: {result}"
        )
        return result

    def describe(self) -> str:
        if not self.__validation_complete or not self.__query_complete:
            raise ValueError(
                "No description. Try calling construct_query() and validate() first."
            )
        description = f"Feature: {self.feature_name}\n\n{self.query_info}\n\n{self.validation_info}"
        description = f"====================\n{description}\n===================="
        description = f"\nTable: {self.subject_table.schema}.{self.subject_table.name}\n{description}"
        return description
