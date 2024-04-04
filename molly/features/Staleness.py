import logging
from dataclasses import dataclass

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

    def validate(self, retrieved_data: pd.DataFrame) -> bool:
        is_validate = True
        self.__validation_result = "Validation output:"
        maximum_latency = parse_date(self.requirements["maximum_latency"])
        logger.debug(f"Maximum Latency: {maximum_latency}")
        availability_column = self.configurations["availability_column"]
        group_by_columns = self.configurations.get("group_by_columns", [])

        if not group_by_columns:
            is_validate = retrieved_data[availability_column].max() < maximum_latency
            self.__validation_result += f"""
            There is no group by columns. Data was last updated at {retrieved_data[availability_column].max()}.
            Validation result: {is_validate}
            """
            return is_validate

        self.__validation_result += (
            f"\nFollowing Group by columns used: {group_by_columns}"
        )
        for index, df in retrieved_data.groupby(group_by_columns):
            is_subgroup_validate = df[availability_column].max() < maximum_latency
            self.__validation_result += (
                f"\nIndex: {index}. Data was last updated at {df[availability_column].max()}. "
                f"Validate: {is_subgroup_validate}"
            )
            if not is_subgroup_validate:
                is_validate = False
        self.__validation_result += f"\nValidation result: {is_validate}"
        return is_validate

    def describe(self) -> str:
        print(self.__query_info)
        print(self.__validation_result)
