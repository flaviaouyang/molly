import logging
from dataclasses import dataclass
from pprint import pformat

import pandas as pd
from sqlalchemy import and_, func, Select, select, text
from tabulate import tabulate

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
        start_time = parse_date(self.configurations["start_time"])
        end_time = parse_date(self.configurations["end_time"])
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
        self.__query_info = query
        return query

    def validate(self, retrieved_data: pd.DataFrame) -> bool:
        required_ticks = self.requirements["required_ticks"]
        maximum_ticks = self.requirements.get("maximum_ticks", None)
        # TODO: retrieved_data can also be a generator??? maybe not
        actual_ticks = retrieved_data.iloc[0, 0]
        result = actual_ticks >= required_ticks
        logger.debug(
            f"Validating Completeness: Required Ticks: {required_ticks}, Actual Ticks: {actual_ticks}. "
            f" Result: {result}"
        )
        if actual_ticks > maximum_ticks:
            logger.warning(
                f"Actual ticks ({actual_ticks}) is greater than maximum ticks ({maximum_ticks})."
            )
        self.__validation_result = result
        return result

    def describe(self) -> str:
        query_info = self.__query_info
        validation_info = self.requirements
        validation_result = self.__validation_result
        description = f"""
            Feature: {self.feature_name}\n
            --------------------------------\n
            Table: {self.subject_table.schema}.{self.subject_table.name}\n
            Rule Configurations:\n{pformat(self.configurations, sort_dicts=False)}\n
            Query executed:\n{query_info}\n
            Requirements:\n{pformat(validation_info, sort_dicts=False)}\n
            Validation Result: {validation_result}
        """
        # XXX: textwrap.dedent doesn't work as expected
        description = "\n".join(line.lstrip() for line in description.split("\n"))
        return tabulate([[description]], tablefmt="grid")
