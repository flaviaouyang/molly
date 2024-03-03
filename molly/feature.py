from collections import defaultdict
from dataclasses import dataclass
import logging
from pprint import pformat
from typing import Iterable, Tuple

from sqlalchemy import (
    Column,
    Date,
    Engine,
    MetaData,
    Selectable,
    String,
    Table,
    and_,
    select,
    table,
)

from molly.expectation import Expectation
from molly.utility.datetime import parse_date


logger = logging.getLogger(__name__)

SUPPORTED_FEATURES = ["completeness", "staleness"]


@dataclass
class Feature:
    user_defined_rules: dict
    credentials: dict

    def __post_init__(self):
        self.user_defined_rules = self.user_defined_rules
        self.credentials = self.credentials
        for feature_name in self.user_defined_rules.keys():
            assert (
                feature_name in SUPPORTED_FEATURES
            ), f"Feature {feature_name} is not supported. Supported features are {SUPPORTED_FEATURES}"

    def __repr__(self):
        return f"Feature(user_defined_rules={pformat(self.user_defined_rules)})"

    def __reduce_to_rule_per_table(self) -> Iterable[Tuple[Tuple[str, str, str], dict]]:
        rule_per_table = defaultdict(list)

        for (
            feature_name,
            table_specific_configurations,
        ) in self.user_defined_rules.items():
            for rule in table_specific_configurations:
                db_name = rule["db_name"]
                schema_name = rule["schema_name"]
                table_name = rule["table_name"]
                configurations = rule["configurations"]
                rule_per_table[(db_name, schema_name, table_name)].append(
                    {"feature_name": feature_name, "configurations": configurations}
                )

        for table, rules in rule_per_table.items():
            yield table, rules

    def parse(self) -> Tuple[str, Selectable, Expectation]:
        for (
            db_name,
            schema_name,
            table_name,
        ), rules in self.__reduce_to_rule_per_table():
            logger.info(f"Processing rules for {db_name}.{schema_name}.{table_name}")
            for rule in rules:
                logger.debug(f"Processing rule: {pformat(rule)}")
                getattr(self, f"_{rule['feature_name']}")(
                    db_name, schema_name, table_name, rule["configurations"]
                )

    def __prepare_db_url(self, db_name) -> str:
        return self.credentials[db_name]

    def _completeness(
        self, db_name, schema_name, table_name, configurations
    ) -> Tuple[str, Selectable, Expectation]:
        # Build the query
        time_series_index_column = configurations["time_series_index_column"]
        # TODO: Communicate with Connector here to get table objects
        metadata = MetaData()
        subject_table = Table(
            table_name,
            metadata,
            Column(time_series_index_column, Date),
            schema=schema_name,
        )
        start_time = parse_date(configurations["delta_start_time"])
        end_time = parse_date(configurations["delta_end_time"])
        query = select(subject_table).where(
            and_(
                subject_table.c[time_series_index_column] > start_time,
                subject_table.c[time_series_index_column] < end_time,
            )
        )
        for attribute_column, attribute_value in configurations[
            "attribute_map"
        ].items():
            subject_table.append_column(Column(attribute_column, String))
            query = query.where(subject_table.c[attribute_column] == attribute_value)
        logger.debug(f"Built Completeness Query: {query}")

        # Create expectation
        # expectation = Expectation(
        #     feature_name="completeness", requirements=configurations["requirements"]
        # )

        # TODO: Update return values
        # No longer returns db urls
        return (self.__prepare_db_url(db_name), query, expectation)

    def _staleness(
        self, db_name, schema_name, table_name, configurations
    ) -> Tuple[str, Selectable, Expectation]:
        pass
