import logging
from collections import defaultdict
from dataclasses import dataclass
from pprint import pformat
from typing import Iterable, Tuple

from sqlalchemy import Table

from molly.connector import SQLConnector
from molly.features.factory import feature_factory
from molly.features.feature import Feature

logger = logging.getLogger(__name__)

SUPPORTED_FEATURES = ["completeness", "staleness"]


@dataclass
class Coordinator(object):
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

    def __reduce_to_rule_per_table(self) -> defaultdict:
        rule_per_table = defaultdict(lambda: defaultdict(list))

        for (
            feature_name,
            table_specific_configurations,
        ) in self.user_defined_rules.items():
            for rule in table_specific_configurations:
                db_name = rule["db_name"]
                schema_name = rule["schema_name"]
                table_name = rule["table_name"]
                configurations = rule["configurations"]
                requirements = rule["requirements"]
                rule_per_table[db_name][(schema_name, table_name)].append(
                    {
                        "feature_name": feature_name,
                        "configurations": configurations,
                        "requirements": requirements,
                    }
                )

        return rule_per_table

    def __connect(self, db_name) -> SQLConnector:
        return SQLConnector(self.credentials[db_name])

    @staticmethod
    def __generate_feature(subject_table: Table, rules: dict) -> Iterable[Feature]:
        for rule in rules:
            logger.debug(f"Processing rule:\n{pformat(rule)}")
            feature = feature_factory(
                feature_name=rule["feature_name"],
                subject_table=subject_table,
                configurations=rule["configurations"],
                requirements=rule["requirements"],
            )
            yield feature

    def execute(self) -> Iterable[Tuple[bool, str]]:
        for db_name, table_rules in self.__reduce_to_rule_per_table().items():
            logger.info(f"Connecting to {db_name} database")
            connector = self.__connect(db_name)
            for (schema_name, table_name), rules in table_rules.items():
                subject_table = connector.construct_table(schema_name, table_name)
                for feature_item in self.__generate_feature(subject_table, rules):
                    query = feature_item.construct_query()
                    # TODO: output can also be a generator??? maybe not
                    output = connector.execute_query(query)
                    result = feature_item.validate(output)
                    description = feature_item.describe()
                    logger.info(description)
                    yield result, description
