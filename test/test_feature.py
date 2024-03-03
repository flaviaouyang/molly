import json
from pytest import fixture

from molly.feature import Feature


@fixture
def user_config():
    file = "/Users/flaviaouyang/Projects/molly/doc/rule_template.json"
    with open(file, "r") as f:
        return json.load(f)


def test_feature(user_config):
    credentials = user_config["credentials"]
    user_defined_rules = user_config["user_defined_rules"]

    feature = Feature(user_defined_rules, credentials)
    feature.parse()
