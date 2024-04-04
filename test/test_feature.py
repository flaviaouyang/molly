import json

import pytest
from pytest import fixture

from molly.coordinator import Coordinator


@fixture
def user_config():
    file = "./config/test_rules.json"
    with open(file, "r") as f:
        return json.load(f)


@pytest.mark.skip(reason="Local Test Only.")
def test_feature(user_config):
    credentials = user_config["credentials"]
    user_defined_rules = user_config["user_defined_rules"]

    feature = Coordinator(user_defined_rules, credentials)
    for result, description in feature.execute():
        print(description)
        # print(result)
        continue
