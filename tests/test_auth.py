import pathlib

import pytest
import yaml

from feature_store.auth.base import Auth


def test_looking_up_key_without_indicating_secret_location_returns_none():
    auth = Auth()
    assert auth.get("test_key") is None


@pytest.fixture
def config_file(tmp_path: pathlib.Path) -> pathlib.Path:
    conf_file = tmp_path.joinpath("featurestore.yaml")
    key_value_dict = {"test_key": "another_value"}
    with conf_file.open("w") as f:
        yaml.dump(key_value_dict, f)
    return conf_file


def test_can_lookup_auth_key_in_config_file(config_file: pathlib.Path):
    auth = Auth(config_file=config_file)
    assert auth.get("test_key") == "another_value"
