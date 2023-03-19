import pathlib

import pytest
import yaml

from feature_store.auth.file_auth import FileAuth


@pytest.fixture
def config_file(tmp_path: pathlib.Path) -> pathlib.Path:
    conf_file = tmp_path.joinpath("featurestore.yaml")
    key_value_dict = {"test_key": "another_value"}
    with conf_file.open("w") as f:
        yaml.dump(key_value_dict, f)
    return conf_file


@pytest.fixture
def file_auth(config_file: pathlib.Path) -> FileAuth:
    return FileAuth(config_file=config_file)


def test_can_lookup_auth_key_in_config_file(file_auth: FileAuth):
    assert file_auth.get("test_key") == "another_value"


def test_can_fetch_whole_file(file_auth: FileAuth):
    assert file_auth._file_config == {"test_key": "another_value"}


def test_non_existent_key_returns_none(file_auth: FileAuth):
    assert file_auth.get("missing_key") == {}


def test_non_existent_file_returns_none():
    auth = FileAuth()
    assert auth.get("any_key") == {}
