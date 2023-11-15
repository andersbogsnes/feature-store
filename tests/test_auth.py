import pathlib

import pytest
import yaml

from feature_store import Client
from feature_store.auth.file_auth import FileAuth
from feature_store.feature_storage import (
    ParquetFeatureStorage,
    SQLAlchemyFeatureStorage,
)


@pytest.fixture
def config_file(tmp_path: pathlib.Path) -> pathlib.Path:
    conf_file = tmp_path.joinpath("featurestore.yaml")
    key_value_dict = {"sources": {"test_key": "another_value"}}
    with conf_file.open("w") as f:
        yaml.dump(key_value_dict, f)
    return conf_file


@pytest.fixture
def file_auth(config_file: pathlib.Path) -> FileAuth:
    return FileAuth(config_file=config_file)


def test_can_lookup_auth_key_in_config_file(file_auth: FileAuth):
    assert file_auth.get_sources_key("test_key") == "another_value"


def test_can_fetch_whole_file(file_auth: FileAuth):
    assert file_auth._file_config == {"sources": {"test_key": "another_value"}}


def test_non_existent_key_returns_none(file_auth: FileAuth):
    assert file_auth.get_sources_key("missing_key") == {}


def test_non_existent_file_returns_none():
    auth = FileAuth()
    assert auth.get_sources_key("any_key") == {}


@pytest.mark.parametrize(
    "location,store",
    [
        ("local::test.parquet", ParquetFeatureStorage),
        ("local_sqlite::main.test", SQLAlchemyFeatureStorage),
    ],
)
def test_auth_gets_correct_correct_storage_based_on_location(
    client: Client, location: str, store
):
    result = client.auth.get_store(location)
    assert isinstance(result, store)
