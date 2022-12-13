import pathlib

import pytest

from feature_store.backends.local import LocalStorageBackend
from feature_store.feature import Feature


@pytest.fixture
def tmp_database_path(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path.joinpath("features.db")


@pytest.fixture()
def backend(tmp_database_path: pathlib.Path) -> LocalStorageBackend:
    db_url = f"sqlite:///{tmp_database_path}"
    return LocalStorageBackend(db_url)


def test_can_add_feature_metadata(backend: LocalStorageBackend):
    new_feature = Feature(name="age", uri="file:///age.parquet")
    backend.add_feature_metadata(new_feature)
    result = backend.get_feature_metadata(new_feature.name)
    assert result == new_feature


def test_can_add_multiple_features(backend: LocalStorageBackend):
    features = [
        Feature(name="age", uri="file:///age.parquet"),
        Feature(name="height", uri="file:///height.parquet"),
    ]

    for feature in features:
        backend.add_feature_metadata(feature)
        result = backend.get_feature_metadata(feature.name)

        assert result == feature
