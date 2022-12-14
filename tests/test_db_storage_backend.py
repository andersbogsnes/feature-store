import pytest

from feature_store.backends.db import DatabaseStorageBackend, mapped_registry
from feature_store.feature import Feature


@pytest.fixture()
def backend() -> DatabaseStorageBackend:
    database_uri = "sqlite:///:memory:"

    db_backend = DatabaseStorageBackend(database_uri)
    mapped_registry.metadata.create_all(db_backend._engine)
    return db_backend


def test_can_get_all_features_when_backend_is_empty(backend: DatabaseStorageBackend):
    assert backend.get_all_features() == []


def test_can_add_and_retrieve_features(backend: DatabaseStorageBackend):
    new_feature = Feature(
        name="test_feature", uri="file:///test.parquet", auth_key=None
    )
    backend.add_feature_metadata(new_feature)
    result = backend.get_feature_metadata(new_feature.name)

    assert result == new_feature


def test_adding_new_feature_shows_up_in_all_features(backend: DatabaseStorageBackend):
    new_feature = Feature(
        name="test_feature2", uri="file://test.parquet", auth_key=None
    )
    backend.add_feature_metadata(new_feature)
    result = backend.get_all_features()
    assert result == [new_feature]
