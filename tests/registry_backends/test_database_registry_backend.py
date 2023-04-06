import pytest

from feature_store.feature import Feature, FeatureKind
from feature_store.registry_backends.db import (DatabaseRegistryBackend,
                                                mapped_registry)


@pytest.fixture()
def backend() -> DatabaseRegistryBackend:
    database_uri = "sqlite:///:memory:"

    db_backend = DatabaseRegistryBackend(database_uri)
    mapped_registry.metadata.create_all(db_backend._engine)
    return db_backend


def test_can_get_all_features_when_backend_is_empty(backend: DatabaseRegistryBackend):
    assert backend.get_available_feature_metadata() == []


def test_can_add_and_retrieve_features(backend: DatabaseRegistryBackend):
    new_feature = Feature(
        name="test_feature",
        id_column="id",
        kind=FeatureKind.parquet,
        location="test.parquet",
        auth_key=None,
    )
    backend.add_feature_metadata(new_feature)
    result = backend.get_feature_metadata(new_feature.name)

    assert result == new_feature


def test_adding_new_feature_shows_up_in_all_features(backend: DatabaseRegistryBackend):
    new_feature = Feature(
        name="test_feature2",
        id_column="id_col",
        kind=FeatureKind.parquet,
        location="test.parquet",
        auth_key=None,
    )
    backend.add_feature_metadata(new_feature)
    result = backend.get_available_feature_metadata()
    assert result == [new_feature]
