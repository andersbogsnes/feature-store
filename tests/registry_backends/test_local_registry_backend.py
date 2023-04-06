import pathlib

import pytest

from feature_store.registry_backends.local import LocalRegistryBackend
from feature_store.feature import Feature, FeatureKind


@pytest.fixture
def tmp_database_path(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path.joinpath("features.db")


@pytest.fixture()
def backend(tmp_database_path: pathlib.Path) -> LocalRegistryBackend:
    db_url = f"sqlite:///{tmp_database_path}"
    return LocalRegistryBackend(db_url)


def test_can_add_feature_metadata(backend: LocalRegistryBackend):
    new_feature = Feature(
        name="age",
        id_column="customer_id",
        kind=FeatureKind.parquet,
        location="age.parquet",
        auth_key=None,
    )
    backend.add_feature_metadata(new_feature)
    result = backend.get_feature_metadata(new_feature.name)
    assert result == new_feature


def test_can_add_multiple_features(backend: LocalRegistryBackend):
    features = [
        Feature(
            name="age",
            id_column="customer_id",
            kind=FeatureKind.parquet,
            location="age.parquet",
            auth_key=None,
        ),
        Feature(
            name="height",
            id_column="customer_id",
            kind=FeatureKind.parquet,
            location="height.parquet",
            auth_key=None,
        ),
    ]

    for feature in features:
        backend.add_feature_metadata(feature)
        result = backend.get_feature_metadata(feature.name)

        assert result == feature
