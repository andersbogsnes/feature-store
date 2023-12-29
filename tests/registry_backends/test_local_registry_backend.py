import pathlib

import pytest

from feature_store.feature import FeatureGroup
from feature_store.registry_backends.local import LocalRegistryBackend


@pytest.fixture
def tmp_database_path(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path.joinpath("features.db")


@pytest.fixture()
def backend(tmp_database_path: pathlib.Path) -> LocalRegistryBackend:
    db_url = f"sqlite:///{tmp_database_path}"
    return LocalRegistryBackend(db_url)


def test_can_add_feature_metadata(backend: LocalRegistryBackend):
    new_feature = FeatureGroup(
        name="age",
        id_column="customer_id",
        location="local::age.parquet",
        description="Age of customer",
    )
    backend.add_feature_group_metadata(new_feature)
    result = backend.get_feature_group_metadata(new_feature.name)
    assert result == new_feature


def test_can_add_multiple_features(backend: LocalRegistryBackend):
    features = [
        FeatureGroup(
            name="age",
            id_column="customer_id",
            location="local::age.parquet",
            description="Age of customer",
        ),
        FeatureGroup(
            name="height",
            id_column="customer_id",
            location="local::height.parquet",
            description="Height of customer",
        ),
    ]

    for feature in features:
        backend.add_feature_group_metadata(feature)
        result = backend.get_feature_group_metadata(feature.name)

        assert result == feature
