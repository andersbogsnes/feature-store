import copy

import pandas as pd
import pytest

from feature_store.exceptions import MismatchedFeatureException
from feature_store.feature import Dataset, Feature, FeatureKind
from feature_store.feature_storage.parquet import ParquetFeatureStorage
from feature_store.feature_storage.sql import SQLAlchemyFeatureStorage


@pytest.fixture()
def dataset(age_parquet_feature, height_parquet_feature) -> Dataset:
    return age_parquet_feature + height_parquet_feature


@pytest.mark.parametrize(
    "feature_type,store",
    [
        (FeatureKind.parquet, ParquetFeatureStorage),
        (FeatureKind.sql, SQLAlchemyFeatureStorage),
    ],
)
def test_feature_kind_uses_correct_store(feature_type: FeatureKind, store):
    feature = Feature(
        name="test", id_column="id_col", kind=feature_type, location="test"
    )
    assert feature.store == store


def test_dataset_has_correct_columns(
    age_parquet_feature, height_parquet_feature, dataset: Dataset
):
    expected_columns = ["customer_id", "age", "date_time", "height"]
    assert dataset.data.column_names == expected_columns


def test_can_add_feature_to_dataset(
    dataset: Dataset, age_parquet_feature: Feature, height_parquet_feature: Feature
):
    price_feature = Feature(
        name="price",
        kind=FeatureKind.sql,
        location="test",
        id_column="id",
    )

    expected = [age_parquet_feature, height_parquet_feature, price_feature]
    result = dataset + price_feature
    assert result.features == expected


def test_dataset_can_convert_to_dataframe(
    age_df: pd.DataFrame, height_df: pd.DataFrame, dataset: Dataset
):
    result = dataset.to_pandas()
    expected = age_df.merge(height_df, on=["customer_id", "date_time"])
    pd.testing.assert_frame_equal(result, expected)


def test_adding_feature_with_mismatched_id_columns_will_error(
    age_parquet_feature: Feature, height_parquet_feature: Feature
):
    new_feature = copy.copy(height_parquet_feature)
    new_feature.id_column = "someothercolumn"
    with pytest.raises(MismatchedFeatureException):
        result = age_parquet_feature + new_feature
        assert result.data
