import pandas as pd
import pytest

from feature_store.exceptions import FeatureDataException
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
    assert isinstance(feature.store, store)


def test_dataset_has_correct_columns(
    age_parquet_feature, height_parquet_feature, dataset: Dataset
):
    expected_columns = ["customer_id", "age", "date_time", "height"]
    assert dataset._data.column_names == expected_columns


def test_dataset_cannot_be_constructed_without_data():
    feature_a = Feature(
        name="feature_a",
        kind=FeatureKind.parquet,
        location="age.parquet",
        id_column="customer_id",
    )
    feature_b = Feature(
        name="feature_b",
        kind=FeatureKind.parquet,
        location="feature_b.parquet",
        id_column="customer_id",
    )

    with pytest.raises(FeatureDataException):
        feature_a + feature_b


def test_dataset_can_convert_to_dataframe(
    age_df: pd.DataFrame, height_df: pd.DataFrame, dataset: Dataset
):
    result = dataset.to_pandas()
    expected = age_df.merge(height_df, on=["customer_id", "date_time"])
    pd.testing.assert_frame_equal(result, expected)
