import copy

import pandas as pd
import pytest

from feature_store.exceptions import MismatchedFeatureException, MissingDataException
from feature_store.feature import Dataset, Feature, FeatureGroup


@pytest.fixture()
def dataset(customer_feature_group_parquet: FeatureGroup) -> Dataset:
    return Dataset(features=customer_feature_group_parquet.features)


def test_dataset_has_correct_columns(dataset: Dataset):
    expected_columns = {"customer_id", "age", "date_time", "height"}
    assert set(dataset.data.column_names) == expected_columns


def test_dataset_errors_if_any_feature_doesnt_have_data():
    ds = Dataset(
        features=[
            Feature(name="feature", id_column="id", datetime_column="datetime_column")
        ]
    )
    with pytest.raises(MissingDataException):
        ds.to_pandas()


def test_can_add_feature_to_dataset(
    dataset: Dataset, customer_feature_group_parquet: FeatureGroup
):
    price_feature = Feature(
        name="price",
        id_column="customer_id",
        datetime_column="date_time",
    )

    expected = [*customer_feature_group_parquet.features, price_feature]
    result = dataset + price_feature
    assert result.features == expected


def test_dataset_can_convert_to_dataframe(
    age_df: pd.DataFrame, height_df: pd.DataFrame, dataset: Dataset
):
    result = dataset.to_pandas()
    expected = age_df.merge(height_df, on=["customer_id", "date_time"])
    pd.testing.assert_frame_equal(result, expected, check_like=True)


def test_adding_feature_with_mismatched_id_columns_will_error(
    customer_feature_group_parquet: FeatureGroup, dataset: Dataset
):
    new_feature = copy.copy(customer_feature_group_parquet.features[0])
    new_feature.id_column = "someothercolumn"
    with pytest.raises(MismatchedFeatureException):
        dataset + new_feature
