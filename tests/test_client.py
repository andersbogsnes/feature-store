import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from feature_store import Client
from feature_store.exceptions import FeatureNotFoundException
from feature_store.feature import Feature


def test_can_list_existing_features(client: Client):
    expected = []
    result = client.get_available_features()
    assert expected == result


def test_can_add_new_feature_to_client(client: Client, age_parquet_feature: Feature):
    expected = [age_parquet_feature.name]
    result = client.get_available_features()
    assert expected == [r.name for r in result]


def test_can_get_metadata_from_feature(client: Client, age_parquet_feature: Feature):
    data = client.get_feature("age")
    assert data.location == age_parquet_feature.location


def test_can_get_auth_from_metadata(client: Client, age_parquet_feature: Feature):
    data = client.get_feature("age")
    assert data.auth_key == age_parquet_feature.auth_key


def test_can_load_arrow_data_from_feature(
    client: Client, age_parquet_feature: Feature, age_df: pd.DataFrame
):
    result = client.get_feature("age").to_pandas()
    expected = age_df
    assert_frame_equal(result, expected)


def test_can_get_dataset(
    client: Client, age_parquet_feature: Feature, height_parquet_feature: Feature
):
    result = client.get_dataset(
        feature_names=[age_parquet_feature.name, height_parquet_feature.name]
    ).to_pandas()
    expected = age_parquet_feature.to_pandas().merge(
        height_parquet_feature.to_pandas(),
        on=[age_parquet_feature.id_column, age_parquet_feature.datetime_column],
    )
    pd.testing.assert_frame_equal(result, expected)


def test_getting_a_feature_that_doesnt_exist_raises_not_found_exception(client: Client):
    with pytest.raises(FeatureNotFoundException):
        client.get_feature("idontexist")
