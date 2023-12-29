import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from feature_store import Client
from feature_store.exceptions import FeatureNotFoundException
from feature_store.feature import FeatureGroup


def test_can_list_existing_features(client: Client):
    expected = []
    result = client.get_available_features()
    assert expected == result


def test_can_add_new_feature_to_client(
    client: Client, customer_feature_group_parquet: FeatureGroup
):
    expected = [
        f"{customer_feature_group_parquet.name}.{feature.name}"
        for feature in customer_feature_group_parquet.features
    ]
    results = client.get_available_features()
    assert expected == results


def test_can_load_arrow_data_from_feature(
    client: Client, customer_feature_group_parquet: FeatureGroup, age_df: pd.DataFrame
):
    result = client.get_features(["customer.age"]).to_pandas()
    expected = age_df
    assert_frame_equal(result, expected, check_like=True)


@pytest.mark.usefixtures("customer_feature_group_parquet")
def test_can_get_multiple_features(
    client: Client,
    customer_table_df: pd.DataFrame,
):
    result = client.get_features(
        feature_names=["customer.age", "customer.height"]
    ).to_pandas()

    pd.testing.assert_frame_equal(result, customer_table_df, check_like=True)


def test_getting_a_feature_that_doesnt_exist_raises_not_found_exception(client: Client):
    with pytest.raises(FeatureNotFoundException):
        client.get_feature("idontexist")


def test_can_get_feature(
    client: Client, age_df: pd.DataFrame, customer_feature_group_parquet: FeatureGroup
):
    result = client.get_feature("customer.age")
    pd.testing.assert_frame_equal(result.to_pandas(), age_df, check_like=True)


def test_feature_group_can_be_downloaded_from_sql(
    client: Client,
    customer_feature_group_sql: FeatureGroup,
    customer_table_df: pd.DataFrame,
):
    ds = client.get_features(["customer.age", "customer.height"])
    result = ds.to_pandas()
    pd.testing.assert_frame_equal(customer_table_df, result, check_like=True)
