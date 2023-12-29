import pandas as pd

from feature_store import Client
from feature_store.feature import Feature, FeatureGroup


def test_creating_new_feature_group_instantiates_correctly():
    fg = FeatureGroup(
        name="demographics",
        features=[
            Feature("age", id_column="customer_id", datetime_column="date_time"),
            Feature("height", id_column="customer_id", datetime_column="date_time"),
        ],
        location="local::demographics.parquet",
        id_column="customer_id",
        datetime_column="date_time",
        description="Customer Demographics dataset",
    )

    assert fg


def test_feature_group_can_be_downloaded_from_parquet(
    client: Client,
    customer_table_df: pd.DataFrame,
    customer_feature_group_parquet: FeatureGroup,
):
    ds = client.get_features(["customer.age", "customer.height"])
    result = ds.to_pandas()
    pd.testing.assert_frame_equal(customer_table_df, result, check_like=True)


def test_feature_group_can_be_downloaded_from_sql(
    client: Client,
    customer_feature_group_sql: FeatureGroup,
    customer_table_df: pd.DataFrame,
):
    ds = client.get_features(["customer.age", "customer.height"])
    result = ds.to_pandas()
    pd.testing.assert_frame_equal(customer_table_df, result, check_like=True)
