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


def test_feature_group_can_be_downloaded(
    client: Client, customer_table_df: pd.DataFrame
):
    ds = client.get_dataset(["customer.age", "customer.height"])
    result = ds.to_pandas()
    pd.testing.assert_frame_equal(customer_table_df, result)
