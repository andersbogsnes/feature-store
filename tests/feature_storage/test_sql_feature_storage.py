import pandas as pd

from feature_store import Client
from feature_store.feature import FeatureGroup


def test_can_get_feature(
    client: Client, age_df: pd.DataFrame, customer_feature_group_parquet: FeatureGroup
):
    result = client.get_feature("customer.age")
    pd.testing.assert_frame_equal(result.to_pandas(), age_df, check_like=True)
