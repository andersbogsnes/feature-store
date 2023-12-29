import pandas as pd

from feature_store import Client
from feature_store.feature import FeatureGroup


def test_feature_group_can_be_downloaded_from_parquet(
    client: Client,
    customer_table_df: pd.DataFrame,
    customer_feature_group_parquet: FeatureGroup,
):
    ds = client.get_features(["customer.age", "customer.height"])
    result = ds.to_pandas()
    pd.testing.assert_frame_equal(customer_table_df, result, check_like=True)
