import pandas as pd

from feature_store import Client
from feature_store.feature import Feature


def test_can_get_feature(
    client: Client, age_df: pd.DataFrame, age_sql_feature: Feature
):
    result = client.get_feature(age_sql_feature.name)
    pd.testing.assert_frame_equal(result.to_pandas(), age_df, check_like=True)