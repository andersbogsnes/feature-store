import pandas as pd
from pandas.testing import assert_frame_equal

from feature_store import Client
from feature_store.feature import Feature


def test_can_list_existing_features(client: Client):
    expected = []
    result = client.get_features()
    assert expected == result


def test_can_add_new_feature_to_client(client: Client, age_feature: Feature):
    expected = [age_feature]
    result = client.get_features()
    assert expected == result


def test_can_get_metadata_from_feature(client: Client, age_feature: Feature):
    data = client.get_feature("age")
    assert data.uri == age_feature.uri


def test_can_get_auth_from_metadata(client: Client, age_feature: Feature):
    data = client.get_feature("age")
    assert data.auth_key == age_feature.auth_key


def test_can_load_arrow_data_from_feature(
    client: Client, age_feature: Feature, age_df: pd.DataFrame
):
    result = client.get_feature("age").to_pandas()
    expected = age_df
    assert_frame_equal(result, expected)
