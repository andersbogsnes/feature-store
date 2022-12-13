import datetime
import pathlib

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from feature_store import Client
from feature_store.client import Feature


@pytest.fixture(scope="session")
def client() -> Client:
    return Client()


@pytest.fixture(scope="session")
def age_df() -> pd.DataFrame:
    n_customers = 100
    customer_ages = np.random.default_rng().integers(
        low=0, high=100, size=(n_customers, 1), dtype=np.int8
    )
    batch_1 = datetime.date(year=2022, month=1, day=1)
    batch_2 = datetime.date(year=2022, month=2, day=1)

    return pd.DataFrame(
        columns=["age"], index=([batch_1] * 50) + ([batch_2] * 50), data=customer_ages
    )


@pytest.fixture(scope="session")
def tmp_dir(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    return tmp_path_factory.mktemp("data")


@pytest.fixture(scope="session")
def age_feature(client: Client, age_df: pd.DataFrame, tmp_dir: pathlib.Path) -> Feature:
    uri = tmp_dir.joinpath("age.parquet").as_uri()
    age_feature = client.register_feature("age", uri)
    return age_feature


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


def test_can_upload_data_to_feature(
    client: Client, age_df: pd.DataFrame, age_feature: Feature
):
    age_feature.upload_batch(age_df)
    file_location = pathlib.Path(age_feature.uri)
    assert file_location.exists()


def test_can_load_arrow_data_from_feature(
    client: Client, age_feature: Feature, age_df: pd.DataFrame
):
    age_feature.upload_batch(age_df)
    result = age_feature.download_batch().to_pandas()
    expected = age_df
    assert_frame_equal(result, expected)
