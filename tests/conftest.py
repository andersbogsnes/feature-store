import datetime
import pathlib

import numpy as np
import pandas as pd
import pytest

from feature_store import Client
from feature_store.backends.local import LocalStorageBackend
from feature_store.feature import Feature


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
def client(tmp_dir) -> Client:
    backend = LocalStorageBackend(
        database_url=f"sqlite:///{tmp_dir.joinpath('features.db')}"
    )
    return Client(registry=backend)


@pytest.fixture(scope="session")
def age_feature(client: Client, age_df: pd.DataFrame, tmp_dir: pathlib.Path) -> Feature:
    uri = tmp_dir.joinpath("age.parquet").as_uri()
    age_feature = client.register_feature("age", uri)
    client.upload_feature_data(age_feature.name, age_df)
    return age_feature
