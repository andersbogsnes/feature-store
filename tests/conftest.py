import datetime
import pathlib
import random

import pandas as pd
import pytest

from feature_store import Client
from feature_store.backends.local import LocalStorageBackend
from feature_store.feature import Feature, FeatureKind


@pytest.fixture(scope="session")
def age_df() -> pd.DataFrame:
    n_customers = 100

    batch_1_date = [datetime.date(year=2022, month=1, day=1)] * 50
    batch_2_date = [datetime.date(year=2022, month=2, day=1)] * 50

    return pd.DataFrame(
        {
            "customer_id": list(range(50)) * 2,
            "age": random.choices(list(range(50)), k=n_customers),
            "date_time": [*batch_1_date, *batch_2_date],
        }
    )


@pytest.fixture(scope="session")
def height_df() -> pd.DataFrame:
    n_customers = 100

    batch_1_date = [datetime.date(year=2022, month=1, day=1)] * 50
    batch_2_date = [datetime.date(year=2022, month=2, day=1)] * 50

    return pd.DataFrame(
        {
            "customer_id": list(range(50)) * 2,
            "height": random.choices(list(range(100, 200)), k=n_customers),
            "date_time": [*batch_1_date, *batch_2_date],
        }
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
    location = tmp_dir.joinpath("age.parquet")
    age_feature = client.register_feature(
        "age", id_column="customer_id", kind=FeatureKind.parquet, location=str(location)
    )
    client.upload_feature_data(age_feature.name, age_df)
    return client.get_feature("age")


@pytest.fixture(scope="session")
def height_feature(
    client: Client, height_df: pd.DataFrame, tmp_dir: pathlib.Path
) -> Feature:
    location = tmp_dir.joinpath("height.parquet")
    height_feature = client.register_feature(
        "height",
        id_column="customer_id",
        kind=FeatureKind.parquet,
        location=str(location),
    )
    client.upload_feature_data(height_feature.name, height_df)
    return client.get_feature("height")
