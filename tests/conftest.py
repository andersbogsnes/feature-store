import datetime
import pathlib
import random

import pandas as pd
import pytest
import yaml

from feature_store import Client
from feature_store.auth.file_auth import FileAuth
from feature_store.feature import Feature, FeatureKind
from feature_store.registry_backends.local import LocalRegistryBackend


@pytest.fixture()
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


@pytest.fixture()
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


@pytest.fixture
def config(tmp_path: pathlib.Path) -> pathlib.Path:
    auth_dict = {"local_sqlite": {"db_url": f"sqlite:///{tmp_path}/features.db"}}
    config_file = tmp_path.joinpath("config.yaml")
    config_file.write_text(yaml.safe_dump(auth_dict))
    return config_file


@pytest.fixture()
def client(tmp_path: pathlib.Path, config: pathlib.Path) -> Client:
    backend = LocalRegistryBackend(
        database_url=f"sqlite:///{tmp_path.joinpath('store.db')}"
    )
    auth = FileAuth(config_file=config)
    return Client(registry=backend, auth=auth)


@pytest.fixture()
def age_parquet_feature(
    client: Client, age_df: pd.DataFrame, tmp_path: pathlib.Path
) -> Feature:
    location = tmp_path.joinpath("age.parquet")
    age_feature = client.register_feature(
        "age", id_column="customer_id", kind=FeatureKind.parquet, location=str(location)
    )
    client.upload_feature_data(age_feature.name, age_df)
    return client.get_feature("age")


@pytest.fixture()
def height_parquet_feature(
    client: Client, height_df: pd.DataFrame, tmp_path: pathlib.Path
) -> Feature:
    location = tmp_path.joinpath("height.parquet")
    height_feature = client.register_feature(
        "height",
        id_column="customer_id",
        kind=FeatureKind.parquet,
        location=str(location),
    )
    client.upload_feature_data(height_feature.name, height_df)
    return client.get_feature("height")


@pytest.fixture()
def age_sql_feature(client: Client, age_df: pd.DataFrame) -> Feature:
    age_feature = client.register_feature(
        "age",
        id_column="customer_id",
        kind=FeatureKind.sql,
        location="main.age",
        auth_key="local_sqlite",
    )
    client.upload_feature_data(age_feature.name, age_df)
    return client.get_feature("age")


@pytest.fixture()
def height_sql_feature(client: Client, height_df: pd.DataFrame) -> Feature:
    height_feature = client.register_feature(
        "height",
        id_column="customer_id",
        kind=FeatureKind.sql,
        location="main.height",
        auth_key="local_sqlite",
    )
    client.upload_feature_data(height_feature.name, height_df)
    return client.get_feature("height")
