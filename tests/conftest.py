import datetime
import pathlib
import random

import pandas as pd
import pytest
import yaml

from feature_store import Client
from feature_store.auth.file_auth import FileAuth
from feature_store.feature import FeatureGroup
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
def customer_table_df(age_df: pd.DataFrame, height_df: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(age_df, height_df, on=["customer_id", "date_time"])


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
    auth_dict = {
        "sources": {
            "local_sqlite": {
                "type": "sqlalchemy",
                "db_url": f"sqlite:///{tmp_path}/features.db",
            },
            "local": {"type": "parquet", "uri": f"file:///{tmp_path}"},
        }
    }
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
def customer_feature_group_parquet(
    client: Client, customer_table_df: pd.DataFrame
) -> FeatureGroup:
    customer_feature_group = client.register_feature_group(
        "customer",
        id_column="customer_id",
        location="local::customer.parquet",
        description="Customer features",
        features=["age", "height"],
    )
    return client.upload_feature_data(customer_feature_group.name, customer_table_df)


@pytest.fixture()
def customer_feature_group_sql(
    client: Client, customer_table_df: pd.DataFrame
) -> FeatureGroup:
    customer_feature_group = client.register_feature_group(
        "customer",
        id_column="customer_id",
        location="local_sqlite::main.customer",
        description="Customer features",
        features=["age", "height"],
    )
    client.upload_feature_data(customer_feature_group.name, customer_table_df)
    return customer_feature_group
