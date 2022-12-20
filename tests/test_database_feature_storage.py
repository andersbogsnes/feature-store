import pandas as pd
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Connection

from feature_store import Client
from feature_store.feature import Feature


@pytest.fixture
def db(age_df: pd.DataFrame) -> Connection:
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    with engine.connect() as conn:
        age_df.to_sql("customer", conn)
        yield conn


@pytest.mark.usefixtures("db")
def test_can_get_feature(client: Client, age_df: pd.DataFrame):
    feature = Feature(name="age", uri="sql://main.customer", auth_key=None)
    client.upload_feature_data(feature.name, age_df)
    result = client.get_feature(feature.name)
    pd.testing.assert_frame_equal(result.to_pandas(), age_df)
