import pathlib

import pandas as pd
import pandas.testing
import pytest

from feature_store.feature import Feature, FeatureGroup
from feature_store.feature_storage import SQLAlchemyFeatureStorage


@pytest.fixture
def sql_feature_storage(tmp_path: pathlib.Path) -> SQLAlchemyFeatureStorage:
    return SQLAlchemyFeatureStorage(db_url=f"sqlite:///{tmp_path.joinpath('test.db')}")


def test_sql_feature_storage_can_upload_a_feature_group(
    sql_feature_storage: SQLAlchemyFeatureStorage, customer_table_df: pd.DataFrame
):
    feature_group = FeatureGroup(
        "customer",
        location="local_sqlite::main.customer",
        id_column="id",
        features=[
            Feature(name="age", id_column="id", datetime_column="date"),
            Feature(name="height", id_column="id", datetime_column="date"),
        ],
        description="customer dataset",
    )
    sql_feature_storage.upload_data(customer_table_df, feature_group)
    result = sql_feature_storage.download_data(feature_group).to_pandas()
    pandas.testing.assert_frame_equal(result, customer_table_df, check_like=False)
