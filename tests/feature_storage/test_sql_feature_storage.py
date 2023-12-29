import pathlib

import pandas as pd
import pytest

from feature_store.feature_storage import SQLAlchemyFeatureStorage


@pytest.fixture
def sql_feature_storage(tmp_path: pathlib.Path) -> SQLAlchemyFeatureStorage:
    return SQLAlchemyFeatureStorage(db_url=f"sqlite:///{tmp_path.joinpath('test.db')}")


def test_sql_feature_storage_can_upload_a_feature_group(
    sql_feature_storage: SQLAlchemyFeatureStorage, customer_table_df: pd.DataFrame
):
    sql = sql_feature_storage.upload_data()
