from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from feature_store.auth import AuthType
    from feature_store.feature import Feature


def _extract_table_parts(location: str) -> tuple[str, str]:
    key, _, table_name = location.partition("::")
    schema, table = table_name.split(".")
    return schema, table


class SQLAlchemyFeatureStorage:
    type = "sqlalchemy"

    def __init__(self, auth: AuthType):
        self.meta = sa.MetaData()
        self.auth = auth

    def _get_engine(self, location: str) -> Engine:
        key, _, _ = location.partition("::")
        config = self.auth.get_sources_key(key)
        return sa.create_engine(config["db_url"])

    def upload_data(self, df: pd.DataFrame, feature: Feature) -> pa.Table:
        engine = self._get_engine(feature.location)
        schema, table = _extract_table_parts(feature.location)
        df.to_sql(table, engine, schema=schema, if_exists="append", index=False)
        return pa.Table.from_pandas(df)

    def download_data(self, feature: Feature) -> pa.Table:
        engine = self._get_engine(feature.location)
        table = self.get_table(feature.location, engine)
        column = getattr(table.c, feature.name)
        id_column = getattr(table.c, feature.id_column)
        date_column = getattr(table.c, feature.datetime_column)

        sql = sa.select(id_column, date_column, column)

        with engine.connect() as conn:
            results = conn.execute(sql).mappings().all()
            return pa.Table.from_pylist(results)

    def get_table(self, location: str, engine: Engine) -> sa.Table:
        schema, table = _extract_table_parts(location)
        self.meta.reflect(engine, only=[table], schema=schema)
        return self.meta.tables[f"{schema}.{table}"]
