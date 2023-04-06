from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from feature_store.auth import AuthType

if TYPE_CHECKING:
    from feature_store.feature import Feature


def _extract_table_parts(table_name: str) -> tuple[str, str]:
    schema, table = table_name.split(".")
    return schema, table


class SQLAlchemyFeatureStorage:
    def __init__(self):
        self.meta = sa.MetaData()

    def upload_data(
        self, df: pd.DataFrame, feature: Feature, auth: AuthType
    ) -> pa.Table:
        schema, table = _extract_table_parts(feature.location)
        db_url = auth.get(feature.auth_key)["db_url"]
        engine = sa.create_engine(db_url, future=True)
        df.to_sql(table, engine, schema=schema, if_exists="replace", index=False)
        return pa.Table.from_pandas(df)

    def download_data(self, feature: Feature, auth: AuthType) -> pa.Table:
        db_url = auth.get(feature.auth_key)["db_url"]
        engine = sa.create_engine(db_url, future=True)

        table = self.get_table(feature.location, engine)
        column = getattr(table.c, feature.name)
        id_column = getattr(table.c, feature.id_column)
        date_column = getattr(table.c, feature.datetime_column)

        sql = sa.select(id_column, date_column, column)

        with engine.connect() as conn:
            results = conn.execute(sql).mappings().all()
            return pa.Table.from_pylist(results)

    def get_table(self, uri: str, engine: Engine) -> sa.Table:
        schema, table = _extract_table_parts(uri)
        self.meta.reflect(engine, only=[table], schema=schema)
        return self.meta.tables[uri]
