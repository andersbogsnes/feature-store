from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from feature_store.feature import Feature


def _extract_table_parts(table_name: str) -> tuple[str, str]:
    schema, table = table_name.split(".")
    return schema, table


class SQLAlchemyFeatureStorage:
    def __init__(self, db_url: str):
        self.meta = sa.MetaData()
        self.db_url = db_url

    @property
    def engine(self) -> Engine:
        return sa.create_engine(self.db_url)

    def upload_data(self, df: pd.DataFrame, feature: Feature) -> pa.Table:
        schema, table = _extract_table_parts(feature.location)
        df.to_sql(table, self.engine, schema=schema, if_exists="append", index=False)
        return pa.Table.from_pandas(df)

    def download_data(self, feature: Feature) -> pa.Table:
        table = self.get_table(feature.location, self.engine)
        column = getattr(table.c, feature.name)
        id_column = getattr(table.c, feature.id_column)
        date_column = getattr(table.c, feature.datetime_column)

        sql = sa.select(id_column, date_column, column)

        with self.engine.connect() as conn:
            results = conn.execute(sql).mappings().all()
            return pa.Table.from_pylist(results)

    def get_table(self, uri: str, engine: Engine) -> sa.Table:
        schema, table = _extract_table_parts(uri)
        self.meta.reflect(engine, only=[table], schema=schema)
        return self.meta.tables[uri]
