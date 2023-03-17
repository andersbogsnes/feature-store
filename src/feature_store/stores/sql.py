from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import sqlalchemy as sa

from feature_store.auth import AuthType

if TYPE_CHECKING:
    from feature_store.feature import Feature


def _extract_table_parts(table_name: str) -> tuple[str, str]:
    schema, table = table_name.split(".")
    return schema, table


class SQLAlchemyFeatureStore:
    def __init__(self):
        self.meta = sa.MetaData()

    def download_data(self, feature: Feature, auth: AuthType) -> pa.Table:
        table = self.get_table(feature.location)
        column = getattr(table, feature.name)

        sql = sa.select([column])
        db_url = auth.get(feature.auth_key)
        engine = self.get_engine(db_url)

        with engine.connect() as conn:
            results = conn.execute(sql).all()
            return pa.Table.from_pylist(results)

    def get_table(self, uri: str) -> sa.Table:
        schema, table = _extract_table_parts(uri)
        self.meta.reflect(self.engine, only=[table], schema=schema)
        return self.meta.tables[table]

    def get_engine(self, engine: str):
        return sa.create_engine(engine, future=True)
