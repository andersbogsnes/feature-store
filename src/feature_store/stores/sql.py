from __future__ import annotations

from typing import TYPE_CHECKING

import dask.dataframe as dd
import pyarrow as pa
import sqlalchemy as sa

from feature_store.auth import AuthType

if TYPE_CHECKING:
    from feature_store.feature import Feature


def _extract_table_parts(table_name: str) -> tuple[str, str]:
    schema, table = table_name.split(".")
    return schema, table


@dataclass
class SQLAlchemyFeatureStore:
    auth: AuthType

    def download_data(self, feature: Feature) -> pa.Table:
        table = self.get_table(feature.location)
        column = getattr(table, feature.name)

        sql = sa.select([column])
        db_url = auth.get(feature.auth_key)
        engine = self.get_engine(db_url)

        with engine.connect() as conn:
            results = conn.execute(sql).all()
            return pa.Table.from_pylist(results)

    def get_sql(self, feature: Feature) -> sa.Select:
        schema, table_name = _extract_table_parts(feature.location)
        table = sa.sql.table(table_name, schema=schema)
