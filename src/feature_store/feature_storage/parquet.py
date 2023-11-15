from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from feature_store.auth import AuthType
    from feature_store.feature import Feature


class ParquetFeatureStorage:
    type = "parquet"

    def __init__(self, auth: AuthType):
        self.auth = auth

    def download_data(self, feature: Feature) -> pa.Table:
        uri = self._get_uri(feature)
        return pq.read_table(uri)

    def upload_data(self, df: pd.DataFrame, feature: Feature) -> pa.Table:
        data = pa.Table.from_pandas(df)
        uri = self._get_uri(feature)
        pq.write_table(data, uri)
        return data

    def _get_uri(self, feature: Feature) -> str:
        key, _, filename = feature.location.partition("::")
        config = self.auth.get_sources_key(key)
        return f"{config['uri']}/{filename}"
