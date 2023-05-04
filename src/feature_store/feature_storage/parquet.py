from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from feature_store.feature import Feature


class ParquetFeatureStorage:
    def __init__(self, **auth_kwargs):
        self.auth_kwargs = auth_kwargs

    def download_data(self, feature: Feature) -> pa.Table:
        return pq.read_table(feature.location, **self.auth_kwargs)

    def upload_data(self, df: pd.DataFrame, feature: Feature) -> pa.Table:
        data = pa.Table.from_pandas(df)
        pq.write_table(data, feature.location, **self.auth_kwargs)
        return data
