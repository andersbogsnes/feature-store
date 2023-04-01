from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from feature_store.auth import AuthType

if TYPE_CHECKING:
    from feature_store.feature import Feature


@dataclass
class ParquetFeatureStore:
    auth: AuthType

    def download_data(self, feature: Feature) -> dd.DataFrame:
        auth_dict = self.auth.get(feature.auth_key)

        return dd.read_parquet(feature.location, storage_options=auth_dict)

    def upload_data(self, df: pd.DataFrame, feature: Feature) -> dd.DataFrame:
        auth_dict = self.auth.get(feature.auth_key)
        data = dd.from_pandas(df)
        data.to_parquet(feature.location, storage_options=auth_dict)
        return data
