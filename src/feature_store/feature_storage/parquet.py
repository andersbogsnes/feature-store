from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from feature_store.auth import AuthType

if TYPE_CHECKING:
    from feature_store.feature import Feature


class ParquetFeatureStorage:
    def download_data(self, feature: Feature, auth: AuthType) -> pa.Table:
        auth_dict = auth.get(feature.auth_key)

        return pq.read_table(feature.location, **auth_dict)

    def upload_data(
        self, df: pd.DataFrame, feature: Feature, auth: AuthType
    ) -> pa.Table:
        data = pa.Table.from_pandas(df)
        pq.write_table(data, feature.location)
        return data
