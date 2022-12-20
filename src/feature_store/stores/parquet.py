from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from feature_store.auth.base import Auth

if TYPE_CHECKING:
    from feature_store.feature import Feature


class ParquetFeatureStore:
    def download_data(self, feature: Feature, auth: Auth) -> pa.Table:
        return pq.read_table(feature.uri)
