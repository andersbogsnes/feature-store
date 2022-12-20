from dataclasses import dataclass
from typing import Optional, Type, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from feature_store.auth.base import Auth
from feature_store.stores.base import Store
from feature_store.stores.parquet import ParquetFeatureStore
from feature_store.stores.sql import SQLAlchemyFeatureStore

STORES: dict[str, Type[Store]] = {
    "file": ParquetFeatureStore,
    "sql": SQLAlchemyFeatureStore,
}


@dataclass
class Feature:
    name: str
    uri: str
    auth_key: Optional[str]
    _data: pa.Table = None

    def to_pandas(self) -> pd.DataFrame:
        return cast(pd.DataFrame, self._data.to_pandas())

    def download_data(self, auth: Auth):
        self._data = self.store.download_data(self, auth)
        return self

    def upload_data(self, df: pd.DataFrame, auth: Auth) -> "Feature":
        """Upload a batch of data to the URI"""
        # noinspection PyArgumentList
        data = pa.Table.from_pandas(df, preserve_index=True)
        pq.write_table(data, self.uri)
        self._data = data
        return self

    @property
    def store(self) -> Store:
        store_type, _, _ = self.uri.partition("://")
        return STORES[store_type]()
