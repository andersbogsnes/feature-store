from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Type, Union, cast

import pandas as pd
import pyarrow as pa

from feature_store.auth import AuthType
from feature_store.exceptions import FeatureDataException
from feature_store.stores.base import Store
from feature_store.stores.parquet import ParquetFeatureStore
from feature_store.stores.sql import SQLAlchemyFeatureStore


class FeatureKind(str, Enum):
    parquet = "parquet"
    sql = "sql"


STORES: dict[FeatureKind, Type[Store]] = {
    FeatureKind.parquet: ParquetFeatureStore,
    FeatureKind.sql: SQLAlchemyFeatureStore,
}


@dataclass
class Feature:
    name: str
    kind: FeatureKind
    location: str
    id_column: str
    datetime_column: str = "date_time"
    auth_key: Optional[str] = None
    _data: pa.Table = None

    def to_pandas(self) -> pd.DataFrame:
        return cast(pd.DataFrame, self._data.to_pandas())

    def download_data(self, auth: AuthType):
        self._data = self.store.download_data(self, auth)
        return self

    def upload_data(self, df: pd.DataFrame, auth: AuthType) -> Feature:
        """Upload a batch of data to the URI"""
        self._data = self.store.upload_data(df, self, auth)
        return self

    @property
    def store(self) -> Store:
        return STORES[self.kind]()

    @property
    def has_data(self) -> bool:
        return self._data is not None

    def __add__(self, other: Union[Feature, Dataset]) -> Dataset:
        if not self.has_data:
            raise FeatureDataException(f"The {self.name} feature has no data")
        if not other.has_data:
            raise FeatureDataException(f"The {other.name} feature has no data")

        joined_data = self._data.join(
            other._data,
            keys=[self.id_column, self.datetime_column],
            right_keys=[other.id_column, other.datetime_column],
        )
        return Dataset(joined_data)


@dataclass
class Dataset:
    _data: pa.Table

    @property
    def has_data(self) -> bool:
        return self._data is not None
