from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property, reduce
from typing import Any, Optional, Type, Union, cast

import pandas as pd
import pyarrow as pa

from feature_store.auth import AuthType
from feature_store.exceptions import MismatchedFeatureException
from feature_store.feature_storage.base import FeatureStorage
from feature_store.feature_storage.parquet import ParquetFeatureStorage
from feature_store.feature_storage.sql import SQLAlchemyFeatureStorage

STORES: dict[str, Type[FeatureStorage]] = {
    storage.type: storage
    for storage in [ParquetFeatureStorage, SQLAlchemyFeatureStorage]
}


def _get_store_from_config(config: dict[str, Any]) -> Type[FeatureStorage]:
    """Return the needed store to fetch the data"""
    store_type = config.pop("type")
    return STORES[store_type](**config)


@dataclass(repr=False)
class Feature:
    """
    Represents a single Feature

    Parameters
    ----------
    name:
        The name of the Feature
    location:
        Where the Feature is located in the storage backend
    id_column:
        The unique id of the feature - used to join with
    """

    name: str
    location: str
    id_column: str
    datetime_column: str = "date_time"
    auth_key: Optional[str] = None
    _data: pa.Table = None

    def __repr__(self):
        return f"<Feature {self.name}>"

    @property
    def data(self):
        """Return the underlying data, fetching it if not already fetched"""
        if self._data is None:
            return self.download_data()

        return self._data

    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame"""
        return cast(pd.DataFrame, self._data.to_pandas())

    def download_data(self, auth: AuthType):
        """Download the data from the source"""

        store = auth.get_store(self.location)
        self._data = store.download_data(self)
        return self

    def upload_data(self, df: pd.DataFrame, auth: AuthType) -> Feature:
        """Upload a batch of data to the URI"""
        store = auth.get_store(self.location)
        self._data = store.upload_data(df, self)
        return self

    @property
    def has_data(self) -> bool:
        return self._data is not None

    def __add__(self, other: Union[Feature, Dataset]) -> Dataset:
        if isinstance(other, Feature):
            return Dataset(features=[self, other])
        if isinstance(other, Dataset):
            return Dataset(features=[*other.features, self])
        return NotImplemented


@dataclass()
class Dataset:
    features: list[Feature] = field(default_factory=list)

    def to_pandas(self) -> pd.DataFrame:
        return self.data.to_pandas()

    @property
    def has_data(self) -> bool:
        return self.features == []

    @cached_property
    def data(self) -> pa.Table:
        initial_table = self.features[0].data
        return reduce(join_features, self.features[1:], initial_table)

    def __add__(self, other: Union[Dataset, Feature]):
        if isinstance(other, Dataset):
            return Dataset(features=[*self.features, *other.features])
        if isinstance(other, Feature):
            return Dataset(features=[*self.features, other])
        return NotImplemented


def join_features(table: pa.Table, feature_b: Feature) -> pa.Table:
    existing_columns = table.column_names
    if feature_b.id_column not in existing_columns:
        raise MismatchedFeatureException(
            f"Trying to join {feature_b.name} "
            f"but {feature_b.id_column} not in "
            f"existing columns {existing_columns}"
        )
    if feature_b.datetime_column not in existing_columns:
        raise MismatchedFeatureException(
            f"Trying to join {feature_b.name} "
            f"but {feature_b.datetime_column} not in "
            f"existing columns {existing_columns}"
        )

    return table.join(
        feature_b.data, keys=[feature_b.id_column, feature_b.datetime_column]
    )
