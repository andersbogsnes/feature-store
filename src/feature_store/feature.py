from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property, reduce
from typing import Any, Type, Union

import pandas as pd
import pyarrow as pa
from typing_extensions import Self

from feature_store.exceptions import MismatchedFeatureException, MissingDataException
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
class FeatureGroup:
    """
    Represents a group of features
    """

    name: str
    location: str
    id_column: str
    description: str
    features: list[Feature] = field(default_factory=list)
    datetime_column: str = "date_time"

    def __repr__(self):
        return f"FeatureGroup(name={self.name}, location={self.location})"


@dataclass(repr=False)
class Feature:
    """
    Represents a single Feature

    Parameters
    ----------
    name:
        The name of the Feature
    """

    name: str
    id_column: str
    datetime_column: str
    _data: pa.Table | None = field(init=False, default=None)

    def __repr__(self):
        return f"<Feature {self.name}>"

    def read_data(self, data: pa.Table) -> Self:
        self._data = data.select([self.id_column, self.datetime_column, self.name])
        return self

    @property
    def data(self) -> pa.Table:
        if self._data is None:
            raise MissingDataException(f"Feature {self.name} is missing data")
        return self._data


@dataclass()
class Dataset:
    features: list[Feature] = field(default_factory=list)

    def to_pandas(self) -> pd.DataFrame:
        return self.data.to_pandas()

    @property
    def has_data(self) -> bool:
        return self.features == []

    @property
    def id_column(self) -> str:
        ids = {feature.id_column for feature in self.features}
        if len(ids) != 1:
            raise MismatchedFeatureException(f"Multiple id columns found {ids}")
        return next(iter(ids))

    @property
    def datetime_column(self) -> str:
        datetime_col = {feature.datetime_column for feature in self.features}
        if len(datetime_col) != 1:
            raise MismatchedFeatureException(
                f"Multiple datetime columns found {datetime_col}"
            )
        return next(iter(datetime_col))

    @cached_property
    def data(self) -> pa.Table:
        if len(self.features) == 1:
            return self.features[0].data

        def join_features(table: pa.Table, feature_b: Feature) -> pa.Table:
            return table.join(
                feature_b.data, keys=[feature_b.id_column, feature_b.datetime_column]
            )

        initial_table = self.features[0].data
        return reduce(join_features, self.features[1:], initial_table)

    def __add__(self, other: Union[Dataset, Feature]):
        if not isinstance(other, (Dataset, Feature)):
            return NotImplemented

        if other.id_column != self.id_column:
            raise MismatchedFeatureException(
                f"Trying to join {other} "
                f"but {other.id_column} not in "
                f"existing columns {self.id_column}"
            )
        if other.datetime_column != self.datetime_column:
            raise MismatchedFeatureException(
                f"Trying to join {other} but {other.datetime_column} not in existing columns "
                f"{self.datetime_column}"
            )
        match other:
            case Dataset():
                return Dataset(features=[*self.features, *other.features])
            case Feature():
                return Dataset(features=[*self.features, other])
