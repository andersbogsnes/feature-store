import contextlib
from dataclasses import dataclass
from functools import cached_property
from typing import Generator, Optional

import sqlalchemy as sa
from sqlalchemy.orm import Session, registry

from feature_store.feature import Feature

mapped_registry: registry = registry()


@mapped_registry.mapped
class FeatureTable:
    __tablename__ = "features"

    id: Optional[int] = sa.Column(sa.Integer, primary_key=True)
    name: str = sa.Column(sa.String, unique=True)
    uri: str = sa.Column(sa.String)

    def __init__(self, name: str, uri: str):
        self.name = name
        self.uri = uri

    @classmethod
    def from_feature(cls, feature: Feature) -> "FeatureTable":
        return FeatureTable(name=feature.name, uri=feature.uri)

    def to_feature(self) -> Feature:
        return Feature(name=self.name, uri=self.uri)


@dataclass
class DatabaseStorageBackend:
    database_url: str

    @cached_property
    def _engine(self) -> sa.engine.Engine:
        return sa.create_engine(self.database_url, future=True)

    @contextlib.contextmanager
    def _session(self) -> Generator[Session, None, None]:
        with Session(self._engine) as session:
            yield session

    def add_feature_metadata(self, feature: Feature) -> None:
        new_row = FeatureTable.from_feature(feature)
        with self._session() as session:
            session.add(new_row)
            session.commit()

    def get_feature_metadata(self, feature_name: str) -> Optional[Feature]:
        sql = sa.select(FeatureTable).where(FeatureTable.name == feature_name)

        with self._session() as session:
            result: Optional[FeatureTable] = session.execute(sql).scalar_one_or_none()

        return None if result is None else result.to_feature()

    def get_all_features(self) -> list[Feature]:
        sql = sa.select(FeatureTable)
        with self._session() as session:
            result: list[FeatureTable] = session.execute(sql).scalars()
            return [feature.to_feature() for feature in result]
