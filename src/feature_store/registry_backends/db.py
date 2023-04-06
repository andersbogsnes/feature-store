import contextlib
from dataclasses import dataclass
from functools import cached_property
from typing import Generator, Optional

import sqlalchemy as sa
from sqlalchemy.orm import Session, registry

from feature_store.feature import Feature, FeatureKind

mapped_registry: registry = registry()


@mapped_registry.mapped
class FeatureTable:
    __tablename__ = "features"

    id: Optional[int] = sa.Column(sa.Integer, primary_key=True)
    name: str = sa.Column(sa.String, unique=True, nullable=False)
    id_column: str = sa.Column(sa.String)
    datetime_column: str = sa.Column(sa.String, default="date_time")
    kind: FeatureKind = sa.Column(sa.Enum(FeatureKind))
    location: str = sa.Column(sa.String, nullable=False)
    auth_key: str = sa.Column(sa.String, nullable=True)

    def __init__(
        self,
        name: str,
        id_column: str,
        datetime_column: str,
        kind: FeatureKind,
        location: str,
        auth_key: Optional[str],
    ):
        self.name = name
        self.id_column = id_column
        self.datetime_column = datetime_column
        self.kind = kind
        self.location = location
        self.auth_key = auth_key

    @classmethod
    def from_feature(cls, feature: Feature) -> "FeatureTable":
        return FeatureTable(
            name=feature.name,
            id_column=feature.id_column,
            datetime_column=feature.datetime_column,
            kind=feature.kind,
            location=feature.location,
            auth_key=feature.auth_key,
        )

    def to_feature(self) -> Feature:
        return Feature(
            name=self.name,
            kind=self.kind,
            id_column=self.id_column,
            datetime_column=self.datetime_column,
            location=self.location,
            auth_key=self.auth_key,
        )


@dataclass
class DatabaseRegistryBackend:
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

    def get_available_feature_metadata(self) -> list[Feature]:
        sql = sa.select(FeatureTable)
        with self._session() as session:
            result: list[FeatureTable] = session.execute(sql).scalars()
            return [feature.to_feature() for feature in result]
