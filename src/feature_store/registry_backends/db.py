import contextlib
from dataclasses import dataclass
from functools import cached_property
from typing import Generator, Optional

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

from feature_store.feature import Feature, FeatureGroup


class Base(DeclarativeBase):
    pass


class FeatureTable(Base):
    __tablename__ = "features"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    id_column: Mapped[str]
    datetime_column: Mapped[str] = mapped_column(default="date_time")
    feature_group_id: Mapped[int] = mapped_column(sa.ForeignKey("feature_groups.id"))
    feature_group: Mapped[FeatureGroup] = relationship(
        "FeatureGroupTable", back_populates="features"
    )

    def to_feature(self) -> Feature:
        return Feature(
            name=self.name,
            id_column=self.id_column,
            datetime_column=self.datetime_column,
        )


class FeatureGroupTable(Base):
    __tablename__ = "feature_groups"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False, unique=True)
    id_column: Mapped[str]
    datetime_column: Mapped[str] = mapped_column(default="date_time")
    location: Mapped[str]
    description: Mapped[str]
    features: Mapped[list[FeatureTable]] = relationship(
        "FeatureTable", back_populates="feature_group", lazy="selectin"
    )

    @classmethod
    def from_feature_group(cls, feature_group: FeatureGroup) -> "FeatureGroupTable":
        return FeatureGroupTable(
            name=feature_group.name,
            id_column=feature_group.id_column,
            datetime_column=feature_group.datetime_column,
            location=feature_group.location,
            description=feature_group.description,
            features=[
                FeatureTable(
                    name=f.name,
                    id_column=f.id_column,
                    datetime_column=f.datetime_column,
                )
                for f in feature_group.features
            ],
        )

    def to_feature_group(self) -> FeatureGroup:
        return FeatureGroup(
            name=self.name,
            id_column=self.id_column,
            datetime_column=self.datetime_column,
            location=self.location,
            description=self.description,
            features=[f.to_feature() for f in self.features],
        )


@dataclass
class DatabaseRegistryBackend:
    database_url: str

    @cached_property
    def _engine(self) -> sa.engine.Engine:
        engine = sa.create_engine(self.database_url)
        Base.metadata.create_all(engine)
        return engine

    @contextlib.contextmanager
    def _session(self) -> Generator[Session, None, None]:
        with Session(self._engine) as session:
            yield session

    def add_feature_group_metadata(self, feature_group: FeatureGroup) -> None:
        new_row = FeatureGroupTable.from_feature_group(feature_group)
        with self._session() as session:
            session.add(new_row)
            session.commit()

    def get_feature_group_metadata(
        self, feature_group_name: str
    ) -> Optional[FeatureGroup]:
        sql = sa.select(FeatureGroupTable).where(
            FeatureGroupTable.name == feature_group_name
        )

        with self._session() as session:
            result: Optional[FeatureGroupTable] = session.execute(
                sql
            ).scalar_one_or_none()

        return None if result is None else result.to_feature_group()

    def get_available_feature_metadata(self) -> list[FeatureGroup]:
        sql = sa.select(FeatureGroupTable)
        with self._session() as session:
            result: list[FeatureGroupTable] = session.execute(sql).scalars()
            return [f.to_feature_group() for f in result]
