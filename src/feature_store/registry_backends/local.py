from dataclasses import dataclass

from feature_store.registry_backends.db import Base, DatabaseRegistryBackend


@dataclass
class LocalRegistryBackend(DatabaseRegistryBackend):
    database_url: str = "sqlite:///features.db"

    def __post_init__(self) -> None:
        Base.metadata.create_all(self._engine)
