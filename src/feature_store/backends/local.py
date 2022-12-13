from dataclasses import dataclass

from feature_store.backends.db import DatabaseStorageBackend, mapped_registry


@dataclass
class LocalStorageBackend(DatabaseStorageBackend):
    database_url: str = "sqlite:///features.db"

    def __post_init__(self) -> None:
        mapped_registry.metadata.create_all(self._engine)
