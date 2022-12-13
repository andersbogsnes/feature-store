from dataclasses import dataclass, field
from typing import Optional

from feature_store.backends.base import Backend
from feature_store.backends.local import LocalStorageBackend
from feature_store.feature import Feature


@dataclass
class Client:
    registry: Backend = field(default_factory=LocalStorageBackend)

    def get_features(self) -> list[Feature]:
        """Get all features stored in the feature store"""
        return self.registry.get_all_features()

    def register_feature(self, feature_name: str, uri: str) -> Feature:
        """Register a new feature to the feature store."""
        new_feature = Feature(name=feature_name, uri=uri)
        self.registry.add_feature_metadata(new_feature)
        return new_feature

    def get_feature(self, feature_name: str) -> Optional[Feature]:
        """Get a single feature from the store"""
        return self.registry.get_feature_metadata(feature_name)
