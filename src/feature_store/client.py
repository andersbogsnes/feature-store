from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from feature_store.auth.base import AuthType
from feature_store.auth.file_auth import FileAuth
from feature_store.backends.base import Backend
from feature_store.backends.local import LocalStorageBackend
from feature_store.feature import Feature


@dataclass
class Client:
    registry: Backend = field(default_factory=LocalStorageBackend)
    auth: AuthType = field(default_factory=FileAuth)

    def get_features(self) -> list[Feature]:
        """Get all features stored in the feature store"""
        return self.registry.get_all_features()

    def register_feature(
        self, feature_name: str, uri: str, auth_key: Optional[str] = None
    ) -> Feature:
        """Register a new feature to the feature store."""
        new_feature = Feature(name=feature_name, uri=uri, auth_key=auth_key)
        self.registry.add_feature_metadata(new_feature)
        return new_feature

    def get_feature(self, feature_name: str) -> Optional[Feature]:
        """Get a single feature from the store"""
        feature = self.registry.get_feature_metadata(feature_name)
        return feature.download_data(self.auth)

    def upload_feature_data(self, feature_name: str, data: pd.DataFrame) -> Feature:
        feature = self.registry.get_feature_metadata(feature_name)
        return feature.upload_data(data, auth=self.auth)
