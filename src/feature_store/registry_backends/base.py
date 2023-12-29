from typing import Optional, Protocol

from feature_store.feature import FeatureGroup


class RegistryBackend(Protocol):
    def add_feature_group_metadata(self, feature_group: FeatureGroup) -> None:
        ...

    def get_feature_group_metadata(
        self, feature_group_name: str
    ) -> Optional[FeatureGroup]:
        ...

    def get_available_feature_metadata(self) -> list[FeatureGroup]:
        """Get names of available feature groups"""
        ...
