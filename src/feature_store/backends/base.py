from typing import Optional, Protocol

from feature_store.feature import Feature


class Backend(Protocol):
    def add_feature_metadata(self, feature: Feature) -> None:
        ...

    def get_feature_metadata(self, feature_name: str) -> Optional[Feature]:
        ...

    def get_available_feature_metadata(self) -> list[Feature]:
        """Get names of available feature"""
        ...
