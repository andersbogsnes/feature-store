from __future__ import annotations

from typing import Protocol

from feature_store.feature_storage import FeatureStorage


class AuthType(Protocol):
    def get_store(self, location: str) -> FeatureStorage:
        ...
