from __future__ import annotations

from typing import Any, Protocol

from feature_store.feature_storage import FeatureStorage


class AuthType(Protocol):
    def get_sources_key(self, key: str) -> dict[str, Any]:
        ...

    def get_store(self, key: str) -> FeatureStorage:
        ...
