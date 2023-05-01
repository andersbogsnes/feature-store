from typing import Any, Protocol


class AuthType(Protocol):
    def get_sources_key(self, key: str) -> dict[str, Any]:
        ...
