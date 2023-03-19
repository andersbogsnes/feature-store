from typing import Any, Protocol


class AuthType(Protocol):
    def get(self, key: str) -> dict[str, Any]:
        ...
