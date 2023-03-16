from typing import Protocol


class AuthType(Protocol):
    def get(self, key: str) -> str:
        ...
