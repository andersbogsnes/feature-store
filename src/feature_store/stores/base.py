from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from feature_store.auth import AuthType

if TYPE_CHECKING:
    from feature_store.feature import Feature


class Store(Protocol):
    def download_data(self, feature: "Feature", auth: AuthType) -> pa.Table:
        ...
