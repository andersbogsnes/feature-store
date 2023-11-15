from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Protocol

import pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    from feature_store.auth import AuthType
    from feature_store.feature import Feature


class FeatureStorage(Protocol):
    type: ClassVar[str]

    def __init__(self, auth: AuthType, *args, **kwargs):
        ...

    def download_data(self, feature: Feature) -> pa.Table:
        ...

    def upload_data(self, df: pd.DataFrame, feature: Feature) -> pa.Table:
        ...
