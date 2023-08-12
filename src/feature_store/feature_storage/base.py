from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd
import pyarrow as pa

from feature_store.auth import AuthType

if TYPE_CHECKING:
    from feature_store.feature import Feature


class FeatureStorage(Protocol):
    def __init__(self, *args, **kwargs):
        pass

    def download_data(self, feature: Feature, auth: AuthType) -> pa.Table:
        ...

    def upload_data(
        self, df: pd.DataFrame, feature: Feature, auth: AuthType
    ) -> pa.Table:
        ...
