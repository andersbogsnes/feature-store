from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    from feature_store.feature import Feature


class FeatureStorage(Protocol):
    def __init__(self, *args, **kwargs):
        pass

    def download_data(self, feature: Feature) -> pa.Table:
        ...

    def upload_data(self, df: pd.DataFrame, feature: Feature) -> pa.Table:
        ...
