from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Protocol

import pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    from feature_store.feature import FeatureGroup


class FeatureStorage(Protocol):
    type: ClassVar[str]

    def download_data(self, feature: FeatureGroup) -> pa.Table:
        ...

    def upload_data(self, df: pd.DataFrame, feature: FeatureGroup) -> pa.Table:
        ...
