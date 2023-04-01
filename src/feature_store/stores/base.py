from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    from feature_store.feature import Feature


class Store(Protocol):
    def download_data(self, feature: Feature) -> dd.DataFrame:
        ...

    def upload_data(self, df: pd.DataFrame, feature: Feature) -> dd.DataFrame:
        ...
