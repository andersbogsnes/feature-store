from dataclasses import dataclass
from typing import cast

import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq


@dataclass
class Feature:
    name: str
    uri: str
    _data: pa.Table = None

    def upload_batch(self, df: pd.DataFrame) -> "Feature":
        """Upload a batch of data to the URI"""
        # noinspection PyArgumentList
        data = pa.Table.from_pandas(df, preserve_index=True)
        pq.write_table(data, self.uri)
        return self

    def download_batch(self) -> "Feature":
        """Download a batch of data from the URI"""
        self._data = pq.read_table(self.uri)
        return self

    def to_pandas(self) -> pd.DataFrame:
        return cast(pd.DataFrame, self._data.to_pandas())
