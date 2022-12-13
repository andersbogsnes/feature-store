from dataclasses import dataclass, field

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class Feature:
    name: str
    uri: str
    _data: pa.Table

    def upload_batch(self, df: pd.DataFrame) -> "Feature":
        """Upload a batch of data to the URI"""
        data = pa.Table.from_pandas(df, preserve_index=True)
        pq.write_table(data, self.uri)
        return self

    def download_batch(self) -> "Feature":
        """Download a batch of data from the URI"""
        self._data = pq.read_table(self.uri)
        return self

    def to_pandas(self) -> pd.DataFrame:
        return self._data.to_pandas()


@dataclass
class Client:
    features: list[Feature] = field(default_factory=list)

    def get_features(self) -> list[Feature]:
        """Get all features stored in the feature store"""
        return self.features

    def register_feature(self, feature_name: str, uri: str) -> Feature:
        """Register a new feature to the feature store."""
        new_feature = Feature(name=feature_name, uri=uri)
        self.features.append(new_feature)
        return new_feature

    def get_feature(self, feature_name: str) -> Feature | None:
        """Get a single feature from the store"""
        try:
            return next(feature for feature in self.features if feature.name == feature_name)
        except StopIteration:
            return None
