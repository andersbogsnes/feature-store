import pathlib

import pytest

from feature_store.feature_storage import ParquetFeatureStorage


@pytest.fixture
def parquet_feature_storage(tmp_path: pathlib.Path) -> ParquetFeatureStorage:
    data_file = tmp_path / ""
    return ParquetFeatureStorage(data_file.as_uri())
