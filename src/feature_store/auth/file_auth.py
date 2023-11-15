import functools
import pathlib
from dataclasses import dataclass
from typing import Any

import yaml

from feature_store.feature import STORES
from feature_store.feature_storage import FeatureStorage


@dataclass
class FileAuth:
    config_file: pathlib.Path = pathlib.Path("featurestore.yaml")

    def get_sources_key(self, key: str) -> dict[str, Any]:
        if key in self._file_config.get("sources", {}):
            return self._file_config["sources"][key]
        return {}

    @functools.cached_property
    def _file_config(self) -> dict:
        if not self.config_file.exists():
            return {}
        with self.config_file.open() as f:
            config = yaml.safe_load(f)
        return config

    def get_store(self, location: str) -> FeatureStorage:
        key, _, _ = location.partition("::")
        config = self.get_sources_key(key)
        store_type = config["type"]
        return STORES[store_type](self)
