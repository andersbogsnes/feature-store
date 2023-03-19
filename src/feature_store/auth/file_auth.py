import functools
import pathlib
from dataclasses import dataclass
from typing import Any

import yaml


@dataclass
class FileAuth:
    config_file: pathlib.Path = pathlib.Path("featurestore.yaml")

    def get(self, key: str) -> dict[str, Any]:
        if key in self._file_config:
            return self._file_config[key]
        return {}

    @functools.cached_property
    def _file_config(self) -> dict:
        if not self.config_file.exists():
            return {}
        with self.config_file.open() as f:
            config = yaml.safe_load(f)
        return config
