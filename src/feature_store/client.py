import operator
from dataclasses import dataclass, field
from functools import reduce
from typing import Optional

import pandas as pd

from feature_store.auth.base import AuthType
from feature_store.auth.file_auth import FileAuth
from feature_store.exceptions import FeatureNotFoundException
from feature_store.feature import Dataset, Feature
from feature_store.registry_backends.base import RegistryBackend
from feature_store.registry_backends.local import LocalRegistryBackend


@dataclass
class Client:
    registry: RegistryBackend = field(default_factory=LocalRegistryBackend)
    auth: AuthType = field(default_factory=FileAuth)

    def get_available_features(self) -> list[Feature]:
        """Get all features stored in the feature store"""
        return self.registry.get_available_feature_metadata()

    def get_dataset(self, feature_names: list[str]) -> Dataset:
        """Get a given dataset by specifying the features that should be in the dataset

        Parameters
        ----------
        feature_names
            The list of features that should be included in the dataset
        """
        empty_dataset = Dataset(features=[])
        features = [
            self.registry.get_feature_metadata(name).download_data(self.auth)
            for name in feature_names
        ]
        return reduce(operator.add, features, empty_dataset)

    def register_feature(
        self,
        feature_name: str,
        location: str,
        id_column: str,
        date_column: str = "date_time",
        auth_key: Optional[str] = None,
    ) -> Feature:
        """Register a new feature to the feature store.

        Parameters
        ----------
        feature_name
            The name of the feature. Should match the name of the column in the data
        location
            The location of the data. Passed to the backend.
        id_column
            The column that is used for joining to other columns
        date_column
            The column that identifies the date for which this feature is valid
        auth_key
            The key to look up in the Auth backend to get correct auth information
        """
        new_feature = Feature(
            name=feature_name,
            location=location,
            id_column=id_column,
            datetime_column=date_column,
            auth_key=auth_key,
        )
        self.registry.add_feature_metadata(new_feature)
        return new_feature

    def get_feature(self, feature_name: str) -> Optional[Feature]:
        """Get a single feature from the store

        Parameters
        ----------
        feature_name
            The name of the feature to get
        """
        feature = self.registry.get_feature_metadata(feature_name)
        if feature is None:
            raise FeatureNotFoundException(f"{feature_name} was not found")
        return feature.download_data(self.auth)

    def upload_feature_data(self, feature_name: str, data: pd.DataFrame) -> Feature:
        """
        Upload data to the backend for a given feature

        Parameters
        ----------
        feature_name
            The name of the feature
        data
            The data to be stored in the backend

        Returns
        -------
        Feature
            The feature that the data was stored to
        """
        feature = self.registry.get_feature_metadata(feature_name)
        return feature.upload_data(data, auth=self.auth)
