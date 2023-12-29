from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from feature_store.auth.base import AuthType
from feature_store.auth.file_auth import FileAuth
from feature_store.exceptions import FeatureNotFoundException
from feature_store.feature import Dataset, Feature, FeatureGroup
from feature_store.registry_backends.base import RegistryBackend
from feature_store.registry_backends.local import LocalRegistryBackend


@dataclass
class Client:
    registry: RegistryBackend = field(default_factory=LocalRegistryBackend)
    auth: AuthType = field(default_factory=FileAuth)

    def get_available_features(self) -> list[str]:
        """Get all features stored in the feature store"""
        feature_groups = self.registry.get_available_feature_metadata()
        return [
            f"{feature_group.name}.{feature.name}"
            for feature_group in feature_groups
            for feature in feature_group.features
        ]

    def get_features(self, feature_names: list[str]) -> Dataset:
        """Get a given dataset by specifying the features that should be in the dataset

        Parameters
        ----------
        feature_names
            The list of features that should be included in the dataset
        """

        if any("." not in feature for feature in feature_names):
            raise FeatureNotFoundException("Features must contain a period ('.')")

        feature_dict = defaultdict(list)

        for feature_group_name, feature_name in (
            feature.split(".") for feature in feature_names
        ):
            feature_dict[feature_group_name].append(feature_name)

        feature_groups = [
            self.registry.get_feature_group_metadata(name) for name in feature_dict
        ]

        for group in feature_groups:
            store = self.auth.get_store(group.location)
            table = store.download_data(group)
            for feature in group.features:
                if feature.name in feature_dict[group.name]:
                    feature.read_data(table)

        dataset = Dataset(
            features=[
                feature
                for feature_group in feature_groups
                for feature in feature_group.features
                if feature.name in feature_dict[feature_group.name]
            ]
        )
        return dataset

    def register_feature_group(
        self,
        feature_group_name: str,
        location: str,
        id_column: str,
        description: str,
        date_column: str = "date_time",
        features: list[str] = None,
    ) -> FeatureGroup:
        """Register a new feature to the feature store.

        Parameters
        ----------
        feature_group_name
            The name of the feature group.
        location
            The location of the data. Passed to the backend.
        id_column
            The column that is used for joining to other columns
        description
            A description of the feature group
        date_column
            The column that identifies the date for which this feature is valid
        features
            A list of features contained in the feature group
        """
        new_feature = FeatureGroup(
            name=feature_group_name,
            location=location,
            id_column=id_column,
            datetime_column=date_column,
            description=description,
            features=[
                Feature(name=f, id_column=id_column, datetime_column=date_column)
                for f in features
            ],
        )
        self.registry.add_feature_group_metadata(new_feature)
        return new_feature

    def get_feature(self, feature_name: str) -> Optional[Dataset]:
        """Get a single feature from the store

        Parameters
        ----------
        feature_name
            The name of the feature to get
        """
        return self.get_features([feature_name])

    def upload_feature_data(
        self, feature_group_name: str, data: pd.DataFrame
    ) -> FeatureGroup:
        """
        Upload data to the backend for a given feature

        Parameters
        ----------
        feature_group_name
            The name of the feature
        data
            The data to be stored in the backend

        Returns
        -------
        Feature
            The feature that the data was stored to
        """
        feature_group = self.registry.get_feature_group_metadata(feature_group_name)
        store = self.auth.get_store(feature_group.location)
        table = store.upload_data(data, feature_group)
        for feature in feature_group.features:
            feature.read_data(table)
        return feature_group
