import json
from typing import Any, Dict, List, Optional

from glacius.data_sources.registry import ENUM_TO_SOURCE_CLS
from glacius.data_sources.source import DataSource, SourceType
from glacius.entity import Entity
from glacius.feature import Feature
from glacius.hash_utils import md5_hash_str


class FeatureBundle:
    def __init__(
        self,
        name: str,
        description: str,
        source: DataSource,
        features: List[Feature] = None,
        entity: Entity = None,
    ):
        """Initializes a FeatureBundle.

        Args:
            name (str): The name of the feature bundle.
            description (str): A description of the bundle.
            source (DataSource): Data source for the bundle.
            features (List[Feature], optional): List of features in the bundle.
            entity_keys (List[EntityKey], optional): List of entity keys.
        """
        self._name = name
        self._description = description
        self._source = source
        self._features = features if features else []
        self._entity = entity

    def add_feature(self, feature: Feature) -> None:
        """Adds a feature to the bundle.

        Args:
            feature (Feature): The feature to be added.
        """
        self.features.append(feature)

    def add_features(self, features: List[Feature]) -> None:
        """Adds a list of features to the bundle

        Args:
            features (List[Feature]): The list of features to be added.
        """
        for feature in features:
            self.features.append(feature)

    @property
    def name(self) -> str:
        """str: The name of the feature bundle."""
        return self._name

    @property
    def description(self) -> str:
        """str: A descriptive text explaining the purpose or content of the bundle."""
        return self._description

    @property
    def source(self) -> DataSource:
        """DataSource: The source of the data for the feature bundle."""
        return self._source

    @property
    def features(self) -> List[Feature]:
        """List[Feature]: The features contained in the bundle."""
        return self._features

    @property
    def entity(self) -> Entity:
        """List[EntityKey]: The entity keys associated with the bundle."""
        return self._entity

    def to_json(self) -> str:
        return json.dumps(
            {
                "name": self.name,
                "description": self.description,
                "source": self.source.to_dict(),  # We'll need to implement to_dict method for the DataSource subclasses
                "features": [
                    feature.to_dict() for feature in self.features
                ],  # We'll need to implement to_dict for Feature
                "entity": self.entity.to_dict()
                if self.entity
                else None,  # Assuming entity has to_dict method
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "FeatureBundle":
        """Creates a FeatureBundle instance from a JSON formatted string.

        Args:
            json_str (str): JSON formatted string representation of a FeatureBundle.

        Returns:
            FeatureBundle: A new instance of FeatureBundle.
        """
        data_dict = json.loads(json_str)

        data_source_constructor = ENUM_TO_SOURCE_CLS[
            data_dict["source"]["source_type"]
        ]  # Assuming the source type is stored as a string in the serialized json

        data_source_instance = data_source_constructor.from_dict(data_dict["source"])

        return cls(
            name=data_dict["name"],
            description=data_dict["description"],
            source=data_source_instance,
            features=[
                Feature.from_dict(feature_dict)
                for feature_dict in data_dict["features"]
            ],
            entity=Entity.from_dict(data_dict["entity"]),
        )

    def __repr__(self):
        """Returns a string representation of the FeatureBundle.

        Returns:
            str: The string representation of the FeatureBundle.
        """
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def to_dict(self) -> Dict[str, Any]:
        """Converts the FeatureBundle instance to a dictionary representation.

        Returns:
            dict: A dictionary representation of the FeatureBundle.
        """
        return {
            "name": self.name,
            "description": self.description,
            "source": self.source.to_dict(),  # Assuming DataSource has a to_dict method
            "features": [
                feature.to_dict() for feature in self.features
            ],  # Assuming Feature has a to_dict method
            "entity": self.entity.to_dict()
            if self.entity
            else None,  # Assuming Entity has a to_dict method
        }

    @classmethod
    def from_dict(cls, data_dict: Dict[str, Any]) -> "FeatureBundle":
        """Creates a FeatureBundle instance from a dictionary.

        Args:
            data_dict (Dict[str, Any]): Dictionary representation of a FeatureBundle.

        Returns:
            FeatureBundle: A new instance of FeatureBundle.
        """
        # Retrieve the DataSource subclass using the source type from the data dictionary
        print(data_dict, type(data_dict))
        data_source_constructor = ENUM_TO_SOURCE_CLS[data_dict["source"]["source_type"]]

        # Create a DataSource instance from its dictionary representation
        data_source_instance = data_source_constructor.from_dict(data_dict["source"])

        return cls(
            name=data_dict["name"],
            description=data_dict["description"],
            source=data_source_instance,
            features=[
                Feature.from_dict(feature_dict)
                for feature_dict in data_dict["features"]
            ],
            entity=Entity.from_dict(data_dict["entity"])
            if data_dict["entity"]
            else None,
        )

    @property
    def identifier(self) -> str:
        """
        Computes a deterministic identifier for the FeatureBundle object.

        Returns:
            str: The computed identifier.
        """
        # Convert the object to its dict representation
        data = self.to_dict()

        # Convert the dict to a JSON string
        serialized_data = json.dumps(data, sort_keys=True)

        # Compute an MD5 hash of the JSON string
        return md5_hash_str(serialized_data)
