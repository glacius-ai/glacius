import json
from typing import List

from glacius.dsl import Expr, reconstruct
from glacius.dtypes import DataType
from glacius.feature import Feature
from glacius.hash_utils import md5_hash_str


class DerivedFeature(Feature):
    """Represents a derived feature dependent on other regular features."""

    def __init__(
        self,
        *,
        name: str,
        description: str,
        expr: Expr,
        dtype: DataType,
        dependencies: List[str] = None,
    ):
        """
        Initializes a DerivedFeature instance.

        Args:
            dependencies (List[str], optional): A list of names of dependent regular features.
        """
        super().__init__(
            name=name,
            description=description,
            expr=expr,
            dtype=dtype,
        )
        self._dependencies = dependencies or []

    @property
    def dependencies(self) -> List[str]:
        """List[str]: A list of names of dependent regular features."""
        return self._dependencies

    @property
    def agg(self) -> None:
        """Derived features don't have aggregation."""
        return None

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def to_json(self) -> str:
        """
        Converts the DerivedFeature object to its JSON string representation.

        Returns:
            str: The JSON string representation.
        """
        data = {
            "name": self.name,
            "description": self.description,
            "expr_sql": self.expr_sql,
            "dtype": self.dtype.value,  # Assuming DataType has a `value` property
            "dependencies": self.dependencies,
        }
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str) -> "DerivedFeature":
        """
        Constructs a DerivedFeature object from its JSON string representation.

        Args:
            json_str (str): The JSON string representation.

        Returns:
            DerivedFeature: The constructed DerivedFeature object.
        """
        data = json.loads(json_str)

        return cls(
            name=data["name"],
            description=data["description"],
            expr=reconstruct(data["expr_sql"]),
            dtype=DataType(
                data["dtype"]
            ),  # Assuming DataType can be constructed from its value
            dependencies=data["dependencies"],
        )

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "expr_sql": self.expr_sql,
            "dtype": self.dtype.value,
            "dependencies": self.dependencies,
        }

    @classmethod
    def from_dict(cls, data_dict: dict) -> "DerivedFeature":
        """
        Constructs a DerivedFeature object from its dictionary representation.

        Args:
            data_dict (dict): The dictionary representation.

        Returns:
            DerivedFeature: The constructed DerivedFeature object.
        """
        print(data_dict)
        return cls(
            name=data_dict["name"],
            description=data_dict["description"],
            expr=reconstruct(data_dict["expr_sql"]),
            dtype=DataType(
                data_dict["dtype"]
            ),  # Assuming DataType can be constructed from its value
            dependencies=data_dict["dependencies"],
        )

    @property
    def identifier(self) -> str:
        """
        Computes a deterministic identifier for the Feature object.

        Returns:
            str: The computed identifier.
        """
        # Convert the object to its dict representation
        data = self.to_dict()

        # Convert the dict to a JSON string
        serialized_data = json.dumps(data, sort_keys=True)

        # Compute an MD5 hash of the JSON string
        return md5_hash_str(serialized_data)
