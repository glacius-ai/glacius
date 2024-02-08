import json
from typing import List, Union


class Entity:
    """Represents an entity that can be identified using either a single key or multiple keys."""

    keys: List[str]  # List of keys where each key is a column name

    def __init__(self, *, key: str = None, keys: List[str] = None):
        """Initializes an Entity instance.

        Args:
            key (str, optional): A single column name.
            keys (List[str], optional): A list of column names.
        """
        if key is not None and keys is not None:
            raise ValueError("Entity can accept either 'key' or 'keys', but not both.")

        if key:
            self.keys = [key]
        elif keys:
            self.keys = keys
        else:
            raise ValueError("Either 'key' or 'keys' must be provided.")

    def add_key(self, key: str):
        """Adds a new key to the entity."""
        if key not in self.keys:
            self.keys.append(key)

    def __repr__(self):
        """Returns a string representation of the Entity.

        Returns:
            str: The string representation of the Entity.
        """
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def to_dict(self) -> dict:
        """Converts the Entity instance to a dictionary representation.

        Returns:
            dict: The dictionary representation of the Entity.
        """
        return {"keys": self.keys}

    @classmethod
    def from_dict(cls, data: dict) -> "Entity":
        """Creates an Entity instance from a dictionary representation.

        Args:
            data (dict): The dictionary representation of an Entity.

        Returns:
            Entity: The Entity instance.
        """
        return cls(keys=data["keys"])

    def id(self, *args):
        if len(args) != len(self.keys):
            raise Exception(
                f"ids should match the total number of keys specified for this entity: {len(self.keys)}"
            )

        ids_and_keys = sorted(zip(self.keys, args), key=lambda x: x[0])

        return ":".join(":".join(pair) for pair in ids_and_keys)
