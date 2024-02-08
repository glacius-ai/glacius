import json
from enum import Enum

from glacius.hash_utils import md5_hash_str


class SourceType(Enum):
    INVALID = "INVALID"
    FILE = "FILE"
    SNOWFLAKE = "SNOWFLAKE"
    BIGQUERY = "BIGQUERY"
    REDSHIFT = "REDSHIFT"


class DataSource:
    name: str
    description: str
    timestamp_col: str
    source_type: SourceType

    def __init__(
        self,
        *,
        name: str,
        description: str,
        timestamp_col: str,
        source_type: SourceType,
    ):
        self._name = name
        self._description = description
        self._timestamp_col = timestamp_col
        self._source_type = source_type

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description

    @property
    def timestamp_col(self):
        return self._timestamp_col

    @property
    def source_type(self):
        return self._source_type

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    @property
    def identifier(self) -> str:
        """
        Computes a deterministic identifier for the RedshiftSource object.

        Returns:
            str: The computed identifier.
        """
        # Convert the object to its dict representation
        data = self.to_dict()

        # Convert the dict to a JSON string
        serialized_data = json.dumps(data, sort_keys=True)

        # Compute an MD5 hash of the JSON string
        return md5_hash_str(serialized_data)
