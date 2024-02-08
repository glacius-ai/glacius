from enum import Enum
from typing import Optional

from glacius.data_sources.source import DataSource, SourceType


class FileType(Enum):
    """Enumeration for supported file types."""

    CSV = "CSV"
    PARQUET = "PARQUET"


class FileSource(DataSource):
    uri: str
    file_type: FileType

    def __init__(
        self,
        *,
        name: str,
        description: str,
        timestamp_col: str,
        uri: str,
        file_type: FileType,
        query: Optional[str] = None,
    ):
        """Initializes a new instance of FileSource."""
        super().__init__(
            name=name,
            description=description,
            timestamp_col=timestamp_col,
            source_type=SourceType.FILE,
        )
        self._uri = uri
        self._file_type = file_type
        self._query = query

    @property
    def uri(self) -> str:
        """str: Gets the URI of the file."""
        return self._uri

    @property
    def query(self):
        return self._query

    @property
    def file_type(self) -> FileType:
        """FileType: Gets the type of the file."""
        return self._file_type

    @property
    def source_type(self):
        return self._source_type

    def to_dict(self) -> dict:
        """Converts the FileSource instance to a dictionary.

        Returns:
            dict: The dictionary representation of the FileSource.
        """
        return {
            "name": self.name,
            "description": self.description,
            "timestamp_col": self.timestamp_col,
            "uri": self.uri,
            "file_type": self.file_type.value,
            "source_type": self.source_type.value,
            "query": self.query,
        }

    @classmethod
    def from_dict(cls, data_dict: dict) -> "FileSource":
        """Creates a new instance of FileSource from a dictionary.

        Args:
            data_dict (dict): Dictionary representation of a FileSource.

        Returns:
            FileSource: A new instance of FileSource.
        """

        print(data_dict)

        return cls(
            name=data_dict["name"],
            description=data_dict["description"],
            timestamp_col=data_dict["timestamp_col"],
            uri=data_dict["uri"],
            file_type=FileType(data_dict["file_type"]),
            query=data_dict["query"] if "query" in data_dict else None,
        )
