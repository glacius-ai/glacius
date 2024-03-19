from typing import Optional

from glacius.data_sources.source import DataSource, SourceType


class SnowflakeSource(DataSource):
    database: str
    schema: str
    table: str
    source_type: SourceType
    query: str

    def __init__(
        self,
        *,
        name: str,
        description: str,
        timestamp_col: str,
        table: str,
        database: str,
        schema: str,
        query: Optional[str] = None,
    ):
        super().__init__(
            name=name,
            description=description,
            timestamp_col=timestamp_col,
            source_type=SourceType.SNOWFLAKE,
        )
        self._table = table
        self._database = database
        self._schema = schema
        self._source_type = SourceType.SNOWFLAKE
        self._query = query

    @property
    def table(self):
        return self._table

    @property
    def database(self):
        return self._database

    @property
    def schema(self):
        return self._schema

    @property
    def source_type(self):
        return self._source_type

    @property
    def query(self):
        return self._query

    def to_dict(self) -> dict:
        """Converts the SnowflakeSource instance to a dictionary.

        Returns:
            dict: The dictionary representation of the SnowflakeSource.
        """
        return {
            "name": self.name,
            "description": self.description,
            "timestamp_col": self.timestamp_col,
            "table": self.table,
            "database": self.database,
            "source_type": self.source_type.value,
            "schema": self.schema,
        }

    @classmethod
    def from_dict(cls, data_dict: dict) -> "SnowflakeSource":
        """Creates a new instance of SnowflakeSource from a dictionary.

        Args:
            data_dict (dict): Dictionary representation of a SnowflakeSource.

        Returns:
            SnowflakeSource: A new instance of SnowflakeSource.
        """
        return cls(
            name=data_dict["name"],
            description=data_dict["description"],
            timestamp_col=data_dict["timestamp_col"],
            table=data_dict["table"],
            database=data_dict["database"],
            schema=data_dict["schema"],
            query=data_dict["query"] if "query" in data_dict else None,
        )
