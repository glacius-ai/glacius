from typing import Optional


from glacius.data_sources.source import DataSource, SourceType


class RedshiftSource(DataSource):
    table: str
    jdbc_url: str
    source_type: SourceType

    def __init__(
        self,
        *,
        name: str,
        description: str,
        timestamp_col: str,
        table: str,
        jdbc_url: str,
        query: Optional[str] = None,
    ):
        super().__init__(
            name=name,
            description=description,
            timestamp_col=timestamp_col,
            source_type=SourceType.REDSHIFT,  # Assuming there is a REDSHIFT enum value
        )
        self._table = table
        self._jdbc_url = jdbc_url
        self._source_type = SourceType.REDSHIFT
        self._query = query

    @property
    def table(self):
        return self._table

    @property
    def source_type(self):
        return self._source_type

    @property
    def jdbc_url(self):
        return self._jdbc_url

    @property
    def query(self):
        return self._query

    def to_dict(self) -> dict:
        """Converts the RedshiftSource instance to a dictionary.

        Returns:
            dict: The dictionary representation of the RedshiftSource.
        """
        return {
            "name": self.name,
            "description": self.description,
            "timestamp_col": self.timestamp_col,
            "table": self.table,
            "source_type": self.source_type.value,
            "query": self.query,
            "jdbc_url": self.jdbc_url,
        }

    @classmethod
    def from_dict(cls, data_dict: dict) -> "RedshiftSource":
        """Creates a new instance of RedshiftSource from a dictionary.

        Args:
            data_dict (dict): Dictionary representation of a RedshiftSource.

        Returns:
            RedshiftSource: A new instance of RedshiftSource.
        """
        return cls(
            name=data_dict["name"],
            description=data_dict["description"],
            timestamp_col=data_dict["timestamp_col"],
            table=data_dict["table"],
            jdbc_url=data_dict["jdbc_url"],
            query=data_dict["query"] if "query" in data_dict else None,
        )
