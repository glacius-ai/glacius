from datetime import timedelta
from enum import Enum


class AggregationType(Enum):
    """
    Enum class to define different aggregation types.
    """

    LATEST = "LATEST"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    DISTINCT = "DISTINCT"


class Aggregation:
    """
    Represents an aggregation method and its associated time window.

    Attributes:
        _method (AggregationType): The aggregation method to use.
        _window (timedelta): The time window for which the aggregation is computed.
    """

    _method: AggregationType
    _window: timedelta

    def __init__(self, method: AggregationType, window: timedelta = timedelta(days=30)):
        self._method = method
        self._window = window

    @property
    def method(self) -> AggregationType:
        """
        Returns the aggregation method.

        Returns:
            AggregationType: The aggregation method.
        """
        return self._method

    @property
    def window(self) -> timedelta:
        """
        Returns the time window for aggregation.

        Returns:
            timedelta: The time window for aggregation.
        """
        return self._window

    def __repr__(self) -> str:
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def to_dict(self) -> dict:
        """
        Converts the Aggregation object to a dictionary representation.

        Returns:
            dict: The dictionary representation.
        """
        return {
            "method": self.method.value,  # Assuming AggregationType has a `value` property
            "window": int(self._window.total_seconds()),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Aggregation":
        """
        Constructs an Aggregation object from its dictionary representation.

        Args:
            data (dict): The dictionary representation.

        Returns:
            Aggregation: The constructed Aggregation object.
        """
        return cls(
            method=AggregationType(data["method"]),
            window=timedelta(seconds=data["window"]),
        )


DEFAULT_AGG = Aggregation(method=AggregationType.LATEST)
