import json
from typing import Union

from glacius.aggregation import DEFAULT_AGG, Aggregation, AggregationType
from glacius.dsl import Expr, reconstruct
from glacius.dtypes import DataType
from glacius.hash_utils import md5_hash_str


class Feature:
    """Represents a feature with its specifications."""

    name: str
    description: str
    expr: Expr
    expr_sql: str
    dtype: DataType
    agg: Aggregation

    def __init__(
        self,
        *,
        name: str,
        description: str,
        expr: Expr,
        dtype: DataType,
        agg: Aggregation = DEFAULT_AGG,
    ):
        """Initializes a Feature instance.

        Args:
            name (str): The name of the feature.
            description (str): Description for the feature.
            expr (str): Expression associated with the feature.
            dtype (DataType): The data type of the feature.
            agg (Aggregation, optional): The aggregation type for the feature. Defaults to DEFAULT_AGG.
        """
        self._name = name
        self._description = description
        self._expr = expr
        self._dtype = dtype
        self._agg = agg

    @property
    def name(self) -> str:
        """str: The name of the feature."""
        return self._name

    @property
    def description(self) -> str:
        """str: A descriptive text explaining the purpose or usage of the feature."""
        return self._description

    @property
    def expr(self) -> Union[str, Expr]:
        """str: The expression representing the computation or extraction of this feature."""
        return self._expr

    @property
    def expr_sql(self) -> str:
        return self.expr.compile()

    @property
    def dtype(self) -> DataType:
        """DataType: Specifies the data type of the feature. For example, it can be integer, float, etc."""
        return self._dtype

    @property
    def agg(self) -> Aggregation:
        """Aggregation: The aggregation type used for the feature. It could be sum, average, etc."""
        return self._agg

    def __repr__(self):
        """Returns a string representation of the feature instance.

        Returns:
            str: String representation of the feature.
        """
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "expr_sql": self.expr_sql,
            "dtype": self.dtype.value,
            "agg": self.agg.to_dict(),  # Assuming Aggregation class has the to_dict method implemented
        }

    @classmethod
    def from_dict(cls, data_dict: dict) -> "Feature":
        """
        Constructs a Feature object from its dictionary representation.

        Args:
            data_dict (dict): The dictionary representation.

        Returns:
            Feature: The constructed Feature object.
        """
        return cls(
            name=data_dict["name"],
            description=data_dict["description"],
            expr=reconstruct(
                data_dict["expr_sql"]
            ),  # Assuming reconstruct works on expr_sql
            dtype=DataType(
                data_dict["dtype"]
            ),  # Assuming DataType can be constructed from its value
            agg=Aggregation.from_dict(
                data_dict["agg"]
            ),  # Assuming Aggregation has a from_dict method
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
