from enum import Enum


class DataType(Enum):
    """Supported DataTypes For Features. Note: These Are All Spark Compatible"""

    INT32 = "INT32"
    INT64 = "INT64"
    FLOAT32 = "FLOAT32"
    FLOAT64 = "FLOAT64"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    SHORT = "SHORT"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    DECIMAL = "DECIMAL"
    BINARY = "BINARY"

    # Complex Types
    ARRAY = "ARRAY"
    MAP = "MAP"
    STRUCT = "STRUCT"


# Atomic Types
Int32 = DataType.INT32
Int64 = DataType.INT64
Float32 = DataType.FLOAT32
Float64 = DataType.FLOAT64
String = DataType.STRING
Boolean = DataType.BOOLEAN
Byte = DataType.BYTE
Short = DataType.SHORT
Date = DataType.DATE
Timestamp = DataType.TIMESTAMP
Decimal = DataType.DECIMAL
Binary = DataType.BINARY

# Complex Types
Array = DataType.ARRAY
Map = DataType.MAP
Struct = DataType.STRUCT
