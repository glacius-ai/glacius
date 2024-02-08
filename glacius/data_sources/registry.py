from typing import Any, Dict

from glacius.data_sources import FileSource, RedshiftSource, SnowflakeSource
from glacius.data_sources.file import SourceType

ENUM_TO_SOURCE_CLS = {
    SourceType.SNOWFLAKE.value: SnowflakeSource,
    SourceType.FILE.value: FileSource,
    SourceType.REDSHIFT.value: RedshiftSource,
}


def construct_data_source_instance(source_dict: Dict[str, Any]):
    source_type_enum = SourceType[source_dict["source_type"]]
    data_source_constructor = ENUM_TO_SOURCE_CLS[source_type_enum.value]
    return data_source_constructor.from_dict(source_dict)
