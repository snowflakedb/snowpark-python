from typing import Any

from snowflake.snowpark._internal.sp_types.sp_data_types import DataType as DataType

class Attribute:
    name: Any
    datatype: Any
    nullable: Any
    def __init__(self, name: str, datatype: DataType, nullable: bool = ...) -> None: ...
