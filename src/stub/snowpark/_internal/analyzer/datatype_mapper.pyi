from typing import Any

from snowflake.snowpark._internal.sp_types.sp_data_types import DataType as SPDataType

class DataTypeMapper:
    MILLIS_PER_DAY: Any
    MICROS_PER_MILLIS: int
    @staticmethod
    def to_sql(value, spark_data_type: SPDataType) -> str: ...
    @staticmethod
    def schema_expression(data_type, is_nullable): ...
    @staticmethod
    def to_sql_without_cast(value, data_type): ...
