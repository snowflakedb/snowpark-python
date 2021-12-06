from typing import Any, Optional

from snowflake.snowpark._internal.sp_types.sp_data_types import (
    DataType as SPDataType,
    StructType as SPStructType,
)
from snowflake.snowpark.types import DataType as DataType, StructType

def convert_to_sf_type(datatype: DataType) -> str: ...
def snow_type_to_sp_type(datatype: DataType) -> Optional[SPDataType]: ...
def to_sp_struct_type(struct_type: StructType) -> SPStructType: ...
def sp_type_to_snow_type(datatype: SPDataType) -> DataType: ...
def to_snow_struct_type(struct_type: SPStructType) -> StructType: ...

size: Any
dt: Any
ColumnOrName: Any
LiteralType: Any
