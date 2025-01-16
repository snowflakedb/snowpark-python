from typing import Any, Callable, List, NamedTuple, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.sp_types.types_package import (
    ColumnOrName as ColumnOrName,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.types import DataType as DataType

logger: Any

class UserDefinedFunction:
    func: Any
    name: Any
    def __init__(
        self,
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        name: str,
        is_return_nullable: bool = ...,
    ) -> None: ...
    def __call__(
        self, *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]]
    ) -> Column: ...

class _UDFColumn(NamedTuple):
    datatype: DataType
    name: str

class UDFRegistration:
    session: Any
    def __init__(self, session: snowflake.snowpark.Session) -> None: ...
    def register(
        self,
        func: Callable,
        return_type: Optional[DataType] = ...,
        input_types: Optional[List[DataType]] = ...,
        name: Optional[str] = ...,
        is_permanent: bool = ...,
        stage_location: Optional[str] = ...,
        replace: bool = ...,
        parallel: int = ...,
    ) -> UserDefinedFunction: ...
