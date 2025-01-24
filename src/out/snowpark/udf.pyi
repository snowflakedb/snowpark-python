from types import ModuleType
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.expression import Expression as Expression
from snowflake.snowpark._internal.type_utils import ColumnOrName as ColumnOrName
from snowflake.snowpark.column import Column
from snowflake.snowpark.types import DataType as DataType

class UserDefinedFunction:
    func: Any
    name: Any
    def __init__(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: DataType,
        input_types: List[DataType],
        name: str,
        is_return_nullable: bool = ...,
    ) -> None: ...
    def __call__(
        self, *cols: Union[ColumnOrName, Iterable[ColumnOrName]]
    ) -> Column: ...

class UDFRegistration:
    def __init__(self, session: snowflake.snowpark.session.Session) -> None: ...
    def describe(
        self, udf_obj: UserDefinedFunction
    ) -> snowflake.snowpark.dataframe.DataFrame: ...
    def register(
        self,
        func: Callable,
        return_type: Optional[DataType] = ...,
        input_types: Optional[List[DataType]] = ...,
        name: Optional[Union[str, Iterable[str]]] = ...,
        is_permanent: bool = ...,
        stage_location: Optional[str] = ...,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = ...,
        packages: Optional[List[Union[str, ModuleType]]] = ...,
        replace: bool = ...,
        parallel: int = ...,
        max_batch_size: Optional[int] = ...,
        **kwargs
    ) -> UserDefinedFunction: ...
    def register_from_file(
        self,
        file_path: str,
        func_name: str,
        return_type: Optional[DataType] = ...,
        input_types: Optional[List[DataType]] = ...,
        name: Optional[Union[str, Iterable[str]]] = ...,
        is_permanent: bool = ...,
        stage_location: Optional[str] = ...,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = ...,
        packages: Optional[List[Union[str, ModuleType]]] = ...,
        replace: bool = ...,
        parallel: int = ...,
    ) -> UserDefinedFunction: ...
