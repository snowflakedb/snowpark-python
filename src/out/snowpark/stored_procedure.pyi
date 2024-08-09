from types import ModuleType
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark.types import DataType as DataType

class StoredProcedure:
    func: Any
    name: Any
    def __init__(
        self,
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        name: str,
    ) -> None: ...
    def __call__(
        self, *args: Any, session: Optional[snowflake.snowpark.session.Session] = ...
    ) -> any: ...

class StoredProcedureRegistration:
    def __init__(self, session: snowflake.snowpark.session.Session) -> None: ...
    def describe(
        self, sproc_obj: StoredProcedure
    ) -> snowflake.snowpark.dataframe.DataFrame: ...
    def register(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: Optional[DataType] = ...,
        input_types: Optional[List[DataType]] = ...,
        name: Optional[Union[str, Iterable[str]]] = ...,
        is_permanent: bool = ...,
        stage_location: Optional[str] = ...,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = ...,
        packages: Optional[List[Union[str, ModuleType]]] = ...,
        replace: bool = ...,
        parallel: int = ...,
    ) -> StoredProcedure: ...
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
    ) -> StoredProcedure: ...
