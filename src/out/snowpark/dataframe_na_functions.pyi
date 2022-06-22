from typing import Any, Dict, Iterable, Optional, Union

import snowflake.snowpark
from snowflake.snowpark._internal.type_utils import LiteralType as LiteralType
from snowflake.snowpark.types import DataType as DataType

logger: Any

class DataFrameNaFunctions:
    def __init__(self, df: snowflake.snowpark.dataframe.DataFrame) -> None: ...
    def drop(
        self,
        how: str = ...,
        thresh: Optional[int] = ...,
        subset: Optional[Iterable[str]] = ...,
    ) -> snowflake.snowpark.dataframe.DataFrame: ...
    def fill(
        self,
        value: Union[LiteralType, Dict[str, LiteralType]],
        subset: Optional[Iterable[str]] = ...,
    ) -> snowflake.snowpark.dataframe.DataFrame: ...
    def replace(
        self,
        to_replace: Union[
            LiteralType, Iterable[LiteralType], Dict[LiteralType, LiteralType]
        ],
        value: Optional[Iterable[LiteralType]] = ...,
        subset: Optional[Iterable[str]] = ...,
    ) -> snowflake.snowpark.dataframe.DataFrame: ...
