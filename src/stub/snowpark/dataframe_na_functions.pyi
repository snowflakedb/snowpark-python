from typing import Any, Dict, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.sp_types.types_package import (
    LiteralType as LiteralType,
)
from snowflake.snowpark.types import DataType as DataType

logger: Any

class DataFrameNaFunctions:
    df: Any
    def __init__(self, df: snowflake.snowpark.dataframe.DataFrame) -> None: ...
    def drop(
        self,
        how: str = ...,
        thresh: Optional[int] = ...,
        subset: Optional[Union[str, List[str], Tuple[str, ...]]] = ...,
    ) -> snowflake.snowpark.dataframe.DataFrame: ...
    def fill(
        self,
        value: Union[LiteralType, Dict[str, LiteralType]],
        subset: Optional[Union[str, List[str], Tuple[str, ...]]] = ...,
    ) -> snowflake.snowpark.dataframe.DataFrame: ...
    def replace(
        self,
        to_replace: Union[
            LiteralType,
            List[LiteralType],
            Tuple[LiteralType, ...],
            Dict[LiteralType, LiteralType],
        ],
        value: Optional[
            Union[LiteralType, List[LiteralType], Tuple[LiteralType, ...]]
        ] = ...,
        subset: Optional[Union[str, List[str], Tuple[str, ...]]] = ...,
    ) -> snowflake.snowpark.dataframe.DataFrame: ...
