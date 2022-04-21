from typing import Any, Dict, Iterable, Optional, Union

import snowflake.snowpark
from snowflake.snowpark._internal.type_utils import ColumnOrName as ColumnOrName

class DataFrameWriter:
    def __init__(self, dataframe: snowflake.snowpark.dataframe.DataFrame) -> None: ...
    def mode(self, save_mode: str) -> DataFrameWriter: ...
    def save_as_table(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        mode: Optional[str] = ...,
        create_temp_table: bool = ...
    ) -> None: ...
    def copy_into_location(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrName] = ...,
        file_format_name: Optional[str] = ...,
        file_format_type: Optional[str] = ...,
        format_type_options: Optional[Dict[str, str]] = ...,
        header: bool = ...,
        **copy_options: Optional[str]
    ) -> None: ...
    saveAsTable: Any
