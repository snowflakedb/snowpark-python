from typing import IO, Any, Dict, List, Optional, Union

from snowflake.connector import SnowflakeConnection as SnowflakeConnection
from snowflake.connector.cursor import ResultMetadata as ResultMetadata
from snowflake.connector.options import pandas as pandas
from snowflake.snowpark._internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import DataType as DataType

logger: Any
PARAM_APPLICATION: str
PARAM_INTERNAL_APPLICATION_NAME: str
PARAM_INTERNAL_APPLICATION_VERSION: str

class ServerConnection:
    class _Decorator:
        @classmethod
        def wrap_exception(cls, func): ...
        @classmethod
        def log_msg_and_telemetry(cls, msg): ...
    def __init__(
        self,
        options: Dict[str, Union[int, str]],
        conn: Optional[SnowflakeConnection] = ...,
    ) -> None: ...
    def close(self) -> None: ...
    def is_closed(self) -> bool: ...
    def get_session_id(self) -> int: ...
    def get_default_database(self) -> Optional[str]: ...
    def get_default_schema(self) -> Optional[str]: ...
    def get_current_database(self) -> Optional[str]: ...
    def get_current_schema(self) -> Optional[str]: ...
    def get_parameter_value(self, parameter_name: str) -> Optional[str]: ...
    @staticmethod
    def convert_result_meta_to_attribute(
        meta: List[ResultMetadata],
    ) -> List[Attribute]: ...
    def get_result_attributes(self, query: str) -> List[Attribute]: ...
    def upload_file(
        self,
        path: str,
        stage_location: str,
        dest_prefix: str = ...,
        parallel: int = ...,
        compress_data: bool = ...,
        source_compression: str = ...,
        overwrite: bool = ...,
    ) -> None: ...
    def upload_stream(
        self,
        input_stream: IO[bytes],
        stage_location: str,
        dest_filename: str,
        dest_prefix: str = ...,
        parallel: int = ...,
        compress_data: bool = ...,
        source_compression: str = ...,
        overwrite: bool = ...,
    ) -> None: ...
    def run_query(
        self, query: str, to_pandas: bool = ..., **kwargs
    ) -> Dict[str, Any]: ...
    def result_set_to_rows(
        self, result_set: List[Any], result_meta: Optional[List[ResultMetadata]] = ...
    ) -> List[Row]: ...
    def execute(
        self, plan: SnowflakePlan, to_pandas: bool = ..., **kwargs
    ) -> Union[List[Row], pandas.DataFrame]: ...
    def get_result_set(
        self, plan: SnowflakePlan, to_pandas: bool = ..., **kwargs
    ) -> Tuple[Union[List[Any], pandas.DataFrame], List[ResultMetadata]]: ...
    def get_result_and_metadata(
        self, plan: SnowflakePlan, **kwargs
    ) -> Tuple[List[Row], List[Attribute]]: ...
    def run_batch_insert(self, query: str, rows: List[Row], **kwargs) -> None: ...
