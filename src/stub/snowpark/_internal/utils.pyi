from enum import Enum
from json import JSONEncoder
from typing import IO, Any, List, Optional, Tuple, Type

QUERY_TAG_TRACEBACK_LIMIT: int
SNOWFLAKE_UNQUOTED_ID_PATTERN: str
SNOWFLAKE_QUOTED_ID_PATTERN: str
SNOWFLAKE_ID_PATTERN: Any
SNOWFLAKE_OBJECT_RE_PATTERN: Any
SNOWFLAKE_STAGE_NAME_PATTERN: Any
TEMP_OBJECT_NAME_PREFIX: str
ALPHANUMERIC: Any

class TempObjectType(Enum):
    TABLE: str
    VIEW: str
    STAGE: str
    FUNCTION: str
    FILE_FORMAT: str

class Utils:
    @staticmethod
    def validate_object_name(name: str): ...
    @staticmethod
    def get_version() -> str: ...
    @staticmethod
    def get_python_version() -> str: ...
    @staticmethod
    def get_connector_version() -> str: ...
    @staticmethod
    def get_os_name() -> str: ...
    @staticmethod
    def get_application_name() -> str: ...
    @staticmethod
    def normalize_stage_location(name: str) -> str: ...
    @staticmethod
    def is_single_quoted(name: str) -> bool: ...
    @staticmethod
    def normalize_local_file(file: str) -> str: ...
    @staticmethod
    def get_local_file_path(file: str) -> str: ...
    @staticmethod
    def get_udf_upload_prefix(udf_name: str) -> str: ...
    @staticmethod
    def random_number() -> int: ...
    @staticmethod
    def generated_py_file_ext() -> Tuple[str, ...]: ...
    @staticmethod
    def zip_file_or_directory_to_stream(
        path: str,
        leading_path: Optional[str] = ...,
        add_init_py: bool = ...,
        ignore_generated_py_file: bool = ...,
    ) -> IO[bytes]: ...
    @staticmethod
    def parse_positional_args_to_list(*inputs) -> List: ...
    @staticmethod
    def calculate_md5(
        path: str,
        chunk_size: int = ...,
        ignore_generated_py_file: bool = ...,
        additional_info: Optional[str] = ...,
    ) -> str: ...
    @staticmethod
    def str_to_enum(value: str, enum_class: Type[Enum], except_str: str) -> Enum: ...
    @staticmethod
    def create_statement_query_tag(skip_levels: int = ...) -> str: ...
    @staticmethod
    def get_stage_file_prefix_length(stage_location: str) -> int: ...
    @staticmethod
    def random_name_for_temp_object(object_type: TempObjectType) -> str: ...
    @staticmethod
    def generate_random_alphanumeric(length: int) -> str: ...

class PythonObjJSONEncoder(JSONEncoder):
    def default(self, value): ...

class _SaveMode(Enum):
    APPEND: str
    OVERWRITE: str
    ERROR_IF_EXISTS: str
    IGNORE: str
