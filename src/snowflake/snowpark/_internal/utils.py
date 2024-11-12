#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import array
import contextlib
import datetime
import decimal
import functools
import hashlib
import importlib
import io
import logging
import os
import platform
import random
import re
import string
import threading
import traceback
import zipfile
from enum import Enum
from functools import lru_cache
from json import JSONEncoder
from random import choice
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import snowflake.snowpark
from snowflake.connector.cursor import ResultMetadata, SnowflakeCursor
from snowflake.connector.description import OPERATING_SYSTEM, PLATFORM
from snowflake.connector.options import (
    MissingOptionalDependency,
    ModuleLikeObject,
    pandas,
)
from snowflake.connector.version import VERSION as connector_version
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.row import Row
from snowflake.snowpark.version import VERSION as snowpark_version

if TYPE_CHECKING:
    try:
        from snowflake.connector.cursor import ResultMetadataV2
    except ImportError:
        ResultMetadataV2 = ResultMetadata

STAGE_PREFIX = "@"
SNOWURL_PREFIX = "snow://"
SNOWFLAKE_PATH_PREFIXES = [
    STAGE_PREFIX,
    SNOWURL_PREFIX,
]

# Scala uses 3 but this can be larger. Consider allowing users to configure it.
QUERY_TAG_TRACEBACK_LIMIT = 3

# https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
SNOWFLAKE_UNQUOTED_ID_PATTERN = r"([a-zA-Z_][\w\$]{0,255})"
SNOWFLAKE_QUOTED_ID_PATTERN = '("([^"]|""){1,255}")'
SNOWFLAKE_ID_PATTERN = (
    f"({SNOWFLAKE_UNQUOTED_ID_PATTERN}|{SNOWFLAKE_QUOTED_ID_PATTERN})"
)
SNOWFLAKE_CASE_INSENSITIVE_QUOTED_ID_PATTERN = r'("([A-Z_][A-Z0-9_\$]{0,255})")'
SNOWFLAKE_CASE_INSENSITIVE_UNQUOTED_SUFFIX_PATTERN = r"([a-zA-Z0-9_\$]{0,255})"

# Valid name can be:
#   identifier
#   identifier.identifier
#   identifier.identifier.identifier
#   identifier..identifier
SNOWFLAKE_OBJECT_RE_PATTERN = re.compile(
    f"^(({SNOWFLAKE_ID_PATTERN}\\.){{0,2}}|({SNOWFLAKE_ID_PATTERN}\\.\\.)){SNOWFLAKE_ID_PATTERN}$"
)

SNOWFLAKE_CASE_INSENSITIVE_QUOTED_ID_RE_PATTERN = re.compile(
    SNOWFLAKE_CASE_INSENSITIVE_QUOTED_ID_PATTERN
)
SNOWFLAKE_CASE_INSENSITIVE_UNQUOTED_SUFFIX_RE_PATTERN = re.compile(
    SNOWFLAKE_CASE_INSENSITIVE_UNQUOTED_SUFFIX_PATTERN
)

# "%?" is for table stage
SNOWFLAKE_STAGE_NAME_PATTERN = f"(%?{SNOWFLAKE_ID_PATTERN})"

# Prefix for allowed temp object names in stored proc
TEMP_OBJECT_NAME_PREFIX = "SNOWPARK_TEMP_"
ALPHANUMERIC = string.digits + string.ascii_lowercase

# select and CTE (https://docs.snowflake.com/en/sql-reference/constructs/with.html) are select statements in Snowflake.
SNOWFLAKE_SELECT_SQL_PREFIX_PATTERN = re.compile(
    r"^(\s|\()*(select|with)", re.IGNORECASE
)

# Anonymous stored procedures: https://docs.snowflake.com/en/sql-reference/sql/call-with
SNOWFLAKE_ANONYMOUS_CALL_WITH_PATTERN = re.compile(
    r"^\s*with\s+\w+\s+as\s+procedure", re.IGNORECASE
)

# A set of widely-used packages,
# whose names in pypi are different from their package name
PACKAGE_NAME_TO_MODULE_NAME_MAP = {
    "absl-py": "absl",
    "async-timeout": "async_timeout",
    "attrs": "attr",
    "brotlipy": "brotli",
    "charset-normalizer": "charset_normalizer",
    "google-auth": "google.auth",
    "google-auth-oauthlib": "google_auth_oauthlib",
    "google-pasta": "pasta",
    "grpcio": "grpc",
    "importlib-metadata": "importlib_metadata",
    "ncurses": "curses",
    "py-xgboost": "xgboost",
    "pyasn1-modules": "pyasn1_modules",
    "pyjwt": "jwt",
    "pyopenssl": "OpenSSL",
    "pysocks": "socks",
    "python-dateutil": "dateutil",
    "python-flatbuffers": "flatbuffers",
    "pytorch": "torch",
    "pyyaml": "yaml",
    "requests-oauthlib": "requests_oauthlib",
    "scikit-learn": "sklearn",
    "sqlite": "sqlite3",
    "tensorboard-plugin-wit": "tensorboard_plugin_wit",
    "tensorflow-estimator": "tensorflow_estimator",
    "typing-extensions": "typing_extensions",
}

MODULE_NAME_TO_PACKAGE_NAME_MAP = {
    v: k for k, v in PACKAGE_NAME_TO_MODULE_NAME_MAP.items()
}

GENERATED_PY_FILE_EXT = (".pyc", ".pyo", ".pyd", ".pyi")

INFER_SCHEMA_FORMAT_TYPES = ("PARQUET", "ORC", "AVRO", "JSON", "CSV")

COPY_INTO_TABLE_COPY_OPTIONS = {
    "ON_ERROR",
    "SIZE_LIMIT",
    "PURGE",
    "RETURN_FAILED_ONLY",
    "MATCH_BY_COLUMN_NAME",
    "ENFORCE_LENGTH",
    "TRUNCATECOLUMNS",
    "FORCE",
    "LOAD_UNCERTAIN_FILES",
}

COPY_INTO_LOCATION_COPY_OPTIONS = {
    "OVERWRITE",
    "SINGLE",
    "MAX_FILE_SIZE",
    "INCLUDE_QUERY_ID",
    "DETAILED_OUTPUT",
}

NON_FORMAT_TYPE_OPTIONS = {
    "PATTERN",
    "VALIDATION_MODE",
    "FILE_FORMAT",
    "FORMAT_NAME",
    "FILES",
    # The following are not copy into SQL command options but client side options.
    "INFER_SCHEMA",
    "INFER_SCHEMA_OPTIONS",
    "FORMAT_TYPE_OPTIONS",
    "TARGET_COLUMNS",
    "TRANSFORMATIONS",
    "COPY_OPTIONS",
}

QUERY_TAG_STRING = "QUERY_TAG"
SKIP_LEVELS_TWO = (
    2  # limit traceback to return up to 2 stack trace entries from traceback object tb
)
SKIP_LEVELS_THREE = (
    3  # limit traceback to return up to 3 stack trace entries from traceback object tb
)
TEMPORARY_STRING = "TEMPORARY"
SCOPED_TEMPORARY_STRING = "SCOPED TEMPORARY"

SUPPORTED_TABLE_TYPES = ["temp", "temporary", "transient"]

PIVOT_VALUES_NONE_OR_DATAFRAME_WARNING = (
    "Calling pivot() with the `value` parameter set to None or to a Snowpark "
    + "DataFrame is in private preview since v1.15.0. Do not use this feature "
    + "in production."
)
PIVOT_DEFAULT_ON_NULL_WARNING = (
    "Calling pivot() with a non-None value for `default_on_null` is in "
    + "private preview since v1.15.0. Do not use this feature in production."
)


class TempObjectType(Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"
    STAGE = "STAGE"
    FUNCTION = "FUNCTION"
    FILE_FORMAT = "FILE_FORMAT"
    QUERY_TAG = "QUERY_TAG"
    COLUMN = "COLUMN"
    PROCEDURE = "PROCEDURE"
    TABLE_FUNCTION = "TABLE_FUNCTION"
    DYNAMIC_TABLE = "DYNAMIC_TABLE"
    AGGREGATE_FUNCTION = "AGGREGATE_FUNCTION"
    CTE = "CTE"


# More info about all allowed aliases here:
# https://docs.snowflake.com/en/sql-reference/functions-date-time#label-supported-date-time-parts

DATETIME_PART_TO_ALIASES = {
    "year": {"year", "y", "yy", "yyy", "yyyy", "yr", "years", "yrs"},
    "quarter": {"quarter", "q", "qtr", "qtrs", "quarters"},
    "month": {"month", "mm", "mon", "mons", "months"},
    "week": {"week", "w", "wk", "weekofyear", "woy", "wy"},
    "day": {"day", "d", "dd", "days", "dayofmonth"},
    "hour": {"hour", "h", "hh", "hr", "hours", "hrs"},
    "minute": {"minute", "m", "mi", "min", "minutes", "mins"},
    "second": {"second", "s", "sec", "seconds", "secs"},
    "millisecond": {"millisecond", "ms", "msec", "milliseconds"},
    "microsecond": {"microsecond", "us", "usec", "microseconds"},
    "nanosecond": {
        "nanosecond",
        "ns",
        "nsec",
        "nanosec",
        "nsecond",
        "nanoseconds",
        "nanosecs",
        "nseconds",
    },
    "dayofweek": {"dayofweek", "weekday", "dow", "dw"},
    "dayofweekiso": {"dayofweekiso", "weekday_iso", "dow_iso", "dw_iso"},
    "dayofyear": {"dayofyear", "yearday", "doy", "dy"},
    "weekiso": {"weekiso", "week_iso", "weekofyeariso", "weekofyear_iso"},
    "yearofweek": {"yearofweek"},
    "yearofweekiso": {"yearofweekiso"},
    "epoch_second": {"epoch_second", "epoch", "epoch_seconds"},
    "epoch_millisecond": {"epoch_millisecond", "epoch_milliseconds"},
    "epoch_microsecond": {"epoch_microsecond", "epoch_microseconds"},
    "epoch_nanosecond": {"epoch_nanosecond", "epoch_nanoseconds"},
    "timezone_hour": {"timezone_hour", "tzh"},
    "timezone_minute": {"timezone_minute", "tzm"},
}

DATETIME_PARTS = set(DATETIME_PART_TO_ALIASES.keys())
ALIASES_TO_DATETIME_PART = {
    v: k for k, l in DATETIME_PART_TO_ALIASES.items() for v in l
}
DATETIME_ALIASES = set(ALIASES_TO_DATETIME_PART.keys())


def unalias_datetime_part(part):
    lowered_part = part.lower()
    if lowered_part in DATETIME_ALIASES:
        return ALIASES_TO_DATETIME_PART[lowered_part]
    else:
        raise ValueError(f"{part} is not a recognized date or time part.")


def parse_duration_string(duration: str) -> Tuple[int, str]:
    length, unit = duration.split(" ")
    length = int(length)
    unit = unalias_datetime_part(unit)
    return length, unit


def validate_object_name(name: str):
    if not SNOWFLAKE_OBJECT_RE_PATTERN.match(name):
        raise SnowparkClientExceptionMessages.GENERAL_INVALID_OBJECT_NAME(name)


@lru_cache
def get_version() -> str:
    return ".".join([str(d) for d in snowpark_version if d is not None])


@lru_cache
def get_python_version() -> str:
    return platform.python_version()


@lru_cache
def get_connector_version() -> str:
    return ".".join([str(d) for d in connector_version if d is not None])


@lru_cache
def get_os_name() -> str:
    return platform.system()


@lru_cache
def get_application_name() -> str:
    return "PythonSnowpark"


def is_single_quoted(name: str) -> bool:
    return name.startswith("'") and name.endswith("'")


def is_snowflake_quoted_id_case_insensitive(name: str) -> bool:
    return SNOWFLAKE_CASE_INSENSITIVE_QUOTED_ID_RE_PATTERN.fullmatch(name) is not None


def is_snowflake_unquoted_suffix_case_insensitive(name: str) -> bool:
    return (
        SNOWFLAKE_CASE_INSENSITIVE_UNQUOTED_SUFFIX_RE_PATTERN.fullmatch(name)
        is not None
    )


def unwrap_single_quote(name: str) -> str:
    new_name = name.strip()
    if is_single_quoted(new_name):
        new_name = new_name[1:-1]
    new_name = new_name.replace("\\'", "'")
    return new_name


def escape_single_quotes(input_str):
    return input_str.replace("'", r"\'")


def is_sql_select_statement(sql: str) -> bool:
    return (
        SNOWFLAKE_SELECT_SQL_PREFIX_PATTERN.match(sql) is not None
        and SNOWFLAKE_ANONYMOUS_CALL_WITH_PATTERN.match(sql) is None
    )


def normalize_path(path: str, is_local: bool) -> str:
    """
    Get a normalized path of a local file or remote stage location for PUT/GET commands.
    If there are any special characters including spaces in the path, it needs to be
    quoted with single quote. For example, 'file:///tmp/load data' for a path containing
    a directory named "load data". Therefore, if `path` is already wrapped by single quotes,
    we do nothing.
    """
    prefixes = ["file://"] if is_local else SNOWFLAKE_PATH_PREFIXES
    if is_single_quoted(path):
        return path
    if is_local and OPERATING_SYSTEM == "Windows":
        path = path.replace("\\", "/")
    path = path.strip().replace("'", "\\'")
    if not any(path.startswith(prefix) for prefix in prefixes):
        path = f"{prefixes[0]}{path}"
    return f"'{path}'"


def warn_session_config_update_in_multithreaded_mode(
    config: str, thread_safe_mode_enabled: bool
) -> None:
    if not thread_safe_mode_enabled:
        return

    if threading.active_count() > 1:
        logger.warning(
            "You might have more than one threads sharing the Session object trying to update "
            f"{config}. Updating this while other tasks are running can potentially cause "
            "unexpected behavior. Please update the session configuration before starting the threads."
        )


def normalize_remote_file_or_dir(name: str) -> str:
    return normalize_path(name, is_local=False)


def normalize_local_file(file: str) -> str:
    return normalize_path(file, is_local=True)


def split_path(path: str) -> Tuple[str, str]:
    """Split a file path into directory and file name."""
    path = unwrap_single_quote(path)
    return path.rsplit("/", maxsplit=1)


def unwrap_stage_location_single_quote(name: str) -> str:
    new_name = unwrap_single_quote(name)
    if any(new_name.startswith(prefix) for prefix in SNOWFLAKE_PATH_PREFIXES):
        return new_name
    return f"{STAGE_PREFIX}{new_name}"


def get_local_file_path(file: str) -> str:
    trim_file = file.strip()
    if is_single_quoted(trim_file):
        trim_file = trim_file[1:-1]  # remove the pair of single quotes
    if trim_file.startswith("file://"):
        return trim_file[7:]  # remove "file://"
    return trim_file


def get_udf_upload_prefix(udf_name: str) -> str:
    """Get the valid stage prefix when uploading a UDF."""
    if re.match("[\\w]+$", udf_name):
        return udf_name
    else:
        return "{}_{}".format(re.sub("\\W", "", udf_name), abs(hash(udf_name)))


def random_number() -> int:
    """Get a random unsigned integer."""
    return random.randint(0, 2**31)


@contextlib.contextmanager
def zip_file_or_directory_to_stream(
    path: str,
    leading_path: Optional[str] = None,
    add_init_py: bool = False,
    ignore_generated_py_file: bool = True,
) -> IO[bytes]:
    """Compresses the file or directory as a zip file to a binary stream.
    Args:
        path: The absolute path to a file or directory.
        leading_path: This argument is used to determine where directory should
            start in the zip file. Basically, this argument works as the role
            of `start` argument in os.path.relpath(path, start), i.e.,
            absolute path = [leading path]/[relative path]. For example,
            when the path is "/tmp/dir1/dir2/test.py", and the leading path
            is "/tmp/dir1", the generated filesystem structure in the zip file
            will be "dir2/test.py". The leading path will compose a namespace package
            that is used for zipimport on the server side.
        ignore_generated_py_file: Whether to ignore some generated python files
            in the directory.

    Returns:
        A byte stream.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} is not found")
    if leading_path and not path.startswith(leading_path):
        raise ValueError(f"{leading_path} doesn't lead to {path}")
    # if leading_path is not provided, just use the parent path,
    # and the compression will start from the parent directory
    start_path = leading_path if leading_path else os.path.join(path, "..")

    input_stream = io.BytesIO()
    with zipfile.ZipFile(
        input_stream, mode="w", compression=zipfile.ZIP_DEFLATED
    ) as zf:
        # Write the folders on the leading path to the zip file to build a namespace package
        cur_path = os.path.dirname(path)
        while os.path.realpath(cur_path) != os.path.realpath(start_path):
            # according to .zip file format specification, only / is valid
            zf.writestr(f"{os.path.relpath(cur_path, start_path)}/", "")
            cur_path = os.path.dirname(cur_path)

        if os.path.isdir(path):
            for dirname, _, files in os.walk(path):
                # ignore __pycache__
                if ignore_generated_py_file and "__pycache__" in dirname:
                    continue
                zf.write(dirname, os.path.relpath(dirname, start_path))
                for file in files:
                    # ignore generated python files
                    if ignore_generated_py_file and file.endswith(
                        GENERATED_PY_FILE_EXT
                    ):
                        continue
                    filename = os.path.join(dirname, file)
                    zf.write(filename, os.path.relpath(filename, start_path))
        else:
            zf.write(path, os.path.relpath(path, start_path))

    yield input_stream
    input_stream.close()


def parse_positional_args_to_list(*inputs: Any) -> List:
    """Convert the positional arguments to a list."""
    if len(inputs) == 1:
        return (
            [*inputs[0]] if isinstance(inputs[0], (list, tuple, set)) else [inputs[0]]
        )
    else:
        return [*inputs]


def parse_positional_args_to_list_variadic(*inputs: Any) -> Tuple[List, bool]:
    """Convert the positional arguments to a list, indicating whether to treat the argument list as a variadic list."""
    if len(inputs) == 1 and isinstance(inputs[0], (list, tuple, set)):
        return ([*inputs[0]], False)
    else:
        return ([*inputs], True)


def _hash_file(
    hash_algo: "hashlib._hashlib.HASH",
    path: str,
    chunk_size: int,
    whole_file_hash: bool,
):
    """
    Reads from a file and updates the given hash algorithm with the read text.

    Args:
        hash_algo: The hash algorithm to updated.
        path: The path to the file to be read.
        chunk_size: How much of the file to read at a time.
        whole_file_hash: When True the whole file is hashed rather than stopping after the first chunk.
    """
    with open(path, "rb") as f:
        data = f.read(chunk_size)
        hash_algo.update(data)
        while data and whole_file_hash:
            data = f.read(chunk_size)
            hash_algo.update(data)


def calculate_checksum(
    path: str,
    chunk_size: int = 8192,
    ignore_generated_py_file: bool = True,
    additional_info: Optional[str] = None,
    algorithm: str = "sha256",
    whole_file_hash: bool = False,
) -> str:
    """Calculates the checksum of a file or a directory.

    Args:
        path: the path to a local file or directory.
            If it points to a file, we read a small chunk from the file and
            calculate the checksum based on it.
            If it points to a directory, the names of all files and subdirectories
            in this directory will also be included for checksum computation.
        chunk_size: The size in byte we will read from the file/directory for
            checksum computation.
        ignore_generated_py_file: Whether to ignore some generated python files
            in the directory.
        additional_info: Any additional information we might want to include
            for checksum computation.
        algorithm: the hash algorithm.
        whole_file_hash: When set to True the files will be completely read while hashing.

    Returns:
        The result checksum.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} is not found")

    hash_algo = hashlib.new(algorithm)
    if os.path.isfile(path):
        _hash_file(hash_algo, path, chunk_size, whole_file_hash)
    elif os.path.isdir(path):
        current_size = 0
        for dirname, dirs, files in os.walk(path):
            # ignore __pycache__
            if ignore_generated_py_file and "__pycache__" in dirname:
                continue
            # sort dirs and files so the result is consistent across different os
            for dir in sorted(dirs):
                if ignore_generated_py_file and dir == "__pycache__":
                    continue
                hash_algo.update(dir.encode("utf8"))
            for file in sorted(files):
                # ignore generated python files
                if ignore_generated_py_file and file.endswith(GENERATED_PY_FILE_EXT):
                    continue

                hash_algo.update(file.encode("utf8"))

                filename = os.path.join(dirname, file)
                file_size = os.path.getsize(filename)

                if whole_file_hash:
                    _hash_file(hash_algo, filename, chunk_size, whole_file_hash)
                    current_size += file_size
                elif current_size < chunk_size:
                    read_size = min(file_size, chunk_size - current_size)
                    current_size += read_size
                    _hash_file(hash_algo, filename, read_size, False)
    else:
        raise ValueError(f"{algorithm} can only be calculated for a file or directory")

    if additional_info:
        hash_algo.update(additional_info.encode("utf8"))

    return hash_algo.hexdigest()


def str_to_enum(value: str, enum_class: Type[Enum], except_str: str) -> Enum:
    try:
        return enum_class(value)
    except ValueError:
        raise ValueError(
            f"{except_str} must be one of {', '.join([e.value for e in enum_class])}"
        )


def create_statement_query_tag(skip_levels: int = 0) -> str:
    stack = traceback.format_stack(limit=QUERY_TAG_TRACEBACK_LIMIT + skip_levels)
    return "".join(stack[:-skip_levels] if skip_levels else stack)


def create_or_update_statement_params_with_query_tag(
    statement_params: Optional[Dict[str, str]] = None,
    exists_session_query_tag: Optional[str] = None,
    skip_levels: int = 0,
) -> Dict[str, str]:
    if exists_session_query_tag or (
        statement_params and QUERY_TAG_STRING in statement_params
    ):
        return statement_params

    ret = statement_params or {}
    # as create_statement_query_tag is called by the method, skip_levels needs to +1 to skip the current call
    ret[QUERY_TAG_STRING] = create_statement_query_tag(skip_levels + 1)
    return ret


def get_stage_file_prefix_length(stage_location: str) -> int:
    normalized = unwrap_stage_location_single_quote(stage_location)
    if not normalized.endswith("/"):
        normalized = f"{normalized}/"

    # Remove the first three characters from @~/...
    if normalized.startswith(f"{STAGE_PREFIX}~"):
        return len(normalized) - 3

    is_quoted = False
    for i, c in enumerate(normalized):
        if c == '"':
            is_quoted = not is_quoted
        elif c == "/" and not is_quoted:
            # Find the first unquoted '/', then the stage name is before it,
            # the path is after it
            full_stage_name = normalized[:i]
            path = normalized[i + 1 :]
            # Find the last match of the first group, which should be the stage name.
            # If not found, the stage name should be invalid
            res = re.findall(SNOWFLAKE_STAGE_NAME_PATTERN, full_stage_name)
            if not res:
                break
            stage_name = res[-1][0]
            # For a table stage, stage name is not in the prefix,
            # so the prefix is path. Otherwise, the prefix is stageName + "/" + path
            return (
                len(path)
                if stage_name.startswith("%")
                else len(path) + len(stage_name.strip('"')) + 1
            )

    raise ValueError(f"Invalid stage {stage_location}")


def is_in_stored_procedure():
    return PLATFORM == "XP"


def random_name_for_temp_object(object_type: TempObjectType) -> str:
    return f"{TEMP_OBJECT_NAME_PREFIX}{object_type.value}_{generate_random_alphanumeric().upper()}"


def generate_random_alphanumeric(length: int = 10) -> str:
    return "".join(choice(ALPHANUMERIC) for _ in range(length))


def column_to_bool(col_):
    """A replacement to bool(col_) to check if ``col_`` is None or Empty.

    ``Column.__bool__` raises an exception to remind users to use &, |, ~ instead of and, or, not for logical operations.
    The side-effect is the implicit call like ``if col_`` also raises an exception.
    Our internal code sometimes needs to check an input column is None, "", or []. So this method will help it by writeint ``if column_to_bool(col_): ...``
    """
    if isinstance(col_, snowflake.snowpark.Column):
        return True
    return bool(col_)


def result_set_to_rows(
    result_set: List[Any],
    result_meta: Optional[Union[List[ResultMetadata], List["ResultMetadataV2"]]] = None,
    case_sensitive: bool = True,
) -> List[Row]:
    col_names = [col.name for col in result_meta] if result_meta else None
    rows = []
    row_struct = Row
    if col_names:
        row_struct = (
            Row._builder.build(*col_names).set_case_sensitive(case_sensitive).to_row()
        )
    for data in result_set:
        if data is None:
            raise ValueError("Result returned from Python connector is None")
        row = row_struct(*data)
        rows.append(row)
    return rows


def result_set_to_iter(
    result_set: SnowflakeCursor,
    result_meta: Optional[List[ResultMetadata]] = None,
    case_sensitive: bool = True,
) -> Iterator[Row]:
    col_names = [col.name for col in result_meta] if result_meta else None
    row_struct = Row
    if col_names:
        row_struct = (
            Row._builder.build(*col_names).set_case_sensitive(case_sensitive).to_row()
        )
    for data in result_set:
        if data is None:
            raise ValueError("Result returned from Python connector is None")
        row = row_struct(*data)
        yield row


class PythonObjJSONEncoder(JSONEncoder):
    """Converts common Python objects to json serializable objects."""

    def default(self, value):
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        elif isinstance(value, decimal.Decimal):
            return float(value)
        elif isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return value.isoformat()
        elif isinstance(value, array.array):
            return value.tolist()
        else:
            return super().default(value)


logger = logging.getLogger("snowflake.snowpark")


class WarningHelper:
    def __init__(self, warning_times: int) -> None:
        self.warning_times = warning_times
        self.count = 0

    def warning(self, text: str) -> None:
        if self.count < self.warning_times:
            logger.warning(text)
        self.count += 1


# TODO: SNOW-1720855: Remove DummyRLock and DummyThreadLocal after the rollout
class DummyRLock:
    """This is a dummy lock that is used in place of threading.Rlock when multithreading is
    disabled."""

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def acquire(self, *args, **kwargs):
        pass  # pragma: no cover

    def release(self, *args, **kwargs):
        pass  # pragma: no cover


class DummyThreadLocal:
    """This is a dummy thread local class that is used in place of threading.local when
    multithreading is disabled."""

    pass


def create_thread_local(
    thread_safe_session_enabled: bool,
) -> Union[threading.local, DummyThreadLocal]:
    if thread_safe_session_enabled:
        return threading.local()
    return DummyThreadLocal()


def create_rlock(
    thread_safe_session_enabled: bool,
) -> Union[threading.RLock, DummyRLock]:
    if thread_safe_session_enabled:
        return threading.RLock()
    return DummyRLock()


warning_dict: Dict[str, WarningHelper] = {}


def warning(name: str, text: str, warning_times: int = 1) -> None:
    if name not in warning_dict:
        warning_dict[name] = WarningHelper(warning_times)
    warning_dict[name].warning(text)


# TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
def infer_ast_enabled_from_global_sessions(func: Callable) -> bool:  # pragma: no cover
    session = None
    try:
        # Multiple default session attempts:
        session = snowflake.snowpark.session._get_sandbox_conditional_active_session(
            None
        )
        assert session is not None
    except (
        snowflake.snowpark.exceptions.SnowparkSessionException,
        AssertionError,
    ):
        # Use modin session retrieval first, as it supports multiple sessions.
        # Expect this to fail if modin was not installed, for Python 3.8, ... but that's ok.
        try:
            import modin.pandas as pd

            session = pd.session
        except Exception as e:  # noqa: F841
            try:
                # Get from default session.
                from snowflake.snowpark.context import get_active_session

                session = get_active_session()
            except Exception as e:  # noqa: F841
                pass
    finally:
        if session is None:
            logging.debug(
                f"Could not retrieve default session "
                f"for function {func.__qualname__}, capturing AST by default."
            )
            # session has not been created yet. To not lose information, always encode AST.
            return True  # noqa: B012
        else:
            return session.ast_enabled  # noqa: B012


def publicapi(func) -> Callable:
    """decorator to safeguard public APIs with global feature flags."""

    # TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
    @functools.wraps(func)
    def func_call_wrapper(*args, **kwargs):  # pragma: no cover
        # warning(func.__qualname__, warning_text)

        # Handle AST encoding, by modifying default behavior.
        # If a function supports AST encoding, it must have a parameter _emit_ast.
        # If now _emit_ast is passed as part of kwargs (we do not allow for the positional syntax!)
        # then we use this value directly. If not, but the function supports _emit_ast,
        # we override _emit_ast with the session parameter.
        if "_emit_ast" in func.__code__.co_varnames and "_emit_ast" not in kwargs:
            # No arguments, or single argument with function.
            if len(args) == 0 or (len(args) == 1 and isinstance(args[0], Callable)):
                if func.__name__ in {
                    "udf",
                    "udtf",
                    "udaf",
                    "pandas_udf",
                    "pandas_udtf",
                    "sproc",
                }:
                    session = kwargs.get("session")
                    # Lookup session directly as in implementation of these decorators.
                    session = snowflake.snowpark.session._get_sandbox_conditional_active_session(
                        session
                    )
                    # If session is None, do nothing (i.e., keep encoding AST).
                    # This happens when the decorator is called before a session is started.
                    if session is not None:
                        kwargs["_emit_ast"] = session.ast_enabled
                # Function passed fully with kwargs only (i.e., not a method - self will always be passed positionally)
                elif len(kwargs) != 0:
                    # Check if one of the kwargs holds a session object. If so, retrieve AST enabled from there.
                    session_vars = [
                        var
                        for var in kwargs.values()
                        if isinstance(var, snowflake.snowpark.session.Session)
                    ]
                    if session_vars:
                        kwargs["_emit_ast"] = session_vars[0].ast_enabled
                    else:
                        kwargs["_emit_ast"] = infer_ast_enabled_from_global_sessions(
                            func
                        )
            elif isinstance(args[0], snowflake.snowpark.dataframe.DataFrame):
                # special case: __init__ called, self._session is then not initialized yet.
                if func.__qualname__.endswith(".__init__"):
                    # Try to find a session argument.
                    session_args = [
                        arg
                        for arg in args
                        if isinstance(arg, snowflake.snowpark.session.Session)
                    ]
                    assert (
                        len(session_args) != 0
                    ), f"{func.__qualname__} must have at least one session arg."
                    kwargs["_emit_ast"] = session_args[0].ast_enabled
                else:
                    kwargs["_emit_ast"] = args[0]._session.ast_enabled
            elif isinstance(
                args[0], snowflake.snowpark.dataframe_reader.DataFrameReader
            ):
                if func.__qualname__.endswith(".__init__"):
                    assert isinstance(
                        args[1], snowflake.snowpark.session.Session
                    ), f"{func.__qualname__} second arg must be session."
                    kwargs["_emit_ast"] = args[1].ast_enabled
                else:
                    kwargs["_emit_ast"] = args[0]._session.ast_enabled
            elif isinstance(
                args[0], snowflake.snowpark.dataframe_writer.DataFrameWriter
            ):
                if func.__qualname__.endswith(".__init__"):
                    assert isinstance(
                        args[1], snowflake.snowpark.DataFrame
                    ), f"{func.__qualname__} second arg must be dataframe."
                    kwargs["_emit_ast"] = args[1]._session.ast_enabled
                else:
                    kwargs["_emit_ast"] = args[0]._dataframe._session.ast_enabled
            elif isinstance(
                args[0],
                (
                    snowflake.snowpark.dataframe_stat_functions.DataFrameStatFunctions,
                    snowflake.snowpark.dataframe_analytics_functions.DataFrameAnalyticsFunctions,
                    snowflake.snowpark.dataframe_na_functions.DataFrameNaFunctions,
                ),
            ):
                kwargs["_emit_ast"] = args[0]._dataframe._session.ast_enabled
            elif hasattr(args[0], "_session") and args[0]._session is not None:
                kwargs["_emit_ast"] = args[0]._session.ast_enabled
            elif isinstance(args[0], snowflake.snowpark.session.Session):
                kwargs["_emit_ast"] = args[0].ast_enabled
            elif isinstance(
                args[0],
                snowflake.snowpark.relational_grouped_dataframe.RelationalGroupedDataFrame,
            ):
                kwargs["_emit_ast"] = args[0]._df._session.ast_enabled
            else:
                kwargs["_emit_ast"] = infer_ast_enabled_from_global_sessions(func)

        # TODO: Could modify internal docstring to display that users should not modify the _emit_ast parameter.

        return func(*args, **kwargs)

    return func_call_wrapper


def func_decorator(
    decorator_type: Literal["deprecated", "experimental", "in private preview"],
    *,
    version: str,
    extra_warning_text: str,
    extra_doc_string: str,
) -> Callable:
    def wrapper(func):
        warning_text = (
            f"{func.__qualname__}() is {decorator_type} since {version}. "
            f"{'Do not use it in production. ' if decorator_type in ('experimental', 'in private preview') else ''}"
            f"{extra_warning_text}"
        )
        doc_string_text = f"This function or method is {decorator_type} since {version}. {extra_doc_string} \n\n"
        func.__doc__ = f"{func.__doc__ or ''}\n\n{' '*8}{doc_string_text}\n"

        @functools.wraps(func)
        def func_call_wrapper(*args, **kwargs):
            warning(func.__qualname__, warning_text)
            return func(*args, **kwargs)

        return func_call_wrapper

    return wrapper


def param_decorator(
    decorator_type: Literal["deprecated", "experimental", "in private preview"],
    *,
    version: str,
) -> Callable:
    def wrapper(param_setter_function):
        warning_text = (
            f"Parameter {param_setter_function.__name__} is {decorator_type} since {version}. "
            f"{'Do not use it in production. ' if decorator_type in ('experimental', 'in private preview') else ''}"
        )

        @functools.wraps(param_setter_function)
        def func_call_wrapper(*args, **kwargs):
            warning(param_setter_function.__name__, warning_text)
            return param_setter_function(*args, **kwargs)

        return func_call_wrapper

    return wrapper


def deprecated(
    *, version: str, extra_warning_text: str = "", extra_doc_string: str = ""
) -> Callable:
    return func_decorator(
        "deprecated",
        version=version,
        extra_warning_text=extra_warning_text,
        extra_doc_string=extra_doc_string,
    )


def experimental(
    *, version: str, extra_warning_text: str = "", extra_doc_string: str = ""
) -> Callable:
    return func_decorator(
        "experimental",
        version=version,
        extra_warning_text=extra_warning_text,
        extra_doc_string=extra_doc_string,
    )


def experimental_parameter(*, version: str) -> Callable:
    return param_decorator(
        "experimental",
        version=version,
    )


def private_preview(
    *, version: str, extra_warning_text: str = "", extra_doc_string: str = ""
) -> Callable:
    return func_decorator(
        "in private preview",
        version=version,
        extra_warning_text=extra_warning_text,
        extra_doc_string=extra_doc_string,
    )


def get_temp_type_for_object(use_scoped_temp_objects: bool, is_generated: bool) -> str:
    return (
        SCOPED_TEMPORARY_STRING
        if use_scoped_temp_objects and is_generated
        else TEMPORARY_STRING
    )


def check_is_pandas_dataframe_in_to_pandas(result: Any) -> None:
    if not isinstance(result, pandas.DataFrame):
        raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_PANDAS(
            "to_pandas() did not return a pandas DataFrame. "
            "If you use session.sql(...).to_pandas(), the input query can only be a "
            "SELECT statement. Or you can use session.sql(...).collect() to get a "
            "list of Row objects for a non-SELECT statement, then convert it to a "
            "pandas DataFrame."
        )


def _get_options(
    options: Dict[str, Any], allowed_options: Set[str]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Helper method that extracts common logic for getting options for
    COPY INTO TABLE and COPY INTO LOCATION command.
    """
    file_format_type_options = options.get("FORMAT_TYPE_OPTIONS", {})
    copy_options = options.get("COPY_OPTIONS", {})
    for k, v in options.items():
        if k in allowed_options:
            copy_options[k] = v
        elif k not in NON_FORMAT_TYPE_OPTIONS:
            file_format_type_options[k] = v
    return file_format_type_options, copy_options


def get_copy_into_table_options(
    options: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Method that extracts options for COPY INTO TABLE command into file
    format type options and copy options.
    """
    return _get_options(options, COPY_INTO_TABLE_COPY_OPTIONS)


def get_copy_into_location_options(
    options: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Method that extracts options for COPY INTO LOCATION command into file
    format type options and copy options.
    """
    return _get_options(options, COPY_INTO_LOCATION_COPY_OPTIONS)


def get_aliased_option_name(
    key: str,
    alias_map: Dict[str, str],
) -> str:
    """Method that takes a key and an option alias map as arguments and returns
    the aliased key if the key is present in the alias map. Also raise a warning
    if alias key is applied.
    """
    upper_key = key.strip().upper()
    aliased_key = alias_map.get(upper_key, upper_key)
    if aliased_key != upper_key:
        logger.warning(
            f"Option '{key}' is aliased to '{aliased_key}'. You may see unexpected behavior."
            " Please refer to format specific options for more information"
        )

    return aliased_key


def strip_double_quotes_in_like_statement_in_table_name(table_name: str) -> str:
    """
    this function is used by method _table_exists to handle double quotes in table name when calling
    SHOW TABLES LIKE
    """
    if not table_name or len(table_name) < 2:
        return table_name

    # escape double quotes, e.g. users pass """a.b""" as table name:
    # df.write.save_as_table('"""a.b"""', mode="append")
    # and we should call SHOW TABLES LIKE '"a.b"'
    table_name = table_name.replace('""', '"')

    # if table_name == '"a.b"', then we should call SHOW TABLES LIKE 'a.b'
    return table_name[1:-1] if table_name[0] == table_name[-1] == '"' else table_name


def parse_table_name(table_name: str) -> List[str]:
    """
    This function implements the algorithm to parse a table name.

    We parse the table name according to the following rules:
    https://docs.snowflake.com/en/sql-reference/identifiers-syntax

    - Unquoted object identifiers:
        - Start with a letter (A-Z, a-z) or an underscore (“_”).
        - Contain only letters, underscores, decimal digits (0-9), and dollar signs (“$”).
        - Are stored and resolved as uppercase characters (e.g. id is stored and resolved as ID).

    - If you put double quotes around an identifier (e.g. “My identifier with blanks and punctuation.”),
        the following rules apply:
        - The case of the identifier is preserved when storing and resolving the identifier (e.g. "id" is
            stored and resolved as id).
        - The identifier can contain and start with ASCII, extended ASCII, and non-ASCII characters.
    """
    validate_object_name(table_name)
    str_len = len(table_name)
    ret = []

    in_double_quotes = False
    i = 0
    cur_word_start_idx = 0

    while i < str_len:
        cur_char = table_name[i]
        if cur_char == '"':
            if in_double_quotes:
                # we have to check whether this `"` is the ending of a double-quoted identifier
                # or it's an escaping double quote
                # to achieve this, we need to preload one more char
                if i < str_len - 1 and table_name[i + 1] == '"':
                    # two consecutive '"', this is an escaping double quotes
                    # the pointer just keeps moving forward
                    i += 1
                else:
                    # the double quotes indicates the ending of an identifier
                    in_double_quotes = False
                    # it should be followed by a '.' for splitting, or it should reach the end of the str
            else:
                # this is the beginning of another double-quoted identifier
                in_double_quotes = True
        elif cur_char == ".":
            if not in_double_quotes:
                # this dot is to split db.schema.database
                # we concatenate the processed chars into a string
                # and append the string to the return list, and set our cur_word_start_idx to position after the dot
                ret.append(table_name[cur_word_start_idx:i])
                cur_word_start_idx = i + 1
            # else dot is part of the table name
        # else cur_char is part of the name
        i += 1

    ret.append(table_name[cur_word_start_idx:i])
    return ret


EMPTY_STRING = ""
DOUBLE_QUOTE = '"'
# Quoted values may also include newlines, so '.' must match _everything_ within quotes
ALREADY_QUOTED = re.compile('^(".+")$', re.DOTALL)
UNQUOTED_CASE_INSENSITIVE = re.compile("^([_A-Za-z]+[_A-Za-z0-9$]*)$")


def quote_name(name: str, keep_case: bool = False) -> str:
    if ALREADY_QUOTED.match(name):
        return validate_quoted_name(name)
    elif UNQUOTED_CASE_INSENSITIVE.match(name) and not keep_case:
        return DOUBLE_QUOTE + escape_quotes(name.upper()) + DOUBLE_QUOTE
    else:
        return DOUBLE_QUOTE + escape_quotes(name) + DOUBLE_QUOTE


def validate_quoted_name(name: str) -> str:
    if DOUBLE_QUOTE in name[1:-1].replace(DOUBLE_QUOTE + DOUBLE_QUOTE, EMPTY_STRING):
        raise SnowparkClientExceptionMessages.PLAN_ANALYZER_INVALID_IDENTIFIER(name)
    else:
        return name


def escape_quotes(unescaped: str) -> str:
    return unescaped.replace(DOUBLE_QUOTE, DOUBLE_QUOTE + DOUBLE_QUOTE)


should_warn_dynamic_pivot_is_in_private_preview = True


def prepare_pivot_arguments(
    df: "snowflake.snowpark.DataFrame",
    df_name: str,
    pivot_col: "snowflake.snowpark._internal.type_utils.ColumnOrName",
    values: Optional[
        Union[
            Iterable["snowflake.snowpark._internal.type_utils.LiteralType"],
            "snowflake.snowpark.DataFrame",
        ]
    ],
    default_on_null: Optional["snowflake.snowpark._internal.type_utils.LiteralType"],
):
    """
    Prepare dataframe pivot arguments to use in the underlying pivot call.  This includes issuing any applicable
    warnings, ensuring column types and valid arguments.
    Returns:
        DateFrame, pivot column, pivot_values and default_on_null value.
    """
    from snowflake.snowpark.dataframe import DataFrame

    if should_warn_dynamic_pivot_is_in_private_preview:
        if values is None or isinstance(values, DataFrame):
            warning(
                df_name,
                PIVOT_VALUES_NONE_OR_DATAFRAME_WARNING,
            )
        if default_on_null is not None:
            warning(
                df_name,
                PIVOT_DEFAULT_ON_NULL_WARNING,
            )

    if values is not None and not values:
        raise ValueError("values cannot be empty")

    pc = df._convert_cols_to_exprs(f"{df_name}()", pivot_col)

    from snowflake.snowpark._internal.analyzer.expression import Literal, ScalarSubquery
    from snowflake.snowpark.column import Column

    if isinstance(values, Iterable):
        pivot_values = [
            v._expression if isinstance(v, Column) else Literal(v) for v in values
        ]
    else:
        if isinstance(values, DataFrame):
            pivot_values = ScalarSubquery(values._plan)
        else:
            pivot_values = None

        if len(df.queries.get("post_actions", [])) > 0:
            df = df.cache_result()

    if default_on_null is not None:
        default_on_null = (
            default_on_null._expression
            if isinstance(default_on_null, Column)
            else Literal(default_on_null)
        )

    return df, pc, pivot_values, default_on_null


def check_flatten_mode(mode: str) -> None:
    if not isinstance(mode, str) or mode.upper() not in ["OBJECT", "ARRAY", "BOTH"]:
        raise ValueError("mode must be one of ('OBJECT', 'ARRAY', 'BOTH')")


class MissingModin(MissingOptionalDependency):
    """The class is specifically for modin optional dependency."""

    _dep_name = "modin"


def import_or_missing_modin_pandas() -> Tuple[ModuleLikeObject, bool]:
    """This function tries importing the following packages: modin.pandas

    If available it returns modin package with a flag of whether it was imported.
    """
    try:
        modin = importlib.import_module("modin.pandas")
        return modin, True
    except ImportError:
        return MissingModin(), False
