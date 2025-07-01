#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import array
import contextlib
import datetime
import decimal
import functools
import hashlib
import heapq
import importlib
import io
import itertools
import logging
import os
import platform
import random
import re
import string
import sys
import time
import threading
import traceback
import uuid
import zipfile
from enum import Enum, IntEnum, auto, unique
from functools import lru_cache, wraps
from itertools import count
from json import JSONEncoder
from random import Random
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
    TypeVar,
)

import snowflake.snowpark
from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.connector.cursor import ResultMetadata, SnowflakeCursor
from snowflake.connector.description import OPERATING_SYSTEM, PLATFORM
from snowflake.connector.options import MissingOptionalDependency, ModuleLikeObject
from snowflake.connector.version import VERSION as connector_version
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.row import Row
from snowflake.snowpark.version import VERSION as snowpark_version

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.snowflake_plan import (
        SnowflakePlan,
        QueryLineInterval,
    )
    from snowflake.snowpark._internal.analyzer.select_statement import Selectable

    try:
        from snowflake.connector.cursor import ResultMetadataV2
    except ImportError:
        ResultMetadataV2 = ResultMetadata

_logger = logging.getLogger("snowflake.snowpark")

STAGE_PREFIX = "@"
SNOWURL_PREFIX = "snow://"
RELATIVE_PATH_PREFIX = "/"
SNOWFLAKE_PATH_PREFIXES = [
    STAGE_PREFIX,
    SNOWURL_PREFIX,
]
SNOWFLAKE_PATH_PREFIXES_FOR_GET = SNOWFLAKE_PATH_PREFIXES + [
    RELATIVE_PATH_PREFIX,
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
    "ENFORCE_EXISTING_FILE_FORMAT",
    "TRY_CAST",
}

XML_ROW_TAG_STRING = "ROWTAG"
XML_ROW_DATA_COLUMN_NAME = "ROW_DATA"
XML_READER_FILE_PATH = os.path.join(os.path.dirname(__file__), "xml_reader.py")
XML_READER_API_SIGNATURE = "DataFrameReader.xml[rowTag]"
XML_READER_SQL_COMMENT = f"/* Python:snowflake.snowpark.{XML_READER_API_SIGNATURE} */"

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

# TODO: merge fixed pandas importer changes to connector.
def _pandas_importer():  # noqa: E302
    """Helper function to lazily import pandas and return MissingPandas if not installed."""
    from snowflake.connector.options import MissingPandas

    pandas = MissingPandas()
    try:
        pandas = importlib.import_module("pandas")
        # since we enable relative imports without dots this import gives us an issues when ran from test directory
        from pandas import DataFrame  # NOQA
    except ImportError:  # pragma: no cover
        pass  # pragma: no cover
    return pandas


pandas = _pandas_importer()
installed_pandas = not isinstance(pandas, MissingOptionalDependency)


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


def validate_stage_location(stage_location: str) -> str:
    stage_location = stage_location.strip()
    if not stage_location:
        raise ValueError(
            "stage_location cannot be empty. It must be a full stage path with prefix and file name like @mystage/stage/prefix/filename"
        )
    if stage_location[-1] == "/":
        raise ValueError(
            "stage_location should end with target filename like @mystage/prefix/stage/filename"
        )
    return stage_location


@lru_cache
def get_version() -> str:
    return ".".join([str(d) for d in snowpark_version if d is not None])


@lru_cache
def get_python_version() -> str:
    return platform.python_version()


@lru_cache
def is_interactive() -> bool:
    return hasattr(sys, "ps1") or sys.flags.interactive or "snowbook" in sys.modules


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
    prefixes = ["file://"] if is_local else SNOWFLAKE_PATH_PREFIXES_FOR_GET
    if is_single_quoted(path):
        return path
    if is_local and OPERATING_SYSTEM == "Windows":
        path = path.replace("\\", "/")
    path = path.strip().replace("'", "\\'")
    if not any(path.startswith(prefix) for prefix in prefixes):
        path = f"{prefixes[0]}{path}"
    return f"'{path}'"


def warn_session_config_update_in_multithreaded_mode(config: str) -> None:
    if threading.active_count() > 1:
        _logger.warning(
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
    if any(new_name.startswith(prefix) for prefix in SNOWFLAKE_PATH_PREFIXES_FOR_GET):
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
    collect_stacktrace: bool = False,
) -> Dict[str, str]:
    if (
        exists_session_query_tag
        or (statement_params and QUERY_TAG_STRING in statement_params)
        or not collect_stacktrace
    ):
        return statement_params

    ret = statement_params or {}
    # as create_statement_query_tag is called by the method, skip_levels needs to +1 to skip the current call
    ret[QUERY_TAG_STRING] = create_statement_query_tag(skip_levels + 1)
    return ret


def get_stage_parts(stage_location: str) -> tuple[str, str]:
    normalized = unwrap_stage_location_single_quote(stage_location)
    if not normalized.endswith("/"):
        normalized = f"{normalized}/"

    # Remove the first three characters from @~/...
    if normalized.startswith(f"{STAGE_PREFIX}~"):
        return "~", normalized[3:]

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
            return stage_name, path

    return None, None


def get_stage_file_prefix_length(stage_location: str) -> int:
    stage_name, path = get_stage_parts(stage_location)

    if stage_name == "~":
        return len(path)

    if stage_name:
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
    return "".join(Random().choice(ALPHANUMERIC) for _ in range(length))


def column_to_bool(col_):
    """A replacement to bool(col_) to check if ``col_`` is None or Empty.

    ``Column.__bool__` raises an exception to remind users to use &, |, ~ instead of and, or, not for logical operations.
    The side-effect is the implicit call like ``if col_`` also raises an exception.
    Our internal code sometimes needs to check an input column is None, "", or []. So this method will help it by writeint ``if column_to_bool(col_): ...``
    """
    if isinstance(col_, snowflake.snowpark.Column):
        return True
    return bool(col_)


def _parse_result_meta(
    result_meta: Union[List[ResultMetadata], List["ResultMetadataV2"]]
) -> Tuple[Optional[List[str]], Optional[List[Callable]]]:
    """
    Takes a list of result metadata objects and returns a list containing the names of all fields as
    well as a list of functions that wrap specific columns.

    A column type may need to be wrapped if the connector is unable to provide the columns data in
    an expected format. For example StructType columns are returned as dict objects, but are better
    represented as Row objects.
    """
    from snowflake.snowpark.context import _should_use_structured_type_semantics

    if not result_meta:
        return None, None
    col_names = []
    wrappers = []
    for col in result_meta:
        col_names.append(col.name)
        if (
            _should_use_structured_type_semantics()
            and FIELD_ID_TO_NAME[col.type_code] == "OBJECT"
            and col.fields is not None
        ):
            wrappers.append(lambda x: Row(**x))
        else:
            wrappers.append(None)
    return col_names, wrappers


def result_set_to_rows(
    result_set: List[Any],
    result_meta: Optional[Union[List[ResultMetadata], List["ResultMetadataV2"]]] = None,
    case_sensitive: bool = True,
) -> List[Row]:
    col_names, wrappers = _parse_result_meta(result_meta or [])
    rows = []
    row_struct = Row
    if col_names:
        row_struct = (
            Row._builder.build(*col_names).set_case_sensitive(case_sensitive).to_row()
        )
    for data in result_set:
        if wrappers:
            data = [wrap(d) if wrap else d for wrap, d in zip(wrappers, data)]

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
    col_names, wrappers = _parse_result_meta(result_meta)
    row_struct = Row
    if col_names:
        row_struct = (
            Row._builder.build(*col_names).set_case_sensitive(case_sensitive).to_row()
        )
    for data in result_set:
        if data is None:
            raise ValueError("Result returned from Python connector is None")
        if wrappers:
            data = [wrap(d) if wrap else d for wrap, d in zip(wrappers, data)]
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


class WarningHelper:
    def __init__(self, warning_times: int) -> None:
        self.warning_times = warning_times
        self.count = 0

    def warning(self, text: str) -> None:
        if self.count < self.warning_times:
            _logger.warning(text)
        self.count += 1


warning_dict: Dict[str, WarningHelper] = {}


def warning(name: str, text: str, warning_times: int = 1) -> None:
    if name not in warning_dict:
        warning_dict[name] = WarningHelper(warning_times)
    warning_dict[name].warning(text)


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


@unique
class AstMode(IntEnum):
    """
    Describes the ast modes that instruct the client to send sql and/or dataframe AST to snowflake server.
    """

    SQL_ONLY = 0
    SQL_AND_AST = 1
    AST_ONLY = 2


@unique
class AstFlagSource(IntEnum):
    """
    Describes the source of the AST feature flag value. This is not just an annotation!
    The enum value determines the precedence of the value.
    """

    LOCAL = auto()
    """Some local criteria determined the value. This has the lowest precedence. Any other source can override it."""
    SERVER = auto()
    """The server set the value."""
    USER = auto()
    """The flag has been set by the user explicitly at their own risk."""
    TEST = auto()
    """
    Do not use this in production Snowpark code. Test code sets the value. This has the highest precedence.
    However, other test code can override previous settings.

    Do not misuse this flag source in production code, as it will lead to unpredictable behavior.
    """


@unique
class _AstFlagState(IntEnum):
    """
    Describes the state of the AST feature flag value.
    """

    NEW = auto()
    """The flag is initialized with a hard-coded default. No source has set the value."""
    TENTATIVE = auto()
    """The flag has been set by a source with lower precedence than SERVER. It can be overridden by SERVER."""
    SERVER_SET = auto()
    """The flag has been set by SERVER."""
    USER_SET = auto()
    """The flag has been set by USER."""
    FINALIZED = auto()
    """The flag state can only be changed by TEST sources."""


class _AstState:
    """
    Tracks the state of the ast_enabled feature flag. This class is thread-safe. The most important role of this
    class is to prevent the flag from flip-flopping. In particular, once the feature is disabled (for any reason),
    nothing can re-enable it.
    """

    def __init__(self) -> None:
        """Creates an instance of _AstState."""
        self._mutex = threading.Lock()
        # The only safe default value is True. If the default is False, the flag can never be enabled.
        # Consider a simple scenario:
        # Initialize Snowpark.
        # Create a few objects that are session-agnostic:
        # a = Col("a")
        # b = Col("b")
        # Initialize a Snowpark session, and try to use a and b with ast_enabled = True.
        # The objects got created with ast_enabled = False and are unusable.
        self._ast_enabled = True
        self._state = _AstFlagState.NEW

    @property
    def enabled(self) -> bool:
        """Gets the value of the ast_enabled feature flag."""
        with self._mutex:
            if self._state == _AstFlagState.NEW:
                # Nothing (test harness, local code, or explicit server setting) has set the value.
                # Transition to TENTATIVE state as if local code had set the value.
                _logger.info(
                    "AST state has not been set explicitly. Defaulting to ast_enabled = %s.",
                    self._ast_enabled,
                )
                self._state = _AstFlagState.TENTATIVE
            return self._ast_enabled

    def set_state(self, source: AstFlagSource, enable: bool) -> None:
        """
        Sets the value of the ast_enabled feature flag. The method may ignore the change request if the requested
        transition is unsafe, or if the flag was already set at a precedence level not greater than the precedence
        level of "source".

        Flip-flopping the flag (enabled -> disabled -> enabled) is unsafe.

        The AST feature can be disabled only once, and stays disabled no matter what happens afterward. The feature can
        be enabled transiently, and once something (server setting or test configuration) makes a final decision, that
        decision is permanent for the life of the process.

        Args:
            source: The source of the request. Using SERVER or TEST will finalize the flag.
            enable: The new value of the flag.
        """
        with self._mutex:
            _logger.debug(
                "Setting AST state. Current state: ast_enabled = %s, state = %s. Request: source = %s, enable = %s.",
                self._ast_enabled,
                self._state,
                source,
                enable,
            )
            if source == AstFlagSource.TEST:
                # TEST behaviors override everything.
                # If you see this code path running in production, the calling code is broken.
                self._state = _AstFlagState.FINALIZED
                self._ast_enabled = enable
                return
            if self._state == _AstFlagState.FINALIZED and self._ast_enabled != enable:
                _logger.warning(
                    "Cannot change AST state after it has been finalized. Frozen ast_enabled = %s. Ignoring value %s.",
                    self._ast_enabled,
                    enable,
                )
                return

            # User is allowed to make the disabled -> enabled transition which is considered unsafe.
            # This is only intended for case when the user wants to enable AST immediately after session
            # is initialized.
            if source == AstFlagSource.USER:
                # User is allowed to override the server setting if it is not
                # finalized by test.
                self._ast_enabled = enable
                self._state = _AstFlagState.USER_SET
                return

            # If the current state is TENTATIVE, and the value is transitioning from disabled -> enabled,
            # then the transition is unsafe.
            safe_transition: bool = not (
                self._state == _AstFlagState.TENTATIVE
                and not self._ast_enabled
                and enable
            )
            if source == AstFlagSource.SERVER:
                if safe_transition:
                    self._ast_enabled = enable
                else:
                    _logger.warning(
                        "Server cannot enable AST after treating it as disabled locally. Ignoring request."
                    )
                self._state = _AstFlagState.SERVER_SET
            elif source == AstFlagSource.LOCAL:
                if safe_transition:
                    self._ast_enabled = enable
                else:
                    _logger.warning(
                        "Cannot enable AST by local preference after treating it as disabled. Ignoring request."
                    )
                self._state = _AstFlagState.TENTATIVE
            else:
                raise NotImplementedError(
                    f"Unhandled transition. Current state: ast_enabled = {self._ast_enabled}, state = {self._state}. Request: source = {source}, enable = {enable}"
                )


_ast_state: _AstState = _AstState()


def is_ast_enabled() -> bool:
    """Gets the value of the ast_enabled feature flag."""
    global _ast_state
    return _ast_state.enabled


def set_ast_state(source: AstFlagSource, enabled: bool) -> None:
    """
    Sets the value of the ast_enabled feature flag.

    See _AstState.set_state for more information.
    """
    global _ast_state
    return _ast_state.set_state(source, enabled)


# When the minimum supported Python version is at least 3.10, the type
# annotations for publicapi should use typing.ParamSpec:
# P = ParamSpec("P")
# ReturnT = TypeVar("ReturnT")
# def publicapi(func: Callable[P, ReturnT]) -> Callable[P, ReturnT]:
#   ...
#   @functools.wraps(func)
#   def call_wrapper(*args: P.args, **kwargs: P.kwargs) -> ReturnT:
#     ...
#   ...
#   return call_wrapper
CallableT = TypeVar("CallableT", bound=Callable)


def publicapi(func: CallableT) -> CallableT:
    """decorator to safeguard public APIs with global feature flags."""

    # Note that co_varnames also includes local variables. This can trigger false positives.
    has_emit_ast: bool = "_emit_ast" in func.__code__.co_varnames

    @functools.wraps(func)
    def call_wrapper(*args, **kwargs):
        # warning(func.__qualname__, warning_text)

        if not has_emit_ast:
            # This callee doesn't have a _emit_ast parameter.
            return func(*args, **kwargs)

        if "_emit_ast" in kwargs:
            # The caller provided _emit_ast explicitly.
            return func(*args, **kwargs)

        kwargs["_emit_ast"] = is_ast_enabled()

        # TODO: Could modify internal docstring to display that users should not modify the _emit_ast parameter.

        return func(*args, **kwargs)

    return call_wrapper


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


def check_imports_type(
    imports: Optional[List[Union[str, Tuple[str, str]]]], name: str = ""
) -> None:
    """Check that import parameter adheres to type hint given, if not raises TypeError."""
    if not (
        imports is None
        or (
            isinstance(imports, list)
            and all(
                isinstance(imp, str)
                or (
                    isinstance(imp, tuple)
                    and len(imp) == 2
                    and isinstance(imp[0], str)
                    and isinstance(imp[1], str)
                )
                for imp in imports
            )
        )
    ):
        raise TypeError(
            f"{name} import can only be a file path (str) or a tuple of the file path (str) and the import path (str)"
        )


def check_output_schema_type(  # noqa: F821
    output_schema: Union[  # noqa: F821
        "StructType", Iterable[str], "PandasDataFrameType"  # noqa: F821
    ]  # noqa: F821
) -> None:
    """Helper function to ensure output_schema adheres to type hint."""

    from snowflake.snowpark.types import StructType

    if installed_pandas:
        from snowflake.snowpark.types import PandasDataFrameType
    else:
        PandasDataFrameType = int  # dummy type.

    if not (
        isinstance(output_schema, StructType)
        or (installed_pandas and isinstance(output_schema, PandasDataFrameType))
        or isinstance(output_schema, Iterable)
    ):
        raise ValueError(
            f"'output_schema' must be a list of column names or StructType or PandasDataFrameType instance to create a UDTF. Got {type(output_schema)}."
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
        _logger.warning(
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


def split_snowflake_identifier_with_dot(s: str) -> list:
    """
    Splits the Snowflake identifier by dots that are not within double-quoted parts.
    Tokens that appear quoted in the input remain unchanged (quotes are kept).
    See details in https://docs.snowflake.com/en/sql-reference/identifiers-syntax.

    Examples:
      'foo.bar."hello.world".baz'
          -> ['foo', 'bar', '"hello.world"', 'baz']
      '"a.b".c."d.e.f".g'
          -> ['"a.b"', 'c', '"d.e.f"', 'g']
    """
    # ensures that dots inside quotes are not used for splitting.
    parts = re.compile(r'"(?:[^"]|"")*"|[^.]+').findall(s)
    return parts


# Define the full-width regex pattern, copied from Spark
full_width_regex = re.compile(
    r"[\u1100-\u115F"
    r"\u2E80-\uA4CF"
    r"\uAC00-\uD7A3"
    r"\uF900-\uFAFF"
    r"\uFE10-\uFE19"
    r"\uFE30-\uFE6F"
    r"\uFF00-\uFF60"
    r"\uFFE0-\uFFE6]"
)


def string_half_width(s: str) -> int:
    """
    Calculate the half-width of a string by adding 1 for each character
    and adding an extra 1 for each full-width character.

    :param s: The input string
    :return: The calculated width
    """
    if s is None:
        return 0
    full_width_count = len(full_width_regex.findall(s))
    return len(s) + full_width_count


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


def check_create_map_parameter(*cols: Any) -> bool:
    """Helper function to check parameter cols for create_map function."""

    error_message = "The 'create_map' function requires an even number of parameters but the actual number is {}"

    # TODO SNOW-1790918: Keep error messages for now identical to current state, make more distinct by replacing text in blocks.
    if len(cols) == 1:
        cols = cols[0]
        if not isinstance(cols, (tuple, list)):
            raise ValueError(error_message.format(len(cols)))

    if not len(cols) % 2 == 0:
        raise ValueError(error_message.format(len(cols)))

    return len(cols) == 1 and isinstance(cols, (tuple, list))


def is_valid_tuple_for_agg(e: Union[list, tuple]) -> bool:
    from snowflake.snowpark import Column

    return len(e) == 2 and isinstance(e[0], (Column, str)) and isinstance(e[1], str)


def check_agg_exprs(
    exprs: Union[
        "snowflake.snowpark.Column",
        Tuple["snowflake.snowpark.ColumnOrName", str],
        Dict[str, str],
    ]
):
    """Helper function to raise exceptions when invalid exprs have been passed."""
    from snowflake.snowpark import Column

    exprs, _ = parse_positional_args_to_list_variadic(*exprs)

    # special case for single list or tuple
    if is_valid_tuple_for_agg(exprs):
        exprs = [exprs]

    if len(exprs) > 0 and isinstance(exprs[0], dict):
        for k, v in exprs[0].items():
            if not (isinstance(k, str) and isinstance(v, str)):
                raise TypeError(
                    "Dictionary passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() "
                    f"should contain only strings: got key-value pair with types {type(k), type(v)}"
                )
    else:
        for e in exprs:
            if not (
                isinstance(e, Column)
                or (isinstance(e, (list, tuple)) and is_valid_tuple_for_agg(e))
            ):
                raise TypeError(
                    "List passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should "
                    "contain only Column objects, or pairs of Column object (or column name) and strings."
                )


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


class GlobalCounter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counter: count[int] = itertools.count()

    def reset(self):
        with self._lock:
            self._counter = itertools.count()

    def next(self) -> int:
        with self._lock:
            return next(self._counter)


global_counter: GlobalCounter = GlobalCounter()


class ExprAliasUpdateDict(dict):
    """
    A specialized dictionary for mapping expressions (UUID keys) to alias names updates tracking.
    This is used to resolve ambiguous column names in join operations.

    This dictionary is designed to store aliases as string values while also tracking whether
    each alias was inherited from child DataFrame plan.
    The values are stored as tuples of the form `(alias: str, updated_from_inherited: bool)`, where:

    - `alias` (str): The alias name for the expression.
    - `updated_from_inheritance` (bool): A flag indicating whether the expr alias was updated
     because it's inherited from child plan (True) or not (False).

    Below is an example for resolving alias mapping:

    data = [25, 30]
    columns = ["age"]
    df1 = session.createDataFrame(data, columns)
    df2 = session.createDataFrame(data, columns)
    df3 = df1.join(df2)
    # in DF3:
    #    df1.age expr_id -> (age_alias_left, False)  # comes from df1 due to being disambiguated in the join condition
    #    df2.age expr_id -> (age_alias_right, False)  # comes from df2 due to being disambiguated in the join condition

    df4 = df3.select(df1.age.alias("age"))

    # in DF4:
    #    df1.age.expr_id -> (age, False)  # comes from df3 explict alias
    #    df2.age.expr_id -> (age_alias_right, False)  # unchanged, inherited from df3
    df5 = df1.join(df4, df1["age"] == df4["age"])

    # in the middle of df1.join(df2) operation, we have the following expr_to_alias mapping:
    #  DF4 intermediate expr_to_alias:
    #    df4.age.expr_id -> (age_alias_right2, False)  # comes from df4 due to being disambiguated in the join condition, note here df4.age is not the same as df1.age, it's a new attribute
    #    df1.age.expr_id -> (age_alias_right2, True)  # comes from df4, updated due to inheritance
    #  DF1 intermediate expr_to_alias:
    #    df1.age.expr_id -> (age_alias_left2, False)  # comes from df1 due to being disambiguated in the join condition

    # when executing merge_multiple_snowflake_plan_expr_to_alias, we drop df1.age.expr_id -> (age_alias_right2, True)

    # in DF5:
    #    df4.age.expr_id -> (age_alias_right2, False)
    #    df1.age.expr_id -> (age_alias_left2, False)
    """

    def __setitem__(
        self, key: Union[uuid.UUID, str], value: Union[Tuple[str, bool], str]
    ):
        """If a string value is provided, it is automatically stored as `(value, False)`.
        If a tuple `(str, bool)` is provided, it must conform to the expected format.
        """
        if isinstance(value, str):
            # if value is a string, we set inherit to False
            value = (value, False)
        if not (
            isinstance(value, tuple)
            and len(value) == 2
            and isinstance(value[0], str)
            and isinstance(value[1], bool)
        ):
            raise ValueError("Value must be a tuple of (str, bool)")
        super().__setitem__(key, value)

    def __getitem__(self, item) -> str:
        """Returns only the alias string (`str`), omitting the inheritance flag."""
        value = super().__getitem__(item)
        return value[0]

    def get(self, key, default=None) -> str:
        value = super().get(key, None)
        if value is not None:
            return value[0]
        return default

    def was_updated_due_to_inheritance(self, key):
        """Returns whether a key was inherited (`True` or `False`)."""
        value = super().get(key, None)
        if value is not None:
            return value[1]
        return False

    def items(self):
        """Return (key, str) pairs instead of (key, (str, bool)) pairs."""
        return ((key, value[0]) for key, value in super().items())

    def values(self) -> Iterable[str]:
        """Return only the string parts of the tuple in the dictionary values."""
        return (value[0] for value in super().values())

    def update(self, other):
        assert isinstance(other, ExprAliasUpdateDict)
        for k in other:
            self[k] = (other[k], other.was_updated_due_to_inheritance(k))

    def copy(self) -> "ExprAliasUpdateDict":
        """Return a shallow copy of the dictionary, preserving the (str, bool) tuple structure."""
        return self.__copy__()

    def __copy__(self) -> "ExprAliasUpdateDict":
        """Shallow copy implementation for copy.copy()"""
        new_copy = ExprAliasUpdateDict()
        new_copy.update(self)
        return new_copy

    def __deepcopy__(self, memo) -> "ExprAliasUpdateDict":
        """Deep copy implementation for copy.deepcopy()"""
        return self.__copy__()


def merge_multiple_snowflake_plan_expr_to_alias(
    snowflake_plans: List["SnowflakePlan"],
) -> ExprAliasUpdateDict:
    """
    Merges expression-to-alias mappings from multiple Snowflake plans, resolving conflicts where possible.
    The conflict resolution strategy is as follows:

    1) If they map to the same alias:
        * Retain the expression in the final expr_to_alias map.
    2) If they map to different aliases:
        a) If one appears in the output attributes of the plan and the other does not:
            * Retain the one present in the output attributes and discard the other. This is because
            I. The one that shows up in the output attribute can be referenced by users to perform dataframe column operations.
            II. The one that does not show up in the output is an intermediate alias mapping and can not be referenced directly by users.
        b) If both appear in the output attributes:
            * Discard both expressions as this constitutes a valid ambiguous case that cannot be resolved.
    3) If neither appears in the output attributes:
        * Discard both expressions since they will no longer be referenced.

    taking our example from the class docstring, the conflicting df1.age in df5:
    #    df1.age -> (age_alias_right2, True)  # comes from df4, updated due to inheritance
    #    df1.age -> (age_alias_left2, False)  # comes from df1 due to being disambiguated in the join condition

    we are going to:
    DROP: df1.age -> (age_alias_right2, True) because it's not directly in the output and can not be referenced
    KEEP: df1.age -> (age_alias_left2, False) is going to be kept because it's directly in the output and can be referenced

    Args:
        snowflake_plans (List[SnowflakePlan]): List of SnowflakePlan objects.

    Returns:
        Dict[Any, str]: Merged expression-to-alias mapping.
    """

    # Gather all expression-to-alias mappings
    all_expr_to_alias_dicts = [plan.expr_to_alias for plan in snowflake_plans]

    # Initialize the merged dictionary
    merged_dict = ExprAliasUpdateDict()

    # Collect all unique keys from all dictionaries
    all_expr_ids = set().union(*all_expr_to_alias_dicts)

    conflicted_expr_ids = {}

    for expr_id in all_expr_ids:
        values = list(
            {
                (d[expr_id], d.was_updated_due_to_inheritance(expr_id))
                for d in all_expr_to_alias_dicts
                if expr_id in d
            }
        )
        # Check if all aliases are identical
        if len(values) == 1:
            merged_dict[expr_id] = values[0]
        else:
            conflicted_expr_ids[expr_id] = values

    if not conflicted_expr_ids:
        return merged_dict

    for expr_id in conflicted_expr_ids:
        expr_id_alias_candidates = set()
        for plan in snowflake_plans:
            output_columns = []
            if plan.schema_query is not None:
                output_columns = [attr.name for attr in plan.output]
            tmp_alias_name, tmp_updated_due_to_inheritance = plan.expr_to_alias[
                expr_id
            ], plan.expr_to_alias.was_updated_due_to_inheritance(expr_id)
            if tmp_alias_name not in output_columns or tmp_updated_due_to_inheritance:
                # alias updated due to inheritance are not considered as they are not used in the output
                # check Analyzer.unary_expression_extractor functions
                continue
            if len(expr_id_alias_candidates) == 1:
                # Only one candidate so far
                candidate_name, candidate_updated_due_to_inheritance = next(
                    iter(expr_id_alias_candidates)
                )
                if candidate_name == tmp_alias_name:
                    # The candidate is the same as the current alias
                    # we keep the non-inherited bool information as our strategy is to keep alias
                    # that shows directly in the output column rather than updates because of inheritance
                    tmp_updated_due_to_inheritance = (
                        candidate_updated_due_to_inheritance
                        and tmp_updated_due_to_inheritance
                    )
                    expr_id_alias_candidates.pop()
            expr_id_alias_candidates.add(
                (tmp_alias_name, tmp_updated_due_to_inheritance)
            )
        # Add the candidate to the merged dictionary if resolved
        if len(expr_id_alias_candidates) == 1:
            merged_dict[expr_id] = expr_id_alias_candidates.pop()
        else:
            # No valid candidate found
            _logger.debug(
                f"Expression '{expr_id}' is associated with multiple aliases across different plans. "
                f"Unable to determine which alias to use. Conflicting values: {conflicted_expr_ids[expr_id]}"
            )

    return merged_dict


def str_contains_alphabet(ver):
    """Return True if ver contains alphabet, e.g., 1a1; otherwise, return False, e.g., 112"""
    return bool(re.search("[a-zA-Z]", ver))


def get_sorted_key_for_version(version_str):
    """Generate a key to sort versions. Note if a version component is not a number, e.g., "1a1", we will treat it as -1. E.g., 1.11.1a1 will be treated as 1.11.-1"""
    return tuple(
        -1 if str_contains_alphabet(num) else int(num) for num in version_str.split(".")
    )


def ttl_cache(ttl_seconds: float):
    """
    A decorator that caches function results with a time-to-live (TTL) expiration.

    Args:
        ttl_seconds (float): Time-to-live in seconds for cached items
    """

    def decorator(func):
        cache = {}
        expiry_heap = []  # heap of (expiry_time, cache_key)
        cache_lock = threading.RLock()

        def _make_cache_key(*args, **kwargs) -> int:
            """Create a hashable cache key from function arguments."""
            key = (args, tuple(sorted(kwargs.items())))
            try:
                # Try to create a simple tuple key
                return hash(key)
            except TypeError:
                # If args contain unhashable types, convert to string representation
                return hash(str(key))

        def _cleanup_expired(current_time: float):
            """Remove expired entries from cache and heap."""
            # Clean up expired entries from the heap and cache
            while expiry_heap:
                expiry_time, cache_key = expiry_heap[0]
                if expiry_time >= current_time:
                    break
                heapq.heappop(expiry_heap)
                if cache_key in cache:
                    del cache[cache_key]

        def _clear_cache():
            with cache_lock:
                cache.clear()
                expiry_heap.clear()

        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = _make_cache_key(*args, **kwargs)
            current_time = time.time()

            with cache_lock:
                _cleanup_expired(current_time)

                if cache_key in cache:
                    return cache[cache_key]

                result = func(*args, **kwargs)

                cache[cache_key] = result
                expiry_time = current_time + ttl_seconds
                heapq.heappush(expiry_heap, (expiry_time, cache_key))

                return result

        # Exposes the cache for testing
        wrapper._cache = cache
        wrapper.clear_cache = _clear_cache

        return wrapper

    return decorator


def remove_comments(sql_query: str, uuids: List[str]) -> str:
    """Removes comments associated with child uuids in a query"""
    from snowflake.snowpark._internal.analyzer import analyzer_utils

    comment_placeholders = {
        analyzer_utils.format_uuid(uuid, with_new_line=False) for uuid in uuids
    }
    return "\n".join(
        line
        for line in sql_query.split("\n")
        if line not in comment_placeholders and line != ""
    )


def get_line_numbers(
    commented_sql_query: str,
    child_uuids: List[str],
    parent_uuid: str,
) -> List["QueryLineInterval"]:
    """
        Helper function to get line numbers associated with child uuids in a query
        The commented_sql_query is a sql query with comments associated with child uuids. For example, it may look like:
        ```
        SELECT col1
        FROM
    -- child-uuid-1:
    table1
    -- child-uuid-1:
    WHERE col1 > 0
    -- child-uuid-2:
    ORDER BY col1
    -- child-uuid-2:
        ```
        For this query, we want to return a list of QueryLineInterval objects with the following intervals:
        - (0, 1, "parent-uuid")
        - (2, 2, "child-uuid-1")
        - (3, 3, "parent-uuid")
        - (4, 4, "child-uuid-2")

        To do so, we split the commented_sql_query into lines and check for comments associated with child uuids. We find all
        intervals associated with child uuids and associate the remaining lines with the parent uuid.
    """
    from snowflake.snowpark._internal.analyzer.snowflake_plan import QueryLineInterval
    from snowflake.snowpark._internal.analyzer import analyzer_utils

    sql_lines = commented_sql_query.split("\n")
    comment_placeholders = {
        analyzer_utils.format_uuid(uuid, with_new_line=False): uuid
        for uuid in child_uuids
    }

    idx = 0
    child_intervals = []
    child_start = -1
    found_start = False
    current_child_uuid = None

    for line in sql_lines:
        if line in comment_placeholders:
            if found_start and current_child_uuid:
                child_intervals.append(
                    QueryLineInterval(child_start, idx - 1, current_child_uuid)
                )
                found_start = False
                current_child_uuid = None
            elif not found_start:
                found_start = True
                child_start = idx
                current_child_uuid = comment_placeholders[line]
        elif line != "":
            idx += 1

    current_pos = 0
    new_intervals = []

    for interval in child_intervals:
        if current_pos < interval.start:
            new_intervals.append(
                QueryLineInterval(current_pos, interval.start - 1, parent_uuid)
            )
        new_intervals.append(interval)
        current_pos = interval.end + 1

    if current_pos < idx:
        new_intervals.append(QueryLineInterval(current_pos, idx - 1, parent_uuid))

    return new_intervals


def get_plan_from_line_numbers(
    plan_node: Union["SnowflakePlan", "Selectable"],
    line_number: int,
) -> "SnowflakePlan":
    """
    Given a parent plan node and a line number, return the plan node that contains the line number.
    Each parent node has a list of disjoint query line intervals, which are sorted by start line number,
    and each interval is associated with a plan node uuid. Each node either knows it is responsible for
    the lines in the interval if its uuid is the same as the interval's uuid, or that one of its descendants
    is responsible otherwise. If we find that the descendant is responsible, then we subtract the offset created
    by the parent before recursively searching the child. For example, if the parent is responsible for lines 0-10 and
    its descendants are responsible for lines 11-20 and we are searching for line 15, we will search the child for line 4.
    We raise ValueError if the line number does not fall within any interval.
    """
    from snowflake.snowpark._internal.analyzer.select_statement import Selectable

    # we binary search for our line number because our intervals are sorted
    def find_interval_containing_line(intervals, line_number):
        left, right = 0, len(intervals) - 1
        while left <= right:
            mid = (left + right) // 2
            start, end = intervals[mid].start, intervals[mid].end

            if start <= line_number <= end:
                return mid
            elif line_number < start:
                right = mid - 1
            else:
                left = mid + 1
        return -1

    # traverse the plan tree to find the plan that contains the line number
    stack = [(plan_node, line_number, None)]
    while stack:
        node, line_number, df_ast_ids = stack.pop()
        if isinstance(node, Selectable):
            node = node.get_snowflake_plan(skip_schema_query=False)
        if node.df_ast_ids is not None:
            df_ast_ids = node.df_ast_ids
        query_line_intervals = node.queries[-1].query_line_intervals
        idx = find_interval_containing_line(query_line_intervals, line_number)
        if idx >= 0:
            uuid = query_line_intervals[idx].uuid
            if node.uuid == uuid:
                node.df_ast_ids = df_ast_ids
                return node
            else:
                for child in node.children_plan_nodes:
                    if child.uuid == uuid:
                        stack.append(
                            (
                                child,
                                line_number - query_line_intervals[idx].start,
                                df_ast_ids,
                            )
                        )
                        break
        else:
            raise ValueError(
                f"Line number {line_number} does not fall within any interval"
            )

    raise ValueError(f"Line number {line_number} does not fall within any interval")
