#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import array
import contextlib
import datetime
import decimal
import functools
import hashlib
import io
import logging
import os
import platform
import random
import re
import string
import traceback
import zipfile
from enum import Enum
from json import JSONEncoder
from random import choice
from typing import IO, Any, Iterator, List, Optional, Type

import snowflake.snowpark
from snowflake.connector.cursor import ResultMetadata, SnowflakeCursor
from snowflake.connector.description import OPERATING_SYSTEM, PLATFORM
from snowflake.connector.version import VERSION as connector_version
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.row import Row
from snowflake.snowpark.version import VERSION as snowpark_version

STAGE_PREFIX = "@"

# Scala uses 3 but this can be larger. Consider allowing users to configure it.
QUERY_TAG_TRACEBACK_LIMIT = 3

# https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
SNOWFLAKE_UNQUOTED_ID_PATTERN = r"([a-zA-Z_][\w\$]{0,255})"
SNOWFLAKE_QUOTED_ID_PATTERN = '("([^"]|""){1,255}")'
SNOWFLAKE_ID_PATTERN = (
    f"({SNOWFLAKE_UNQUOTED_ID_PATTERN}|{SNOWFLAKE_QUOTED_ID_PATTERN})"
)

# Valid name can be:
#   identifier
#   identifier.identifier
#   identifier.identifier.identifier
#   identifier..identifier
SNOWFLAKE_OBJECT_RE_PATTERN = re.compile(
    f"^(({SNOWFLAKE_ID_PATTERN}\\.){{0,2}}|({SNOWFLAKE_ID_PATTERN}\\.\\.)){SNOWFLAKE_ID_PATTERN}$"
)

# "%?" is for table stage
SNOWFLAKE_STAGE_NAME_PATTERN = f"(%?{SNOWFLAKE_ID_PATTERN})"

# Prefix for allowed temp object names in stored proc
TEMP_OBJECT_NAME_PREFIX = "SNOWPARK_TEMP_"
ALPHANUMERIC = string.digits + string.ascii_lowercase


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

INFER_SCHEMA_FORMAT_TYPES = ("PARQUET", "ORC", "AVRO")

COPY_OPTIONS = {
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


def validate_object_name(name: str):
    if not SNOWFLAKE_OBJECT_RE_PATTERN.match(name):
        raise SnowparkClientExceptionMessages.GENERAL_INVALID_OBJECT_NAME(name)


def get_version() -> str:
    return ".".join([str(d) for d in snowpark_version if d is not None])


def get_python_version() -> str:
    return platform.python_version()


def get_connector_version() -> str:
    return ".".join([str(d) for d in connector_version if d is not None])


def get_os_name() -> str:
    return platform.system()


def get_application_name() -> str:
    return "PythonSnowpark"


def is_single_quoted(name: str) -> bool:
    return name.startswith("'") and name.endswith("'")


def unwrap_single_quote(name: str) -> str:
    new_name = name.strip()
    if is_single_quoted(new_name):
        new_name = new_name[1:-1]
    new_name = new_name.replace("\\'", "'")
    return new_name


def normalize_path(path: str, is_local: bool) -> str:
    """
    Get a normalized path of a local file or remote stage location for PUT/GET commands.
    If there are any special characters including spaces in the path, it needs to be
    quoted with single quote. For example, 'file:///tmp/load data' for a path containing
    a directory named "load data". Therefore, if `path` is already wrapped by single quotes,
    we do nothing.
    """
    symbol = "file://" if is_local else STAGE_PREFIX
    if is_single_quoted(path):
        return path
    if is_local and OPERATING_SYSTEM == "Windows":
        path = path.replace("\\", "/")
    path = path.strip().replace("'", "\\'")
    if not path.startswith(symbol):
        path = f"{symbol}{path}"
    return f"'{path}'"


def normalize_remote_file_or_dir(name: str) -> str:
    return normalize_path(name, is_local=False)


def normalize_local_file(file: str) -> str:
    return normalize_path(file, is_local=True)


def unwrap_stage_location_single_quote(name: str) -> str:
    new_name = unwrap_single_quote(name)
    if new_name.startswith(STAGE_PREFIX):
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
            will be "dir2/test.py".
        add_init_py: Whether to add __init__.py along the compressed path.
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

        # __init__.py is needed for all directories along the import path
        # when importing a module as a zip file
        if add_init_py:
            relative_path = os.path.relpath(path, start_path)
            head, _ = os.path.split(relative_path)
            while head and head != os.sep:
                zf.writestr(os.path.join(head, "__init__.py"), "")
                head, _ = os.path.split(head)

    yield input_stream
    input_stream.close()


def parse_positional_args_to_list(*inputs) -> List:
    """Convert the positional arguments to a list."""
    if len(inputs) == 1:
        return (
            [*inputs[0]] if isinstance(inputs[0], (list, tuple, set)) else [inputs[0]]
        )
    else:
        return [*inputs]


def calculate_checksum(
    path: str,
    chunk_size: int = 8192,
    ignore_generated_py_file: bool = True,
    additional_info: Optional[str] = None,
    algorithm: str = "sha256",
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

    Returns:
        The result checksum.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} is not found")

    hash_algo = hashlib.new(algorithm)
    if os.path.isfile(path):
        with open(path, "rb") as f:
            hash_algo.update(f.read(chunk_size))
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
                if current_size < chunk_size:
                    filename = os.path.join(dirname, file)
                    file_size = os.path.getsize(filename)
                    read_size = min(file_size, chunk_size - current_size)
                    current_size += read_size
                    with open(filename, "rb") as f:
                        hash_algo.update(f.read(read_size))
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
    result_set: List[Any], result_meta: Optional[List[ResultMetadata]] = None
) -> List[Row]:
    if result_meta:
        col_names = [col.name for col in result_meta]
        rows = []
        for data in result_set:
            row = Row(*data)
            # row might have duplicated column names
            row._fields = col_names
            rows.append(row)
    else:
        rows = [Row(*row) for row in result_set]
    return rows


def result_set_to_iter(
    result_set: SnowflakeCursor, result_meta: Optional[List[ResultMetadata]] = None
) -> Iterator[Row]:
    col_names = [col.name for col in result_meta] if result_meta else None
    for data in result_set:
        row = Row(*data)
        if col_names:
            row._fields = col_names
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


def deprecate(*, deprecate_version, extra_warning_text="", extra_doc_string=""):
    def deprecate_wrapper(func):
        warning_text = (
            f"{func.__name__} is deprecated since {deprecate_version}. "
            f"{extra_warning_text}"
        )
        doc_string_text = (
            f"Deprecated since {deprecate_version}. {extra_doc_string} \n\n"
        )
        func.__doc__ = f"{func.__doc__ or ''}\n\n{' '*8}{doc_string_text}\n"

        @functools.wraps(func)
        def func_call_wrapper(*args, **kwargs):
            deprecate_warning_times = getattr(func, "deprecate_warning_times", 0)
            if getattr(func, "deprecate_warning_times", 0) < 1:
                logger.warning(warning_text)
                func.deprecate_warning_times = deprecate_warning_times + 1
            return func(*args, **kwargs)

        return func_call_wrapper

    return deprecate_wrapper
