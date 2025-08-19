#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import csv
import glob
import json
import os
import platform
import re
import shutil
import tempfile
import uuid
from collections import defaultdict
from functools import partial
from logging import getLogger
from typing import IO, TYPE_CHECKING, Dict, List, Tuple

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.type_utils import infer_type
from snowflake.snowpark._internal.utils import (
    quote_name,
    unwrap_stage_location_single_quote,
)
from snowflake.snowpark.mock._functions import mock_to_char
from snowflake.snowpark.mock._options import pandas as pd
from snowflake.snowpark.mock._snowflake_data_type import (
    ColumnEmulator,
    ColumnType,
    TableEmulator,
)
from snowflake.snowpark.mock._snowflake_to_pandas_converter import CONVERT_MAP
from snowflake.snowpark.mock._util import convert_snowflake_datetime_format
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.types import (
    DecimalType,
    StringType,
    VariantType,
    TimestampType,
    DateType,
    TimeType,
)

if TYPE_CHECKING:
    from snowflake.snowpark.mock._analyzer import MockAnalyzer
    from snowflake.snowpark.mock._connection import MockServerConnection

_logger = getLogger(__name__)

DEFAULT_TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS"
DEFAULT_DATE_FORMAT = "YYYY-MM-DD"
DEFAULT_TIME_FORMAT = "HH24:MI:SS"

PUT_RESULT_KEYS = [
    "source",
    "target",
    "source_size",
    "target_size",
    "source_compression",
    "target_compression",
    "status",
    "message",
]


GET_RESULT_KEYS = [
    "file",
    "size",
    "status",
    "message",
]

# option support map
# top level:
#   key: file format name
#   value: dict of supported option
# child level:
#   key: option name
#   value (two categories):
#     - tuple, enum of valid option values in the string form
#     - None, users set the value
SUPPORT_READ_OPTIONS = {
    "csv": {
        "SKIP_HEADER": None,
        "SKIP_BLANK_LINES": None,
        "FIELD_DELIMITER": None,
        "FIELD_OPTIONALLY_ENCLOSED_BY": None,
        "INFER_SCHEMA": ("FALSE",),
        "PARSE_HEADER": ("TRUE", "FALSE"),
        "PURGE": ("TRUE", "FALSE"),
        "COMPRESSION": ("AUTO", "NONE"),
        "PATTERN": None,
        "ENCODING": ("UTF8", "UTF-8"),
        "NULL_IF": None,
        "DATE_FORMAT": None,
        "TIMESTAMP_FORMAT": None,
        "TIME_FORMAT": None,
    },
    "json": {
        "INFER_SCHEMA": ("TRUE", "FALSE"),
        "FILE_EXTENSION": None,
        "PURGE": ("TRUE", "FALSE"),
        "COMPRESSION": ("AUTO", "NONE"),
        "PATTERN": None,
        "ENCODING": ("UTF8", "UTF-8"),
    },
    # TODO: Support avro, xml, parquet, orc in local test.
    "avro": {},
    "xml": {},
    "parquet": {},
    "orc": {},
}


RAISE_ERROR_ON_UNSUPPORTED_READ_OPTIONS = True
_INVALID_STAGE_LOCATION_ERR_MSG = (
    lambda stage_location: f"Invalid stage {stage_location}, stage name should start with character '@' or snow://"
)


def extract_stage_name_and_prefix(stage_location: str) -> Tuple[str, str]:
    """
    extract the stage name and dir path in the stage_location
    inspired by utils.get_stage_file_prefix_length

    currently we don't support fully qualified namespace stage
    TODO: https://snowflakecomputing.atlassian.net/browse/SNOW-1235144 for fully qualified namespace support
    """
    normalized = unwrap_stage_location_single_quote(stage_location)
    if not normalized.endswith("/"):
        normalized = f"{normalized}/"

    if normalized.startswith("@"):
        normalized = normalized[1:]  # remove the beginning '@'
    elif normalized.startswith("snow://"):
        normalized = "snow:/" + normalized[7:]  # remove one of the two slashes

    if normalized.startswith("~/"):
        return "~", normalized[3:]  # skip '/'

    is_quoted = False
    stage_name_start_idx, stage_name_end_idx = 0, None
    prefix_start_idx = None

    if normalized[0] == '"':
        stage_name_start_idx = 1
        for i, c in enumerate(normalized):
            if c == '"':
                is_quoted = (
                    not is_quoted
                )  # this handles escaping consecutive double quotes
            elif c == "/" and not is_quoted:
                # all chars prior to ith char is part of the stage name
                stage_name_end_idx = i - 1
                prefix_start_idx = i + 1
                break

        if not stage_name_end_idx:
            raise SnowparkLocalTestingException(
                f"Invalid stage_location {stage_location}."
            )
    else:
        stage_name_end_idx = normalized.find("/")
        prefix_start_idx = stage_name_end_idx + 1
    stage_name = normalized[stage_name_start_idx:stage_name_end_idx]
    dir_path = normalized[prefix_start_idx:-1]  # remove the first and last '/'

    if platform.system() == "Windows":
        # On Windows the separator is \\, we convert non-quoted '/' to '\\'
        # so that dirs can be created on windows
        def replace_dir_separator(input):
            idx, in_quote, output = 0, False, ""
            while idx < len(input):
                to_append_char = input[idx]
                if input[idx] == '"':
                    in_quote = not in_quote
                elif input[idx] == "/" and not in_quote:
                    to_append_char = os.sep
                output += to_append_char
                idx += 1
            return output

        dir_path = replace_dir_separator(dir_path)

    return stage_name, dir_path


def copy_files_and_dirs(src, dst):
    if os.path.isdir(src):
        shutil.copytree(src, dst)
    else:
        shutil.copy(src, dst)


class StageEntity:
    def __init__(
        self, root_dir_path: str, stage_name: str, conn: "MockServerConnection"
    ) -> None:
        self._stage_name = stage_name
        # stage name might contain special chars which can not be used as dir name
        # so we generate uuid as name
        self._dir_name = str(uuid.uuid4())
        self._working_directory = os.path.join(root_dir_path, self._dir_name)

        if os.path.exists(self._working_directory):
            shutil.rmtree(self._working_directory)

        os.mkdir(self._working_directory)
        self._conn = conn

    def put_file(
        self, local_file_name: str, stage_prefix: str, overwrite: bool = False
    ) -> TableEmulator:
        local_file_name = local_file_name[
            len("`file://") : -1
        ]  # skip normalized prefix "`file://" (be aware of the 0th backtick) and tailing backtick suffix
        # glob supports wildcard '?' and '*' searching
        list_of_files = glob.glob(local_file_name)
        result_df = TableEmulator(
            columns=PUT_RESULT_KEYS,
            sf_types={
                "source": ColumnType(StringType(), True),
                "target": ColumnType(StringType(), True),
                "source_size": ColumnType(DecimalType(10, 0), True),
                "target_size": ColumnType(DecimalType(10, 0), True),
                "source_compression": ColumnType(StringType(), True),
                "target_compression": ColumnType(StringType(), True),
                "status": ColumnType(StringType(), True),
                "message": ColumnType(StringType(), True),
            },
            dtype=object,
        )

        if not list_of_files:
            raise SnowparkLocalTestingException(
                f"File doesn't exist: {local_file_name}"
            )

        for local_file_name in list_of_files:

            file_name = os.path.basename(local_file_name)
            stage_target_dir_path = os.path.join(self._working_directory, stage_prefix)
            target_local_file_path = os.path.join(stage_target_dir_path, file_name)

            if os.path.exists(stage_target_dir_path) and os.path.isfile(
                stage_target_dir_path
            ):
                # we do not support file and folder sharing the same name under a dir in local testing
                # this is supported in snowflake.
                # Adding suffix to file is one potential solution to local testing, but it doesn't work
                # well for udf/sproc import cases.
                # check https://snowflakecomputing.atlassian.net/browse/SNOW-1254908 for more context
                self._conn.log_not_supported_error(
                    error_message="The target directory cannot have the same name as a file in the directory.",
                    internal_feature_name="StageEntity.put_file",
                    parameters_info={
                        "details": "Conflict names between file and directory"
                    },
                    raise_error=NotImplementedError,
                )

            if not os.path.exists(stage_target_dir_path):
                try:
                    os.makedirs(stage_target_dir_path)
                except BaseException:
                    self._conn.log_not_supported_error(
                        error_message=f"Unable to created directory {stage_target_dir_path} on the local file system. This could be caused by system limitations",
                        internal_feature_name="StageEntity.put_file",
                        parameters_info={"platform": platform.system()},
                        raise_error=NotImplementedError,
                    )

            if os.path.isfile(target_local_file_path) and not overwrite:
                status = "SKIPPED"
            else:
                copy_files_and_dirs(local_file_name, target_local_file_path)
                status = "UPLOADED"

            file_size = os.path.getsize(local_file_name)

            values = [
                file_name,
                file_name,
                file_size,
                file_size,
                "NONE",
                "NONE",
                status,
                "",
            ]
            result_df.loc[len(result_df)] = dict(zip(PUT_RESULT_KEYS, values))
        return result_df

    def upload_stream(
        self,
        input_stream: IO[bytes],
        stage_prefix: str,
        file_name: str,
        overwrite: bool = False,
    ) -> Dict:
        stage_target_dir_path = os.path.join(self._working_directory, stage_prefix)
        target_local_file_path = os.path.join(stage_target_dir_path, file_name)

        if os.path.exists(stage_target_dir_path) and os.path.isfile(
            stage_target_dir_path
        ):
            # we do not support file and folder sharing the same name under a dir in local testing
            # this is supported in snowflake.
            # Adding suffix to file is one potential solution to local testing, but it doesn't work
            # well for udf/sproc import cases.
            # check https://snowflakecomputing.atlassian.net/browse/SNOW-1254908 for more context
            self._conn.log_not_supported_error(
                error_message="The target directory cannot have the same name as a file in the directory.",
                internal_feature_name="StageEntity.upload_stream",
                parameters_info={
                    "details": "Conflict names between file and directory"
                },
                raise_error=NotImplementedError,
            )

        if not os.path.exists(stage_target_dir_path):
            os.makedirs(stage_target_dir_path)

        status = "UPLOADED"
        if os.path.isfile(target_local_file_path) and not overwrite:
            status = "SKIPPED"
        else:
            # TODO: SNOW-1235716 for error experience in local testing
            with open(target_local_file_path, "wb") as f:
                f.write(input_stream.read())

        file_size = os.path.getsize(target_local_file_path)
        return {
            "data": [
                (file_name, file_name, file_size, file_size, "NONE", "NONE", status, "")
            ],
            "sfqid": None,
        }

    def get_file(
        self,
        stage_location: str,
        target_directory: str,
        options: Dict[str, str] = None,
    ) -> TableEmulator:
        if target_directory.startswith("'file://"):
            target_directory = target_directory[
                len("'file://") : -1
            ]  # skip normalized prefix `file:// and suffix `
        stage_source_dir_path = os.path.join(self._working_directory, stage_location)

        result_df = TableEmulator(
            columns=GET_RESULT_KEYS,
            sf_types={
                "file": ColumnType(StringType(), True),
                "size": ColumnType(DecimalType(10, 0), True),
                "status": ColumnType(StringType(), True),
                "message": ColumnType(StringType(), True),
            },
            dtype=object,
        )

        # looking for a directory or a file
        if not (
            os.path.exists(stage_source_dir_path)
            or os.path.exists(stage_source_dir_path)
        ):
            raise SnowparkLocalTestingException(
                f"the file does not exist: {stage_source_dir_path}"
            )

        if os.path.isfile(stage_source_dir_path):
            list_of_files = [stage_source_dir_path]
        else:
            # here we get all the file names with suffix removed so that pattern can match the original names
            list_of_files = sorted(
                os.path.join(root, file)
                for root, dirs, files in os.walk(stage_source_dir_path)
                for file in files
            )

        pattern = options.get("pattern") if options else None

        for file in list_of_files:
            file_name = os.path.basename(file)
            # pattern[1:-1] to remove heading and tailing single quotes
            if pattern and not re.match(pattern[1:-1], file_name):
                continue
            stage_file = file
            copy_files_and_dirs(stage_file, os.path.join(target_directory, file_name))
            file_size = os.path.getsize(stage_file)
            result_df.loc[len(result_df)] = dict(
                zip(
                    GET_RESULT_KEYS,
                    [
                        file_name,
                        file_size,
                        "DOWNLOADED",
                        "",
                    ],
                )
            )
        return result_df

    def read_file(
        self,
        stage_location,
        format: str,
        schema: List[Attribute],
        analyzer: "MockAnalyzer",
        options: Dict[str, str],
    ) -> TableEmulator:
        from snowflake.snowpark.mock import CUSTOM_JSON_DECODER

        stage_source_dir_path = os.path.join(self._working_directory, stage_location)

        if os.path.isfile(stage_source_dir_path):
            local_files = [stage_source_dir_path]
        else:
            local_files = [
                os.path.join(stage_source_dir_path, f)
                for f in os.listdir(stage_source_dir_path)
                if os.path.isfile(os.path.join(stage_source_dir_path, f))
            ]

        file_format = format.lower()
        if file_format in SUPPORT_READ_OPTIONS:
            supported_options_for_format = SUPPORT_READ_OPTIONS[file_format]
            for option in options:
                if str(options[option]).upper() == "NONE":
                    # ignore if option value is None, or string of "None"
                    continue
                if (option not in supported_options_for_format) or (
                    supported_options_for_format[option]
                    and str(options[option]).upper()
                    not in supported_options_for_format[option]
                ):
                    # either the option is not supported or only partially supported
                    self._conn.log_not_supported_error(
                        external_feature_name=f"Read option '{option}' of value '{str(options[option])}' for file format '{file_format}'",
                        internal_feature_name="StageEntity.read_file",
                        parameters_info={
                            "format": format,
                            "option": option,
                            "option_value": str(options[option]),
                        },
                        raise_error=NotImplementedError
                        if RAISE_ERROR_ON_UNSUPPORTED_READ_OPTIONS
                        else None,
                        warning_logger=_logger,
                    )

        # process options
        purge = options.get("PURGE", False)

        # TODO: SNOW-1253672, there is a bug in the non-local testing code that
        #  snowflake.snowpark.dataframe_reader.DataFrameReader._infer_schema_for_file_format does not
        #  take PATTERN into account, when inferring schema from multiple files
        pattern = options.get("PATTERN") if options else None

        if pattern:
            local_files = [f for f in local_files if re.match(pattern, f)]

        if file_format == "csv":
            # check SNOW-1355487 for improvements
            skip_header = options.get("SKIP_HEADER", 0)
            skip_blank_lines = options.get("SKIP_BLANK_LINES", False)
            field_delimiter = options.get("FIELD_DELIMITER", ",")
            field_optionally_enclosed_by = options.get(
                "FIELD_OPTIONALLY_ENCLOSED_BY", None
            )
            null_if = options.get("NULL_IF", None)
            date_format, _ = convert_snowflake_datetime_format(
                options.get("DATE_FORMAT", None), DEFAULT_DATE_FORMAT
            )
            time_format, _ = convert_snowflake_datetime_format(
                options.get("TIME_FORMAT", None), DEFAULT_TIME_FORMAT
            )
            timestamp_format, _ = convert_snowflake_datetime_format(
                options.get("TIMESTAMP_FORMAT", None), DEFAULT_TIMESTAMP_FORMAT
            )

            if field_optionally_enclosed_by and len(field_optionally_enclosed_by) >= 2:
                raise SnowparkLocalTestingException(
                    f"Invalid value ['{field_optionally_enclosed_by}'] for parameter 'FIELD_OPTIONALLY_ENCLOSED_BY'"
                )
            if (
                field_delimiter[0]
                and field_delimiter[-1] == "'"
                and len(field_delimiter) >= 2
            ):
                # extract the field_delimiter as field_delimiter is normalized to be single quoted
                # e.g. field_delimiter="'.'", we should remove the normalized single quotes to extract the single char "."
                field_delimiter = field_delimiter[1:-1]

            # construct the returning dataframe
            result_df = TableEmulator()
            result_df_sf_types = {}
            converters_dict = {}
            for i in range(len(schema)):
                column_name = analyzer.analyze(schema[i], defaultdict(dict))
                column_series = ColumnEmulator(
                    data=None,
                    dtype=object,
                    name=column_name,
                    sf_type=ColumnType(schema[i].datatype, schema[i].nullable),
                )
                result_df[column_name], result_df_sf_types[column_name] = (
                    column_series,
                    column_series.sf_type,
                )
                if type(column_series.sf_type.datatype) not in CONVERT_MAP:
                    self._conn.log_not_supported_error(
                        error_message=f"Reading snowflake data type {type(column_series.sf_type.datatype)}"
                        " is not supported. It will be treated as a raw string in the dataframe.",
                        internal_feature_name="StageEntity.read_file",
                        parameters_info={
                            "format": format,
                            "column_series.sf_type.datatype": type(
                                column_series.sf_type.datatype
                            ).__name__,
                        },
                        warning_logger=_logger,
                    )
                    continue
                converter = CONVERT_MAP[type(column_series.sf_type.datatype)]
                kwargs = {
                    "datatype": column_series.sf_type.datatype,
                    "null_if": null_if,
                }
                if field_optionally_enclosed_by:
                    kwargs[
                        "field_optionally_enclosed_by"
                    ] = field_optionally_enclosed_by
                if isinstance(column_series.sf_type.datatype, DateType):
                    kwargs["format"] = date_format
                if isinstance(column_series.sf_type.datatype, TimeType):
                    kwargs["format"] = time_format
                if isinstance(column_series.sf_type.datatype, TimestampType):
                    kwargs["format"] = timestamp_format
                converters_dict[i] = partial(converter, **kwargs)
            for local_file in local_files:
                # pre-read to check columns number
                df = pd.read_csv(
                    local_file,
                    header=None,
                    skiprows=skip_header,
                    skip_blank_lines=skip_blank_lines,
                    delimiter=field_delimiter,
                )
                df.dtype = object
                if len(df.columns) != len(schema):
                    raise SnowparkLocalTestingException(
                        f"Number of columns in file ({len(df.columns)}) does not match that of"
                        f" the corresponding table ({len(schema)})."
                    )

                # read again with converters dict
                df = pd.read_csv(
                    local_file,
                    header=None,
                    skiprows=skip_header,
                    skip_blank_lines=skip_blank_lines,
                    delimiter=field_delimiter,
                    dtype=object,
                    converters=converters_dict,
                    # check definition here: https://docs.python.org/3/library/csv.html#csv.QUOTE_MINIMAL
                    # csv.QUOTE_MINIMAL, the engine will parse the value for us using the quote value/field_optionally_enclosed_by
                    # csv.QUOTE_NONE, by default snowflake FIELD_OPTIONALLY_ENCLOSED_BY is None
                    quoting=csv.QUOTE_MINIMAL
                    if field_optionally_enclosed_by
                    else csv.QUOTE_NONE,
                    quotechar=field_optionally_enclosed_by,
                )
                # set df columns to be result_df columns such that it can be concatenated
                df.columns = result_df.columns
                result_df = pd.concat([result_df, df], ignore_index=True)
            result_df.sf_types = result_df_sf_types
            return result_df
        elif file_format == "json":
            infer_schema_opt = options.get("INFER_SCHEMA", False)

            result_df = TableEmulator()
            result_df_sf_types = {}

            if not infer_schema_opt:
                # if infer schema option is False, then snowflake converts the data into
                # a single column table, values are treated as raw strings
                assert len(schema) == 1, (
                    f"[Local Testing] Unexpected schema length {len(schema)} when loading "
                    f"json data without inferring schema."
                )
                column_name = analyzer.analyze(schema[0])
                column_series = ColumnEmulator(
                    data=None,
                    dtype=object,
                    name=column_name,
                    sf_type=ColumnType(VariantType(), True),
                )
                result_df[column_name], result_df_sf_types[column_name] = (
                    column_series,
                    column_series.sf_type,
                )

                for local_file in local_files:
                    with open(local_file) as file:
                        content = json.load(file, cls=CUSTOM_JSON_DECODER)
                        df = pd.DataFrame({result_df.columns[0]: [content]})
                        result_df = pd.concat([result_df, df], ignore_index=True)
            else:
                # need to infer schema
                contents = []
                for local_file in local_files:
                    with open(local_file) as file:
                        content = json.load(file, cls=CUSTOM_JSON_DECODER)
                        tmp_content = {}
                        # snowflake escape double quotes by adding extra double quote
                        for key, value in content.items():
                            tmp_content[quote_name(key, keep_case=True)] = value
                        content = tmp_content
                        contents.append(content)
                        # extract the schema from the content
                        for column_name, value in content.items():
                            # snowflake double quote column name read from json file
                            target_datatype = infer_type(value)
                            # multiple json files can be of different schema
                            # if we find an existing schema but type is different from the inferred one
                            # we convert the column datatype to string, this is snowflake behavior
                            if column_name in result_df_sf_types and not isinstance(
                                target_datatype,
                                type(result_df_sf_types[column_name].datatype),
                            ):
                                # we cast target_datatype to VariantType first, and then we reuse mock_to_char
                                # which converts the data into StringType
                                target_datatype = VariantType()

                            column_series = ColumnEmulator(
                                data=None,
                                dtype=object,
                                name=column_name,
                                sf_type=ColumnType(target_datatype, nullable=True),
                            )
                            result_df[column_name], result_df_sf_types[column_name] = (
                                column_series,
                                column_series.sf_type,
                            )
                # fill empty cells with None value, this aligns with snowflake
                for content in contents:
                    for miss_key in set(result_df_sf_types.keys()) - set(
                        content.keys()
                    ):
                        content[miss_key] = None
                    df = TableEmulator([content])
                    result_df = pd.concat([result_df, df], ignore_index=True)
                # when concat is called, sf_type information gets lost, so we reset the type info in the end
                result_df.sf_types = result_df_sf_types

                # in the case that there are values of different types in the same column, snowflake will
                # convert data into string
                for col_name in result_df.columns:
                    if isinstance(result_df_sf_types[col_name].datatype, VariantType):
                        result_df[col_name] = mock_to_char(result_df[col_name])
                # snowflake output sorted column names
                sorted_columns = sorted(list(result_df.columns))
                result_df = result_df[sorted_columns]
                result_df.columns = sorted_columns

            result_df.sf_types = result_df_sf_types
            return result_df

        if purge and local_files:
            for file_path in local_files:
                try:
                    os.remove(file_path)
                except Exception as exc:
                    _logger.debug(f"failed to remove file due to exception {exc}")

        self._conn.log_not_supported_error(
            external_feature_name=f"Read file format {format}",
            internal_feature_name="StageEntity.read_file",
            parameters_info={"format": format},
            raise_error=NotImplementedError,
        )


class StageEntityRegistry:
    # Registry to store tables and views.
    def __init__(self, conn: "MockServerConnection") -> None:
        self._root_dir = tempfile.TemporaryDirectory()
        self._stage_registry = {}
        self._conn = conn
        self._lock = conn.get_lock()

    def create_or_replace_stage(self, stage_name):
        with self._lock:
            self._stage_registry[stage_name] = StageEntity(
                self._root_dir.name, stage_name, self._conn
            )

    def __getitem__(self, stage_name: str):
        # the assumption here is that stage always exists
        with self._lock:
            if stage_name not in self._stage_registry:
                self.create_or_replace_stage(stage_name)
            return self._stage_registry[stage_name]

    def put(
        self, local_file_name: str, stage_location: str, overwrite: bool = False
    ) -> TableEmulator:
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
        # the assumption here is that stage always exists
        with self._lock:
            if stage_name not in self._stage_registry:
                self.create_or_replace_stage(stage_name)
            return self._stage_registry[stage_name].put_file(
                local_file_name=local_file_name,
                stage_prefix=stage_prefix,
                overwrite=overwrite,
            )

    def upload_stream(
        self,
        input_stream: IO[bytes],
        stage_location: str,
        file_name: str,
        overwrite: bool = False,
    ) -> Dict:
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
        # the assumption here is that stage always exists
        with self._lock:
            if stage_name not in self._stage_registry:
                self.create_or_replace_stage(stage_name)
            return self._stage_registry[stage_name].upload_stream(
                input_stream=input_stream,
                stage_prefix=stage_prefix,
                file_name=file_name,
                overwrite=overwrite,
            )

    def get(
        self,
        stage_location: str,
        target_directory: str,
        options: Dict[str, str] = None,
    ):
        if not stage_location.startswith(("@", "snow://")):
            raise SnowparkLocalTestingException(
                _INVALID_STAGE_LOCATION_ERR_MSG(stage_location)
            )
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
        with self._lock:
            if stage_name not in self._stage_registry:
                self.create_or_replace_stage(stage_name)

            return self._stage_registry[stage_name].get_file(
                stage_location=stage_prefix,
                target_directory=target_directory,
                options=options,
            )

    def read_file(
        self,
        stage_location,
        format: str,
        schema: List[Attribute],
        analyzer: "MockAnalyzer",
        options: Dict[str, str],
    ):
        if not stage_location.startswith(("@", "snow://")):
            raise SnowparkLocalTestingException(
                _INVALID_STAGE_LOCATION_ERR_MSG(stage_location)
            )
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
        with self._lock:
            if stage_name not in self._stage_registry:
                self.create_or_replace_stage(stage_name)

            return self._stage_registry[stage_name].read_file(
                stage_location=stage_prefix,
                format=format,
                schema=schema,
                analyzer=analyzer,
                options=options,
            )
