#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import glob
import os
import re
import shutil
import tempfile
import uuid
from functools import partial
from logging import getLogger
from typing import IO, TYPE_CHECKING, Dict, List, Tuple

from snowflake.connector.options import pandas as pd
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.utils import unwrap_stage_location_single_quote
from snowflake.snowpark.exceptions import (
    SnowparkSQLException,
    SnowparkUploadFileException,
)
from snowflake.snowpark.mock._snowflake_data_type import (
    ColumnEmulator,
    ColumnType,
    TableEmulator,
)
from snowflake.snowpark.mock._snowflake_to_pandas_converter import CONVERT_MAP
from snowflake.snowpark.types import DecimalType, StringType

if TYPE_CHECKING:
    from snowflake.snowpark.mock._analyzer import MockAnalyzer
    from snowflake.snowpark.mock._connection import MockServerConnection

_logger = getLogger(__name__)


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


SUPPORTED_CSV_READ_OPTIONS = (
    "SKIP_HEADER",
    "SKIP_BLANK_LINES",
    "FIELD_DELIMITER",
    "FIELD_OPTIONALLY_ENCLOSED_BY",
)


def extract_stage_name_and_prefix(stage_location: str) -> Tuple[str, str]:
    """
    extra the stage name and dir path in the stage_location
    inspired by utils.get_stage_file_prefix_length

    currently we don't suppose fully qualified name space stage
    """
    if not stage_location.startswith("@"):
        raise SnowparkSQLException("SQL compilation error")

    normalized = unwrap_stage_location_single_quote(stage_location)
    if not normalized.endswith("/"):
        normalized = f"{normalized}/"
    normalized = normalized[1:]  # remove the beginning '@'

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
            raise SnowparkSQLException(f"Invalid stage_location {stage_location}.")
    else:
        stage_name_end_idx = normalized.find("/")
        prefix_start_idx = stage_name_end_idx + 1
    stage_name = normalized[stage_name_start_idx:stage_name_end_idx]
    dir_path = normalized[prefix_start_idx:-1]  # remove the first and last '/'
    return stage_name, dir_path


class StageEntity:
    def __init__(self, root_dir_path: str, stage_name: str) -> None:
        self._stage_name = stage_name
        # stage name might contain special chars which can not be used as dir name
        # so we generate uuid as name
        self._dir_name = str(uuid.uuid4())
        self._working_directory = os.path.join(root_dir_path, self._dir_name)

        if os.path.exists(self._working_directory):
            shutil.rmtree(self._working_directory)

        os.mkdir(self._working_directory)
        self._files = set()

    def put_file(
        self, local_file_name: str, stage_prefix: str, overwrite: bool = False
    ) -> TableEmulator:
        local_file_name = local_file_name[
            len("`file://") : -1
        ]  # skip normalized prefix `file:// and suffix `
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
            raise SnowparkSQLException(f"File doesn't exist: {local_file_name}")

        for local_file_name in list_of_files:

            file_name = os.path.basename(local_file_name)
            stage_target_dir_path = os.path.join(self._working_directory, stage_prefix)

            if not os.path.exists(stage_target_dir_path):
                os.makedirs(stage_target_dir_path)

            if (
                os.path.isfile(os.path.join(stage_target_dir_path, file_name))
                and not overwrite
            ):
                status = "SKIPPED"
            else:
                shutil.copy(local_file_name, stage_target_dir_path)
                status = "UPLOADED"

            file_size = os.path.getsize(os.path.expanduser(local_file_name))
            result_df.loc[len(result_df)] = {
                k: v
                for k, v in zip(
                    PUT_RESULT_KEYS,
                    [
                        file_name,
                        file_name,
                        file_size,
                        file_size,
                        "NONE",
                        "NONE",
                        status,
                        "",
                    ],
                )
            }
        return result_df

    def upload_stream(
        self,
        input_stream: IO[bytes],
        stage_prefix: str,
        file_name: str,
        overwrite: bool = False,
    ) -> Dict:
        stage_target_dir_path = os.path.join(self._working_directory, stage_prefix)

        if not os.path.exists(stage_target_dir_path):
            os.makedirs(stage_target_dir_path)

        status = "UPLOADED"
        if (
            os.path.isfile(os.path.join(stage_target_dir_path, file_name))
            and not overwrite
        ):
            status = "SKIPPED"
        else:
            with open(os.path.join(stage_target_dir_path, file_name), "wb") as f:
                try:
                    f.write(input_stream.read())
                except ValueError as exc:
                    raise SnowparkUploadFileException(
                        message="[Local Testing] Reading closed file while uploading stream",
                        error_code="1408",
                    ) from exc

        file_size = os.path.getsize(os.path.join(stage_target_dir_path, file_name))
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

        if not os.path.exists(stage_source_dir_path):
            raise SnowparkSQLException(
                f"[Local Testing] the file does not exist: {stage_source_dir_path}"
            )

        list_of_files = (
            sorted(
                os.path.join(root, file)
                for root, dirs, files in os.walk(stage_source_dir_path)
                for file in files
            )
            if os.path.isdir(stage_source_dir_path)
            else [stage_source_dir_path]
        )

        pattern = options.get("pattern") if options else None

        for file in list_of_files:
            file_name = os.path.basename(file)
            # pattern[1:-1] to remove heading and tailing single quotes
            if pattern and not re.match(pattern[1:-1], file_name):
                continue
            shutil.copy(file, os.path.join(target_directory, file_name))
            file_size = os.path.getsize(file)
            result_df.loc[len(result_df)] = {
                k: v
                for k, v in zip(
                    GET_RESULT_KEYS,
                    [
                        file_name,
                        file_size,
                        "DOWNLOADED",
                        "",
                    ],
                )
            }
        return result_df

    def read_file(
        self,
        stage_location,
        format: str,
        schema: List[Attribute],
        analyzer: "MockAnalyzer",
        options: Dict[str, str],
    ) -> TableEmulator:
        stage_source_dir_path = os.path.join(self._working_directory, stage_location)
        local_files = (
            [stage_source_dir_path]
            if os.path.isfile(stage_source_dir_path)
            else [
                os.path.join(stage_source_dir_path, f)
                for f in os.listdir(stage_source_dir_path)
            ]
        )

        if format.lower() == "csv":
            for option in options:
                if option not in SUPPORTED_CSV_READ_OPTIONS:
                    _logger.warning(
                        f"[Local Testing] read file option {option} is not supported."
                    )
            skip_header = options.get("SKIP_HEADER", 0)
            skip_blank_lines = options.get("SKIP_BLANK_LINES", False)
            field_delimiter = options.get("FIELD_DELIMITER", ",")
            field_optionally_enclosed_by = options.get(
                "FIELD_OPTIONALLY_ENCLOSED_BY", None
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
                column_name = analyzer.analyze(schema[i])
                column_series = ColumnEmulator(
                    data=None, dtype=object, name=column_name
                )
                column_series.sf_type = ColumnType(
                    schema[i].datatype, schema[i].nullable
                )
                result_df[column_name] = column_series
                result_df_sf_types[column_name] = column_series.sf_type
                if type(column_series.sf_type.datatype) not in CONVERT_MAP:
                    _logger.warning(
                        f"[Local Testing] Reading snowflake data type {type(column_series.sf_type.datatype)} is not supported. It will be treated as a raw string in the dataframe."
                    )
                    continue
                converter = CONVERT_MAP[type(column_series.sf_type.datatype)]
                converters_dict[i] = (
                    partial(
                        converter,
                        datatype=column_series.sf_type.datatype,
                        field_optionally_enclosed_by=field_optionally_enclosed_by,
                    )
                    if field_optionally_enclosed_by
                    else partial(converter, datatype=column_series.sf_type.datatype)
                )

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
                    raise SnowparkSQLException(
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
                    quoting=3,  # QUOTE_NONE
                )
                # set df columns to be result_df columns such that it can be concatenated
                df.columns = result_df.columns
                result_df = pd.concat([result_df, df], ignore_index=True)
            result_df.sf_types = result_df_sf_types
            return result_df
        raise NotImplementedError(
            f"[Local Testing] File format {format} is not supported."
        )


class StageEntityRegistry:
    # Registry to store tables and views.
    def __init__(self, conn: "MockServerConnection") -> None:
        self._root_dir = tempfile.TemporaryDirectory()
        self._stage_registry = {}

    def create_or_replace_stage(self, stage_name):
        self._stage_registry[stage_name] = StageEntity(self._root_dir.name, stage_name)

    def __getitem__(self, stage_name: str):
        # the assumption here is that stage always exists
        return self._stage_registry[stage_name]

    def put(
        self, local_file_name: str, stage_location: str, overwrite: bool = False
    ) -> TableEmulator:
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
        # the assumption here is that stage always exists
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
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
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
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
        if stage_name not in self._stage_registry:
            self.create_or_replace_stage(stage_name)

        return self._stage_registry[stage_name].read_file(
            stage_location=stage_prefix,
            format=format,
            schema=schema,
            analyzer=analyzer,
            options=options,
        )
