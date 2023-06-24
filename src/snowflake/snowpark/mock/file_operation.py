#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import os
from collections import defaultdict
from functools import partial
from logging import getLogger
from typing import TYPE_CHECKING, Dict, List

import pandas as pd

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock.snowflake_data_type import (
    ColumnEmulator,
    ColumnType,
    TableEmulator,
)
from snowflake.snowpark.mock.snowflake_to_pandas_converter import CONVERT_MAP
from snowflake.snowpark.types import DecimalType, StringType

if TYPE_CHECKING:
    from snowflake.snowpark.mock.analyzer import MockAnalyzer


_logger = getLogger(__name__)

# this map keeps the information for remote file to local file,
# key is the remote 'stage/file', value is the local file path
_FILE_STAGE_MAP = {}

# this map keeps the information the files each stage stores
# key is the remote 'stage', value is the set of file names stored in the stage
_STAGE_FILE_MAP = defaultdict(set)

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

SUPPORTED_CSV_READ_OPTIONS = (
    "SKIP_HEADER",
    "SKIP_BLANK_LINES",
    "FIELD_DELIMITER",
    "FIELD_OPTIONALLY_ENCLOSED_BY",
)


def put(
    local_file_name: str, stage_location: str, auto_compress: bool = True
) -> TableEmulator:
    """
    Put a file into in memory map, key being stage location and value being the local file path
    """
    local_file_name = local_file_name[
        len("`file://") : -1
    ]  # skip normalized prefix `file:// and suffix `
    file_name = os.path.basename(local_file_name)
    remote_file_path = f"{stage_location}/{file_name}"
    _FILE_STAGE_MAP[remote_file_path] = local_file_name
    _STAGE_FILE_MAP[f"{stage_location}/"].add(local_file_name)
    file_size = os.path.getsize(os.path.expanduser(local_file_name))

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
    result_df.loc[len(result_df)] = {
        k: v
        for k, v in zip(
            PUT_RESULT_KEYS,
            [file_name, file_name, file_size, file_size, None, None, None, None],
        )
    }
    return result_df


def read_file(
    stage_location,
    format: str,
    schema: List[Attribute],
    analyzer: "MockAnalyzer",
    options: Dict[str, str],
) -> TableEmulator:
    try:
        local_files = [_FILE_STAGE_MAP[stage_location]]
    except KeyError:
        if stage_location and stage_location[0] != "@":
            raise SnowparkSQLException("SQL compilation error")
        if stage_location in _STAGE_FILE_MAP.keys():
            # all files within the stage
            local_files = list(_STAGE_FILE_MAP[stage_location])
        else:
            raise
    if format.lower() == "csv":
        for option in options:
            if option not in SUPPORTED_CSV_READ_OPTIONS:
                _logger.warning(
                    f"[Local Testing] read file option {option} is not supported."
                )
        skip_header = options.get("SKIP_HEADER", 0)
        skip_blank_lines = options.get("SKIP_BLANK_LINES", False)
        field_delimiter = options.get("FIELD_DELIMITER", ",")
        field_optionally_enclosed_by = options.get("FIELD_OPTIONALLY_ENCLOSED_BY", None)
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
            column_series = ColumnEmulator(data=None, dtype=object, name=column_name)
            column_series.sf_type = ColumnType(schema[i].datatype, schema[i].nullable)
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
                dtype=object,
            )

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
    raise NotImplementedError(f"[Local Testing] File format {format} is not supported.")
