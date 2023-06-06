#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import datetime
import os
from collections import defaultdict
from functools import partial
from logging import getLogger
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock.constants import SUPPORTED_CSV_READ_OPTIONS
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, TableEmulator
from snowflake.snowpark.types import (
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
    TimeType,
)

if TYPE_CHECKING:
    from snowflake.snowpark.mock.mock_analyzer import MockAnalyzer


_logger = getLogger(__name__)

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_DATE_FORMAT = "%Y-%m-%d"
_TIME_FORMAT = "%H:%M:%S"

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


def _integer_snowflake_to_pandas_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[int]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(value) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return int(value)
    except ValueError:
        raise SnowparkSQLException(
            f"[Local Testing] Numeric value '{value}' is not recognized."
        )


def _fraction_snowflake_to_pandas_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[float]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return float(value)
    except ValueError:
        raise SnowparkSQLException(
            f"[Local Testing] Numeric value '{value}' is not recognized."
        )


def _bool_snowflake_to_pandas_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[bool]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    if value.lower() in ("true", "t", "yes", "y", "on", "1"):
        return True
    if value.lower() in ("false", "f", "no", "n", "off", "0"):
        return False
    try:
        float_value = float(value)
        return bool(float_value != 0)
    except TypeError:
        raise SnowparkSQLException(
            f"[Local Testing] Boolean value '{value}' is not recognized."
        )


def _string_snowflake_to_pandas_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[str]:
    if value is None or value == "":
        return value
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    return value


def _date_snowflake_to_pandas_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[datetime.date]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return datetime.datetime.strptime(value, _DATE_FORMAT).date()
    except Exception as e:
        raise SnowparkSQLException(
            f"[Local Testing] DATE value '{value}' is not recognized due to error {e!r}."
        )


def _timestamp_snowflake_to_pandas_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[datetime.datetime]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return datetime.datetime.strptime(value, _TIMESTAMP_FORMAT)
    except Exception as e:
        raise SnowparkSQLException(
            f"[Local Testing] TIMESTAMP value '{value}' is not recognized due to error {e!r}."
        )


def _time_snowflake_to_pandas_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[datetime.time]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return datetime.datetime.strptime(value, _TIME_FORMAT).time()
    except Exception as e:
        raise SnowparkSQLException(
            f"[Local Testing] TIMESTAMP value '{value}' is not recognized due to error {e!r}."
        )


CONVERT_MAP = {
    IntegerType: _integer_snowflake_to_pandas_converter,
    LongType: _integer_snowflake_to_pandas_converter,
    ByteType: _integer_snowflake_to_pandas_converter,
    ShortType: _integer_snowflake_to_pandas_converter,
    DoubleType: _fraction_snowflake_to_pandas_converter,
    FloatType: _fraction_snowflake_to_pandas_converter,
    DecimalType: _fraction_snowflake_to_pandas_converter,
    BooleanType: _bool_snowflake_to_pandas_converter,
    DateType: _date_snowflake_to_pandas_converter,
    TimeType: _time_snowflake_to_pandas_converter,
    TimestampType: _timestamp_snowflake_to_pandas_converter,
    StringType: _string_snowflake_to_pandas_converter,
}


def put(local_file_name: str, stage_location: str) -> TableEmulator:
    """
    Put a file into in memory map, key being stage location and value being the local file path
    """
    local_file_name = local_file_name[
        8:-1
    ]  # skip normalized prefix `file:// and suffix `
    file_name = os.path.basename(local_file_name)
    remote_file_path = f"{stage_location}/{file_name}"
    _FILE_STAGE_MAP[remote_file_path] = local_file_name
    _STAGE_FILE_MAP[f"{stage_location}/"].add(local_file_name)
    file_size = os.path.getsize(os.path.expanduser(local_file_name))

    result_df = TableEmulator(
        columns=PUT_RESULT_KEYS,
        sf_types={
            "source": StringType(),
            "target": StringType(),
            "source_size": DecimalType(10, 0),
            "target_size": DecimalType(10, 0),
            "source_compression": StringType(),
            "target_compression": StringType(),
            "status": StringType(),
            "message": StringType(),
        },
        dtype=object,
    )
    result_df = result_df.append(
        {
            k: v
            for k, v in zip(
                PUT_RESULT_KEYS,
                [file_name, file_name, file_size, file_size, None, None, None, None],
            )
        },
        ignore_index=True,
    )
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
                    f"[Local Testing] read option {option} is not supported."
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
            field_delimiter = field_delimiter[1:-1]

        # construct the returning dataframe
        result_df = TableEmulator()
        convertersdict = {}
        for i in range(len(schema)):
            column_name = analyzer.analyze(schema[i])
            column_series = ColumnEmulator(data=None, dtype=object, name=column_name)
            column_series.sf_type = schema[i].datatype
            result_df[column_name] = column_series
            if type(column_series.sf_type) not in CONVERT_MAP:
                raise NotImplementedError(
                    f"[Local Testing] Snowflake data type {type(column_series.sf_type)} is not supported."
                )
            converter = CONVERT_MAP[type(column_series.sf_type)]
            convertersdict[i] = (
                partial(
                    converter,
                    datatype=column_series.sf_type,
                    field_optionally_enclosed_by=field_optionally_enclosed_by,
                )
                if field_optionally_enclosed_by
                else partial(converter, datatype=column_series.sf_type)
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
                converters=convertersdict,
                quoting=3,  # QUOTE_NONE
            )
            # set df columns to be result_df columns such that it can be concatenated
            df.columns = result_df.columns
            result_df = pd.concat([result_df, df], ignore_index=True)
        return result_df
    raise NotImplementedError(f"[Local Testing] File format {format} is not supported.")
