#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
from typing import TYPE_CHECKING, Dict, List

import pandas as pd

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, TableEmulator
from snowflake.snowpark.types import (
    DecimalType,
    StringType,
    _FractionalType,
    _IntegralType,
)

if TYPE_CHECKING:
    from snowflake.snowpark.mock.mock_analyzer import MockAnalyzer

_FILE_STAGE_MAP = {}

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
    local_file = _FILE_STAGE_MAP[stage_location]
    if format.lower() == "csv":
        skip_header = options.get("SKIP_HEADER", 0)
        skip_blank_lines = options.get("SKIP_BLANK_LINES", False)
        field_delimiter = options.get("FIELD_DELIMITER", ",")
        result_df = TableEmulator()
        df = pd.read_csv(
            local_file,
            header=None,
            skiprows=skip_header,
            skip_blank_lines=skip_blank_lines,
            delimiter=field_delimiter,
        )
        if len(df.columns) != len(schema):
            raise SnowparkSQLException(
                f"Number of columns in file ({len(df.columns)}) does not match that of"
                f" the corresponding table ({len(schema)})."
            )
        for i in range(len(schema)):
            column_name = analyzer.analyze(schema[i])
            column_series = ColumnEmulator(
                data=df.iloc[:, i], dtype=object, name=column_name
            )
            validate_column_data(column_series, schema[i])
            column_series.sf_type = schema[i].datatype
            result_df[column_name] = column_series
        return result_df
    raise NotImplementedError(f"[Local Testing] File format {format} is not supported.")


def validate_column_data(column: ColumnEmulator, attribute: Attribute):
    sf_type = attribute.datatype
    nullable = attribute.nullable
    for data in column:
        if data is None and not nullable:
            # TODO: raise error
            pass
        if isinstance(sf_type, _IntegralType):
            if not isinstance(data, int):
                raise SnowparkSQLException(f"Numeric value '{data}' is not recognized.")
        elif isinstance(data, _FractionalType):
            if not isinstance(data, (float, int)):
                raise SnowparkSQLException(f"Numeric value '{data}' is not recognized.")
        else:
            pass
            # TODO: more checks
    pass
