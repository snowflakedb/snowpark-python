#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import binascii
import json
import math
from array import array
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import snowflake.snowpark._internal.analyzer.analyzer_utils as analyzer_utils
from snowflake.snowpark._internal.type_utils import convert_sp_to_sf_type
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    GeographyType,
    MapType,
    NullType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)

MILLIS_PER_DAY = 24 * 3600 * 1000
MICROS_PER_MILLIS = 1000


def str_to_sql(value: str) -> str:
    sql_str = str(value).replace("\\", "\\\\").replace("'", "''").replace("\n", "\\n")
    return f"'{sql_str}'"


def to_sql(value: Any, datatype: DataType, from_values_statement: bool = False) -> str:
    """Convert a value with DataType to a snowflake compatible sql"""

    # Handle null values
    if isinstance(
        datatype,
        (NullType, ArrayType, MapType, StructType, GeographyType),
    ):
        if value is None:
            return "NULL"
    if isinstance(datatype, BinaryType):
        if value is None:
            return "NULL :: BINARY"
    if isinstance(datatype, _IntegralType):
        if value is None:
            return "NULL :: INT"
    if isinstance(datatype, _FractionalType):
        if value is None:
            return "NULL :: FLOAT"
    if isinstance(datatype, StringType):
        if value is None:
            return "NULL :: STRING"
    if isinstance(datatype, BooleanType):
        if value is None:
            return "NULL :: BOOLEAN"
    if value is None:
        return "NULL"

    # Not nulls
    if isinstance(value, str) and isinstance(datatype, StringType):
        # If this is used in a values statement (e.g., create_dataframe),
        # the sql value has to be casted to make sure the varchar length
        # will not be limited.
        return (
            f"{str_to_sql(value)} :: STRING"
            if from_values_statement
            else str_to_sql(value)
        )

    if isinstance(datatype, _IntegralType):
        return f"{value} :: INT"

    if isinstance(datatype, BooleanType):
        return f"{value} :: BOOLEAN"

    if isinstance(value, float) and isinstance(datatype, _FractionalType):
        if math.isnan(value):
            cast_value = "'NaN'"
        elif math.isinf(value) and value > 0:
            cast_value = "'inf'"
        elif math.isinf(value) and value < 0:
            cast_value = "'-inf'"
        else:
            cast_value = f"'{value}'"
        return f"{cast_value} :: FLOAT"

    if isinstance(value, Decimal) and isinstance(datatype, DecimalType):
        return f"{value} :: {analyzer_utils.number(datatype.precision, datatype.scale)}"

    if isinstance(datatype, DateType):
        if isinstance(value, int):
            # add value as number of days to 1970-01-01
            target_date = date(1970, 1, 1) + timedelta(days=value)
            return f"DATE '{target_date.isoformat()}'"
        elif isinstance(value, date):
            return f"DATE '{value.isoformat()}'"

    if isinstance(datatype, TimestampType):
        if isinstance(value, int):
            # add value as microseconds to 1970-01-01 00:00:00.00.
            target_time = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
                microseconds=value
            )
            trimmed_ms = target_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            return f"TIMESTAMP '{trimmed_ms}'"
        elif isinstance(value, datetime):
            trimmed_ms = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            return f"TIMESTAMP '{trimmed_ms}'"

    if isinstance(datatype, TimeType):
        if isinstance(value, time):
            trimmed_ms = value.strftime("%H:%M:%S.%f")[:-3]
            return f"TIME('{trimmed_ms}')"

    if isinstance(value, (list, bytes, bytearray)) and isinstance(datatype, BinaryType):
        return f"'{binascii.hexlify(value).decode()}' :: BINARY"

    if isinstance(value, (list, tuple, array)) and isinstance(datatype, ArrayType):
        return f"PARSE_JSON({str_to_sql(json.dumps(value))}) :: ARRAY"

    if isinstance(value, dict) and isinstance(datatype, MapType):
        return f"PARSE_JSON({str_to_sql(json.dumps(value))}) :: OBJECT"

    raise TypeError(f"Unsupported datatype {datatype}, value {value} by to_sql()")


def schema_expression(data_type: DataType, is_nullable: bool) -> str:
    if is_nullable:
        if isinstance(data_type, GeographyType):
            return "TRY_TO_GEOGRAPHY(NULL)"
        if isinstance(data_type, ArrayType):
            return "PARSE_JSON('NULL') :: ARRAY"
        if isinstance(data_type, MapType):
            return "PARSE_JSON('NULL') :: OBJECT"
        if isinstance(data_type, VariantType):
            return "PARSE_JSON('NULL') :: VARIANT"
        return "NULL :: " + convert_sp_to_sf_type(data_type)

    if isinstance(data_type, _NumericType):
        return "0 :: " + convert_sp_to_sf_type(data_type)
    if isinstance(data_type, StringType):
        return "'a' :: STRING"
    if isinstance(data_type, BinaryType):
        return "to_binary(hex_encode(1))"
    if isinstance(data_type, DateType):
        return "date('2020-9-16')"
    if isinstance(data_type, BooleanType):
        return "true"
    if isinstance(data_type, TimeType):
        return "to_time('04:15:29.999')"
    if isinstance(data_type, TimestampType):
        return "to_timestamp_ntz('2020-09-16 06:30:00')"
    if isinstance(data_type, ArrayType):
        return "to_array(0)"
    if isinstance(data_type, MapType):
        return "to_object(parse_json('0'))"
    if isinstance(data_type, VariantType):
        return "to_variant(0)"
    if isinstance(data_type, GeographyType):
        return "to_geography('POINT(-122.35 37.55)')"
    raise Exception(f"Unsupported data type: {data_type.__class__.__name__}")


def to_sql_without_cast(value: Any, datatype: DataType) -> str:
    if value is None:
        return "NULL"
    if isinstance(datatype, StringType):
        return f"'{value}'"
    return str(value)
