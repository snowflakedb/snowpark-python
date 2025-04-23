#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
from snowflake.snowpark._internal.utils import (
    PythonObjJSONEncoder,
)
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    GeographyType,
    GeometryType,
    MapType,
    NullType,
    StringType,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
    VectorType,
    FileType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)

MILLIS_PER_DAY = 24 * 3600 * 1000
MICROS_PER_MILLIS = 1000


def str_to_sql(value: str) -> str:
    sql_str = str(value).replace("\\", "\\\\").replace("'", "''").replace("\n", "\\n")
    return f"'{sql_str}'"


def float_nan_inf_to_sql(value: float) -> str:
    """
    convert the float nan and inf value to a snowflake compatible sql.
    Note that nan and inf value will always require a cast with ::FLOAT in snowflake
    """
    if math.isnan(value):
        cast_value = "'NAN'"
    elif math.isinf(value) and value > 0:
        cast_value = "'INF'"
    elif math.isinf(value) and value < 0:
        cast_value = "'-INF'"
    else:
        raise ValueError("None inf or nan float value is received")

    return f"{cast_value} :: FLOAT"


def to_sql_no_cast(
    value: Any,
    datatype: DataType,
) -> str:
    if value is None:
        return "NULL"
    if isinstance(datatype, VariantType):
        # PARSE_JSON returns VARIANT, so no need to append :: VARIANT here explicitly.
        return f"PARSE_JSON({str_to_sql(json.dumps(value, cls=PythonObjJSONEncoder))})"
    if isinstance(value, str):
        if isinstance(datatype, GeographyType):
            return f"TO_GEOGRAPHY({str_to_sql(value)})"
        if isinstance(datatype, GeometryType):
            return f"TO_GEOMETRY({str_to_sql(value)})"
        return str_to_sql(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        cast_value = float_nan_inf_to_sql(value)
        return cast_value[:-9]
    if isinstance(value, (list, bytes, bytearray)) and isinstance(datatype, BinaryType):
        return str(bytes(value))
    if isinstance(value, (list, tuple, array)) and isinstance(datatype, ArrayType):
        return f"PARSE_JSON({str_to_sql(json.dumps(value, cls=PythonObjJSONEncoder))})"
    if isinstance(value, dict) and isinstance(datatype, MapType):
        return f"PARSE_JSON({str_to_sql(json.dumps(value, cls=PythonObjJSONEncoder))})"
    if isinstance(datatype, DateType):
        if isinstance(value, int):
            # add value as number of days to 1970-01-01
            target_date = date(1970, 1, 1) + timedelta(days=value)
            return f"'{target_date.isoformat()}'"
        elif isinstance(value, date):
            return f"'{value.isoformat()}'"

    if isinstance(datatype, TimestampType):
        if isinstance(value, (int, datetime)):
            if isinstance(value, int):
                # add value as microseconds to 1970-01-01 00:00:00.00.
                value = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
                    microseconds=value
                )
            return f"'{value}'"
    return f"{value}"


def to_sql(
    value: Any,
    datatype: DataType,
    from_values_statement: bool = False,
) -> str:
    """Convert a value with DataType to a snowflake compatible sql"""
    # Handle null values
    if isinstance(
        datatype,
        (NullType, ArrayType, MapType, StructType, GeographyType, GeometryType),
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
            return f"NULL :: {analyzer_utils.string(datatype.length)}"
    if isinstance(datatype, BooleanType):
        if value is None:
            return "NULL :: BOOLEAN"
    if isinstance(datatype, VariantType):
        if value is None:
            return "NULL :: VARIANT"
    if isinstance(datatype, VectorType):
        if value is None:
            return f"NULL :: VECTOR({datatype.element_type},{datatype.dimension})"
    if isinstance(datatype, FileType):
        if value is None:
            return "TO_FILE(NULL)"
    if value is None:
        return "NULL"

    # Not nulls
    if isinstance(value, str) and isinstance(datatype, StringType):
        # If this is used in a values statement (e.g., create_dataframe),
        # the sql value has to be casted to make sure the varchar length
        # will not be limited.
        return (
            f"{str_to_sql(value)} :: {analyzer_utils.string(datatype.length)}"
            if from_values_statement
            else str_to_sql(value)
        )

    if isinstance(datatype, _IntegralType):
        return f"{value} :: INT"

    if isinstance(datatype, BooleanType):
        return f"{value} :: BOOLEAN"

    if isinstance(value, float) and isinstance(datatype, _FractionalType):
        if math.isnan(value) or math.isinf(value):
            return float_nan_inf_to_sql(value)
        else:
            return f"'{value}' :: FLOAT"

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
        if isinstance(value, (int, datetime)):
            if isinstance(value, int):
                # add value as microseconds to 1970-01-01 00:00:00.00.
                value = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
                    microseconds=value
                )
            if datatype.tz == TimestampTimeZone.NTZ:
                return f"'{value}'::TIMESTAMP_NTZ"
            elif datatype.tz == TimestampTimeZone.LTZ:
                return f"'{value}'::TIMESTAMP_LTZ"
            elif datatype.tz == TimestampTimeZone.TZ:
                return f"'{value}'::TIMESTAMP_TZ"
            else:
                return f"TIMESTAMP '{value}'"

    if isinstance(datatype, TimeType):
        if isinstance(value, time):
            trimmed_ms = value.strftime("%H:%M:%S.%f")[:-3]
            return f"TIME('{trimmed_ms}')"

    if isinstance(value, (list, bytes, bytearray)) and isinstance(datatype, BinaryType):
        return f"'{binascii.hexlify(bytes(value)).decode()}' :: BINARY"

    if isinstance(value, (list, tuple, array)) and isinstance(datatype, ArrayType):
        type_str = "ARRAY"
        if datatype.structured:
            type_str = convert_sp_to_sf_type(datatype)
        return f"PARSE_JSON({str_to_sql(json.dumps(value, cls=PythonObjJSONEncoder))}) :: {type_str}"

    if isinstance(value, dict) and isinstance(datatype, MapType):
        type_str = "OBJECT"
        if datatype.structured:
            type_str = convert_sp_to_sf_type(datatype)
        return f"PARSE_JSON({str_to_sql(json.dumps(value, cls=PythonObjJSONEncoder))}) :: {type_str}"

    if isinstance(datatype, VariantType):
        # PARSE_JSON returns VARIANT, so no need to append :: VARIANT here explicitly.
        return f"PARSE_JSON({str_to_sql(json.dumps(value, cls=PythonObjJSONEncoder))})"

    if isinstance(value, str) and isinstance(datatype, GeographyType):
        return f"TO_GEOGRAPHY({str_to_sql(value)})"

    if isinstance(value, str) and isinstance(datatype, GeometryType):
        return f"TO_GEOMETRY({str_to_sql(value)})"

    if isinstance(datatype, VectorType):
        return f"{value} :: VECTOR({datatype.element_type},{datatype.dimension})"

    if isinstance(value, str) and isinstance(datatype, FileType):
        return f"TO_FILE({str_to_sql(value)})"

    raise TypeError(f"Unsupported datatype {datatype}, value {value} by to_sql()")


def schema_expression(data_type: DataType, is_nullable: bool) -> str:
    if is_nullable:
        if isinstance(data_type, GeographyType):
            return "TRY_TO_GEOGRAPHY(NULL)"
        if isinstance(data_type, GeometryType):
            return "TRY_TO_GEOMETRY(NULL)"
        if isinstance(data_type, ArrayType) and not data_type.structured:
            return "PARSE_JSON('NULL') :: ARRAY"
        if isinstance(data_type, MapType) and not data_type.structured:
            return "PARSE_JSON('NULL') :: OBJECT"
        if isinstance(data_type, VariantType):
            return "PARSE_JSON('NULL') :: VARIANT"
        return "NULL :: " + convert_sp_to_sf_type(data_type)

    if isinstance(data_type, _NumericType):
        return "0 :: " + convert_sp_to_sf_type(data_type)
    if isinstance(data_type, StringType):
        return f"'a' :: {analyzer_utils.string(data_type.length)}"
    if isinstance(data_type, BinaryType):
        return "'01' :: BINARY"
    if isinstance(data_type, DateType):
        return "date('2020-9-16')"
    if isinstance(data_type, BooleanType):
        return "true"
    if isinstance(data_type, TimeType):
        return "to_time('04:15:29.999')"
    if isinstance(data_type, TimestampType):
        if data_type.tz == TimestampTimeZone.NTZ:
            return "to_timestamp_ntz('2020-09-16 06:30:00')"
        elif data_type.tz == TimestampTimeZone.LTZ:
            return "to_timestamp_ltz('2020-09-16 06:30:00')"
        elif data_type.tz == TimestampTimeZone.TZ:
            return "to_timestamp_tz('2020-09-16 06:30:00')"
        else:
            return "to_timestamp('2020-09-16 06:30:00')"
    if isinstance(data_type, ArrayType):
        if data_type.structured:
            assert data_type.element_type is not None
            element = schema_expression(data_type.element_type, data_type.contains_null)
            return f"to_array({element}) :: {convert_sp_to_sf_type(data_type)}"
        return "to_array(0)"
    if isinstance(data_type, MapType):
        if data_type.structured:
            assert data_type.key_type is not None and data_type.value_type is not None
            # Key values can never be null
            key = schema_expression(data_type.key_type, False)
            # Value nullability is variable. Defaults to True
            value = schema_expression(
                data_type.value_type, data_type.value_contains_null
            )
            return f"object_construct_keep_null({key}, {value}) :: {convert_sp_to_sf_type(data_type)}"
        return "to_object(parse_json('0'))"
    if isinstance(data_type, StructType):
        if data_type.structured:
            schema_strings = []
            for field in data_type.fields:
                # Even if nulls are allowed the cast will fail due to schema mismatch when passed a null field.
                schema_strings += [
                    f"'{field.name}'",
                    schema_expression(field.datatype, is_nullable=False),
                ]
            return f"object_construct_keep_null({', '.join(schema_strings)}) :: {convert_sp_to_sf_type(data_type)}"
        return "to_object(parse_json('{}'))"
    if isinstance(data_type, VariantType):
        return "to_variant(0)"
    if isinstance(data_type, GeographyType):
        return "to_geography('POINT(-122.35 37.55)')"
    if isinstance(data_type, GeometryType):
        return "to_geometry('POINT(-122.35 37.55)')"
    if isinstance(data_type, VectorType):
        if data_type.element_type == "int":
            zero = int(0)
        elif data_type.element_type == "float":
            zero = float(0)
        else:
            raise TypeError(f"Invalid vector element type: {data_type.element_type}")
        values = [i + zero for i in range(data_type.dimension)]
        return f"{values} :: VECTOR({data_type.element_type},{data_type.dimension})"
    if isinstance(data_type, FileType):
        return (
            "TO_FILE(OBJECT_CONSTRUCT('RELATIVE_PATH', 'some_new_file.jpeg', 'STAGE', '@myStage', "
            "'STAGE_FILE_URL', 'some_new_file.jpeg', 'SIZE', 123, 'ETAG', 'xxx', 'CONTENT_TYPE', 'image/jpeg', "
            "'LAST_MODIFIED', '2025-01-01'))"
        )
    raise Exception(f"Unsupported data type: {data_type.__class__.__name__}")


def numeric_to_sql_without_cast(value: Any, datatype: DataType) -> str:
    """
    Generate the sql str for numeric datatype without cast expression. One exception
    is for float nan and inf, where a cast is always required for Snowflake to be able
    to handle it correctly.
    """
    if value is None:
        return "NULL"

    if not isinstance(datatype, _NumericType):
        # if the value is not numeric or the datatype is not numeric, fallback to the
        # regular to_sql generation
        return to_sql(value, datatype)

    if isinstance(value, float) and isinstance(datatype, _FractionalType):
        # when the float value is NAN or INF, a cast is still required
        if math.isnan(value) or math.isinf(value):
            return float_nan_inf_to_sql(value)
    return str(value)
