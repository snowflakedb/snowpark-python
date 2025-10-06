#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import binascii
import json
import math
import re
from array import array
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import snowflake.snowpark.context as context
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
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FileType,
    FloatType,
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
    YearMonthIntervalType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)

MILLIS_PER_DAY = 24 * 3600 * 1000
MICROS_PER_MILLIS = 1000

# Regex patterns for detecting properly quoted interval values based on Snowflake documentation
# INTERVAL YEAR TO MONTH format: '[<sign>]<Y>-<MM>'
YEAR_MONTH_INTERVAL_QUOTED_PATTERN = re.compile(
    r"INTERVAL\s+'[+-]?\d+-\d{2}'\s+(YEAR(\s+TO\s+MONTH)?|MONTH)(\s+.*)?$",
    re.IGNORECASE,
)

# INTERVAL DAY TO SECOND format: '[<sign>]<D> <HH24>:<MI>:<SS>[.<F>]'
DAY_TIME_INTERVAL_QUOTED_PATTERN = re.compile(
    r"INTERVAL\s+'[+-]?(\d+\s+)?\d{2}:\d{2}:\d{2}(\.\d+)?'\s+(DAY(\s+TO\s+(HOUR|MINUTE|SECOND))?|HOUR(\s+TO\s+(MINUTE|SECOND))?|MINUTE(\s+TO\s+SECOND)?|SECOND)(\s+.*)?$",
    re.IGNORECASE,
)

# Also match simpler quoted patterns for single fields
SIMPLE_QUOTED_INTERVAL_PATTERN = re.compile(
    r"INTERVAL\s+'[^']+'\s+(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)(\s+.*)?$", re.IGNORECASE
)


def str_to_sql(value: str) -> str:
    sql_str = str(value).replace("\\", "\\\\").replace("'", "''").replace("\n", "\\n")
    return f"'{sql_str}'"


def str_to_sql_for_year_month_interval(
    value: str, datatype: YearMonthIntervalType
) -> str:
    """
    Converts "INTERVAL YY-MM [YEAR TO MONTH | YEAR | MONTH]" to quoted format:
    - Same start/end field: extracts specific value (YEAR or MONTH only)
    - Different fields: uses full "YEAR TO MONTH" format
    - Supports passthrough for already-quoted intervals

    Examples:
        "INTERVAL 1-2 YEAR TO MONTH", YearMonthIntervalType(0,1) -> "INTERVAL '1-2' YEAR TO MONTH"
        "INTERVAL 5-0 YEAR TO MONTH", YearMonthIntervalType(0,0) -> "INTERVAL '5' YEAR"
        "INTERVAL 0-3 YEAR TO MONTH", YearMonthIntervalType(1,1) -> "INTERVAL '3' MONTH"
        "INTERVAL '1-2' YEAR TO MONTH", YearMonthIntervalType(0,1) -> "INTERVAL '1-2' YEAR TO MONTH" (passthrough)
    """
    # Check for properly quoted intervals using regex patterns (passthrough)
    if YEAR_MONTH_INTERVAL_QUOTED_PATTERN.match(
        value
    ) or SIMPLE_QUOTED_INTERVAL_PATTERN.match(value):
        return value  # passthrough

    parts = value.split(" ")
    if len(parts) < 2:
        raise ValueError(f"Invalid interval format: {value}")

    extracted_values = parts[1]
    start_field = (
        datatype.start_field
        if datatype.start_field is not None
        else YearMonthIntervalType.YEAR
    )
    end_field = (
        datatype.end_field
        if datatype.end_field is not None
        else YearMonthIntervalType.MONTH
    )
    # When the start_field equals the end_field, it implies our YearMonthIntervalType is only
    # using a single field. Either YEAR for 0 or MONTH for 1.
    if datatype.start_field == datatype.end_field:
        extracted_values = extracted_values.split("-")
        extracted_value = extracted_values[0]
        if (
            datatype.start_field == YearMonthIntervalType.MONTH
            and len(extracted_values) == 2
        ):
            # Extract the second value if we only have a MONTH interval.
            extracted_value = extracted_values[1]
        return (
            f"INTERVAL '{extracted_value}' {datatype._FIELD_NAMES[start_field].upper()}"
        )
    return f"INTERVAL '{extracted_values}' {datatype._FIELD_NAMES[start_field].upper()} TO {datatype._FIELD_NAMES[end_field].upper()}"


def _extract_time_component(time_components: list, field: int) -> str:
    """Extract a specific time component (HOUR, MINUTE, SECOND) from parsed time parts."""
    if field == DayTimeIntervalType.HOUR:
        return time_components[0] if time_components else "0"
    elif field == DayTimeIntervalType.MINUTE:
        return time_components[1] if len(time_components) > 1 else "0"
    elif field == DayTimeIntervalType.SECOND:
        if len(time_components) > 2:
            seconds_part = time_components[2]
            # Return the full seconds value including fractional part (e.g., "15.123456")
            return seconds_part
        return "0"
    return "0"


def _truncate_time_to_field(time_part: str, end_field: int) -> str:
    """Truncate time part based on the target end field."""
    time_components = time_part.split(":")

    if end_field == DayTimeIntervalType.HOUR:
        return time_components[0] if time_components else "0"
    elif end_field == DayTimeIntervalType.MINUTE:
        if len(time_components) >= 2:
            return f"{time_components[0]}:{time_components[1]}"
        elif len(time_components) == 1:
            return f"{time_components[0]}:00"
        return "0:00"
    else:  # SECOND or beyond
        return time_part


def _extract_time_range(time_part: str, start_field: int, end_field: int) -> str:
    """Extract time range from start_field to end_field."""
    time_components = time_part.split(":")

    if start_field == DayTimeIntervalType.HOUR:
        if end_field == DayTimeIntervalType.MINUTE:
            # HOUR TO MINUTE: HH:MM
            if len(time_components) >= 2:
                return f"{time_components[0]}:{time_components[1]}"
            return "0:00"
        elif end_field == DayTimeIntervalType.SECOND:
            # HOUR TO SECOND: HH:MM:SS[.fff]
            return time_part
    elif start_field == DayTimeIntervalType.MINUTE:
        if end_field == DayTimeIntervalType.SECOND:
            # MINUTE TO SECOND: MM:SS[.fff]
            if len(time_components) == 3:
                # Input is HH:MM:SS format, extract MM:SS part (skip hour component)
                return f"{time_components[1]}:{time_components[2]}"
            elif len(time_components) == 2:
                # Input is already in MM:SS format, return as-is
                return time_part
            return "0:00"

    return time_part


def str_to_sql_for_day_time_interval(value: str, datatype: DayTimeIntervalType) -> str:
    """
    Converts "INTERVAL DD HH:MM:SS.ffffff [DAY | HOUR | MINUTE | SECOND (TO) (DAY | HOUR | MINUTE | SECOND)]" to quoted format:
    - Same start/end field: extracts specific value (DAY, HOUR, MINUTE, or SECOND only)
    - Different fields: uses full range format (e.g., "DAY TO SECOND", "HOUR TO MINUTE")
    - Supports passthrough for already-quoted intervals

    Examples:
        "INTERVAL 1 01:01:01.7878 DAY TO SECOND", DayTimeIntervalType(0,3) -> "INTERVAL '1 01:01:01.7878' DAY TO SECOND"
        "INTERVAL 5 00:00:00 DAY TO SECOND", DayTimeIntervalType(0,0) -> "INTERVAL '5' DAY"
        "INTERVAL 0 03:30:00 DAY TO SECOND", DayTimeIntervalType(1,1) -> "INTERVAL '03' HOUR"
        "INTERVAL 0 00:45:00 DAY TO SECOND", DayTimeIntervalType(2,2) -> "INTERVAL '45' MINUTE"
        "INTERVAL 0 00:00:30.5 DAY TO SECOND", DayTimeIntervalType(3,3) -> "INTERVAL '30.5' SECOND"
        "INTERVAL '1 01:01:01.7878' DAY TO SECOND", DayTimeIntervalType(0, 3) -> "INTERVAL '1 01:01:01.7878' DAY TO SECOND" (passthrough)
    """
    # Check for properly quoted intervals using regex patterns (passthrough)
    if DAY_TIME_INTERVAL_QUOTED_PATTERN.match(
        value
    ) or SIMPLE_QUOTED_INTERVAL_PATTERN.match(value):
        return value  # passthrough

    parts = value.split(" ")
    if len(parts) < 2:
        raise ValueError(f"Invalid interval format: {value}")

    start_field = (
        datatype.start_field
        if datatype.start_field is not None
        else DayTimeIntervalType.DAY
    )
    end_field = (
        datatype.end_field
        if datatype.end_field is not None
        else DayTimeIntervalType.SECOND
    )

    if start_field == end_field:
        # Single field: extract specific component
        if len(parts) >= 3 and parts[2].upper() in ["DAY", "HOUR", "MINUTE", "SECOND"]:
            extracted_value = parts[1]  # Simple format like "INTERVAL 23 HOUR"
        elif len(parts) >= 3 and ":" in parts[2]:
            # Complex format like "INTERVAL 1 01:01:01.7878 DAY TO SECOND"
            if start_field == DayTimeIntervalType.DAY:
                extracted_value = parts[1]
            else:
                time_components = parts[2].split(":")
                extracted_value = _extract_time_component(time_components, start_field)
        else:
            extracted_value = parts[1]

        return (
            f"INTERVAL '{extracted_value}' {datatype._FIELD_NAMES[start_field].upper()}"
        )

    elif start_field == DayTimeIntervalType.DAY:
        # DAY TO [HOUR|MINUTE|SECOND]: need to handle time truncation
        day_value = parts[1]
        if len(parts) >= 3 and parts[2] not in ["DAY", "HOUR", "MINUTE", "SECOND"]:
            # parts[2] is a time component, not a field name
            time_value = _truncate_time_to_field(parts[2], end_field)
        else:
            time_value = "0"

        return f"INTERVAL '{day_value} {time_value}' {datatype._FIELD_NAMES[start_field].upper()} TO {datatype._FIELD_NAMES[end_field].upper()}"

    else:
        # HOUR TO [MINUTE|SECOND] or MINUTE TO SECOND
        # Check if input is in full DAY TO SECOND format with time component
        if len(parts) >= 3 and ":" in parts[2]:
            # Input like "INTERVAL 1 01:30:45 DAY TO SECOND" - extract time range
            time_part = parts[2]
            extracted_value = _extract_time_range(time_part, start_field, end_field)
        elif len(parts) >= 2 and ":" in parts[1]:
            # Input like "INTERVAL 01:30:45 HOUR TO SECOND" - extract time range
            extracted_value = _extract_time_range(parts[1], start_field, end_field)
        else:
            # Simple format like "INTERVAL 23 HOUR"
            extracted_value = parts[1]

        return f"INTERVAL '{extracted_value}' {datatype._FIELD_NAMES[start_field].upper()} TO {datatype._FIELD_NAMES[end_field].upper()}"


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
        if isinstance(datatype, YearMonthIntervalType):
            return str_to_sql_for_year_month_interval(value, datatype)
        if isinstance(datatype, DayTimeIntervalType):
            return str_to_sql_for_day_time_interval(value, datatype)
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
        (
            NullType,
            ArrayType,
            MapType,
            StructType,
            GeographyType,
            GeometryType,
        ),
    ):
        if value is None:
            return "NULL"
    if isinstance(datatype, BinaryType):
        if value is None:
            return "NULL :: BINARY"
    if isinstance(datatype, _IntegralType):
        if value is None:
            return "NULL :: INT"
    if isinstance(datatype, DecimalType):
        if value is None:
            return f"NULL :: DECIMAL({datatype.precision},{datatype.scale})"
    if isinstance(datatype, (FloatType, DoubleType)):
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
    if isinstance(datatype, YearMonthIntervalType):
        if value is None:
            return "NULL :: INTERVAL YEAR TO MONTH"
    if isinstance(datatype, DayTimeIntervalType):
        if value is None:
            return "NULL :: INTERVAL DAY TO SECOND"
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

    if isinstance(value, str) and isinstance(datatype, YearMonthIntervalType):
        return f"{str_to_sql_for_year_month_interval(value, datatype)} :: {convert_sp_to_sf_type(datatype)}"

    if isinstance(value, str) and isinstance(datatype, DayTimeIntervalType):
        return f"{str_to_sql_for_day_time_interval(value, datatype)} :: {convert_sp_to_sf_type(datatype)}"

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
            if context._enable_fix_2360274:
                element = "NULL"
            else:
                element = schema_expression(
                    data_type.element_type, data_type.contains_null
                )
            return f"to_array({element}) :: {convert_sp_to_sf_type(data_type)}"
        return "to_array(0)"
    if isinstance(data_type, MapType):
        if data_type.structured:
            assert data_type.key_type is not None and data_type.value_type is not None
            # Key values can never be null
            key = schema_expression(data_type.key_type, False)
            if context._enable_fix_2360274:
                value = "NULL"
            else:
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
                    "NULL"
                    if context._enable_fix_2360274
                    else schema_expression(field.datatype, is_nullable=False),
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
    if isinstance(data_type, YearMonthIntervalType):
        return "INTERVAL '1-0' YEAR TO MONTH"
    if isinstance(data_type, DayTimeIntervalType):
        return "INTERVAL '1 01:01:01.0001' DAY TO SECOND"
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
