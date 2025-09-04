#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.analyzer.datatype_mapper import (
    numeric_to_sql_without_cast,
    schema_expression,
    to_sql,
    to_sql_no_cast,
)
from snowflake.snowpark._internal.udf_utils import generate_call_python_sp_sql
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    GeometryType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
    VectorType,
    YearMonthIntervalType,
)


@pytest.fixture(
    params=[
        TimestampTimeZone.DEFAULT,
        TimestampTimeZone.NTZ,
        TimestampTimeZone.LTZ,
        TimestampTimeZone.TZ,
    ]
)
def timezone(request):
    """
    cache keyword to pass to to_datetime.
    """
    return request.param


def test_to_sql():
    # Test nulls
    assert to_sql(None, NullType()) == "NULL"
    assert to_sql(None, ArrayType(DoubleType())) == "NULL"
    assert to_sql(None, MapType(IntegerType(), ByteType())) == "NULL"
    assert to_sql(None, StructType([])) == "NULL"
    assert to_sql(None, GeographyType()) == "NULL"
    assert to_sql(None, GeometryType()) == "NULL"

    assert to_sql(None, IntegerType()) == "NULL :: INT"
    assert to_sql(None, ShortType()) == "NULL :: INT"
    assert to_sql(None, ByteType()) == "NULL :: INT"
    assert to_sql(None, LongType()) == "NULL :: INT"
    assert to_sql(None, FloatType()) == "NULL :: FLOAT"
    assert to_sql(None, StringType()) == "NULL :: STRING"
    assert to_sql(None, DoubleType()) == "NULL :: FLOAT"
    assert to_sql(None, BooleanType()) == "NULL :: BOOLEAN"
    assert to_sql(None, YearMonthIntervalType()) == "NULL :: INTERVAL YEAR TO MONTH"
    assert to_sql(None, DayTimeIntervalType()) == "NULL :: INTERVAL DAY TO SECOND"

    assert to_sql(None, "Not any of the previous types") == "NULL"

    # Test non-nulls
    assert to_sql("\\ '  ' abc \n \\", StringType()) == "'\\\\ ''  '' abc \\n \\\\'"
    assert (
        to_sql("\\ '  ' abc \n \\", StringType(), True)
        == "'\\\\ ''  '' abc \\n \\\\' :: STRING"
    )
    assert to_sql(1, ByteType()) == "1 :: INT"
    assert to_sql(1, ShortType()) == "1 :: INT"
    assert to_sql(1, IntegerType()) == "1 :: INT"
    assert to_sql(1, LongType()) == "1 :: INT"
    assert to_sql(1, BooleanType()) == "1 :: BOOLEAN"
    assert to_sql(0, ByteType()) == "0 :: INT"
    assert to_sql(0, ShortType()) == "0 :: INT"
    assert to_sql(0, IntegerType()) == "0 :: INT"
    assert to_sql(0, LongType()) == "0 :: INT"
    assert to_sql(0, BooleanType()) == "0 :: BOOLEAN"

    assert to_sql(float("nan"), FloatType()) == "'NAN' :: FLOAT"
    assert to_sql(float("inf"), FloatType()) == "'INF' :: FLOAT"
    assert to_sql(float("-inf"), FloatType()) == "'-INF' :: FLOAT"
    assert to_sql(1.2, FloatType()) == "'1.2' :: FLOAT"

    assert to_sql(float("nan"), DoubleType()) == "'NAN' :: FLOAT"
    assert to_sql(float("inf"), DoubleType()) == "'INF' :: FLOAT"
    assert to_sql(float("-inf"), DoubleType()) == "'-INF' :: FLOAT"
    assert to_sql(1.2, DoubleType()) == "'1.2' :: FLOAT"

    assert to_sql(Decimal(0.5), DecimalType(2, 1)) == "0.5 ::  NUMBER (2, 1)"

    assert to_sql(397, DateType()) == "DATE '1971-02-02'"
    # value type must be int
    with pytest.raises(TypeError, match="Unsupported datatype DateType"):
        to_sql(0.397, DateType())

    assert (
        to_sql(1622002533000000, TimestampType())
        == "TIMESTAMP '2021-05-26 04:15:33+00:00'"
    )
    # value type must be int
    with pytest.raises(TypeError, match="Unsupported datatype TimestampType"):
        to_sql(0.2, TimestampType())

    assert (
        to_sql(bytearray.fromhex("2Ef0 F1f2 "), BinaryType()) == "'2ef0f1f2' :: BINARY"
    )

    assert (
        to_sql([1, "2", 3.5], ArrayType()) == "PARSE_JSON('[1, \"2\", 3.5]') :: ARRAY"
    )
    assert (
        to_sql([1, 2, 3], ArrayType(IntegerType(), structured=True))
        == "PARSE_JSON('[1, 2, 3]') :: ARRAY(INT)"
    )
    assert (
        to_sql({"'": '"'}, MapType()) == 'PARSE_JSON(\'{"\'\'": "\\\\""}\') :: OBJECT'
    )
    assert (
        to_sql({"'": '"'}, MapType(StringType(), structured=True))
        == 'PARSE_JSON(\'{"\'\'": "\\\\""}\') :: MAP(STRING, STRING)'
    )
    assert to_sql([{1: 2}], ArrayType()) == "PARSE_JSON('[{\"1\": 2}]') :: ARRAY"
    assert to_sql({1: [2]}, MapType()) == "PARSE_JSON('{\"1\": [2]}') :: OBJECT"

    assert (
        to_sql([1, bytearray(1)], ArrayType()) == "PARSE_JSON('[1, \"00\"]') :: ARRAY"
    )

    assert (
        to_sql(["2", Decimal(0.5)], ArrayType())
        == "PARSE_JSON('[\"2\", 0.5]') :: ARRAY"
    )

    dt = datetime.datetime.today()
    assert (
        to_sql({1: dt}, MapType())
        == 'PARSE_JSON(\'{"1": "' + dt.isoformat() + "\"}') :: OBJECT"
    )

    assert to_sql([1, 2, 3.5], VectorType(float, 3)) == "[1, 2, 3.5] :: VECTOR(float,3)"
    assert (
        to_sql([1, 2, 3.5, 4.1234567, -3.8], VectorType("float", 5))
        == "[1, 2, 3.5, 4.1234567, -3.8] :: VECTOR(float,5)"
    )
    assert to_sql([1, 2, 3], VectorType(int, 3)) == "[1, 2, 3] :: VECTOR(int,3)"
    assert (
        to_sql([1, 2, 31234567, -1928, 0, -3], VectorType(int, 5))
        == "[1, 2, 31234567, -1928, 0, -3] :: VECTOR(int,5)"
    )

    assert (
        to_sql("INTERVAL 1-2 YEAR TO MONTH", YearMonthIntervalType())
        == "INTERVAL '1-2' YEAR TO MONTH :: INTERVAL YEAR TO MONTH"
    )
    assert (
        to_sql("INTERVAL 5-0 YEAR TO MONTH", YearMonthIntervalType(0, 0))
        == "INTERVAL '5' YEAR :: INTERVAL YEAR"
    )
    assert (
        to_sql("INTERVAL 0-3 YEAR TO MONTH", YearMonthIntervalType(1, 1))
        == "INTERVAL '3' MONTH :: INTERVAL MONTH"
    )

    assert (
        to_sql("INTERVAL '3-11' YEAR TO MONTH", YearMonthIntervalType(0, 1))
        == "INTERVAL '3-11' YEAR TO MONTH :: INTERVAL YEAR TO MONTH"
    )

    assert (
        to_sql("INTERVAL 1 01:01:01.7878 DAY TO SECOND", DayTimeIntervalType())
        == "INTERVAL '1 01:01:01.7878' DAY TO SECOND :: INTERVAL DAY TO SECOND"
    )
    assert (
        to_sql("INTERVAL 5 12:30:45.123 DAY TO SECOND", DayTimeIntervalType(0, 3))
        == "INTERVAL '5 12:30:45.123' DAY TO SECOND :: INTERVAL DAY TO SECOND"
    )
    assert (
        to_sql("INTERVAL 3 08:45 DAY TO MINUTE", DayTimeIntervalType(0, 2))
        == "INTERVAL '3 08:45' DAY TO MINUTE :: INTERVAL DAY TO MINUTE"
    )
    assert (
        to_sql("INTERVAL 7 14 DAY TO HOUR", DayTimeIntervalType(0, 1))
        == "INTERVAL '7 14' DAY TO HOUR :: INTERVAL DAY TO HOUR"
    )
    assert (
        to_sql("INTERVAL 10 DAY", DayTimeIntervalType(0))
        == "INTERVAL '10' DAY :: INTERVAL DAY"
    )
    assert (
        to_sql("INTERVAL 15:30:45.999 HOUR TO SECOND", DayTimeIntervalType(1, 3))
        == "INTERVAL '15:30:45.999' HOUR TO SECOND :: INTERVAL HOUR TO SECOND"
    )
    assert (
        to_sql("INTERVAL 9:15 HOUR TO MINUTE", DayTimeIntervalType(1, 2))
        == "INTERVAL '9:15' HOUR TO MINUTE :: INTERVAL HOUR TO MINUTE"
    )
    assert (
        to_sql("INTERVAL 23 HOUR", DayTimeIntervalType(1))
        == "INTERVAL '23' HOUR :: INTERVAL HOUR"
    )
    assert (
        to_sql("INTERVAL 45:30.500 MINUTE TO SECOND", DayTimeIntervalType(2, 3))
        == "INTERVAL '45:30.500' MINUTE TO SECOND :: INTERVAL MINUTE TO SECOND"
    )
    assert (
        to_sql("INTERVAL 90 MINUTE", DayTimeIntervalType(2))
        == "INTERVAL '90' MINUTE :: INTERVAL MINUTE"
    )
    assert (
        to_sql("INTERVAL 125.750 SECOND", DayTimeIntervalType(3))
        == "INTERVAL '125.750' SECOND :: INTERVAL SECOND"
    )


def test_to_sql_system_function():
    # Test nulls
    assert to_sql_no_cast(None, NullType()) == "NULL"
    assert to_sql_no_cast(None, ArrayType(DoubleType())) == "NULL"
    assert to_sql_no_cast(None, MapType(IntegerType(), ByteType())) == "NULL"
    assert to_sql_no_cast(None, StructType([])) == "NULL"
    assert to_sql_no_cast(None, GeographyType()) == "NULL"
    assert to_sql_no_cast(None, GeometryType()) == "NULL"

    assert to_sql_no_cast(None, IntegerType()) == "NULL"
    assert to_sql_no_cast(None, ShortType()) == "NULL"
    assert to_sql_no_cast(None, ByteType()) == "NULL"
    assert to_sql_no_cast(None, LongType()) == "NULL"
    assert to_sql_no_cast(None, FloatType()) == "NULL"
    assert to_sql_no_cast(None, StringType()) == "NULL"
    assert to_sql_no_cast(None, DoubleType()) == "NULL"
    assert to_sql_no_cast(None, BooleanType()) == "NULL"
    assert to_sql_no_cast(None, YearMonthIntervalType()) == "NULL"
    assert to_sql_no_cast(None, DayTimeIntervalType()) == "NULL"

    assert to_sql_no_cast(None, "Not any of the previous types") == "NULL"

    # Test non-nulls
    assert (
        to_sql_no_cast("\\ '  ' abc \n \\", StringType())
        == "'\\\\ ''  '' abc \\n \\\\'"
    )
    assert (
        to_sql_no_cast("\\ '  ' abc \n \\", StringType())
        == "'\\\\ ''  '' abc \\n \\\\'"
    )
    assert to_sql_no_cast(1, ByteType()) == "1"
    assert to_sql_no_cast(1, ShortType()) == "1"
    assert to_sql_no_cast(1, IntegerType()) == "1"
    assert to_sql_no_cast(1, LongType()) == "1"
    assert to_sql_no_cast(1, BooleanType()) == "1"
    assert to_sql_no_cast(0, ByteType()) == "0"
    assert to_sql_no_cast(0, ShortType()) == "0"
    assert to_sql_no_cast(0, IntegerType()) == "0"
    assert to_sql_no_cast(0, LongType()) == "0"
    assert to_sql_no_cast(0, BooleanType()) == "0"

    assert to_sql_no_cast(float("nan"), FloatType()) == "'NAN'"
    assert to_sql_no_cast(float("inf"), FloatType()) == "'INF'"
    assert to_sql_no_cast(float("-inf"), FloatType()) == "'-INF'"
    assert to_sql_no_cast(1.2, FloatType()) == "1.2"

    assert to_sql_no_cast(float("nan"), DoubleType()) == "'NAN'"
    assert to_sql_no_cast(float("inf"), DoubleType()) == "'INF'"
    assert to_sql_no_cast(float("-inf"), DoubleType()) == "'-INF'"
    assert to_sql_no_cast(1.2, DoubleType()) == "1.2"

    assert to_sql_no_cast(Decimal(0.5), DecimalType(2, 1)) == "0.5"

    assert to_sql_no_cast(397, DateType()) == "'1971-02-02'"

    assert to_sql_no_cast(datetime.date(1971, 2, 2), DateType()) == "'1971-02-02'"

    assert (
        to_sql_no_cast(1622002533000000, TimestampType())
        == "'2021-05-26 04:15:33+00:00'"
    )

    assert (
        to_sql_no_cast(bytearray.fromhex("2Ef0 F1f2 "), BinaryType())
        == "b'.\\xf0\\xf1\\xf2'"
    )

    assert to_sql_no_cast([1, "2", 3.5], ArrayType()) == "PARSE_JSON('[1, \"2\", 3.5]')"
    assert to_sql_no_cast({"'": '"'}, MapType()) == 'PARSE_JSON(\'{"\'\'": "\\\\""}\')'
    assert to_sql_no_cast([{1: 2}], ArrayType()) == "PARSE_JSON('[{\"1\": 2}]')"
    assert to_sql_no_cast({1: [2]}, MapType()) == "PARSE_JSON('{\"1\": [2]}')"

    assert to_sql_no_cast([1, bytearray(1)], ArrayType()) == "PARSE_JSON('[1, \"00\"]')"

    assert (
        to_sql_no_cast(["2", Decimal(0.5)], ArrayType()) == "PARSE_JSON('[\"2\", 0.5]')"
    )

    dt = datetime.datetime.today()
    assert (
        to_sql_no_cast({1: dt}, MapType())
        == 'PARSE_JSON(\'{"1": "' + dt.isoformat() + "\"}')"
    )

    assert to_sql_no_cast([1, 2, 3.5], VectorType(float, 3)) == "[1, 2, 3.5]"
    assert (
        to_sql_no_cast("POINT(-122.35 37.55)", GeographyType())
        == "TO_GEOGRAPHY('POINT(-122.35 37.55)')"
    )
    assert (
        to_sql_no_cast("POINT(-122.35 37.55)", GeometryType())
        == "TO_GEOMETRY('POINT(-122.35 37.55)')"
    )
    assert to_sql_no_cast("1", VariantType()) == "PARSE_JSON('\"1\"')"
    assert (
        to_sql_no_cast([1, 2, 3.5, 4.1234567, -3.8], VectorType("float", 5))
        == "[1, 2, 3.5, 4.1234567, -3.8]"
    )
    assert to_sql_no_cast([1, 2, 3], VectorType(int, 3)) == "[1, 2, 3]"
    assert (
        to_sql_no_cast([1, 2, 31234567, -1928, 0, -3], VectorType(int, 5))
        == "[1, 2, 31234567, -1928, 0, -3]"
    )

    assert (
        to_sql_no_cast("INTERVAL 1-2 YEAR TO MONTH", YearMonthIntervalType())
        == "INTERVAL '1-2' YEAR TO MONTH"
    )
    assert (
        to_sql_no_cast("INTERVAL 5-0 YEAR TO MONTH", YearMonthIntervalType(0, 0))
        == "INTERVAL '5' YEAR"
    )
    assert (
        to_sql_no_cast("INTERVAL 0-3 YEAR TO MONTH", YearMonthIntervalType(1, 1))
        == "INTERVAL '3' MONTH"
    )

    assert (
        to_sql_no_cast("INTERVAL '3-11' YEAR TO MONTH", YearMonthIntervalType(0, 1))
        == "INTERVAL '3-11' YEAR TO MONTH"
    )

    assert (
        to_sql_no_cast("INTERVAL 1 01:01:01.7878 DAY TO SECOND", DayTimeIntervalType())
        == "INTERVAL '1 01:01:01.7878' DAY TO SECOND"
    )
    assert (
        to_sql_no_cast(
            "INTERVAL 5 12:30:45.123 DAY TO SECOND", DayTimeIntervalType(0, 3)
        )
        == "INTERVAL '5 12:30:45.123' DAY TO SECOND"
    )
    assert (
        to_sql_no_cast("INTERVAL 3 08:45 DAY TO MINUTE", DayTimeIntervalType(0, 2))
        == "INTERVAL '3 08:45' DAY TO MINUTE"
    )
    assert (
        to_sql_no_cast("INTERVAL 7 14 DAY TO HOUR", DayTimeIntervalType(0, 1))
        == "INTERVAL '7 14' DAY TO HOUR"
    )
    assert (
        to_sql_no_cast("INTERVAL 10 DAY", DayTimeIntervalType(0)) == "INTERVAL '10' DAY"
    )
    assert (
        to_sql_no_cast(
            "INTERVAL 15:30:45.999 HOUR TO SECOND", DayTimeIntervalType(1, 3)
        )
        == "INTERVAL '15:30:45.999' HOUR TO SECOND"
    )
    assert (
        to_sql_no_cast("INTERVAL 9:15 HOUR TO MINUTE", DayTimeIntervalType(1, 2))
        == "INTERVAL '9:15' HOUR TO MINUTE"
    )
    assert (
        to_sql_no_cast("INTERVAL 23 HOUR", DayTimeIntervalType(1))
        == "INTERVAL '23' HOUR"
    )
    assert (
        to_sql_no_cast("INTERVAL 45:30.500 MINUTE TO SECOND", DayTimeIntervalType(2, 3))
        == "INTERVAL '45:30.500' MINUTE TO SECOND"
    )
    assert (
        to_sql_no_cast("INTERVAL 90 MINUTE", DayTimeIntervalType(2))
        == "INTERVAL '90' MINUTE"
    )
    assert (
        to_sql_no_cast("INTERVAL 125.750 SECOND", DayTimeIntervalType(3))
        == "INTERVAL '125.750' SECOND"
    )


def test_generate_call_python_sp_sql():
    fake_session = MagicMock(Session)
    assert (
        generate_call_python_sp_sql(fake_session, "system$wait", 1)
        == "CALL system$wait(1)"
    )


@pytest.mark.parametrize(
    "timezone, expected",
    [
        (TimestampTimeZone.DEFAULT, "TIMESTAMP '2021-05-26 04:15:33+00:00'"),
        (TimestampTimeZone.NTZ, "'2021-05-26 04:15:33+00:00'::TIMESTAMP_NTZ"),
        (TimestampTimeZone.LTZ, "'2021-05-26 04:15:33+00:00'::TIMESTAMP_LTZ"),
        (TimestampTimeZone.TZ, "'2021-05-26 04:15:33+00:00'::TIMESTAMP_TZ"),
    ],
)
def test_int_to_sql_timestamp(timezone, expected):
    assert to_sql(1622002533000000, TimestampType(timezone)) == expected


@pytest.mark.parametrize(
    "timezone, expected",
    [
        (TimestampTimeZone.DEFAULT, "TIMESTAMP '1970-01-01 00:00:00.000123+01:00'"),
        (TimestampTimeZone.NTZ, "'1970-01-01 00:00:00.000123+01:00'::TIMESTAMP_NTZ"),
        (TimestampTimeZone.LTZ, "'1970-01-01 00:00:00.000123+01:00'::TIMESTAMP_LTZ"),
        (TimestampTimeZone.TZ, "'1970-01-01 00:00:00.000123+01:00'::TIMESTAMP_TZ"),
    ],
)
def test_datetime_to_sql_timestamp(timezone, expected):
    dt = datetime.datetime(
        1970, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=1))
    ) + datetime.timedelta(microseconds=123)
    assert to_sql(dt, TimestampType(timezone)) == expected


def test_numeric_to_sql_without_cast():
    assert numeric_to_sql_without_cast(None, NullType()) == "NULL"
    assert numeric_to_sql_without_cast(None, IntegerType()) == "NULL"

    assert numeric_to_sql_without_cast("abc", StringType()) == "'abc'"

    assert numeric_to_sql_without_cast(123, IntegerType()) == "123"
    assert numeric_to_sql_without_cast(0.2, FloatType()) == "0.2"
    assert numeric_to_sql_without_cast(0.2, DoubleType()) == "0.2"

    assert numeric_to_sql_without_cast(float("nan"), FloatType()) == "'NAN' :: FLOAT"
    assert numeric_to_sql_without_cast(float("inf"), DoubleType()) == "'INF' :: FLOAT"


@pytest.mark.parametrize(
    "value, datatype",
    [
        (123, StringType()),
        (0.2, StringType()),
    ],
)
def test_numeric_to_sql_without_cast_invalid(value, datatype):
    with pytest.raises(TypeError, match="Unsupported datatype"):
        assert numeric_to_sql_without_cast(value, datatype)


def test_schema_expression():
    assert schema_expression(GeographyType(), True) == "TRY_TO_GEOGRAPHY(NULL)"
    assert schema_expression(GeometryType(), True) == "TRY_TO_GEOMETRY(NULL)"
    assert schema_expression(ArrayType(None), True) == "PARSE_JSON('NULL') :: ARRAY"
    assert (
        schema_expression(MapType(IntegerType(), ByteType()), True)
        == "PARSE_JSON('NULL') :: OBJECT"
    )
    assert schema_expression(VariantType(), True) == "PARSE_JSON('NULL') :: VARIANT"
    assert schema_expression(IntegerType(), True) == "NULL :: INT"
    assert schema_expression(ShortType(), True) == "NULL :: SMALLINT"
    assert schema_expression(ByteType(), True) == "NULL :: BYTEINT"
    assert schema_expression(LongType(), True) == "NULL :: BIGINT"
    assert schema_expression(FloatType(), True) == "NULL :: FLOAT"
    assert schema_expression(DoubleType(), True) == "NULL :: DOUBLE"
    assert schema_expression(StringType(), True) == "NULL :: STRING"
    assert schema_expression(StringType(19), True) == "NULL :: STRING(19)"
    assert schema_expression(NullType(), True) == "NULL :: STRING"
    assert schema_expression(BooleanType(), True) == "NULL :: BOOLEAN"
    assert schema_expression(DateType(), True) == "NULL :: DATE"
    assert schema_expression(TimeType(), True) == "NULL :: TIME"
    assert schema_expression(TimestampType(), True) == "NULL :: TIMESTAMP"
    assert schema_expression(BinaryType(), True) == "NULL :: BINARY"
    assert schema_expression(VectorType(int, 3), True) == "NULL :: VECTOR(int,3)"
    assert schema_expression(VectorType(float, 2), True) == "NULL :: VECTOR(float,2)"
    assert (
        schema_expression(YearMonthIntervalType(), True)
        == "NULL :: INTERVAL YEAR TO MONTH"
    )
    assert (
        schema_expression(DayTimeIntervalType(), True)
        == "NULL :: INTERVAL DAY TO SECOND"
    )

    assert (
        schema_expression(GeographyType(), False)
        == "to_geography('POINT(-122.35 37.55)')"
    )
    assert (
        schema_expression(GeometryType(), False)
        == "to_geometry('POINT(-122.35 37.55)')"
    )
    assert schema_expression(ArrayType(None), False) == "to_array(0)"
    assert (
        schema_expression(MapType(IntegerType(), ByteType()), False)
        == "to_object(parse_json('0'))"
    )
    assert schema_expression(VariantType(), False) == "to_variant(0)"
    assert schema_expression(IntegerType(), False) == "0 :: INT"
    assert schema_expression(ShortType(), False) == "0 :: SMALLINT"
    assert schema_expression(ByteType(), False) == "0 :: BYTEINT"
    assert schema_expression(LongType(), False) == "0 :: BIGINT"
    assert schema_expression(FloatType(), False) == "0 :: FLOAT"
    assert schema_expression(DoubleType(), False) == "0 :: DOUBLE"
    assert schema_expression(StringType(), False) == "'a' :: STRING"
    assert schema_expression(StringType(19), False) == "'a' ::  STRING (19)"
    assert schema_expression(BooleanType(), False) == "true"
    assert schema_expression(DateType(), False) == "date('2020-9-16')"
    assert schema_expression(TimeType(), False) == "to_time('04:15:29.999')"
    assert (
        schema_expression(TimestampType(), False)
        == "to_timestamp('2020-09-16 06:30:00')"
    )
    assert (
        schema_expression(TimestampType(TimestampTimeZone.DEFAULT), False)
        == "to_timestamp('2020-09-16 06:30:00')"
    )
    assert (
        schema_expression(TimestampType(TimestampTimeZone.NTZ), False)
        == "to_timestamp_ntz('2020-09-16 06:30:00')"
    )
    assert (
        schema_expression(TimestampType(TimestampTimeZone.LTZ), False)
        == "to_timestamp_ltz('2020-09-16 06:30:00')"
    )
    assert (
        schema_expression(TimestampType(TimestampTimeZone.TZ), False)
        == "to_timestamp_tz('2020-09-16 06:30:00')"
    )

    assert schema_expression(BinaryType(), False) == "'01' :: BINARY"

    assert schema_expression(VectorType(int, 2), False) == "[0, 1] :: VECTOR(int,2)"
    assert (
        schema_expression(VectorType(float, 3), False)
        == "[0.0, 1.0, 2.0] :: VECTOR(float,3)"
    )
    assert schema_expression(StructType([]), False) == "to_object(parse_json('{}'))"
    assert (
        schema_expression(YearMonthIntervalType(), False)
        == "INTERVAL '1-0' YEAR TO MONTH"
    )
    assert (
        schema_expression(DayTimeIntervalType(), False)
        == "INTERVAL '1 01:01:01.0001' DAY TO SECOND"
    )
