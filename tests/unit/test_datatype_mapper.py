#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
from decimal import Decimal

import pytest

from snowflake.snowpark._internal.analyzer.datatype_mapper import (
    schema_expression,
    to_sql,
    to_sql_without_cast,
)
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
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
        to_sql({"'": '"'}, MapType()) == 'PARSE_JSON(\'{"\'\'": "\\\\""}\') :: OBJECT'
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


def test_to_sql_without_cast():
    assert to_sql_without_cast(None, NullType()) == "NULL"
    assert to_sql_without_cast(None, IntegerType()) == "NULL"

    assert to_sql_without_cast("abc", StringType()) == "'abc'"
    assert to_sql_without_cast(123, StringType()) == "'123'"
    assert to_sql_without_cast(0.2, StringType()) == "'0.2'"

    assert to_sql_without_cast(123, IntegerType()) == "123"
    assert to_sql_without_cast(0.2, FloatType()) == "0.2"
    assert to_sql_without_cast(0.2, DoubleType()) == "0.2"


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
