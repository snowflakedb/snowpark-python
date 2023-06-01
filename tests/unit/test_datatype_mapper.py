#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
from decimal import Decimal

import pytest

from snowflake.snowpark._internal.analyzer.datatype_mapper import (
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
    TimestampType,
)


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
    with pytest.raises(Exception):
        to_sql(0.397, DateType())

    assert (
        to_sql(1622002533000000, TimestampType())
        == "TIMESTAMP '2021-05-26 04:15:33.000'"
    )
    # value type must be int
    with pytest.raises(Exception):
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

    # value must be json serializable
    with pytest.raises(TypeError, match="is not JSON serializable"):
        to_sql([1, bytearray(1)], ArrayType())

    with pytest.raises(TypeError, match="is not JSON serializable"):
        to_sql(["2", Decimal(0.5)], ArrayType())

    with pytest.raises(TypeError, match="is not JSON serializable"):
        to_sql({1: datetime.datetime.today()}, MapType())


def test_to_sql_without_cast():
    assert to_sql_without_cast(None, NullType()) == "NULL"
    assert to_sql_without_cast(None, IntegerType()) == "NULL"

    assert to_sql_without_cast("abc", StringType()) == "'abc'"
    assert to_sql_without_cast(123, StringType()) == "'123'"
    assert to_sql_without_cast(0.2, StringType()) == "'0.2'"

    assert to_sql_without_cast(123, IntegerType()) == "123"
    assert to_sql_without_cast(0.2, FloatType()) == "0.2"
    assert to_sql_without_cast(0.2, DoubleType()) == "0.2"
