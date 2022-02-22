#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from decimal import Decimal

import pytest

from snowflake.snowpark._internal.analyzer.datatype_mapper import DataTypeMapper
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
    to_sql = DataTypeMapper.to_sql

    # Test nulls
    assert to_sql(None, NullType()) == "NULL"
    assert to_sql(None, ArrayType(DoubleType())) == "NULL"
    assert to_sql(None, MapType(IntegerType(), ByteType())) == "NULL"
    assert to_sql(None, StructType([])) == "NULL"
    assert to_sql(None, GeographyType()) == "NULL"

    assert to_sql(None, IntegerType()) == "NULL :: int"
    assert to_sql(None, ShortType()) == "NULL :: smallint"
    assert to_sql(None, ByteType()) == "NULL :: tinyint"
    assert to_sql(None, LongType()) == "NULL :: bigint"
    assert to_sql(None, FloatType()) == "NULL :: float"
    assert to_sql(None, StringType()) == "NULL :: string"
    assert to_sql(None, DoubleType()) == "NULL :: double"
    assert to_sql(None, BooleanType()) == "NULL :: boolean"

    assert to_sql(None, "Not any of the previous types") == "NULL"

    # Test non-nulls
    assert to_sql("\\ '  ' abc \n \\", StringType()) == "'\\\\ ''  '' abc \\n \\\\'"
    assert to_sql(1, ByteType()) == "1 :: tinyint"
    assert to_sql(1, ShortType()) == "1 :: smallint"
    assert to_sql(1, IntegerType()) == "1 :: int"
    assert to_sql(1, LongType()) == "1 :: bigint"
    assert to_sql(1, BooleanType()) == "1 :: boolean"
    assert to_sql(0, ByteType()) == "0 :: tinyint"
    assert to_sql(0, ShortType()) == "0 :: smallint"
    assert to_sql(0, IntegerType()) == "0 :: int"
    assert to_sql(0, LongType()) == "0 :: bigint"
    assert to_sql(0, BooleanType()) == "0 :: boolean"

    assert to_sql(float("nan"), FloatType()) == "'Nan' :: FLOAT"
    assert to_sql(float("inf"), FloatType()) == "'Infinity' :: FLOAT"
    assert to_sql(float("-inf"), FloatType()) == "'-Infinity' :: FLOAT"
    assert to_sql(1.2, FloatType()) == "'1.2' :: FLOAT"

    assert to_sql(float("nan"), DoubleType()) == "'Nan' :: DOUBLE"
    assert to_sql(float("inf"), DoubleType()) == "'Infinity' :: DOUBLE"
    assert to_sql(float("-inf"), DoubleType()) == "'-Infinity' :: DOUBLE"
    assert to_sql(1.2, DoubleType()) == "'1.2' :: DOUBLE"

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
        to_sql(bytearray.fromhex("2Ef0 F1f2 "), BinaryType()) == "'2ef0f1f2' :: binary"
    )


def test_to_sql_without_cast():
    f = DataTypeMapper.to_sql_without_cast

    assert f(None, NullType()) == "NULL"
    assert f(None, IntegerType()) == "NULL"

    assert f("abc", StringType()) == """abc"""
    assert f(123, StringType()) == "123"
    assert f(0.2, StringType()) == "0.2"

    assert f(123, IntegerType()) == "123"
    assert f(0.2, FloatType()) == "0.2"
    assert f(0.2, DoubleType()) == "0.2"
