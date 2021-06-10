#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from src.snowflake.snowpark.internal.analyzer.datatype_mapper import DataTypeMapper

from src.snowflake.snowpark.types.sp_data_types import NullType as SPNullType, \
    LongType as SPLongType, StringType as SPStringType, DoubleType as SPDoubleType, \
    BinaryType as SPBinaryType, DecimalType as SPDecimalType, DateType as SPDateType, \
    TimestampType as SPTimestampType, IntegerType as SPIntegerType, ShortType as SPShortType, \
    FloatType as SPFloatType, ArrayType as SPArrayType, MapType as SPMapType, \
    StructType as SPStructType, ByteType as SPByteType, BooleanType as SPBooleanType, \
    GeographyType as SPGeographyType

from decimal import Decimal
import pytest


def test_to_sql():
    to_sql = DataTypeMapper.to_sql

    # Test nulls
    assert to_sql(None, SPNullType()) == "NULL"
    assert to_sql(None, SPArrayType(SPDoubleType(), False)) == "NULL"
    assert to_sql(None, SPMapType(SPIntegerType(), SPByteType(), False)) == "NULL"
    assert to_sql(None, SPStructType([])) == "NULL"
    assert to_sql(None, SPGeographyType(SPByteType())) == "NULL"

    assert to_sql(None, SPIntegerType()) == "NULL :: int"
    assert to_sql(None, SPShortType()) == "NULL :: smallint"
    assert to_sql(None, SPByteType()) == "NULL :: tinyint"
    assert to_sql(None, SPLongType()) == "NULL :: bigint"
    assert to_sql(None, SPFloatType()) == "NULL :: float"
    assert to_sql(None, SPStringType()) == "NULL :: string"
    assert to_sql(None, SPDoubleType()) == "NULL :: double"
    assert to_sql(None, SPBooleanType()) == "NULL :: boolean"

    assert to_sql(None, "Not any of the previous SP types") == "NULL"

    # Test non-nulls
    assert to_sql("\\\\ '  ' abc \n \\\\", SPStringType()) == "'\\\\\\\\ ''  '' abc \\\\n \\\\\\\\'"
    assert to_sql(1, SPByteType()) == "1:: tinyint"
    assert to_sql(1, SPShortType()) == "1:: smallint"
    assert to_sql(1, SPIntegerType()) == "1:: int"
    assert to_sql(0, SPLongType()) == "0:: bigint"
    assert to_sql(1, SPLongType()) == "1:: bigint"
    assert to_sql(1, SPBooleanType()) == "1:: boolean"

    assert to_sql(float('nan'), SPFloatType()) == "'Nan' :: FLOAT"
    assert to_sql(float('inf'), SPFloatType()) == "'Infinity' :: FLOAT"
    assert to_sql(float('-inf'), SPFloatType()) == "'-Infinity' :: FLOAT"
    assert to_sql(1.2, SPFloatType()) == "'1.2' :: FLOAT"

    assert to_sql(float('nan'), SPDoubleType()) == "'Nan' :: DOUBLE"
    assert to_sql(float('inf'), SPDoubleType()) == "'Infinity' :: DOUBLE"
    assert to_sql(float('-inf'), SPDoubleType()) == "'-Infinity' :: DOUBLE"
    assert to_sql(1.2, SPDoubleType()) == "'1.2' :: DOUBLE"

    assert to_sql(Decimal(0.5), SPDecimalType(2, 1)) == '0.5 ::  NUMBER (2, 1)'

    assert to_sql(397, SPDateType()) == "DATE '1971-02-02'"
    # value type must be int
    with pytest.raises(Exception):
        to_sql(.397, SPDateType())

    assert to_sql(1622002533000000, SPTimestampType()) == "TIMESTAMP '2021-05-26 04:15:33.000'"
    # value type must be int
    with pytest.raises(Exception):
        to_sql(.2, SPTimestampType())

    assert to_sql(bytearray.fromhex('2Ef0 F1f2 '), SPBinaryType()) == "'2ef0f1f2' :: binary"


def test_to_sql_without_cast():
    f = DataTypeMapper.to_sql_without_cast

    assert f(None, SPNullType) == "NULL"
    assert f(None, SPIntegerType) == "NULL"

    assert f('abc', SPStringType()) == """abc"""
    assert f(123, SPStringType()) == "123"
    assert f(0.2, SPStringType()) == "0.2"

    assert f(123, SPIntegerType()) == "123"
    assert f(0.2, SPFloatType()) == "0.2"
    assert f(0.2, SPDoubleType()) == "0.2"
