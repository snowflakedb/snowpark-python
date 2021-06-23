#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from array import array
from datetime import date, datetime, time
from decimal import Decimal

import pytest

from src.snowflake.snowpark.types.sp_data_types import (
    ArrayType as SPArrayType,
    BinaryType as SPBinaryType,
    ByteType as SPByteType,
    DateType as SPDateType,
    DecimalType as SPDecimalType,
    DoubleType as SPDoubleType,
    FloatType as SPFloatType,
    IntegerType as SPIntegerType,
    LongType as SPLongType,
    MapType as SPMapType,
    NullType as SPNullType,
    ShortType as SPShortType,
    StringType as SPStringType,
    TimestampType as SPTimestampType,
    TimeType as SPTimeType,
)
from src.snowflake.snowpark.types.types_package import _infer_type


# TODO complete for schema case
def test_py_to_sp_type():
    assert type(_infer_type(None)) == SPNullType
    assert type(_infer_type(1)) == SPLongType
    assert type(_infer_type(3.14)) == SPDoubleType
    assert type(_infer_type("a")) == SPStringType
    assert type(_infer_type(bytearray("a", "utf-8"))) == SPBinaryType
    assert (
        type(_infer_type(Decimal(0.00000000000000000000000000000000000000233)))
        == SPDecimalType
    )
    assert type(_infer_type(date(2021, 5, 25))) == SPDateType
    assert type(_infer_type(datetime(2021, 5, 25, 0, 47, 41))) == SPTimestampType
    assert type(_infer_type(time(17, 57, 10))) == SPTimeType
    assert type(_infer_type((1024).to_bytes(2, byteorder="big")))

    res = _infer_type({1: "abc"})
    assert type(res) == SPMapType
    assert type(res.key_type) == SPLongType
    assert type(res.value_type) == SPStringType

    res = _infer_type({None: None})
    assert type(res) == SPMapType
    assert type(res.key_type) == SPNullType
    assert type(res.value_type) == SPNullType

    res = _infer_type({None: 1})
    assert type(res) == SPMapType
    assert type(res.key_type) == SPNullType
    assert type(res.value_type) == SPNullType

    res = _infer_type({1: None})
    assert type(res) == SPMapType
    assert type(res.key_type) == SPNullType
    assert type(res.value_type) == SPNullType

    res = _infer_type([1, 2, 3])
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPLongType
    assert type(res.contains_null)

    res = _infer_type([None])
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPNullType
    assert type(res.contains_null)

    # Arrays
    res = _infer_type(array("f"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPFloatType

    res = _infer_type(array("d"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPDoubleType

    res = _infer_type(array("l"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPLongType

    with pytest.raises(TypeError):
        _infer_type(array("L"))

    res = _infer_type(array("b"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPByteType

    res = _infer_type(array("B"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPShortType

    res = _infer_type(array("u"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPStringType

    res = _infer_type(array("h"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPShortType

    res = _infer_type(array("H"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPIntegerType

    res = _infer_type(array("i"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPIntegerType

    res = _infer_type(array("I"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPLongType

    with pytest.raises(TypeError):
        _infer_type(array("q"))

    with pytest.raises(TypeError):
        _infer_type(array("Q"))
