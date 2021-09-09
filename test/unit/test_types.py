#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import typing
from array import array
from collections import OrderedDict, defaultdict
from datetime import date, datetime, time
from decimal import Decimal
from test.utils import IS_WINDOWS

import pytest

from snowflake.snowpark.types.sf_types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    ColumnIdentifier,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntegralType,
    LongType,
    MapType,
    NullType,
    NumericType,
    ShortType,
    StringType,
    StructField,
    TimestampType,
    TimeType,
    VariantType,
)
from snowflake.snowpark.types.sp_data_types import (
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
from snowflake.snowpark.types.types_package import (
    _infer_type,
    _python_type_to_snow_type,
)


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
    if IS_WINDOWS:
        assert type(res.element_type) == SPIntegerType
    else:
        assert type(res.element_type) == SPLongType

    if IS_WINDOWS:
        res = _infer_type(array("L"))
        assert type(res) == SPArrayType
        assert type(res.element_type) == SPLongType
    else:
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

    res = _infer_type(array("q"))
    assert type(res) == SPArrayType
    assert type(res.element_type) == SPLongType

    with pytest.raises(TypeError):
        _infer_type(array("Q"))


def test_sf_datatype_names():
    assert DataType().type_name == "Data"
    assert MapType(BinaryType(), FloatType()).type_name == "MapType[Binary,Float]"
    assert VariantType().type_name == "Variant"
    assert BinaryType().type_name == "Binary"
    assert BooleanType().type_name == "Boolean"
    assert DateType().type_name == "Date"
    assert StringType().type_name == "String"
    assert NumericType().type_name == "Numeric"
    assert IntegralType().type_name == "Integral"
    assert NumericType().type_name == "Numeric"
    assert TimeType().type_name == "Time"
    assert ByteType().type_name == "Byte"
    assert ShortType().type_name == "Short"
    assert IntegerType().type_name == "Integer"
    assert LongType().type_name == "Long"
    assert FloatType().type_name == "Float"
    assert DoubleType().type_name == "Double"
    assert DecimalType(1, 2).type_name == "Decimal(1,2)"

    assert str(DataType()) == "Data"
    assert str(MapType(BinaryType(), FloatType())) == "MapType[Binary,Float]"
    assert str(VariantType()) == "Variant"
    assert str(BinaryType()) == "Binary"
    assert str(BooleanType()) == "Boolean"
    assert str(DateType()) == "Date"
    assert str(StringType()) == "String"
    assert str(NumericType()) == "Numeric"
    assert str(IntegralType()) == "Integral"
    assert str(NumericType()) == "Numeric"
    assert str(TimeType()) == "Time"
    assert str(ByteType()) == "Byte"
    assert str(ShortType()) == "Short"
    assert str(IntegerType()) == "Integer"
    assert str(LongType()) == "Long"
    assert str(FloatType()) == "Float"
    assert str(DoubleType()) == "Double"
    assert str(DecimalType(1, 2)) == "Decimal(1,2)"


def test_struct_field_name():
    column_identifier = ColumnIdentifier("identifier")
    assert StructField(column_identifier, IntegerType(), False).name == "identifier"
    assert (
        str(StructField(column_identifier, IntegerType(), False))
        == "StructField(identifier, Integer, Nullable=False)"
    )


def test_strip_unnecessary_quotes():
    # Get a function reference for brevity
    func = ColumnIdentifier.strip_unnecessary_quotes

    # UPPER CASE
    #
    # No quotes, some spaces
    assert func("ABC") == "ABC"
    assert func(" ABC") == " ABC"
    assert func("ABC ") == "ABC "
    assert func(" ABC  ") == " ABC  "
    assert func(" ABC12  ") == " ABC12  "
    assert func(" $123  ") == " $123  "

    # Double quotes, some spaces
    assert func('"ABC"') == "ABC"
    assert func('" ABC"') == '" ABC"'
    assert func('"ABC "') == '"ABC "'
    assert func('" ABC  "') == '" ABC  "'
    assert func('"ABC') == '"ABC'
    assert func('ABC"') == 'ABC"'
    assert func('" ABC12  "') == '" ABC12  "'
    assert func('" $123  "') == '" $123  "'

    # LOWER CASE
    #
    # No quotes, some spaces
    assert func("abc") == "abc"
    assert func(" abc") == " abc"
    assert func("abc ") == "abc "
    assert func(" abc  ") == " abc  "
    assert func(" abc12  ") == " abc12  "
    assert func(" $123  ") == " $123  "

    # Double quotes, some spaces
    assert func('"abc"') == '"abc"'
    assert func('" abc"') == '" abc"'
    assert func('"abc "') == '"abc "'
    assert func('" abc  "') == '" abc  "'
    assert func('"abc') == '"abc'
    assert func('abc"') == 'abc"'
    assert func(" abc12  ") == " abc12  "
    assert func(" $123  ") == " $123  "

    # $ followed by digits
    #
    # No quotes, some spaces
    assert func("$123") == "$123"
    assert func("$123A") == "$123A"
    assert func("$ABC") == "$ABC"
    assert func(" $123") == " $123"
    assert func("$123 ") == "$123 "
    assert func(" $abc  ") == " $abc  "

    # Double quotes, some spaces
    assert func('"$123"') == "$123"
    assert func('"$123A"') == '"$123A"'
    assert func('"$ABC"') == '"$ABC"'
    assert func('" $123"') == '" $123"'
    assert func('"$123 "') == '"$123 "'
    assert func('" $abc  "') == '" $abc  "'


def test_python_type_to_snow_type():
    # basic types
    assert _python_type_to_snow_type(int) == (LongType(), False)
    assert _python_type_to_snow_type(float) == (DoubleType(), False)
    assert _python_type_to_snow_type(str) == (StringType(), False)
    assert _python_type_to_snow_type(bool) == (BooleanType(), False)
    assert _python_type_to_snow_type(bytes) == (BinaryType(), False)
    assert _python_type_to_snow_type(bytearray) == (BinaryType(), False)
    assert _python_type_to_snow_type(type(None)) == (NullType(), False)
    assert _python_type_to_snow_type(date) == (DateType(), False)
    assert _python_type_to_snow_type(time) == (TimeType(), False)
    assert _python_type_to_snow_type(datetime) == (TimestampType(), False)
    assert _python_type_to_snow_type(Decimal) == (DecimalType(), False)
    assert _python_type_to_snow_type(typing.Optional[str]) == (StringType(), True)
    assert _python_type_to_snow_type(typing.Union[str, None]) == (
        StringType(),
        True,
    )
    assert _python_type_to_snow_type(typing.List[int]) == (
        ArrayType(LongType()),
        False,
    )
    assert _python_type_to_snow_type(typing.List) == (
        ArrayType(StringType()),
        False,
    )
    assert _python_type_to_snow_type(list) == (ArrayType(StringType()), False)
    assert _python_type_to_snow_type(typing.Tuple[int]) == (
        ArrayType(LongType()),
        False,
    )
    assert _python_type_to_snow_type(typing.Tuple) == (
        ArrayType(StringType()),
        False,
    )
    assert _python_type_to_snow_type(tuple) == (ArrayType(StringType()), False)
    assert _python_type_to_snow_type(typing.Dict[str, int]) == (
        MapType(StringType(), LongType()),
        False,
    )
    assert _python_type_to_snow_type(typing.Dict) == (
        MapType(StringType(), StringType()),
        False,
    )
    assert _python_type_to_snow_type(dict) == (
        MapType(StringType(), StringType()),
        False,
    )
    assert _python_type_to_snow_type(typing.DefaultDict[str, int]) == (
        MapType(StringType(), LongType()),
        False,
    )
    assert _python_type_to_snow_type(typing.DefaultDict) == (
        MapType(StringType(), StringType()),
        False,
    )
    assert _python_type_to_snow_type(defaultdict) == (
        MapType(StringType(), StringType()),
        False,
    )
    # TODO: add typing.OrderedDict after upgrading to Python 3.8
    # assert _python_type_to_snowpark_type(typing.OrderedDict[str, int]) == (
    #     MapType(StringType(), LongType()),
    #     False,
    # )
    # assert _python_type_to_snowpark_type(typing.OrderedDict) == (
    #     MapType(StringType(), StringType()),
    #     False,
    # )
    assert _python_type_to_snow_type(OrderedDict) == (
        MapType(StringType(), StringType()),
        False,
    )
    assert _python_type_to_snow_type(typing.Any) == (VariantType(), False)

    # complicated (nested) types
    assert _python_type_to_snow_type(typing.Optional[typing.Optional[str]]) == (
        StringType(),
        True,
    )
    assert _python_type_to_snow_type(typing.Optional[typing.List[str]]) == (
        ArrayType(StringType()),
        True,
    )
    assert _python_type_to_snow_type(typing.List[typing.List[float]]) == (
        ArrayType(ArrayType(DoubleType())),
        False,
    )
    assert _python_type_to_snow_type(
        typing.List[typing.List[typing.Optional[datetime]]]
    ) == (ArrayType(ArrayType(TimestampType())), False)
    assert _python_type_to_snow_type(typing.Dict[str, typing.List]) == (
        MapType(StringType(), ArrayType(StringType())),
        False,
    )

    # unsupported types
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.AnyStr)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.TypeVar)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.Callable)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.IO)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.Iterable)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.Generic)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.Set)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(set)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.Union[str, int, None])
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.Union[None, str])
    with pytest.raises(TypeError):
        _python_type_to_snow_type(StringType)
