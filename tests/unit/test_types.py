#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import os
import typing
from array import array
from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal

import pandas
import pytest

from snowflake.snowpark._internal.type_utils import (
    _get_number_precision_scale,
    _infer_type,
    _python_type_to_snow_type,
    _retrieve_func_type_hints_from_source,
)
from snowflake.snowpark.types import (
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
    Geography,
    GeographyType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    PandasDataFrame,
    PandasDataFrameType,
    PandasSeries,
    PandasSeriesType,
    ShortType,
    StringType,
    StructField,
    TimestampType,
    TimeType,
    Variant,
    VariantType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)
from tests.utils import IS_WINDOWS, TestFiles

resources_path = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../resources")
)
test_files = TestFiles(resources_path)


# TODO complete for schema case
def test_py_to_type():
    assert type(_infer_type(None)) == NullType
    assert type(_infer_type(1)) == LongType
    assert type(_infer_type(3.14)) == FloatType
    assert type(_infer_type("a")) == StringType
    assert type(_infer_type(bytearray("a", "utf-8"))) == BinaryType
    assert (
        type(_infer_type(Decimal(0.00000000000000000000000000000000000000233)))
        == DecimalType
    )
    assert type(_infer_type(date(2021, 5, 25))) == DateType
    assert type(_infer_type(datetime(2021, 5, 25, 0, 47, 41))) == TimestampType
    assert type(_infer_type(time(17, 57, 10))) == TimeType
    assert type(_infer_type((1024).to_bytes(2, byteorder="big")))

    res = _infer_type({1: "abc"})
    assert type(res) == MapType
    assert type(res.key_type) == LongType
    assert type(res.value_type) == StringType

    res = _infer_type({None: None})
    assert type(res) == MapType
    assert type(res.key_type) == NullType
    assert type(res.value_type) == NullType

    res = _infer_type({None: 1})
    assert type(res) == MapType
    assert type(res.key_type) == NullType
    assert type(res.value_type) == NullType

    res = _infer_type({1: None})
    assert type(res) == MapType
    assert type(res.key_type) == NullType
    assert type(res.value_type) == NullType

    res = _infer_type([1, 2, 3])
    assert type(res) == ArrayType
    assert type(res.element_type) == LongType

    res = _infer_type([None])
    assert type(res) == ArrayType
    assert type(res.element_type) == NullType

    # Arrays
    res = _infer_type(array("f"))
    assert type(res) == ArrayType
    assert type(res.element_type) == FloatType

    res = _infer_type(array("d"))
    assert type(res) == ArrayType
    assert type(res.element_type) == DoubleType

    res = _infer_type(array("l"))
    assert type(res) == ArrayType
    if IS_WINDOWS:
        assert type(res.element_type) == IntegerType
    else:
        assert type(res.element_type) == LongType

    if IS_WINDOWS:
        res = _infer_type(array("L"))
        assert type(res) == ArrayType
        assert type(res.element_type) == LongType
    else:
        with pytest.raises(TypeError):
            _infer_type(array("L"))

    res = _infer_type(array("b"))
    assert type(res) == ArrayType
    assert type(res.element_type) == ByteType

    res = _infer_type(array("B"))
    assert type(res) == ArrayType
    assert type(res.element_type) == ShortType

    res = _infer_type(array("u"))
    assert type(res) == ArrayType
    assert type(res.element_type) == StringType

    res = _infer_type(array("h"))
    assert type(res) == ArrayType
    assert type(res.element_type) == ShortType

    res = _infer_type(array("H"))
    assert type(res) == ArrayType
    assert type(res.element_type) == IntegerType

    res = _infer_type(array("i"))
    assert type(res) == ArrayType
    assert type(res.element_type) == IntegerType

    res = _infer_type(array("I"))
    assert type(res) == ArrayType
    assert type(res.element_type) == LongType

    res = _infer_type(array("q"))
    assert type(res) == ArrayType
    assert type(res.element_type) == LongType

    with pytest.raises(TypeError):
        _infer_type(array("Q"))


def test_sf_datatype_names():
    assert DataType().type_name == "Data"
    assert MapType(BinaryType(), FloatType()).type_name == "MapType[Binary, Float]"
    assert VariantType().type_name == "Variant"
    assert BinaryType().type_name == "Binary"
    assert BooleanType().type_name == "Boolean"
    assert DateType().type_name == "Date"
    assert StringType().type_name == "String"
    assert _NumericType().type_name == "_Numeric"
    assert _IntegralType().type_name == "_Integral"
    assert _FractionalType().type_name == "_Fractional"
    assert TimeType().type_name == "Time"
    assert ByteType().type_name == "Byte"
    assert ShortType().type_name == "Short"
    assert IntegerType().type_name == "Integer"
    assert LongType().type_name == "Long"
    assert FloatType().type_name == "Float"
    assert DoubleType().type_name == "Double"
    assert DecimalType(1, 2).type_name == "Decimal(1, 2)"

    assert str(DataType()) == "Data"
    assert str(MapType(BinaryType(), FloatType())) == "MapType[Binary, Float]"
    assert str(VariantType()) == "Variant"
    assert str(BinaryType()) == "Binary"
    assert str(BooleanType()) == "Boolean"
    assert str(DateType()) == "Date"
    assert str(StringType()) == "String"
    assert str(_NumericType()) == "_Numeric"
    assert str(_IntegralType()) == "_Integral"
    assert str(_FractionalType()) == "_Fractional"
    assert str(TimeType()) == "Time"
    assert str(ByteType()) == "Byte"
    assert str(ShortType()) == "Short"
    assert str(IntegerType()) == "Integer"
    assert str(LongType()) == "Long"
    assert str(FloatType()) == "Float"
    assert str(DoubleType()) == "Double"
    assert str(DecimalType(1, 2)) == "Decimal(1, 2)"


def test_struct_field_name():
    column_identifier = ColumnIdentifier("identifier")
    assert StructField(column_identifier, IntegerType(), False).name == "identifier"
    assert (
        str(StructField(column_identifier, IntegerType(), False))
        == "StructField(identifier, Integer, Nullable=False)"
    )


def test_strip_unnecessary_quotes():
    # Get a function reference for brevity
    func = ColumnIdentifier._strip_unnecessary_quotes

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
    def check_type(python_type, snow_type, is_nullable):
        assert _python_type_to_snow_type(python_type) == (snow_type, is_nullable)
        assert _python_type_to_snow_type(
            getattr(python_type, "__name__", str(python_type))
        ) == (snow_type, is_nullable)

    # basic types
    check_type(int, LongType(), False)
    check_type(float, FloatType(), False)
    check_type(str, StringType(), False)
    check_type(bool, BooleanType(), False)
    check_type(bytes, BinaryType(), False)
    check_type(bytearray, BinaryType(), False)
    check_type(type(None), NullType(), False)
    check_type(date, DateType(), False)
    check_type(time, TimeType(), False)
    check_type(datetime, TimestampType(), False)
    check_type(Decimal, DecimalType(38, 18), False)
    check_type(typing.Optional[str], StringType(), True)
    check_type(
        typing.Union[str, None],
        StringType(),
        True,
    )
    check_type(
        typing.List[int],
        ArrayType(LongType()),
        False,
    )
    check_type(
        typing.List,
        ArrayType(StringType()),
        False,
    )
    check_type(list, ArrayType(StringType()), False)
    check_type(
        typing.Tuple[int],
        ArrayType(LongType()),
        False,
    )
    check_type(
        typing.Tuple,
        ArrayType(StringType()),
        False,
    )
    check_type(tuple, ArrayType(StringType()), False)
    check_type(
        typing.Dict[str, int],
        MapType(StringType(), LongType()),
        False,
    )
    check_type(
        typing.Dict,
        MapType(StringType(), StringType()),
        False,
    )
    check_type(
        dict,
        MapType(StringType(), StringType()),
        False,
    )
    check_type(Variant, VariantType(), False)
    check_type(Geography, GeographyType(), False)
    check_type(pandas.Series, PandasSeriesType(None), False)
    check_type(pandas.DataFrame, PandasDataFrameType(()), False)
    check_type(PandasSeries, PandasSeriesType(None), False)
    check_type(PandasDataFrame, PandasDataFrameType(()), False)

    # complicated (nested) types
    check_type(
        typing.Optional[typing.Optional[str]],
        StringType(),
        True,
    )
    check_type(
        typing.Optional[typing.List[str]],
        ArrayType(StringType()),
        True,
    )
    check_type(
        typing.List[typing.List[float]],
        ArrayType(ArrayType(FloatType())),
        False,
    )
    check_type(
        typing.List[typing.List[typing.Optional[datetime]]],
        ArrayType(ArrayType(TimestampType())),
        False,
    )
    check_type(
        typing.Dict[str, typing.List],
        MapType(StringType(), ArrayType(StringType())),
        False,
    )
    check_type(PandasSeries[float], PandasSeriesType(FloatType()), False)
    check_type(
        PandasSeries[typing.Dict[str, typing.List]],
        PandasSeriesType(MapType(StringType(), ArrayType(StringType()))),
        False,
    )
    check_type(
        PandasDataFrame[int, str],
        PandasDataFrameType([LongType(), StringType()]),
        False,
    )
    check_type(
        PandasDataFrame[int, PandasSeries[datetime]],
        PandasDataFrameType([LongType(), PandasSeriesType(TimestampType())]),
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
        _python_type_to_snow_type(typing.OrderedDict)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(defaultdict)
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.Union[str, int, None])
    with pytest.raises(TypeError):
        _python_type_to_snow_type(typing.Union[None, str])
    with pytest.raises(TypeError):
        _python_type_to_snow_type(StringType)

    # invalid type str
    with pytest.raises(NameError):
        _python_type_to_snow_type("string")


@pytest.mark.parametrize("decimal_word", ["number", "numeric", "decimal"])
def test_decimal_regular_expression(decimal_word):
    assert _get_number_precision_scale(f"{decimal_word}") is None
    assert _get_number_precision_scale(f" {decimal_word}") is None
    assert _get_number_precision_scale(f"{decimal_word} ") is None
    assert _get_number_precision_scale(f"{decimal_word}") is None
    assert _get_number_precision_scale(f"{decimal_word}(2) ") is None
    assert _get_number_precision_scale(f"a{decimal_word}(2,1)") is None
    assert _get_number_precision_scale(f"{decimal_word}(2,1) a") is None

    assert _get_number_precision_scale(f"{decimal_word}(2,1)") == (2, 1)
    assert _get_number_precision_scale(f" {decimal_word}(2,1)") == (2, 1)
    assert _get_number_precision_scale(f"{decimal_word}(2,1) ") == (2, 1)
    assert _get_number_precision_scale(f"  {decimal_word}  (  2  ,  1  )  ") == (2, 1)


def test_retrieve_func_type_hints_from_source():
    func_name = "foo"

    source = f"""
def {func_name}() -> None:
    return None
"""
    assert _retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "return": "NoneType"
    }

    source = f"""
def {func_name}() -> int:
    return 1
"""
    assert _retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "return": "int"
    }

    source = f"""
def {func_name}() -> int:
    return 1

def {func_name}_{func_name}(x: int) -> int:
    return x
"""
    assert _retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "return": "int"
    }

    source = f"""
def {func_name}(x: bytes) -> int:
    return 1
"""
    assert _retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "x": "bytes",
        "return": "int",
    }

    source = f"""
def {func_name}(x: List[str], y: None) -> Optional[int]:
    return None
"""
    assert _retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "x": "List[str]",
        "y": "NoneType",
        "return": "Optional[int]",
    }

    source = f"""
def {func_name}(x: collections.defaultdict, y: Union[datetime.date, time]) -> Optional[typing.Tuple[decimal.Decimal, Variant, List[float]]]:
    return (1, 2)
"""
    assert _retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "x": "collections.defaultdict",
        "y": "Union[datetime.date, time]",
        "return": "Optional[typing.Tuple[decimal.Decimal, Variant, List[float]]]",
    }

    assert _retrieve_func_type_hints_from_source(
        test_files.test_udf_py_file, "mod5"
    ) == {"x": "int", "return": "int"}

    # negative case
    with pytest.raises(UnicodeDecodeError):
        _retrieve_func_type_hints_from_source(test_files.test_file_avro, "mod5")

    source = f"""
def {func_name}_{func_name}(x: int) -> int:
    return x
"""
    with pytest.raises(ValueError) as ex_info:
        _retrieve_func_type_hints_from_source("", func_name, _source=source)
    assert "is not found in file" in str(ex_info)

    source = f"""
def {func_name}() -> 1:
    return 1
"""
    with pytest.raises(TypeError) as ex_info:
        _retrieve_func_type_hints_from_source("", func_name, _source=source)
    assert "invalid type annotation" in str(ex_info)
