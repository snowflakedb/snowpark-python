#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import decimal
import os
import sys
import typing
from array import array
from collections import defaultdict
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from snowflake.snowpark.dataframe import DataFrame

try:
    import pandas

    from snowflake.snowpark.types import (
        PandasDataFrame,
        PandasDataFrameType,
        PandasSeries,
        PandasSeriesType,
    )

    is_pandas_available = True
except ImportError:
    is_pandas_available = False


from snowflake.snowpark._internal.type_utils import (
    convert_sf_to_sp_type,
    convert_sp_to_sf_type,
    get_number_precision_scale,
    infer_schema,
    infer_type,
    merge_type,
    python_type_to_snow_type,
    python_value_str_to_object,
    retrieve_func_type_hints_from_source,
    snow_type_to_dtype_str,
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
    Geometry,
    GeometryType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    Variant,
    VariantType,
    VectorType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)
from tests.utils import IS_WINDOWS, TestFiles

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

resources_path = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../resources")
)
test_files = TestFiles(resources_path)


# TODO complete for schema case
def test_py_to_type():
    assert type(infer_type(None)) == NullType
    assert type(infer_type(1)) == LongType
    assert type(infer_type(3.14)) == FloatType
    assert type(infer_type("a")) == StringType
    assert type(infer_type(bytearray("a", "utf-8"))) == BinaryType
    assert (
        type(infer_type(Decimal(0.00000000000000000000000000000000000000233)))
        == DecimalType
    )
    assert type(infer_type(date(2021, 5, 25))) == DateType
    assert type(infer_type(datetime(2021, 5, 25, 0, 47, 41))) == TimestampType
    # infer tz-aware datetime to TIMESTAMP_TZ
    assert infer_type(
        datetime(2021, 5, 25, 0, 47, 41, tzinfo=timezone.utc)
    ) == TimestampType(TimestampTimeZone.TZ)

    assert type(infer_type(time(17, 57, 10))) == TimeType
    assert type(infer_type((1024).to_bytes(2, byteorder="big")))

    res = infer_type({1: "abc"})
    assert type(res) == MapType
    assert type(res.key_type) == LongType
    assert type(res.value_type) == StringType

    res = infer_type({None: None})
    assert type(res) == MapType
    assert type(res.key_type) == NullType
    assert type(res.value_type) == NullType

    res = infer_type({None: 1})
    assert type(res) == MapType
    assert type(res.key_type) == NullType
    assert type(res.value_type) == NullType

    res = infer_type({1: None})
    assert type(res) == MapType
    assert type(res.key_type) == NullType
    assert type(res.value_type) == NullType

    res = infer_type([1, 2, 3])
    assert type(res) == ArrayType
    assert type(res.element_type) == LongType

    res = infer_type([None])
    assert type(res) == ArrayType
    assert type(res.element_type) == NullType

    # Arrays
    res = infer_type(array("f"))
    assert type(res) == ArrayType
    assert type(res.element_type) == FloatType

    res = infer_type(array("d"))
    assert type(res) == ArrayType
    assert type(res.element_type) == DoubleType

    res = infer_type(array("l"))
    assert type(res) == ArrayType
    if IS_WINDOWS:
        assert type(res.element_type) == IntegerType
    else:
        assert type(res.element_type) == LongType

    if IS_WINDOWS:
        res = infer_type(array("L"))
        assert type(res) == ArrayType
        assert type(res.element_type) == LongType
    else:
        with pytest.raises(TypeError):
            infer_type(array("L"))

    res = infer_type(array("b"))
    assert type(res) == ArrayType
    assert type(res.element_type) == ByteType

    res = infer_type(array("B"))
    assert type(res) == ArrayType
    assert type(res.element_type) == ShortType

    res = infer_type(array("u"))
    assert type(res) == ArrayType
    assert type(res.element_type) == StringType

    res = infer_type(array("h"))
    assert type(res) == ArrayType
    assert type(res.element_type) == ShortType

    res = infer_type(array("H"))
    assert type(res) == ArrayType
    assert type(res.element_type) == IntegerType

    res = infer_type(array("i"))
    assert type(res) == ArrayType
    assert type(res.element_type) == IntegerType

    res = infer_type(array("I"))
    assert type(res) == ArrayType
    assert type(res.element_type) == LongType

    res = infer_type(array("q"))
    assert type(res) == ArrayType
    assert type(res.element_type) == LongType

    with pytest.raises(TypeError):
        infer_type(array("Q"))

    with pytest.raises(TypeError, match="not supported type"):
        infer_type(DataType())


def test_sf_datatype_names():
    assert str(DataType()) == "DataType()"
    assert (
        str(MapType(BinaryType(), FloatType())) == "MapType(BinaryType(), FloatType())"
    )
    assert str(VariantType()) == "VariantType()"
    assert str(BinaryType()) == "BinaryType()"
    assert str(BooleanType()) == "BooleanType()"
    assert str(DateType()) == "DateType()"
    assert str(StringType()) == "StringType()"
    assert str(StringType(23)) == "StringType(23)"
    assert str(_NumericType()) == "_NumericType()"
    assert str(_IntegralType()) == "_IntegralType()"
    assert str(_FractionalType()) == "_FractionalType()"
    assert str(TimeType()) == "TimeType()"
    assert str(ByteType()) == "ByteType()"
    assert str(ShortType()) == "ShortType()"
    assert str(IntegerType()) == "IntegerType()"
    assert str(LongType()) == "LongType()"
    assert str(FloatType()) == "FloatType()"
    assert str(DoubleType()) == "DoubleType()"
    assert str(DecimalType(1, 2)) == "DecimalType(1, 2)"


def test_sf_datatype_hashes():
    assert hash(DataType()) == hash("DataType()")
    assert hash(MapType(BinaryType(), FloatType())) == hash(
        "MapType(BinaryType(), FloatType())"
    )
    assert hash(VariantType()) == hash("VariantType()")
    assert hash(BinaryType()) == hash("BinaryType()")
    assert hash(BooleanType()) == hash("BooleanType()")
    assert hash(DateType()) == hash("DateType()")
    assert hash(StringType()) == hash("StringType()")
    assert hash(StringType(12)) == hash("StringType(12)")
    assert hash(StringType()) == hash(StringType(is_max_size=True))
    assert hash(_NumericType()) == hash("_NumericType()")
    assert hash(_IntegralType()) == hash("_IntegralType()")
    assert hash(_FractionalType()) == hash("_FractionalType()")
    assert hash(TimeType()) == hash("TimeType()")
    assert hash(ByteType()) == hash("ByteType()")
    assert hash(ShortType()) == hash("ShortType()")
    assert hash(IntegerType()) == hash("IntegerType()")
    assert hash(LongType()) == hash("LongType()")
    assert hash(FloatType()) == hash("FloatType()")
    assert hash(DoubleType()) == hash("DoubleType()")
    assert hash(DecimalType(1, 2)) == hash("DecimalType(1, 2)")


def test_merge_type():
    sf_a = StructField("A", LongType(), False)
    sf_b = StructField("B", LongType(), False)
    sf_c = StructField("C", LongType(), False)

    type_1 = StructType([sf_a, sf_b])
    type_2 = StructType([sf_b, sf_c])

    merge_12 = merge_type(type_1, type_2)
    merge_21 = merge_type(type_2, type_1)

    assert merge_12["A"] == merge_21["A"]
    assert merge_12["B"] == merge_21["B"]
    assert merge_12["C"] == merge_21["C"]


def test_struct_field_name():
    column_identifier = ColumnIdentifier("identifier")
    assert StructField(column_identifier, IntegerType(), False).name == "IDENTIFIER"
    assert (
        str(StructField(column_identifier, IntegerType(), False))
        == "StructField('IDENTIFIER', IntegerType(), nullable=False)"
    )

    # check that we cover __eq__ works with types other than str and ColumnIdentifier
    assert (column_identifier == 7) is False

    # check StructField name setter works
    sf = StructField(column_identifier, IntegerType(), False)
    sf.name = "integer type"
    assert sf.column_identifier.name == '"integer type"'


def test_struct_get_item():
    field_a = StructField("a", IntegerType())
    field_b = StructField("b", StringType())
    field_c = StructField("c", LongType())

    struct_type = StructType([field_a, field_b, field_c])

    assert struct_type[0] == field_a
    assert struct_type[1] == field_b
    assert struct_type[2] == field_c

    assert struct_type["A"] == field_a
    assert struct_type["B"] == field_b
    assert struct_type["C"] == field_c

    assert struct_type[0:3] == StructType([field_a, field_b, field_c])
    assert struct_type[1:3] == StructType([field_b, field_c])
    assert struct_type[1:2] == StructType([field_b])
    assert struct_type[2:3] == StructType([field_c])

    with pytest.raises(KeyError, match="No StructField named d"):
        struct_type["d"]

    with pytest.raises(IndexError, match="list index out of range"):
        struct_type[5]

    with pytest.raises(
        TypeError,
        match="StructType items should be strings, integers or slices, but got float",
    ):
        struct_type[5.0]

    with pytest.raises(
        TypeError, match="StructType object does not support item assignment"
    ):
        struct_type[0] = field_c


def test_struct_type_add():
    field_a = StructField("a", IntegerType())
    field_b = StructField("b", StringType())
    field_c = StructField("c", LongType())

    expected = StructType([field_a, field_b, field_c])
    struct_type = StructType().add(field_a).add(field_b).add("c", LongType())
    assert struct_type == expected
    with pytest.raises(
        ValueError,
        match="field argument must be one of str, ColumnIdentifier or StructField.",
    ):
        struct_type.add(7)

    with pytest.raises(
        ValueError,
        match="When field argument is str or ColumnIdentifier, datatype must not be None.",
    ):
        struct_type.add("d")


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


@pytest.mark.skipif(not is_pandas_available, reason="Includes testing for pandas types")
def test_python_type_to_snow_type():
    # In python 3.10, the __name__ of nested type only contains the parent type, which breaks our test. And for this
    # reason, we introduced type_str_override to test the expected string.
    def check_type(
        python_type,
        snow_type,
        is_nullable,
        type_str_override=None,
        is_return_type_of_sproc=False,
    ):
        assert python_type_to_snow_type(python_type, is_return_type_of_sproc) == (
            snow_type,
            is_nullable,
        )
        type_str = type_str_override or getattr(
            python_type, "__name__", str(python_type)
        )
        assert python_type_to_snow_type(type_str, is_return_type_of_sproc) == (
            snow_type,
            is_nullable,
        )

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
    check_type(typing.Optional[str], StringType(), True, "Optional[str]")
    check_type(typing.Union[str, None], StringType(), True, "Union[str, None]")
    check_type(typing.List[int], ArrayType(LongType()), False, "List[int]")
    check_type(
        typing.List,
        ArrayType(StringType()),
        False,
    )
    check_type(list, ArrayType(StringType()), False)
    check_type(typing.Tuple[int], ArrayType(LongType()), False, "Tuple[int]")
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
        "Dict[str, int]",
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
    check_type(Geometry, GeometryType(), False)
    check_type(pandas.Series, PandasSeriesType(None), False)
    check_type(pandas.DataFrame, PandasDataFrameType(()), False)
    check_type(PandasSeries, PandasSeriesType(None), False)
    check_type(PandasDataFrame, PandasDataFrameType(()), False)
    check_type(DataFrame, StructType(), False, is_return_type_of_sproc=True)

    # complicated (nested) types
    check_type(
        typing.Optional[typing.Optional[str]],
        StringType(),
        True,
        "Optional[Optional[str]]",
    )
    check_type(
        typing.Optional[typing.List[str]],
        ArrayType(StringType()),
        True,
        "Optional[List[str]]",
    )
    check_type(
        typing.List[typing.List[float]],
        ArrayType(ArrayType(FloatType())),
        False,
        "List[List[float]]",
    )
    check_type(
        typing.List[typing.List[typing.Optional[datetime]]],
        ArrayType(ArrayType(TimestampType())),
        False,
        "List[List[Optional[datetime.datetime]]]",
    )
    check_type(
        typing.Dict[str, typing.List],
        MapType(StringType(), ArrayType(StringType())),
        False,
        "Dict[str, List]",
    )
    check_type(
        PandasSeries[float], PandasSeriesType(FloatType()), False, "PandasSeries[float]"
    )
    check_type(
        PandasSeries[typing.Dict[str, typing.List]],
        PandasSeriesType(MapType(StringType(), ArrayType(StringType()))),
        False,
        "PandasSeries[Dict[str, List]]",
    )
    check_type(
        PandasDataFrame[int, str],
        PandasDataFrameType([LongType(), StringType()]),
        False,
        "PandasDataFrame[int, str]",
    )
    check_type(
        PandasDataFrame[int, PandasSeries[datetime]],
        PandasDataFrameType([LongType(), PandasSeriesType(TimestampType())]),
        False,
        "PandasDataFrame[int, PandasSeries[datetime.datetime]]",
    )

    # unsupported types
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.AnyStr)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.TypeVar)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.Callable)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.IO)
    with pytest.raises(TypeError):
        python_type_to_snow_type(Iterable)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.Generic)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.Set)
    with pytest.raises(TypeError):
        python_type_to_snow_type(set)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.OrderedDict)
    with pytest.raises(TypeError):
        python_type_to_snow_type(defaultdict)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.Union[str, int, None])
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.Union[None, str])
    with pytest.raises(TypeError):
        python_type_to_snow_type(StringType)

    # invalid type str
    with pytest.raises(NameError):
        python_type_to_snow_type("string")


@pytest.mark.parametrize("decimal_word", ["number", "numeric", "decimal"])
def test_decimal_regular_expression(decimal_word):
    assert get_number_precision_scale(f"{decimal_word}") is None
    assert get_number_precision_scale(f" {decimal_word}") is None
    assert get_number_precision_scale(f"{decimal_word} ") is None
    assert get_number_precision_scale(f"{decimal_word}") is None
    assert get_number_precision_scale(f"{decimal_word}(2) ") is None
    assert get_number_precision_scale(f"a{decimal_word}(2,1)") is None
    assert get_number_precision_scale(f"{decimal_word}(2,1) a") is None

    assert get_number_precision_scale(f"{decimal_word}(2,1)") == (2, 1)
    assert get_number_precision_scale(f" {decimal_word}(2,1)") == (2, 1)
    assert get_number_precision_scale(f"{decimal_word}(2,1) ") == (2, 1)
    assert get_number_precision_scale(f"  {decimal_word}  (  2  ,  1  )  ") == (2, 1)


@pytest.mark.parametrize(
    "value_str,datatype,expected_value",
    [
        ("1", IntegerType(), 1),
        ("True", BooleanType(), True),
        ("1.0", FloatType(), 1.0),
        ("decimal.Decimal('3.14')", DecimalType(), decimal.Decimal("3.14")),
        ("decimal.Decimal(1.0)", DecimalType(), decimal.Decimal(1.0)),
        ("one", StringType(), "one"),
        (None, StringType(), None),
        ("None", StringType(), "None"),
        ("POINT(-122.35 37.55)", GeographyType(), "POINT(-122.35 37.55)"),
        ("POINT(-122.35 37.55)", GeometryType(), "POINT(-122.35 37.55)"),
        ('{"key": "val"}', VariantType(), '{"key": "val"}'),
        ("b'one'", BinaryType(), b"one"),
        ("bytearray('one', 'utf-8')", BinaryType(), bytearray("one", "utf-8")),
        ("datetime.date(2024, 4, 1)", DateType(), date(2024, 4, 1)),
        (
            "datetime.time(12, 0, second=20, tzinfo=datetime.timezone.utc)",
            TimeType(),
            time(12, 0, second=20, tzinfo=timezone.utc),
        ),
        (
            "datetime.datetime(2024, 4, 1, 12, 0, 20)",
            TimestampType(),
            datetime(2024, 4, 1, 12, 0, 20),
        ),
        ("['1', '2', '3']", ArrayType(IntegerType()), [1, 2, 3]),
        ("['a', 'b', 'c']", ArrayType(StringType()), ["a", "b", "c"]),
        ("['a', 'b', 'c']", ArrayType(), ["a", "b", "c"]),
        (
            "[\"['1', '2', '3']\", \"['4', '5', '6']\"]",
            ArrayType(ArrayType(IntegerType())),
            [[1, 2, 3], [4, 5, 6]],
        ),
        ("{'1': 'a'}", MapType(), {"1": "a"}),
        ("{'1': 'a'}", MapType(IntegerType(), StringType()), {1: "a"}),
        (
            "{'1': \"['a', 'b']\"}",
            MapType(IntegerType(), ArrayType(StringType())),
            {1: ["a", "b"]},
        ),
    ],
)
def test_python_value_str_to_object(value_str, datatype, expected_value):
    assert python_value_str_to_object(value_str, datatype) == expected_value


@pytest.mark.parametrize(
    "datatype",
    [
        IntegerType(),
        BooleanType(),
        FloatType(),
        DecimalType(),
        BinaryType(),
        DateType(),
        TimeType(),
        TimestampType(),
        ArrayType(),
        MapType(),
        VariantType(),
        GeographyType(),
        GeometryType(),
    ],
)
def test_python_value_str_to_object_for_none(datatype):
    "StringType() is excluded here and tested in test_python_value_str_to_object"
    assert python_value_str_to_object("None", datatype) is None


def test_python_value_str_to_object_negative():
    with pytest.raises(
        TypeError,
        match="Unsupported data type: invalid type, value thanksgiving by python_value_str_to_object()",
    ):
        python_value_str_to_object("thanksgiving", "invalid type")


def test_retrieve_func_type_hints_from_source():
    func_name = "foo"

    source = f"""
def {func_name}() -> None:
    return None
"""
    assert retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "return": "NoneType"
    }

    source = f"""
def {func_name}() -> int:
    return 1
"""
    assert retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "return": "int"
    }

    source = f"""
def {func_name}() -> int:
    return 1

def {func_name}_{func_name}(x: int) -> int:
    return x
"""
    assert retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "return": "int"
    }

    source = f"""
def {func_name}(x: bytes) -> int:
    return 1
"""
    assert retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "x": "bytes",
        "return": "int",
    }

    source = f"""
def {func_name}(x: List[str], y: None) -> Optional[int]:
    return None
"""
    assert retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "x": "List[str]",
        "y": "NoneType",
        "return": "Optional[int]",
    }

    source = f"""
def {func_name}(x: collections.defaultdict, y: Union[datetime.date, time]) -> Optional[typing.Tuple[decimal.Decimal, Variant, List[float]]]:
    return (1, 2)
"""
    assert retrieve_func_type_hints_from_source("", func_name, _source=source) == {
        "x": "collections.defaultdict",
        "y": "Union[datetime.date, time]",
        "return": "Optional[typing.Tuple[decimal.Decimal, Variant, List[float]]]",
    }

    assert retrieve_func_type_hints_from_source(
        test_files.test_udf_py_file, "mod5"
    ) == {"x": "int", "return": "int"}

    # negative case
    with pytest.raises(UnicodeDecodeError):
        retrieve_func_type_hints_from_source(test_files.test_file_avro, "mod5")

    source = f"""
def {func_name}_{func_name}(x: int) -> int:
    return x
"""
    # Func not found in file
    assert retrieve_func_type_hints_from_source("", func_name, _source=source) is None

    # Class not found in file
    assert (
        retrieve_func_type_hints_from_source(
            "", func_name, class_name="FakeClass", _source=source
        )
        is None
    )

    source = f"""
def {func_name}() -> 1:
    return 1
"""
    with pytest.raises(TypeError) as ex_info:
        retrieve_func_type_hints_from_source("", func_name, _source=source)
    assert "invalid type annotation" in str(ex_info)


def test_convert_sf_to_sp_type_basic():
    assert isinstance(convert_sf_to_sp_type("ARRAY", 0, 0, 0, 0), ArrayType)
    assert isinstance(convert_sf_to_sp_type("VARIANT", 0, 0, 0, 0), VariantType)
    assert isinstance(convert_sf_to_sp_type("OBJECT", 0, 0, 0, 0), MapType)
    assert isinstance(convert_sf_to_sp_type("GEOGRAPHY", 0, 0, 0, 0), GeographyType)
    assert isinstance(convert_sf_to_sp_type("GEOMETRY", 0, 0, 0, 0), GeometryType)
    assert isinstance(convert_sf_to_sp_type("BOOLEAN", 0, 0, 0, 0), BooleanType)
    assert isinstance(convert_sf_to_sp_type("BINARY", 0, 0, 0, 0), BinaryType)
    assert isinstance(convert_sf_to_sp_type("TEXT", 0, 0, 0, 0), StringType)
    assert isinstance(convert_sf_to_sp_type("TIME", 0, 0, 0, 0), TimeType)
    assert isinstance(convert_sf_to_sp_type("TIMESTAMP", 0, 0, 0, 0), TimestampType)
    assert isinstance(convert_sf_to_sp_type("TIMESTAMP_LTZ", 0, 0, 0, 0), TimestampType)
    assert isinstance(convert_sf_to_sp_type("TIMESTAMP_TZ", 0, 0, 0, 0), TimestampType)
    assert isinstance(convert_sf_to_sp_type("TIMESTAMP_NTZ", 0, 0, 0, 0), TimestampType)
    assert isinstance(convert_sf_to_sp_type("DATE", 0, 0, 0, 0), DateType)
    assert isinstance(convert_sf_to_sp_type("REAL", 0, 0, 0, 0), DoubleType)

    with pytest.raises(NotImplementedError, match="Unsupported type"):
        convert_sf_to_sp_type("FAKE", 0, 0, 0, 0)


def test_convert_sp_to_sf_type_tz():
    assert convert_sf_to_sp_type("TIMESTAMP", 0, 0, 0, 0) == TimestampType()
    assert convert_sf_to_sp_type("TIMESTAMP_NTZ", 0, 0, 0, 0) == TimestampType(
        timezone=TimestampTimeZone.NTZ
    )
    assert convert_sf_to_sp_type("TIMESTAMP_LTZ", 0, 0, 0, 0) == TimestampType(
        timezone=TimestampTimeZone.LTZ
    )
    assert convert_sf_to_sp_type("TIMESTAMP_TZ", 0, 0, 0, 0) == TimestampType(
        timezone=TimestampTimeZone.TZ
    )


def test_convert_sf_to_sp_type_precision_scale():
    def assert_type_with_precision(type_name):
        sp_type = convert_sf_to_sp_type(
            type_name, DecimalType._MAX_PRECISION + 1, 20, 0, 0
        )
        assert isinstance(sp_type, DecimalType)
        assert sp_type.precision == DecimalType._MAX_PRECISION
        assert sp_type.scale == 21

        sp_type = convert_sf_to_sp_type(
            type_name, DecimalType._MAX_PRECISION - 1, 20, 0, 0
        )
        assert isinstance(sp_type, DecimalType)
        assert sp_type.precision == DecimalType._MAX_PRECISION - 1
        assert sp_type.scale == 20

    assert_type_with_precision("DECIMAL")
    assert_type_with_precision("FIXED")
    assert_type_with_precision("NUMBER")

    snowpark_type = convert_sf_to_sp_type("DECIMAL", 0, 0, 0, 0)
    assert isinstance(snowpark_type, DecimalType)
    assert snowpark_type.precision == 38
    assert snowpark_type.scale == 18


def test_convert_sf_to_sp_type_internal_size():
    snowpark_type = convert_sf_to_sp_type("TEXT", 0, 0, 0, 16777216)
    assert isinstance(snowpark_type, StringType)
    assert snowpark_type.length is None

    snowpark_type = convert_sf_to_sp_type("TEXT", 0, 0, 31, 16777216)
    assert isinstance(snowpark_type, StringType)
    assert snowpark_type.length == 31

    snowpark_type = convert_sf_to_sp_type("TEXT", 0, 0, 16777216, 16777216)
    assert isinstance(snowpark_type, StringType)
    assert snowpark_type.length == 16777216
    assert snowpark_type._is_max_size

    with pytest.raises(
        ValueError, match="Negative value is not a valid input for StringType"
    ):
        snowpark_type = convert_sf_to_sp_type("TEXT", 0, 0, -1, 16777216)


def test_convert_sp_to_sf_type():
    assert convert_sp_to_sf_type(DecimalType(38, 0)) == "NUMBER(38, 0)"
    assert convert_sp_to_sf_type(IntegerType()) == "INT"
    assert convert_sp_to_sf_type(ShortType()) == "SMALLINT"
    assert convert_sp_to_sf_type(ByteType()) == "BYTEINT"
    assert convert_sp_to_sf_type(LongType()) == "BIGINT"
    assert convert_sp_to_sf_type(FloatType()) == "FLOAT"
    assert convert_sp_to_sf_type(DoubleType()) == "DOUBLE"
    assert convert_sp_to_sf_type(StringType()) == "STRING"
    assert convert_sp_to_sf_type(StringType(77)) == "STRING(77)"
    assert convert_sp_to_sf_type(NullType()) == "STRING"
    assert convert_sp_to_sf_type(BooleanType()) == "BOOLEAN"
    assert convert_sp_to_sf_type(DateType()) == "DATE"
    assert convert_sp_to_sf_type(TimeType()) == "TIME"
    assert convert_sp_to_sf_type(TimestampType()) == "TIMESTAMP"
    assert (
        convert_sp_to_sf_type(TimestampType(timezone=TimestampTimeZone.DEFAULT))
        == "TIMESTAMP"
    )
    assert (
        convert_sp_to_sf_type(TimestampType(timezone=TimestampTimeZone.LTZ))
        == "TIMESTAMP_LTZ"
    )
    assert (
        convert_sp_to_sf_type(TimestampType(timezone=TimestampTimeZone.NTZ))
        == "TIMESTAMP_NTZ"
    )
    assert (
        convert_sp_to_sf_type(TimestampType(timezone=TimestampTimeZone.TZ))
        == "TIMESTAMP_TZ"
    )
    assert convert_sp_to_sf_type(BinaryType()) == "BINARY"
    assert convert_sp_to_sf_type(ArrayType()) == "ARRAY"
    assert convert_sp_to_sf_type(MapType()) == "OBJECT"
    assert convert_sp_to_sf_type(StructType()) == "OBJECT"
    assert convert_sp_to_sf_type(VariantType()) == "VARIANT"
    assert convert_sp_to_sf_type(GeographyType()) == "GEOGRAPHY"
    assert convert_sp_to_sf_type(GeometryType()) == "GEOMETRY"
    assert convert_sp_to_sf_type(VectorType(int, 3)) == "VECTOR(int,3)"
    assert convert_sp_to_sf_type(VectorType("int", 5)) == "VECTOR(int,5)"
    assert convert_sp_to_sf_type(VectorType(float, 5)) == "VECTOR(float,5)"
    assert convert_sp_to_sf_type(VectorType("float", 3)) == "VECTOR(float,3)"
    with pytest.raises(TypeError, match="Unsupported data type"):
        convert_sp_to_sf_type(None)


def test_infer_schema_exceptions():
    with pytest.raises(TypeError, match="Can not infer schema for type"):
        infer_schema(IntegerType())

    with pytest.raises(TypeError, match="Unable to infer the type of the field"):
        infer_schema([IntegerType()])


def test_string_type_eq():
    st0 = StringType()
    st1 = StringType(1)
    st2 = StringType(is_max_size=True)

    assert st0 != IntegerType()

    assert st0 != st1
    assert st0 == st2
    assert st1 != st2
    assert st1 == StringType(1)


def test_snow_type_to_dtype_str():
    assert snow_type_to_dtype_str(BinaryType()) == "binary"
    assert snow_type_to_dtype_str(BooleanType()) == "boolean"
    assert snow_type_to_dtype_str(FloatType()) == "float"
    assert snow_type_to_dtype_str(DoubleType()) == "double"
    assert snow_type_to_dtype_str(StringType(35)) == "string(35)"
    assert snow_type_to_dtype_str(DateType()) == "date"
    assert snow_type_to_dtype_str(TimestampType()) == "timestamp"
    assert snow_type_to_dtype_str(TimeType()) == "time"
    assert snow_type_to_dtype_str(GeographyType()) == "geography"
    assert snow_type_to_dtype_str(GeometryType()) == "geometry"
    assert snow_type_to_dtype_str(VariantType()) == "variant"
    assert snow_type_to_dtype_str(VectorType("int", 3)) == "vector<int,3>"
    assert snow_type_to_dtype_str(ByteType()) == "tinyint"
    assert snow_type_to_dtype_str(ShortType()) == "smallint"
    assert snow_type_to_dtype_str(IntegerType()) == "int"
    assert snow_type_to_dtype_str(LongType()) == "bigint"
    assert snow_type_to_dtype_str(DecimalType(20, 5)) == "decimal(20,5)"

    assert snow_type_to_dtype_str(ArrayType(StringType())) == "array<string>"
    assert snow_type_to_dtype_str(ArrayType(StringType(11))) == "array<string(11)>"
    assert (
        snow_type_to_dtype_str(ArrayType(ArrayType(DoubleType())))
        == "array<array<double>>"
    )
    assert (
        snow_type_to_dtype_str(MapType(StringType(67), BooleanType()))
        == "map<string(67),boolean>"
    )
    assert (
        snow_type_to_dtype_str(MapType(StringType(56), ArrayType(VariantType())))
        == "map<string(56),array<variant>>"
    )
    assert (
        snow_type_to_dtype_str(
            StructType(
                [
                    StructField("str", StringType(30)),
                    StructField("array", ArrayType(VariantType())),
                    StructField("map", MapType(StringType(93), BooleanType())),
                    StructField(
                        "struct", StructType([StructField("time", TimeType())])
                    ),
                ]
            )
        )
        == "struct<string(30),array<variant>,map<string(93),boolean>,struct<time>>"
    )

    with pytest.raises(TypeError, match="invalid DataType"):
        snow_type_to_dtype_str(None)
