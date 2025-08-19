#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
    extract_bracket_content,
    extract_nullable_keyword,
    get_number_precision_scale,
    infer_schema,
    infer_type,
    is_likely_struct,
    merge_type,
    most_permissive_type,
    parse_struct_field_list,
    python_type_to_snow_type,
    python_value_str_to_object,
    retrieve_func_defaults_from_source,
    retrieve_func_type_hints_from_source,
    snow_type_to_dtype_str,
    split_top_level_comma_fields,
    type_string_to_type_object,
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
    FileType,
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
    TimeType,
    TimestampTimeZone,
    TimestampType,
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
    assert (
        str(TimestampType(TimestampTimeZone.TZ))
        == "TimestampType(timezone=TimestampTimeZone('tz'))"
    )


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
    assert hash(TimestampType(TimestampTimeZone.TZ)) == hash(
        "TimestampType(timezone=TimestampTimeZone('tz'))"
    )


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


@pytest.mark.parametrize("test_from_class", [True, False])
@pytest.mark.parametrize("test_from_file", [True, False])
@pytest.mark.parametrize("add_type_hint", [True, False])
@pytest.mark.parametrize(
    "datatype,annotated_value,extracted_value",
    [
        ("int", "None", None),
        ("int", "1", "1"),
        ("bool", "True", "True"),
        ("float", "1.0", "1.0"),
        ("decimal.Decimal", "decimal.Decimal('3.14')", "decimal.Decimal('3.14')"),
        ("decimal.Decimal", "decimal.Decimal(1.0)", "decimal.Decimal(1.0)"),
        ("str", "one", "one"),
        ("str", "None", None),
        ("bytes", "b'one'", "b'one'"),
        ("bytearray", "bytearray('one', 'utf-8')", "bytearray('one', 'utf-8')"),
        ("datetime.date", "datetime.date(2024, 4, 1)", "datetime.date(2024, 4, 1)"),
        (
            "datetime.time",
            "datetime.time(12, 0, second=20, tzinfo=datetime.timezone.utc)",
            "datetime.time(12, 0, second=20, tzinfo=datetime.timezone.utc)",
        ),
        (
            "datetime.datetime",
            "datetime.datetime(2024, 4, 1, 12, 0, 20)",
            "datetime.datetime(2024, 4, 1, 12, 0, 20)",
        ),
        ("List[int]", "[1, 2, 3]", "['1', '2', '3']"),
        ("List[str]", "['a', 'b', 'c']", "['a', 'b', 'c']"),
        (
            "List[List[int]]",
            "[[1, 2, 3], [4, 5, 6]]",
            "[\"['1', '2', '3']\", \"['4', '5', '6']\"]",
        ),
        ("Map[int, str]", "{1: 'a'}", "{'1': 'a'}"),
        ("Map[int, List[str]]", "{1: ['a', 'b']}", "{'1': \"['a', 'b']\"}"),
        ("Variant", "{'key': 'val'}", "{'key': 'val'}"),
        ("Geography", "'POINT(-122.35 37.55)'", "POINT(-122.35 37.55)"),
        ("Geometry", "'POINT(-122.35 37.55)'", "POINT(-122.35 37.55)"),
    ],
)
def test_retrieve_func_defaults_from_source(
    datatype,
    annotated_value,
    extracted_value,
    add_type_hint,
    test_from_file,
    test_from_class,
    tmpdir,
):
    func_name = "foo"
    class_name = "Foo"

    if test_from_class:
        source = f"""
class {class_name}:
    def {func_name}() -> None:
        return None
"""
    else:
        source = f"""
def {func_name}(self) -> None:
    return None
"""
    if test_from_file:
        file = tmpdir.join("test_udf.py")
        file.write(source)
        assert retrieve_func_defaults_from_source(file, func_name) == []
    else:
        assert retrieve_func_defaults_from_source("", func_name, _source=source) == []

    datatype_str = f": {datatype}" if add_type_hint else ""
    if test_from_class:
        source = f"""
class {class_name}:
    def {func_name}(self, x, y {datatype_str} = {annotated_value}) -> None:
        return None
"""
    else:
        source = f"""
def {func_name}(x, y {datatype_str} = {annotated_value}) -> None:
    return None
"""
    if test_from_file:
        file = tmpdir.join("test_udf.py")
        file.write(source)
        assert retrieve_func_defaults_from_source(file, func_name) == [extracted_value]
    else:
        assert retrieve_func_defaults_from_source("", func_name, _source=source) == [
            extracted_value
        ]


@pytest.mark.parametrize(
    "value_str,datatype,expected_value",
    [
        (None, None, None),
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
    assert (
        convert_sp_to_sf_type(ArrayType(IntegerType(), structured=True)) == "ARRAY(INT)"
    )
    assert (
        convert_sp_to_sf_type(
            ArrayType(IntegerType(), structured=True, contains_null=False)
        )
        == "ARRAY(INT NOT NULL)"
    )
    assert convert_sp_to_sf_type(MapType()) == "OBJECT"
    assert (
        convert_sp_to_sf_type(MapType(StringType(), StringType(), structured=True))
        == "MAP(STRING, STRING)"
    )
    assert (
        convert_sp_to_sf_type(
            MapType(
                StringType(), StringType(), structured=True, value_contains_null=False
            )
        )
        == "MAP(STRING, STRING NOT NULL)"
    )
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


@pytest.mark.parametrize(
    "tpe, simple_string, json, type_name, json_value",
    [
        (DataType(), "data", '"data"', "data", "data"),
        (BinaryType(), "binary", '"binary"', "binary", "binary"),
        (BooleanType(), "boolean", '"boolean"', "boolean", "boolean"),
        (ByteType(), "tinyint", '"byte"', "byte", "byte"),
        (DateType(), "date", '"date"', "date", "date"),
        (
            DecimalType(20, 10),
            "decimal(20,10)",
            '"decimal(20,10)"',
            "decimal",
            "decimal(20,10)",
        ),
        (DoubleType(), "double", '"double"', "double", "double"),
        (FloatType(), "float", '"float"', "float", "float"),
        (IntegerType(), "int", '"integer"', "integer", "integer"),
        (LongType(), "bigint", '"long"', "long", "long"),
        (ShortType(), "smallint", '"short"', "short", "short"),
        (StringType(), "string", '"string"', "string", "string"),
        (VariantType(), "variant", '"variant"', "variant", "variant"),
        (
            StructType(
                [StructField("a", StringType()), StructField("b", IntegerType())]
            ),
            "struct<A:string,B:int>",
            '{"fields":[{"name":"A","nullable":true,"type":"string"},{"name":"B","nullable":true,"type":"integer"}],"type":"struct"}',
            "struct",
            {
                "type": "struct",
                "fields": [
                    {"name": "A", "type": "string", "nullable": True},
                    {"name": "B", "type": "integer", "nullable": True},
                ],
            },
        ),
        (
            StructField("AA", StringType()),
            "AA:string",
            '{"name":"AA","nullable":true,"type":"string"}',
            "",
            {
                "name": "AA",
                "type": "string",
                "nullable": True,
            },
        ),
        (TimestampType(), "timestamp", '"timestamp"', "timestamp", "timestamp"),
        (
            TimestampType(TimestampTimeZone.TZ),
            "timestamp_tz",
            '"timestamp_tz"',
            "timestamp",
            "timestamp_tz",
        ),
        (
            TimestampType(TimestampTimeZone.LTZ),
            "timestamp_ltz",
            '"timestamp_ltz"',
            "timestamp",
            "timestamp_ltz",
        ),
        (
            TimestampType(TimestampTimeZone.NTZ),
            "timestamp_ntz",
            '"timestamp_ntz"',
            "timestamp",
            "timestamp_ntz",
        ),
        (TimeType(), "time", '"time"', "time", "time"),
        (
            ArrayType(IntegerType()),
            "array<int>",
            '{"contains_null":true,"element_type":"integer","type":"array"}',
            "array",
            {
                "contains_null": True,
                "element_type": "integer",
                "type": "array",
            },
        ),
        (
            ArrayType(ArrayType(IntegerType())),
            "array<array<int>>",
            '{"contains_null":true,"element_type":{"contains_null":true,"element_type":"integer","type":"array"},"type":"array"}',
            "array",
            {
                "contains_null": True,
                "element_type": {
                    "contains_null": True,
                    "element_type": "integer",
                    "type": "array",
                },
                "type": "array",
            },
        ),
        (
            MapType(IntegerType(), StringType()),
            "map<int,string>",
            '{"key_type":"integer","type":"map","value_type":"string"}',
            "map",
            {
                "key_type": "integer",
                "type": "map",
                "value_type": "string",
            },
        ),
        (
            MapType(StringType(), MapType(IntegerType(), StringType())),
            "map<string,map<int,string>>",
            '{"key_type":"string","type":"map","value_type":{"key_type":"integer","type":"map","value_type":"string"}}',
            "map",
            {
                "type": "map",
                "key_type": "string",
                "value_type": {
                    "type": "map",
                    "key_type": "integer",
                    "value_type": "string",
                },
            },
        ),
        (
            StructType(
                [
                    StructField(
                        "nested",
                        StructType(
                            [
                                StructField("A", IntegerType()),
                                StructField("B", StringType()),
                            ]
                        ),
                    )
                ]
            ),
            "struct<NESTED:struct<A:int,B:string>>",
            '{"fields":[{"name":"NESTED","nullable":true,"type":{"fields":[{"name":"A","nullable":true,"type":"integer"},{"name":"B","nullable":true,"type":"string"}],"type":"struct"}}],"type":"struct"}',
            "struct",
            {
                "type": "struct",
                "fields": [
                    {
                        "name": "NESTED",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "A",
                                    "type": "integer",
                                    "nullable": True,
                                },
                                {
                                    "name": "B",
                                    "type": "string",
                                    "nullable": True,
                                },
                            ],
                        },
                        "nullable": True,
                    }
                ],
            },
        ),
        (
            VectorType(int, 8),
            "vector(int,8)",
            '"vector(int,8)"',
            "vector",
            "vector(int,8)",
        ),
        (
            VectorType(float, 8),
            "vector(float,8)",
            '"vector(float,8)"',
            "vector",
            "vector(float,8)",
        ),
        (
            PandasDataFrameType(
                [StringType(), IntegerType(), FloatType()], ["id", "col1", "col2"]
            ),
            "pandas<string,int,float>",
            '{"fields":[{"name":"id","type":"string"},{"name":"col1","type":"integer"},{"name":"col2","type":"float"}],"type":"pandas_dataframe"}',
            "pandas_dataframe",
            {
                "type": "pandas_dataframe",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "col1", "type": "integer"},
                    {"name": "col2", "type": "float"},
                ],
            },
        )
        if is_pandas_available
        else (None, None, None, None, None),
        (
            PandasDataFrameType(
                [ArrayType(ArrayType(IntegerType())), IntegerType(), FloatType()]
            ),
            "pandas<array<array<int>>,int,float>",
            '{"fields":[{"name":"","type":{"contains_null":true,"element_type":{"contains_null":true,"element_type":"integer","type":"array"},"type":"array"}},{"name":"","type":"integer"},{"name":"","type":"float"}],"type":"pandas_dataframe"}',
            "pandas_dataframe",
            {
                "type": "pandas_dataframe",
                "fields": [
                    {
                        "name": "",
                        "type": {
                            "contains_null": True,
                            "type": "array",
                            "element_type": {
                                "contains_null": True,
                                "type": "array",
                                "element_type": "integer",
                            },
                        },
                    },
                    {"name": "", "type": "integer"},
                    {"name": "", "type": "float"},
                ],
            },
        )
        if is_pandas_available
        else (None, None, None, None, None),
        (
            PandasSeriesType(IntegerType()),
            "pandas_series<int>",
            '{"element_type":"integer","type":"pandas_series"}',
            "pandas_series",
            {"type": "pandas_series", "element_type": "integer"},
        )
        if is_pandas_available
        else (None, None, None, None, None),
        (
            PandasSeriesType(None),
            "pandas_series<>",
            '{"element_type":null,"type":"pandas_series"}',
            "pandas_series",
            {"type": "pandas_series", "element_type": None},
        )
        if is_pandas_available
        else (None, None, None, None, None),
    ],
)
def test_datatype(tpe, simple_string, json, type_name, json_value):
    if tpe is None:
        pytest.skip("skip because pandas is not available")
    assert tpe.simple_string() == simple_string
    assert tpe.json_value() == json_value
    assert tpe.json() == json
    if isinstance(tpe, StructField):
        with pytest.raises(
            TypeError,
            match="StructField does not have typeName. Use typeName on its type explicitly instead",
        ):
            tpe.type_name()
    else:
        assert tpe.type_name() == type_name

    # test alias
    assert tpe.simpleString() == simple_string
    assert tpe.jsonValue() == json_value
    if isinstance(tpe, StructField):
        with pytest.raises(
            TypeError,
            match="StructField does not have typeName. Use typeName on its type explicitly instead",
        ):
            tpe.typeName()
    else:
        assert tpe.typeName() == type_name


@pytest.mark.parametrize(
    "datatype, tpe",
    [
        (
            MapType,
            MapType(IntegerType(), StringType()),
        ),
        (
            MapType,
            MapType(StringType(), MapType(IntegerType(), StringType())),
        ),
        (
            ArrayType,
            ArrayType(IntegerType()),
        ),
        (
            ArrayType,
            ArrayType(ArrayType(IntegerType())),
        ),
        (
            StructType,
            StructType(
                [
                    StructField(
                        "nested",
                        StructType(
                            [
                                StructField("A", IntegerType()),
                                StructField("B", StringType()),
                            ]
                        ),
                    )
                ]
            ),
        ),
        (StructType, StructType([StructField("variant", VariantType())])),
        (
            StructField,
            StructField("AA", StringType()),
        ),
        (
            StructType,
            StructType(
                [StructField("a", StringType()), StructField("b", IntegerType())]
            ),
        ),
        (
            StructField,
            StructField("AA", DecimalType()),
        ),
        (
            StructField,
            StructField("AA", DecimalType(20, 10)),
        ),
        (
            StructField,
            StructField("AA", VectorType(int, 1)),
        ),
        (
            StructField,
            StructField("AA", VectorType(float, 8)),
        ),
        (
            PandasDataFrameType,
            PandasDataFrameType(
                [StringType(), IntegerType(), FloatType()], ["id", "col1", "col2"]
            ),
        )
        if is_pandas_available
        else (None, None),
        (
            PandasDataFrameType,
            PandasDataFrameType(
                [ArrayType(ArrayType(IntegerType())), IntegerType(), FloatType()]
            ),
        )
        if is_pandas_available
        else (None, None),
        (PandasSeriesType, PandasSeriesType(IntegerType()))
        if is_pandas_available
        else (None, None),
        (PandasSeriesType, PandasSeriesType(None))
        if is_pandas_available
        else (None, None),
    ],
)
def test_structtype_from_json(datatype, tpe):
    if datatype is None:
        pytest.skip("skip because pandas is not available")
    json_dict = tpe.json_value()
    new_obj = datatype.from_json(json_dict)
    assert new_obj == tpe


def test_from_json_wrong_data_type():
    wrong_json = {
        "name": "AA",
        "type": "wrong_type",
        "nullable": True,
    }
    with pytest.raises(ValueError, match="Cannot parse data type: wrong_type"):
        StructField.from_json(wrong_json)

    wrong_json = {
        "name": "AA",
        "type": {
            "type": "wrong_type",
            "key_type": "integer",
            "value_type": "string",
        },
        "nullable": True,
    }
    with pytest.raises(ValueError, match="Unsupported data type: wrong_type"):
        StructField.from_json(wrong_json)


def test_timestamp_json_round_trip():
    timestamp_types = [
        "timestamp",
        "timestamp_tz",
        "timestamp_ntz",
        "timestamp_ltz",
    ]

    for ts in timestamp_types:
        assert (
            StructField.from_json(
                {"name": "TS", "type": ts, "nullable": True}
            ).json_value()["type"]
            == ts
        )


def test_maptype_alias():
    expected_key = StringType()
    expected_value = IntegerType()
    tpe = MapType(expected_key, expected_value)
    assert tpe.valueType == expected_value
    assert tpe.keyType == expected_key

    assert tpe.valueType == tpe.value_type
    assert tpe.keyType == tpe.key_type


def test_type_string_to_type_object_basic_int():
    dt = type_string_to_type_object("int")
    assert isinstance(dt, IntegerType), f"Expected IntegerType, got {dt}"


def test_type_string_to_type_object_smallint():
    dt = type_string_to_type_object("smallint")
    assert isinstance(dt, ShortType), f"Expected ShortType, got {dt}"


def test_type_string_to_type_object_byteint():
    dt = type_string_to_type_object("byteint")
    assert isinstance(dt, ByteType), f"Expected ByteType, got {dt}"


def test_type_string_to_type_object_bigint():
    dt = type_string_to_type_object("bigint")
    assert isinstance(dt, LongType), f"Expected LongType, got {dt}"


def test_type_string_to_type_object_number_decimal():
    # For number(precision, scale) => DecimalType
    dt = type_string_to_type_object("number(10,2)")
    assert isinstance(dt, DecimalType), f"Expected DecimalType, got {dt}"
    assert dt.precision == 10, f"Expected precision=10, got {dt.precision}"
    assert dt.scale == 2, f"Expected scale=2, got {dt.scale}"
    dt = type_string_to_type_object("decimal")
    assert isinstance(dt, DecimalType), f"Expected DecimalType, got {dt}"
    assert dt.precision == 38, f"Expected precision=38, got {dt.precision}"
    assert dt.scale == 0, f"Expected scale=0, got {dt.scale}"


def test_type_string_to_type_object_numeric_decimal():
    dt = type_string_to_type_object("numeric(20, 5)")
    assert isinstance(dt, DecimalType), f"Expected DecimalType, got {dt}"
    assert dt.precision == 20, f"Expected precision=20, got {dt.precision}"
    assert dt.scale == 5, f"Expected scale=5, got {dt.scale}"


def test_type_string_to_type_object_decimal_spaces():
    # Check spaces inside parentheses
    dt = type_string_to_type_object("  decimal  (  2  ,  1  )  ")
    assert isinstance(dt, DecimalType), f"Expected DecimalType, got {dt}"
    assert dt.precision == 2, f"Expected precision=2, got {dt.precision}"
    assert dt.scale == 1, f"Expected scale=1, got {dt.scale}"


def test_type_string_to_type_object_string_with_length():
    dt = type_string_to_type_object("string(50)")
    assert isinstance(dt, StringType), f"Expected StringType, got {dt}"
    # Snowpark's StringType typically doesn't store length internally,
    # but here, you're returning StringType(50) in your code, so let's check
    if hasattr(dt, "length"):
        assert dt.length == 50, f"Expected length=50, got {dt.length}"


def test_type_string_to_type_object_text_with_length():
    dt = type_string_to_type_object("text(100)")
    assert isinstance(dt, StringType), f"Expected StringType, got {dt}"
    if hasattr(dt, "length"):
        assert dt.length == 100, f"Expected length=100, got {dt.length}"


def test_type_string_to_type_object_timestamp():
    dt = type_string_to_type_object("timestamp")
    assert isinstance(dt, TimestampType)
    assert dt.tz == TimestampTimeZone.DEFAULT
    dt = type_string_to_type_object("timestamp_ntz")
    assert isinstance(dt, TimestampType)
    assert dt.tz == TimestampTimeZone.NTZ
    dt = type_string_to_type_object("timestamp_tz")
    assert isinstance(dt, TimestampType)
    assert dt.tz == TimestampTimeZone.TZ
    dt = type_string_to_type_object("timestamp_ltz")
    assert isinstance(dt, TimestampType)
    assert dt.tz == TimestampTimeZone.LTZ


def test_type_string_to_type_object_array_of_int():
    dt = type_string_to_type_object("array<int>")
    assert isinstance(dt, ArrayType), f"Expected ArrayType, got {dt}"
    assert isinstance(
        dt.element_type, IntegerType
    ), f"Expected element_type=IntegerType, got {dt.element_type}"


def test_type_string_to_type_object_array_of_decimal():
    dt = type_string_to_type_object("array<decimal(10,2)>")
    assert isinstance(dt, ArrayType), f"Expected ArrayType, got {dt}"
    assert isinstance(
        dt.element_type, DecimalType
    ), f"Expected element_type=DecimalType, got {dt.element_type}"
    assert dt.element_type.precision == 10
    assert dt.element_type.scale == 2

    try:
        type_string_to_type_object("array<decimal(10,2>")
        raise AssertionError("Expected ValueError for not a supported type")
    except ValueError as ex:
        assert "is not a supported type" in str(
            ex
        ), f"Expected not a supported type, got: {ex}"


def test_type_string_to_type_object_map_of_int_string():
    dt = type_string_to_type_object("map<int, string>")
    assert isinstance(dt, MapType), f"Expected MapType, got {dt}"
    assert isinstance(
        dt.key_type, IntegerType
    ), f"Expected key_type=IntegerType, got {dt.key_type}"
    assert isinstance(
        dt.value_type, StringType
    ), f"Expected value_type=StringType, got {dt.value_type}"


def test_type_string_to_type_object_map_of_array_decimal():
    dt = type_string_to_type_object("map< array<int>, decimal(12,5)>")
    assert isinstance(dt, MapType), f"Expected MapType, got {dt}"
    assert isinstance(
        dt.key_type, ArrayType
    ), f"Expected key_type=ArrayType, got {dt.key_type}"
    assert isinstance(
        dt.key_type.element_type, IntegerType
    ), f"Expected key_type.element_type=IntegerType, got {dt.key_type.element_type}"
    assert isinstance(
        dt.value_type, DecimalType
    ), f"Expected value_type=DecimalType, got {dt.value_type}"
    assert dt.value_type.precision == 12
    assert dt.value_type.scale == 5


def test_type_string_to_type_object_explicit_struct_simple():
    dt = type_string_to_type_object("struct<a: int, b: string>")
    assert isinstance(dt, StructType), f"Expected StructType, got {dt}"
    assert len(dt.fields) == 2, f"Expected 2 fields, got {len(dt.fields)}"

    # Now assert exact StructField matches
    expected_field_a = StructField("a", IntegerType(), nullable=True)
    expected_field_b = StructField("b", StringType(), nullable=True)
    assert (
        dt.fields[0] == expected_field_a
    ), f"Expected {expected_field_a}, got {dt.fields[0]}"
    assert (
        dt.fields[1] == expected_field_b
    ), f"Expected {expected_field_b}, got {dt.fields[1]}"


def test_type_string_to_type_object_explicit_struct_nested():
    dt = type_string_to_type_object(
        "struct<x: array<int>, y: map<string, decimal(5,2)>>"
    )
    assert isinstance(dt, StructType), f"Expected StructType, got {dt}"
    assert len(dt.fields) == 2, f"Expected 2 fields, got {len(dt.fields)}"

    # Check each field directly against StructField(...)
    expected_field_x = StructField("x", ArrayType(IntegerType()), nullable=True)
    expected_field_y = StructField(
        "y", MapType(StringType(), DecimalType(5, 2)), nullable=True
    )

    assert (
        dt.fields[0] == expected_field_x
    ), f"Expected {expected_field_x}, got {dt.fields[0]}"
    assert (
        dt.fields[1] == expected_field_y
    ), f"Expected {expected_field_y}, got {dt.fields[1]}"


def test_type_string_to_type_object_unknown_type():
    try:
        type_string_to_type_object("unknown_type")
        raise AssertionError("Expected ValueError for unknown type")
    except ValueError as ex:
        assert "unknown_type" in str(
            ex
        ), f"Error message doesn't mention 'unknown_type': {ex}"


def test_type_string_to_type_object_mismatched_bracket_array():
    try:
        type_string_to_type_object("array<int")
        raise AssertionError("Expected ValueError for mismatched bracket")
    except ValueError as ex:
        assert "Missing closing" in str(ex) or "Mismatched" in str(
            ex
        ), f"Expected bracket mismatch error, got: {ex}"


def test_type_string_to_type_object_mismatched_bracket_map():
    try:
        print(type_string_to_type_object("map<int, string>>"))
        raise AssertionError("Expected ValueError for mismatched bracket")
    except ValueError as ex:
        assert "Unexpected characters after closing '>' in" in str(
            ex
        ), f"Expected Unexpected characters after closing '>' error, got: {ex}"


def test_type_string_to_type_object_bad_decimal():
    try:
        type_string_to_type_object("decimal(10,2,5)")
        raise AssertionError("Expected ValueError for a malformed decimal argument")
    except ValueError:
        # "decimal(10,2,5)" doesn't match the DECIMAL_RE regex => unknown type => ValueError
        pass


def test_type_string_to_type_object_bad_struct_syntax():
    try:
        type_string_to_type_object("struct<x int, y: string")
        raise AssertionError(
            "Expected ValueError for mismatched bracket or parse error"
        )
    except ValueError as ex:
        assert (
            "Missing closing" in str(ex)
            or "Mismatched" in str(ex)
            or "syntax" in str(ex).lower()
        ), f"Expected bracket or parse syntax error, got: {ex}"


def test_type_string_to_type_object_implicit_struct_simple():
    """
    Verify that a comma-separated list of 'name: type' fields parses as a StructType,
    even without 'struct<...>'.
    """
    dt = type_string_to_type_object("a: int, b: string")
    assert isinstance(dt, StructType), f"Expected StructType, got {dt}"
    assert len(dt.fields) == 2, f"Expected 2 fields, got {len(dt.fields)}"

    expected_field_a = StructField("a", IntegerType(), nullable=True)
    expected_field_b = StructField("b", StringType(), nullable=True)

    assert (
        dt.fields[0] == expected_field_a
    ), f"Expected {expected_field_a}, got {dt.fields[0]}"
    assert (
        dt.fields[1] == expected_field_b
    ), f"Expected {expected_field_b}, got {dt.fields[1]}"


def test_type_string_to_type_object_implicit_struct_single_field():
    """
    Even a single 'name: type' with no commas should parse to StructType
    if your parser logic treats it as an implicit struct.
    """
    dt = type_string_to_type_object("c: decimal(10,2)")
    assert isinstance(dt, StructType), f"Expected StructType, got {dt}"
    assert len(dt.fields) == 1, f"Expected 1 field, got {len(dt.fields)}"

    expected_field_c = StructField("c", DecimalType(10, 2), nullable=True)
    assert (
        dt.fields[0] == expected_field_c
    ), f"Expected {expected_field_c}, got {dt.fields[0]}"


def test_type_string_to_type_object_implicit_struct_nested():
    """
    Test an implicit struct with multiple fields,
    including nested array/map types.
    """
    dt = type_string_to_type_object("arr: array<int>, kv: map<string, decimal(5,2)>")
    assert isinstance(dt, StructType), f"Expected StructType, got {dt}"
    assert len(dt.fields) == 2, f"Expected 2 fields, got {len(dt.fields)}"

    expected_field_arr = StructField("arr", ArrayType(IntegerType()), nullable=True)
    expected_field_kv = StructField(
        "kv", MapType(StringType(), DecimalType(5, 2)), nullable=True
    )

    assert (
        dt.fields[0] == expected_field_arr
    ), f"Expected {expected_field_arr}, got {dt.fields[0]}"
    assert (
        dt.fields[1] == expected_field_kv
    ), f"Expected {expected_field_kv}, got {dt.fields[1]}"


def test_type_string_to_type_object_implicit_struct_with_spaces():
    """
    Test spacing variations. E.g. "  col1  :   int  ,  col2  :  map< string , decimal(5,2) >  ".
    """
    dt = type_string_to_type_object(
        "  col1 :  int  ,  col2 :   map< string , decimal( 5 , 2 ) > "
    )
    assert isinstance(dt, StructType), f"Expected StructType, got {dt}"
    assert len(dt.fields) == 2, f"Expected 2 fields, got {len(dt.fields)}"

    expected_field_col1 = StructField("col1", IntegerType(), nullable=True)
    expected_field_col2 = StructField(
        "col2", MapType(StringType(), DecimalType(5, 2)), nullable=True
    )

    assert (
        dt.fields[0] == expected_field_col1
    ), f"Expected {expected_field_col1}, got {dt.fields[0]}"
    assert (
        dt.fields[1] == expected_field_col2
    ), f"Expected {expected_field_col2}, got {dt.fields[1]}"


def test_type_string_to_type_object_implicit_struct_inner_colon():
    dt = type_string_to_type_object("struct struct<i: integer not null>")
    assert isinstance(dt, StructType), f"Expected StructType, got {dt}"
    assert len(dt.fields) == 1, f"Expected 1 field, got {len(dt.fields)}"
    expected_field_i = StructField(
        "STRUCT",
        StructType([StructField("I", IntegerType(), nullable=False)]),
        nullable=True,
    )
    assert (
        dt.fields[0] == expected_field_i
    ), f"Expected {expected_field_i}, got {dt.fields[0]}"


def test_type_string_to_type_object_implicit_struct_error():
    """
    Check a malformed implicit struct that should raise ValueError
    (e.g. trailing comma or missing bracket for nested).
    """
    try:
        type_string_to_type_object("a: int, b:")
        raise AssertionError("Expected ValueError for malformed struct (b: )")
    except ValueError as ex:
        # We expect an error message about Empty type string
        assert "Empty type string" in str(
            ex
        ), f"Expected error 'Empty type string', got: {ex}"

    try:
        type_string_to_type_object("arr: array<int, b: string")
        raise AssertionError("Expected ValueError for mismatched bracket")
    except ValueError as ex:
        # We expect an error about bracket mismatch or missing '>'
        assert "Missing closing" in str(
            ex
        ), f"Expected Missing closing error, got: {ex}"


def test_extract_bracket_content_array_ok():
    s = "array<int>"
    # We expect to extract "int" from inside <...>
    content = extract_bracket_content(s, keyword="array")
    assert content == "int", f"Expected 'int', got {content}"


def test_extract_bracket_content_map_spaces():
    s = " map< int , string >"
    content = extract_bracket_content(s, keyword="map")
    assert content == "int , string", f"Expected 'int , string', got {content}"


def test_extract_bracket_content_missing_closing():
    s = "array<int"
    try:
        extract_bracket_content(s, keyword="array")
        raise AssertionError("Expected ValueError for missing '>'")
    except ValueError as ex:
        assert (
            "Missing closing" in str(ex) or "mismatched" in str(ex).lower()
        ), f"Error does not mention missing bracket: {ex}"


def test_extract_bracket_content_mismatched_extra_close():
    s = "struct<a: int>>"
    try:
        extract_bracket_content(s, keyword="struct")
        raise AssertionError("Expected ValueError for extra '>'")
    except ValueError as ex:
        assert "Unexpected characters after closing '>' in" in str(
            ex
        ), f"Error does not mention Unexpected characters after closing '>' in: {ex}"


def test_split_top_level_comma_fields_no_brackets():
    s = "int, string, decimal(10,2)"
    parts = split_top_level_comma_fields(s)
    assert parts == ["int", "string", "decimal(10,2)"], f"Got unexpected parts: {parts}"


def test_split_top_level_comma_fields_nested_brackets():
    s = "int, array<long>, decimal(10,2), map<int, array<string>>"
    parts = split_top_level_comma_fields(s)
    assert parts == [
        "int",
        "array<long>",
        "decimal(10,2)",
        "map<int, array<string>>",
    ], f"Got unexpected parts: {parts}"


def test_parse_struct_field_list_simple():
    s = "a: int, b: string"
    struct_type = parse_struct_field_list(s)
    assert (
        len(struct_type.fields) == 2
    ), f"Expected 2 fields, got {len(struct_type.fields)}"
    # Direct equality checks on each StructField
    from snowflake.snowpark.types import StructField, IntegerType, StringType

    assert struct_type.fields[0] == StructField("a", IntegerType(), nullable=True)
    assert struct_type.fields[1] == StructField("b", StringType(), nullable=True)


def test_parse_struct_field_list_malformed():
    s = "col1: int, col2"
    try:
        parse_struct_field_list(s)
        raise AssertionError("Expected ValueError for missing type in 'col2'")
    except ValueError as ex:
        assert (
            "Cannot parse struct field definition" in str(ex)
            or "missing" in str(ex).lower()
        ), f"Unexpected error message: {ex}"


def test_parse_struct_field_list_single_type_with_space():
    s = "decimal(1, 2)"
    assert parse_struct_field_list(s) is None, "Expected None for single type"


def test_is_likely_struct_colon():
    """
    Strings with a top-level colon (outside any <...> or (...))
    should return True.
    """
    s = "col: int"
    assert is_likely_struct(s) is True


def test_is_likely_struct_comma():
    """
    Strings with a top-level comma (outside brackets)
    should return True (e.g. multiple fields).
    """
    s = "col1: int, col2: string"
    assert is_likely_struct(s) is True


def test_is_likely_struct_top_level_space():
    """
    Strings with a top-level space (and no colon/comma)
    should return True (single field, e.g. 'arr array<int>').
    """
    s = "arr array<int>"
    assert is_likely_struct(s) is True


def test_is_likely_struct_no_space_colon_comma():
    """
    If there's no top-level space, colon, or comma,
    we return False (likely a single-type definition like 'decimal(10,2)').
    """
    s = "decimal(10,2)"
    assert is_likely_struct(s) is False


def test_is_likely_struct_space_inside_brackets():
    """
    Spaces inside <...> should not trigger struct mode.
    E.g. 'array< int >' has spaces inside brackets,
    but no top-level space => should return False.
    """
    s = "array< int >"
    assert is_likely_struct(s) is False


def test_is_likely_struct_comma_inside_brackets():
    """
    Comma inside <...> is not top-level,
    so it shouldn't make the string 'likely struct'.
    Example: 'array<int, string>' is not a struct definition,
    it's a single type definition for an array of multiple types
    (though typically invalid in Snowpark, but let's test bracket logic).
    """
    s = "array<int, string>"
    assert is_likely_struct(s) is False


def test_is_likely_struct_colon_inside_brackets():
    """
    If a colon is inside brackets, e.g. 'map<int, struct<x: int>>',
    that colon is not top-level => should return False.
    """
    s = "map<int, struct<x: int>>"
    assert is_likely_struct(s) is False


def test_is_likely_struct_complex_no_top_level_space():
    """
    Example: 'struct<x int, y int>' => top-level
    colon/space are inside <...> => so top-level
    has none => returns False.
    But note if you want 'struct<x: int, y: int>',
    you might parse it differently. This test ensures
    bracket-depth logic works properly.
    """
    s = "struct<x int, y int>"
    # top-level bracket depth covers entire string
    # => no top-level space/colon/comma
    assert is_likely_struct(s) is False


def test_extract_nullable_keyword_no_not_null():
    """
    Verifies that if there's no NOT NULL keyword, the function
    returns the original string and nullable=True.
    """
    base_str, is_nullable = extract_nullable_keyword("integer")
    assert base_str == "integer"
    assert is_nullable is True


def test_extract_nullable_keyword_case_insensitive():
    """
    Verifies that NOT NULL is matched regardless of case,
    and the returned base_str excludes that portion.
    """
    base_str, is_nullable = extract_nullable_keyword("INT NOT NULL")
    assert base_str == "INT"
    assert is_nullable is False


def test_extract_nullable_keyword_weird_spacing():
    """
    Verifies that arbitrary spacing in 'not   null' is handled,
    returning the correct base_str and nullable=False.
    """
    base_str, is_nullable = extract_nullable_keyword("decimal(10,2)  not    null")
    assert base_str == "decimal(10,2)"
    assert is_nullable is False


def test_extract_nullable_keyword_random_case():
    """
    Verifies that random case usage like 'NoT nUlL' is detected,
    returning nullable=False.
    """
    base_str, is_nullable = extract_nullable_keyword("decimal(10,2) NoT nUlL")
    assert base_str == "decimal(10,2)"
    assert is_nullable is False


def test_extract_nullable_keyword_with_leading_trailing_spaces():
    """
    Verifies leading/trailing whitespace is stripped properly,
    and the base_str excludes 'not null'.
    """
    base_str, is_nullable = extract_nullable_keyword("  decimal(10,2) not null   ")
    assert base_str == "decimal(10,2)"
    assert is_nullable is False


def test_extract_nullable_keyword_mix_of_no_keywords():
    """
    If there's a keyword 'null' alone (no 'not'),
    it's not recognized by this pattern, so we treat it as normal text.
    """
    base_str, is_nullable = extract_nullable_keyword("mytype null")
    # This doesn't match 'NOT NULL', so it returns original string with is_nullable=True
    assert base_str == "mytype null"
    assert is_nullable is True


def test_most_permissive_type():
    basic = [
        NullType(),
        BinaryType(),
        BooleanType(),
        DateType(),
        TimestampType(),
        TimeType(),
        FileType(),
        VariantType(),
        GeographyType(),
        GeometryType(),
    ]
    for basic_type in basic:
        assert most_permissive_type(basic_type) == basic_type
        array_type = ArrayType(basic_type)
        assert most_permissive_type(array_type) == array_type
        struct_type = StructType([StructField("A", basic_type)])
        assert most_permissive_type(struct_type) == struct_type
        map_type = MapType(StringType(), basic_type)
        assert most_permissive_type(map_type) == map_type

    numerics = [
        FloatType(),
        DoubleType(),
        DecimalType(),
        ByteType(),
        ShortType(),
        IntegerType(),
        LongType(),
    ]
    for numeric_type in numerics:
        assert most_permissive_type(numeric_type) == DoubleType()
        assert most_permissive_type(ArrayType(numeric_type)) == ArrayType(DoubleType())
        assert most_permissive_type(
            StructType([StructField("A", numeric_type)])
        ) == StructType([StructField("A", DoubleType())])
        assert most_permissive_type(MapType(StringType(), numeric_type)) == MapType(
            StringType(), DoubleType()
        )

    assert most_permissive_type(StringType(5)) == StringType()
    assert most_permissive_type(VectorType("int", 2)) == VectorType("int", 2)
