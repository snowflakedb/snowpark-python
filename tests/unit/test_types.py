#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import os
import typing
from array import array
from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal

import pandas
import pytest

from snowflake.snowpark._internal.type_utils import (
    convert_sf_to_sp_type,
    convert_sp_to_sf_type,
    get_number_precision_scale,
    infer_schema,
    infer_type,
    python_type_to_snow_type,
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
    StructType,
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


def test_struct_field_name():
    column_identifier = ColumnIdentifier("identifier")
    assert StructField(column_identifier, IntegerType(), False).name == "identifier"
    assert (
        str(StructField(column_identifier, IntegerType(), False))
        == "StructField('identifier', IntegerType(), nullable=False)"
    )

    # check that we cover __eq__ works with types other than str and ColumnIdentifier
    assert (column_identifier == 7) is False

    # check StructField name setter works
    sf = StructField(column_identifier, IntegerType(), False)
    sf.name = "integer type"
    assert sf.column_identifier.name == "integer type"


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
        assert python_type_to_snow_type(python_type) == (snow_type, is_nullable)
        assert python_type_to_snow_type(
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
        python_type_to_snow_type(typing.AnyStr)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.TypeVar)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.Callable)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.IO)
    with pytest.raises(TypeError):
        python_type_to_snow_type(typing.Iterable)
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
    with pytest.raises(ValueError, match="function.*is not found in file"):
        retrieve_func_type_hints_from_source("", func_name, _source=source)

    with pytest.raises(ValueError, match="class.*is not found in file"):
        retrieve_func_type_hints_from_source(
            "", func_name, class_name="FakeClass", _source=source
        )

    source = f"""
def {func_name}() -> 1:
    return 1
"""
    with pytest.raises(TypeError) as ex_info:
        retrieve_func_type_hints_from_source("", func_name, _source=source)
    assert "invalid type annotation" in str(ex_info)


def test_convert_sf_to_sp_type_basic():
    assert isinstance(convert_sf_to_sp_type("ARRAY", 0, 0), ArrayType)
    assert isinstance(convert_sf_to_sp_type("VARIANT", 0, 0), VariantType)
    assert isinstance(convert_sf_to_sp_type("OBJECT", 0, 0), MapType)
    assert isinstance(convert_sf_to_sp_type("GEOGRAPHY", 0, 0), GeographyType)
    assert isinstance(convert_sf_to_sp_type("BOOLEAN", 0, 0), BooleanType)
    assert isinstance(convert_sf_to_sp_type("BINARY", 0, 0), BinaryType)
    assert isinstance(convert_sf_to_sp_type("TEXT", 0, 0), StringType)
    assert isinstance(convert_sf_to_sp_type("TIME", 0, 0), TimeType)
    assert isinstance(convert_sf_to_sp_type("TIMESTAMP", 0, 0), TimestampType)
    assert isinstance(convert_sf_to_sp_type("TIMESTAMP_LTZ", 0, 0), TimestampType)
    assert isinstance(convert_sf_to_sp_type("TIMESTAMP_TZ", 0, 0), TimestampType)
    assert isinstance(convert_sf_to_sp_type("TIMESTAMP_NTZ", 0, 0), TimestampType)
    assert isinstance(convert_sf_to_sp_type("DATE", 0, 0), DateType)
    assert isinstance(convert_sf_to_sp_type("REAL", 0, 0), DoubleType)

    with pytest.raises(NotImplementedError, match="Unsupported type"):
        convert_sf_to_sp_type("FAKE", 0, 0)


def test_convert_sf_to_sp_type_precision_scale():
    def assert_type_with_precision(type_name):
        sp_type = convert_sf_to_sp_type(type_name, DecimalType._MAX_PRECISION + 1, 20)
        assert isinstance(sp_type, DecimalType)
        assert sp_type.precision == DecimalType._MAX_PRECISION
        assert sp_type.scale == 21

        sp_type = convert_sf_to_sp_type(type_name, DecimalType._MAX_PRECISION - 1, 20)
        assert isinstance(sp_type, DecimalType)
        assert sp_type.precision == DecimalType._MAX_PRECISION - 1
        assert sp_type.scale == 20

    assert_type_with_precision("DECIMAL")
    assert_type_with_precision("FIXED")
    assert_type_with_precision("NUMBER")

    snowpark_type = convert_sf_to_sp_type("DECIMAL", 0, 0)
    assert isinstance(snowpark_type, DecimalType)
    assert snowpark_type.precision == 38
    assert snowpark_type.scale == 18


def test_convert_sp_to_sf_type():
    assert convert_sp_to_sf_type(DecimalType(38, 0)) == "NUMBER(38, 0)"
    assert convert_sp_to_sf_type(IntegerType()) == "INT"
    assert convert_sp_to_sf_type(ShortType()) == "SMALLINT"
    assert convert_sp_to_sf_type(ByteType()) == "BYTEINT"
    assert convert_sp_to_sf_type(LongType()) == "BIGINT"
    assert convert_sp_to_sf_type(FloatType()) == "FLOAT"
    assert convert_sp_to_sf_type(DoubleType()) == "DOUBLE"
    assert convert_sp_to_sf_type(StringType()) == "STRING"
    assert convert_sp_to_sf_type(NullType()) == "STRING"
    assert convert_sp_to_sf_type(BooleanType()) == "BOOLEAN"
    assert convert_sp_to_sf_type(DateType()) == "DATE"
    assert convert_sp_to_sf_type(TimeType()) == "TIME"
    assert convert_sp_to_sf_type(TimestampType()) == "TIMESTAMP"
    assert convert_sp_to_sf_type(BinaryType()) == "BINARY"
    assert convert_sp_to_sf_type(ArrayType()) == "ARRAY"
    assert convert_sp_to_sf_type(MapType()) == "OBJECT"
    assert convert_sp_to_sf_type(VariantType()) == "VARIANT"
    assert convert_sp_to_sf_type(GeographyType()) == "GEOGRAPHY"
    with pytest.raises(TypeError, match="Unsupported data type"):
        convert_sp_to_sf_type(None)


def test_infer_schema_exceptions():
    with pytest.raises(TypeError, match="Can not infer schema for type"):
        infer_schema(IntegerType())

    with pytest.raises(TypeError, match="Unable to infer the type of the field"):
        infer_schema([IntegerType()])


def test_snow_type_to_dtype_str():
    assert snow_type_to_dtype_str(BinaryType()) == "binary"
    assert snow_type_to_dtype_str(BooleanType()) == "boolean"
    assert snow_type_to_dtype_str(FloatType()) == "float"
    assert snow_type_to_dtype_str(DoubleType()) == "double"
    assert snow_type_to_dtype_str(StringType()) == "string"
    assert snow_type_to_dtype_str(DateType()) == "date"
    assert snow_type_to_dtype_str(TimestampType()) == "timestamp"
    assert snow_type_to_dtype_str(TimeType()) == "time"
    assert snow_type_to_dtype_str(GeographyType()) == "geography"
    assert snow_type_to_dtype_str(VariantType()) == "variant"
    assert snow_type_to_dtype_str(ByteType()) == "tinyint"
    assert snow_type_to_dtype_str(ShortType()) == "smallint"
    assert snow_type_to_dtype_str(IntegerType()) == "int"
    assert snow_type_to_dtype_str(LongType()) == "bigint"
    assert snow_type_to_dtype_str(DecimalType(20, 5)) == "decimal(20,5)"

    assert snow_type_to_dtype_str(ArrayType(StringType())) == "array<string>"
    assert (
        snow_type_to_dtype_str(ArrayType(ArrayType(DoubleType())))
        == "array<array<double>>"
    )
    assert (
        snow_type_to_dtype_str(MapType(StringType(), BooleanType()))
        == "map<string,boolean>"
    )
    assert (
        snow_type_to_dtype_str(MapType(StringType(), ArrayType(VariantType())))
        == "map<string,array<variant>>"
    )
    assert (
        snow_type_to_dtype_str(
            StructType(
                [
                    StructField("str", StringType()),
                    StructField("array", ArrayType(VariantType())),
                    StructField("map", MapType(StringType(), BooleanType())),
                    StructField(
                        "struct", StructType([StructField("time", TimeType())])
                    ),
                ]
            )
        )
        == "struct<string,array<variant>,map<string,boolean>,struct<time>>"
    )

    with pytest.raises(TypeError, match="invalid DataType"):
        snow_type_to_dtype_str(None)
