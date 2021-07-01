#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from snowflake.snowpark.types.sf_types import (
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
    NumericType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimeType,
    VariantType,
)


def test_datatype_names():

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

    assert DataType().to_string() == "Data"
    assert MapType(BinaryType(), FloatType()).to_string() == "MapType[Binary,Float]"
    assert VariantType().to_string() == "Variant"
    assert BinaryType().to_string() == "Binary"
    assert BooleanType().to_string() == "Boolean"
    assert DateType().to_string() == "Date"
    assert StringType().to_string() == "String"
    assert NumericType().to_string() == "Numeric"
    assert IntegralType().to_string() == "Integral"
    assert NumericType().to_string() == "Numeric"
    assert TimeType().to_string() == "Time"
    assert ByteType().to_string() == "Byte"
    assert ShortType().to_string() == "Short"
    assert IntegerType().to_string() == "Integer"
    assert LongType().to_string() == "Long"
    assert FloatType().to_string() == "Float"
    assert DoubleType().to_string() == "Double"
    assert DecimalType(1, 2).to_string() == "Decimal(1,2)"


def test_struct_field_name():
    column_identifier = ColumnIdentifier("identifier")
    assert StructField(column_identifier, IntegerType(), False).name == "identifier"
    assert (
        StructField(column_identifier, IntegerType(), False).to_string()
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
