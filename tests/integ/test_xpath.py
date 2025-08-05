#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
from snowflake.snowpark.functions import (
    xpath,
    xpath_string,
    xpath_boolean,
    xpath_number,
    xpath_int,
    xpath_float,
    xpath_double,
    xpath_long,
    xpath_short,
    col,
)
from snowflake.snowpark.types import (
    ArrayType,
    StringType,
    BooleanType,
    DoubleType,
    LongType,
)


def test_xpath(session):
    """Test basic xpath function returning array of strings"""
    # Test extracting text content
    df = session.create_dataframe(
        [["<root><a>1</a><a>2</a><a>3</a></root>"]], schema=["xml"]
    )
    result = df.select(xpath("xml", "//a/text()").alias("values")).collect()
    assert json.loads(result[0].VALUES) == ["1", "2", "3"]

    # Test extracting elements (should return text content)
    result = df.select(xpath("xml", "//a").alias("values")).collect()
    assert json.loads(result[0].VALUES) == ["1", "2", "3"]

    # Test no matches returns empty array
    result = df.select(xpath("xml", "//b").alias("values")).collect()
    assert json.loads(result[0].VALUES) == []

    # Test with attributes
    df2 = session.create_dataframe(
        [['<root><item id="1">A</item><item id="2">B</item></root>']], schema=["xml"]
    )
    result = df2.select(
        xpath("xml", '//item[@id="2"]/text()').alias("values")
    ).collect()
    assert json.loads(result[0].VALUES) == ["B"]

    # Test extracting attribute values
    result = df2.select(xpath("xml", "//item/@id").alias("ids")).collect()
    assert json.loads(result[0].IDS) == ["1", "2"]


def test_xpath_string(session):
    """Test xpath_string function returning first match as string"""
    df = session.create_dataframe(
        [["<root><name>John</name><name>Jane</name></root>"]], schema=["xml"]
    )

    # Test getting first match
    result = df.select(xpath_string("xml", "//name/text()").alias("name")).collect()
    assert result[0].NAME == "John"

    # Test no match returns NULL
    result = df.select(xpath_string("xml", "//missing").alias("name")).collect()
    assert result[0].NAME is None

    # Test with complex path
    df2 = session.create_dataframe(
        [["<root><person><first>John</first><last>Doe</last></person></root>"]],
        schema=["xml"],
    )
    result = df2.select(
        xpath_string("xml", "//person/last/text()").alias("last")
    ).collect()
    assert result[0].LAST == "Doe"


def test_xpath_boolean(session):
    """Test xpath_boolean function with various conditions"""
    df = session.create_dataframe([["<root><a>1</a><b>2</b></root>"]], schema=["xml"])

    # Test element exists
    result = df.select(xpath_boolean("xml", "//a").alias("has_a")).collect()
    assert result[0].HAS_A is True

    # Test element doesn't exist
    result = df.select(xpath_boolean("xml", "//c").alias("has_c")).collect()
    assert result[0].HAS_C is False

    # Test XPath boolean expressions
    result = df.select(
        xpath_boolean("xml", "count(//a) > 0").alias("count_check")
    ).collect()
    assert result[0].COUNT_CHECK is True

    # Test comparison
    df2 = session.create_dataframe(
        [["<root><price>10.5</price></root>"]], schema=["xml"]
    )
    result = df2.select(
        xpath_boolean("xml", "//price > 5").alias("expensive")
    ).collect()
    assert result[0].EXPENSIVE is True


def test_xpath_float_and_int(session):
    """Test xpath_float and xpath_int functions"""
    df = session.create_dataframe(
        [["<root><price>19.99</price><count>42</count></root>"]], schema=["xml"]
    )

    # Test xpath_float returns float
    result = df.select(xpath_float("xml", "//price/text()").alias("price")).collect()
    assert result[0].PRICE == 19.99
    assert isinstance(result[0].PRICE, float)

    # Test xpath_int returns integer
    result = df.select(xpath_int("xml", "//count/text()").alias("count")).collect()
    assert result[0].COUNT == 42
    assert isinstance(result[0].COUNT, int)

    # Test xpath_int truncates floats
    result = df.select(xpath_int("xml", "//price/text()").alias("price_int")).collect()
    assert result[0].PRICE_INT == 19

    # Test no match returns NULL
    result = df.select(xpath_float("xml", "//missing").alias("val")).collect()
    assert result[0].VAL is None

    # Test invalid number returns NULL
    df2 = session.create_dataframe(
        [["<root><value>not-a-number</value></root>"]], schema=["xml"]
    )
    result = df2.select(xpath_float("xml", "//value/text()").alias("val")).collect()
    assert result[0].VAL is None


def test_xpath_aliases(session):
    """Test that Spark compatibility aliases work correctly"""
    df = session.create_dataframe(
        [["<root><value>42.5</value></root>"]], schema=["xml"]
    )

    # Test float aliases
    float_result = df.select(
        xpath_number("xml", "//value/text()").alias("val")
    ).collect()
    double_result = df.select(
        xpath_double("xml", "//value/text()").alias("val")
    ).collect()
    assert float_result[0].VAL == double_result[0].VAL == 42.5

    # Test int aliases
    int_result = df.select(xpath_int("xml", "//value/text()").alias("val")).collect()
    long_result = df.select(xpath_long("xml", "//value/text()").alias("val")).collect()
    short_result = df.select(
        xpath_short("xml", "//value/text()").alias("val")
    ).collect()
    assert int_result[0].VAL == long_result[0].VAL == short_result[0].VAL == 42


def test_xpath_null_handling(session):
    """Test NULL handling in XPath functions"""
    # Create DataFrame with NULL values
    df = session.create_dataframe([[None], ["<root><a>1</a></root>"]], schema=["xml"])

    # Test xpath with NULL input
    result = df.select(xpath("xml", "//a").alias("values")).collect()
    assert json.loads(result[0].VALUES) == []  # NULL input returns empty array
    assert json.loads(result[1].VALUES) == ["1"]

    # Test xpath_string with NULL
    result = df.select(xpath_string("xml", "//a").alias("val")).collect()
    assert result[0].VAL is None
    assert result[1].VAL == "1"

    # Test xpath_boolean with NULL
    result = df.select(xpath_boolean("xml", "//a").alias("val")).collect()
    assert result[0].VAL is False
    assert result[1].VAL is True

    # Test xpath_float with NULL
    result = df.select(xpath_float("xml", "//a").alias("val")).collect()
    assert result[0].VAL is None
    assert result[1].VAL == 1.0


def test_xpath_complex_expressions(session):
    """Test complex XPath expressions"""
    df = session.create_dataframe(
        [
            [
                """<books>
            <book id="1" category="fiction">
                <title>Book A</title>
                <price>10.99</price>
            </book>
            <book id="2" category="tech">
                <title>Book B</title>
                <price>29.99</price>
            </book>
            <book id="3" category="fiction">
                <title>Book C</title>
                <price>15.99</price>
            </book>
        </books>"""
            ]
        ],
        schema=["xml"],
    )

    # Test selecting by attribute
    result = df.select(
        xpath("xml", '//book[@category="fiction"]/title/text()').alias("fiction_titles")
    ).collect()
    assert json.loads(result[0].FICTION_TITLES) == ["Book A", "Book C"]

    # Test positional selection
    result = df.select(
        xpath_string("xml", "//book[2]/title/text()").alias("second_book")
    ).collect()
    assert result[0].SECOND_BOOK == "Book B"

    # Test numeric comparison
    result = df.select(
        xpath("xml", "//book[price > 15]/title/text()").alias("expensive_books")
    ).collect()
    assert json.loads(result[0].EXPENSIVE_BOOKS) == ["Book B", "Book C"]

    # Test count function
    result = df.select(
        xpath_float("xml", "count(//book)").alias("book_count")
    ).collect()
    assert result[0].BOOK_COUNT == 3.0


def test_xpath_namespaces(session):
    """Test XPath with XML namespaces"""
    df = session.create_dataframe(
        [
            [
                """<root xmlns:ns="http://example.com">
            <ns:item>Value1</ns:item>
            <ns:item>Value2</ns:item>
        </root>"""
            ]
        ],
        schema=["xml"],
    )

    # Note: Default namespace handling varies by implementation
    # Testing with explicit namespace prefix
    result = df.select(
        xpath("xml", '//*[local-name()="item"]/text()').alias("items")
    ).collect()
    assert json.loads(result[0].ITEMS) == ["Value1", "Value2"]


def test_xpath_malformed_xml(session):
    """Test malformed XML"""
    # Test with malformed XML
    df = session.create_dataframe([["<root><unclosed>value"]], schema=["xml"])
    result = df.select(xpath("xml", "//unclosed/text()").alias("val")).collect()
    # lxml with recover=True should handle malformed XML gracefully
    assert json.loads(result[0].VAL) == ["value"]

    # Test with empty string
    df2 = session.create_dataframe([[""]], schema=["xml"])
    result = df2.select(xpath("xml", "//a").alias("val")).collect()
    assert json.loads(result[0].VAL) == []

    # Test with invalid XPath expression
    df3 = session.create_dataframe([["<root><a>1</a></root>"]], schema=["xml"])
    result = df3.select(xpath("xml", "//[invalid").alias("val")).collect()
    # Should return empty array on error
    assert json.loads(result[0].VAL) == []


def test_xpath_with_column_expressions(session):
    """Test XPath functions with column references"""
    df = session.create_dataframe(
        [
            ["<root><value>10</value></root>", "//value/text()"],
            ["<root><item>20</item></root>", "//item/text()"],
        ],
        schema=["xml", "path"],
    )

    # Using column for XPath expression
    result = df.select(xpath_string(col("xml"), col("path")).alias("result")).collect()
    assert result[0].RESULT == "10"
    assert result[1].RESULT == "20"


def test_xpath_return_types(session):
    """Verify that XPath functions return correct data types"""
    df = session.create_dataframe(
        [["<root><a>1</a><b>true</b><c>1.5</c></root>"]], schema=["xml"]
    )

    # Check return type schema
    xpath_df = df.select(
        xpath("xml", "//a").alias("array_col"),
        xpath_string("xml", "//a").alias("string_col"),
        xpath_boolean("xml", "//b").alias("bool_col"),
        xpath_float("xml", "//c").alias("float_col"),
        xpath_int("xml", "//c").alias("int_col"),
    )

    schema = xpath_df.schema
    assert isinstance(schema["ARRAY_COL"].datatype, ArrayType)
    assert isinstance(schema["ARRAY_COL"].datatype.element_type, StringType)
    assert isinstance(schema["STRING_COL"].datatype, StringType)
    assert isinstance(schema["BOOL_COL"].datatype, BooleanType)
    assert isinstance(schema["FLOAT_COL"].datatype, DoubleType)
    assert isinstance(schema["INT_COL"].datatype, LongType)
