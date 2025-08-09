#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

# Import the handlers to test
from snowflake.snowpark._internal.xpath_handlers import (
    _xpath_evaluate_internal,
    xpath_array_handler,
    xpath_string_handler,
    xpath_boolean_handler,
    xpath_int_handler,
    xpath_float_handler,
)


# Test xpath_array_handler
def test_xpath_array_basic():
    """Test basic array extraction."""
    xml = "<root><item>1</item><item>2</item><item>3</item></root>"
    assert xpath_array_handler(xml, "//item") == ["1", "2", "3"]
    assert xpath_array_handler(xml, "//missing") == []


@pytest.mark.parametrize("xml_input", [None, ""])
def test_xpath_array_null_inputs(xml_input):
    """Test array handler with NULL inputs."""
    assert xpath_array_handler(xml_input, "//item") == []
    assert xpath_array_handler("<root/>", None) == []


def test_xpath_array_attributes():
    """Test array handler extracting attribute values."""
    xml = '<root><item id="1"/><item id="2"/><item id="3"/></root>'
    assert xpath_array_handler(xml, "//item/@id") == ["1", "2", "3"]


def test_xpath_array_mixed_content():
    """Test array handler with mixed element and text content."""
    xml = "<root><item>text1<sub>nested</sub>text2</item></root>"
    assert xpath_array_handler(xml, "//item") == ["text1nestedtext2"]


def test_xpath_array_invalid():
    """Test array handler with invalid XML/XPath."""
    # Invalid XML - should recover
    assert isinstance(xpath_array_handler("<root><item>unclosed", "//item"), list)
    # Invalid XPath - should return empty
    assert xpath_array_handler("<root><item>1</item></root>", "//item[invalid") == []


# Test xpath_string_handler
def test_xpath_string_basic():
    """Test basic string extraction."""
    xml = "<root><name>John Doe</name><name>Jane Doe</name></root>"
    assert xpath_string_handler(xml, "//name") == "John Doe"  # First match only
    assert xpath_string_handler(xml, "//missing") is None


@pytest.mark.parametrize("xml_input", [None, ""])
def test_xpath_string_null_inputs(xml_input):
    """Test string handler with NULL inputs."""
    assert xpath_string_handler(xml_input, "//item") is None
    assert xpath_string_handler("<root/>", None) is None


def test_xpath_string_attributes_and_text():
    """Test string handler with attributes and text functions."""
    xml = '<root><item id="test123">content</item></root>'
    assert xpath_string_handler(xml, "//item/@id") == "test123"
    assert xpath_string_handler(xml, "//item/text()") == "content"


def test_xpath_string_nested():
    """Test string handler with nested elements."""
    xml = "<root><item>outer<nested>inner</nested>text</item></root>"
    assert xpath_string_handler(xml, "//item") == "outerinnertext"


# Test xpath_boolean_handler
def test_xpath_boolean_basic():
    """Test basic boolean evaluation."""
    xml = "<root><item>value</item></root>"
    assert xpath_boolean_handler(xml, "//item") is True
    assert xpath_boolean_handler(xml, "//missing") is False


@pytest.mark.parametrize("xml_input", [None, ""])
def test_xpath_boolean_null_inputs(xml_input):
    """Test boolean handler with NULL inputs."""
    assert xpath_boolean_handler(xml_input, "//item") is False
    assert xpath_boolean_handler("<root/>", None) is False


@pytest.mark.parametrize(
    "xpath,expected",
    [
        ("count(//item) > 0", True),
        ("count(//item) = 3", False),
        ("string-length('test') > 0", True),
        ("1 = 1", True),
        ("1 = 2", False),
        ("true()", True),
        ("false()", False),
    ],
)
def test_xpath_boolean_functions(xpath, expected):
    """Test boolean handler with XPath functions."""
    xml = "<root><item>1</item></root>"
    assert xpath_boolean_handler(xml, xpath) == expected


def test_xpath_boolean_string_coercion():
    """Test boolean coercion for string results."""
    xml = "<root><item></item><item>text</item></root>"
    assert xpath_boolean_handler(xml, "//item[1]/text()") is False  # Empty
    assert xpath_boolean_handler(xml, "string(//item[2])") is True  # Non-empty


# Test xpath_int_handler
def test_xpath_int_basic():
    """Test basic integer extraction."""
    xml = "<root><count>42</count><value>3.7</value><neg>-15</neg></root>"
    assert xpath_int_handler(xml, "//count") == 42
    assert xpath_int_handler(xml, "//value") == 3  # Truncates decimal
    assert xpath_int_handler(xml, "//neg") == -15
    assert xpath_int_handler(xml, "//missing") is None


@pytest.mark.parametrize("xml_input", [None, ""])
def test_xpath_int_null_inputs(xml_input):
    """Test integer handler with NULL inputs."""
    assert xpath_int_handler(xml_input, "//value") is None
    assert xpath_int_handler("<root/>", None) is None


def test_xpath_int_invalid_and_whitespace():
    """Test integer handler with invalid numbers and whitespace."""
    xml = "<root><invalid>not_a_number</invalid><spaces>  42  </spaces></root>"
    assert xpath_int_handler(xml, "//invalid") is None
    assert xpath_int_handler(xml, "//spaces") == 42


def test_xpath_int_functions():
    """Test integer handler with XPath numeric functions."""
    xml = "<root><item>1</item><item>2</item><item>3</item></root>"
    assert xpath_int_handler(xml, "count(//item)") == 3


# Test xpath_float_handler
def test_xpath_float_basic():
    """Test basic float extraction."""
    xml = (
        "<root><price>19.99</price><int>42</int><sci>1.5e2</sci><neg>-3.14</neg></root>"
    )
    assert xpath_float_handler(xml, "//price") == 19.99
    assert xpath_float_handler(xml, "//int") == 42.0
    assert xpath_float_handler(xml, "//sci") == 150.0
    assert xpath_float_handler(xml, "//neg") == -3.14
    assert xpath_float_handler(xml, "//missing") is None


@pytest.mark.parametrize("xml_input", [None, ""])
def test_xpath_float_null_inputs(xml_input):
    """Test float handler with NULL inputs."""
    assert xpath_float_handler(xml_input, "//value") is None
    assert xpath_float_handler("<root/>", None) is None


def test_xpath_float_invalid_and_whitespace():
    """Test float handler with invalid numbers and whitespace."""
    xml = "<root><invalid>not_a_number</invalid><spaces>  3.14159  </spaces></root>"
    assert xpath_float_handler(xml, "//invalid") is None
    assert xpath_float_handler(xml, "//spaces") == 3.14159


def test_xpath_float_functions():
    """Test float handler with XPath numeric functions."""
    xml = "<root><item>1.5</item><item>2.5</item><item>3.0</item></root>"
    assert xpath_float_handler(xml, "sum(//item)") == 7.0


# Test _xpath_evaluate_internal special cases
def test_xpath_namespaces():
    """Test XPath evaluation with XML namespaces."""
    xml = '<root xmlns:ns="http://example.com"><ns:item>value</ns:item></root>'
    result = _xpath_evaluate_internal(xml, "//*[local-name()='item']", "string")
    assert result == "value"


def test_xpath_complex_expressions():
    """Test complex XPath expressions."""
    xml = """
    <root>
        <person age="25"><name>Alice</name></person>
        <person age="30"><name>Bob</name></person>
        <person age="35"><name>Charlie</name></person>
    </root>
    """
    result = _xpath_evaluate_internal(xml, "//person[@age > 25]/name", "array")
    assert result == ["Bob", "Charlie"]

    result = _xpath_evaluate_internal(xml, "//person[position() = 2]/name", "string")
    assert result == "Bob"


def test_xpath_malformed_xml_recovery():
    """Test that malformed XML is handled with recovery."""
    xml = "<root><item>value</item><broken"
    result = _xpath_evaluate_internal(xml, "//item", "string")
    assert result == "value"  # Should recover and find the item


def test_xpath_utf8_and_special_chars():
    """Test UTF-8 and special character handling."""
    xml = "<root><text>Café naïve 中文</text></root>"
    assert _xpath_evaluate_internal(xml, "//text", "string") == "Café naïve 中文"

    xml = '<root><item name="test&amp;value">content</item></root>'
    assert xpath_string_handler(xml, "//item[@name='test&value']") == "content"


def test_xpath_cdata():
    """Test handling of CDATA sections."""
    xml = "<root><item><![CDATA[<special>characters</special>]]></item></root>"
    assert xpath_string_handler(xml, "//item") == "<special>characters</special>"


@pytest.mark.parametrize(
    "return_type,expected_default",
    [
        ("array", []),
        ("string", None),
        ("boolean", False),
        ("int", None),
        ("float", None),
    ],
)
def test_xpath_null_handling(return_type, expected_default):
    """Test NULL handling for different return types."""
    result = _xpath_evaluate_internal(None, "//item", return_type)
    assert result == expected_default


def test_xpath_exception_handling():
    """Test exception handling with invalid XPath."""
    xml = "<root>test</root>"
    # Invalid XPath that will cause an exception
    xpath = "///invalid//xpath[["

    assert _xpath_evaluate_internal(xml, xpath, "array") == []
    assert _xpath_evaluate_internal(xml, xpath, "string") is None
    assert _xpath_evaluate_internal(xml, xpath, "boolean") is False
    assert _xpath_evaluate_internal(xml, xpath, "int") is None
    assert _xpath_evaluate_internal(xml, xpath, "float") is None


def test_xpath_string_functions():
    """Test XPath string manipulation functions."""
    xml = "<root><item>TEST</item></root>"

    # Test substring
    assert xpath_string_handler(xml, "substring(//item, 1, 2)") == "TE"

    # Test string-length
    assert xpath_int_handler(xml, "string-length(//item)") == 4


def test_xpath_mixed_content():
    """Test handling of mixed content (text and elements)."""
    xml = """
    <root>
        <para>This is <bold>bold</bold> and this is <italic>italic</italic> text.</para>
    </root>
    """
    result = xpath_string_handler(xml, "//para")
    assert result == "This is bold and this is italic text."
