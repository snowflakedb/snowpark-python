#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io
import re
import tempfile
import os
import lxml.etree as ET
import html.entities
from unittest import mock
from unittest.mock import patch
import pytest

from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    attribute_to_schema_string_deep,
    single_quote,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    DEFAULT_MAX_WORKERS,
    SnowflakePlanBuilder,
    _positive_int_option,
    XML_BATCH_MAX_FILES,
    XML_BATCH_TARGET_BYTES,
    _pack_xml_assignments,
    _stage_listing_basename,
    _xml_worker_assignment_sql,
    _xml_worker_assignments,
    _XML_WORKER_ASSIGNMENT_MAX_ROWS_PER_VALUES,
)
from snowflake.snowpark._internal.udf_utils import (
    retrieve_func_type_hints_from_source,
)
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark._internal.utils import (
    XML_READER_FILE_PATH,
    XML_ROW_TAG_STRING,
)
from snowflake.snowpark._internal.utils import (
    quote_name,
    use_xml_variant_projection,
    xml_variant_projection,
)
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark._internal.xml_reader import (
    replace_entity,
    element_to_dict_or_str,
    strip_xml_namespaces,
    find_next_closing_tag_pos,
    find_next_opening_tag_pos,
    tag_is_self_closing,
    process_xml_range,
    DEFAULT_CHUNK_SIZE,
    struct_type_to_result_template,
    schema_string_to_result_dict_and_struct_type,
    _escape_colons_in_quotes,
    _restore_colons_in_template,
    _COLON_PLACEHOLDER,
    _can_cast_to_type,
    _validate_row_for_type_mismatch,
    XMLReader,
    XMLReaderWithPos,
    _process_batch_concurrently,
    decode_batch_or_single,
    encode_batch,
)
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
    ArrayType,
    MapType,
    LongType,
    BooleanType,
    TimestampType,
)


def _records_only(record_iter):
    """Drop the byte-offset half of process_xml_range's (record, offset) tuples."""
    return [record for record, _ in record_iter]


def test_replace_entity_predefined():
    # Predefined XML entity (e.g., amp) should remain unchanged.
    match = re.match(r"&(amp);", "&amp;")
    assert replace_entity(match) == "&amp;"


def test_replace_entity_non_predefined():
    # Known non-predefined entities (e.g., copy) should be replaced.
    match = re.match(r"&(copy);", "&copy;")
    expected = chr(html.entities.name2codepoint["copy"])
    assert replace_entity(match) == expected


def test_replace_entity_unknown():
    # Unknown entities should be left unchanged.
    match = re.match(r"&(foo);", "&foo;")
    assert replace_entity(match) == "&foo;"


@pytest.mark.parametrize(
    "text", ("   \n\t  ", " ", "\n\n", "  \t  ", "  hello world  ")
)
def test_element_to_dict_or_str_text(text):
    # Element with only text.
    element = ET.Element("greeting")
    element.text = text
    result = element_to_dict_or_str(element)
    assert result == text


@pytest.mark.parametrize("attribute_prefix", ["_", ""])
def test_element_to_dict_or_str_attributes(attribute_prefix):
    element = ET.Element("person", attrib={"name": "Alice", "age": "30"})
    element.text = None
    result = element_to_dict_or_str(
        element, attribute_prefix=attribute_prefix, value_tag="value"
    )
    expected = {f"{attribute_prefix}name": "Alice", f"{attribute_prefix}age": "30"}
    assert result == expected
    element.text = "xxx"
    result = element_to_dict_or_str(
        element, attribute_prefix=attribute_prefix, value_tag="value"
    )
    expected = {
        f"{attribute_prefix}name": "Alice",
        f"{attribute_prefix}age": "30",
        "value": "xxx",
    }
    assert result == expected


def test_element_to_dict_or_str_exclude_attributes():
    element = ET.Element("person", attrib={"name": "Alice", "age": "30"})
    result = element_to_dict_or_str(
        element, attribute_prefix="_", exclude_attributes=True, value_tag="value"
    )
    assert result is None
    element.text = "xxx"
    result = element_to_dict_or_str(element, exclude_attributes=True, value_tag="value")
    assert result == "xxx"


def test_element_to_dict_or_str_children():
    # Element with children including repeated tags.
    root = ET.Element("data")
    child1 = ET.SubElement(root, "item")
    child1.text = "value1"
    child2 = ET.SubElement(root, "item")
    child2.text = "value2"
    child3 = ET.SubElement(root, "note")
    child3.text = "note1"
    result = element_to_dict_or_str(root)
    expected = {"item": ["value1", "value2"], "note": "note1"}
    assert result == expected


@pytest.mark.parametrize("null_value", ["", "NULL", "<empty>", "N/A"])
def test_element_to_dict_or_str_null_value(null_value):
    element = ET.Element("empty")
    element.text = null_value
    result = element_to_dict_or_str(element, null_value=null_value)
    assert result is None


@pytest.mark.parametrize("null_value", ["", "NULL", "<empty>", "N/A"])
def test_element_to_dict_or_str_null_value_with_attributes(null_value):
    element = ET.Element("empty", attrib={"attr": null_value})
    element.text = null_value
    result = element_to_dict_or_str(element, null_value=null_value)
    assert result == {"_attr": None}


@pytest.mark.parametrize("null_value", ["", "NULL", "<empty>", "N/A"])
def test_element_to_dict_or_str_null_value_with_children(null_value):
    element = ET.Element("empty")
    child = ET.SubElement(element, "child")
    child.text = null_value
    result = element_to_dict_or_str(element, null_value=null_value)
    assert result == {"child": None}


@pytest.mark.parametrize("ignore_surrounding_whitespace", [True, False])
def test_element_to_dict_or_str_ignore_surrounding_whitespace_text(
    ignore_surrounding_whitespace,
):
    """Test ignore_surrounding_whitespace parameter with element text containing surrounding whitespace."""
    element = ET.Element("greeting")
    element.text = "  \n\t  hello world  \n\t  "
    result = element_to_dict_or_str(
        element, ignore_surrounding_whitespace=ignore_surrounding_whitespace
    )
    if ignore_surrounding_whitespace:
        assert result == "hello world"
    else:
        assert result == "  \n\t  hello world  \n\t  "


@pytest.mark.parametrize("ignore_surrounding_whitespace", [True, False])
def test_element_to_dict_or_str_ignore_surrounding_whitespace_with_attributes(
    ignore_surrounding_whitespace,
):
    """Test ignore_surrounding_whitespace parameter with element text and attributes containing whitespace."""
    element = ET.Element("person", attrib={"name": "  Alice  ", "age": " 30 "})
    element.text = "  \n  content text  \n  "
    result = element_to_dict_or_str(
        element,
        attribute_prefix="_",
        value_tag="value",
        ignore_surrounding_whitespace=ignore_surrounding_whitespace,
    )
    expected_result = (
        {"_name": "Alice", "_age": "30", "value": "content text"}
        if ignore_surrounding_whitespace
        else {"_name": "  Alice  ", "_age": " 30 ", "value": "  \n  content text  \n  "}
    )
    assert result == expected_result


@pytest.mark.parametrize("ignore_surrounding_whitespace", [True, False])
def test_element_to_dict_or_str_ignore_surrounding_whitespace_with_null_value(
    ignore_surrounding_whitespace,
):
    """Test ignore_surrounding_whitespace parameter with null_value handling."""
    element = ET.Element("test")
    element.text = "  NULL  "
    result = element_to_dict_or_str(
        element,
        null_value="NULL",
        ignore_surrounding_whitespace=ignore_surrounding_whitespace,
    )
    if ignore_surrounding_whitespace:
        assert result is None  # "  NULL  ".strip() == "NULL" -> None
    else:
        assert result == "  NULL  "  # "  NULL  " != "NULL" -> keep original


def test_default_namespace():
    """
    Test that a default namespace is correctly stripped from tags and attributes.
    """
    xml_data = """<Return xmlns="http://www.irs.gov/efile" returnVersion="2020v4.1">
                      <Name>John Doe</Name>
                  </Return>"""
    root = ET.fromstring(xml_data)
    root = strip_xml_namespaces(root)
    assert root.tag == "Return"
    assert root.attrib.get("returnVersion") == "2020v4.1"
    name_elem = root.find("Name")
    assert name_elem is not None
    assert name_elem.tag == "Name"
    assert name_elem.text.strip() == "John Doe"


def test_multiple_namespaces():
    """
    Test that multiple namespaces (including prefixed ones) are stripped properly.
    """
    xml_data = """
        <Return xmlns="http://www.irs.gov/efile" xmlns:abc="http://example.com">
            <Name>John Doe</Name>
            <abc:Detail>Some detail</abc:Detail>
        </Return>
    """
    root = ET.fromstring(xml_data)
    root = strip_xml_namespaces(root)
    assert root.tag == "Return"
    name_elem = root.find("Name")
    assert name_elem is not None
    assert name_elem.tag == "Name"
    assert name_elem.text.strip() == "John Doe"
    # The namespaced tag <abc:Detail> becomes 'Detail' after stripping
    detail_elem = root.find("Detail")
    assert detail_elem is not None
    assert detail_elem.tag == "Detail"
    assert detail_elem.text.strip() == "Some detail"


def test_attributes_with_namespaces():
    """
    Test that attributes with namespaced keys are properly renamed to their local names.
    """
    xml_data = """
        <Return xmlns="http://www.irs.gov/efile"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://www.irs.gov/efile"
                returnVersion="2020v4.1">
            <Name>John Doe</Name>
        </Return>
    """
    root = ET.fromstring(xml_data)
    root = strip_xml_namespaces(root)
    assert root.tag == "Return"
    # The namespaced attribute should be available with its local name.
    assert root.attrib.get("schemaLocation") == "http://www.irs.gov/efile"
    assert root.attrib.get("returnVersion") == "2020v4.1"


def test_nested_elements():
    """
    Test that nested elements and their attributes are processed recursively.
    """
    xml_data = """
        <Return xmlns="http://www.irs.gov/efile">
            <Info>
                <Detail returnVersion="v1">Data</Detail>
            </Info>
        </Return>
    """
    root = ET.fromstring(xml_data)
    root = strip_xml_namespaces(root)
    assert root.tag == "Return"
    info_elem = root.find("Info")
    assert info_elem is not None
    assert info_elem.tag == "Info"
    detail_elem = info_elem.find("Detail")
    assert detail_elem is not None
    assert detail_elem.tag == "Detail"
    assert detail_elem.attrib.get("returnVersion") == "v1"
    assert detail_elem.text.strip() == "Data"


def test_undeclared_namespace():
    xml_data = """
<px:intermediaryCommission>
  <px:intermediaryPremiumClass>GLASS</px:intermediaryPremiumClass>
  <px:newBusinessCommission>0.2</px:newBusinessCommission>
  <px:otherCommission>0.2</px:otherCommission>
</px:intermediaryCommission>
"""
    parser = ET.XMLParser(recover=True, ns_clean=True)
    root = ET.fromstring(xml_data, parser)
    root = strip_xml_namespaces(root)

    # Verify prefixes remain since they don't follow {namespace}tag format
    assert root.tag == "px:intermediaryCommission"

    # Check that child elements also retain their prefixes
    children = list(root)
    assert len(children) == 3
    assert children[0].tag == "px:intermediaryPremiumClass"
    assert children[0].text.strip() == "GLASS"
    assert children[1].tag == "px:newBusinessCommission"
    assert children[1].text.strip() == "0.2"
    assert children[2].tag == "px:otherCommission"
    assert children[2].text.strip() == "0.2"


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_find_next_closing_tag_pos_normal(chunk_size):
    # Test a normal closing tag.
    record = b"<row>data</row> trailing content"
    file_obj = io.BytesIO(record)
    closing_tag = b"</row>"
    pos = find_next_closing_tag_pos(file_obj, closing_tag, chunk_size=chunk_size)
    expected_pos = record.find(closing_tag) + len(closing_tag)
    assert pos == expected_pos


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_find_next_closing_tag_pos_split(chunk_size):
    # Simulate a closing tag (</row>) split across chunk boundaries.
    part1 = b"data <row>some content</ro"
    part2 = b"w> trailing"
    record = part1 + part2
    file_obj = io.BytesIO(record)
    closing_tag = b"</row>"
    # Use a small chunk size to force a split.
    pos = find_next_closing_tag_pos(file_obj, closing_tag, chunk_size=chunk_size)
    expected_pos = record.find(closing_tag) + len(closing_tag)
    assert pos == expected_pos


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_find_next_closing_tag_pos_no_tag(chunk_size):
    # When no closing tag is present, an EOFError should be raised.
    record = b"data without closing tag"
    file_obj = io.BytesIO(record)
    closing_tag = b"</row>"
    with pytest.raises(EOFError):
        find_next_closing_tag_pos(file_obj, closing_tag, chunk_size=chunk_size)


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_tag_not_self_closing(chunk_size):
    record = b'<row attr="abc">payload</row>'
    f = io.BytesIO(record)
    is_self, end_pos = tag_is_self_closing(f, chunk_size=chunk_size)
    assert is_self is False
    assert end_pos == record.find(b">") + 1


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_tag_self_closing(chunk_size):
    record = b'<row attr1="abc" attr2="cde"/> trailing text'
    f = io.BytesIO(record)
    is_self, end_pos = tag_is_self_closing(f, chunk_size=chunk_size)
    assert is_self is True
    assert end_pos == record.find(b">") + 1


@pytest.mark.parametrize("chunk_size", [2, 5, DEFAULT_CHUNK_SIZE])
def test_tag_with_mixed_quote_chars_self_closing(chunk_size):
    record = b"<row note='She said \"Hi\"'/> trailing"
    f = io.BytesIO(record)
    is_self, end_pos = tag_is_self_closing(f, chunk_size=chunk_size)
    assert is_self is True
    assert end_pos == record.find(b">") + 1


@pytest.mark.parametrize("chunk_size", [1, 4, 8, DEFAULT_CHUNK_SIZE])
def test_tag_with_gt_inside_quotes(chunk_size):
    record = b'<row note="1 > 0" id="42">content</row>'
    f = io.BytesIO(record)
    is_self, end_pos = tag_is_self_closing(f, chunk_size=chunk_size)
    assert is_self is False
    # '>' before 'content' is the correct end pos
    assert end_pos == record.find(b"content")


@pytest.mark.parametrize("chunk_size", [2, 5])
def test_tag_split_across_chunks(chunk_size):
    record = (
        b'<row verylongattribute="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"/>' b" tail"
    )
    f = io.BytesIO(record)
    is_self, end_pos = tag_is_self_closing(f, chunk_size=chunk_size)
    assert is_self is True
    assert end_pos == record.find(b">") + 1


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_find_next_opening_tag_pos_normal(chunk_size):
    # Test with a normal opening tag.
    record = b"prefix text <row attr='value'> middle text <row> extra"
    file_obj = io.BytesIO(record)
    tag_start_1 = b"<row>"
    tag_start_2 = b"<row "
    end_limit = len(record)
    pos = find_next_opening_tag_pos(
        file_obj, tag_start_1, tag_start_2, end_limit, chunk_size=chunk_size
    )
    # Expect the first occurrence of either variant.
    expected_pos = record.find(b"<row ")
    assert pos == expected_pos


@pytest.mark.parametrize("chunk_size", [10, 100, DEFAULT_CHUNK_SIZE])
def test_find_next_opening_tag_pos_full_chunk_before_tag(chunk_size):
    # This tests that the overlap logic works correctly when multiple chunks
    # must be read before finding the tag.
    prefix = b"x" * (chunk_size * 2 + 10)  # More than 2 full chunks
    record = prefix + b"<row attr='value'> more content here </row>"
    file_obj = io.BytesIO(record)
    tag_start_1 = b"<row>"
    tag_start_2 = b"<row "
    end_limit = len(record)
    pos = find_next_opening_tag_pos(
        file_obj, tag_start_1, tag_start_2, end_limit, chunk_size=chunk_size
    )
    # Should find the first tag after all the prefix data
    expected_pos = len(prefix)
    assert pos == expected_pos
    # Verify file pointer is at the correct position
    assert file_obj.tell() == expected_pos


@pytest.mark.parametrize("chunk_size", [10, 100, DEFAULT_CHUNK_SIZE])
def test_find_next_opening_tag_pos_tag_spans_chunk_boundary(chunk_size):
    # Position the tag so it splits exactly across a chunk boundary.
    # This is the most challenging case for the overlap logic.
    # Place the tag start 2 bytes before the chunk boundary
    prefix = b"x" * (chunk_size - 2)
    record = prefix + b"<row attr='value'> content </row>"
    file_obj = io.BytesIO(record)
    tag_start_1 = b"<row>"
    tag_start_2 = b"<row "
    end_limit = len(record)
    pos = find_next_opening_tag_pos(
        file_obj, tag_start_1, tag_start_2, end_limit, chunk_size=chunk_size
    )
    expected_pos = len(prefix)
    assert pos == expected_pos


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_find_next_opening_tag_pos_both_variants(chunk_size):
    # Test when both "<row>" and "<row " exist.
    record = b"some text <row> complete tag, then <row attr='val'> another tag"
    file_obj = io.BytesIO(record)
    tag_start_1 = b"<row>"
    tag_start_2 = b"<row "
    end_limit = len(record)
    pos = find_next_opening_tag_pos(
        file_obj, tag_start_1, tag_start_2, end_limit, chunk_size=chunk_size
    )
    pos1 = record.find(tag_start_1)
    pos2 = record.find(tag_start_2)
    expected_pos = pos1 if (pos1 != -1 and (pos1 < pos2 or pos2 == -1)) else pos2
    assert pos == expected_pos


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_find_next_opening_tag_pos_split(chunk_size):
    # Simulate an opening tag split across chunk boundaries.
    record = b"prefix text <ro" + b"w attr='value'> extra"
    file_obj = io.BytesIO(record)
    tag_start_1 = b"<row>"
    tag_start_2 = b"<row "
    end_limit = len(record)
    # Use a small chunk size to force a split.
    pos = find_next_opening_tag_pos(
        file_obj, tag_start_1, tag_start_2, end_limit, chunk_size=chunk_size
    )
    expected_pos = record.find(b"<row ")
    assert pos == expected_pos


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_find_next_opening_tag_pos_no_tag(chunk_size):
    # When no opening tag is found within end_limit, an EOFError should be raised.
    record = b"this text does not contain the tag"
    file_obj = io.BytesIO(record)
    tag_start_1 = b"<row>"
    tag_start_2 = b"<row "
    end_limit = len(record)
    with pytest.raises(EOFError):
        find_next_opening_tag_pos(
            file_obj, tag_start_1, tag_start_2, end_limit, chunk_size=chunk_size
        )


@pytest.mark.parametrize("charset", ["utf-8", "iso-8859-1", "ascii"])
def test_process_xml_range_charset(charset):
    """Test that process_xml_range handles different character encodings correctly."""
    # Create test XML content with special characters
    if charset == "utf-8":
        xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<root><record>Café</record><record>Naïve</record></root>'
        text_values = ["Café", "Naïve"]
    elif charset == "iso-8859-1":
        xml_content = '<?xml version="1.0" encoding="ISO-8859-1"?>\n<root><record>Café</record><record>résumé</record></root>'
        text_values = ["Café", "résumé"]
    else:  # ascii
        xml_content = '<?xml version="1.0" encoding="ASCII"?>\n<root><record>test</record><record>data</record></root>'
        text_values = ["test", "data"]

    # Write XML content to a temporary file with the specified encoding
    with tempfile.NamedTemporaryFile(
        mode="w", encoding=charset, delete=False, suffix=".xml"
    ) as f:
        f.write(xml_content)
        temp_file_path = f.name

    try:
        # Mock file operations for testing - create a BytesIO with the encoded content
        xml_bytes = xml_content.encode(charset)

        # Mock SnowflakeFile.open to return our test data
        mock_file = io.BytesIO(xml_bytes)
        with patch(
            "snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file
        ):
            # Process the XML with the specified charset
            results = _records_only(
                process_xml_range(
                    file_path="test.xml",
                    tag_name="record",
                    approx_start=0,
                    approx_end=len(xml_bytes),
                    mode="PERMISSIVE",
                    column_name_of_corrupt_record="_corrupt_record",
                    ignore_namespace=True,
                    attribute_prefix="_",
                    exclude_attributes=False,
                    value_tag="_VALUE",
                    null_value="",
                    charset=charset,
                    ignore_surrounding_whitespace=True,
                    row_validation_xsd_path="",
                )
            )

        # Verify that the records were parsed correctly with the right charset
        assert len(results) == 2
        for i, result in enumerate(results):
            assert result == {"_VALUE": text_values[i]}

    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def test_process_xml_range_charset_decode_error():
    """Test that process_xml_range handles encoding errors gracefully with errors='replace'."""
    from unittest.mock import patch

    # Create XML content with UTF-8 characters but try to decode as ASCII
    xml_content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n<root><record>Café</record></root>'
    )
    xml_bytes = xml_content.encode("utf-8")

    mock_file = io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        # Process the XML with ASCII charset (should use errors='replace')
        results = _records_only(
            process_xml_range(
                file_path="test.xml",
                tag_name="record",
                approx_start=0,
                approx_end=len(xml_bytes),
                mode="PERMISSIVE",
                column_name_of_corrupt_record="_corrupt_record",
                ignore_namespace=True,
                attribute_prefix="_",
                exclude_attributes=False,
                value_tag="_VALUE",
                null_value="",
                charset="ascii",  # This will cause decode errors
                ignore_surrounding_whitespace=True,
                row_validation_xsd_path="",
            )
        )

    # Should still get a result, but with replacement characters
    assert len(results) == 1
    # The replacement character () should be present in the decoded text
    assert "Caf" in str(results[0])  # Should get "Caf" or similar


@pytest.mark.parametrize(
    "user_schema, expected_result_template, expected_result",
    [
        (
            StructType(  # matched schema
                [
                    StructField("author", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("genre", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("publish_date", DateType(), True),
                    StructField("description", StringType(), True),
                ]
            ),
            {
                "AUTHOR": None,
                "TITLE": None,
                "GENRE": None,
                "PRICE": None,
                "PUBLISH_DATE": None,
                "DESCRIPTION": None,
            },
            {
                "AUTHOR": "Corets, Eva",
                "TITLE": "Oberon's Legacy",
                "GENRE": "Fantasy",
                "PRICE": "5.95",
                "PUBLISH_DATE": "2001-03-10",
                "DESCRIPTION": "In post-apocalypse England, the mysterious\n          agent known only as Oberon helps to create a new life\n          for the inhabitants of London. Sequel to Maeve\n          Ascendant.",
            },
        ),
        (
            StructType(  # schema with extra column
                [
                    StructField("author", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("genre", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("publish_date", DateType(), True),
                    StructField("description", StringType(), True),
                    StructField("extra_col", StringType(), True),
                ]
            ),
            {
                "AUTHOR": None,
                "TITLE": None,
                "GENRE": None,
                "PRICE": None,
                "PUBLISH_DATE": None,
                "DESCRIPTION": None,
                "EXTRA_COL": None,
            },
            {
                "AUTHOR": "Corets, Eva",
                "TITLE": "Oberon's Legacy",
                "GENRE": "Fantasy",
                "PRICE": "5.95",
                "PUBLISH_DATE": "2001-03-10",
                "DESCRIPTION": "In post-apocalypse England, the mysterious\n          agent known only as Oberon helps to create a new life\n          for the inhabitants of London. Sequel to Maeve\n          Ascendant.",
                "EXTRA_COL": None,
            },
        ),
        (
            StructType(  # schema with less column
                [
                    StructField("author", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("genre", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("publish_date", DateType(), True),
                ]
            ),
            {
                "AUTHOR": None,
                "TITLE": None,
                "GENRE": None,
                "PRICE": None,
                "PUBLISH_DATE": None,
            },
            {
                "AUTHOR": "Corets, Eva",
                "TITLE": "Oberon's Legacy",
                "GENRE": "Fantasy",
                "PRICE": "5.95",
                "PUBLISH_DATE": "2001-03-10",
            },
        ),
    ],
)
def test_flat_xml_custom_schema(user_schema, expected_result_template, expected_result):
    xml_string = """
        <book id="bk104">
          <author>Corets, Eva</author>
          <title>Oberon's Legacy</title>
          <genre>Fantasy</genre>
          <price>5.95</price>
          <publish_date>2001-03-10</publish_date>
          <description>In post-apocalypse England, the mysterious
          agent known only as Oberon helps to create a new life
          for the inhabitants of London. Sequel to Maeve
          Ascendant.</description>
       </book>
        """
    result_template = struct_type_to_result_template(user_schema)
    assert result_template == expected_result_template

    element = ET.fromstring(xml_string)
    res = element_to_dict_or_str(element, result_template=result_template)
    assert res == expected_result


def test_nested_xml_custom_schema():
    xml_string = """
  <book id="1">
    <title>The Art of Snowflake</title>
    <author>Jane Doe</author>
    <price>29.99</price>
    <reviews>
      <review>
        <user>tech_guru_87</user>
        <rating>5</rating>
        <comment>Very insightful and practical.</comment>
      </review>
      <review>
        <user>datawizard</user>
        <rating>4</rating>
        <comment>Great read for data engineers.</comment>
      </review>
    </reviews>
    <editions>
      <edition year="2023" format="Hardcover"/>
      <edition year="2024" format="eBook"/>
    </editions>
  </book>
        """

    review_schema = StructType(
        [
            StructField('"User"', StringType(), True),
            StructField(
                '"Rating"', StringType(), True
            ),  # keep as StringType (XML reader returns strings)
            StructField('"comment"', StringType(), True),
        ]
    )

    edition_schema = StructType(
        [
            StructField("_year", StringType(), True),  # attributes -> prefixed with "_"
            StructField("_format", StringType(), True),
        ]
    )

    user_schema = StructType(
        [
            StructField('"Title"', StringType(), True),
            StructField('"Author"', StringType(), True),
            StructField('"Price"', StringType(), True),
            StructField(
                "reviews",
                StructType(
                    [
                        StructField("review", ArrayType(review_schema), True),
                    ]
                ),
                True,
            ),
            StructField(
                "editions",
                StructType(
                    [
                        StructField("edition", ArrayType(edition_schema), True),
                    ]
                ),
                True,
            ),
        ]
    )

    result_template = struct_type_to_result_template(user_schema)
    assert result_template == {
        "Title": None,
        "Author": None,
        "Price": None,
        "REVIEWS": {"REVIEW": {"User": None, "Rating": None, "comment": None}},
        "EDITIONS": {"EDITION": {"_YEAR": None, "_FORMAT": None}},
    }
    element = ET.fromstring(xml_string)
    res = element_to_dict_or_str(element, result_template=result_template)
    assert res == {
        "Title": "The Art of Snowflake",
        "Author": "Jane Doe",
        "Price": "29.99",
        "REVIEWS": {
            "REVIEW": [
                {
                    "User": "tech_guru_87",
                    "Rating": "5",
                    "comment": "Very insightful and practical.",
                },
                {
                    "User": "datawizard",
                    "Rating": "4",
                    "comment": "Great read for data engineers.",
                },
            ]
        },
        "EDITIONS": {
            "EDITION": [
                {"_YEAR": "2023", "_FORMAT": "Hardcover"},
                {"_YEAR": "2024", "_FORMAT": "eBook"},
            ]
        },
    }


def test_case_sensitive_in_custom_schema():
    xml_string = """
    <book id="bk104">
      <author>Corets, Eva</author>
      <title>Oberon's Legacy</title>
      <genre>Fantasy</genre>
      <price>5.95</price>
      <publish_date>2001-03-10</publish_date>
      <description>In post-apocalypse England, the mysterious
      agent known only as Oberon helps to create a new life
      for the inhabitants of London. Sequel to Maeve
      Ascendant.</description>
   </book>
    """

    user_schema = StructType(
        [
            StructField('"Author"', StringType(), True),
            StructField("title", StringType(), True),
            StructField('"GENRE"', StringType(), True),
            StructField('"Price"', DoubleType(), True),
            StructField('"publish_Date"', DateType(), True),
            StructField('"description"', StringType(), True),
        ]
    )

    result_template = struct_type_to_result_template(user_schema)
    assert result_template == {
        "Author": None,
        "TITLE": None,
        "GENRE": None,
        "Price": None,
        "publish_Date": None,
        "description": None,
    }

    element = ET.fromstring(xml_string)
    res = element_to_dict_or_str(element, result_template=result_template)
    assert res == {
        "Author": "Corets, Eva",
        "TITLE": "Oberon's Legacy",
        "GENRE": "Fantasy",
        "Price": "5.95",
        "publish_Date": "2001-03-10",
        "description": "In post-apocalypse England, the mysterious\n      agent known only as Oberon helps to create a new life\n      for the inhabitants of London. Sequel to Maeve\n      Ascendant.",
    }


def test_attribute_to_schema_string_deep(session):
    review_schema = StructType(
        [
            StructField("User", StringType(), True),
            StructField(
                "Rating", StringType(), True
            ),  # keep as StringType (XML reader returns strings)
            StructField("comment", StringType(), True),
        ]
    )

    edition_schema = StructType(
        [
            StructField("_year", StringType(), True),  # attributes -> prefixed with "_"
            StructField("_format", StringType(), True),
        ]
    )

    user_schema = StructType(
        [
            StructField("Title", StringType(), True),
            StructField("Author", StringType(), True),
            StructField("Price", StringType(), True),
            StructField(
                "reviews",
                StructType(
                    [
                        StructField("review", ArrayType(review_schema), True),
                    ]
                ),
                True,
            ),
            StructField(
                "editions",
                StructType(
                    [
                        StructField("edition", ArrayType(edition_schema), True),
                    ]
                ),
                True,
            ),
        ]
    )
    attr, _, _ = session.read._get_schema_from_user_input(user_schema)
    schema_string = attribute_to_schema_string_deep(attr)
    assert (
        schema_string
        == """struct<"Title": string, "Author": string, "Price": string, "reviews": struct<"review": array<struct<"User": string, "Rating": string, "comment": string>>>, "editions": struct<"edition": array<struct<"_year": string, "_format": string>>>>"""
    )


def test_schema_string_to_result_dict_and_struct_type(session):
    user_schema = StructType(
        [
            StructField("Author", StringType(), True),
            StructField("TITLE", StringType(), True),
            StructField("GENRE", StringType(), True),
            StructField("Price", DoubleType(), True),
            StructField("publish_Date", DateType(), True),
            StructField("description", StringType(), True),
            StructField("map_type", MapType(), True),
        ]
    )
    attr, _, _ = session.read._get_schema_from_user_input(user_schema)
    schema_string = attribute_to_schema_string_deep(attr)
    template, schema_type = schema_string_to_result_dict_and_struct_type(schema_string)
    assert template == {
        "Author": None,
        "TITLE": None,
        "GENRE": None,
        "Price": None,
        "publish_Date": None,
        "description": None,
        "map_type": None,
    }
    assert isinstance(schema_type, StructType)


def test_user_schema_value_tag():
    xml_string = """
    <test>
        <num>1</num>
        <str1>NULL</str1>
        <str2></str2>
        <str3 id="empty">xxx</str3>
    </test>
    """

    user_schema = StructType(
        [
            StructField("num", StringType(), True),
            StructField("str1", StringType(), True),
            StructField("str2", StringType(), True),
            StructField(
                "str3",
                StructType(
                    [
                        StructField(
                            "_VALUE", StringType(), True
                        ),  # element text (because str3 has an attribute)
                        StructField(
                            "_id", StringType(), True
                        ),  # attribute id (default attributePrefix is "_")
                    ]
                ),
                True,
            ),
        ]
    )

    result_template = struct_type_to_result_template(user_schema)

    element = ET.fromstring(xml_string)
    res = element_to_dict_or_str(element, result_template=result_template)
    assert result_template == {
        "NUM": None,
        "STR1": None,
        "STR2": None,
        "STR3": {"_VALUE": None, "_ID": None},
    }
    assert res == {
        "NUM": "1",
        "STR1": "NULL",
        "STR2": None,
        "STR3": {"_VALUE": "xxx", "_ID": "empty"},
    }

    user_schema = StructType(
        [
            StructField("num", StringType(), True),
            StructField("str1", StringType(), True),
            StructField("str2", StringType(), True),
            StructField("str3", StringType(), True),
        ]
    )

    result_template = struct_type_to_result_template(user_schema)

    element = ET.fromstring(xml_string)
    res = element_to_dict_or_str(element, result_template=result_template)
    assert result_template == {"NUM": None, "STR1": None, "STR2": None, "STR3": None}
    assert res == {"NUM": "1", "STR1": "NULL", "STR2": None, "STR3": "xxx"}


def test_escape_colons_in_quotes():
    s = '"px:name": string'
    escaped = _escape_colons_in_quotes(s)
    assert escaped.count(":") == 1
    assert _COLON_PLACEHOLDER in escaped

    s = '"px:name": string, "px:value": string'
    escaped = _escape_colons_in_quotes(s)
    assert escaped.count(":") == 2
    assert escaped.count(_COLON_PLACEHOLDER) == 2

    s = 'struct<"px:name": string, "detail": struct<"px:sub": string>>'
    escaped = _escape_colons_in_quotes(s)
    assert escaped.count(_COLON_PLACEHOLDER) == 2
    assert escaped.count(":") == 3

    s = '"Author": string, "Title": string'
    assert _escape_colons_in_quotes(s) == s
    assert _escape_colons_in_quotes("") == ""
    assert _escape_colons_in_quotes("a: int, b: string") == "a: int, b: string"


def test_restore_colons_in_template():
    assert _restore_colons_in_template(None) is None
    assert _restore_colons_in_template({"Author": None}) == {"Author": None}

    # Flat
    template = {
        f"px{_COLON_PLACEHOLDER}name": None,
        f"px{_COLON_PLACEHOLDER}value": None,
    }
    assert _restore_colons_in_template(template) == {
        "px:name": None,
        "px:value": None,
    }

    # Nested
    template = {
        f"eq{_COLON_PLACEHOLDER}event": {
            f"eq{_COLON_PLACEHOLDER}sub-id": None,
        },
        "plain": None,
    }
    assert _restore_colons_in_template(template) == {
        "eq:event": {"eq:sub-id": None},
        "plain": None,
    }


def test_schema_string_round_trip_with_colons():
    # Flat
    schema_str = 'struct<"px:name": string, "px:value": string>'
    template, _ = schema_string_to_result_dict_and_struct_type(schema_str)
    assert template == {
        "px:name": None,
        "px:value": None,
    }

    # Nested
    schema_str = (
        'struct<"eq:event-id": string,' '"eq:detail": struct<"eq:sub-id": string>>'
    )
    template, _ = schema_string_to_result_dict_and_struct_type(schema_str)
    assert template == {
        "eq:event-id": None,
        "eq:detail": {"eq:sub-id": None},
    }

    # Mixed
    schema_str = 'struct<"px:name": string, "Title": string, price: double>'
    template, _ = schema_string_to_result_dict_and_struct_type(schema_str)
    assert template == {
        "px:name": None,
        "Title": None,
        "PRICE": None,
    }

    # No colon fields
    schema_str = 'struct<"Author": string, "TITLE": string>'
    template, _ = schema_string_to_result_dict_and_struct_type(schema_str)
    assert template == {
        "Author": None,
        "TITLE": None,
    }


@pytest.mark.parametrize(
    "value, target_type, expected",
    [
        # StringType always passes
        ("anything", StringType(), True),
        ("", StringType(), True),
        # LongType
        ("42", LongType(), True),
        ("-7", LongType(), True),
        ("0", LongType(), True),
        ("3.14", LongType(), False),
        ("hello", LongType(), False),
        ("", LongType(), False),
        # DoubleType
        ("3.14", DoubleType(), True),
        ("-0.5", DoubleType(), True),
        ("42", DoubleType(), True),
        ("NaN", DoubleType(), True),  # Python float("NaN") succeeds
        ("hello", DoubleType(), False),
        # BooleanType
        ("true", BooleanType(), True),
        ("false", BooleanType(), True),
        ("True", BooleanType(), True),
        ("1", BooleanType(), True),
        ("0", BooleanType(), True),
        ("yes", BooleanType(), False),
        ("maybe", BooleanType(), False),
        # DateType
        ("2024-01-15", DateType(), True),
        ("not-a-date", DateType(), False),
        ("2024-13-01", DateType(), False),
        # TimestampType
        ("2024-01-15T10:30:00", TimestampType(), True),
        ("2024-01-15", TimestampType(), True),
        ("not-a-ts", TimestampType(), False),
    ],
)
def test_can_cast_to_type(value, target_type, expected):
    assert _can_cast_to_type(value, target_type) == expected


# ---------------------------------------------------------------------------
# _validate_row_for_type_mismatch tests
# ---------------------------------------------------------------------------


def _make_schema(*fields):
    """Helper to build a StructType from (name, datatype) tuples."""
    return StructType([StructField(f'"{n}"', t) for n, t in fields])


def test_validate_permissive_all_valid_no_corrupt():
    schema = _make_schema(("name", StringType()), ("age", LongType()))
    row = {"name": "Alice", "age": "30"}
    result = _validate_row_for_type_mismatch(row, schema, "PERMISSIVE", "<row/>")
    assert result["name"] == "Alice"
    assert result["age"] == "30"
    assert "_corrupt_record" not in result


def test_validate_permissive_single_field_nulled():
    schema = _make_schema(("name", StringType()), ("age", LongType()))
    row = {"name": "Bob", "age": "hello"}
    result = _validate_row_for_type_mismatch(row, schema, "PERMISSIVE", "<row/>")
    assert result["name"] == "Bob"
    assert result["age"] is None
    assert result["_corrupt_record"] == "<row/>"


def test_validate_permissive_multiple_fields_nulled():
    schema = _make_schema(("a", LongType()), ("b", BooleanType()), ("c", DoubleType()))
    row = {"a": "not_int", "b": "maybe", "c": "1.5"}
    result = _validate_row_for_type_mismatch(row, schema, "PERMISSIVE", "<r/>")
    assert result["a"] is None
    assert result["b"] is None
    assert result["c"] == "1.5"
    assert result["_corrupt_record"] == "<r/>"


def test_validate_permissive_missing_field_ignored():
    schema = _make_schema(("name", StringType()), ("age", LongType()))
    row = {"name": "Carol"}
    result = _validate_row_for_type_mismatch(row, schema, "PERMISSIVE", "<row/>")
    assert result["name"] == "Carol"
    assert "_corrupt_record" not in result


def test_validate_permissive_none_value_ignored():
    schema = _make_schema(("val", LongType()))
    row = {"val": None}
    result = _validate_row_for_type_mismatch(row, schema, "PERMISSIVE", "<row/>")
    assert result["val"] is None
    assert "_corrupt_record" not in result


def test_validate_permissive_complex_type_skipped():
    schema = _make_schema(
        ("data", StructType([StructField("x", StringType())])),
        ("tags", ArrayType(StringType())),
    )
    row = {"data": {"x": "val"}, "tags": ["a", "b"]}
    result = _validate_row_for_type_mismatch(row, schema, "PERMISSIVE", "<row/>")
    assert result["data"] == {"x": "val"}
    assert result["tags"] == ["a", "b"]
    assert "_corrupt_record" not in result


def test_validate_permissive_custom_corrupt_col_name():
    schema = _make_schema(("val", LongType()))
    row = {"val": "bad"}
    result = _validate_row_for_type_mismatch(
        row, schema, "PERMISSIVE", "<r/>", column_name_of_corrupt_record="bad_rec"
    )
    assert result["val"] is None
    assert result["bad_rec"] == "<r/>"
    assert "_corrupt_record" not in result


def test_validate_failfast_raises_on_mismatch():
    schema = _make_schema(("val", LongType()))
    row = {"val": "hello"}
    with pytest.raises(RuntimeError, match="Failed to cast value 'hello'"):
        _validate_row_for_type_mismatch(row, schema, "FAILFAST", "<row/>")


def test_validate_failfast_no_error_when_valid():
    schema = _make_schema(("val", LongType()))
    row = {"val": "42"}
    result = _validate_row_for_type_mismatch(row, schema, "FAILFAST", "<row/>")
    assert result["val"] == "42"


def test_validate_dropmalformed_returns_none_on_mismatch():
    schema = _make_schema(("val", LongType()))
    row = {"val": "hello"}
    assert (
        _validate_row_for_type_mismatch(row, schema, "DROPMALFORMED", "<row/>") is None
    )


def test_validate_dropmalformed_returns_row_when_valid():
    schema = _make_schema(("val", LongType()))
    row = {"val": "42"}
    result = _validate_row_for_type_mismatch(row, schema, "DROPMALFORMED", "<row/>")
    assert result["val"] == "42"


def test_schema_string_to_result_dict_empty_string():
    template, schema_type = schema_string_to_result_dict_and_struct_type("")
    assert template is None
    assert schema_type is None


def test_can_cast_to_complex_type_returns_true():
    assert _can_cast_to_type("anything", ArrayType(StringType())) is True
    assert _can_cast_to_type("anything", MapType(StringType(), StringType())) is True
    assert _can_cast_to_type("anything", StructType([])) is True


def test_element_to_dict_leaf_with_template_edge_cases():
    xml_str = "<publisher/>"
    element = ET.fromstring(xml_str)
    result_template = {"_VALUE": None, "_country": None, "_language": None}
    result = element_to_dict_or_str(element, result_template=result_template)
    assert isinstance(result, dict)
    assert result["_VALUE"] is None
    assert result["_country"] is None
    assert result["_language"] is None

    xml_str = "<publisher>Penguin</publisher>"
    element = ET.fromstring(xml_str)
    result = element_to_dict_or_str(element, result_template=result_template)
    assert isinstance(result, dict)
    assert result["_VALUE"] == "Penguin"
    assert result["_country"] is None
    assert result["_language"] is None

    xml_str = "<publisher>N/A</publisher>"
    element = ET.fromstring(xml_str)
    result_template = {"_VALUE": None, "_country": None}
    result = element_to_dict_or_str(
        element, result_template=result_template, null_value="N/A"
    )
    assert isinstance(result, dict)
    assert result["_VALUE"] is None


def test_process_xml_range_scos_permissive_type_validation():
    """process_xml_range validates types when is_snowpark_connect_compatible=True"""
    xml_content = (
        "<root>"
        "<ROW><name>Alice</name><value>100</value></ROW>"
        "<ROW><name>Frank</name><value>hello</value></ROW>"
        "</root>"
    )
    xml_bytes = xml_content.encode("utf-8")
    schema = _make_schema(("name", StringType()), ("value", LongType()))
    mock_file = io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        results = _records_only(
            process_xml_range(
                file_path="test.xml",
                tag_name="ROW",
                approx_start=0,
                approx_end=len(xml_bytes),
                mode="PERMISSIVE",
                column_name_of_corrupt_record="_corrupt_record",
                ignore_namespace=True,
                attribute_prefix="_",
                exclude_attributes=False,
                value_tag="_VALUE",
                null_value="",
                charset="utf-8",
                ignore_surrounding_whitespace=True,
                row_validation_xsd_path="",
                result_template={"name": None, "value": None},
                schema_type=schema,
                is_snowpark_connect_compatible=True,
            )
        )
    assert len(results) == 2
    frank = [r for r in results if r.get("name") == "Frank"][0]
    assert frank["value"] is None
    assert "_corrupt_record" in frank


def test_process_xml_range_scos_dropmalformed_type_validation_skips_row():
    """When is_snowpark_connect_compatible=True and DROPMALFORMED encounters a value
    that can't cast to the schema type, _validate_row_for_type_mismatch returns None --
    process_xml_range must skip yielding that row entirely, not yield None."""
    xml_content = (
        "<root>"
        "<ROW><name>Alice</name><value>100</value></ROW>"
        "<ROW><name>Frank</name><value>hello</value></ROW>"
        "</root>"
    )
    xml_bytes = xml_content.encode("utf-8")
    schema = _make_schema(("name", StringType()), ("value", LongType()))
    mock_file = io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        results = _records_only(
            process_xml_range(
                file_path="test.xml",
                tag_name="ROW",
                approx_start=0,
                approx_end=len(xml_bytes),
                mode="DROPMALFORMED",
                column_name_of_corrupt_record="_corrupt_record",
                ignore_namespace=True,
                attribute_prefix="_",
                exclude_attributes=False,
                value_tag="_VALUE",
                null_value="",
                charset="utf-8",
                ignore_surrounding_whitespace=True,
                row_validation_xsd_path="",
                result_template={"name": None, "value": None},
                schema_type=schema,
                is_snowpark_connect_compatible=True,
            )
        )
    assert len(results) == 1
    assert results[0]["name"] == "Alice"


def test_xml_reader_process_with_scos_compatible_param():
    """XMLReader.process passes is_snowpark_connect_compatible through"""
    xml_content = "<root><record><a>1</a></record></root>"
    xml_bytes = xml_content.encode("utf-8")
    mock_file = io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        results = list(
            XMLReader().process(
                "test.xml",
                0,
                len(xml_bytes),
                "record",
                "PERMISSIVE",
                "_corrupt_record",
                True,
                "_",
                False,
                "_VALUE",
                "",
                "utf-8",
                True,
                "",
                "",
                True,
                DEFAULT_CHUNK_SIZE,
                False,
            )
        )
    assert len(results) == 1
    assert results[0][0]["a"] == "1"


#
# Output projection: direct VARIANT key projection instead of flatten + dynamic pivot.
#


def _render(column) -> str:
    """Render a Column to SQL without needing a Snowflake connection.

    A real Analyzer is used rather than a mocked one so the assertions below pin the
    SQL that actually reaches the server, including identifier/literal escaping.
    """
    return Analyzer(mock.MagicMock()).analyze(column._expression, {})


def _reader(**options) -> DataFrameReader:
    reader = DataFrameReader.__new__(DataFrameReader)
    reader._cur_options = {k.upper(): v for k, v in options.items()}
    reader._file_path = "@stage/test.xml"
    return reader


@pytest.mark.parametrize(
    "key,expected",
    [
        ("author", "\"ROW_DATA\"['author'] AS \"'author'\""),
        # A namespace prefix must stay part of the key: ROW_DATA:px:name would parse the
        # colon as a further path segment, so subfield notation is used instead.
        ("px:name", "\"ROW_DATA\"['px:name'] AS \"'px:name'\""),
        (
            "{http://example.com/px}item",
            "\"ROW_DATA\"['{http://example.com/px}item'] "
            "AS \"'{http://example.com/px}item'\"",
        ),
        ("_corrupt_record", "\"ROW_DATA\"['_corrupt_record'] AS \"'_corrupt_record'\""),
        ("_VALUE", "\"ROW_DATA\"['_VALUE'] AS \"'_VALUE'\""),
        # A quote in the key must not terminate the literal or the alias early.
        ("a'b", "\"ROW_DATA\"['a''b'] AS \"'a''b'\""),
        ('a"b', '"ROW_DATA"[\'a"b\'] AS "\'a""b\'"'),
    ],
)
def test_xml_variant_projection_sql(key, expected):
    assert _render(xml_variant_projection(key)) == expected


def test_xml_variant_projection_alias_matches_pivot_naming():
    """The alias must keep the single-quoted names dynamic pivot produced, since
    _apply_xml_schema looks columns up via single_quote(field name)."""
    assert _render(xml_variant_projection("author")).endswith(
        f"AS {quote_name(single_quote('author'))}"
    )


@pytest.mark.parametrize(
    "options,schema_known,expected",
    [
        # Unset: an unconfigured read keeps the flatten+pivot output it always produced,
        # whatever else is set.
        ({}, True, False),
        ({}, False, False),
        ({"CACHERESULT": True}, True, False),
        ({"CACHERESULT": False}, False, False),
        ({"MODE": "FAILFAST"}, True, False),
        # Explicitly off is the same as unset.
        ({"USEVARIANTPROJECTION": False}, True, False),
        ({"USEVARIANTPROJECTION": False}, False, False),
        # Opted in with cacheResult at its default: applies whether or not a schema is known.
        ({"USEVARIANTPROJECTION": True}, True, True),
        ({"USEVARIANTPROJECTION": True}, False, True),
        ({"USEVARIANTPROJECTION": True, "CACHERESULT": True}, True, True),
        # Opted in, but cacheResult=False leaves nothing materialized: no keys to discover,
        # and no cheap way to test for corrupt records, so flatten+pivot keeps .xml() lazy.
        ({"USEVARIANTPROJECTION": True, "CACHERESULT": False}, False, False),
        ({"USEVARIANTPROJECTION": True, "CACHERESULT": False}, True, False),
        # ... except when no corrupt-record probe is needed, i.e. outside PERMISSIVE mode.
        (
            {"USEVARIANTPROJECTION": True, "CACHERESULT": False, "MODE": "FAILFAST"},
            True,
            True,
        ),
        (
            {
                "USEVARIANTPROJECTION": True,
                "CACHERESULT": False,
                "MODE": "DROPMALFORMED",
            },
            True,
            True,
        ),
        (
            {"USEVARIANTPROJECTION": True, "CACHERESULT": False, "MODE": "failfast"},
            True,
            True,
        ),
        # Without a schema there are still no keys to discover, whatever the mode.
        (
            {"USEVARIANTPROJECTION": True, "CACHERESULT": False, "MODE": "FAILFAST"},
            False,
            False,
        ),
    ],
)
def test_use_xml_variant_projection(options, schema_known, expected):
    assert use_xml_variant_projection(options, schema_known) is expected


def _project_from_keys(keys, **options):
    """Drive _xml_project_from_variant_cache over a DataFrame whose key discovery
    returns *keys*, and return (rendered projections, projected DataFrame)."""
    discovery = mock.MagicMock()
    discovery.select.return_value.distinct.return_value.collect.return_value = [
        (k,) for k in keys
    ]
    projected = mock.MagicMock()
    df = mock.MagicMock()
    df.select.side_effect = [discovery, projected]

    result = _reader(rowtag="record", **options)._xml_project_from_variant_cache(df)

    assert result is projected
    projections = [_render(c) for c in df.select.call_args_list[1].args]
    return projections, projected


def test_xml_project_from_variant_cache_projects_discovered_keys():
    projections, projected = _project_from_keys(["title", "_id", "author"])
    # Discovered keys are projected in sorted order for a deterministic column order.
    assert projections == [
        "\"ROW_DATA\"['_id'] AS \"'_id'\"",
        "\"ROW_DATA\"['author'] AS \"'author'\"",
        "\"ROW_DATA\"['title'] AS \"'title'\"",
    ]
    # Preserved so dot-notation (df.select("'nested'.child")) keeps working.
    assert projected._all_variant_cols is True


def test_xml_project_from_variant_cache_deduplicates_and_sorts():
    projections, _ = _project_from_keys(["b", "a", "b"])
    assert projections == [
        "\"ROW_DATA\"['a'] AS \"'a'\"",
        "\"ROW_DATA\"['b'] AS \"'b'\"",
        "\"ROW_DATA\"['b'] AS \"'b'\"",
    ]


def test_xml_project_from_variant_cache_projects_namespaced_key():
    projections, _ = _project_from_keys(["px:name"])
    assert projections == ["\"ROW_DATA\"['px:name'] AS \"'px:name'\""]


def test_xml_project_from_variant_cache_projects_corrupt_record_when_discovered():
    """Without a schema the corrupt-record column needs no gating: the reader only
    writes that key for records that fail to parse, so discovering it at all means
    the data really contains a corrupt record."""
    projections, _ = _project_from_keys(["id", "_corrupt_record"])
    assert "\"ROW_DATA\"['_corrupt_record'] AS \"'_corrupt_record'\"" in projections


def test_xml_project_from_variant_cache_omits_corrupt_record_when_absent():
    projections, _ = _project_from_keys(["id", "name"])
    assert not any("_corrupt_record" in p for p in projections)


def test_xml_project_from_variant_cache_no_keys_raises_row_tag_not_found():
    """An empty result means the row tag matched nothing. Under pivot this surfaced as
    a "SELECT with no columns" SQL error that was translated into this exception."""
    with pytest.raises(
        SnowparkDataframeReaderException, match="Cannot find the row tag"
    ):
        _project_from_keys([])


#
# Malformed records: FAILFAST must raise on every EOF path, not just PERMISSIVE.
#


def test_process_xml_range_raises_in_failfast_mode_when_opening_tag_is_unclosed():
    """FAILFAST must raise, not just PERMISSIVE, when the row tag's own opening tag is
    truncated before EOF with no ">" ever found."""
    xml_bytes = b'<ROOT><PARENT id="p1"'
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        with pytest.raises(EOFError, match="Malformed XML record at bytes"):
            list(
                process_xml_range(
                    "test.xml",
                    "PARENT",
                    0,
                    len(xml_bytes),
                    "FAILFAST",
                    "_corrupt_record",
                    True,
                    "_",
                    False,
                    "_VALUE",
                    "",
                    "utf-8",
                    False,
                    "",
                )
            )


def test_process_xml_range_raises_in_failfast_mode_when_closing_tag_is_missing():
    """FAILFAST must raise, not just PERMISSIVE, when a non-self-closing tag's closing
    tag is never found before EOF."""
    xml_bytes = b'<ROOT><PARENT id="p1">some content with no closing tag'
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        with pytest.raises(EOFError, match="Malformed XML record at bytes"):
            list(
                process_xml_range(
                    "test.xml",
                    "PARENT",
                    0,
                    len(xml_bytes),
                    "FAILFAST",
                    "_corrupt_record",
                    True,
                    "_",
                    False,
                    "_VALUE",
                    "",
                    "utf-8",
                    False,
                    "",
                )
            )


def test_process_xml_range_permissive_mode_yields_corrupt_record_when_tag_is_unclosed():
    """Known gap (tracked separately, not fixed here): the file cursor is already at EOF
    when tag_is_self_closing raises, and this branch never seeks back to record_start
    before reading, so the corrupt-record column ends up empty instead of containing the
    malformed bytes. Asserts today's actual behavior so a future fix updates this test
    deliberately rather than it silently passing either way."""
    xml_bytes = b'<ROOT><PARENT id="p1"'
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        records = _records_only(
            process_xml_range(
                "test.xml",
                "PARENT",
                0,
                len(xml_bytes),
                "PERMISSIVE",
                "_corrupt_record",
                True,
                "_",
                False,
                "_VALUE",
                "",
                "utf-8",
                False,
                "",
            )
        )
    assert records == [{"_corrupt_record": ""}]


def test_process_xml_range_permissive_mode_yields_corrupt_record_when_closing_tag_is_missing():
    """Known gap (tracked separately, not fixed here): same missing-seek issue as
    test_process_xml_range_permissive_mode_yields_corrupt_record_when_tag_is_unclosed,
    but for find_next_closing_tag_pos's EOF path instead of tag_is_self_closing's."""
    xml_bytes = b'<ROOT><PARENT id="p1">some content with no closing tag'
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        records = _records_only(
            process_xml_range(
                "test.xml",
                "PARENT",
                0,
                len(xml_bytes),
                "PERMISSIVE",
                "_corrupt_record",
                True,
                "_",
                False,
                "_VALUE",
                "",
                "utf-8",
                False,
                "",
            )
        )
    assert records == [{"_corrupt_record": ""}]


def test_process_xml_range_permissive_mode_yields_corrupt_record_on_parse_error():
    """A record whose closing tag is found correctly but whose inner content is not
    well-formed XML raises ET.ParseError during parsing; PERMISSIVE must yield it as a
    corrupt record rather than dropping it or crashing."""
    xml_bytes = (
        b"<root>"
        b"<record><id>41</id><name>Ann</name></record>"
        b"<record><id>42</id><name>Bob</name></email></record>"
        b"</root>"
    )
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        records = _records_only(
            process_xml_range(
                "test.xml",
                "record",
                0,
                len(xml_bytes),
                "PERMISSIVE",
                "_corrupt_record",
                True,
                "_",
                False,
                "_VALUE",
                "",
                "utf-8",
                False,
                "",
            )
        )
    assert records == [
        {"id": "41", "name": "Ann"},
        {"_corrupt_record": "<record><id>42</id><name>Bob</name></email></record>"},
    ]


#
# Byte-range worker assignment: explicit (FILE_PATH, APPROX_START, APPROX_END) ranges.
#


def _ranges(file_size, max_workers=DEFAULT_MAX_WORKERS, chunk_size=DEFAULT_CHUNK_SIZE):
    return [
        (start, end)
        for _, start, end in _xml_worker_assignments(
            "@stage/f.xml", file_size, max_workers, chunk_size
        )
    ]


@pytest.mark.parametrize(
    "file_size,max_workers,chunk_size,expected_count",
    [
        # Defaults reproduce the historical split: min(16, size // 1024 + 1).
        (5533, 16, 1024, 6),
        (1023, 16, 1024, 1),
        (1024, 16, 1024, 2),
        (0, 16, 1024, 1),
        # numWorkers caps the count once the file is large enough to exceed it.
        (10**6, 16, 1024, 16),
        (10**6, 32, 1024, 32),
        (10**6, 1, 1024, 1),
        # chunkSize drives how many ways the file is divided. These are the cases that
        # regress if the boundary math reverts to the fixed DEFAULT_CHUNK_SIZE constant:
        # every one of them would collapse to the chunk_size=1024 count instead.
        (5533, 16, 4096, 2),
        (5533, 16, 65536, 1),
        (10**6, 16, 65536, 16),
        (10**6, 16, 10**6, 2),
        (10**6, 16, 10**7, 1),
    ],
)
def test_xml_worker_assignments_count(
    file_size, max_workers, chunk_size, expected_count
):
    assert len(_ranges(file_size, max_workers, chunk_size)) == expected_count


def test_xml_worker_assignments_chunk_size_is_not_hardcoded():
    """chunkSize must change the work split, not just the UDTF's read buffer size.

    The POC passed chunkSize to the UDTF but computed worker boundaries from the fixed
    1024-byte constant, so raising chunkSize left the split unchanged.
    """
    file_size = 10**6
    counts = {
        chunk_size: len(_ranges(file_size, 16, chunk_size))
        for chunk_size in (1024, 65536, 10**6)
    }
    assert counts == {1024: 16, 65536: 16, 10**6: 2}
    # A chunk size at or above the file size means a single worker reads the whole file.
    assert _ranges(file_size, 16, 10**7) == [(0, file_size)]


@pytest.mark.parametrize(
    "file_size,max_workers,chunk_size",
    [
        (5533, 16, 1024),
        (10**6, 16, 1024),
        (10**6, 32, 4096),
        (10**6, 7, 1024),
        (10**9 + 7, 16, 1024),
        (1023, 16, 1024),
        (0, 16, 1024),
    ],
)
def test_xml_worker_assignments_cover_file_exactly(file_size, max_workers, chunk_size):
    """Ranges must be contiguous, non-overlapping, and cover [0, file_size) exactly --
    otherwise records would be dropped or read twice."""
    ranges = _ranges(file_size, max_workers, chunk_size)
    assert ranges[0][0] == 0
    assert ranges[-1][1] == file_size
    for (_, prev_end), (next_start, _) in zip(ranges, ranges[1:]):
        assert prev_end == next_start


def test_xml_worker_assignments_carries_file_path():
    assignments = _xml_worker_assignments("@stage/dir/books.xml", 5533, 16, 1024)
    assert {path for path, _, _ in assignments} == {"@stage/dir/books.xml"}


@pytest.mark.parametrize("num_workers", [1, 2, 3, 5, 16])
def test_process_xml_range_does_not_split_records_across_boundaries(num_workers):
    """Reading a file as N byte ranges must yield exactly the same records as reading it
    in one range -- no record dropped at a boundary, none read twice."""
    records = "".join(
        f"<record><id>{i}</id><name>name-{i}</name></record>" for i in range(40)
    )
    xml_bytes = f"<root>{records}</root>".encode()

    def read_range(approx_start, approx_end):
        with patch(
            "snowflake.snowpark.files.SnowflakeFile.open",
            side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
        ):
            return _records_only(
                process_xml_range(
                    "test.xml",
                    "record",
                    approx_start,
                    approx_end,
                    "PERMISSIVE",
                    "_corrupt_record",
                    True,
                    "_",
                    False,
                    "_VALUE",
                    "",
                    "utf-8",
                    False,
                    "",
                )
            )

    whole = read_range(0, len(xml_bytes))
    assert len(whole) == 40

    split = []
    for _, start, end in _xml_worker_assignments(
        "test.xml", len(xml_bytes), num_workers, 1
    ):
        split.extend(read_range(start, end))

    assert [r["id"] for r in split] == [r["id"] for r in whole]


@pytest.mark.parametrize(
    "options,expected",
    [
        ({}, 16),
        ({"NUMWORKERS": 4}, 4),
        ({"NUMWORKERS": "4"}, 4),
        ({"NUMWORKERS": 1}, 1),
    ],
)
def test_positive_int_option_accepts_valid_values(options, expected):
    assert _positive_int_option(options, "NUMWORKERS", 16) == expected


@pytest.mark.parametrize("value", [0, -1, "not-a-number"])
def test_positive_int_option_rejects_degenerate_values(value):
    with pytest.raises(ValueError, match="Must be a positive integer"):
        _positive_int_option({"NUMWORKERS": value}, "NUMWORKERS", 16)


#
# Single-file reads: stage listing basename resolution and per-file size lookup.
#


@pytest.mark.parametrize(
    "name,expected",
    [
        # Internal stage: LIST drops the db/schema qualifier and lowercases the
        # stage-name segment, but the basename is just the final path segment either way.
        ("stage/dir/f.xml", "f.xml"),
        ("mystage/SubDir/File.xml", "File.xml"),
        # Named external stage: LIST reports the underlying cloud URL, unrelated to the
        # stage's own name -- the basename is still the final segment.
        ("s3://bucket/prefix/security_master_1.xml", "security_master_1.xml"),
        ("azure://account.blob.core.windows.net/container/dir/f.xml", "f.xml"),
        ("gcs://bucket/dir/f.xml", "f.xml"),
        # A bare filename (no directory) is its own basename.
        ("f.xml", "f.xml"),
    ],
)
def test_stage_listing_basename(name, expected):
    assert _stage_listing_basename(name) == expected


def _fake_builder(listing):
    """A SnowflakePlanBuilder whose session records `ls` targets and replays `listing`."""
    builder = SnowflakePlanBuilder.__new__(SnowflakePlanBuilder)
    listed = []

    def fake_sql(sql, **kwargs):
        listed.append(sql)
        target = sql[len("ls ") :]
        result = mock.MagicMock()
        result.collect.return_value = listing.get(target, [])
        return result

    builder.session = mock.MagicMock()
    builder.session.sql.side_effect = fake_sql
    return builder, listed


def test_resolve_stage_file_size_lists_by_exact_path():
    """A single-file read is listed by the file's own path, not its directory, so it
    never has to enumerate a directory that may hold far more."""
    listing = {"@stage/dir/a.xml": [{"name": "stage/dir/a.xml", "size": 10}]}
    builder, listed = _fake_builder(listing)
    assert builder._resolve_stage_file_size("@stage/dir/a.xml") == 10
    assert listed == ["ls @stage/dir/a.xml"]


def test_resolve_stage_file_size_matches_mixed_case_stage_listing():
    """A mixed-case stage name resolves against LIST's lowercased form -- matching by
    basename means the stage-name segment's case never has to be reconstructed at all."""
    listing = {
        "@db.schema.MyStage/SubDir/a.xml": [
            {"name": "mystage/SubDir/a.xml", "size": 11},
        ]
    }
    builder, _ = _fake_builder(listing)
    assert builder._resolve_stage_file_size("@db.schema.MyStage/SubDir/a.xml") == 11


def test_resolve_stage_file_size_matches_named_external_stage_listing():
    """A named external stage's LIST reports the underlying cloud URL, not
    ``stage/path`` -- this is the bug this fix exists for: reconstructing an expected
    listing name from the stage identifier missed every external-stage file, since the
    URL has no relation to the stage's own name."""
    listing = {
        "@db.schema.ext_stage/dir/security_master_1.xml": [
            {
                "name": "s3://ecosystem-sas/test_files_xml/security_master_1.xml",
                "size": 123,
            },
        ]
    }
    builder, _ = _fake_builder(listing)
    assert (
        builder._resolve_stage_file_size(
            "@db.schema.ext_stage/dir/security_master_1.xml"
        )
        == 123
    )


def test_resolve_stage_file_size_missing_file_raises():
    listing = {"@stage/dir/does_not_exist.xml": []}
    builder, _ = _fake_builder(listing)
    with pytest.raises(ValueError, match="does not exist"):
        builder._resolve_stage_file_size("@stage/dir/does_not_exist.xml")


#
# readDirectory: per-directory size lookup for every file under a stage directory.
#


def test_list_directory_file_sizes_lists_once_for_every_file():
    """A directory read must cost one LIST for every file in it, not one apiece -- a
    LIST per file would be a round trip per file before the read even starts."""
    listing = {
        "@stage/dir/": [
            {"name": "stage/dir/a.xml", "size": 10},
            {"name": "stage/dir/b.xml", "size": 20},
            {"name": "stage/dir/c.xml", "size": 30},
        ],
    }
    builder, listed = _fake_builder(listing)
    sizes = builder._list_directory_file_sizes("@stage/dir")
    assert sizes == {
        "@stage/dir/a.xml": 10,
        "@stage/dir/b.xml": 20,
        "@stage/dir/c.xml": 30,
    }
    assert listed == ["ls @stage/dir/"]


def test_list_directory_file_sizes_lists_with_trailing_slash_not_bare_prefix():
    """LIST does raw string-prefix matching, not directory-boundary matching: listing a
    bare directory name would also match a sibling directory sharing that prefix (e.g.
    "orders" matching "orders_archive"), and since the sibling sits at the same path
    depth, the depth filter can't catch it either. Listing must target the directory
    with a trailing slash so the prefix matches the actual boundary."""
    listing = {
        # A bare, no-trailing-slash listing would (incorrectly) return this too.
        "@stage/orders": [
            {"name": "stage/orders/a.xml", "size": 10},
            {"name": "stage/orders_archive/c.xml", "size": 999},
        ],
        "@stage/orders/": [
            {"name": "stage/orders/a.xml", "size": 10},
        ],
    }
    builder, listed = _fake_builder(listing)
    sizes = builder._list_directory_file_sizes("@stage/orders")
    assert sizes == {"@stage/orders/a.xml": 10}
    assert listed == ["ls @stage/orders/"]


def test_list_directory_file_sizes_matches_mixed_case_stage_listing():
    """A mixed-case stage name resolves against LIST's lowercased form -- matching by
    basename means the stage-name segment's case never has to be reconstructed at all."""
    listing = {
        "@db.schema.MyStage/SubDir/": [
            {"name": "mystage/SubDir/a.xml", "size": 11},
            {"name": "mystage/SubDir/b.xml", "size": 22},
        ]
    }
    builder, _ = _fake_builder(listing)
    sizes = builder._list_directory_file_sizes("@db.schema.MyStage/SubDir")
    assert sizes == {
        "@db.schema.MyStage/SubDir/a.xml": 11,
        "@db.schema.MyStage/SubDir/b.xml": 22,
    }


def test_list_directory_file_sizes_matches_named_external_stage_listing():
    """A named external stage's LIST reports the underlying cloud URL, not
    ``stage/path`` -- this is the bug this fix exists for: reconstructing an expected
    listing name from the stage identifier missed every external-stage file, since the
    URL has no relation to the stage's own name."""
    listing = {
        "@db.schema.ext_stage/dir/": [
            {
                "name": "s3://ecosystem-sas/test_files_xml/security_master_1.xml",
                "size": 123,
            },
            {
                "name": "s3://ecosystem-sas/test_files_xml/security_master_2.xml",
                "size": 456,
            },
        ]
    }
    builder, _ = _fake_builder(listing)
    sizes = builder._list_directory_file_sizes("@db.schema.ext_stage/dir")
    assert sizes == {
        "@db.schema.ext_stage/dir/security_master_1.xml": 123,
        "@db.schema.ext_stage/dir/security_master_2.xml": 456,
    }


def test_list_directory_file_sizes_missing_directory_raises():
    builder, _ = _fake_builder({})
    with pytest.raises(ValueError, match="does not exist or contains no files"):
        builder._list_directory_file_sizes("@stage/does_not_exist")


def test_list_directory_file_sizes_ignores_recursively_listed_subdirectory_files():
    """LIST recurses into subdirectories, so listing a directory can also return a file
    several levels down that happens to share a basename with a direct child. Depth-
    filtering must exclude it, or the wrong size gets assigned and byte ranges silently
    cover only part of the real file."""
    listing = {
        "@stage/dir/": [
            {"name": "stage/dir/a.xml", "size": 32},
            {"name": "stage/dir/b.xml", "size": 32},
            # Same basename as dir/a.xml, one level deeper -- must not be matched.
            {"name": "stage/dir/sub/a.xml", "size": 512},
        ]
    }
    builder, _ = _fake_builder(listing)
    sizes = builder._list_directory_file_sizes("@stage/dir")
    assert sizes == {"@stage/dir/a.xml": 32, "@stage/dir/b.xml": 32}


def test_list_directory_file_sizes_ignores_deeper_match_regardless_of_list_order():
    """The shallower row must win even when LIST returns the deeper duplicate first."""
    listing = {
        "@stage/dir/": [
            {"name": "stage/dir/sub/a.xml", "size": 512},
            {"name": "stage/dir/a.xml", "size": 32},
            {"name": "stage/dir/b.xml", "size": 64},
        ]
    }
    builder, _ = _fake_builder(listing)
    sizes = builder._list_directory_file_sizes("@stage/dir")
    assert sizes == {"@stage/dir/a.xml": 32, "@stage/dir/b.xml": 64}


#
# skipChildren: read only the row tag's own attributes.
#


def _skip_children_records(xml_bytes, **overrides):
    kwargs = {
        "file_path": "test.xml",
        "tag_name": "PARENT",
        "approx_start": 0,
        "approx_end": len(xml_bytes),
        "mode": "PERMISSIVE",
        "column_name_of_corrupt_record": "_corrupt_record",
        "ignore_namespace": True,
        "attribute_prefix": "_",
        "exclude_attributes": False,
        "value_tag": "_VALUE",
        "null_value": "",
        "charset": "utf-8",
        "ignore_surrounding_whitespace": False,
        "row_validation_xsd_path": "",
        "skip_children": True,
        **overrides,
    }
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        return list(process_xml_range(**kwargs))


def test_skip_children_yields_only_opening_tag_attributes():
    xml_bytes = (
        b"<ROOT>"
        b'<PARENT id="p1" region="north"><CHILD sku="x"><name>A</name></CHILD></PARENT>'
        b'<PARENT id="p2" region="south"><CHILD sku="y"><name>B</name></CHILD></PARENT>'
        b"</ROOT>"
    )
    records = _skip_children_records(xml_bytes)
    assert [record for record, _ in records] == [
        {"_id": "p1", "_region": "north"},
        {"_id": "p2", "_region": "south"},
    ]


def test_skip_children_ignores_gt_inside_quoted_attribute_value():
    """A literal ">" inside a quoted attribute value must not be mistaken for the tag's
    own terminating ">" -- same quote-awareness the non-skip_children path relies on."""
    xml_bytes = b'<ROOT><PARENT note="a>b" x="1"><CHILD/></PARENT></ROOT>'
    records = _skip_children_records(xml_bytes)
    assert [record for record, _ in records] == [{"_note": "a>b", "_x": "1"}]


def test_skip_children_handles_single_quoted_attributes():
    """Single-quoted attribute values are as legal in XML as double-quoted ones, and
    tag_is_self_closing treats both as quote characters -- attribute extraction must too,
    or a single-quoted attribute is silently dropped."""
    xml_bytes = b"<ROOT><PARENT id='p1' region=\"north\"><CHILD/></PARENT></ROOT>"
    records = _skip_children_records(xml_bytes)
    assert [record for record, _ in records] == [{"_id": "p1", "_region": "north"}]


def test_skip_children_decodes_entities_in_attribute_values():
    """skipChildren must decode entities the same way the non-skip_children path does
    via ElementTree's element.attrib, not yield the raw undecoded text."""
    xml_bytes = (
        b'<ROOT><PARENT note="a&amp;b &lt;x&gt; &#39;y&#39;"><CHILD/></PARENT></ROOT>'
    )
    records = _skip_children_records(xml_bytes)
    assert [record for record, _ in records] == [{"_note": "a&b <x> 'y'"}]


def test_skip_children_respects_exclude_attributes():
    xml_bytes = b'<ROOT><PARENT id="p1"><CHILD/></PARENT></ROOT>'
    records = _skip_children_records(xml_bytes, exclude_attributes=True)
    assert [record for record, _ in records] == [{}]


def test_skip_children_respects_ignore_namespace_flag():
    """A colon-containing attribute name must only be namespace-stripped when
    ignore_namespace=True is actually set -- not unconditionally."""
    xml_bytes = b'<ROOT><PARENT xyz:id="p1"><CHILD/></PARENT></ROOT>'
    stripped = _skip_children_records(xml_bytes, ignore_namespace=True)
    assert [record for record, _ in stripped] == [{"_id": "p1"}]

    # xyz is an undeclared namespace prefix; with ignore_namespace=False there is no
    # recovery path to resolve it, same as the non-skip_children path failing to parse
    # -- and like every other malformed-record path here, PERMISSIVE yields it as a
    # corrupt record rather than silently dropping the attributes.
    not_stripped = _skip_children_records(xml_bytes, ignore_namespace=False)
    assert [record for record, _ in not_stripped] == [
        {"_corrupt_record": '<PARENT xyz:id="p1">'}
    ]


def test_skip_children_on_charset_mismatch_yields_corrupt_record_in_permissive_mode():
    """A declared charset that doesn't match the file's actual encoding can decode the
    tag's own bytes into text that no longer starts with the expected "<tag_name"
    prefix. This is a malformed record like any other: PERMISSIVE yields it as a
    corrupt record instead of silently misparsing it into an empty attributes dict."""
    xml_bytes = b'<ROOT><PARENT id="p1"><CHILD/></PARENT></ROOT>'
    records = _skip_children_records(xml_bytes, charset="utf-16")
    assert [record for record, _ in records] == [{"_corrupt_record": "值剁久⁔摩∽ㅰ㸢"}]


def test_skip_children_on_charset_mismatch_drops_record_in_dropmalformed_mode():
    xml_bytes = b'<ROOT><PARENT id="p1"><CHILD/></PARENT></ROOT>'
    records = _skip_children_records(xml_bytes, charset="utf-16", mode="DROPMALFORMED")
    assert records == []


def test_skip_children_on_charset_mismatch_raises_in_failfast_mode():
    xml_bytes = b'<ROOT><PARENT id="p1"><CHILD/></PARENT></ROOT>'
    with pytest.raises(RuntimeError, match="Malformed XML record at bytes"):
        _skip_children_records(xml_bytes, charset="utf-16", mode="FAILFAST")


def test_skip_children_reports_record_start_offsets():
    xml_bytes = b'<ROOT><PARENT id="p1"><CHILD/></PARENT><PARENT id="p2"><CHILD/></PARENT></ROOT>'
    records = _skip_children_records(xml_bytes)
    offsets = [offset for _, offset in records]
    assert offsets == [
        xml_bytes.index(b'<PARENT id="p1"'),
        xml_bytes.index(b'<PARENT id="p2"'),
    ]


def test_skip_children_handles_self_closing_and_attributeless_tags():
    xml_bytes = b'<ROOT><PARENT /><PARENT id="p2"/><PARENT></PARENT></ROOT>'
    assert [record for record, _ in _skip_children_records(xml_bytes)] == [
        {},
        {"_id": "p2"},
        {},
    ]


def test_skip_children_ignores_bare_self_closing_tag_like_the_default_path():
    """A bare "<TAG/>" isn't recognized as an opening tag, same as the ordinary read path."""
    xml_bytes = b'<ROOT><PARENT/><PARENT x="1"/></ROOT>'
    assert [record for record, _ in _skip_children_records(xml_bytes)] == [{"_x": "1"}]


def _skip_children_truncated_tag(mode):
    xml_bytes = b'<ROOT><PARENT id="p1"'
    tag_start = xml_bytes.index(b"<PARENT")
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        records = list(
            process_xml_range(
                "test.xml",
                "PARENT",
                0,
                tag_start + 1,
                mode,
                "_corrupt_record",
                True,
                "_",
                False,
                "_VALUE",
                "",
                "utf-8",
                False,
                "",
                skip_children=True,
            )
        )
    return records, tag_start


def test_skip_children_on_truncated_tag_yields_corrupt_record_in_permissive_mode():
    """A malformed opening tag with no ">" before EOF must go through the same
    corrupt-record handling as every other malformed record here, not silently yield an
    empty record regardless of mode."""
    records, tag_start = _skip_children_truncated_tag("PERMISSIVE")
    assert records == [({"_corrupt_record": '<PARENT id="p1"'}, tag_start)]


def test_skip_children_on_truncated_tag_drops_record_in_dropmalformed_mode():
    records, _ = _skip_children_truncated_tag("DROPMALFORMED")
    assert records == []


def test_skip_children_on_truncated_tag_raises_in_failfast_mode():
    with pytest.raises(EOFError, match="Malformed XML record at bytes"):
        _skip_children_truncated_tag("FAILFAST")


def test_skip_children_stops_when_worker_range_ends_at_tag_boundary():
    """When approx_end lands exactly at (or before) the current tag's own closing ">",
    the loop must stop there rather than searching past this worker's assigned range for
    a next record that belongs to the next worker."""
    xml_bytes = b'<ROOT><PARENT id="p1"></PARENT><PARENT id="p2"></PARENT></ROOT>'
    tag_end = xml_bytes.index(b'<PARENT id="p1">') + len(b'<PARENT id="p1">')
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        records = list(
            process_xml_range(
                "test.xml",
                "PARENT",
                0,
                tag_end,
                "PERMISSIVE",
                "_corrupt_record",
                True,
                "_",
                False,
                "_VALUE",
                "",
                "utf-8",
                False,
                "",
                skip_children=True,
            )
        )
    assert [record for record, _ in records] == [{"_id": "p1"}]


def test_skip_children_reads_past_a_small_chunk_size():
    """chunk_size can be smaller than the opening tag; skip_children must still read far
    enough to capture every attribute rather than truncating mid-tag."""
    xml_bytes = (
        b"<ROOT>"
        b'<PARENT alpha="1" beta="2" gamma="3" delta="4" epsilon="5"></PARENT>'
        b"</ROOT>"
    )
    records = _skip_children_records(xml_bytes, chunk_size=8)
    assert [record for record, _ in records] == [
        {
            "_alpha": "1",
            "_beta": "2",
            "_gamma": "3",
            "_delta": "4",
            "_epsilon": "5",
        }
    ]


def test_skip_children_respects_attribute_prefix():
    xml_bytes = b'<ROOT><PARENT id="p1"></PARENT></ROOT>'
    records = _skip_children_records(xml_bytes, attribute_prefix="")
    assert [record for record, _ in records] == [{"id": "p1"}]


#
# includeSourcePos: the XMLReaderWithPos handler.
#


def _process_args(xml_bytes, skip_children=False):
    return (
        "test.xml",
        0,
        len(xml_bytes),
        "record",
        "PERMISSIVE",
        "_corrupt_record",
        True,
        "_",
        False,
        "_VALUE",
        "",
        "utf-8",
        True,
        "",
        "",
        False,
        DEFAULT_CHUNK_SIZE,
        skip_children,
    )


def test_xml_reader_with_pos_emits_byte_offset_and_file_path():
    xml_bytes = b"<root><record><a>1</a></record><record><a>2</a></record></root>"
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        rows = list(XMLReaderWithPos().process(*_process_args(xml_bytes)))
    assert [row[0]["a"] for row in rows] == ["1", "2"]
    assert [row[1] for row in rows] == [
        xml_bytes.index(b"<record><a>1</a>"),
        xml_bytes.index(b"<record><a>2</a>"),
    ]
    # Comes from the handler, not the worker row, so it's correct if a row covers multiple files.
    assert {row[2] for row in rows} == {"test.xml"}


def test_xml_reader_emits_only_the_record():
    """The default handler's output shape is unchanged by includeSourcePos existing."""
    xml_bytes = b"<root><record><a>1</a></record></root>"
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda *a, **k: io.BytesIO(xml_bytes),
    ):
        rows = list(XMLReader().process(*_process_args(xml_bytes)))
    assert rows == [({"a": "1"},)]


@pytest.mark.parametrize("handler", ["XMLReader", "XMLReaderWithPos"])
def test_xml_udtf_handler_input_types_are_recoverable_from_source(handler):
    """Both handlers must declare the full annotated signature on their own `process`.

    UDTF registration recovers input types by AST-scanning the named class, which does not
    follow inheritance -- a handler that delegated without repeating the annotations would
    register with the wrong input types.
    """
    hints = retrieve_func_type_hints_from_source(
        XML_READER_FILE_PATH, "process", class_name=handler
    )
    assert hints is not None
    input_hints = {name: t for name, t in hints.items() if name != "return"}
    assert len(input_hints) == 18
    assert input_hints["filename"] == "str"
    assert input_hints["approx_start"] == "int"
    assert input_hints["approx_end"] == "int"
    assert input_hints["chunk_size"] == "int"
    assert input_hints["skip_children"] == "bool"


#
# AST emission.
#


def _reader_with_ast(ast_enabled):
    """A DataFrameReader wired up just enough to reach xml()'s AST-emission branch.

    Callers must pass _emit_ast explicitly: the @publicapi decorator otherwise overwrites
    it with the global AST flag, which is off under unit tests.
    """
    reader = DataFrameReader.__new__(DataFrameReader)
    reader._cur_options = {XML_ROW_TAG_STRING: "record"}
    reader._session = mock.MagicMock()
    reader._ast = proto.Expr() if ast_enabled else None
    reader._user_schema = None
    reader._xml_inferred_schema = None
    reader._read_semi_structured_file = mock.MagicMock(return_value=mock.MagicMock())
    return reader


def test_xml_ast_emission_records_the_path():
    reader = _reader_with_ast(ast_enabled=True)
    DataFrameReader.xml(reader, "@stage/a.xml", _emit_ast=True)
    reader._read_semi_structured_file.assert_called_once_with("@stage/a.xml", "XML")


#
# Worker-assignment SQL: must stay a single statement at any file count.
#


def test_xml_worker_assignment_sql_shape():
    sql = _xml_worker_assignment_sql(
        [("@stage/a.xml", 0, 10), ("@stage/b.xml", 10, 25)],
        "FILE_PATH",
        "APPROX_START",
        "APPROX_END",
    )
    assert sql == (
        "SELECT $1 AS FILE_PATH, $2 AS APPROX_START, $3 AS APPROX_END FROM VALUES "
        "('@stage/a.xml', 0::BIGINT, 10::BIGINT), "
        "('@stage/b.xml', 10::BIGINT, 25::BIGINT)"
    )


@pytest.mark.parametrize("assignment_count", [1, 199, 200, 3200])
def test_xml_worker_assignment_sql_is_one_statement_at_any_scale(assignment_count):
    """The assignment table must be a single inline VALUES query no matter how many files
    are read.

    Session.create_dataframe would switch to CREATE TEMP TABLE + INSERT + SELECT somewhere
    below 200 rows, and only one query survives into the reader's plan -- so the setup
    statements would be dropped and the surviving SELECT would reference a table that was
    never created. That failure only appears once enough files are read to cross the
    threshold, which is why the row counts here straddle it.
    """
    assignments = [(f"@stage/f{i}.xml", i, i + 10) for i in range(assignment_count)]
    sql = _xml_worker_assignment_sql(
        assignments, "FILE_PATH", "APPROX_START", "APPROX_END"
    )
    assert sql.count("SELECT") == 1
    assert "CREATE" not in sql.upper()
    assert "INSERT" not in sql.upper()
    assert ";" not in sql
    assert sql.count("::BIGINT") == 2 * assignment_count


@pytest.mark.parametrize(
    "assignment_count",
    [
        _XML_WORKER_ASSIGNMENT_MAX_ROWS_PER_VALUES,
        _XML_WORKER_ASSIGNMENT_MAX_ROWS_PER_VALUES + 1,
        2 * _XML_WORKER_ASSIGNMENT_MAX_ROWS_PER_VALUES + 137,
    ],
)
def test_xml_worker_assignment_sql_chunks_past_the_values_list_limit(assignment_count):
    """Snowflake rejects a single VALUES list past 200,000 expressions ("maximum number
    of expressions in a list exceeded") -- confirmed against a live account. A directory
    of many large files, each needing multiple byte-range workers, has no other ceiling
    on row count. Once the limit would be crossed, the statement must split into
    multiple VALUES clauses joined by UNION ALL rather than exceeding any single one,
    while remaining a single statement (no CREATE/INSERT, no semicolon) for the same
    reason as above.
    """
    assignments = [(f"@stage/f{i}.xml", i, i + 10) for i in range(assignment_count)]
    sql = _xml_worker_assignment_sql(
        assignments, "FILE_PATH", "APPROX_START", "APPROX_END"
    )
    expected_chunks = -(
        -assignment_count // _XML_WORKER_ASSIGNMENT_MAX_ROWS_PER_VALUES
    )  # ceil division
    assert sql.count("SELECT") == expected_chunks
    assert sql.count(" FROM VALUES ") == expected_chunks
    assert sql.count(" UNION ALL ") == expected_chunks - 1
    assert "CREATE" not in sql.upper()
    assert "INSERT" not in sql.upper()
    assert ";" not in sql
    assert sql.count("::BIGINT") == 2 * assignment_count


def test_xml_worker_assignment_sql_stays_one_chunk_at_the_limit_boundary():
    """Exactly at the limit, one VALUES clause is still enough -- no unnecessary split."""
    assignments = [
        (f"@stage/f{i}.xml", i, i + 10)
        for i in range(_XML_WORKER_ASSIGNMENT_MAX_ROWS_PER_VALUES)
    ]
    sql = _xml_worker_assignment_sql(
        assignments, "FILE_PATH", "APPROX_START", "APPROX_END"
    )
    assert sql.count("SELECT") == 1
    assert "UNION ALL" not in sql


@pytest.mark.parametrize(
    "path",
    [
        "@stage/it's.xml",
        "@stage/back\\slash.xml",
        "@stage/new\nline.xml",
        "@stage/'; DROP TABLE t; --",
    ],
)
def test_xml_worker_assignment_sql_escapes_paths(path):
    """Paths reach the statement as literals, so a quote or backslash must not be able to
    terminate the literal early."""
    sql = _xml_worker_assignment_sql([(path, 0, 1)], "F", "S", "E")
    body = sql[sql.index("FROM VALUES ") + len("FROM VALUES ") :]
    literal = body[body.index("(") + 1 : body.index(", 0::BIGINT")]
    assert literal.startswith("'") and literal.endswith("'")
    # No unescaped single quote can appear inside the literal body.
    assert "'" not in literal[1:-1].replace("''", "")


#
# Small-file batching: packing several files into one worker-assignment row.
#


def test_batch_encode_decode_round_trip():
    entries = [
        ("@stage/a.xml", 0, 10),
        ("@stage/b.xml", 0, 20),
        ("@stage/c.xml", 5, 25),
    ]
    assert decode_batch_or_single(encode_batch(entries), 0, 0) == entries


def test_decode_passes_through_an_unbatched_row():
    """An unbatched row's range comes from its own columns, not from the path."""
    assert decode_batch_or_single("@stage/a.xml", 7, 99) == [("@stage/a.xml", 7, 99)]


def test_decode_handles_a_batch_of_one():
    """A one-entry batch must decode from the encoded string rather than falling through to
    the row's range columns, which are set to 0 for batched rows.

    The planner emits a lone file unencoded today, so this only matters as insurance
    against the decoder silently depending on that.
    """
    encoded = encode_batch([("@stage/only.xml", 3, 42)])
    assert encoded[0].isdigit()  # recognized as an encoded batch even with one entry
    assert decode_batch_or_single(encoded, 0, 0) == [("@stage/only.xml", 3, 42)]


@pytest.mark.parametrize(
    "path",
    [
        "@stage/5:weird.xml",
        "@stage/contains\x01control\x02bytes.xml",
        "@stage/11:@s/b.xml1:01:1.xml",
    ],
)
def test_batch_encode_decode_survives_paths_that_look_like_the_encoding(path):
    """A length-prefixed encoding can't be confused by path content that happens to look
    like a length prefix, a colon, or (unlike the old delimiter scheme) even a literal
    control byte -- decoding always walks by the byte counts it wrote, never by scanning
    for a separator that could collide with the path itself."""
    entries = [(path, 3, 42), ("@stage/other.xml", 0, 5)]
    assert decode_batch_or_single(encode_batch(entries), 0, 0) == entries


def _single(path, size):
    return (size, [(path, 0, size)])


def test_pack_emits_a_lone_small_file_unencoded():
    """The ordinary single-file read must keep producing exactly the row it always did."""
    assert _pack_xml_assignments([_single("@s/a.xml", 100)]) == [("@s/a.xml", 0, 100)]


def test_pack_batches_several_small_files_into_one_row():
    packed = _pack_xml_assignments(
        [_single("@s/a.xml", 100), _single("@s/b.xml", 200), _single("@s/c.xml", 300)]
    )
    assert len(packed) == 1
    assert decode_batch_or_single(packed[0][0], 0, 0) == [
        ("@s/a.xml", 0, 100),
        ("@s/b.xml", 0, 200),
        ("@s/c.xml", 0, 300),
    ]


def test_pack_never_batches_a_file_split_across_workers():
    """A multi-megabyte byte range packed alongside whole small files would make that row's
    runtime wildly uneven, and per-invocation overhead is not what dominates for large
    files."""
    packed = _pack_xml_assignments(
        [
            _single("@s/a.xml", 100),
            (5000, [("@s/big.xml", 0, 2500), ("@s/big.xml", 2500, 5000)]),
            _single("@s/c.xml", 100),
        ]
    )
    assert packed == [
        ("@s/a.xml", 0, 100),
        ("@s/big.xml", 0, 2500),
        ("@s/big.xml", 2500, 5000),
        ("@s/c.xml", 0, 100),
    ]


def test_pack_flushes_at_the_file_count_limit():
    at_limit = _pack_xml_assignments(
        [_single(f"@s/f{i}.xml", 1) for i in range(XML_BATCH_MAX_FILES)]
    )
    assert len(at_limit) == 1
    assert len(decode_batch_or_single(at_limit[0][0], 0, 0)) == XML_BATCH_MAX_FILES

    over_limit = _pack_xml_assignments(
        [_single(f"@s/f{i}.xml", 1) for i in range(XML_BATCH_MAX_FILES + 1)]
    )
    assert len(over_limit) == 2
    assert len(decode_batch_or_single(over_limit[0][0], 0, 0)) == XML_BATCH_MAX_FILES
    # The leftover file is alone, so it is emitted unencoded.
    assert over_limit[1] == (f"@s/f{XML_BATCH_MAX_FILES}.xml", 0, 1)


def test_pack_honors_a_caller_supplied_target_bytes():
    """The byte target is a parameter, not just the module constant -- callers (the
    ``batchTargetBytes`` reader option) must be able to override it."""
    packed = _pack_xml_assignments(
        [_single("@s/a.xml", 30), _single("@s/b.xml", 30)], target_bytes=50
    )
    assert len(packed) == 2, "50 is too small for both 30-byte files in one row"

    packed = _pack_xml_assignments(
        [_single("@s/a.xml", 30), _single("@s/b.xml", 30)], target_bytes=60
    )
    assert len(packed) == 1, "60 fits both 30-byte files in one row"


def test_pack_fills_a_batch_exactly_to_the_byte_target():
    half = XML_BATCH_TARGET_BYTES // 2
    packed = _pack_xml_assignments(
        [_single("@s/a.xml", half), _single("@s/b.xml", XML_BATCH_TARGET_BYTES - half)]
    )
    assert len(packed) == 1, "a batch summing exactly to the target should not be split"


def test_pack_flushes_when_one_more_byte_would_exceed_the_target():
    half = XML_BATCH_TARGET_BYTES // 2
    packed = _pack_xml_assignments(
        [
            _single("@s/a.xml", half),
            _single("@s/b.xml", XML_BATCH_TARGET_BYTES - half + 1),
        ]
    )
    assert len(packed) == 2


def test_pack_keeps_a_file_larger_than_the_target_on_its_own_row():
    packed = _pack_xml_assignments(
        [
            _single("@s/a.xml", 10),
            _single("@s/huge.xml", XML_BATCH_TARGET_BYTES + 1),
            _single("@s/b.xml", 10),
        ]
    )
    # The oversized file forces a flush before it, then cannot share with what follows.
    assert len(packed) == 3


def test_batched_row_escapes_through_the_worker_assignment_sql():
    """The encoded batch is a caller-influenced string embedded as a SQL literal, so it has
    to survive the same escaping as a plain path -- length prefixes included."""
    entries = [("@s/it's.xml", 0, 1), ("@s/b.xml", 1, 2)]
    encoded = encode_batch(entries)
    sql = _xml_worker_assignment_sql([(encoded, 0, 0)], "F", "S", "E")
    assert sql.count("SELECT") == 1
    body = sql[sql.index("FROM VALUES ") :]
    # The quote in the path is doubled by SQL escaping...
    assert "it''s" in body
    # ...and undoing just that escaping recovers the exact original encoding.
    literal = body[body.index("'") + 1 : body.index(", 0::BIGINT")]
    assert literal.endswith("'")
    assert decode_batch_or_single(literal[:-1].replace("''", "'"), 0, 0) == entries


def test_process_batch_concurrently_returns_every_record_from_every_file():
    """All entries' records must come back, tagged with the file each came from."""
    contents = {
        "a.xml": b"<r><record><id>a1</id></record><record><id>a2</id></record></r>",
        "b.xml": b"<r><record><id>b1</id></record></r>",
        "c.xml": b"<r><record><id>c1</id></record><record><id>c2</id></record></r>",
    }
    batch = [(name, 0, len(data)) for name, data in contents.items()]

    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        side_effect=lambda path, *a, **k: io.BytesIO(contents[path]),
    ):
        produced = list(
            _process_batch_concurrently(
                batch,
                "record",
                mode="PERMISSIVE",
                column_name_of_corrupt_record="_corrupt_record",
                ignore_namespace=True,
                attribute_prefix="_",
                exclude_attributes=False,
                value_tag="_VALUE",
                null_value="",
                charset="utf-8",
                ignore_surrounding_whitespace=False,
                row_validation_xsd_path="",
                chunk_size=DEFAULT_CHUNK_SIZE,
                result_template=None,
                schema_type=None,
                is_snowpark_connect_compatible=False,
                skip_children=False,
            )
        )

    # Order across files is not preserved -- results are yielded as each file finishes.
    assert sorted((path, record["id"]) for path, record, _ in produced) == [
        ("a.xml", "a1"),
        ("a.xml", "a2"),
        ("b.xml", "b1"),
        ("c.xml", "c1"),
        ("c.xml", "c2"),
    ]
