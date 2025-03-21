#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io
import re
import xml.etree.ElementTree as ET
import html.entities
import pytest

from snowflake.snowpark._internal.xml_reader import (
    replace_entity,
    element_to_dict,
    find_next_closing_tag_pos,
    find_next_opening_tag_pos,
    SELF_CLOSING_TAG,
    DEFAULT_CHUNK_SIZE,
)


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


def test_element_to_dict_text():
    # Element with only text.
    element = ET.Element("greeting")
    element.text = "  hello world  "
    result = element_to_dict(element)
    assert result == "hello world"


def test_element_to_dict_attributes():
    # Element with attributes only.
    element = ET.Element("person", attrib={"name": "Alice", "age": "30"})
    result = element_to_dict(element)
    expected = {"_name": "Alice", "_age": "30"}
    assert result == expected


def test_element_to_dict_children():
    # Element with children including repeated tags.
    root = ET.Element("data")
    child1 = ET.SubElement(root, "item")
    child1.text = "value1"
    child2 = ET.SubElement(root, "item")
    child2.text = "value2"
    child3 = ET.SubElement(root, "note")
    child3.text = "note1"
    result = element_to_dict(root)
    expected = {"item": ["value1", "value2"], "note": "note1"}
    assert result == expected


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
def test_find_next_closing_tag_pos_self_closing(chunk_size):
    # Test a self-closing tag.
    record = b"<row attr='value'/> trailing content"
    file_obj = io.BytesIO(record)
    closing_tag = b"</row>"  # Function should detect the self-closing tag.
    pos = find_next_closing_tag_pos(file_obj, closing_tag, chunk_size=chunk_size)
    expected_pos = record.find(SELF_CLOSING_TAG) + len(SELF_CLOSING_TAG)
    assert pos == expected_pos


@pytest.mark.parametrize("chunk_size", [3, 10, DEFAULT_CHUNK_SIZE])
def test_find_next_closing_tag_pos_both_variants(chunk_size):
    # When both self-closing and full closing tags exist, the earliest occurrence should be chosen.
    record = b"start <row attr='value'/> some text </row> end"
    file_obj = io.BytesIO(record)
    closing_tag = b"</row>"
    pos = find_next_closing_tag_pos(file_obj, closing_tag, chunk_size=chunk_size)
    expected_pos = record.find(SELF_CLOSING_TAG) + len(SELF_CLOSING_TAG)
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
