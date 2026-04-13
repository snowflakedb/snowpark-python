#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import json
import os
from unittest import mock

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    DateType,
    BooleanType,
    TimestampType,
    ArrayType,
    VariantType,
)
import snowflake.snowpark.context as context
from tests.utils import TestFiles, Utils


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="xml not supported in local testing mode",
    ),
    pytest.mark.udf,
]

tmp_stage_name = Utils.random_stage_name()

# Resource XML file names (uploaded from tests/resources/)
RES_BOOKS_XML = "books.xml"
RES_BOOKS2_XML = "books2.xml"
RES_DK_TRACE_XML = "dk_trace_sample.xml"
RES_DBLP_XML = "dblp_6kb.xml"
RES_BOOKS_ATTR_VAL_XML = "books_attribute_value.xml"

# Inline XML strings uploaded to stage as files for testing
# Each covers specific inference scenarios without needing separate resource files.

# Primitive type inference: bool, double, long, string, timestamp
PRIMITIVES_XML = """\
<?xml version="1.0"?>
<data>
  <ROW>
    <bool1>true</bool1>
    <double1>+10.1</double1>
    <long1>-10</long1>
    <long2>10</long2>
    <string1>8E9D</string1>
    <ts1>2015-01-01 00:00:00</ts1>
  </ROW>
</data>
"""

# Date and timestamp inference
DATE_TIME_XML = """\
<?xml version="1.0"?>
<data>
  <book>
    <author>John Smith</author>
    <date>2021-02-01</date>
    <date2>02-01-2021</date2>
  </book>
</data>
"""

TIMESTAMP_XML = """\
<?xml version="1.0"?>
<data>
  <book>
    <author>John Smith</author>
    <time>2011-12-03T10:15:30Z</time>
    <time2>not-a-timestamp</time2>
  </book>
</data>
"""

# Root-level _VALUE: attribute-only row element with text content
ROOT_VALUE_XML = """\
<?xml version="1.0"?>
<ROWSET>
    <ROW>value1</ROW>
    <ROW attr="attr1">value2</ROW>
    <ROW>value3</ROW>
</ROWSET>
"""

# Root-level _VALUE with child elements
ROOT_VALUE_MIXED_XML = """\
<?xml version="1.0"?>
<ROWSET>
    <ROW>value1</ROW>
    <ROW attr="attr1">value2</ROW>
    <ROW>4<tag>5</tag></ROW>
    <ROW><tag>6</tag>7</ROW>
    <ROW attr="8"></ROW>
</ROWSET>
"""

# Nested object: struct child element
NESTED_OBJECT_XML = """\
<?xml version="1.0"?>
<catalog>
  <book id="1">
    <title>Book A</title>
    <price>44.95</price>
    <info>
      <publisher>Acme</publisher>
      <year>2020</year>
    </info>
  </book>
  <book id="2">
    <title>Book B</title>
    <price>29.99</price>
    <info>
      <publisher>Beta</publisher>
      <year>2021</year>
    </info>
  </book>
</catalog>
"""

# Nested array: repeated sibling elements
NESTED_ARRAY_XML = """\
<?xml version="1.0"?>
<catalog>
  <book id="1">
    <title>Book A</title>
    <tag>fiction</tag>
    <tag>classic</tag>
  </book>
  <book id="2">
    <title>Book B</title>
    <tag>science</tag>
  </book>
</catalog>
"""

# Element with attribute on leaf: <price unit="$">5.95</price> → struct(_VALUE, _unit)
ATTR_ON_LEAF_XML = """\
<?xml version="1.0"?>
<catalog>
  <book id="1">
    <title>Book A</title>
    <price unit="$">44.95</price>
  </book>
  <book id="2">
    <title>Book B</title>
    <price unit="EUR">29.99</price>
  </book>
  <book id="3">
    <title>Book C</title>
    <price>15.00</price>
  </book>
</catalog>
"""

# Missing nested struct: some rows have nested struct, some don't
MISSING_NESTED_XML = """\
<?xml version="1.0"?>
<root>
    <item>
        <name>Item A</name>
        <details>
            <color>red</color>
        </details>
    </item>
    <item>
        <name>Item B</name>
    </item>
</root>
"""

# Unbalanced types: same field has different types across rows
UNBALANCED_TYPES_XML = """\
<?xml version="1.0"?>
<data>
  <ROW>
    <field1>123</field1>
    <field2>hello</field2>
  </ROW>
  <ROW>
    <field1>45.6</field1>
    <field2>world</field2>
  </ROW>
</data>
"""

# Mixed content: text + child elements
MIXED_CONTENT_XML = """\
<?xml version="1.0"?>
<root>
  <item>
    <desc>Simple text</desc>
    <nested>
      <a>1</a>
    </nested>
  </item>
  <item>
    <desc>Has <bold>mixed</bold> content</desc>
    <nested>
      <a>2</a>
    </nested>
  </item>
</root>
"""

# ExcludeAttributes with inferSchema
EXCLUDE_ATTRS_XML = """\
<?xml version="1.0"?>
<data>
  <item id="1" category="A">
    <name>Widget</name>
    <price>9.99</price>
  </item>
  <item id="2" category="B">
    <name>Gadget</name>
    <price>19.99</price>
  </item>
</data>
"""

# Big integer inference
BIG_INT_XML = """\
<?xml version="1.0"?>
<data>
  <ROW>
    <small_int>42</small_int>
    <big_int>92233720368547758070</big_int>
    <normal_double>3.14</normal_double>
  </ROW>
</data>
"""

# Nested element same name as parent
PARENT_NAME_COLLISION_XML = """\
<?xml version="1.0"?>
<people>
  <parent>
    <parent>
      <child>Child 1.1</child>
    </parent>
    <child>Child 1.2</child>
  </parent>
  <parent>
    <parent>
      <child>Child 2.1</child>
    </parent>
    <child>Child 2.2</child>
  </parent>
</people>
"""

# Complicated nested: struct containing array of structs with attributes
COMPLICATED_NESTED_XML = """\
<?xml version="1.0"?>
<catalog>
  <book id="1">
    <author>Author A</author>
    <genre>
      <genreid>1</genreid>
      <name>Fiction</name>
    </genre>
    <dates>
      <date tag="first">
        <year>2020</year>
        <month>1</month>
      </date>
      <date tag="second">
        <year>2020</year>
        <month>6</month>
      </date>
    </dates>
  </book>
  <book id="2">
    <author>Author B</author>
    <genre>
      <genreid>2</genreid>
      <name>Science</name>
    </genre>
    <dates>
      <date>
        <year>2021</year>
        <month>3</month>
      </date>
    </dates>
  </book>
</catalog>
"""

# Sampling heterogeneous: 20 rows where the first 15 have value as integer,
# but rows 16-20 have value as a float string.  With a low sampling ratio
# deterministic seed may only see the first chunk → LongType instead of DoubleType.
SAMPLING_HETERO_XML = """\
<?xml version="1.0"?>
<data>
  <ROW><name>r01</name><value>1</value></ROW>
  <ROW><name>r02</name><value>2</value></ROW>
  <ROW><name>r03</name><value>3</value></ROW>
  <ROW><name>r04</name><value>4</value></ROW>
  <ROW><name>r05</name><value>5</value></ROW>
  <ROW><name>r06</name><value>6</value></ROW>
  <ROW><name>r07</name><value>7</value></ROW>
  <ROW><name>r08</name><value>8</value></ROW>
  <ROW><name>r09</name><value>9</value></ROW>
  <ROW><name>r10</name><value>10</value></ROW>
  <ROW><name>r11</name><value>11</value></ROW>
  <ROW><name>r12</name><value>12</value></ROW>
  <ROW><name>r13</name><value>13</value></ROW>
  <ROW><name>r14</name><value>14</value></ROW>
  <ROW><name>r15</name><value>15</value></ROW>
  <ROW><name>r16</name><value>16.1</value></ROW>
  <ROW><name>r17</name><value>17.2</value></ROW>
  <ROW><name>r18</name><value>18.3</value></ROW>
  <ROW><name>r19</name><value>19.4</value></ROW>
  <ROW><name>r20</name><value>20.5</value></ROW>
</data>
"""

# Processing instruction XML
PROCESSING_INSTRUCTION_XML = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="transform.xsl"?>
<data>
  <foo>
    <bar>1</bar>
    <baz>hello</baz>
  </foo>
</data>
"""


def _upload_xml_string(session, stage, filename, xml_content):
    """Write XML string to a temp file and upload to stage."""
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".xml", delete=False, prefix=filename.replace(".xml", "_")
    ) as f:
        f.write(xml_content)
        tmp_path = f.name
    try:
        Utils.upload_to_stage(session, stage, tmp_path, compress=False)
    finally:
        os.unlink(tmp_path)
    return os.path.basename(tmp_path)


# Map of logical name -> (xml_content, staged_filename) populated during setup
_staged_files = {}


@pytest.fixture(autouse=True)
def enable_scos_compatible_mode():
    """XML inferSchema is gated behind _is_snowpark_connect_compatible_mode.
    Enable it for every test in this module."""
    with mock.patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        yield


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path, local_testing_mode):
    test_files = TestFiles(resources_path)
    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)

    # Upload resource XML files
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_xml_infer_types, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_xml_infer_mixed, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_books_xml, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_books2_xml, compress=False
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_dk_trace_sample_xml,
        compress=False,
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_dblp_6kb_xml, compress=False
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_books_attribute_value_xml,
        compress=False,
    )

    # Upload inline XML strings as files
    inline_xmls = {
        "primitives": PRIMITIVES_XML,
        "date_time": DATE_TIME_XML,
        "timestamp": TIMESTAMP_XML,
        "root_value": ROOT_VALUE_XML,
        "root_value_mixed": ROOT_VALUE_MIXED_XML,
        "nested_object": NESTED_OBJECT_XML,
        "nested_array": NESTED_ARRAY_XML,
        "attr_on_leaf": ATTR_ON_LEAF_XML,
        "missing_nested": MISSING_NESTED_XML,
        "unbalanced_types": UNBALANCED_TYPES_XML,
        "mixed_content": MIXED_CONTENT_XML,
        "exclude_attrs": EXCLUDE_ATTRS_XML,
        "big_int": BIG_INT_XML,
        "parent_collision": PARENT_NAME_COLLISION_XML,
        "complicated_nested": COMPLICATED_NESTED_XML,
        "processing_instr": PROCESSING_INSTRUCTION_XML,
        "sampling_hetero": SAMPLING_HETERO_XML,
    }
    for name, xml_str in inline_xmls.items():
        staged = _upload_xml_string(
            session, "@" + tmp_stage_name, f"{name}.xml", xml_str
        )
        _staged_files[name] = staged

    yield
    if not local_testing_mode:
        session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name}").collect()


def _schema_types(df):
    """Return {lowercase_field_name: datatype_class} from df.schema."""
    return {f.name.strip('"').lower(): type(f.datatype) for f in df.schema.fields}


# ─── Primitive type inference ───────────────────────────────────────────────


def test_infer_primitives(session):
    """Bool, double, long, string, timestamp inferred from inline XML."""
    df = session.read.option("rowTag", "ROW").xml(
        f"@{tmp_stage_name}/{_staged_files['primitives']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert types["bool1"] == BooleanType
    assert types["double1"] == DoubleType
    assert types["long1"] == LongType
    assert types["long2"] == LongType
    assert types["string1"] == StringType
    assert types["ts1"] == TimestampType
    result = df.collect()
    assert len(result) == 1
    assert result[0]["bool1"] is True
    assert result[0]["double1"] == 10.1
    assert result[0]["long1"] == -10
    assert result[0]["string1"] == "8E9D"


def test_infer_date(session):
    """ISO date string inferred as DateType, non-ISO stays StringType."""
    df = session.read.option("rowTag", "book").xml(
        f"@{tmp_stage_name}/{_staged_files['date_time']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert types["date"] == DateType
    assert types["date2"] == StringType  # non-ISO format stays string
    assert types["author"] == StringType
    result = df.collect()
    assert result[0]["date"] == datetime.date(2021, 2, 1)


def test_infer_timestamp(session):
    """ISO timestamp inferred as TimestampType, non-timestamp stays StringType."""
    df = session.read.option("rowTag", "book").xml(
        f"@{tmp_stage_name}/{_staged_files['timestamp']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert types["time"] == TimestampType
    assert types["time2"] == StringType


# ─── Root-level _VALUE tag ──────────────────────────────────────────────────


def test_infer_root_value_attrs_only(session):
    """<ROW attr="x">value</ROW> → _VALUE + _attr columns."""
    df = session.read.option("rowTag", "ROW").xml(
        f"@{tmp_stage_name}/{_staged_files['root_value']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert "_value" in types or "_VALUE" in types.keys()
    result = df.collect()
    assert len(result) == 3


def test_infer_root_value_with_child_elements(session):
    """<ROW>4<tag>5</tag></ROW> → _VALUE + tag columns."""
    df = session.read.option("rowTag", "ROW").xml(
        f"@{tmp_stage_name}/{_staged_files['root_value_mixed']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert "tag" in types
    result = df.collect()
    assert len(result) == 5


# ─── Nested structures ─────────────────────────────────────────────────────


def test_infer_nested_object(session):
    """Child element becomes nested struct: info.publisher, info.year."""
    df = session.read.option("rowTag", "book").xml(
        f"@{tmp_stage_name}/{_staged_files['nested_object']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    # info should be VariantType (Snowpark represents nested structs as Variant)
    assert types["info"] == VariantType
    assert types["price"] == DoubleType
    result = df.collect()
    assert len(result) == 2
    info = json.loads(result[0]["info"])
    assert info["publisher"] in ["Acme", "Beta"]


def test_infer_nested_array(session):
    """Repeated sibling <tag> elements → ArrayType or VariantType."""
    df = session.read.option("rowTag", "book").xml(
        f"@{tmp_stage_name}/{_staged_files['nested_array']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    # Repeated tag elements: Variant wrapping an array
    assert types["tag"] in (VariantType, ArrayType)
    result = df.collect()
    assert len(result) == 2
    # Book with 2 tags should be an array; _id may be Long or String
    book1 = [r for r in result if str(r["_id"]).strip('"') == "1"][0]
    tag_val = (
        json.loads(book1["tag"]) if isinstance(book1["tag"], str) else book1["tag"]
    )
    assert isinstance(tag_val, list)
    assert len(tag_val) == 2


def test_infer_complicated_nested(session):
    """Struct with sub-struct and array of structs with attributes."""
    df = session.read.option("rowTag", "book").xml(
        f"@{tmp_stage_name}/{_staged_files['complicated_nested']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert types["genre"] == VariantType
    assert types["dates"] == VariantType
    result = df.collect()
    assert len(result) == 2
    # Verify genre nested content
    genre1 = json.loads(result[0]["genre"])
    assert "genreid" in genre1 or "name" in genre1
    # Verify dates nested array
    dates1 = json.loads(result[0]["dates"])
    assert "date" in dates1


# ─── Attribute on leaf element (_VALUE pattern) ────────────────────────────


def test_infer_attribute_on_leaf(session):
    """<price unit="$">44.95</price> → struct with _VALUE + _unit."""
    df = session.read.option("rowTag", "book").xml(
        f"@{tmp_stage_name}/{_staged_files['attr_on_leaf']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    # price has attr on some rows → becomes VariantType (struct with _VALUE/_unit)
    assert types["price"] == VariantType
    result = df.collect()
    assert len(result) == 3
    # Book 1 has unit="$"; _id may be Long or String
    book1 = [r for r in result if str(r["_id"]).strip('"') == "1"][0]
    price_data = json.loads(book1["price"])
    assert "_VALUE" in price_data
    assert price_data["_unit"] == "$"


# ─── Missing nested struct ─────────────────────────────────────────────────


def test_infer_missing_nested_struct(session):
    """Row missing a nested struct field gets null/empty, not crash."""
    df = session.read.option("rowTag", "item").xml(
        f"@{tmp_stage_name}/{_staged_files['missing_nested']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert "details" in types
    result = df.collect()
    assert len(result) == 2
    # Item B has no details
    item_b = [r for r in result if r["name"] == "Item B"][0]
    details_val = item_b["details"]
    # Either null or struct with null fields
    if details_val is not None:
        parsed = json.loads(details_val)
        assert parsed.get("color") is None


# ─── Unbalanced types (type widening across rows) ──────────────────────────


def test_infer_unbalanced_types(session):
    """Field1 is 123 in one row and 45.6 in another → widened to DoubleType."""
    df = session.read.option("rowTag", "ROW").xml(
        f"@{tmp_stage_name}/{_staged_files['unbalanced_types']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert types["field1"] == DoubleType  # Long widened to Double
    assert types["field2"] == StringType
    result = df.collect()
    assert len(result) == 2


# ─── excludeAttributes with inferSchema ─────────────────────────────────────


def test_infer_exclude_attributes(session):
    """excludeAttributes=true removes id/category from inferred schema."""
    df = (
        session.read.option("rowTag", "item")
        .option("excludeAttributes", True)
        .xml(f"@{tmp_stage_name}/{_staged_files['exclude_attrs']}")
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert "_id" not in types
    assert "_category" not in types
    assert "name" in types
    assert "price" in types
    result = df.collect()
    assert len(result) == 2

    # Without excludeAttributes, attributes should appear
    df2 = session.read.option("rowTag", "item").xml(
        f"@{tmp_stage_name}/{_staged_files['exclude_attrs']}"
    )
    types2 = _schema_types(df2)
    assert "_id" in types2
    assert "_category" in types2


# ─── Big integer handling ───────────────────────────────────────────────────


def test_infer_big_integer(session):
    """Very large integer doesn't fit in Long → inferred as Double or String."""
    df = session.read.option("rowTag", "ROW").xml(
        f"@{tmp_stage_name}/{_staged_files['big_int']}"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert types["small_int"] == LongType
    # Big int overflows Long → should become Double or String
    assert types["big_int"] in (DoubleType, StringType)
    assert types["normal_double"] == DoubleType


# ─── Nested element same name as parent ─────────────────────────────────────


def test_infer_parent_name_collision(session):
    """<parent><parent><child>...</child></parent><child>...</child></parent>
    Known limitation: when rowTag matches a child element name, byte-scanning
    picks up inner tags as row boundaries, causing schema inference to fall back
    to all-variant or produce extra rows. Verify it doesn't crash."""
    df = session.read.option("rowTag", "parent").xml(
        f"@{tmp_stage_name}/{_staged_files['parent_collision']}"
    )
    result = df.collect()
    # May produce 2 or 4 rows depending on inner <parent> matching
    assert len(result) >= 2


# ─── Processing instruction ────────────────────────────────────────────────


def test_infer_with_processing_instruction(session):
    """XML with <?xml-stylesheet ...?> should parse without error."""
    df = session.read.option("rowTag", "foo").xml(
        f"@{tmp_stage_name}/{_staged_files['processing_instr']}"
    )
    assert df._all_variant_cols is False
    result = df.collect()
    assert len(result) == 1


# ─── Mixed content (text + child elements) ──────────────────────────────────


def test_infer_mixed_content(session):
    """Text mixed with child elements: desc field has both text and <bold>."""
    df = session.read.option("rowTag", "item").xml(
        f"@{tmp_stage_name}/{_staged_files['mixed_content']}"
    )
    assert df._all_variant_cols is False
    result = df.collect()
    assert len(result) == 2


# ─── Resource file: xml_infer_types.xml ─────────────────────────────────────


def test_infer_types_resource_file(session):
    """Comprehensive resource file: primitives, nested, array, attributes, mixed."""
    df = session.read.option("rowTag", "item").xml(
        f"@{tmp_stage_name}/xml_infer_types.xml"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)

    # Flat fields
    assert types["_id"] in (LongType, StringType)
    assert types["name"] == StringType
    assert types["quantity"] == LongType
    assert types["in_stock"] == BooleanType
    assert types["release_date"] == DateType
    assert types["last_updated"] == TimestampType

    # price has attribute on some rows → complex type
    assert types["price"] == VariantType

    # tags → nested with array → Variant
    assert types["tags"] == VariantType

    # specs → nested struct → Variant
    assert types["specs"] == VariantType

    # rating has attribute on some rows → complex
    assert types["rating"] == VariantType

    result = df.collect()
    assert len(result) == 3

    # Verify nested data
    item1 = [r for r in result if str(r["_id"]) in ("1", '"1"')][0]
    specs = json.loads(item1["specs"])
    assert "weight" in specs or "dimensions" in specs
    tags = json.loads(item1["tags"])
    assert "tag" in tags


def test_infer_mixed_resource_file(session):
    """Resource file with name collisions, sparse fields, mixed content."""
    df = session.read.option("rowTag", "record").xml(
        f"@{tmp_stage_name}/xml_infer_mixed.xml"
    )
    assert df._all_variant_cols is False
    types = _schema_types(df)
    assert "_type" in types
    assert "parent" in types
    assert "age" in types
    assert "value" in types
    result = df.collect()
    assert len(result) == 3

    # Verify opt_struct is null for sparse record
    sparse = [r for r in result if r["_type"] == "sparse"][0]
    try:
        opt_val = sparse["opt_struct"]
    except (KeyError, IndexError):
        opt_val = None
    if opt_val is not None:
        parsed = json.loads(opt_val)
        assert parsed["a"] is None


def test_read_xml_infer_schema_books_flat(session):
    """Infer schema on books.xml: all flat primitives, 12 rows, 7 columns."""
    expected_schema = StructType(
        [
            StructField("_id", StringType()),
            StructField("author", StringType()),
            StructField("description", StringType()),
            StructField("genre", StringType()),
            StructField("price", DoubleType()),
            StructField("publish_date", DateType()),
            StructField("title", StringType()),
        ]
    )
    df = session.read.option("rowTag", "book").xml(f"@{tmp_stage_name}/{RES_BOOKS_XML}")
    assert df._all_variant_cols is False
    actual = {f.name.strip('"').lower(): type(f.datatype) for f in df.schema.fields}
    expected = {f.name.lower(): type(f.datatype) for f in expected_schema.fields}
    assert actual == expected
    result = df.collect()
    assert len(result) == 12
    assert len(result[0]) == 7
    Utils.check_answer(
        df.filter(col('"_id"') == "bk101").select(
            col('"price"'), col('"publish_date"'), col('"author"')
        ),
        [Row(44.95, datetime.date(2000, 10, 1), "Gambardella, Matthew")],
    )


def test_read_xml_infer_schema_books2_nested(session):
    """Infer schema on books2.xml: complex types become VariantType, verify nested data."""
    expected_schema = StructType(
        [
            StructField("_id", LongType()),
            StructField("author", StringType()),
            StructField("editions", VariantType()),
            StructField("price", DoubleType()),
            StructField("reviews", VariantType()),
            StructField("title", StringType()),
        ]
    )
    df = session.read.option("rowTag", "book").xml(
        f"@{tmp_stage_name}/{RES_BOOKS2_XML}"
    )
    assert df._all_variant_cols is False
    actual = {f.name.strip('"').lower(): type(f.datatype) for f in df.schema.fields}
    expected = {f.name.lower(): type(f.datatype) for f in expected_schema.fields}
    assert actual == expected
    result = df.collect()
    assert len(result) == 2
    assert len(result[0]) == 6

    book1 = df.filter(col('"_id"') == 1).collect()
    assert len(book1) == 1
    reviews = json.loads(book1[0]["reviews"])
    assert isinstance(reviews["review"], list)
    assert len(reviews["review"]) == 2
    assert reviews["review"][0]["user"] == "tech_guru_87"
    editions = json.loads(book1[0]["editions"])
    assert isinstance(editions["edition"], list)
    assert len(editions["edition"]) == 2

    book2 = df.filter(col('"_id"') == 2).collect()
    assert len(book2) == 1
    review_data = json.loads(book2[0]["reviews"])["review"]
    if isinstance(review_data, dict):
        assert review_data["user"] == "xml_master"
    else:
        assert review_data[0]["user"] == "xml_master"
    assert book1[0]["price"] == 29.99
    assert book2[0]["price"] == 35.50


def test_read_xml_namespace_infer_schema(session):
    """Infer schema on dk_trace_sample.xml with namespace-prefixed eqTrace:event rowTag."""
    expected_schema = StructType(
        [
            StructField("eqTrace:date-time", TimestampType()),
            StructField("eqTrace:equipment-cycle-status-changed", VariantType()),
            StructField("eqTrace:event-descriptor-list", VariantType()),
            StructField("eqTrace:event-id", StringType()),
            StructField("eqTrace:event-name", StringType()),
            StructField("eqTrace:event-version-number", DoubleType()),
            StructField("eqTrace:interchanged", VariantType()),
            StructField("eqTrace:is-planned", BooleanType()),
            StructField("eqTrace:is-synthetic", BooleanType()),
            StructField("eqTrace:location-id", VariantType()),
            StructField("eqTrace:placement-at-industry-planned", VariantType()),
            StructField("eqTrace:publisher-identification", StringType()),
            StructField("eqTrace:reporting-detail", VariantType()),
            StructField("eqTrace:tag-name-list", VariantType()),
            StructField("eqTrace:waybill-applied", VariantType()),
        ]
    )
    df = (
        session.read.option("rowTag", "eqTrace:event")
        .option("ignoreNamespace", False)
        .xml(f"@{tmp_stage_name}/{RES_DK_TRACE_XML}")
    )
    assert df._all_variant_cols is False
    actual = {f.name.strip('"'): type(f.datatype) for f in df.schema.fields}
    expected = {f.name.strip('"'): type(f.datatype) for f in expected_schema.fields}
    assert actual == expected
    result = df.collect()
    assert len(result) == 5

    first_event = df.filter(
        col('"eqTrace:event-id"') == "f0e765d9-599b-46bf-9aef-bd33e0c2183f"
    ).collect()
    assert len(first_event) == 1
    Utils.check_answer(
        df.filter(
            col('"eqTrace:event-id"') == "f0e765d9-599b-46bf-9aef-bd33e0c2183f"
        ).select(col('"eqTrace:event-name"'), col('"eqTrace:is-planned"')),
        [Row("equipment/equipment-placement-at-industry-planned", True)],
    )

    second_event = df.filter(
        col('"eqTrace:event-id"') == "dd9a4616-e41c-4571-9bda-9a5506a2b78d"
    ).collect()
    assert len(second_event) == 1
    assert second_event[0]["eqTrace:is-planned"] is False


def test_read_xml_dblp_mastersthesis_infer_schema(session):
    """Infer schema on dblp_6kb.xml mastersthesis: verify schema, nested ee data, printSchema."""
    expected_schema = StructType(
        [
            StructField("_key", StringType()),
            StructField("_mdate", DateType()),
            StructField("author", StringType()),
            StructField("ee", VariantType()),
            StructField("note", StringType()),
            StructField("school", StringType()),
            StructField("title", StringType()),
            StructField("year", LongType()),
        ]
    )
    df = session.read.option("rowTag", "mastersthesis").xml(
        f"@{tmp_stage_name}/{RES_DBLP_XML}"
    )
    assert df._all_variant_cols is False
    actual = {f.name.strip('"').lower(): type(f.datatype) for f in df.schema.fields}
    expected = {f.name.lower(): type(f.datatype) for f in expected_schema.fields}
    assert actual == expected
    result = df.collect()
    assert len(result) == 6

    hoffmann = df.filter(col('"_key"') == "ms/Hoffmann2008").collect()
    assert len(hoffmann) == 1
    ee = json.loads(hoffmann[0]["ee"])
    assert ee["_type"] == "oa"
    assert "dblp.uni-trier.de" in ee["_VALUE"]

    vollmer = df.filter(col('"_key"') == "ms/Vollmer2006").collect()
    assert len(vollmer) == 1
    ee_v = json.loads(vollmer[0]["ee"])
    assert ee_v["_type"] is None
    assert ee_v["_VALUE"] is not None

    brown = df.filter(col('"_key"') == "ms/Brown92").collect()
    assert len(brown) == 1
    assert json.loads(brown[0]["ee"]) == {"_VALUE": None, "_type": None}

    schema_str = df._format_schema()
    assert "LongType" in schema_str
    assert "StringType" in schema_str
    assert "VariantType" in schema_str


def test_read_xml_dblp_incollection_infer_schema(session):
    """Infer schema on dblp_6kb.xml incollection: author array, ee struct, verify data."""
    expected_schema = StructType(
        [
            StructField("_corrupt_record", StringType()),
            StructField("_key", StringType()),
            StructField("_mdate", DateType()),
            StructField("author", VariantType()),
            StructField("booktitle", StringType()),
            StructField("crossref", StringType()),
            StructField("ee", VariantType()),
            StructField("pages", StringType()),
            StructField("title", StringType()),
            StructField("url", StringType()),
            StructField("year", LongType()),
        ]
    )
    df = session.read.option("rowTag", "incollection").xml(
        f"@{tmp_stage_name}/{RES_DBLP_XML}"
    )
    assert df._all_variant_cols is False
    actual = {f.name.strip('"').lower(): type(f.datatype) for f in df.schema.fields}
    expected = {f.name.lower(): type(f.datatype) for f in expected_schema.fields}
    assert actual == expected
    result = df.collect()
    assert len(result) == 6

    parker = df.filter(col('"_key"') == "series/ifip/ParkerD14").collect()
    assert len(parker) == 1
    authors = json.loads(parker[0]["author"])
    assert isinstance(authors, list)
    assert len(authors) == 2
    assert authors[0]["_VALUE"] == "Kevin R. Parker"
    assert authors[0]["_orcid"] == "0000-0003-0549-3687"
    ee = json.loads(parker[0]["ee"])
    assert ee["_type"] == "oa"

    lecomber = df.filter(col('"_key"') == "series/ifip/Lecomber14").collect()
    assert len(lecomber) == 1
    author_val = json.loads(lecomber[0]["author"])
    if isinstance(author_val, dict):
        assert author_val["_VALUE"] == "Angela Lecomber"
        assert author_val["_orcid"] is None
    elif isinstance(author_val, list):
        assert len(author_val) == 1
        assert author_val[0]["_VALUE"] == "Angela Lecomber"


def test_read_xml_attribute_value_infer_schema(session):
    """Infer schema on books_attribute_value.xml: publisher mixed struct + plain text."""
    expected_schema = StructType(
        [
            StructField("_id", LongType()),
            StructField("author", StringType()),
            StructField("pages", LongType()),
            StructField("price", DoubleType()),
            StructField("publisher", VariantType()),
            StructField("title", StringType()),
        ]
    )
    df = session.read.option("rowTag", "book").xml(
        f"@{tmp_stage_name}/{RES_BOOKS_ATTR_VAL_XML}"
    )
    assert df._all_variant_cols is False
    actual = {f.name.strip('"').lower(): type(f.datatype) for f in df.schema.fields}
    expected = {f.name.lower(): type(f.datatype) for f in expected_schema.fields}
    assert actual == expected
    result = df.collect()
    assert len(result) == 5
    assert len(result[0]) == 6

    book1 = df.filter(col('"_id"') == 1).collect()
    assert len(book1) == 1
    pub1 = json.loads(book1[0]["publisher"])
    assert pub1 == {"_VALUE": "O'Reilly Media", "_country": "USA", "_language": None}

    book3 = df.filter(col('"_id"') == 3).collect()
    assert len(book3) == 1
    pub3 = json.loads(book3[0]["publisher"])
    assert pub3 == {"_VALUE": "Springer", "_country": "Canada", "_language": "English"}

    book4 = df.filter(col('"_id"') == 4).collect()
    assert len(book4) == 1
    pub4 = json.loads(book4[0]["publisher"])
    assert pub4 == {"_VALUE": "Some Publisher", "_country": None, "_language": None}

    book5 = df.filter(col('"_id"') == 5).collect()
    assert len(book5) == 1
    pub5 = json.loads(book5[0]["publisher"])
    assert pub5 == {"_VALUE": None, "_country": None, "_language": None}


# ─── samplingRatio tests ─────────────────────────────────────────────────────


def test_sampling_ratio_schema_books_flat(session):
    """samplingRatio=0.5 on homogeneous books.xml: correct schema and deterministic across runs."""
    path = f"@{tmp_stage_name}/{RES_BOOKS_XML}"
    schemas = []
    for _ in range(3):
        df = (
            session.read.option("rowTag", "book").option("samplingRatio", 0.5).xml(path)
        )
        schemas.append(df.schema)

    assert schemas[0] == schemas[1] == schemas[2]
    assert df._all_variant_cols is False
    assert _schema_types(df) == {
        "_id": StringType,
        "author": StringType,
        "description": StringType,
        "genre": StringType,
        "price": DoubleType,
        "publish_date": DateType,
        "title": StringType,
    }
    assert df.count() == 12


@pytest.mark.parametrize("invalid_ratio", [0, -0.5])
def test_sampling_ratio_invalid(session, invalid_ratio):
    with pytest.raises(ValueError, match="should be greater than 0"):
        session.read.option("rowTag", "ROW").option("samplingRatio", invalid_ratio).xml(
            f"@{tmp_stage_name}/{_staged_files['primitives']}"
        )


def test_sampling_ratio_hetero_may_narrow_schema(session):
    """Low samplingRatio on heterogeneous data may infer a narrower schema."""
    path = f"@{tmp_stage_name}/{_staged_files['sampling_hetero']}"

    df_full = session.read.option("rowTag", "ROW").xml(path)
    assert _schema_types(df_full)["value"] == DoubleType

    df_sampled = (
        session.read.option("rowTag", "ROW").option("samplingRatio", 0.3).xml(path)
    )
    types_sampled = _schema_types(df_sampled)
    assert types_sampled["value"] in (LongType, DoubleType)
    assert types_sampled["name"] == StringType
    assert df_sampled.count() == 20


def test_sampling_ratio_nested_schema_preserved(session):
    """samplingRatio < 1.0 on nested data still infers nested structure."""
    df = (
        session.read.option("rowTag", "book")
        .option("samplingRatio", 0.5)
        .xml(f"@{tmp_stage_name}/{RES_BOOKS2_XML}")
    )
    assert df._all_variant_cols is False
    assert _schema_types(df) == {
        "_id": LongType,
        "author": StringType,
        "editions": VariantType,
        "price": DoubleType,
        "reviews": VariantType,
        "title": StringType,
    }
    assert df.count() == 2
    book1 = df.filter(col('"_id"') == 1).collect()[0]
    reviews = json.loads(book1["reviews"])
    assert isinstance(reviews["review"], list)
    assert len(reviews["review"]) == 2


def test_infer_schema_non_existing_file(session):
    with pytest.raises(ValueError, match="does not exist"):
        session.read.option("rowTag", "row").xml(
            f"@{tmp_stage_name}/non_existing_file.xml"
        )


def test_infer_schema_use_leaf_row_tag(session):
    xml_content = "<root><item>hello</item><item>world</item></root>"
    actual_filename = _upload_xml_string(
        session, tmp_stage_name, "leaf_only_infer.xml", xml_content
    )
    df = session.read.option("rowTag", "item").xml(
        f"@{tmp_stage_name}/{actual_filename}"
    )
    assert df.count() == 2
    assert "_VALUE" in [f.name for f in df.schema.fields] or len(df.schema.fields) == 1


# ─── inferSchema=False ──────────────────────────────


def _assert_all_leaves_string(dt):
    if isinstance(dt, StructType):
        for f in dt.fields:
            _assert_all_leaves_string(f.datatype)
    elif isinstance(dt, ArrayType):
        _assert_all_leaves_string(dt.element_type)
    else:
        assert isinstance(
            dt, StringType
        ), f"Expected StringType leaf, got {type(dt).__name__}"


@pytest.mark.parametrize(
    "res_file, row_tag, extra_opts, expected_count, expected_variant_cols",
    [
        (RES_BOOKS2_XML, "book", {}, 2, {"editions", "reviews"}),
        (
            RES_DK_TRACE_XML,
            "eqTrace:event",
            {"ignoreNamespace": False},
            5,
            {
                "eqtrace:equipment-cycle-status-changed",
                "eqtrace:event-descriptor-list",
                "eqtrace:interchanged",
                "eqtrace:location-id",
                "eqtrace:placement-at-industry-planned",
                "eqtrace:reporting-detail",
                "eqtrace:tag-name-list",
                "eqtrace:waybill-applied",
            },
        ),
        (RES_DBLP_XML, "mastersthesis", {}, 6, {"ee"}),
        (RES_DBLP_XML, "incollection", {}, 6, {"author", "ee"}),
        (RES_BOOKS_ATTR_VAL_XML, "book", {}, 5, {"publisher"}),
        (RES_BOOKS_XML, "book", {}, 12, set()),
        ("xml_infer_types.xml", "item", {}, 3, {"price", "tags", "specs", "rating"}),
        ("primitives", "ROW", {}, 1, set()),
        ("nested_object", "book", {}, 2, {"info"}),
        ("unbalanced_types", "ROW", {}, 2, set()),
    ],
)
def test_infer_schema_false_resource_files(
    session, res_file, row_tag, extra_opts, expected_count, expected_variant_cols
):
    """inferSchema=False on resource files: complex fields stay VariantType,
    scalar fields become StringType, all inferred leaves are StringType."""
    reader_false = session.read.option("rowTag", row_tag).option("inferSchema", False)
    for k, v in extra_opts.items():
        reader_false = reader_false.option(k, v)
    file_name = _staged_files.get(res_file, res_file)
    path = f"@{tmp_stage_name}/{file_name}"

    df = reader_false.xml(path)

    assert df._all_variant_cols is False
    assert len(df.collect()) == expected_count

    types = _schema_types(df)
    for field_name, type_cls in types.items():
        if field_name in expected_variant_cols:
            assert (
                type_cls == VariantType
            ), f"{field_name}: expected VariantType, got {type_cls.__name__}"
        else:
            assert (
                type_cls == StringType
            ), f"{field_name}: expected StringType, got {type_cls.__name__}"

    schema_false = reader_false._xml_inferred_schema
    assert schema_false is not None
    _assert_all_leaves_string(schema_false)


def test_user_schema_takes_precedence_over_infer_schema(session):
    """User schema overrides inference regardless of inferSchema value."""
    user_schema = StructType(
        [
            StructField("_id", LongType()),
            StructField("author", StringType()),
            StructField("title", StringType()),
        ]
    )
    path = f"@{tmp_stage_name}/{RES_BOOKS2_XML}"
    for infer in (True, False):
        df = (
            session.read.option("rowTag", "book")
            .option("inferSchema", infer)
            .schema(user_schema)
            .xml(path)
        )
        types = _schema_types(df)
        assert types == {"_id": LongType, "author": StringType, "title": StringType}
        assert df.count() == 2
