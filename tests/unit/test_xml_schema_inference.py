#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io
from contextlib import ExitStack
from unittest import mock
from unittest.mock import patch

import lxml.etree as ET
import pytest

from snowflake.snowpark._internal.xml_schema_inference import (
    _normalize_text,
    infer_type,
    infer_element_schema,
    compatible_type,
    merge_struct_types,
    add_or_update_type,
    canonicalize_type,
    _case_preserving_simple_string,
    _struct_has_value_tag,
    _merge_struct_with_primitive,
    infer_schema_for_xml_range,
    XMLSchemaInference,
)
from snowflake.snowpark import DataFrameReader
import snowflake.snowpark.dataframe_reader as _dr_mod
from snowflake.snowpark.types import (
    StructType,
    StructField,
    ArrayType,
    MapType,
    NullType,
    StringType,
    BooleanType,
    LongType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
)


def _xml(xml_str: str) -> ET.Element:
    """Parse an XML string into an lxml Element."""
    return ET.fromstring(xml_str)


def _infer_and_merge(xml_records, **kwargs):
    """Infer schema for each record and merge, mimicking the UDTF behavior."""
    merged = None
    value_tag = kwargs.get("value_tag", "_VALUE")
    for rec_str in xml_records:
        rec_str = rec_str.strip()
        if not rec_str:
            continue
        try:
            elem = _xml(rec_str)
        except Exception:
            continue
        schema = infer_element_schema(elem, **kwargs)
        if not isinstance(schema, StructType):
            schema = StructType(
                [StructField(value_tag, schema, nullable=True)],
                structured=False,
            )
        if merged is None:
            merged = schema
        else:
            merged = merge_struct_types(merged, schema, value_tag)
    return merged


def _full_pipeline(xml_records, **kwargs):
    """Run the full schema inference pipeline: infer + merge + canonicalize."""
    merged = _infer_and_merge(xml_records, **kwargs)
    if merged is not None:
        merged = canonicalize_type(merged)
    return merged


# ===========================================================================
# _normalize_text
# ===========================================================================


@pytest.mark.parametrize(
    "text, strip, expected",
    [
        (None, True, None),
        (None, False, None),
        ("  hello  ", True, "hello"),
        ("\n\thello\n\t", True, "hello"),
        ("  hello  ", False, "  hello  "),
        ("", True, ""),
        ("", False, ""),
        ("   ", True, ""),
        ("   ", False, "   "),
    ],
)
def test_normalize_text(text, strip, expected):
    assert _normalize_text(text, strip) == expected


# ===========================================================================
# infer_type
# ===========================================================================


@pytest.mark.parametrize(
    "value, kwargs",
    [
        (None, {}),
        ("", {}),
    ],
)
def test_infer_type_null(value, kwargs):
    """Null/empty inputs → NullType."""
    assert isinstance(infer_type(value, **kwargs), NullType)


@pytest.mark.parametrize(
    "value",
    [
        "42",
        "-42",
        "0",
        str(2**63 - 1),
        str(-(2**63)),
        "+123",
        "1",
        "+0",
        "-0",
        "007",
    ],
)
def test_infer_type_long(value):
    """Integer strings within Long range → LongType."""
    assert isinstance(infer_type(value), LongType)


@pytest.mark.parametrize(
    "value",
    [
        "3.14",
        "1.5e10",
        "-2.5",
        ".5",
        "0.0",
        "+3.14",
        "NaN",
        "Infinity",
        "+Infinity",
        "-Infinity",
        "92233720368547758070",
        "-92233720368547758080",
        "1.7976931348623157e+308",
    ],
)
def test_infer_type_double(value):
    """Decimals, scientific notation, special floats, and beyond-Long ints → DoubleType."""
    assert isinstance(infer_type(value), DoubleType)


@pytest.mark.parametrize("value", ["true", "false", "True", "FALSE"])
def test_infer_type_boolean(value):
    assert isinstance(infer_type(value), BooleanType)


@pytest.mark.parametrize("value", ["2024-01-15", "2000-12-31", "1999-01-01"])
def test_infer_type_date(value):
    assert isinstance(infer_type(value), DateType)


@pytest.mark.parametrize(
    "value",
    ["2024-01-15T10:30:00", "2024-01-15T10:30:00+00:00", "2011-12-03T10:15:30Z"],
)
def test_infer_type_timestamp(value):
    assert isinstance(infer_type(value), TimestampType)


@pytest.mark.parametrize(
    "value",
    [
        "hello",
        "abc123",
        "foo@bar.com",
        "inf",
        "Inf",
        "+inf",
        "+Inf",
        "1.5d",
        "1.5f",
        "1.5D",
        "1.5F",
    ],
)
def test_infer_type_string(value):
    """Plain strings and values rejected by d/D/f/F suffix check → StringType."""
    assert isinstance(infer_type(value), StringType)


def test_infer_type_whitespace_stripped():
    """With ignore_surrounding_whitespace, '  42  ' is parsed as Long."""
    assert isinstance(
        infer_type("  42  ", ignore_surrounding_whitespace=True), LongType
    )
    assert isinstance(
        infer_type("  42  ", ignore_surrounding_whitespace=False), StringType
    )


# ===========================================================================
# infer_element_schema – leaf elements
# ===========================================================================


@pytest.mark.parametrize(
    "xml_str, expected_type, kwargs",
    [
        ("<a>42</a>", LongType, {}),
        ("<a></a>", NullType, {}),
        ("<a/>", NullType, {}),
        ("<a>N/A</a>", StringType, {}),
        ("<a>true</a>", BooleanType, {}),
        ("<a>false</a>", BooleanType, {}),
        ("<a>3.14</a>", DoubleType, {}),
        ("<a>hello</a>", StringType, {}),
        ("<a>2024-01-15</a>", DateType, {}),
        ("<a>2024-01-15T10:30:00</a>", TimestampType, {}),
        ("<a>2024-01-15T10:30:00Z</a>", TimestampType, {}),
        ("<a>NaN</a>", DoubleType, {}),
        ("<a>92233720368547758070</a>", DoubleType, {}),
    ],
)
def test_infer_element_schema_leaf(xml_str, expected_type, kwargs):
    """Leaf elements (no children, no attributes) infer scalar types."""
    assert isinstance(infer_element_schema(_xml(xml_str), **kwargs), expected_type)


# ===========================================================================
# infer_element_schema – attributes
# ===========================================================================


def test_infer_element_schema_attributes_only_root():
    """Root-level attribute-only element has no _VALUE."""
    result = infer_element_schema(_xml('<a name="Alice" age="30"/>'), is_root=True)
    assert isinstance(result, StructType)
    field_names = {f._name for f in result.fields}
    assert "_name" in field_names and "_age" in field_names
    assert "_VALUE" not in field_names


def test_infer_element_schema_attributes_only_child():
    """Child-level attribute-only element adds _VALUE as NullType."""
    result = infer_element_schema(_xml('<a name="Alice"/>'), is_root=False)
    field_names = {f._name for f in result.fields}
    assert "_name" in field_names and "_VALUE" in field_names


def test_infer_element_schema_attributes_with_text():
    """Element with attributes and text adds _VALUE."""
    result = infer_element_schema(_xml('<a name="Alice">hello</a>'), is_root=True)
    field_names = {f._name for f in result.fields}
    assert "_name" in field_names and "_VALUE" in field_names


def test_infer_element_schema_custom_attribute_prefix():
    result = infer_element_schema(
        _xml('<a name="Alice"/>'), attribute_prefix="@", is_root=True
    )
    assert "@name" in {f._name for f in result.fields}


def test_infer_element_schema_exclude_attributes():
    """When exclude_attributes=True, attributes are ignored."""
    assert isinstance(
        infer_element_schema(
            _xml('<a name="Alice">hello</a>'), exclude_attributes=True
        ),
        StringType,
    )
    assert isinstance(
        infer_element_schema(_xml('<a name="Alice"/>'), exclude_attributes=True),
        NullType,
    )


# ===========================================================================
# infer_element_schema – children
# ===========================================================================


def test_infer_element_schema_simple_children():
    result = infer_element_schema(_xml("<row><a>1</a><b>hello</b></row>"))
    assert isinstance(result, StructType)
    assert {"a", "b"} <= {f._name for f in result.fields}


def test_infer_element_schema_child_types():
    result = infer_element_schema(_xml("<row><a>42</a><b>true</b><c>3.14</c></row>"))
    field_map = {f._name: f.datatype for f in result.fields}
    assert isinstance(field_map["a"], LongType)
    assert isinstance(field_map["b"], BooleanType)
    assert isinstance(field_map["c"], DoubleType)


def test_infer_element_schema_repeated_children_array():
    """Repeated child tags → ArrayType."""
    result = infer_element_schema(
        _xml("<row><item>1</item><item>2</item><item>3</item></row>")
    )
    field_map = {f._name: f.datatype for f in result.fields}
    assert isinstance(field_map["item"], ArrayType)
    assert isinstance(field_map["item"].element_type, LongType)


def test_infer_element_schema_nested_children():
    result = infer_element_schema(
        _xml("<row><outer><inner>hello</inner></outer></row>")
    )
    outer_field = next(f for f in result.fields if f._name == "outer")
    assert isinstance(outer_field.datatype, StructType)
    inner_field = next(f for f in outer_field.datatype.fields if f._name == "inner")
    assert isinstance(inner_field.datatype, StringType)


def test_infer_element_schema_child_with_attributes():
    result = infer_element_schema(_xml('<row><child id="1">text</child></row>'))
    child_field = next(f for f in result.fields if f._name == "child")
    assert isinstance(child_field.datatype, StructType)
    child_names = {f._name for f in child_field.datatype.fields}
    assert "_id" in child_names and "_VALUE" in child_names


# ===========================================================================
# infer_element_schema – mixed content
# ===========================================================================


def test_infer_element_schema_mixed_content_value_tag():
    """Element with text and children: text goes into _VALUE."""
    result = infer_element_schema(_xml("<row>some text<a>1</a></row>"))
    field_names = {f._name for f in result.fields}
    assert "_VALUE" in field_names and "a" in field_names


def test_infer_element_schema_whitespace_only_no_value_tag():
    """Whitespace-only text between children does not create _VALUE."""
    result = infer_element_schema(_xml("<row>\n  <a>1</a>\n  <b>2</b>\n</row>"))
    assert "_VALUE" not in {f._name for f in result.fields}


# ===========================================================================
# infer_element_schema – namespace handling
# ===========================================================================


def test_infer_element_schema_namespace_clark():
    """Clark notation {uri}tag → strip namespace part."""
    from snowflake.snowpark._internal.xml_reader import strip_xml_namespaces

    elem = strip_xml_namespaces(
        ET.fromstring('<row xmlns="http://example.com"><child>val</child></row>')
    )
    result = infer_element_schema(elem, ignore_namespace=True)
    assert "child" in {f._name for f in result.fields}


def test_infer_element_schema_namespace_prefix():
    parser = ET.XMLParser(recover=True, ns_clean=True)
    elem = ET.fromstring("<px:row><px:child>val</px:child></px:row>", parser)
    result = infer_element_schema(elem, ignore_namespace=True)
    assert "child" in {f._name for f in result.fields}


# ===========================================================================
# infer_element_schema – whitespace and sorting
# ===========================================================================


def test_infer_element_schema_whitespace_leaf():
    assert isinstance(
        infer_element_schema(_xml("<a>  42  </a>"), ignore_surrounding_whitespace=True),
        LongType,
    )
    assert isinstance(
        infer_element_schema(
            _xml("<a>  42  </a>"), ignore_surrounding_whitespace=False
        ),
        StringType,
    )


def test_infer_element_schema_fields_sorted():
    result = infer_element_schema(_xml("<row><z>1</z><a>2</a><m>3</m></row>"))
    names = [f._name for f in result.fields]
    assert names == sorted(names)


def test_infer_element_schema_custom_value_tag():
    result = infer_element_schema(_xml('<row attr="1">text</row>'), value_tag="myval")
    field_names = {f._name for f in result.fields}
    assert "myval" in field_names and "_VALUE" not in field_names


# ===========================================================================
# Spark TestXmlData-derived tests (multi-record infer + merge)
# ===========================================================================


def test_spark_primitive_field_value_type_conflict():
    """Merging records with conflicting types for same fields."""
    records = [
        """<ROW>
          <num_num_1>11</num_num_1><num_num_2/><num_num_3>1.1</num_num_3>
          <num_bool>true</num_bool><num_str>13.1</num_str><str_bool>str1</str_bool>
        </ROW>""",
        """<ROW>
          <num_num_1/><num_num_2>21474836470.9</num_num_2><num_num_3/>
          <num_bool>12</num_bool><num_str/><str_bool>true</str_bool>
        </ROW>""",
        """<ROW>
          <num_num_1>21474836470</num_num_1><num_num_2>92233720368547758070</num_num_2>
          <num_num_3>100</num_num_3><num_bool>false</num_bool>
          <num_str>str1</num_str><str_bool>false</str_bool>
        </ROW>""",
        """<ROW>
          <num_num_1>21474836570</num_num_1><num_num_2>1.1</num_num_2>
          <num_num_3>21474836470</num_num_3><num_bool/>
          <num_str>92233720368547758070</num_str><str_bool/>
        </ROW>""",
    ]
    merged = _infer_and_merge(records)
    fm = {f._name: f.datatype for f in canonicalize_type(merged).fields}
    assert isinstance(fm["num_bool"], StringType)
    assert isinstance(fm["num_num_1"], LongType)
    assert isinstance(fm["num_num_2"], DoubleType)
    assert isinstance(fm["num_num_3"], DoubleType)
    assert isinstance(fm["num_str"], StringType)
    assert isinstance(fm["str_bool"], StringType)


def test_spark_complex_field_value_type_conflict():
    """Merging arrays with singletons and struct/primitive conflicts."""
    records = [
        """<ROW>
          <num_struct>11</num_struct>
          <str_array>1</str_array><str_array>2</str_array><str_array>3</str_array>
          <array></array><struct_array></struct_array><struct></struct>
        </ROW>""",
        """<ROW>
          <num_struct><field>false</field></num_struct>
          <str_array/><array/><struct_array></struct_array><struct/>
        </ROW>""",
        """<ROW>
          <num_struct/>
          <str_array>str</str_array>
          <array>4</array><array>5</array><array>6</array>
          <struct_array>7</struct_array><struct_array>8</struct_array>
          <struct_array>9</struct_array>
          <struct><field/></struct>
        </ROW>""",
        """<ROW>
          <num_struct></num_struct>
          <str_array>str1</str_array><str_array>str2</str_array><str_array>33</str_array>
          <array>7</array>
          <struct_array><field>true</field></struct_array>
          <struct><field>str</field></struct>
        </ROW>""",
    ]
    merged = _infer_and_merge(records, ignore_surrounding_whitespace=True)
    fm = {f._name: f.datatype for f in canonicalize_type(merged).fields}
    assert isinstance(fm["array"], ArrayType) and isinstance(
        fm["array"].element_type, LongType
    )
    assert isinstance(fm["num_struct"], StringType)
    assert isinstance(fm["str_array"], ArrayType) and isinstance(
        fm["str_array"].element_type, StringType
    )
    assert isinstance(fm["struct"], StructType)
    assert isinstance(fm["struct_array"], ArrayType) and isinstance(
        fm["struct_array"].element_type, StringType
    )


def test_spark_missing_fields():
    """Different records have different fields; merge produces superset."""
    records = [
        "<ROW><a>true</a></ROW>",
        "<ROW><b>21474836470</b></ROW>",
        "<ROW><c>33</c><c>44</c></ROW>",
        "<ROW><d><field>true</field></d></ROW>",
        "<ROW><e>str</e></ROW>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["a"], BooleanType)
    assert isinstance(fm["b"], LongType)
    assert isinstance(fm["c"], ArrayType) and isinstance(fm["c"].element_type, LongType)
    assert isinstance(fm["d"], StructType)
    assert isinstance(fm["e"], StringType)


def test_spark_null_struct():
    """Null/empty struct fields merged correctly."""
    records = [
        """<ROW><nullstr></nullstr><ip>27.31.100.29</ip>
          <headers><Host>1.abc.com</Host><Charset>UTF-8</Charset></headers></ROW>""",
        "<ROW><nullstr></nullstr><ip>27.31.100.29</ip><headers/></ROW>",
        "<ROW><nullstr></nullstr><ip>27.31.100.29</ip><headers></headers></ROW>",
        "<ROW><nullstr/><ip>27.31.100.29</ip><headers/></ROW>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["nullstr"], StringType)
    assert isinstance(fm["ip"], StringType)
    assert isinstance(fm["headers"], StructType)
    hf = {f._name: f.datatype for f in fm["headers"].fields}
    assert isinstance(hf["Host"], StringType) and isinstance(hf["Charset"], StringType)


def test_spark_empty_records():
    """Structs with empty/null children properly handled."""
    records = [
        "<ROW><a><struct></struct></a></ROW>",
        "<ROW><a><struct><b><c/></b></struct></a></ROW>",
        "<ROW><b><item><c><struct></struct></c></item><item/></b></ROW>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["a"], StructType)
    a_struct = {f._name: f.datatype for f in fm["a"].fields}["struct"]
    b_c = {
        f._name: f.datatype
        for f in {f._name: f.datatype for f in a_struct.fields}["b"].fields
    }["c"]
    assert isinstance(b_c, StringType)
    assert isinstance(fm["b"], StructType)
    assert isinstance({f._name: f.datatype for f in fm["b"].fields}["item"], ArrayType)


def test_spark_nulls_in_arrays():
    """Null entries in arrays don't disrupt type inference."""
    records = [
        """<ROW><field1><array1><array2>value1</array2><array2>value2</array2></array1>
          <array1/></field1><field1/></ROW>""",
        """<ROW><field2/><field2><array1><Test>1</Test></array1>
          <array1/></field2></ROW>""",
        "<ROW><field1/><field1><array1/></field1><field2/></ROW>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["field1"], ArrayType)
    assert isinstance(fm["field2"], ArrayType)


def test_spark_value_tags_type_conflict():
    """Mixed text and children with type conflicts in _VALUE.

    lxml element.text only captures text BEFORE the first child.
    Tail text is not captured, diverging from Spark's StAX parser.
    """
    records = [
        "<ROW>\n13.1\n<a>\n11\n<b>\ntrue\n<c>1</c></b></a></ROW>",
        "<ROW>\nstring\n<a>\n21474836470\n<b>\nfalse\n<c>2</c></b></a></ROW>",
        "<ROW><a><b>\n12\n<c>3</c></b></a></ROW>",
    ]
    fm = {
        f._name: f.datatype
        for f in _full_pipeline(records, ignore_surrounding_whitespace=True).fields
    }
    assert isinstance(fm["_VALUE"], StringType)
    assert isinstance(fm["a"], StructType)
    af = {f._name: f.datatype for f in fm["a"].fields}
    assert isinstance(af["_VALUE"], LongType)
    bf = {f._name: f.datatype for f in af["b"].fields}
    assert isinstance(bf["_VALUE"], StringType)
    assert isinstance(bf["c"], LongType)


def test_spark_value_tag_conflict_name():
    """When valueTag="a" conflicts with child <a>, they merge.

    lxml element.text only captures text BEFORE the first child.
    """
    records_before = ["<ROW>\n2\n<a>1</a></ROW>"]
    fm = {
        f._name: f.datatype
        for f in _full_pipeline(
            records_before, value_tag="a", ignore_surrounding_whitespace=True
        ).fields
    }
    assert isinstance(fm["a"], ArrayType) and isinstance(fm["a"].element_type, LongType)

    records_after = ["<ROW><a>1</a>\n2\n</ROW>"]
    fm2 = {
        f._name: f.datatype
        for f in _full_pipeline(
            records_after, value_tag="a", ignore_surrounding_whitespace=True
        ).fields
    }
    assert isinstance(fm2["a"], LongType)


# ===========================================================================
# compatible_type
# ===========================================================================


@pytest.mark.parametrize(
    "t1, t2, expected_type",
    [
        (LongType(), LongType(), LongType),
        (StringType(), StringType(), StringType),
        (BooleanType(), BooleanType(), BooleanType),
        (DoubleType(), DoubleType(), DoubleType),
        (DateType(), DateType(), DateType),
        (TimestampType(), TimestampType(), TimestampType),
        (NullType(), NullType(), NullType),
    ],
)
def test_compatible_type_same(t1, t2, expected_type):
    """Same types stay the same."""
    assert isinstance(compatible_type(t1, t2), expected_type)


@pytest.mark.parametrize(
    "t1, t2, expected_type",
    [
        (NullType(), LongType(), LongType),
        (LongType(), NullType(), LongType),
        (NullType(), StringType(), StringType),
    ],
)
def test_compatible_type_null_plus_t(t1, t2, expected_type):
    """NullType + T → T."""
    assert isinstance(compatible_type(t1, t2), expected_type)


def test_compatible_type_null_plus_struct():
    s = StructType([StructField("a", LongType(), True)])
    assert isinstance(compatible_type(NullType(), s), StructType)


def test_compatible_type_null_plus_array():
    a = ArrayType(LongType(), structured=False)
    assert isinstance(compatible_type(NullType(), a), ArrayType)


@pytest.mark.parametrize(
    "t1, t2",
    [(LongType(), DoubleType()), (DoubleType(), LongType())],
)
def test_compatible_type_long_double_widening(t1, t2):
    """Long + Double → Double."""
    assert isinstance(compatible_type(t1, t2), DoubleType)


@pytest.mark.parametrize(
    "t1, t2",
    [(DoubleType(), DecimalType(10, 2)), (DecimalType(10, 2), DoubleType())],
)
def test_compatible_type_double_decimal(t1, t2):
    """Double + Decimal → Double."""
    assert isinstance(compatible_type(t1, t2), DoubleType)


@pytest.mark.parametrize(
    "t1, t2",
    [(TimestampType(), DateType()), (DateType(), TimestampType())],
)
def test_compatible_type_timestamp_date(t1, t2):
    """Timestamp + Date → Timestamp."""
    assert isinstance(compatible_type(t1, t2), TimestampType)


def test_compatible_type_long_plus_decimal():
    result = compatible_type(LongType(), DecimalType(10, 2))
    assert (
        isinstance(result, DecimalType) and result.precision == 22 and result.scale == 2
    )

    result2 = compatible_type(DecimalType(10, 2), LongType())
    assert (
        isinstance(result2, DecimalType)
        and result2.precision == 22
        and result2.scale == 2
    )


def test_compatible_type_same_struct_merge():
    s1 = StructType(
        [StructField("a", LongType(), True), StructField("b", StringType(), True)]
    )
    s2 = StructType(
        [StructField("a", DoubleType(), True), StructField("c", BooleanType(), True)]
    )
    result = compatible_type(s1, s2)
    fm = {f._name: f.datatype for f in result.fields}
    assert (
        isinstance(fm["a"], DoubleType)
        and isinstance(fm["b"], StringType)
        and isinstance(fm["c"], BooleanType)
    )


def test_compatible_type_same_array_merge():
    result = compatible_type(
        ArrayType(LongType(), structured=False),
        ArrayType(DoubleType(), structured=False),
    )
    assert isinstance(result, ArrayType) and isinstance(result.element_type, DoubleType)


def test_compatible_type_decimal_widen():
    result = compatible_type(DecimalType(10, 2), DecimalType(15, 5))
    assert (
        isinstance(result, DecimalType) and result.precision == 15 and result.scale == 5
    )


def test_compatible_type_decimal_overflow_to_double():
    """Decimal with precision > 38 falls back to Double."""
    result = compatible_type(DecimalType(38, 10), DecimalType(38, 20))
    assert isinstance(result, DoubleType)


def test_compatible_type_array_plus_primitive():
    result = compatible_type(ArrayType(LongType(), structured=False), DoubleType())
    assert isinstance(result, ArrayType) and isinstance(result.element_type, DoubleType)

    result2 = compatible_type(LongType(), ArrayType(StringType(), structured=False))
    assert isinstance(result2, ArrayType) and isinstance(
        result2.element_type, StringType
    )


def test_compatible_type_struct_value_tag_plus_primitive():
    s = StructType(
        [
            StructField("_VALUE", LongType(), True),
            StructField("_attr", StringType(), True),
        ]
    )
    result = compatible_type(s, DoubleType())
    fm = {f._name: f.datatype for f in result.fields}
    assert isinstance(fm["_VALUE"], DoubleType) and isinstance(fm["_attr"], StringType)

    s2 = StructType(
        [
            StructField("_VALUE", StringType(), True),
            StructField("_id", LongType(), True),
        ]
    )
    result2 = compatible_type(BooleanType(), s2)
    assert isinstance(
        {f._name: f.datatype for f in result2.fields}["_VALUE"], StringType
    )


@pytest.mark.parametrize(
    "t1, t2",
    [
        (BooleanType(), LongType()),
        (StringType(), LongType()),
        (BooleanType(), DoubleType()),
        (DateType(), LongType()),
        (StringType(), BooleanType()),
        (DateType(), DoubleType()),
        (BooleanType(), DateType()),
        (StringType(), DateType()),
        (LongType(), BooleanType()),
    ],
)
def test_compatible_type_fallback_string(t1, t2):
    """Incompatible types → StringType."""
    assert isinstance(compatible_type(t1, t2), StringType)


def test_compatible_type_struct_no_value_tag_plus_primitive():
    """Struct without _VALUE + primitive → StringType fallback."""
    s = StructType([StructField("a", LongType(), True)])
    assert isinstance(compatible_type(s, LongType()), StringType)


# ===========================================================================
# _struct_has_value_tag
# ===========================================================================


def test_struct_has_value_tag():
    s_yes = StructType(
        [StructField("_VALUE", LongType(), True), StructField("a", StringType(), True)]
    )
    assert _struct_has_value_tag(s_yes, "_VALUE") is True

    s_no = StructType([StructField("a", LongType(), True)])
    assert _struct_has_value_tag(s_no, "_VALUE") is False

    s_custom = StructType([StructField("myval", LongType(), True)])
    assert _struct_has_value_tag(s_custom, "MYVAL") is True
    assert _struct_has_value_tag(s_custom, "myval") is False
    assert _struct_has_value_tag(s_custom, "_VALUE") is False


# ===========================================================================
# _merge_struct_with_primitive
# ===========================================================================


def test_merge_struct_with_primitive():
    s = StructType(
        [
            StructField("_VALUE", LongType(), True),
            StructField("_attr", StringType(), True),
        ]
    )
    result = _merge_struct_with_primitive(s, DoubleType(), "_VALUE")
    fm = {f._name: f.datatype for f in result.fields}
    assert isinstance(fm["_VALUE"], DoubleType) and isinstance(fm["_attr"], StringType)

    s2 = StructType(
        [
            StructField("_VALUE", StringType(), True),
            StructField("other", LongType(), True),
        ]
    )
    result2 = _merge_struct_with_primitive(s2, LongType(), "_VALUE")
    fm2 = {f._name: f.datatype for f in result2.fields}
    assert isinstance(fm2["_VALUE"], StringType) and isinstance(fm2["other"], LongType)


# ===========================================================================
# merge_struct_types
# ===========================================================================


def test_merge_struct_types_identical():
    s = StructType(
        [StructField("a", LongType(), True), StructField("b", StringType(), True)]
    )
    fm = {f._name: f.datatype for f in merge_struct_types(s, s).fields}
    assert isinstance(fm["a"], LongType) and isinstance(fm["b"], StringType)


def test_merge_struct_types_disjoint():
    result = merge_struct_types(
        StructType([StructField("a", LongType(), True)]),
        StructType([StructField("b", StringType(), True)]),
    )
    fm = {f._name: f.datatype for f in result.fields}
    assert "a" in fm and "b" in fm


def test_merge_struct_types_widening():
    result = merge_struct_types(
        StructType([StructField("a", LongType(), True)]),
        StructType([StructField("a", DoubleType(), True)]),
    )
    assert isinstance({f._name: f.datatype for f in result.fields}["a"], DoubleType)


def test_merge_struct_types_overlapping_plus_unique():
    s1 = StructType(
        [StructField("a", LongType(), True), StructField("b", StringType(), True)]
    )
    s2 = StructType(
        [StructField("a", DoubleType(), True), StructField("c", BooleanType(), True)]
    )
    fm = {f._name: f.datatype for f in merge_struct_types(s1, s2).fields}
    assert (
        isinstance(fm["a"], DoubleType)
        and isinstance(fm["b"], StringType)
        and isinstance(fm["c"], BooleanType)
    )


def test_merge_struct_types_sorted():
    result = merge_struct_types(
        StructType([StructField("z", LongType(), True)]),
        StructType([StructField("a", StringType(), True)]),
    )
    names = [f._name for f in result.fields]
    assert names == sorted(names)


def test_merge_struct_types_case_sensitive():
    """'Name' and 'name' are separate fields (case-sensitive)."""
    result = merge_struct_types(
        StructType([StructField("Name", LongType(), True)]),
        StructType([StructField("name", StringType(), True)]),
    )
    fm = {f._name: f.datatype for f in result.fields}
    assert isinstance(fm["Name"], LongType) and isinstance(fm["name"], StringType)


# ===========================================================================
# add_or_update_type
# ===========================================================================


def test_add_or_update_type_first_occurrence():
    d = {}
    add_or_update_type(d, "a", LongType())
    assert isinstance(d["a"], LongType)


def test_add_or_update_type_second_promotes_array():
    d = {"a": LongType()}
    add_or_update_type(d, "a", LongType())
    assert isinstance(d["a"], ArrayType) and isinstance(d["a"].element_type, LongType)


def test_add_or_update_type_second_different_types():
    d = {"a": LongType()}
    add_or_update_type(d, "a", DoubleType())
    assert isinstance(d["a"], ArrayType) and isinstance(d["a"].element_type, DoubleType)


def test_add_or_update_type_third_merges():
    d = {"a": ArrayType(LongType(), structured=False)}
    add_or_update_type(d, "a", DoubleType())
    assert isinstance(d["a"], ArrayType) and isinstance(d["a"].element_type, DoubleType)


def test_add_or_update_type_progressive():
    """Multiple occurrences keep widening the array element type."""
    d = {}
    add_or_update_type(d, "a", LongType())
    assert isinstance(d["a"], LongType)
    add_or_update_type(d, "a", LongType())
    assert isinstance(d["a"], ArrayType)
    add_or_update_type(d, "a", DoubleType())
    assert isinstance(d["a"].element_type, DoubleType)
    add_or_update_type(d, "a", StringType())
    assert isinstance(d["a"].element_type, StringType)


# ===========================================================================
# canonicalize_type
# ===========================================================================


@pytest.mark.parametrize(
    "dtype, expected_type",
    [
        (NullType(), StringType),
        (LongType(), LongType),
        (StringType(), StringType),
        (DoubleType(), DoubleType),
        (BooleanType(), BooleanType),
        (DateType(), DateType),
        (TimestampType(), TimestampType),
        (DecimalType(10, 2), DecimalType),
        (DecimalType(38, 18), DecimalType),
    ],
)
def test_canonicalize_type_primitives(dtype, expected_type):
    """NullType → StringType; other primitives unchanged."""
    assert isinstance(canonicalize_type(dtype), expected_type)


def test_canonicalize_type_array_of_null():
    result = canonicalize_type(ArrayType(NullType(), structured=False))
    assert isinstance(result, ArrayType) and isinstance(result.element_type, StringType)


def test_canonicalize_type_array_of_long():
    result = canonicalize_type(ArrayType(LongType(), structured=False))
    assert isinstance(result, ArrayType) and isinstance(result.element_type, LongType)


def test_canonicalize_type_array_of_empty_struct():
    """Array containing empty struct → None (removed)."""
    result = canonicalize_type(
        ArrayType(StructType([], structured=False), structured=False)
    )
    assert result is None


def test_canonicalize_type_nested_array_null():
    """Array<Array<NullType>> → Array<Array<StringType>>."""
    inner = ArrayType(NullType(), structured=False)
    result = canonicalize_type(ArrayType(inner, structured=False))
    assert isinstance(result.element_type, ArrayType)
    assert isinstance(result.element_type.element_type, StringType)


def test_canonicalize_type_struct_null_fields():
    s = StructType(
        [StructField("a", NullType(), True), StructField("b", LongType(), True)],
        structured=False,
    )
    fm = {f._name: f.datatype for f in canonicalize_type(s).fields}
    assert isinstance(fm["a"], StringType) and isinstance(fm["b"], LongType)


def test_canonicalize_type_empty_struct():
    """Per SPARK-8093: empty structs → None."""
    assert canonicalize_type(StructType([], structured=False)) is None


def test_canonicalize_type_struct_empty_name_field():
    """Fields with empty names are removed; if none remain, struct is None."""
    assert (
        canonicalize_type(
            StructType([StructField("", LongType(), True)], structured=False)
        )
        is None
    )


def test_canonicalize_type_struct_mixed_fields():
    s = StructType(
        [
            StructField("a", NullType(), True),
            StructField("", LongType(), True),
            StructField("b", DoubleType(), True),
        ],
        structured=False,
    )
    result = canonicalize_type(s)
    assert len(result.fields) == 2
    fm = {f._name: f.datatype for f in result.fields}
    assert isinstance(fm["a"], StringType) and isinstance(fm["b"], DoubleType)


def test_canonicalize_type_nested_struct():
    """Nested struct: inner NullType fields become StringType."""
    inner = StructType([StructField("x", NullType(), True)], structured=False)
    outer = StructType([StructField("child", inner, True)], structured=False)
    result = canonicalize_type(outer)
    assert isinstance(result.fields[0].datatype.fields[0].datatype, StringType)


def test_canonicalize_type_nested_struct_empty_inner():
    """Nested struct where inner becomes empty → field is removed."""
    inner = StructType([], structured=False)
    outer = StructType(
        [StructField("child", inner, True), StructField("other", LongType(), True)],
        structured=False,
    )
    result = canonicalize_type(outer)
    assert len(result.fields) == 1 and result.fields[0]._name == "other"


# ===========================================================================
# _case_preserving_simple_string
# ===========================================================================


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (StringType(), "string"),
        (LongType(), "bigint"),
        (DoubleType(), "double"),
        (BooleanType(), "boolean"),
        (DateType(), "date"),
        (TimestampType(), "timestamp"),
        (NullType(), "null"),
    ],
)
def test_case_preserving_simple_string_primitives(dtype, expected):
    assert _case_preserving_simple_string(dtype) == expected


def test_case_preserving_simple_string_struct():
    s = StructType([StructField("author", StringType(), True)], structured=False)
    assert _case_preserving_simple_string(s) == "struct<author:string>"


def test_case_preserving_simple_string_preserves_case():
    s = StructType(
        [
            StructField("Author", StringType(), True),
            StructField("title", LongType(), True),
        ],
        structured=False,
    )
    result = _case_preserving_simple_string(s)
    assert "Author:string" in result and "title:bigint" in result


def test_case_preserving_simple_string_nested():
    inner = StructType([StructField("inner_field", LongType(), True)], structured=False)
    outer = StructType([StructField("outer", inner, True)], structured=False)
    assert (
        _case_preserving_simple_string(outer)
        == "struct<outer:struct<inner_field:bigint>>"
    )


def test_case_preserving_simple_string_array():
    s = StructType(
        [StructField("items", ArrayType(StringType(), structured=False), True)],
        structured=False,
    )
    assert _case_preserving_simple_string(s) == "struct<items:array<string>>"


def test_case_preserving_simple_string_array_of_struct():
    inner = StructType([StructField("name", StringType(), True)], structured=False)
    s = StructType(
        [StructField("people", ArrayType(inner, structured=False), True)],
        structured=False,
    )
    assert (
        _case_preserving_simple_string(s) == "struct<people:array<struct<name:string>>>"
    )


def test_case_preserving_simple_string_colon_in_name():
    """Field names with colons get quoted."""
    s = StructType([StructField("px:name", StringType(), True)], structured=False)
    assert _case_preserving_simple_string(s) == 'struct<"px:name":string>'

    s2 = StructType(
        [
            StructField("px:name", StringType(), True),
            StructField("px:value", LongType(), True),
        ],
        structured=False,
    )
    result = _case_preserving_simple_string(s2)
    assert '"px:name":string' in result and '"px:value":bigint' in result


def test_case_preserving_simple_string_no_unnecessary_quotes():
    s = StructType([StructField("author", StringType(), True)], structured=False)
    assert '"' not in _case_preserving_simple_string(s)


# ===========================================================================
# End-to-end: infer + merge + canonicalize (Spark parity)
# ===========================================================================


def test_e2e_simple_flat_record():
    records = [
        """<book><title>The Great Gatsby</title><author>F. Scott Fitzgerald</author>
        <year>1925</year><price>12.99</price></book>"""
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["title"], StringType) and isinstance(fm["author"], StringType)
    assert isinstance(fm["year"], LongType) and isinstance(fm["price"], DoubleType)


def test_e2e_repeated_elements_array():
    records = [
        "<catalog><item>Apple</item><item>Banana</item><item>Cherry</item></catalog>"
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["item"], ArrayType) and isinstance(
        fm["item"].element_type, StringType
    )


def test_e2e_nested_struct_and_array():
    records = [
        """<book><title>Test Book</title><reviews>
        <review><user>user1</user><rating>5</rating></review>
        <review><user>user2</user><rating>3</rating></review>
        </reviews></book>"""
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["title"], StringType)
    rev_fields = {f._name: f.datatype for f in fm["reviews"].fields}
    assert isinstance(rev_fields["review"], ArrayType)
    elem_fields = {
        f._name: f.datatype for f in rev_fields["review"].element_type.fields
    }
    assert isinstance(elem_fields["user"], StringType) and isinstance(
        elem_fields["rating"], LongType
    )


def test_e2e_attributes_with_text():
    records = [
        """<book id="1"><title>Test</title>
        <edition year="2023" format="Hardcover"/></book>"""
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["_id"], LongType) and isinstance(fm["title"], StringType)
    ef = {f._name: f.datatype for f in fm["edition"].fields}
    assert isinstance(ef["_year"], LongType) and isinstance(ef["_format"], StringType)
    assert isinstance(ef["_VALUE"], StringType)


def test_e2e_schema_evolution():
    """Field types evolve across records: Long + Double → Double."""
    records = [
        "<item><name>Widget</name><price>10</price></item>",
        "<item><name>Gadget</name><price>19.99</price></item>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["name"], StringType) and isinstance(fm["price"], DoubleType)


def test_e2e_schema_evolution_bool_to_string():
    """Boolean + String → String."""
    records = [
        "<item><flag>true</flag></item>",
        "<item><flag>maybe</flag></item>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["flag"], StringType)


def test_e2e_schema_evolution_date_to_timestamp():
    """Date + Timestamp → Timestamp."""
    records = [
        "<item><when>2024-01-15</when></item>",
        "<item><when>2024-01-15T10:30:00</when></item>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["when"], TimestampType)


def test_e2e_null_fields_canonicalized():
    """All-null fields become StringType after canonicalization."""
    records = ["<row><a/><b>hello</b></row>", "<row><a></a><b>world</b></row>"]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["a"], StringType) and isinstance(fm["b"], StringType)


def test_e2e_serialization_round_trip():
    records = [
        """<row><author>Jane</author><price>29.99</price>
        <tags><tag>fiction</tag><tag>bestseller</tag></tags></row>"""
    ]
    schema_str = _case_preserving_simple_string(_full_pipeline(records))
    assert "author:string" in schema_str and "price:double" in schema_str
    assert "tags:struct<tag:array<string>>" in schema_str


def test_e2e_case_sensitive_fields():
    """'b' and 'B' are different fields (case-sensitive)."""
    records = [
        "<ROW><a>1</a><b>2</b><c>3</c></ROW>",
        "<ROW><a>4</a><B>5</B><c>6</c></ROW>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert all(isinstance(fm[k], LongType) for k in ["a", "b", "B", "c"])


def test_e2e_case_sensitive_value_tag():
    """<a> with children vs <A> without → different fields."""
    records = [
        "<ROW><a>\n1\n<b>2</b></a></ROW>",
        "<ROW><A>3</A></ROW>",
    ]
    fm = {
        f._name: f.datatype
        for f in _full_pipeline(records, ignore_surrounding_whitespace=True).fields
    }
    assert isinstance(fm["A"], LongType)
    assert isinstance(fm["a"], StructType)
    af = {f._name: f.datatype for f in fm["a"].fields}
    assert isinstance(af["_VALUE"], LongType) and isinstance(af["b"], LongType)


def test_e2e_case_sensitive_attributes():
    """'attr' and 'aTtr' are different attribute fields."""
    records = [
        '<ROW attr="1"><a>1</a><b>2</b><c>3</c></ROW>',
        '<ROW aTtr="2"><a>4</a><b>5</b><c>6</c></ROW>',
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["_attr"], LongType) and isinstance(fm["_aTtr"], LongType)


def test_e2e_case_sensitive_struct():
    """<A> and <a> with children → different struct fields."""
    records = [
        "<ROW><A><a>1</a><c>3</c></A></ROW>",
        "<ROW><a><A>5</A><c>7</c></a></ROW>",
    ]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["A"], StructType) and isinstance(fm["a"], StructType)
    assert all(
        isinstance(v, LongType)
        for v in {f._name: f.datatype for f in fm["A"].fields}.values()
    )


def test_e2e_case_sensitive_array_complex():
    records = [
        "<ROW><a>\n1\n<b>2</b><c>3</c></a><A>4</A></ROW>",
        "<ROW><A>5</A></ROW>",
    ]
    fm = {
        f._name: f.datatype
        for f in _full_pipeline(records, ignore_surrounding_whitespace=True).fields
    }
    assert isinstance(fm["A"], LongType)
    assert isinstance(fm["a"], StructType)
    af = {f._name: f.datatype for f in fm["a"].fields}
    assert isinstance(af["_VALUE"], LongType) and isinstance(af["b"], LongType)


def test_e2e_case_sensitive_array_simple():
    records = ["<ROW><a><b>1</b><c>2</c></a><A><B>3</B><c>4</c></A></ROW>"]
    fm = {f._name: f.datatype for f in _full_pipeline(records).fields}
    assert isinstance(fm["A"], StructType) and isinstance(fm["a"], StructType)
    assert isinstance({f._name: f.datatype for f in fm["A"].fields}["B"], LongType)
    assert isinstance({f._name: f.datatype for f in fm["a"].fields}["b"], LongType)


def test_e2e_value_tags_spaces_and_empty_values():
    """lxml element.text only captures text BEFORE the first child."""
    records = [
        "<ROW>\n    str1\n    <a>  <b>1</b>\n    </a></ROW>",
        "<ROW> <a><b/> value</a></ROW>",
        "<ROW><a><b>3</b> </a></ROW>",
        "<ROW><a><b>4</b> </a></ROW>",
    ]
    fm_ws = {
        f._name: f.datatype
        for f in _full_pipeline(records, ignore_surrounding_whitespace=True).fields
    }
    assert isinstance(fm_ws["_VALUE"], StringType)
    assert isinstance(fm_ws["a"], StructType)
    assert isinstance({f._name: f.datatype for f in fm_ws["a"].fields}["b"], LongType)

    fm_nws = {
        f._name: f.datatype
        for f in _full_pipeline(records, ignore_surrounding_whitespace=False).fields
    }
    assert isinstance(fm_nws["_VALUE"], StringType)


def test_e2e_value_tags_multiline():
    """element.text only captures text before first child."""
    records = [
        "<ROW>\nvalue1\n<a>1</a></ROW>",
        "<ROW>\nvalue3\nvalue4<a>1</a></ROW>",
    ]
    fm = {
        f._name: f.datatype
        for f in _full_pipeline(records, ignore_surrounding_whitespace=True).fields
    }
    assert isinstance(fm["_VALUE"], StringType) and isinstance(fm["a"], LongType)


def test_e2e_value_tag_comments():
    """Comments should not affect value tag inference."""
    records = ['<ROW>\n2\n<a></a><a attr="1"></a></ROW>']
    fm = {
        f._name: f.datatype
        for f in _full_pipeline(records, ignore_surrounding_whitespace=True).fields
    }
    assert isinstance(fm["_VALUE"], LongType) and isinstance(fm["a"], ArrayType)


def test_e2e_value_tag_null_value_option():
    """nullValue option doesn't affect schema inference."""
    fm = {
        f._name: f.datatype
        for f in _full_pipeline(
            ["<ROW>\n    1\n</ROW>"], ignore_surrounding_whitespace=True
        ).fields
    }
    assert isinstance(fm["_VALUE"], LongType)


# ---------------------------------------------------------------------------
# infer_schema_for_xml_range
# ---------------------------------------------------------------------------


def _mock_xml_range(xml_content, row_tag, **kwargs):
    """Helper: run infer_schema_for_xml_range against an in-memory XML string."""
    xml_bytes = xml_content.encode("utf-8")
    mock_file = kwargs.get("file_obj") or io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        return infer_schema_for_xml_range(
            file_path="test.xml",
            row_tag=row_tag,
            approx_start=0,
            approx_end=kwargs.get("approx_end", len(xml_bytes)),
            sampling_ratio=kwargs.get("sampling_ratio", 1.0),
            ignore_namespace=kwargs.get("ignore_namespace", True),
            attribute_prefix=kwargs.get("attribute_prefix", "_"),
            exclude_attributes=kwargs.get("exclude_attributes", False),
            value_tag=kwargs.get("value_tag", "_VALUE"),
            charset="utf-8",
            ignore_surrounding_whitespace=kwargs.get(
                "ignore_surrounding_whitespace", True
            ),
        )


def test_infer_range_single_leaf_record():
    """Single leaf record → StructType with _VALUE."""
    schema = _mock_xml_range("<root><item>hello</item></root>", "item")
    assert schema is not None
    assert len(schema.fields) == 1
    assert schema.fields[0]._name == "_VALUE"
    assert isinstance(schema.fields[0].datatype, StringType)


def test_infer_range_multiple_typed_records():
    """Multiple records with different types → merged schema."""
    xml = "<r><row><a>1</a><b>true</b></row><row><a>2</a><b>false</b><c>3.14</c></row></r>"
    schema = _mock_xml_range(xml, "row")
    fm = {f._name: f.datatype for f in schema.fields}
    assert isinstance(fm["a"], LongType)
    assert isinstance(fm["b"], BooleanType)
    assert isinstance(fm["c"], DoubleType)


def test_infer_range_type_widening():
    """Long in first record, Double in second → merged to DoubleType."""
    xml = "<r><row><val>42</val></row><row><val>3.14</val></row></r>"
    schema = _mock_xml_range(xml, "row")
    assert isinstance(schema.fields[0].datatype, DoubleType)


def test_infer_range_repeated_children_become_array():
    """Repeated child tags → ArrayType."""
    xml = "<r><row><item>a</item><item>b</item></row></r>"
    schema = _mock_xml_range(xml, "row")
    assert isinstance(schema.fields[0].datatype, ArrayType)
    assert isinstance(schema.fields[0].datatype.element_type, StringType)


def test_infer_range_attributes():
    """Attributes → prefixed struct fields."""
    xml = '<r><row id="1" active="true"><name>Alice</name></row></r>'
    schema = _mock_xml_range(xml, "row")
    fm = {f._name: f.datatype for f in schema.fields}
    assert isinstance(fm["_id"], LongType)
    assert isinstance(fm["_active"], BooleanType)
    assert isinstance(fm["name"], StringType)


def test_infer_range_nested_struct():
    """Nested elements → nested StructType."""
    xml = "<r><row><address><city>NYC</city><zip>10001</zip></address></row></r>"
    schema = _mock_xml_range(xml, "row")
    addr = schema.fields[0]
    assert addr._name == "address"
    assert isinstance(addr.datatype, StructType)
    nested_fm = {f._name: f.datatype for f in addr.datatype.fields}
    assert isinstance(nested_fm["city"], StringType)
    assert isinstance(nested_fm["zip"], LongType)


def test_infer_range_self_closing_tag():
    """Self-closing tags → StructType with attributes."""
    xml = '<r><row id="1"/><row id="2"/></r>'
    schema = _mock_xml_range(xml, "row")
    assert len(schema.fields) == 1
    assert schema.fields[0]._name == "_id"
    assert isinstance(schema.fields[0].datatype, LongType)


def test_infer_range_no_records():
    xml = "<r><other>data</other></r>"
    schema = _mock_xml_range(xml, "row")
    assert schema is None


def test_infer_range_malformed_record_skipped():
    """Malformed XML between valid records is skipped."""
    xml = "<r><row><a>1</a></row><row><<<bad</row><row><a>2</a></row></r>"
    schema = _mock_xml_range(xml, "row")
    assert schema is not None
    assert isinstance(schema.fields[0].datatype, LongType)


def test_infer_range_namespace_stripped():
    """Namespaces are stripped when ignore_namespace=True."""
    xml = '<r><ns:row xmlns:ns="http://example.com"><ns:val>42</ns:val></ns:row></r>'
    schema = _mock_xml_range(xml, "ns:row", ignore_namespace=True)
    assert schema is not None


def test_infer_range_exclude_attributes():
    """exclude_attributes=True omits attribute fields."""
    xml = '<r><row id="1"><name>Alice</name></row></r>'
    schema = _mock_xml_range(xml, "row", exclude_attributes=True)
    fm = {f._name for f in schema.fields}
    assert "_id" not in fm
    assert "name" in fm


def test_infer_range_mixed_content():
    xml = "<r><row>text<child>1</child></row></r>"
    schema = _mock_xml_range(xml, "row")
    fm = {f._name: f.datatype for f in schema.fields}
    assert "_VALUE" in fm
    assert "child" in fm


def test_infer_range_schema_merge_across_records():
    """Fields present only in some records are merged into the union schema."""
    xml = "<r><row><a>1</a></row><row><b>hello</b></row><row><a>2</a><c>true</c></row></r>"
    schema = _mock_xml_range(xml, "row")
    fm = {f._name: f.datatype for f in schema.fields}
    assert isinstance(fm["a"], LongType)
    assert isinstance(fm["b"], StringType)
    assert isinstance(fm["c"], BooleanType)


def test_infer_range_sampling_ratio_skips():
    """With a very low sampling_ratio, some records may be skipped."""
    import random

    xml = "<r>"
    for i in range(20):
        xml += f"<row><val>{i}</val></row>"
    xml += "</r>"
    xml_bytes = xml.encode("utf-8")

    random.seed(42)
    mock_file = io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        schema = infer_schema_for_xml_range(
            file_path="test.xml",
            row_tag="row",
            approx_start=0,
            approx_end=len(xml_bytes),
            sampling_ratio=0.3,
            ignore_namespace=True,
            attribute_prefix="_",
            exclude_attributes=False,
            value_tag="_VALUE",
            charset="utf-8",
            ignore_surrounding_whitespace=True,
        )
    assert schema is not None
    assert isinstance(schema.fields[0].datatype, LongType)


def test_infer_range_truncated_tag_handled():
    """A truncated opening tag that causes tag_is_self_closing to fail is skipped."""
    xml = "<r><row><a>1</a></row><row <row><a>2</a></row></r>"
    xml_bytes = xml.encode("utf-8")
    mock_file = io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        schema = infer_schema_for_xml_range(
            file_path="test.xml",
            row_tag="row",
            approx_start=0,
            approx_end=len(xml_bytes),
            sampling_ratio=1.0,
            ignore_namespace=True,
            attribute_prefix="_",
            exclude_attributes=False,
            value_tag="_VALUE",
            charset="utf-8",
            ignore_surrounding_whitespace=True,
        )
    assert schema is not None


def test_xml_schema_inference_process_empty_results():
    """Cases where process should yield ("",): empty/None file size, invalid worker id, no records."""
    base = ("test.xml",)
    tail = (1.0, True, "_", False, "_VALUE", "utf-8", True)

    # empty file (size 0)
    assert list(XMLSchemaInference().process(*base, 1, "row", 0, *tail, 0)) == [("",)]

    # None file size
    assert list(XMLSchemaInference().process(*base, 1, "row", 0, *tail, None)) == [
        ("",)
    ]

    # worker id >= num_workers
    assert list(XMLSchemaInference().process(*base, 2, "row", 5, *tail, 1000)) == [
        ("",)
    ]

    # no matching row tags
    xml_bytes = b"<r><other>data</other></r>"
    with patch(
        "snowflake.snowpark.files.SnowflakeFile.open",
        return_value=io.BytesIO(xml_bytes),
    ):
        assert list(
            XMLSchemaInference().process(*base, 1, "row", 0, *tail, len(xml_bytes))
        ) == [("",)]


def test_xml_schema_inference_process_param_defaults():
    """None num_workers defaults to 1; negative worker id defaults to 0."""
    xml_bytes = b"<r><row><a>42</a></row></r>"
    tail = (1.0, True, "_", False, "_VALUE", "utf-8", True)

    for num_workers, i in [(None, 0), (1, -1)]:
        mock_file = io.BytesIO(xml_bytes)
        with patch(
            "snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file
        ):
            results = list(
                XMLSchemaInference().process(
                    "test.xml", num_workers, "row", i, *tail, len(xml_bytes)
                )
            )
        assert len(results) == 1
        assert "bigint" in results[0][0]


def test_xml_schema_inference_process_with_sampling():
    """Sampling ratio < 1.0 sets a deterministic seed."""
    xml = "<r>"
    for i in range(10):
        xml += f"<row><val>{i}</val></row>"
    xml += "</r>"
    xml_bytes = xml.encode("utf-8")
    mock_file = io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        results = list(
            XMLSchemaInference().process(
                "test.xml",
                1,
                "row",
                0,
                0.5,
                True,
                "_",
                False,
                "_VALUE",
                "utf-8",
                True,
                len(xml_bytes),
            )
        )
    assert len(results) == 1
    assert results[0][0] != ""


def test_xml_schema_inference_process_multi_worker():
    """Multi-worker processing partitions the file correctly."""
    xml = "<r><row><a>1</a></row><row><a>2</a></row><row><a>3</a></row></r>"
    xml_bytes = xml.encode("utf-8")

    all_schemas = []
    for worker_id in range(2):
        mock_file = io.BytesIO(xml_bytes)
        with patch(
            "snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file
        ):
            udtf = XMLSchemaInference()
            results = list(
                udtf.process(
                    "test.xml",
                    2,
                    "row",
                    worker_id,
                    1.0,
                    True,
                    "_",
                    False,
                    "_VALUE",
                    "utf-8",
                    True,
                    len(xml_bytes),
                )
            )
            all_schemas.append(results[0][0])
    assert any(s != "" for s in all_schemas)


def test_infer_element_schema_clark_ns_in_children():
    """Clark {uri}tag stripped with ignore_namespace=True"""
    root = ET.Element("row")
    ET.SubElement(root, "{http://example.com}child").text = "val"
    assert "child" in {
        f._name for f in infer_element_schema(root, ignore_namespace=True).fields
    }


def test_infer_range_approx_end_breaks():
    """open_pos >= approx_end → immediate break."""
    assert _mock_xml_range("<r><row><a>1</a></row></r>", "row", approx_end=3) is None


def test_infer_range_tag_exception_continues():
    """Truncated tag at EOF → tag_is_self_closing raises → seek back, continue"""
    assert _mock_xml_range("<r><row><a>1</a></row><row", "row") is not None


def test_infer_range_sampling_skip_past_approx_end():
    """Sampling skip + record past approx_end → break"""
    with patch("snowflake.snowpark._internal.xml_schema_inference.random") as rng:
        rng.random.return_value = 0.99
        rng.seed = lambda *a, **kw: None
        schema = _mock_xml_range(
            "<r><row><a>long_value</a></row></r>",
            "row",
            approx_end=10,
            sampling_ratio=0.01,
        )
    assert schema is None


def test_infer_range_parse_error_past_approx_end():
    """Parse error + record past approx_end → break"""
    schema = _mock_xml_range(
        "<r><row><ns:a>1</ns:a></row></r>",
        "row",
        approx_end=10,
        ignore_namespace=False,
    )
    assert schema is None


def test_infer_range_stdlib_et_fallback():
    """stdlib ET used when lxml not installed"""
    import xml.etree.ElementTree as stdlib_ET

    with patch(
        "snowflake.snowpark._internal.xml_schema_inference.lxml_installed", False
    ), patch("snowflake.snowpark._internal.xml_schema_inference.ET", stdlib_ET):
        assert _mock_xml_range("<r><row><a>42</a></row></r>", "row") is not None


def test_infer_range_seek_failures():
    class FailSeek(io.BytesIO):
        """BytesIO that raises OSError on the Nth seek to a target position.

        skip=0 means fail on the very first seek to fail_pos.
        skip=1 means allow the first seek (e.g. inside find_next_closing_tag_pos)
        and fail on the second (the coverage-target seek).
        """

        def __init__(self, data, fail_pos, skip=0) -> None:
            super().__init__(data)
            self._fail_pos = fail_pos
            self._skip = skip
            self._hits = 0

        def seek(self, pos, whence=0):
            if whence == 0 and pos == self._fail_pos:
                self._hits += 1
                if self._hits > self._skip:
                    raise OSError("forced")
            return super().seek(pos, whence)

    def _end(b):
        return b.find(b"</row>") + len(b"</row>")

    # successful parse → seek failure after merge
    # skip=1: first seek to record_end happens inside find_next_closing_tag_pos
    b = b"<r><row><a>7</a></row></r>"
    assert (
        _mock_xml_range(b.decode(), "row", file_obj=FailSeek(b, _end(b), skip=1))
        is not None
    )

    # parse error → seek failure
    b = b"<r><row><ns:a>1</ns:a></row></r>"
    assert (
        _mock_xml_range(
            b.decode(),
            "row",
            file_obj=FailSeek(b, _end(b), skip=1),
            ignore_namespace=False,
        )
        is None
    )

    # sampling skip → seek failure
    b = b"<r><row><a>123</a></row></r>"
    with patch("snowflake.snowpark._internal.xml_schema_inference.random") as rng:
        rng.random.return_value = 0.99
        rng.seed = lambda *a, **kw: None
        assert (
            _mock_xml_range(
                b.decode(),
                "row",
                file_obj=FailSeek(b, _end(b), skip=1),
                sampling_ratio=0.01,
            )
            is None
        )

    # tag exception → seek failure on record_start + 1
    # skip=0: position record_start+1 is only hit in the except handler
    b = b"<r><row><a>1</a></row><row"
    second_row = b.find(b"<row", b.find(b"<row") + 1)
    assert (
        _mock_xml_range(
            b.decode(),
            "row",
            file_obj=FailSeek(b, second_row + 1),
        )
        is not None
    )


def _run_infer_xml(schema_rows, *, type_string=None, canonicalize=None):
    """Call _infer_schema_for_xml with mocked UDTF results."""
    s = mock.MagicMock()
    s.sql.return_value.collect.return_value = [{"size": "1024"}]
    df = mock.MagicMock()
    df.to_df.return_value = df
    df.select.return_value.collect.return_value = schema_rows
    s.range.return_value = df
    reader = DataFrameReader(s, _emit_ast=False)
    reader._cur_options[_dr_mod.XML_ROW_TAG_STRING] = "row"

    with ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(
                _dr_mod, "get_types_from_type_hints", return_value=(None, [])
            )
        )
        if type_string is not None:
            stack.enter_context(
                mock.patch.object(
                    _dr_mod, "type_string_to_type_object", side_effect=type_string
                )
            )
        if canonicalize is not None:
            stack.enter_context(
                mock.patch.object(
                    _dr_mod, "canonicalize_type", return_value=canonicalize
                )
            )
        return reader._infer_schema_for_xml("@s/f.xml")


def test_infer_xml_bad_schema_string_and_non_struct_partial():
    """type_string_to_type_object raises → skip; non-StructType → wrapped."""
    result = _run_infer_xml(
        [("bad",), ("bigint",)],
        type_string=[ValueError("bad"), LongType()],
        canonicalize=StructType([StructField("_VALUE", LongType())]),
    )
    assert result is not None and result.fields[0]._name == "_VALUE"


def test_infer_xml_canonical_non_struct_returns_none():
    """canonicalize_type returns non-StructType → None."""
    result = _run_infer_xml(
        [("struct<a:bigint>",)],
        type_string=[StructType([StructField("a", LongType())])],
        canonicalize=LongType(),
    )
    assert result is None


def test_infer_xml_map_type_field_names_cleaned():
    """MapType fields get quoted names stripped."""
    result = _run_infer_xml(
        [("x",)],
        type_string=[StructType([StructField("a", LongType())])],
        canonicalize=StructType(
            [
                StructField(
                    '"m"',
                    MapType(
                        StructType([StructField('"k"', StringType())]),
                        StructType([StructField('"v"', LongType())]),
                    ),
                )
            ]
        ),
    )
    assert result.fields[0]._name == "m"
    assert result.fields[0].datatype.key_type.fields[0]._name == "k"
    assert result.fields[0].datatype.value_type.fields[0]._name == "v"


# ===========================================================================
# string_types_only parameter (inferSchema=False)
# ===========================================================================


def test_infer_type_string_types_only():
    """string_types_only=True: non-empty text → StringType, null/empty → NullType.
    Spark's inferField returns NullType for empty/self-closing elements before
    inferFrom is called.  inferFrom returns StringType unconditionally when
    inferSchema=false."""
    for val in ("42", "3.14", "true", "2024-01-15", "hello", "N/A"):
        assert isinstance(infer_type(val, string_types_only=True), StringType), val
    for val in (None, ""):
        assert isinstance(infer_type(val, string_types_only=True), NullType), val


def test_infer_element_schema_string_types_only():
    """One complex XML exercises all infer_element_schema codepaths with string_types_only:
    attributes, nested struct, repeated children (array), mixed content, empty children.
    Non-empty leaves → StringType; empty leaves → NullType; structural types preserved."""
    xml_str = """<book id="1">
        mixed text
        <title>Test</title>
        <price>29.99</price>
        <empty/>
        <tag>fiction</tag><tag>classic</tag>
        <info><publisher>Acme</publisher><year>2020</year></info>
    </book>"""
    result = infer_element_schema(_xml(xml_str), string_types_only=True)
    assert isinstance(result, StructType)
    fm = {f._name: f.datatype for f in result.fields}

    assert isinstance(fm["_id"], StringType)
    assert isinstance(fm["_VALUE"], StringType)
    assert isinstance(fm["title"], StringType)
    assert isinstance(fm["price"], StringType)
    assert isinstance(fm["empty"], NullType)

    assert isinstance(fm["tag"], ArrayType)
    assert isinstance(fm["tag"].element_type, StringType)

    assert isinstance(fm["info"], StructType)
    info_fm = {f._name: f.datatype for f in fm["info"].fields}
    assert isinstance(info_fm["publisher"], StringType)
    assert isinstance(info_fm["year"], StringType)

    # Empty element + struct element across records: struct must survive merge
    records = [
        "<row><nested><a>1</a><b>2</b></nested></row>",
        "<row><nested/></row>",
    ]
    merged = canonicalize_type(_infer_and_merge(records, string_types_only=True))
    nested = {f._name: f.datatype for f in merged.fields}["nested"]
    assert isinstance(nested, StructType), f"Expected StructType, got {type(nested)}"


def test_udtf_process_string_types_only():
    """End-to-end: XMLSchemaInference.process threads string_types_only through
    infer_schema_for_xml_range → infer_element_schema → infer_type."""
    xml_bytes = b"<r><row><a>42</a><b>2024-01-15</b><c>true</c></row></r>"
    tail = (1.0, True, "_", False, "_VALUE", "utf-8", True)
    mock_file = io.BytesIO(xml_bytes)
    with patch("snowflake.snowpark.files.SnowflakeFile.open", return_value=mock_file):
        schema_str = list(
            XMLSchemaInference().process(
                "test.xml", 1, "row", 0, *tail, len(xml_bytes), True
            )
        )[0][0]
    assert "string" in schema_str
    assert "bigint" not in schema_str and "date" not in schema_str


# ===========================================================================
# _XML_SKIP_INFERENCE option
# ===========================================================================


@pytest.mark.parametrize("skip_inference", [True, False])
def test_xml_skip_inference_option(skip_inference):
    reader = DataFrameReader(mock.MagicMock(), _emit_ast=False)
    reader._cur_options[_dr_mod.XML_ROW_TAG_STRING] = "row"
    if skip_inference:
        reader._cur_options["_XML_SKIP_INFERENCE"] = True

    with mock.patch.object(
        reader, "_infer_schema_for_xml", return_value=None
    ) as mock_infer, mock.patch.object(
        _dr_mod.context, "_is_snowpark_connect_compatible_mode", True
    ):
        try:
            reader._read_semi_structured_file("@s/f.xml", "XML")
        except Exception:
            pass

        if skip_inference:
            mock_infer.assert_not_called()
        else:
            mock_infer.assert_called_once()
