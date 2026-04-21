#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark._internal.type_utils import (
    _extract_paren_content,
    _parse_structured_type_str,
    _sf_type_to_type_object,
)
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
)


# ---------------------------------------------------------------------------
# _extract_paren_content
# ---------------------------------------------------------------------------


class TestExtractParenContent:
    def test_simple_type_with_parens(self):
        assert _extract_paren_content("NUMBER(38,0)") == ("NUMBER", "38,0")

    def test_structured_object(self):
        result = _extract_paren_content("OBJECT(city VARCHAR, zip NUMBER(38,0))")
        assert result == ("OBJECT", "city VARCHAR, zip NUMBER(38,0)")

    def test_nested_parens(self):
        result = _extract_paren_content("ARRAY(MAP(VARCHAR, NUMBER(10,0)))")
        assert result == ("ARRAY", "MAP(VARCHAR, NUMBER(10,0))")

    def test_no_parens(self):
        assert _extract_paren_content("VARCHAR") is None

    def test_empty_string(self):
        assert _extract_paren_content("") is None

    def test_unclosed_paren_raises(self):
        with pytest.raises(ValueError, match="Unbalanced parentheses"):
            _extract_paren_content("NUMBER(38,0")

    def test_unclosed_paren_structured_raises(self):
        with pytest.raises(ValueError, match="Unbalanced parentheses"):
            _extract_paren_content("OBJECT(a INT, b VARCHAR")

    def test_base_with_whitespace(self):
        result = _extract_paren_content("  NUMBER  (38,0)")
        assert result is not None
        assert result[0] == "NUMBER"
        assert result[1] == "38,0"

    def test_deeply_nested(self):
        type_str = "OBJECT(a ARRAY(MAP(VARCHAR, NUMBER(10,0))), b BOOLEAN)"
        result = _extract_paren_content(type_str)
        assert result == (
            "OBJECT",
            "a ARRAY(MAP(VARCHAR, NUMBER(10,0))), b BOOLEAN",
        )

    def test_string_with_length(self):
        assert _extract_paren_content("VARCHAR(100)") == ("VARCHAR", "100")

    def test_trailing_text_after_close(self):
        # Only content up to the matching close paren is returned
        result = _extract_paren_content("ARRAY(VARCHAR) NOT NULL")
        assert result == ("ARRAY", "VARCHAR")


# ---------------------------------------------------------------------------
# _sf_type_to_type_object
# ---------------------------------------------------------------------------


class TestSfTypeToTypeObject:
    # --- simple types ---

    def test_varchar(self):
        assert _sf_type_to_type_object("VARCHAR") == StringType()

    def test_boolean(self):
        assert _sf_type_to_type_object("BOOLEAN") == BooleanType()

    def test_timestamp_ntz(self):
        assert _sf_type_to_type_object("TIMESTAMP_NTZ") == TimestampType(
            timezone=TimestampTimeZone.NTZ
        )

    def test_timestamp_tz(self):
        assert _sf_type_to_type_object("TIMESTAMP_TZ") == TimestampType(
            timezone=TimestampTimeZone.TZ
        )

    def test_timestamp_ltz(self):
        assert _sf_type_to_type_object("TIMESTAMP_LTZ") == TimestampType(
            timezone=TimestampTimeZone.LTZ
        )

    def test_date(self):
        assert _sf_type_to_type_object("DATE") == DateType()

    def test_time(self):
        assert _sf_type_to_type_object("TIME") == TimeType()

    # --- extra type mappings ---

    def test_text(self):
        assert _sf_type_to_type_object("TEXT") == StringType()

    def test_real(self):
        assert _sf_type_to_type_object("REAL") == DoubleType()

    def test_fixed(self):
        assert _sf_type_to_type_object("FIXED") == LongType()

    # --- numeric types with precision/scale ---

    def test_number_integer(self):
        result = _sf_type_to_type_object("NUMBER(38,0)")
        assert result == DecimalType(38, 0)

    def test_number_with_scale(self):
        result = _sf_type_to_type_object("NUMBER(10,2)")
        assert result == DecimalType(10, 2)

    def test_decimal_type_str(self):
        result = _sf_type_to_type_object("DECIMAL(18,4)")
        assert result == DecimalType(18, 4)

    # --- string with length ---

    def test_varchar_with_length(self):
        result = _sf_type_to_type_object("VARCHAR(100)")
        assert result == StringType(100)

    # --- ARRAY ---

    def test_array_nullable_elements(self):
        result = _sf_type_to_type_object("ARRAY(VARCHAR)")
        assert isinstance(result, ArrayType)
        assert result.element_type == StringType()
        assert result.structured is True
        assert result.contains_null is True

    def test_array_not_null_elements(self):
        result = _sf_type_to_type_object("ARRAY(NUMBER(10,0) NOT NULL)")
        assert isinstance(result, ArrayType)
        assert result.element_type == DecimalType(10, 0)
        assert result.structured is True
        assert result.contains_null is False

    def test_array_boolean(self):
        result = _sf_type_to_type_object("ARRAY(BOOLEAN)")
        assert isinstance(result, ArrayType)
        assert result.element_type == BooleanType()
        assert result.structured is True

    # --- MAP ---

    def test_map_nullable_values(self):
        result = _sf_type_to_type_object("MAP(VARCHAR, VARCHAR)")
        assert isinstance(result, MapType)
        assert result.key_type == StringType()
        assert result.value_type == StringType()
        assert result.structured is True
        assert result.value_contains_null is True

    def test_map_not_null_values(self):
        result = _sf_type_to_type_object("MAP(VARCHAR, NUMBER(10,0) NOT NULL)")
        assert isinstance(result, MapType)
        assert result.key_type == StringType()
        assert result.value_type == DecimalType(10, 0)
        assert result.structured is True
        assert result.value_contains_null is False

    def test_map_with_boolean_value(self):
        result = _sf_type_to_type_object("MAP(VARCHAR, BOOLEAN)")
        assert isinstance(result, MapType)
        assert result.key_type == StringType()
        assert result.value_type == BooleanType()
        assert result.structured is True

    def test_map_wrong_part_count_raises(self):
        with pytest.raises(ValueError, match="Invalid MAP type definition"):
            _sf_type_to_type_object("MAP(VARCHAR)")

    def test_map_too_many_parts_raises(self):
        with pytest.raises(ValueError, match="Invalid MAP type definition"):
            _sf_type_to_type_object("MAP(VARCHAR, VARCHAR, VARCHAR)")

    # --- OBJECT ---

    def test_object_simple_fields(self):
        result = _sf_type_to_type_object("OBJECT(city VARCHAR, zip NUMBER(38,0))")
        assert isinstance(result, StructType)
        assert result.structured is True
        assert len(result.fields) == 2
        assert result.fields[0].name == "CITY"
        assert result.fields[0].datatype == StringType()
        assert result.fields[0].nullable is True
        assert result.fields[1].name == "ZIP"
        assert result.fields[1].datatype == DecimalType(38, 0)
        assert result.fields[1].nullable is True

    def test_object_with_not_null_field(self):
        result = _sf_type_to_type_object(
            "OBJECT(city VARCHAR, zip NUMBER(38,0) NOT NULL)"
        )
        assert isinstance(result, StructType)
        assert result.structured is True
        assert result.fields[0].nullable is True
        assert result.fields[1].nullable is False

    def test_object_single_field(self):
        result = _sf_type_to_type_object("OBJECT(name VARCHAR)")
        assert isinstance(result, StructType)
        assert result.structured is True
        assert len(result.fields) == 1
        assert result.fields[0].name == "NAME"
        assert result.fields[0].datatype == StringType()

    def test_object_field_parse_error_raises(self):
        with pytest.raises(ValueError, match="Cannot parse OBJECT field definition"):
            _sf_type_to_type_object("OBJECT(badfield)")

    def test_object_empty_is_valid(self):
        result = _sf_type_to_type_object("OBJECT()")
        assert isinstance(result, StructType)
        assert result.structured is True
        assert result.fields == []

    def test_object_trailing_comma_raises(self):
        with pytest.raises(ValueError, match="Empty field in OBJECT type"):
            _sf_type_to_type_object("OBJECT(a NUMBER(10,0),)")

    def test_object_leading_comma_raises(self):
        with pytest.raises(ValueError, match="Empty field in OBJECT type"):
            _sf_type_to_type_object("OBJECT(,a NUMBER(10,0))")

    def test_object_all_empty_fields_raises(self):
        with pytest.raises(ValueError, match="Empty field in OBJECT type"):
            _sf_type_to_type_object("OBJECT(,,,)")

    # --- deeply nested ---

    def test_nested_object_with_array_and_map(self):
        type_str = (
            "OBJECT(name VARCHAR, tags ARRAY(VARCHAR NOT NULL), "
            "metadata MAP(VARCHAR, NUMBER(10,0)))"
        )
        result = _sf_type_to_type_object(type_str)
        assert isinstance(result, StructType)
        assert result.structured is True
        assert len(result.fields) == 3

        assert result.fields[0].name == "NAME"
        assert result.fields[0].datatype == StringType()

        tags = result.fields[1].datatype
        assert isinstance(tags, ArrayType)
        assert tags.structured is True
        assert tags.element_type == StringType()
        assert tags.contains_null is False

        metadata = result.fields[2].datatype
        assert isinstance(metadata, MapType)
        assert metadata.structured is True
        assert metadata.key_type == StringType()
        assert metadata.value_type == DecimalType(10, 0)

    def test_array_of_objects(self):
        result = _sf_type_to_type_object(
            "ARRAY(OBJECT(x NUMBER(10,0), y VARCHAR) NOT NULL)"
        )
        assert isinstance(result, ArrayType)
        assert result.structured is True
        assert result.contains_null is False
        inner = result.element_type
        assert isinstance(inner, StructType)
        assert inner.structured is True
        assert len(inner.fields) == 2

    def test_map_with_array_values(self):
        result = _sf_type_to_type_object("MAP(VARCHAR, ARRAY(BOOLEAN NOT NULL))")
        assert isinstance(result, MapType)
        assert result.structured is True
        inner = result.value_type
        assert isinstance(inner, ArrayType)
        assert inner.structured is True
        assert inner.element_type == BooleanType()
        assert inner.contains_null is False

    # --- NOT NULL on top-level ---

    def test_top_level_not_null_stripped(self):
        result = _sf_type_to_type_object("VARCHAR NOT NULL")
        assert result == StringType()

    # --- simple types with parens that fall through to the keyword
    #     lookup (DATA_TYPE_STRING_OBJECT_MAPPINGS / _SF_EXTRA_TYPE_MAPPINGS) ---

    def test_timestamp_ntz_with_precision_paren(self):
        # base is TIMESTAMP_NTZ → not in OBJECT/MAP/ARRAY, not matched by
        # DECIMAL_RE / STRING_RE → falls through to the keyword lookup.
        result = _sf_type_to_type_object("TIMESTAMP_NTZ(9)")
        assert result == TimestampType(timezone=TimestampTimeZone.NTZ)

    def test_real_with_paren_uses_extra_mapping(self):
        # base is REAL → only in _SF_EXTRA_TYPE_MAPPINGS, not the main one.
        result = _sf_type_to_type_object("REAL(53)")
        assert result == DoubleType()

    def test_fixed_with_paren_uses_extra_mapping(self):
        # base is FIXED → only in _SF_EXTRA_TYPE_MAPPINGS.
        result = _sf_type_to_type_object("FIXED(38)")
        assert result == LongType()

    def test_unsupported_type_with_paren_raises(self):
        # base is FOOBAR → not in either mapping; trailing raise should fire
        # with the original (unstripped) type_str in the message.
        with pytest.raises(ValueError, match="'FOOBAR\\(1\\)' is not a supported type"):
            _sf_type_to_type_object("FOOBAR(1)")

    # --- error cases ---

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Empty type string"):
            _sf_type_to_type_object("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="Empty type string"):
            _sf_type_to_type_object("   ")

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="is not a supported type"):
            _sf_type_to_type_object("FOOBAR")

    # --- case insensitivity ---

    def test_lowercase_varchar(self):
        assert _sf_type_to_type_object("varchar") == StringType()

    def test_mixed_case_array(self):
        result = _sf_type_to_type_object("Array(Boolean)")
        assert isinstance(result, ArrayType)
        assert result.element_type == BooleanType()
        assert result.structured is True


# ---------------------------------------------------------------------------
# _parse_structured_type_str
# ---------------------------------------------------------------------------

MAX_STRING_SIZE = 16777216


class TestParseStructuredTypeStr:
    def test_empty_string(self):
        assert _parse_structured_type_str("", MAX_STRING_SIZE) == VariantType()

    def test_whitespace_only(self):
        assert _parse_structured_type_str("   ", MAX_STRING_SIZE) == VariantType()

    def test_simple_boolean(self):
        result = _parse_structured_type_str("BOOLEAN", MAX_STRING_SIZE)
        assert result == BooleanType()

    def test_simple_date(self):
        result = _parse_structured_type_str("DATE", MAX_STRING_SIZE)
        assert result == DateType()

    def test_number_with_precision_and_scale(self):
        result = _parse_structured_type_str("NUMBER(38,6)", MAX_STRING_SIZE)
        assert result == DecimalType(38, 6)

    def test_number_integer(self):
        result = _parse_structured_type_str("NUMBER(38,0)", MAX_STRING_SIZE)
        assert result == LongType()

    def test_structured_array(self):
        result = _parse_structured_type_str("ARRAY(VARCHAR)", MAX_STRING_SIZE)
        assert isinstance(result, ArrayType)
        assert result.structured is True
        assert result.element_type == StringType()

    def test_structured_map(self):
        result = _parse_structured_type_str("MAP(VARCHAR, BOOLEAN)", MAX_STRING_SIZE)
        assert isinstance(result, MapType)
        assert result.structured is True
        assert result.key_type == StringType()
        assert result.value_type == BooleanType()

    def test_structured_object(self):
        result = _parse_structured_type_str(
            "OBJECT(a VARCHAR, b NUMBER(10,0))", MAX_STRING_SIZE
        )
        assert isinstance(result, StructType)
        assert result.structured is True
        assert len(result.fields) == 2
        assert result.fields[0].name == "A"
        assert result.fields[0].datatype == StringType()
        assert result.fields[1].name == "B"
        assert result.fields[1].datatype == DecimalType(10, 0)

    def test_simple_text(self):
        result = _parse_structured_type_str("TEXT", MAX_STRING_SIZE)
        assert isinstance(result, StringType)

    def test_timestamp_ntz(self):
        result = _parse_structured_type_str("TIMESTAMP_NTZ", MAX_STRING_SIZE)
        assert result == TimestampType(timezone=TimestampTimeZone.NTZ)

    def test_non_structured_with_parens(self):
        # VARCHAR(100) is not OBJECT/MAP/ARRAY, so it goes through
        # convert_sf_to_sp_type with parsed precision/scale
        result = _parse_structured_type_str("NUMBER(10,2)", MAX_STRING_SIZE)
        assert result == DecimalType(10, 2)

    def test_nested_structured(self):
        result = _parse_structured_type_str(
            "ARRAY(OBJECT(x NUMBER(10,0), y VARCHAR NOT NULL))",
            MAX_STRING_SIZE,
        )
        assert isinstance(result, ArrayType)
        assert result.structured is True
        inner = result.element_type
        assert isinstance(inner, StructType)
        assert inner.structured is True
        assert inner.fields[1].nullable is False

    def test_bare_object_returns_variant(self):
        result = _parse_structured_type_str("OBJECT", MAX_STRING_SIZE)
        assert result == VariantType()

    def test_bare_map_returns_variant(self):
        result = _parse_structured_type_str("MAP", MAX_STRING_SIZE)
        assert result == VariantType()

    def test_bare_array_returns_variant(self):
        result = _parse_structured_type_str("ARRAY", MAX_STRING_SIZE)
        assert result == VariantType()

    def test_bare_object_lowercase_returns_variant(self):
        result = _parse_structured_type_str("object", MAX_STRING_SIZE)
        assert result == VariantType()

    # --- precision/scale parse fallback: non-structured type with
    #     non-numeric parens content should fall through to
    #     convert_sf_to_sp_type with precision=0, scale=0 instead of
    #     bubbling the ValueError from int() ---

    def test_non_numeric_paren_inner_falls_back_to_zero_precision(self):
        # "TEXT(MAX)" — inner "MAX" is not parseable as int. The except
        # branch should swallow the ValueError, produce precision=0/
        # scale=0, and convert_sf_to_sp_type("TEXT", 0, 0, ...) returns
        # a default-sized StringType.
        result = _parse_structured_type_str("TEXT(MAX)", MAX_STRING_SIZE)
        assert isinstance(result, StringType)

    def test_empty_paren_inner_falls_back_to_zero_precision(self):
        # "FIXED()" — inner is empty, int("") raises ValueError. With
        # precision=0/scale=0, convert_sf_to_sp_type returns LongType.
        result = _parse_structured_type_str("FIXED()", MAX_STRING_SIZE)
        assert isinstance(result, LongType)


# ---------------------------------------------------------------------------
# _infer_schema_for_file_format  (mock-based)
# ---------------------------------------------------------------------------


def _make_mock_session(use_structured=True):
    """Build a minimal mock session for DataFrameReader."""
    session = mock.MagicMock()
    session._use_structured_type_infer_schema = use_structured
    session._use_scoped_temp_objects = True
    session._conn.max_string_size = MAX_STRING_SIZE
    session.get_fully_qualified_name_if_possible = lambda n: n
    return session


def _build_infer_schema_rows(columns):
    """Build fake INFER_SCHEMA result rows.

    Each element of *columns* is a tuple of
    (column_name, type_string, nullable, expression, filenames, order_id).
    """
    return [
        (name, type_str, nullable, expr, "file.parquet", idx)
        for idx, (name, type_str, nullable, expr) in enumerate(columns)
    ]


class TestInferSchemaStructuredTypePath:
    """Tests the structured-type branch inside _infer_schema_for_file_format."""

    def _run_infer(
        self,
        columns,
        use_structured=True,
        use_relaxed_types=False,
        file_format="PARQUET",
    ):
        session = _make_mock_session(use_structured=use_structured)
        rows = _build_infer_schema_rows(columns)

        # run_query returns the INFER_SCHEMA result on the first call,
        # and an empty dict for the file-format cleanup call.
        session._conn.run_query.side_effect = [
            # CREATE FILE FORMAT
            {},
            # INFER_SCHEMA query
            {"data": rows},
            # DROP FILE FORMAT cleanup
            {},
        ]

        reader = DataFrameReader(session, _emit_ast=False)
        if use_relaxed_types:
            reader._cur_options["INFER_SCHEMA_OPTIONS"] = {
                "USE_RELAXED_TYPES": True,
            }

        (
            new_schema,
            schema_to_cast,
            transformations,
            exception,
        ) = reader._infer_schema_for_file_format("@stage/path", file_format)
        assert exception is None, f"Unexpected exception: {exception}"
        return new_schema, schema_to_cast, transformations

    # --- schema parsing ---

    def test_simple_types_parsed(self):
        columns = [
            ("id", "NUMBER(38,0)", True, "$1:id::NUMBER(38,0)"),
            ("name", "TEXT", True, "$1:name::TEXT"),
        ]
        schema, _, _ = self._run_infer(columns)

        assert len(schema) == 2
        assert schema[0].datatype == LongType()
        assert schema[1].datatype == StringType()

    def test_structured_array_parsed(self):
        columns = [
            (
                "tags",
                "ARRAY(VARCHAR NOT NULL)",
                True,
                "$1:tags::ARRAY(VARCHAR NOT NULL)",
            ),
        ]
        schema, _, _ = self._run_infer(columns)

        dt = schema[0].datatype
        assert isinstance(dt, ArrayType)
        assert dt.structured is True
        assert dt.element_type == StringType()
        assert dt.contains_null is False

    def test_structured_map_parsed(self):
        columns = [
            ("kv", "MAP(VARCHAR, BOOLEAN)", True, "$1:kv::MAP(VARCHAR, BOOLEAN)"),
        ]
        schema, _, _ = self._run_infer(columns)

        dt = schema[0].datatype
        assert isinstance(dt, MapType)
        assert dt.structured is True
        assert dt.key_type == StringType()
        assert dt.value_type == BooleanType()

    def test_structured_object_parsed(self):
        columns = [
            (
                "address",
                "OBJECT(city VARCHAR, zip NUMBER(38,0) NOT NULL)",
                True,
                "$1:address::OBJECT(city VARCHAR, zip NUMBER(38,0) NOT NULL)",
            ),
        ]
        schema, _, _ = self._run_infer(columns)

        dt = schema[0].datatype
        assert isinstance(dt, StructType)
        assert dt.structured is True
        assert len(dt.fields) == 2
        assert dt.fields[0].datatype == StringType()
        assert dt.fields[0].nullable is True
        assert dt.fields[1].datatype == DecimalType(38, 0)
        assert dt.fields[1].nullable is False

    # --- SQL identifier generation ---

    def test_parquet_identifier_uses_raw_type_string(self):
        type_str = "OBJECT(city VARCHAR, zip NUMBER(38,0))"
        columns = [
            ("address", type_str, True, "$1:address::something"),
        ]
        _, schema_to_cast, _ = self._run_infer(columns)

        identifier = schema_to_cast[0][0]
        assert f"::{type_str}" in identifier

    def test_parquet_identifier_strips_not_null(self):
        type_str = "ARRAY(NUMBER(10,0) NOT NULL)"
        columns = [
            ("nums", type_str, True, "$1:nums::something"),
        ]
        _, schema_to_cast, _ = self._run_infer(columns)

        identifier = schema_to_cast[0][0]
        assert "NOT NULL" not in identifier
        assert "::ARRAY(NUMBER(10,0))" in identifier

    def test_parquet_relaxed_identifier_uses_converted_type(self):
        columns = [
            ("val", "NUMBER(10,2)", True, "$1:val::NUMBER(10,2)"),
        ]
        _, schema_to_cast, _ = self._run_infer(columns, use_relaxed_types=True)

        identifier = schema_to_cast[0][0]
        # With relaxed types, NUMBER(10,2) -> DoubleType -> "DOUBLE" in SQL
        assert "DOUBLE" in identifier.upper()

    # --- relaxed types applied to structured types ---

    def test_relaxed_types_on_structured_array(self):
        columns = [
            ("nums", "ARRAY(NUMBER(10,2) NOT NULL)", True, "$1:nums::something"),
        ]
        schema, _, _ = self._run_infer(columns, use_relaxed_types=True)

        dt = schema[0].datatype
        assert isinstance(dt, ArrayType)
        assert dt.structured is True
        # most_permissive_type converts numeric element to DoubleType
        assert dt.element_type == DoubleType()
        # most_permissive_type sets contains_null to True
        assert dt.contains_null is True

    def test_relaxed_types_on_structured_object(self):
        columns = [
            (
                "rec",
                "OBJECT(x NUMBER(10,2), y VARCHAR(50))",
                True,
                "$1:rec::something",
            ),
        ]
        schema, _, _ = self._run_infer(columns, use_relaxed_types=True)

        dt = schema[0].datatype
        assert isinstance(dt, StructType)
        assert dt.structured is True
        assert dt.fields[0].datatype == DoubleType()
        assert dt.fields[1].datatype == StringType()

    # --- flag disabled: legacy path used ---

    def test_legacy_path_when_flag_disabled(self):
        columns = [
            ("id", "NUMBER(38,0)", True, "$1:id::NUMBER(38,0)"),
        ]
        schema, schema_to_cast, _ = self._run_infer(columns, use_structured=False)

        assert schema[0].datatype == LongType()
        identifier = schema_to_cast[0][0]
        # Legacy path uses the raw type string directly
        assert "::NUMBER(38,0)" in identifier

    # --- mixed simple and structured columns ---

    def test_mixed_columns(self):
        columns = [
            ("id", "NUMBER(38,0)", True, "$1:id::NUMBER(38,0)"),
            ("name", "TEXT", True, "$1:name::TEXT"),
            ("tags", "ARRAY(VARCHAR)", True, "$1:tags::ARRAY(VARCHAR)"),
            ("meta", "MAP(VARCHAR, BOOLEAN)", True, "$1:meta::MAP(VARCHAR, BOOLEAN)"),
            (
                "addr",
                "OBJECT(city VARCHAR, zip NUMBER(10,0))",
                True,
                "$1:addr::OBJECT(city VARCHAR, zip NUMBER(10,0))",
            ),
        ]
        schema, schema_to_cast, transformations = self._run_infer(columns)

        assert len(schema) == 5
        assert schema[0].datatype == LongType()
        assert schema[1].datatype == StringType()
        assert isinstance(schema[2].datatype, ArrayType)
        assert isinstance(schema[3].datatype, MapType)
        assert isinstance(schema[4].datatype, StructType)

        assert len(schema_to_cast) == 5
        assert len(transformations) == 5

    # --- bare structured keywords (older backends) ---

    def test_bare_object_returns_variant_type(self):
        columns = [
            ("address", "OBJECT", True, "$1:address::OBJECT"),
        ]
        schema, schema_to_cast, _ = self._run_infer(columns)

        assert schema[0].datatype == VariantType()
        assert schema_to_cast[0][0] == '$1:"address"'

    def test_bare_map_returns_variant_type(self):
        columns = [
            ("props", "MAP", True, "$1:props::MAP"),
        ]
        schema, schema_to_cast, _ = self._run_infer(columns)

        assert schema[0].datatype == VariantType()
        assert schema_to_cast[0][0] == '$1:"props"'

    def test_bare_array_returns_variant_type(self):
        columns = [
            ("tags", "ARRAY", True, "$1:tags::ARRAY"),
        ]
        schema, schema_to_cast, _ = self._run_infer(columns)

        assert schema[0].datatype == VariantType()
        assert schema_to_cast[0][0] == '$1:"tags"'

    def test_mixed_bare_and_detailed_structured(self):
        columns = [
            ("id", "NUMBER(38,0)", True, "$1:id::NUMBER(38,0)"),
            ("addr", "OBJECT", True, "$1:addr::OBJECT"),
            (
                "tags",
                "ARRAY(VARCHAR NOT NULL)",
                True,
                "$1:tags::ARRAY(VARCHAR NOT NULL)",
            ),
            ("meta", "MAP", True, "$1:meta::MAP"),
        ]
        schema, schema_to_cast, _ = self._run_infer(columns)

        assert schema[0].datatype == LongType()
        assert schema[1].datatype == VariantType()
        assert isinstance(schema[2].datatype, ArrayType)
        assert schema[3].datatype == VariantType()
        # bare keywords get no cast; detailed types get the cast
        assert schema_to_cast[1][0] == '$1:"addr"'
        assert "::ARRAY(VARCHAR)" in schema_to_cast[2][0]
        assert schema_to_cast[3][0] == '$1:"meta"'

    # --- JSON format path ---

    def test_json_format_uses_structured_path(self):
        columns = [
            ("id", "NUMBER(38,0)", True, "$1:id::NUMBER(38,0)"),
            ("name", "TEXT", True, "$1:name::TEXT"),
        ]
        schema, schema_to_cast, _ = self._run_infer(columns, file_format="JSON")

        assert len(schema) == 2
        assert schema[0].datatype == LongType()
        assert schema[1].datatype == StringType()
        assert "::NUMBER(38,0)" in schema_to_cast[0][0]
        assert "::TEXT" in schema_to_cast[1][0]

    def test_json_format_structured_array(self):
        columns = [
            (
                "tags",
                "ARRAY(VARCHAR NOT NULL)",
                True,
                "$1:tags::ARRAY(VARCHAR NOT NULL)",
            ),
        ]
        schema, schema_to_cast, _ = self._run_infer(columns, file_format="JSON")

        dt = schema[0].datatype
        assert isinstance(dt, ArrayType)
        assert dt.structured is True
        assert dt.contains_null is False
        assert "NOT NULL" not in schema_to_cast[0][0]

    def test_json_format_bare_map(self):
        columns = [
            ("props", "MAP", True, "$1:props::MAP"),
        ]
        schema, schema_to_cast, _ = self._run_infer(columns, file_format="JSON")

        assert schema[0].datatype == VariantType()
        assert schema_to_cast[0][0] == '$1:"props"'


# ---------------------------------------------------------------------------
# Session parameter defaults
# ---------------------------------------------------------------------------


class TestSessionParameterDefaults:
    def test_structured_infer_schema_default_is_false(self):
        session = _make_mock_session(use_structured=False)
        assert session._use_structured_type_infer_schema is False

    def test_structured_infer_schema_can_be_enabled(self):
        session = _make_mock_session(use_structured=True)
        assert session._use_structured_type_infer_schema is True

    def test_flag_controls_parser_path(self):
        """When the flag is True, structured types are parsed recursively;
        when False, the legacy identifier path is used."""
        struct_columns = [
            (
                "addr",
                "OBJECT(city VARCHAR, zip NUMBER(38,0))",
                True,
                "$1:addr::OBJECT(city VARCHAR, zip NUMBER(38,0))",
            ),
        ]

        # With flag ON: recursive parser produces StructType
        session_on = _make_mock_session(use_structured=True)
        rows = _build_infer_schema_rows(struct_columns)
        session_on._conn.run_query.side_effect = [{}, {"data": rows}, {}]
        reader_on = DataFrameReader(session_on, _emit_ast=False)
        schema_on, _, _, exc_on = reader_on._infer_schema_for_file_format(
            "@stage/path", "PARQUET"
        )
        assert exc_on is None
        assert isinstance(schema_on[0].datatype, StructType)

        # With flag OFF: uses the legacy identifier path (raw type string)
        simple_columns = [
            ("id", "NUMBER(38,0)", True, "$1:id::NUMBER(38,0)"),
        ]
        session_off = _make_mock_session(use_structured=False)
        rows = _build_infer_schema_rows(simple_columns)
        session_off._conn.run_query.side_effect = [{}, {"data": rows}, {}]
        reader_off = DataFrameReader(session_off, _emit_ast=False)
        schema_off, cast_off, _, exc_off = reader_off._infer_schema_for_file_format(
            "@stage/path", "PARQUET"
        )
        assert exc_off is None
        assert schema_off[0].datatype == LongType()
        assert "::NUMBER(38,0)" in cast_off[0][0]
