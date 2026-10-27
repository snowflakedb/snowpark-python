#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.document_reader_options import (
    _DEFAULT_CORRUPT_RECORD_COLUMN,
    _PARSE_MODES,
    _USER_PARSE_MODES,
    DocumentReaderOptions,
    ExtractionSpec,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import BooleanType, DoubleType, LongType, StringType

# `.option()` upper-cases every key it stores in `_cur_options`
# (`get_aliased_option_name`), so `from_reader_options` only ever reads upper-case
# keys. These dicts are built with the same keys the reader would produce.

# A flat {name: prompt} dict is AI_EXTRACT's own primary documented
# response_format shape -- see functions.py::ai_extract's docstring. It has no
# "properties" wrapper; the top-level keys ARE the field names.
FLAT_SCHEMA = {
    "invoice_number": "What is the invoice number?",
    "total": "What is the total amount?",
}

# The JSON-Schema-with-properties shape the P0 design also wants supported
# (full formal support in AI_EXTRACT's own wrapper is separate follow-up work;
# this reader passes it through untouched either way).
PROPERTIES_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string"},
        "total": {"type": "number"},
    },
}


def _build(options: dict) -> DocumentReaderOptions:
    return DocumentReaderOptions.from_reader_options(options)


def _build_direct(**kwargs) -> DocumentReaderOptions:
    """`from_reader_options` only reads SCHEMA, PROMPT, PARSE_MODE and ROW_BOUNDARY, so
    the remaining fields (mode, extraction_engine, ...) are only reachable by
    constructing DocumentReaderOptions directly and running validate() ourselves, the
    way from_reader_options itself does."""
    options = DocumentReaderOptions(**kwargs)
    options.validate()
    return options


def _expect_error(error_code: str, options: dict) -> SnowparkDataframeReaderException:
    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        _build(options)
    assert exc_info.value.error_code == error_code
    return exc_info.value


def _expect_validate_error(
    error_code: str, **kwargs
) -> SnowparkDataframeReaderException:
    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        _build_direct(**kwargs)
    assert exc_info.value.error_code == error_code
    return exc_info.value


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


class TestDefaults:
    def test_empty_options(self):
        options = _build({})
        assert options.parse_mode == "layout"
        assert options.row_boundary == "document"
        assert options.extract_images is False
        assert options.page_filter is None
        assert options.mode == "PERMISSIVE"
        assert options.corrupt_record_column == _DEFAULT_CORRUPT_RECORD_COLUMN
        assert options.extraction_engine == "ai_complete"
        assert options.extraction is None
        assert options.prompt is None

    def test_default_corrupt_record_column_differs_from_csv_json(self):
        # The documents reader intentionally uses "_document_error", not the
        # CSV/JSON reader's "_corrupt_record".
        assert _DEFAULT_CORRUPT_RECORD_COLUMN == "_document_error"
        assert _build({}).corrupt_record_column == "_document_error"

    def test_derived_properties_under_defaults(self):
        options = _build({})
        assert options.parse_enabled is True
        assert options.extract_enabled is False
        assert options.complete_enabled is False
        assert options.page_rows is False

    def test_custom_corrupt_record_column_case_is_preserved(self):
        # columnNameOfCorruptRecord is no longer settable through from_reader_options
        # (see TestUnsupportedOptionsRejected), so this is exercised by constructing
        # DocumentReaderOptions directly. Only the *key* was ever upper-cased by
        # .option(); the value is the user's chosen column name and must survive
        # verbatim.
        options = _build_direct(corrupt_record_column="myErrorCol")
        assert options.corrupt_record_column == "myErrorCol"

    def test_page_filter_passes_through(self):
        page_filter = [{"start": 0, "end": 2}]
        options = _build_direct(page_filter=page_filter)
        assert options.page_filter == page_filter


# ---------------------------------------------------------------------------
# User options -- from_reader_options reads SCHEMA, PROMPT, PARSE_MODE and
# ROW_BOUNDARY (keys arrive upper-cased via .option()); every other key, such as
# MODEL, MODE or EXTRACTION_ENGINE, is chosen by the reader and ignored.
# ---------------------------------------------------------------------------


class TestUserOptions:
    @pytest.mark.parametrize(
        "key",
        [
            "PAGE_FILTER",
            "MODE",
            "COLUMNNAMEOFCORRUPTRECORD",
            "EXTRACTION_ENGINE",
            "MODEL",
            "BOGUS_OPTION",
        ],
    )
    def test_options_chosen_by_the_reader_are_ignored(self, key):
        options = _build({key: "some-value"})
        defaults = DocumentReaderOptions()
        assert options.mode == defaults.mode == "PERMISSIVE"
        assert options.extraction_engine == defaults.extraction_engine
        assert options.extract_images is False
        assert options.page_filter is None
        assert options.corrupt_record_column == defaults.corrupt_record_column

    @pytest.mark.parametrize("value", ["layout", "ocr", "text", "OCR", "Text"])
    def test_parse_mode_is_accepted_case_insensitively(self, value):
        assert _build({"PARSE_MODE": value}).parse_mode == value.lower()

    @pytest.mark.parametrize("value", ["page", "PAGE", "document"])
    def test_row_boundary_is_accepted_case_insensitively(self, value):
        assert _build({"ROW_BOUNDARY": value}).row_boundary == value.lower()

    def test_page_rows_follow_from_row_boundary(self):
        assert _build({"ROW_BOUNDARY": "page"}).page_rows is True

    def test_parse_mode_none_is_not_a_user_choice(self):
        error = _expect_error("1116", {"PARSE_MODE": "none"})
        assert "parse_mode" in error.message

    def test_user_parse_modes_are_every_parse_mode_but_none(self):
        assert _USER_PARSE_MODES == _PARSE_MODES - {"none"}
        assert _USER_PARSE_MODES == {"layout", "ocr", "text"}

    def test_invalid_parse_mode_raises_1116(self):
        error = _expect_error("1116", {"PARSE_MODE": "bogus"})
        assert "layout" in error.message and "ocr" in error.message

    def test_invalid_row_boundary_raises_1116(self):
        error = _expect_error("1116", {"ROW_BOUNDARY": "chapter"})
        assert "row_boundary" in error.message

    @pytest.mark.parametrize(
        "extra", [{"SCHEMA": FLAT_SCHEMA}, {"PROMPT": "Extract fields."}]
    )
    def test_parse_options_are_ignored_with_a_schema_or_prompt(self, extra):
        options = _build({"PARSE_MODE": "ocr", "ROW_BOUNDARY": "page", **extra})
        assert options.parse_mode == "none"
        assert options.row_boundary == "document"

    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("True", True),
            ("FALSE", False),
        ],
    )
    def test_extract_images_accepts_booleans_in_any_case(self, value, expected):
        assert _build({"EXTRACT_IMAGES": value}).extract_images is expected

    def test_extract_images_is_off_by_default(self):
        assert _build({}).extract_images is False

    @pytest.mark.parametrize("value", ["yes", "1", 1, None, "truee"])
    def test_extract_images_rejects_other_values_with_1116(self, value):
        error = _expect_error("1116", {"EXTRACT_IMAGES": value})
        assert "extract_images" in error.message
        assert "true" in error.message and "false" in error.message

    def test_extract_images_works_with_layout_and_page_rows(self):
        options = _build({"EXTRACT_IMAGES": True, "ROW_BOUNDARY": "page"})
        assert options.extract_images is True
        assert options.parse_mode == "layout"
        assert options.page_rows is True

    @pytest.mark.parametrize("mode", ["ocr", "text", "OCR"])
    def test_extract_images_needs_layout_mode(self, mode):
        error = _expect_error("1119", {"EXTRACT_IMAGES": True, "PARSE_MODE": mode})
        assert "layout" in error.message and mode.lower() in error.message

    def test_extract_images_false_is_fine_with_any_parse_mode(self):
        assert (
            _build({"EXTRACT_IMAGES": False, "PARSE_MODE": "ocr"}).extract_images
            is False
        )

    @pytest.mark.parametrize(
        "extra", [{"SCHEMA": FLAT_SCHEMA}, {"PROMPT": "Extract fields."}]
    )
    def test_extract_images_is_ignored_with_a_schema_or_prompt(self, extra):
        # Not even validated, like PARSE_MODE and ROW_BOUNDARY: such a read skips parsing.
        options = _build({"EXTRACT_IMAGES": True, "PARSE_MODE": "ocr", **extra})
        assert options.extract_images is False
        assert options.parse_mode == "none"

    def test_schema_and_prompt_together_are_still_accepted(self):
        # Does not raise.
        options = _build({"SCHEMA": FLAT_SCHEMA, "PROMPT": "Extract fields."})
        assert options.extract_enabled is True
        assert options.prompt == "Extract fields."


# ---------------------------------------------------------------------------
# complete_enabled (schema or prompt present) is what now decides parse_mode and
# which stage(s) can error -- see from_reader_options and error_stages.
# ---------------------------------------------------------------------------


class TestCompleteEnabledDefaults:
    def test_neither_schema_nor_prompt_keeps_todays_default_parse_read(self):
        options = _build({})
        assert options.parse_mode == "layout"
        assert options.extraction is None
        assert options.complete_enabled is False
        assert [stage for stage, _ in options.error_stages] == ["parse"]

    def test_schema_switches_to_unparsed_ai_complete(self):
        options = _build({"SCHEMA": FLAT_SCHEMA})
        assert options.parse_mode == "none"
        assert options.parse_enabled is False
        assert options.extraction_engine == "ai_complete"
        assert options.complete_enabled is True

    def test_prompt_only_has_no_extraction_but_is_complete_enabled(self):
        options = _build({"PROMPT": "Summarize this document."})
        assert options.extraction is None
        assert options.extract_enabled is False
        assert options.complete_enabled is True
        assert options.parse_mode == "none"
        assert [stage for stage, _ in options.error_stages] == ["extract"]


# ---------------------------------------------------------------------------
# Closed-set enum checks (code 1116) -- the only remaining guards on parse_mode,
# row_boundary, mode and extraction_engine. They drive our own Python routing and
# are never sent to Snowflake, so nothing downstream would catch an unrecognized
# value. Only reachable now by constructing DocumentReaderOptions directly, since
# from_reader_options() no longer accepts any of these keys.
# ---------------------------------------------------------------------------


class TestClosedSetEnums:
    @pytest.mark.parametrize(
        "attribute,valid_value",
        [
            ("parse_mode", "ocr"),
            ("row_boundary", "page"),
            ("mode", "FAILFAST"),
            ("extraction_engine", "ai_extract"),
        ],
    )
    def test_valid_value_accepted(self, attribute, valid_value):
        options = _build_direct(**{attribute: valid_value})
        assert getattr(options, attribute) == valid_value

    @pytest.mark.parametrize(
        "attribute",
        ["parse_mode", "row_boundary", "mode", "extraction_engine"],
    )
    def test_invalid_value_raises_1116(self, attribute):
        error = _expect_validate_error("1116", **{attribute: "bogus"})
        assert attribute in error.message
        assert "bogus" in error.message.lower()

    @pytest.mark.parametrize("parse_mode", ["layout", "ocr", "text", "none"])
    def test_every_parse_mode_is_accepted(self, parse_mode):
        assert _build_direct(parse_mode=parse_mode).parse_mode == parse_mode

    def test_invalid_value_message_lists_valid_values(self):
        error = _expect_validate_error("1116", row_boundary="chapter")
        assert "document" in error.message
        assert "page" in error.message


# ---------------------------------------------------------------------------
# ExtractionSpec.from_response_format -- derives output field names across every
# shape AI_EXTRACT's response_format documents, without restricting which shapes
# are accepted. response_format itself is never validated; whatever the user
# passes goes to AI_EXTRACT/AI_COMPLETE untouched, and AISQL is what rejects a
# malformed one.
# ---------------------------------------------------------------------------


def _fields(response_format) -> list:
    spec = ExtractionSpec.from_response_format(response_format)
    return spec.fields if spec else []


class TestExtractionSpecFields:
    def test_none_or_empty_has_no_fields(self):
        assert _fields(None) == []
        assert _fields({}) == []
        assert _fields([]) == []

    def test_flat_dict_uses_its_own_keys(self):
        # AI_EXTRACT's primary documented shape: {name: prompt}.
        assert _fields(FLAT_SCHEMA) == ["invoice_number", "total"]

    def test_properties_dict_uses_properties_keys(self):
        assert _fields(PROPERTIES_SCHEMA) == ["invoice_number", "total"]

    def test_dict_with_a_field_literally_named_properties_is_not_misread(self):
        # "properties" mapping to a STRING (a prompt, not a nested schema) means
        # this is a flat Q&A dict with a field that happens to be named
        # "properties", not a JSON-Schema wrapper -- must not be misdetected.
        schema = {"properties": "What are the material properties?"}
        assert _fields(schema) == ["properties"]

    def test_list_of_name_prompt_pairs_uses_first_element(self):
        schema = [["name", "What is the name?"], ["city", "What city?"]]
        assert _fields(schema) == ["name", "city"]

    def test_list_of_colon_separated_strings_uses_text_before_colon(self):
        schema = ["name: What is the name?", "city: What city?"]
        assert _fields(schema) == ["name", "city"]

    def test_list_of_bare_prompt_strings_has_no_separate_name(self):
        # This AI_EXTRACT shape gives no field name at all -- the prompt text
        # itself is the only available key.
        schema = ["What is the name?", "What city?"]
        assert _fields(schema) == ["What is the name?", "What city?"]

    def test_unrecognized_shape_has_no_derivable_fields(self):
        assert _fields(42) == []
        assert _fields("invoice_number") == []

    def test_list_with_non_string_items_does_not_raise(self):
        # A malformed response_format (e.g. a stray int) must not blow up with
        # an AttributeError from field.upper() -- field_and_question() always
        # returns a string name, even for a shape this reader doesn't otherwise
        # recognize.
        assert _fields([42, "city: What city?"]) == ["42", "city"]
        assert _fields([[42, "What is it?"]]) == ["42"]


# ---------------------------------------------------------------------------
# ExtractionSpec.field_types -- a JSON-Schema response_format's declared per-field
# type, when it maps to an unambiguous Snowpark scalar. Drives whether
# cast_extracted_field() casts the output column or leaves it as the raw VARIANT
# AI_EXTRACT/AI_COMPLETE returned.
# ---------------------------------------------------------------------------


def _field_types(response_format) -> dict:
    return ExtractionSpec.from_response_format(response_format).field_types


class TestExtractionSpecFieldTypes:
    def test_flat_dict_fields_resolve_to_string_from_the_converted_schema(self):
        # AI_EXTRACT's own flat {name: prompt} shape carries no type contract of
        # its own, but it is now converted into a string-typed JSON Schema for
        # AI_COMPLETE (see ExtractionSpec.from_response_format) -- field_types is
        # derived from that converted schema, so every field resolves to
        # StringType rather than the raw-VARIANT None it used to get.
        types = _field_types(FLAT_SCHEMA)
        assert isinstance(types["invoice_number"], StringType)
        assert isinstance(types["total"], StringType)

    def test_json_schema_scalar_types_resolve(self):
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "amount": {"type": "number"},
                "quantity": {"type": "integer"},
                "is_paid": {"type": "boolean"},
            },
        }
        types = _field_types(schema)
        assert isinstance(types["name"], StringType)
        assert isinstance(types["amount"], DoubleType)
        assert isinstance(types["quantity"], LongType)
        assert isinstance(types["is_paid"], BooleanType)

    def test_json_schema_composite_types_have_no_declared_scalar(self):
        # array/object are deliberately absent from the scalar map: AI_EXTRACT/
        # AI_COMPLETE already return these as a real ARRAY/OBJECT VARIANT, and
        # forcing them to a scalar would discard the nesting the caller declared.
        schema = {
            "type": "object",
            "properties": {
                "tags": {"type": "array", "items": {"type": "string"}},
                "address": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                },
            },
        }
        assert _field_types(schema) == {"tags": None, "address": None}

    def test_json_schema_missing_or_malformed_property_has_no_declared_type(self):
        schema = {
            "type": "object",
            "properties": {
                "untyped": {},
                # A type union (e.g. ["string", "null"]) isn't a single scalar --
                # falls back to None rather than raising on the unhashable list.
                "nullable_string": {"type": ["string", "null"]},
                # Not itself a dict -- malformed, but must not raise.
                "malformed": "not a schema",
            },
        }
        assert _field_types(schema) == {
            "untyped": None,
            "nullable_string": None,
            "malformed": None,
        }

    def test_unrecognized_json_schema_type_has_no_declared_scalar(self):
        schema = {"type": "object", "properties": {"blob": {"type": "null"}}}
        assert _field_types(schema) == {"blob": None}


# ---------------------------------------------------------------------------
# schema option -- accepted verbatim, in any shape AI_EXTRACT documents, and
# normalized once into options.extraction (an ExtractionSpec). There is no
# client-side shape guard: this reader is not the one that decides what
# AI_EXTRACT/AI_COMPLETE will accept.
# ---------------------------------------------------------------------------


class TestSchemaPassesThroughUnrestricted:
    def test_flat_schema_passes(self):
        options = _build({"SCHEMA": FLAT_SCHEMA})
        assert options.extraction.ai_extract_format == FLAT_SCHEMA
        assert options.extract_enabled is True
        assert options.extraction.fields == ["invoice_number", "total"]

    def test_properties_schema_passes(self):
        options = _build({"SCHEMA": PROPERTIES_SCHEMA})
        # AI_EXTRACT rejects a bare JSON-Schema dict outright ("invalid response
        # format") -- confirmed live -- so it must be wrapped as {"schema": ...},
        # not passed through as given the way AI_EXTRACT's own flat Q&A dicts are.
        assert options.extraction.ai_extract_format == {"schema": PROPERTIES_SCHEMA}
        assert options.extraction.fields == ["invoice_number", "total"]

    def test_no_schema_means_extraction_is_disabled(self):
        options = _build({})
        assert options.extraction is None
        assert options.extract_enabled is False

    @pytest.mark.parametrize(
        "schema",
        [
            pytest.param(["What is the invoice number?"], id="list_of_strings"),
            pytest.param(
                [["invoice_number", "What is the invoice number?"]], id="list_of_pairs"
            ),
            pytest.param(
                ["invoice_number: What is the invoice number?"],
                id="colon_separated_strings",
            ),
        ],
    )
    def test_every_shape_is_accepted_without_error(self, schema):
        # None of these raise -- shape validation is AISQL's job, not ours.
        options = _build({"SCHEMA": schema})
        assert options.extraction.ai_extract_format == schema

    def test_empty_dict_schema_disables_extraction(self):
        # {} is falsy, so it's treated the same as "no schema given" -- there
        # are no fields to derive and nothing to extract.
        options = _build({"SCHEMA": {}})
        assert options.extraction is None
        assert options.extract_enabled is False


# ---------------------------------------------------------------------------
# ExtractionSpec.ai_complete_format -- AI_COMPLETE only accepts a JSON Schema.
# A bare JSON-Schema dict (has a "properties" dict) is wrapped as given;
# AI_EXTRACT's own flat Q&A shapes (dict or list) have no question/answer notion
# AI_COMPLETE understands, so they are converted into a JSON Schema whose fields
# are all strings, with the question (if any) carried as "description". Computed
# once per ExtractionSpec, not per call.
# ---------------------------------------------------------------------------


def _ai_complete_format(response_format):
    return ExtractionSpec.from_response_format(response_format).ai_complete_format


class TestAiCompleteFormat:
    def test_properties_schema_gets_wrapped(self):
        assert _ai_complete_format(PROPERTIES_SCHEMA) == {
            "type": "json",
            "schema": PROPERTIES_SCHEMA,
        }

    def test_flat_dict_is_converted_to_a_string_typed_json_schema(self):
        assert _ai_complete_format(FLAT_SCHEMA) == {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {
                    "invoice_number": {
                        "type": "string",
                        "description": "What is the invoice number?",
                    },
                    "total": {
                        "type": "string",
                        "description": "What is the total amount?",
                    },
                },
            },
        }

    def test_list_of_pairs_is_converted_with_descriptions(self):
        schema = [["name", "What is the name?"], ["city", "What city?"]]
        assert _ai_complete_format(schema) == {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "What is the name?"},
                    "city": {"type": "string", "description": "What city?"},
                },
            },
        }

    def test_list_of_colon_separated_strings_is_converted_with_descriptions(self):
        schema = ["name: What is the name?"]
        assert _ai_complete_format(schema) == {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "What is the name?"}
                },
            },
        }

    def test_list_of_bare_prompt_strings_has_no_description(self):
        # No separate name/question split -- the field has no question to carry
        # as "description".
        schema = ["What is the name?"]
        assert _ai_complete_format(schema) == {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {"What is the name?": {"type": "string"}},
            },
        }

    def test_extraction_spec_precomputes_both_engine_formats(self):
        options = _build({"SCHEMA": PROPERTIES_SCHEMA})
        assert options.extraction.ai_extract_format == {"schema": PROPERTIES_SCHEMA}
        assert options.extraction.ai_complete_format == {
            "type": "json",
            "schema": PROPERTIES_SCHEMA,
        }


# ---------------------------------------------------------------------------
# ExtractionSpec.ai_extract_format -- AI_EXTRACT rejects a bare JSON-Schema
# dict outright ("invalid response format", confirmed live); it needs
# {"schema": {...}}. AI_EXTRACT's own flat Q&A dict/array shapes need no
# wrapping or conversion at all -- confirmed live those work exactly as given.
# ---------------------------------------------------------------------------


def _ai_extract_format(response_format):
    return ExtractionSpec.from_response_format(response_format).ai_extract_format


class TestAiExtractFormat:
    def test_properties_schema_gets_wrapped(self):
        assert _ai_extract_format(PROPERTIES_SCHEMA) == {"schema": PROPERTIES_SCHEMA}

    def test_flat_schema_is_not_wrapped(self):
        assert _ai_extract_format(FLAT_SCHEMA) == FLAT_SCHEMA

    def test_list_shapes_are_not_wrapped(self):
        schema = [["name", "What is the name?"]]
        assert _ai_extract_format(schema) == schema


# ---------------------------------------------------------------------------
# Schema field names colliding with a reserved output column, or with each
# other once upper-cased, would silently overwrite a real column via
# with_columns()'s replace-if-exists semantics -- this IS ours to guard,
# unlike response_format's shape, because it protects our own output
# construction rather than AI_EXTRACT's/AI_COMPLETE's input.
# ---------------------------------------------------------------------------


class TestSchemaFieldNameCollisions:
    @pytest.mark.parametrize(
        "reserved_name",
        [
            "source_file",
            "SOURCE_FILE",
            "content",
            "total_pages",
            "images",
            "extraction_scores",
            "response",
        ],
    )
    def test_field_colliding_with_a_reserved_column_raises_1117(self, reserved_name):
        error = _expect_error("1117", {"SCHEMA": {reserved_name: "a question"}})
        assert reserved_name.upper() in error.message

    def test_field_colliding_with_the_corrupt_record_column_raises_1117(self):
        # columnNameOfCorruptRecord is no longer settable through
        # from_reader_options, so this collision is exercised via direct
        # construction instead.
        _expect_validate_error(
            "1117",
            extraction=ExtractionSpec.from_response_format({"my_error": "a question"}),
            corrupt_record_column="MY_ERROR",
        )

    def test_fields_colliding_with_each_other_after_upper_casing_raises_1117(self):
        schema = {"Total": "a question", "total": "another question"}
        _expect_error("1117", {"SCHEMA": schema})

    def test_default_corrupt_record_column_does_not_collide_by_default(self):
        # "_document_error" only collides with an upper-cased field if the user
        # renames columnNameOfCorruptRecord to something uppercase -- the default
        # lowercase name can never match a schema field's .upper() form.
        options = _build({"SCHEMA": {"document_error": "a question"}})
        assert options.extraction.field_columns == ["DOCUMENT_ERROR"]

    def test_non_colliding_fields_pass(self):
        options = _build({"SCHEMA": FLAT_SCHEMA})
        assert options.extraction.field_columns == ["INVOICE_NUMBER", "TOTAL"]


# ---------------------------------------------------------------------------
# A schema that derives zero fields (an unrecognized response_format shape)
# is harmless everywhere finalize_errors() can anchor on CONTENT or RESPONSE,
# or has no FAILFAST check to anchor at all -- except parse_mode="none" with no
# prompt and mode="FAILFAST", which has no column left to anchor on and
# previously raised a bare IndexError. Exercised via direct construction since
# PARSE_MODE/MODE are no longer settable through from_reader_options.
# ---------------------------------------------------------------------------


class TestSchemaWithNoFields:
    def test_failfast_without_parsing_raises_1118(self):
        error = _expect_validate_error(
            "1118",
            parse_mode="none",
            mode="FAILFAST",
            extraction=ExtractionSpec.from_response_format(42),
        )
        assert "did not resolve to any extractable fields" in error.message

    def test_permissive_without_parsing_does_not_raise(self):
        options = _build_direct(
            parse_mode="none",
            extraction=ExtractionSpec.from_response_format({"properties": {}}),
        )
        assert options.extraction.field_columns == []

    def test_failfast_with_parsing_does_not_raise(self):
        # parse_mode != "none" means finalize_errors() anchors on CONTENT,
        # regardless of how many fields the schema derived.
        options = _build_direct(
            mode="FAILFAST",
            extraction=ExtractionSpec.from_response_format({"properties": {}}),
        )
        assert options.extraction.field_columns == []


# ---------------------------------------------------------------------------
# ai_complete works with or without a parse phase: its file variant extracts
# directly from SOURCE_FILE via named-argument file + response_format, the same
# way ai_extract already does -- see extract_with_ai_complete. Exercised via
# direct construction since EXTRACTION_ENGINE/PARSE_MODE are no longer settable
# through from_reader_options.
# ---------------------------------------------------------------------------


class TestAiCompleteWorksWithOrWithoutParse:
    def test_ai_complete_with_parse_mode_none_is_not_an_error(self):
        options = _build_direct(
            extraction_engine="ai_complete",
            parse_mode="none",
            extraction=ExtractionSpec.from_response_format(FLAT_SCHEMA),
        )
        assert options.extraction_engine == "ai_complete"
        assert options.parse_enabled is False
        assert options.extract_enabled is True

    @pytest.mark.parametrize("parse_mode", ["layout", "ocr", "text"])
    def test_ai_complete_with_a_parse_phase_passes(self, parse_mode):
        options = _build_direct(
            extraction_engine="ai_complete",
            parse_mode=parse_mode,
            extraction=ExtractionSpec.from_response_format(FLAT_SCHEMA),
        )
        assert options.extraction_engine == "ai_complete"
        assert options.parse_enabled is True

    def test_ai_extract_with_parse_mode_none_passes(self):
        # ai_extract accepts a FILE input directly, so skipping parse is fine.
        options = _build_direct(
            parse_mode="none",
            extraction=ExtractionSpec.from_response_format(FLAT_SCHEMA),
        )
        assert options.parse_enabled is False
        assert options.extract_enabled is True


# ---------------------------------------------------------------------------
# Documented non-guards. Exercised via direct construction since ROW_BOUNDARY/
# PARSE_MODE/PAGE_FILTER are no longer settable through from_reader_options.
# ---------------------------------------------------------------------------


class TestDocumentedNonGuards:
    def test_page_row_boundary_with_parse_mode_none_is_a_no_op(self):
        # Parse is skipped entirely when parse_mode == "none", so row_boundary
        # is simply unused -- explicitly not an error.
        options = _build_direct(row_boundary="page", parse_mode="none")
        assert options.row_boundary == "page"
        assert options.page_rows is False

    def test_page_row_boundary_with_a_parse_phase_enables_page_rows(self):
        options = _build_direct(row_boundary="page", parse_mode="layout")
        assert options.page_rows is True

    def test_page_filter_with_parse_mode_none_is_not_an_error(self):
        # page_filter only ever applies during Parse (ai_parse_document's page_filter
        # kwarg, or the PDF UDTF's page_filter_json arg); with Parse skipped there is
        # no page-range concept to apply it to, so -- like row_boundary above -- this
        # is a no-op, not a guarded combination.
        options = _build_direct(page_filter=[{"start": 0, "end": 1}], parse_mode="none")
        assert options.parse_enabled is False
