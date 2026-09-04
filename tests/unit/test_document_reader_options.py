#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.document_reader_options import (
    _DEFAULT_CORRUPT_RECORD_COLUMN,
    DocumentReaderOptions,
    ExtractionSpec,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException

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


def _expect_error(error_code: str, options: dict) -> SnowparkDataframeReaderException:
    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        _build(options)
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
        assert options.extraction_engine == "ai_extract"
        assert options.extraction is None
        assert options.model is None
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
        assert options.page_rows is False

    def test_custom_corrupt_record_column_case_is_preserved(self):
        # Only the option *key* is upper-cased by `.option()`; the value is the
        # user's chosen column name and must survive verbatim.
        options = _build({"COLUMNNAMEOFCORRUPTRECORD": "myErrorCol"})
        assert options.corrupt_record_column == "myErrorCol"

    def test_page_filter_passes_through(self):
        page_filter = [{"start": 0, "end": 2}]
        assert _build({"PAGE_FILTER": page_filter}).page_filter == page_filter


# ---------------------------------------------------------------------------
# Closed-set enum checks (code 1116) -- the only remaining client-side guard.
# parse_mode, row_boundary, mode and extraction_engine drive our own Python
# routing and are never sent to Snowflake, so nothing downstream would catch
# an unrecognized value. Everything else (response_format, model, prompt,
# extract_images) is either forwarded to AI_EXTRACT/AI_COMPLETE untouched --
# theirs to validate -- or silently unused when another option makes it
# inapplicable.
# ---------------------------------------------------------------------------


class TestClosedSetEnums:
    @pytest.mark.parametrize(
        "key,valid_value,attribute,expected",
        [
            ("PARSE_MODE", "ocr", "parse_mode", "ocr"),
            ("ROW_BOUNDARY", "page", "row_boundary", "page"),
            ("MODE", "FAILFAST", "mode", "FAILFAST"),
            (
                "EXTRACTION_ENGINE",
                "ai_complete",
                "extraction_engine",
                "ai_complete",
            ),
        ],
    )
    def test_valid_value_accepted(self, key, valid_value, attribute, expected):
        options = _build({key: valid_value})
        assert getattr(options, attribute) == expected

    @pytest.mark.parametrize(
        "key,option_name",
        [
            ("PARSE_MODE", "parse_mode"),
            ("ROW_BOUNDARY", "row_boundary"),
            ("MODE", "mode"),
            ("EXTRACTION_ENGINE", "extraction_engine"),
        ],
    )
    def test_invalid_value_raises_1116(self, key, option_name):
        error = _expect_error("1116", {key: "bogus"})
        assert option_name in error.message
        # The message echoes the case-normalized value ("mode" is upper-cased,
        # the other three are lower-cased) rather than the raw input.
        assert "bogus" in error.message.lower()

    @pytest.mark.parametrize("parse_mode", ["layout", "ocr", "text", "none"])
    def test_every_parse_mode_is_accepted(self, parse_mode):
        assert _build({"PARSE_MODE": parse_mode}).parse_mode == parse_mode

    @pytest.mark.parametrize(
        "key,given,attribute,expected",
        [
            ("PARSE_MODE", "LAYOUT", "parse_mode", "layout"),
            ("PARSE_MODE", "Ocr", "parse_mode", "ocr"),
            ("ROW_BOUNDARY", "PAGE", "row_boundary", "page"),
            ("MODE", "permissive", "mode", "PERMISSIVE"),
            ("MODE", "failfast", "mode", "FAILFAST"),
            ("EXTRACTION_ENGINE", "AI_EXTRACT", "extraction_engine", "ai_extract"),
        ],
    )
    def test_value_case_is_normalized(self, key, given, attribute, expected):
        assert getattr(_build({key: given}), attribute) == expected

    def test_invalid_value_message_lists_valid_values(self):
        error = _expect_error("1116", {"ROW_BOUNDARY": "chapter"})
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
        assert options.extraction.ai_extract_format == PROPERTIES_SCHEMA
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
# ExtractionSpec.ai_complete_format -- AI_COMPLETE's structured-output shape
# is {"type": "json", "schema": {...}}, stricter than AI_EXTRACT's flat Q&A
# dicts. The wrap applies only when response_format already looks like a
# JSON Schema (has a "properties" dict); anything else passes through
# untouched. Computed once per ExtractionSpec, not per call.
# ---------------------------------------------------------------------------


def _ai_complete_format(response_format):
    return ExtractionSpec.from_response_format(response_format).ai_complete_format


class TestAiCompleteFormat:
    def test_properties_schema_gets_wrapped(self):
        assert _ai_complete_format(PROPERTIES_SCHEMA) == {
            "type": "json",
            "schema": PROPERTIES_SCHEMA,
        }

    def test_flat_schema_is_not_wrapped(self):
        # A flat Q&A dict isn't a JSON Schema at all; wrapping it would not make
        # it valid, so it passes through for AI_COMPLETE itself to reject.
        assert _ai_complete_format(FLAT_SCHEMA) == FLAT_SCHEMA

    def test_list_shapes_are_not_wrapped(self):
        schema = [["name", "What is the name?"]]
        assert _ai_complete_format(schema) == schema

    def test_extraction_spec_precomputes_both_engine_formats(self):
        options = _build(
            {"SCHEMA": PROPERTIES_SCHEMA, "EXTRACTION_ENGINE": "ai_complete"}
        )
        assert options.extraction.ai_extract_format == PROPERTIES_SCHEMA
        assert options.extraction.ai_complete_format == {
            "type": "json",
            "schema": PROPERTIES_SCHEMA,
        }


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
        ],
    )
    def test_field_colliding_with_a_reserved_column_raises_1117(self, reserved_name):
        error = _expect_error("1117", {"SCHEMA": {reserved_name: "a question"}})
        assert reserved_name.upper() in error.message

    def test_field_colliding_with_the_corrupt_record_column_raises_1117(self):
        _expect_error(
            "1117",
            {
                "SCHEMA": {"my_error": "a question"},
                "COLUMNNAMEOFCORRUPTRECORD": "MY_ERROR",
            },
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
# Options that are simply inapplicable given another option's value are
# silently unused, not guarded -- the same convention dataframe_reader.py
# already uses elsewhere (e.g. "predicates will be ignored if column is
# specified", "stream ... time travel options will be ignored"), and the same
# convention row_boundary/page_filter already follow under parse_mode="none".
# ---------------------------------------------------------------------------


class TestInapplicableOptionsAreSilentlyUnused:
    @pytest.mark.parametrize("parse_mode", ["ocr", "text", "none"])
    def test_extract_images_outside_layout_is_not_an_error(self, parse_mode):
        # AI_PARSE_DOCUMENT requires mode=LAYOUT for extract_images; outside
        # layout the option is either never sent (parse_mode="text"/"none") or
        # forwarded and left for AI_PARSE_DOCUMENT itself to reject per-row.
        options = _build({"EXTRACT_IMAGES": True, "PARSE_MODE": parse_mode})
        assert options.extract_images is True

    def test_extract_images_under_layout_passes(self):
        options = _build({"EXTRACT_IMAGES": True, "PARSE_MODE": "layout"})
        assert options.extract_images is True

    @pytest.mark.parametrize(
        "key,attribute", [("MODEL", "model"), ("PROMPT", "prompt")]
    )
    def test_model_or_prompt_with_ai_extract_is_not_an_error(self, key, attribute):
        # ai_extract has no model/prompt parameters -- the value is stored but
        # extract_with_ai_extract never reads it.
        options = _build(
            {
                key: "some-value",
                "SCHEMA": FLAT_SCHEMA,
                "EXTRACTION_ENGINE": "ai_extract",
            }
        )
        assert getattr(options, attribute) == "some-value"
        assert options.extraction_engine == "ai_extract"

    @pytest.mark.parametrize(
        "key,attribute", [("MODEL", "model"), ("PROMPT", "prompt")]
    )
    def test_model_or_prompt_without_schema_is_not_an_error(self, key, attribute):
        # Extract never runs without schema, so model/prompt are stored but unused.
        options = _build({key: "some-value", "EXTRACTION_ENGINE": "ai_complete"})
        assert getattr(options, attribute) == "some-value"
        assert options.extract_enabled is False

    def test_model_and_prompt_with_ai_complete_and_schema_are_used(self):
        options = _build(
            {
                "MODEL": "claude-4-sonnet",
                "PROMPT": "Extract the invoice fields.",
                "EXTRACTION_ENGINE": "ai_complete",
                "SCHEMA": FLAT_SCHEMA,
            }
        )
        assert options.model == "claude-4-sonnet"
        assert options.prompt == "Extract the invoice fields."
        assert options.extract_enabled is True


# ---------------------------------------------------------------------------
# ai_complete no longer requires a parse phase: AI_COMPLETE's file variant
# extracts directly from SOURCE_FILE via named-argument file + response_format,
# the same way ai_extract already does -- see extract_with_ai_complete.
# ---------------------------------------------------------------------------


class TestAiCompleteWorksWithOrWithoutParse:
    def test_ai_complete_with_parse_mode_none_is_not_an_error(self):
        options = _build(
            {
                "EXTRACTION_ENGINE": "ai_complete",
                "PARSE_MODE": "none",
                "SCHEMA": FLAT_SCHEMA,
            }
        )
        assert options.extraction_engine == "ai_complete"
        assert options.parse_enabled is False
        assert options.extract_enabled is True

    @pytest.mark.parametrize("parse_mode", ["layout", "ocr", "text"])
    def test_ai_complete_with_a_parse_phase_passes(self, parse_mode):
        options = _build(
            {
                "EXTRACTION_ENGINE": "ai_complete",
                "PARSE_MODE": parse_mode,
                "SCHEMA": FLAT_SCHEMA,
            }
        )
        assert options.extraction_engine == "ai_complete"
        assert options.parse_enabled is True

    def test_ai_extract_with_parse_mode_none_passes(self):
        # ai_extract accepts a FILE input directly, so skipping parse is fine.
        options = _build({"PARSE_MODE": "none", "SCHEMA": FLAT_SCHEMA})
        assert options.parse_enabled is False
        assert options.extract_enabled is True


# ---------------------------------------------------------------------------
# Documented non-guards
# ---------------------------------------------------------------------------


class TestDocumentedNonGuards:
    def test_page_row_boundary_with_parse_mode_none_is_a_no_op(self):
        # Parse is skipped entirely when parse_mode == "none", so row_boundary
        # is simply unused -- explicitly not an error.
        options = _build({"ROW_BOUNDARY": "page", "PARSE_MODE": "none"})
        assert options.row_boundary == "page"
        assert options.page_rows is False

    def test_page_row_boundary_with_a_parse_phase_enables_page_rows(self):
        options = _build({"ROW_BOUNDARY": "page", "PARSE_MODE": "layout"})
        assert options.page_rows is True

    def test_page_filter_with_parse_mode_none_is_not_an_error(self):
        # page_filter only ever applies during Parse (ai_parse_document's page_filter
        # kwarg, or the PDF UDTF's page_filter_json arg); with Parse skipped there is
        # no page-range concept to apply it to, so -- like row_boundary above -- this
        # is a no-op, not a guarded combination.
        options = _build(
            {"PAGE_FILTER": [{"start": 0, "end": 1}], "PARSE_MODE": "none"}
        )
        assert options.parse_enabled is False
