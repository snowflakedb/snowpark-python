#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.document_reader_heuristics import (
    EXTRACT_SCANNED_SCALE,
    apply_document_heuristics,
    infer_row_boundary,
    looks_generative,
    needs_images,
    needs_structure,
    plan_extract_scale,
    plan_extraction_engine,
    plan_parse_mode,
    question_for_field,
    rewrite_response_format,
    skip_parse_for_extract,
    smart_enabled,
)
from snowflake.snowpark._internal.document_reader_options import DocumentReaderOptions

INVOICE_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string"},
        "total_amount": {"type": "string"},
    },
}

TABLE_SCHEMA = {
    "type": "object",
    "properties": {
        "line_items": {
            "type": "object",
            "properties": {
                "item_code": {"type": "array"},
                "quantity": {"type": "array"},
            },
        }
    },
}

SUMMARY_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string", "description": "2-3 sentence summary"},
    },
}


class TestSmartFlag:
    def test_default_on(self):
        assert smart_enabled({}) is True

    def test_opt_out(self):
        assert smart_enabled({"SMART": False}) is False
        assert smart_enabled({"SMART": "false"}) is False


class TestSchemaSignals:
    def test_invoice_is_document_scoped_extract(self):
        assert infer_row_boundary(INVOICE_SCHEMA) == "document"
        assert skip_parse_for_extract(INVOICE_SCHEMA) is True
        assert looks_generative(INVOICE_SCHEMA) is False
        assert plan_extraction_engine(INVOICE_SCHEMA) == "ai_extract"

    def test_table_schema_uses_page_rows(self):
        assert infer_row_boundary(TABLE_SCHEMA) == "page"
        assert skip_parse_for_extract(TABLE_SCHEMA) is False
        assert (
            needs_structure(TABLE_SCHEMA) is False
        )  # field is line_items, not "table"

    def test_structure_tokens(self):
        schema = {
            "type": "object",
            "properties": {"tables_on_page": {"type": "string"}},
        }
        assert needs_structure(schema) is True
        assert infer_row_boundary(schema) == "page"

    def test_summary_uses_complete_and_must_parse(self):
        assert looks_generative(SUMMARY_SCHEMA) is True
        assert plan_extraction_engine(SUMMARY_SCHEMA) == "ai_complete"
        assert skip_parse_for_extract(SUMMARY_SCHEMA) is False

    def test_signature_wants_images(self):
        schema = {
            "type": "object",
            "properties": {"is_signed": {"description": "Is the signature present?"}},
        }
        assert needs_images(schema) is True
        assert skip_parse_for_extract(schema) is False


class TestExtractImagesIsNeverInferred:
    """The planner must not switch extract_images on by itself.

    It only adds an IMAGES output column; extract() feeds the engine CONTENT or
    SOURCE_FILE and never IMAGES, so inferring it cannot improve an extraction and
    can only add AI_PARSE_DOCUMENT cost. It fired on 206 of 252 short-split
    documents before this, because needs_images() substring-matches description
    prose -- "document creation timestamp" contains "stamp".
    """

    SIGNATURE_SCHEMA = {
        "type": "object",
        "properties": {"is_signed": {"description": "Is the signature present?"}},
    }
    # The real false positive from the corpus: dd1155's own description.
    TIMESTAMP_SCHEMA = {
        "type": "object",
        "properties": {
            "created_on": {"description": "document creation timestamp ('created on:')"}
        },
    }

    def test_signature_schema_does_not_turn_images_on(self):
        options = apply_document_heuristics(
            DocumentReaderOptions(), explicit={"SCHEMA"}, schema=self.SIGNATURE_SCHEMA
        )
        assert needs_images(self.SIGNATURE_SCHEMA) is True
        assert options.extract_images is False

    def test_timestamp_description_does_not_turn_images_on(self):
        assert needs_images(self.TIMESTAMP_SCHEMA) is True  # the substring collision
        options = apply_document_heuristics(
            DocumentReaderOptions(), explicit={"SCHEMA"}, schema=self.TIMESTAMP_SCHEMA
        )
        assert options.extract_images is False

    def test_caller_request_is_honoured(self):
        options = apply_document_heuristics(
            DocumentReaderOptions(extract_images=True),
            explicit={"SCHEMA", "EXTRACT_IMAGES"},
            schema=self.SIGNATURE_SCHEMA,
        )
        assert options.extract_images is True

    def test_parse_mode_still_reaches_layout_via_needs_images(self):
        # Unchanged on purpose: whether the page needs a structure-aware parse is a
        # different question from whether the caller wants images returned.
        options = apply_document_heuristics(
            DocumentReaderOptions(), explicit={"SCHEMA"}, schema=self.SIGNATURE_SCHEMA
        )
        assert options.parse_mode == "layout"
        assert options.extract_images is False


class TestQuestionsAndTables:
    def test_specific_date_question(self):
        assert question_for_field("date", {}) != "What is the date?"

    def test_vendor_name(self):
        q = question_for_field("vendor_name", {})
        assert "vendor name" in q.lower()

    def test_rewrite_adds_questions_and_table_ordering(self):
        rewritten = rewrite_response_format(TABLE_SCHEMA)
        table = rewritten["properties"]["line_items"]
        assert table["column_ordering"] == ["item_code", "quantity"]
        assert table["properties"]["item_code"]["description"] == "Item Code"
        invoice = rewrite_response_format(INVOICE_SCHEMA)
        assert invoice["properties"]["invoice_number"]["description"].endswith("?")


class TestParseMode:
    def test_extractive_schema_skips_parse(self):
        assert plan_parse_mode([("a.pdf", 10_000)], schema=INVOICE_SCHEMA) == "none"

    def test_scanned_pdf_uses_ocr_when_parse_needed(self):
        assert plan_parse_mode([("scan.pdf", 2_000_000)], schema=None) == "ocr"

    def test_small_pdf_without_schema_uses_layout(self):
        assert plan_parse_mode([("a.pdf", 20_000)]) == "layout"

    def test_images_use_ocr(self):
        assert plan_parse_mode([("page.png", 1000)]) == "ocr"

    def test_scale_factor(self):
        assert plan_extract_scale([("a.pdf", 1000)]) == 1.0
        assert plan_extract_scale([("a.pdf", 2_000_000)]) == EXTRACT_SCANNED_SCALE


class TestApplyHeuristicsRespectsExplicitOptions:
    def test_fills_parse_none_for_invoice_schema(self):
        options = DocumentReaderOptions.from_reader_options({"SCHEMA": INVOICE_SCHEMA})
        options = apply_document_heuristics(
            options, ["SCHEMA"], schema=INVOICE_SCHEMA, files=[("a.pdf", 10_000)]
        )
        assert options.parse_mode == "none"
        assert options.row_boundary == "document"
        assert options.extraction_engine == "ai_extract"
        assert options.extraction.ai_extract_format["schema"]["properties"][
            "invoice_number"
        ]["description"].endswith("?")

    CODED_BOX_SCHEMA = {
        "$defs": {
            "Entry": {
                "type": "object",
                "properties": {
                    "code": {"type": "string"},
                    "amount": {"type": "number"},
                },
            }
        },
        "type": "object",
        "properties": {
            "box_14": {
                "anyOf": [
                    {"type": "array", "items": {"$ref": "#/$defs/Entry"}},
                    {"type": "null"},
                ]
            },
            "flag": {
                "anyOf": [{"type": "boolean"}, {"type": "null"}],
            },
        },
    }

    def test_ref_wrapped_array_of_objects_is_not_extract_legal(self):
        """A ``$ref``/``anyOf`` to an object is the same shape as an inline one.

        AI_EXTRACT accepts such a call and then emits string items or nothing,
        so judging the two spellings differently silently routed whole document
        classes to an engine that cannot produce their rows -- K-1 coded boxes
        came back as ``"A -5,307."`` or null, and 990PF's 16-row transaction
        table came back entirely null.

        Compare ``test_explicit_array_of_objects_is_not_extract_legal``, which
        asserts this same verdict for the explicit spelling.
        """
        from snowflake.snowpark._internal.cortex_document_schema import is_extract_legal

        assert is_extract_legal(self.CODED_BOX_SCHEMA) is False
        assert plan_extraction_engine(self.CODED_BOX_SCHEMA) == "ai_complete"
        options = DocumentReaderOptions.from_reader_options(
            {"SCHEMA": self.CODED_BOX_SCHEMA}
        )
        options = apply_document_heuristics(
            options, ["SCHEMA"], schema=self.CODED_BOX_SCHEMA
        )
        assert options.extraction_engine == "ai_complete"

    def test_scalar_only_anyof_stays_extract_legal(self):
        """The fix must reject object-shaped items, not unions in general."""
        from snowflake.snowpark._internal.cortex_document_schema import is_extract_legal

        schema = {
            "type": "object",
            "properties": {
                "flag": {"anyOf": [{"type": "boolean"}, {"type": "null"}]},
                "note": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            },
        }
        assert is_extract_legal(schema) is True
        assert plan_extraction_engine(schema) == "ai_extract"

    def test_for_cortex_extract_keeps_refs_un_inlined(self):
        """The legality walk resolves references; the EXTRACT payload must not.

        Cortex rejects the inlined form ("Items in the array under key ... must
        accept type 'string'"), so ``for_cortex_extract`` stringifies scalar
        leaves and leaves ``$ref`` alone. That contract is independent of which
        engine the planner picks.
        """
        from snowflake.snowpark._internal.cortex_document_schema import (
            for_cortex_extract,
        )

        prepared = for_cortex_extract(self.CODED_BOX_SCHEMA)
        assert "$defs" in prepared
        assert prepared["properties"]["box_14"]["anyOf"][0]["items"] == {
            "$ref": "#/$defs/Entry"
        }
        assert prepared["$defs"]["Entry"]["properties"]["amount"]["type"] == "string"
        assert prepared["properties"]["flag"]["anyOf"][0]["type"] == "string"

    def test_explicit_array_of_objects_is_not_extract_legal(self):
        from snowflake.snowpark._internal.cortex_document_schema import is_extract_legal

        schema = {
            "type": "object",
            "properties": {
                "box_11": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"code": {"type": "string"}},
                    },
                }
            },
        }
        assert is_extract_legal(schema) is False
        assert plan_extraction_engine(schema) == "ai_complete"

    def test_extract_legal_invoice_rewrites_numbers_to_string(self):
        from snowflake.snowpark._internal.cortex_document_schema import is_extract_legal

        schema = {
            "type": "object",
            "properties": {
                "total": {"type": "number"},
                "invoice_number": {"type": "string"},
            },
        }
        assert is_extract_legal(schema) is True
        options = DocumentReaderOptions.from_reader_options({"SCHEMA": schema})
        options = apply_document_heuristics(options, ["SCHEMA"], schema=schema)
        assert options.extraction_engine == "ai_extract"
        assert (
            options.extraction.ai_extract_format["schema"]["properties"]["total"][
                "type"
            ]
            == "string"
        )

    def test_json_schema_type_union_is_extract_legal(self):
        from snowflake.snowpark._internal.cortex_document_schema import (
            for_cortex_extract,
            is_extract_legal,
        )

        schema = {
            "type": "object",
            "properties": {
                "total": {"type": ["number", "null"]},
                "ok": {"type": ["boolean", "null"]},
            },
        }
        assert is_extract_legal(schema) is True
        prepared = for_cortex_extract(schema)
        assert prepared["properties"]["total"]["type"] == ["string", "null"]
        assert prepared["properties"]["ok"]["type"] == ["string", "null"]
        options = DocumentReaderOptions.from_reader_options(
            {"SCHEMA": INVOICE_SCHEMA, "PARSE_MODE": "layout"}
        )
        options = apply_document_heuristics(
            options,
            ["SCHEMA", "PARSE_MODE"],
            schema=INVOICE_SCHEMA,
            files=[("a.pdf", 10_000)],
        )
        assert options.parse_mode == "layout"
