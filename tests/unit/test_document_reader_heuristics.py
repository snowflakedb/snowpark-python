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

    def test_explicit_parse_mode_wins(self):
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
