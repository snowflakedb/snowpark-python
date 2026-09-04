#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json

import pytest

from snowflake.snowpark.exceptions import (
    SnowparkDataframeReaderException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import col
from tests.utils import TestFiles, Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="AI functions are not yet supported in local testing mode.",
    ),
]

# doc.pdf is 3 pages, invoice.pdf is 1 page. The page count matters for the
# row_boundary="page" assertions below.
DOC_PAGE_COUNT = 3

INVOICE_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string"},
        "total_amount": {"type": "string"},
    },
}


def _unquoted(df) -> list:
    return [c.strip('"') for c in df.columns]


@pytest.fixture(scope="module")
def doc_stage(session, resources_path):
    """A stage holding the two PDF fixtures. SNOWFLAKE_SSE encryption is required
    for the FILE-typed SOURCE_FILE column to be readable by the AI functions."""
    stage_name = Utils.random_stage_name()
    session.sql(
        f"CREATE OR REPLACE TEMP STAGE {stage_name} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
    ).collect()
    test_files = TestFiles(resources_path)
    session.file.put(test_files.test_doc_pdf, f"@{stage_name}", auto_compress=False)
    session.file.put(test_files.test_invoice_pdf, f"@{stage_name}", auto_compress=False)
    yield stage_name
    Utils.drop_stage(session, stage_name)


@pytest.fixture(scope="module")
def unsupported_stage(session, resources_path):
    """A stage holding a file no document parser can read, for the
    PERMISSIVE/FAILFAST error paths. Kept separate so the PDF tests can read
    their whole stage without tripping over it."""
    stage_name = Utils.random_stage_name()
    session.sql(
        f"CREATE OR REPLACE TEMP STAGE {stage_name} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
    ).collect()
    session.file.put(
        TestFiles(resources_path).test_audio_ogg, f"@{stage_name}", auto_compress=False
    )
    yield stage_name
    Utils.drop_stage(session, stage_name)


@pytest.fixture
def invoice_path(doc_stage):
    return f"@{doc_stage}/invoice.pdf"


@pytest.fixture
def doc_path(doc_stage):
    return f"@{doc_stage}/doc.pdf"


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------


def test_documents_default_options(session, invoice_path):
    df = session.read._documents(invoice_path)

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "TOTAL_PAGES",
        "CONTENT",
        "_document_error",
    ]

    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["SOURCE_FILE"] is not None
    assert rows[0]["TOTAL_PAGES"] == 1
    assert rows[0]["CONTENT"]
    assert rows[0]["_document_error"] is None


@pytest.mark.parametrize("parse_mode", ["layout", "ocr", "text"])
def test_parse_mode_variants_yield_one_row_per_document(
    session, invoice_path, parse_mode
):
    df = session.read.option("parse_mode", parse_mode)._documents(invoice_path)
    rows = df.collect()

    assert len(rows) == 1
    assert rows[0]["CONTENT"]
    assert rows[0]["TOTAL_PAGES"] == 1
    assert rows[0]["_document_error"] is None


def test_parse_mode_none_without_schema_projects_only_the_identity_column(
    session, invoice_path
):
    # Nothing can fail when neither Parse nor Extract runs, so no error column
    # is added either.
    df = session.read.option("parse_mode", "none")._documents(invoice_path)

    assert _unquoted(df) == ["SOURCE_FILE"]
    assert len(df.collect()) == 1


def test_row_boundary_page_yields_one_row_per_page(session, doc_path):
    document_rows = (
        session.read.option("parse_mode", "layout")._documents(doc_path).collect()
    )
    assert len(document_rows) == 1

    page_df = (
        session.read.option("parse_mode", "layout")
        .option("row_boundary", "page")
        ._documents(doc_path)
    )
    assert _unquoted(page_df) == [
        "SOURCE_FILE",
        "PAGE_INDEX",
        "TOTAL_PAGES",
        "CONTENT",
        "_document_error",
    ]

    page_rows = page_df.order_by(col("PAGE_INDEX")).collect()
    assert len(page_rows) == DOC_PAGE_COUNT
    assert [row["PAGE_INDEX"] for row in page_rows] == list(range(DOC_PAGE_COUNT))
    for row in page_rows:
        assert row["TOTAL_PAGES"] == DOC_PAGE_COUNT
        assert row["CONTENT"]
        assert row["_document_error"] is None


def test_row_boundary_page_in_text_mode_yields_one_row_per_page(session, doc_path):
    page_rows = (
        session.read.option("parse_mode", "text")
        .option("row_boundary", "page")
        ._documents(doc_path)
        .order_by(col("PAGE_INDEX"))
        .collect()
    )

    assert len(page_rows) == DOC_PAGE_COUNT
    assert [row["PAGE_INDEX"] for row in page_rows] == list(range(DOC_PAGE_COUNT))
    assert all(row["TOTAL_PAGES"] == DOC_PAGE_COUNT for row in page_rows)


def test_row_boundary_document_concatenates_every_page(session, doc_path):
    document_row = (
        session.read.option("parse_mode", "text")._documents(doc_path).collect()[0]
    )
    page_rows = (
        session.read.option("parse_mode", "text")
        .option("row_boundary", "page")
        ._documents(doc_path)
        .collect()
    )

    for page_row in page_rows:
        assert page_row["CONTENT"].strip() in document_row["CONTENT"]


@pytest.mark.parametrize("parse_mode", ["layout", "ocr", "text"])
def test_page_filter_narrows_the_pages_read(session, doc_path, parse_mode):
    page_rows = (
        session.read.option("parse_mode", parse_mode)
        .option("row_boundary", "page")
        .option("page_filter", [{"start": 0, "end": 2}])
        ._documents(doc_path)
        .order_by(col("PAGE_INDEX"))
        .collect()
    )

    assert len(page_rows) == 2
    assert [row["PAGE_INDEX"] for row in page_rows] == [0, 1]


def test_page_filter_with_document_boundary_aggregates_back_to_one_row(
    session, doc_path
):
    # page_filter implies page_split server-side, so the document-boundary path
    # has to collapse the page array back into a single row.
    rows = (
        session.read.option("parse_mode", "layout")
        .option("page_filter", [{"start": 0, "end": 2}])
        ._documents(doc_path)
        .collect()
    )

    assert len(rows) == 1
    assert rows[0]["CONTENT"]
    assert rows[0]["_document_error"] is None


def test_extract_images_under_layout(session, doc_path):
    df = (
        session.read.option("parse_mode", "layout")
        .option("extract_images", True)
        ._documents(doc_path)
    )

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "TOTAL_PAGES",
        "CONTENT",
        "IMAGES",
        "_document_error",
    ]
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["_document_error"] is None


def test_multiple_files_produce_one_row_each(session, doc_stage):
    rows = (
        session.read.option("parse_mode", "layout")
        ._documents(f"@{doc_stage}")
        .collect()
    )

    assert len(rows) == 2
    assert all(row["CONTENT"] for row in rows)
    assert {row["TOTAL_PAGES"] for row in rows} == {1, DOC_PAGE_COUNT}


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------


def test_ai_extract_from_parsed_content(session, invoice_path):
    df = (
        session.read.option("parse_mode", "layout")
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
    )

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "TOTAL_PAGES",
        "CONTENT",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT",
        "EXTRACTION_SCORES",
        "_document_error",
    ]

    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["CONTENT"]
    # Asserting the fields actually contain extracted values, not just that the
    # call didn't error -- a wrong response_format shape or an unwrapped
    # return_error_details payload could otherwise leave these silently null
    # while still passing a weaker "no error" check.
    assert rows[0]["INVOICE_NUMBER"]
    assert rows[0]["TOTAL_AMOUNT"]
    assert rows[0]["EXTRACTION_SCORES"] is not None
    assert rows[0]["_document_error"] is None


def test_ai_extract_from_the_source_file_without_parsing(session, invoice_path):
    # ai_extract accepts a FILE input, so parse_mode="none" feeds SOURCE_FILE
    # straight into extraction and no CONTENT/TOTAL_PAGES columns exist.
    df = (
        session.read.option("parse_mode", "none")
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
    )

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT",
        "EXTRACTION_SCORES",
        "_document_error",
    ]

    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["INVOICE_NUMBER"]
    assert rows[0]["TOTAL_AMOUNT"]
    assert rows[0]["EXTRACTION_SCORES"] is not None
    assert rows[0]["_document_error"] is None


def test_ai_extract_per_page(session, doc_path):
    rows = (
        session.read.option("parse_mode", "layout")
        .option("row_boundary", "page")
        .option("schema", INVOICE_SCHEMA)
        ._documents(doc_path)
        .collect()
    )

    assert len(rows) == DOC_PAGE_COUNT
    assert all(row["EXTRACTION_SCORES"] is not None for row in rows)
    # doc.pdf isn't an invoice, so the fields themselves may legitimately come
    # back empty per page -- CONTENT is the one value every page must have.
    assert all(row["CONTENT"] for row in rows)


def test_ai_complete_extraction_emits_no_scores_column(session, invoice_path):
    df = (
        session.read.option("parse_mode", "layout")
        .option("extraction_engine", "ai_complete")
        .option("model", "claude-4-sonnet")
        .option("prompt", "Extract the invoice number and the total amount due.")
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
    )

    # AI_COMPLETE has no scoring output, so EXTRACTION_SCORES is ai_extract-only.
    assert _unquoted(df) == [
        "SOURCE_FILE",
        "TOTAL_PAGES",
        "CONTENT",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT",
        "_document_error",
    ]

    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["INVOICE_NUMBER"]
    assert rows[0]["TOTAL_AMOUNT"]
    assert rows[0]["_document_error"] is None


def test_ai_complete_extraction_uses_a_default_model(session, invoice_path):
    rows = (
        session.read.option("parse_mode", "layout")
        .option("extraction_engine", "ai_complete")
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
        .collect()
    )

    assert len(rows) == 1
    assert rows[0]["INVOICE_NUMBER"]
    assert rows[0]["TOTAL_AMOUNT"]
    assert rows[0]["_document_error"] is None


def test_ai_complete_extracts_directly_from_the_file_with_no_parse_phase(
    session, invoice_path
):
    # AI_COMPLETE's file variant supports file + response_format together via
    # named-argument SQL, the same way ai_extract already does -- so, unlike
    # an earlier design of this reader assumed, ai_complete does not require a
    # Parse phase to have run first.
    df = (
        session.read.option("parse_mode", "none")
        .option("extraction_engine", "ai_complete")
        .option("model", "claude-4-sonnet")
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
    )

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT",
        "_document_error",
    ]
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["INVOICE_NUMBER"]
    assert rows[0]["TOTAL_AMOUNT"]
    assert rows[0]["_document_error"] is None


# ---------------------------------------------------------------------------
# PERMISSIVE / FAILFAST
# ---------------------------------------------------------------------------


def test_permissive_captures_a_parse_error_without_aborting(session, unsupported_stage):
    # parse_mode="text" routes through our own PDF UDTF, which fails
    # deterministically on a non-PDF instead of depending on server-side
    # format support.
    rows = (
        session.read.option("parse_mode", "text")
        ._documents(f"@{unsupported_stage}")
        .collect()
    )

    assert len(rows) == 1
    error = json.loads(rows[0]["_document_error"])
    assert error["stage"] == "parse"
    assert error["message"]


def test_permissive_uses_a_custom_corrupt_record_column(session, unsupported_stage):
    df = (
        session.read.option("parse_mode", "text")
        .option("columnNameOfCorruptRecord", "docError")
        ._documents(f"@{unsupported_stage}")
    )

    assert _unquoted(df)[-1] == "docError"
    rows = df.collect()
    assert len(rows) == 1
    assert json.loads(rows[0]["docError"])["stage"] == "parse"


def test_permissive_leaves_the_error_column_null_for_a_good_file(session, invoice_path):
    rows = session.read.option("parse_mode", "text")._documents(invoice_path).collect()

    assert len(rows) == 1
    assert rows[0]["_document_error"] is None
    assert rows[0]["CONTENT"]


def test_failfast_raises_on_an_unreadable_file(session, unsupported_stage):
    df = (
        session.read.option("parse_mode", "text")
        .option("mode", "FAILFAST")
        ._documents(f"@{unsupported_stage}")
    )

    with pytest.raises(SnowparkSQLException):
        df.collect()


def test_failfast_omits_the_error_column_and_still_returns_content(
    session, invoice_path
):
    df = (
        session.read.option("parse_mode", "layout")
        .option("mode", "FAILFAST")
        ._documents(invoice_path)
    )

    assert _unquoted(df) == ["SOURCE_FILE", "TOTAL_PAGES", "CONTENT"]
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["CONTENT"]


def test_failfast_with_extraction_only(session, invoice_path):
    # With no Parse phase the FAILFAST guard has to anchor on the first schema
    # field instead of CONTENT.
    df = (
        session.read.option("parse_mode", "none")
        .option("mode", "FAILFAST")
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
    )

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT",
        "EXTRACTION_SCORES",
    ]
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["INVOICE_NUMBER"]
    assert rows[0]["TOTAL_AMOUNT"]


# ---------------------------------------------------------------------------
# Option validation through the public .option() surface
#
# `DocumentReaderOptions.from_reader_options` is unit-tested directly in
# tests/unit/test_document_reader_options.py; these cases additionally prove
# that the keys `.option()` stores line up with the keys it reads.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "options,error_code",
    [
        pytest.param({"parse_mode": "bogus"}, "1116", id="parse_mode"),
        pytest.param({"row_boundary": "chapter"}, "1116", id="row_boundary"),
        pytest.param({"mode": "DROPMALFORMED"}, "1116", id="mode"),
        pytest.param({"extraction_engine": "ai_agent"}, "1116", id="engine"),
    ],
)
def test_invalid_options_raise_before_any_query(
    session, invoice_path, options, error_code
):
    # parse_mode, row_boundary, mode and extraction_engine are the only
    # client-side guard left (code 1116) -- they drive our own Python routing
    # and are never sent to Snowflake, so nothing downstream would catch an
    # unrecognized value. Everything else (schema, model, prompt,
    # extract_images) is either forwarded to AI_EXTRACT/AI_COMPLETE untouched
    # or silently unused when another option makes it inapplicable; see
    # test_previously_guarded_combinations_no_longer_raise below.
    reader = session.read
    for key, value in options.items():
        reader = reader.option(key, value)

    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        reader._documents(invoice_path)
    assert exc_info.value.error_code == error_code


@pytest.mark.parametrize(
    "options",
    [
        pytest.param(
            {"extract_images": True, "parse_mode": "ocr"},
            id="extract_images_outside_layout",
        ),
        pytest.param(
            {"model": "claude-4-sonnet", "schema": INVOICE_SCHEMA},
            id="model_with_ai_extract",
        ),
        pytest.param(
            {"prompt": "Extract fields.", "schema": INVOICE_SCHEMA},
            id="prompt_with_ai_extract",
        ),
        pytest.param(
            {"model": "claude-4-sonnet", "extraction_engine": "ai_complete"},
            id="model_without_schema",
        ),
        pytest.param({"schema": {"properties": {}}}, id="schema_with_empty_properties"),
        pytest.param({"schema": ["invoice_number"]}, id="schema_as_a_bare_list"),
        pytest.param(
            {"extraction_engine": "ai_complete", "parse_mode": "none"},
            id="ai_complete_without_parse",
        ),
    ],
)
def test_previously_guarded_combinations_no_longer_raise(
    session, invoice_path, options
):
    # These combinations used to be client-side errors; they are now either
    # silently-unused option values or genuinely valid (ai_complete's file
    # variant). Building the reader must not raise -- correctness of the
    # resulting read (where one exists) is covered by the dedicated tests
    # above/elsewhere in this file.
    reader = session.read
    for key, value in options.items():
        reader = reader.option(key, value)
    reader._documents(invoice_path)


def test_page_row_boundary_with_parse_mode_none_is_accepted(session, invoice_path):
    # Documented no-op rather than an error: Parse is skipped, so row_boundary
    # has nothing to act on.
    df = (
        session.read.option("parse_mode", "none")
        .option("row_boundary", "page")
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
    )

    assert "PAGE_INDEX" not in _unquoted(df)
    assert len(df.collect()) == 1


def test_non_stage_path_raises(session):
    with pytest.raises(ValueError, match="invalid Snowflake stage location"):
        session.read._documents("/local/path/doc.pdf")
