#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json

import pytest

from snowflake.snowpark._internal.document_reader import (
    cast_extracted_field,
    merge_extract_error,
)
from snowflake.snowpark._internal.document_reader_options import ExtractionSpec
from snowflake.snowpark.exceptions import (
    SnowparkDataframeReaderException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import col, try_parse_json
from snowflake.snowpark.types import BooleanType, DoubleType, StringType, VariantType
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

# AI_EXTRACT's JSON-Schema validator only accepts "string" and "array" (of strings)
# at the property level -- "number"/"integer"/"boolean" are rejected outright with
# "Error parsing JSON Schema: Incorrect 2nd-level type", confirmed live. AI_COMPLETE's
# response_format has no such restriction. So the scalar-type-casting behavior can
# only be exercised end-to-end via ai_complete; the array (no-cast/VARIANT) case is
# exercised via both engines below.
TYPED_SCALAR_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string"},
        "total_amount_number": {"type": "number"},
        "is_invoice": {"type": "boolean"},
    },
}

ARRAY_FIELD_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string"},
        "line_item_descriptions": {"type": "array", "items": {"type": "string"}},
    },
}

FLAT_NL_SCHEMA = {"invoice_number": "What is the invoice number?"}


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


def test_extract_images_with_page_filter_aggregates_back_to_one_row(session, doc_path):
    # page_filter forces the page-array explode/aggregate path even under a
    # document row_boundary; extract_images has to survive that same aggregation.
    df = (
        session.read.option("parse_mode", "layout")
        .option("extract_images", True)
        .option("page_filter", [{"start": 0, "end": 2}])
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


def _datatype(df, column_name: str):
    return next(
        f.datatype for f in df.schema.fields if f.name.strip('"') == column_name
    )


def test_ai_extract_array_field_stays_variant_not_stringified(session, invoice_path):
    # "array" has no entry in the scalar-type map, on purpose: AI_EXTRACT already
    # returns it as a real ARRAY, and forcing it to a string would just
    # re-serialize the structure the schema asked for.
    df = (
        session.read.option("parse_mode", "layout")
        .option("schema", ARRAY_FIELD_SCHEMA)
        ._documents(invoice_path)
    )
    assert isinstance(_datatype(df, "INVOICE_NUMBER"), StringType)
    assert isinstance(_datatype(df, "LINE_ITEM_DESCRIPTIONS"), VariantType)

    row = df.collect()[0]
    assert row["INVOICE_NUMBER"]
    # A VARIANT column round-trips through collect() as its JSON text -- the point
    # isn't the Python-side representation, it's that the SQL-level type is a real
    # ARRAY/VARIANT the caller can flatten() themselves, not a StringType column
    # that was already collapsed before they ever saw it.
    assert json.loads(row["LINE_ITEM_DESCRIPTIONS"])
    assert row["_document_error"] is None


def test_ai_complete_scalar_types_are_cast_not_stringified(session, invoice_path):
    df = (
        session.read.option("parse_mode", "layout")
        .option("extraction_engine", "ai_complete")
        .option("schema", TYPED_SCALAR_SCHEMA)
        ._documents(invoice_path)
    )
    assert isinstance(_datatype(df, "INVOICE_NUMBER"), StringType)
    assert isinstance(_datatype(df, "TOTAL_AMOUNT_NUMBER"), DoubleType)
    assert isinstance(_datatype(df, "IS_INVOICE"), BooleanType)

    row = df.collect()[0]
    assert row["INVOICE_NUMBER"]
    # These land as real Python float/bool, not "34.49"/"true" strings -- proof the
    # normalization contract holds end to end, not just at the declared-dtype level.
    assert isinstance(row["TOTAL_AMOUNT_NUMBER"], float)
    assert row["TOTAL_AMOUNT_NUMBER"] > 0
    assert isinstance(row["IS_INVOICE"], bool)
    assert row["IS_INVOICE"] is True
    assert row["_document_error"] is None


def test_ai_complete_array_field_stays_variant_not_stringified(session, invoice_path):
    df = (
        session.read.option("parse_mode", "layout")
        .option("extraction_engine", "ai_complete")
        .option("schema", ARRAY_FIELD_SCHEMA)
        ._documents(invoice_path)
    )
    assert isinstance(_datatype(df, "LINE_ITEM_DESCRIPTIONS"), VariantType)
    row = df.collect()[0]
    assert json.loads(row["LINE_ITEM_DESCRIPTIONS"])
    assert row["_document_error"] is None


def test_natural_language_schema_field_stays_variant_not_stringified(
    session, invoice_path
):
    # A flat {name: prompt} response_format carries no type contract at all -- the
    # field's actual shape could vary row to row, so there is nothing safe to cast
    # to. The output column must stay the raw VARIANT AI_EXTRACT returned rather
    # than being forced into StringType the way every field used to be.
    df = (
        session.read.option("parse_mode", "layout")
        .option("schema", FLAT_NL_SCHEMA)
        ._documents(invoice_path)
    )
    assert isinstance(_datatype(df, "INVOICE_NUMBER"), VariantType)
    row = df.collect()[0]
    assert json.loads(row["INVOICE_NUMBER"])
    assert row["_document_error"] is None


def test_ai_extract_rejects_non_string_scalar_property_types(session, invoice_path):
    # Documents today's actual AI_EXTRACT limitation (confirmed live, not assumed):
    # its JSON-Schema validator only accepts "string"/"array" at the property level.
    # "number"/"integer"/"boolean" fail the call itself with an in-band extract
    # error -- the existing PERMISSIVE error machinery, not a new failure mode this
    # normalization introduces.
    rows = (
        session.read.option("parse_mode", "layout")
        .option("schema", TYPED_SCALAR_SCHEMA)
        ._documents(invoice_path)
        .collect()
    )
    assert len(rows) == 1
    assert rows[0]["_document_error"] is not None
    assert "Incorrect 2nd-level type" in rows[0]["_document_error"]


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


def test_ai_complete_extraction_with_row_boundary_page(session, doc_path):
    # ai_complete chained after ai_parse_document is materialized (collected and
    # rebuilt) before ai_complete runs; row_boundary="page" means that
    # materialization step also has to carry PAGE_INDEX through.
    rows = (
        session.read.option("parse_mode", "layout")
        .option("row_boundary", "page")
        .option("extraction_engine", "ai_complete")
        .option("schema", INVOICE_SCHEMA)
        ._documents(doc_path)
        .collect()
    )

    assert len(rows) == DOC_PAGE_COUNT
    assert {row["PAGE_INDEX"] for row in rows} == set(range(DOC_PAGE_COUNT))
    assert all(row["_document_error"] is None for row in rows)


def test_ai_complete_extraction_with_extract_images(session, invoice_path):
    # Same materialization step as above, this time carrying IMAGES (a VARIANT,
    # collected back as a JSON string) through the collect()-and-rebuild.
    df = (
        session.read.option("parse_mode", "layout")
        .option("extract_images", True)
        .option("extraction_engine", "ai_complete")
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
    )

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "TOTAL_PAGES",
        "CONTENT",
        "IMAGES",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT",
        "_document_error",
    ]
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["INVOICE_NUMBER"]
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
    # _document_error is an array, one entry per phase that failed -- not a single
    # struct -- so that a row failing in both Parse and Extract doesn't lose one.
    errors = json.loads(rows[0]["_document_error"])
    assert len(errors) == 1
    assert errors[0]["stage"] == "parse"
    assert errors[0]["message"]


def test_permissive_uses_a_custom_corrupt_record_column(session, unsupported_stage):
    df = (
        session.read.option("parse_mode", "text")
        .option("columnNameOfCorruptRecord", "docError")
        ._documents(f"@{unsupported_stage}")
    )

    assert _unquoted(df)[-1] == "docError"
    rows = df.collect()
    assert len(rows) == 1
    # docError is an array, one entry per phase that failed -- see
    # test_permissive_captures_a_parse_error_without_aborting.
    errors = json.loads(rows[0]["docError"])
    assert len(errors) == 1
    assert errors[0]["stage"] == "parse"


def test_permissive_leaves_the_error_column_null_for_a_good_file(session, invoice_path):
    rows = session.read.option("parse_mode", "text")._documents(invoice_path).collect()

    assert len(rows) == 1
    assert rows[0]["_document_error"] is None
    assert rows[0]["CONTENT"]


def test_extract_type_mismatch_is_surfaced_not_silently_dropped(session):
    # AI_EXTRACT/AI_COMPLETE's own schema validation normally catches a genuine
    # type mismatch before it ever reaches this reader's TRY_CAST layer (verified
    # separately, live: AI_COMPLETE reports its own "json mode output validation
    # error" for a field that doesn't fit its declared type). That still leaves a
    # defense-in-depth question this test targets directly: if a value ever does
    # get past that validation not matching its declared type, does TRY_CAST's
    # NULL-on-mismatch stay silent, or does the reader surface it? Provoking a
    # real mismatch out of the live model isn't reliably reproducible (it may
    # just comply), so this drives the reader's own cast_extracted_field()/
    # merge_extract_error() directly against a synthetic AI_COMPLETE-shaped
    # response instead -- real SQL execution (TRY_CAST, COALESCE, array
    # functions), just not a real model call.
    synthetic_response = (
        '{"value": {"invoice_number": "INV-1", "amount": "not-a-number", '
        '"is_paid": true}, "error": null}'
    )
    df = session.create_dataframe([[synthetic_response]], schema=["RAW"]).with_column(
        "EXTRACTED", try_parse_json(col("RAW"))
    )
    spec = ExtractionSpec(
        fields=["invoice_number", "amount", "is_paid"],
        field_columns=["INVOICE_NUMBER", "AMOUNT", "IS_PAID"],
        ai_extract_format=None,
        ai_complete_format=None,
        field_types={
            "invoice_number": StringType(),
            "amount": DoubleType(),
            "is_paid": BooleanType(),
        },
    )
    extracted = col("EXTRACTED")
    value = extracted["value"]
    cast_results = [cast_extracted_field(value[f], f, spec) for f in spec.fields]
    names = spec.field_columns + ["_DOC_EXTRACT_ERROR"]
    values = [column for column, _ in cast_results] + [
        merge_extract_error(
            extracted["error"], spec.field_columns, [m for _, m in cast_results]
        )
    ]
    row = df.with_columns(names, values).collect()[0]

    # The two fields that do fit their declared type are unaffected.
    assert row["INVOICE_NUMBER"] == "INV-1"
    assert row["IS_PAID"] is True
    # The one that doesn't degrades to NULL rather than aborting the whole row...
    assert row["AMOUNT"] is None
    # ...but isn't silently dropped: it's named in the error column, the same one
    # AI_EXTRACT/AI_COMPLETE's own in-band error already populates.
    assert (
        row["_DOC_EXTRACT_ERROR"]
        == "Field(s) did not match their declared type: AMOUNT"
    )


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
