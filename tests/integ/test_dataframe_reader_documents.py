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
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.functions import col, try_parse_json
from snowflake.snowpark.types import BooleanType, DoubleType, StringType, VariantType
from tests.utils import TestFiles, Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="AI functions are not yet supported in local testing mode.",
    ),
]

# doc.pdf is 3 pages, invoice.pdf is 1 page -- used only to prove a multi-file
# read returns one row per file, each with its own page count.
DOC_PAGE_COUNT = 3

INVOICE_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string"},
        "total_amount": {"type": "string"},
    },
}

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

# A flat {name: question} dict carries no JSON-Schema type contract of its own; it is
# normalized into a string-typed JSON Schema so it can be sent to AI_COMPLETE at all
# (private preview only ever calls AI_COMPLETE -- see from_response_format).
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
    """A stage holding a file no document parser can read, for the PERMISSIVE
    error-column test. Kept separate so the PDF tests can read their whole
    stage without tripping over it."""
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


# ---------------------------------------------------------------------------
# Parse: no schema, no prompt -> AI_PARSE_DOCUMENT layout mode
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


def test_multiple_files_produce_one_row_each(session, doc_stage):
    rows = session.read._documents(f"@{doc_stage}").collect()

    assert len(rows) == 2
    assert all(row["CONTENT"] for row in rows)
    assert {row["TOTAL_PAGES"] for row in rows} == {1, DOC_PAGE_COUNT}


# ---------------------------------------------------------------------------
# Extract: schema and/or prompt -> AI_COMPLETE on the FILE, unparsed
# ---------------------------------------------------------------------------


def _datatype(df, column_name: str):
    return next(
        f.datatype for f in df.schema.fields if f.name.strip('"') == column_name
    )


def test_schema_scalar_types_are_cast_not_stringified(session, invoice_path):
    df = session.read.option("schema", TYPED_SCALAR_SCHEMA)._documents(invoice_path)

    # No CONTENT/TOTAL_PAGES (parse is skipped for a schema read) and no
    # EXTRACTION_SCORES (that's an AI_EXTRACT-only output, unreachable now that
    # schema reads always go through AI_COMPLETE).
    assert _unquoted(df) == [
        "SOURCE_FILE",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT_NUMBER",
        "IS_INVOICE",
        "_document_error",
    ]
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


def test_schema_array_field_stays_variant_not_stringified(session, invoice_path):
    df = session.read.option("schema", ARRAY_FIELD_SCHEMA)._documents(invoice_path)

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


def test_flat_schema_field_returns_a_string_column(session, invoice_path):
    # A flat {name: question} dict/list schema used to pass straight through to
    # AI_EXTRACT and come back as an untyped VARIANT. Now it's converted to a
    # string-typed JSON Schema so AI_COMPLETE (the only engine left) can use it.
    df = session.read.option("schema", FLAT_NL_SCHEMA)._documents(invoice_path)

    assert isinstance(_datatype(df, "INVOICE_NUMBER"), StringType)
    row = df.collect()[0]
    assert row["INVOICE_NUMBER"]
    assert row["_document_error"] is None


def test_prompt_only_read_returns_a_response_column(session, invoice_path):
    # No schema -> no response_format; AI_COMPLETE answers the prompt as free text
    # in a RESPONSE column instead of a per-field extraction.
    df = session.read.option(
        "prompt", "Summarize this document in one sentence."
    )._documents(invoice_path)

    assert _unquoted(df) == ["SOURCE_FILE", "RESPONSE", "_document_error"]
    row = df.collect()[0]
    assert row["RESPONSE"]
    assert row["_document_error"] is None


def test_schema_and_prompt_together(session, invoice_path):
    # schema drives response_format as usual; prompt replaces the default
    # extraction instruction that would otherwise be sent alongside it.
    df = (
        session.read.option(
            "prompt", "Extract the invoice number and total amount due."
        )
        .option("schema", INVOICE_SCHEMA)
        ._documents(invoice_path)
    )

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT",
        "_document_error",
    ]
    row = df.collect()[0]
    assert row["INVOICE_NUMBER"]
    assert row["TOTAL_AMOUNT"]
    assert row["_document_error"] is None


# ---------------------------------------------------------------------------
# PERMISSIVE errors (the only mode left -- FAILFAST was dropped with `mode`)
# ---------------------------------------------------------------------------


def test_permissive_captures_a_parse_error_without_aborting(session, unsupported_stage):
    rows = session.read._documents(f"@{unsupported_stage}").collect()

    assert len(rows) == 1
    # _document_error is an array, one entry per phase that failed -- not a single
    # struct -- so that a row failing in both Parse and Extract doesn't lose one.
    errors = json.loads(rows[0]["_document_error"])
    assert len(errors) == 1
    assert errors[0]["stage"] == "parse"
    assert errors[0]["message"]


def test_extract_type_mismatch_is_surfaced_not_silently_dropped(session):
    # AI_COMPLETE's own schema validation normally catches a genuine type mismatch
    # before it ever reaches this reader's TRY_CAST layer (verified separately,
    # live: AI_COMPLETE reports its own "json mode output validation error" for a
    # field that doesn't fit its declared type). That still leaves a
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
    # AI_COMPLETE's own in-band error already populates.
    assert (
        row["_DOC_EXTRACT_ERROR"]
        == "Field(s) did not match their declared type: AMOUNT"
    )


# ---------------------------------------------------------------------------
# Option validation through the public .option() surface
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key,value",
    [
        pytest.param("parse_mode", "layout", id="parse_mode"),
        pytest.param("row_boundary", "page", id="row_boundary"),
        pytest.param("mode", "FAILFAST", id="mode"),
        pytest.param("extraction_engine", "ai_extract", id="extraction_engine"),
        pytest.param("model", "claude-sonnet-4-5", id="model"),
        pytest.param("extract_images", True, id="extract_images"),
        pytest.param("page_filter", [{"start": 0, "end": 1}], id="page_filter"),
        pytest.param(
            "columnNameOfCorruptRecord", "docError", id="corrupt_record_column"
        ),
        pytest.param("bogus_option", "anything", id="unknown_option"),
    ],
)
def test_unsupported_option_raises_before_any_query(session, invoice_path, key, value):
    # Private preview locks `.option()` down to just `schema` and `prompt` -- every
    # other option that used to drive this reader (including ones that used to be
    # valid, like parse_mode/mode/extraction_engine) is now rejected outright.
    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        session.read.option(key, value)._documents(invoice_path)
    assert exc_info.value.error_code == "1119"


def test_non_stage_path_raises(session):
    with pytest.raises(ValueError, match="invalid Snowflake stage location"):
        session.read._documents("/local/path/doc.pdf")
