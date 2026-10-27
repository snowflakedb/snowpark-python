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

# Tests that make the reader register server-side Python (the extraction procedure for
# schema reads, or a stored procedure of their own) carry @pytest.mark.udf, so the
# test runs that exclude server-side Python ("not udf") skip them.
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
    df = session.read.documents(invoice_path)

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
    rows = session.read.documents(f"@{doc_stage}").collect()

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


@pytest.mark.udf
def test_schema_scalar_types_are_cast_not_stringified(session, invoice_path):
    df = session.read.option("schema", TYPED_SCALAR_SCHEMA).documents(invoice_path)

    # No CONTENT/TOTAL_PAGES (parse is skipped for a schema read); EXTRACTION_SCORES
    # holds the model's confidence for each top-level field.
    assert _unquoted(df) == [
        "SOURCE_FILE",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT_NUMBER",
        "IS_INVOICE",
        "EXTRACTION_SCORES",
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


@pytest.mark.udf
def test_schema_array_field_stays_variant_not_stringified(session, invoice_path):
    df = session.read.option("schema", ARRAY_FIELD_SCHEMA).documents(invoice_path)

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


@pytest.mark.udf
def test_flat_schema_field_returns_a_string_column(session, invoice_path):
    # A flat {name: question} dict/list schema used to pass straight through to
    # AI_EXTRACT and come back as an untyped VARIANT. Now it's converted to a
    # string-typed JSON Schema so AI_COMPLETE (the only engine left) can use it.
    df = session.read.option("schema", FLAT_NL_SCHEMA).documents(invoice_path)

    assert isinstance(_datatype(df, "INVOICE_NUMBER"), StringType)
    row = df.collect()[0]
    assert row["INVOICE_NUMBER"]
    assert row["_document_error"] is None


def test_prompt_only_read_returns_a_response_column(session, invoice_path):
    # No schema -> no response_format; AI_COMPLETE answers the prompt as free text
    # in a RESPONSE column instead of a per-field extraction.
    df = session.read.option(
        "prompt", "Summarize this document in one sentence."
    ).documents(invoice_path)

    assert _unquoted(df) == ["SOURCE_FILE", "RESPONSE", "_document_error"]
    row = df.collect()[0]
    assert row["RESPONSE"]
    assert row["_document_error"] is None


@pytest.mark.udf
def test_schema_and_prompt_together(session, invoice_path):
    # schema drives response_format as usual; prompt replaces the default
    # extraction instruction that would otherwise be sent alongside it.
    df = (
        session.read.option(
            "prompt", "Extract the invoice number and total amount due."
        )
        .option("schema", INVOICE_SCHEMA)
        .documents(invoice_path)
    )

    assert _unquoted(df) == [
        "SOURCE_FILE",
        "INVOICE_NUMBER",
        "TOTAL_AMOUNT",
        "EXTRACTION_SCORES",
        "_document_error",
    ]
    row = df.collect()[0]
    assert row["INVOICE_NUMBER"]
    assert row["TOTAL_AMOUNT"]
    assert row["_document_error"] is None
    # One confidence score between 0 and 1 per top-level schema field.
    scores = json.loads(row["EXTRACTION_SCORES"])
    assert set(scores) <= {"invoice_number", "total_amount"} and scores
    assert all(0 <= v <= 1 for v in scores.values())


# ---------------------------------------------------------------------------
# PERMISSIVE errors (the only mode left -- FAILFAST was dropped with `mode`)
# ---------------------------------------------------------------------------


def test_permissive_captures_a_parse_error_without_aborting(session, unsupported_stage):
    rows = session.read.documents(f"@{unsupported_stage}").collect()

    assert len(rows) == 1
    # _document_error is an array, one entry per phase that failed -- not a single
    # struct -- so that a row failing in both Parse and Extract doesn't lose one.
    errors = json.loads(rows[0]["_document_error"])
    assert len(errors) == 1
    assert errors[0]["stage"] == "parse"
    assert errors[0]["message"]


def _stage_with(session, files):
    """A temp stage with SNOWFLAKE_SSE encryption holding ``files`` ({name: path})."""
    stage_name = Utils.random_stage_name()
    session.sql(
        f"CREATE OR REPLACE TEMP STAGE {stage_name} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
    ).collect()
    for name, path in files.items():
        session.file.put(
            path, f"@{stage_name}/{name}", auto_compress=False, overwrite=True
        )
    return stage_name


def _text_pdf(page_lines):
    """A minimal valid PDF with one page per entry of ``page_lines``, each page showing
    its lines in Helvetica. Written by hand because pypdf is a package of the stored
    procedure's environment, not of the client's test environment."""

    def escape(text):
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    count = len(page_lines)
    page_ids = [4 + 2 * i for i in range(count)]
    objects = {
        1: "<< /Type /Catalog /Pages 2 0 R >>",
        2: f"<< /Type /Pages /Kids [{' '.join(f'{i} 0 R' for i in page_ids)}] "
        f"/Count {count} >>",
        3: "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    }
    for page_id, lines in zip(page_ids, page_lines):
        text = " T* ".join(f"({escape(line)}) Tj" for line in lines)
        stream = f"BT /F1 12 Tf 14 TL 72 720 Td {text} ET"
        objects[page_id] = (
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {page_id + 1} 0 R >>"
        )
        objects[
            page_id + 1
        ] = f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream"
    out = bytearray(b"%PDF-1.4\n")
    offsets = {}
    for number in sorted(objects):
        offsets[number] = len(out)
        out += f"{number} 0 obj\n{objects[number]}\nendobj\n".encode("latin-1")
    xref = len(out)
    size = max(objects) + 1
    out += f"xref\n0 {size}\n0000000000 65535 f \n".encode()
    for number in range(1, size):
        out += f"{offsets[number]:010d} 00000 n \n".encode()
    out += (
        f"trailer\n<< /Size {size} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    )
    return bytes(out)


def _long_invoice_pages(count):
    """Text for a ``count``-page invoice: a header, line items on every page, and the
    total on the last page."""
    pages = []
    for page in range(1, count + 1):
        lines = [f"Invoice number: INV-4821      Page {page} of {count}"]
        if page == 1:
            lines += ["Bill to: Example Supply Co.", "Date: 2026-03-01"]
        lines += [
            f"Item {page}-{row}: Widget model {row}, quantity {row}, price USD 1.00"
            for row in range(1, 11)
        ]
        if page == count:
            lines.append("Total amount: USD 600.00")
        pages.append(lines)
    return pages


# Fields that appear on one end of the long invoice only: bill_to and invoice_date on
# page 1, total_amount on page 60. Both ends coming back proves the first and last
# page ranges were extracted and merged.
LONG_INVOICE_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string"},
        "bill_to": {"type": "string"},
        "invoice_date": {"type": "string"},
        "total_amount": {"type": "string"},
    },
}


def _ai_complete_calls_in_session(session):
    """AI_COMPLETE statements run so far in this session, including the ones the
    extraction procedure issued."""
    return session.sql(
        "select count(*) from table(information_schema.query_history_by_session("
        "result_limit => 10000)) where query_text ilike 'SELECT AI_COMPLETE(%'"
    ).collect()[0][0]


@pytest.mark.udf
def test_long_pdf_goes_through_the_split_procedure(session, tmp_path):
    """A PDF over 50 pages is planned first (planning-only calls choose the page
    ranges) and then extracted range by range; the merged result is one row with
    values from both the first and the last page."""
    long_pdf = tmp_path / "long.pdf"
    # 60 pages, above the 50-page planning limit.
    long_pdf.write_bytes(_text_pdf(_long_invoice_pages(60)))

    stage_name = _stage_with(session, {"": str(long_pdf)})
    try:
        calls_before = _ai_complete_calls_in_session(session)
        df = session.read.option("schema", LONG_INVOICE_SCHEMA).documents(
            f"@{stage_name}"
        )
        assert _unquoted(df) == [
            "SOURCE_FILE",
            "INVOICE_NUMBER",
            "BILL_TO",
            "INVOICE_DATE",
            "TOTAL_AMOUNT",
            "EXTRACTION_SCORES",
            "_document_error",
        ]
        rows = df.collect()
        assert len(rows) == 1
        row = rows[0]
        assert row["_document_error"] is None
        assert row["INVOICE_NUMBER"] == "INV-4821"
        # Page 1 only.
        assert "Example Supply" in row["BILL_TO"]
        assert "2026-03-01" in row["INVOICE_DATE"]
        # Page 60 only.
        assert "600" in row["TOTAL_AMOUNT"]
        # Planning plus at least one extraction call, so the file was not sent whole.
        assert _ai_complete_calls_in_session(session) - calls_before > 1
    finally:
        Utils.drop_stage(session, stage_name)


@pytest.mark.udf
def test_an_unreadable_file_does_not_stop_the_other_files(
    session, resources_path, tmp_path
):
    """One bad file among good ones: every file still gets a row, only the bad row
    carries an error (permissive)."""
    bad = tmp_path / "bad.pdf"
    bad.write_bytes(b"this is not a pdf")
    stage_name = _stage_with(
        session,
        {"": TestFiles(resources_path).test_invoice_pdf},
    )
    session.file.put(str(bad), f"@{stage_name}", auto_compress=False, overwrite=True)
    try:
        rows = (
            session.read.option("schema", INVOICE_SCHEMA)
            .documents(f"@{stage_name}")
            .collect()
        )
        assert len(rows) == 2
        failed = [r for r in rows if r["_document_error"] is not None]
        good = [r for r in rows if r["_document_error"] is None]
        assert len(failed) == 1 and len(good) == 1
        assert failed[0]["INVOICE_NUMBER"] is None
        assert good[0]["INVOICE_NUMBER"] is not None
    finally:
        Utils.drop_stage(session, stage_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Stored procedures are not supported in local testing mode.",
)
@pytest.mark.udf
def test_documents_can_be_called_from_inside_a_stored_procedure(session, doc_stage):
    """Calling documents() inside a user's stored procedure registers and calls the
    extraction procedure from within a procedure. This is the
    is_in_stored_procedure() branch of document_reader._register_proc, which uploads
    the procedure file to a temporary import stage first. It needs the client code under
    test inside the procedure, so the snowpark package directory is imported."""
    import os

    import snowflake.snowpark
    from snowflake.snowpark import Session
    from snowflake.snowpark.types import IntegerType

    def count_rows(sess: Session, path: str) -> int:
        import pkgutil
        import tempfile

        from snowflake.snowpark._internal import document_reader

        # The imported package lives in a zip, so the helper files the reader
        # uploads by path don't exist on disk (an installed package is a plain
        # directory). Write them out and point the reader at the copies.
        out_dir = tempfile.mkdtemp()
        for attr in ("_EXTRACT_PROC_FILE_PATH", "_PDF_READER_FILE_PATH"):
            src_path = getattr(document_reader, attr)
            if os.path.isfile(src_path):
                continue
            name = os.path.basename(src_path)
            data = pkgutil.get_data("snowflake.snowpark._internal", name)
            with open(os.path.join(out_dir, name), "wb") as f:
                f.write(data)
            setattr(document_reader, attr, os.path.join(out_dir, name))
        return len(sess.read.option("schema", INVOICE_SCHEMA).documents(path).collect())

    package_dir = os.path.dirname(snowflake.snowpark.__file__)
    sproc = session.sproc.register(
        count_rows,
        return_type=IntegerType(),
        input_types=[StringType()],
        packages=["snowflake-snowpark-python", "pypdf"],
        imports=[(package_dir, "snowflake.snowpark")],
        # The reader registers temporary functions and procedures, which an
        # owner's-rights procedure is not allowed to create.
        execute_as="caller",
    )
    assert sproc(f"@{doc_stage}/invoice.pdf") == 1


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
        pytest.param("parse_mode", "bogus", id="parse_mode"),
        pytest.param("row_boundary", "chapter", id="row_boundary"),
    ],
)
def test_invalid_parse_option_raises_before_any_query(
    session, invoice_path, key, value
):
    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        session.read.option(key, value).documents(invoice_path)
    assert exc_info.value.error_code == "1116"


@pytest.mark.parametrize(
    "key,value",
    [
        pytest.param("mode", "FAILFAST", id="mode"),
        pytest.param("extraction_engine", "ai_extract", id="extraction_engine"),
        pytest.param("model", "claude-sonnet-4-5", id="model"),
        pytest.param("bogus_option", "anything", id="unknown_option"),
    ],
)
def test_options_chosen_by_the_reader_are_ignored(session, invoice_path, key, value):
    # Same read as without the option: the reader chooses the model, fail mode and
    # engine itself, so these keys do nothing.
    columns = session.read.option(key, value).documents(invoice_path).columns
    assert columns == session.read.documents(invoice_path).columns


def test_extract_images_adds_an_images_column(session, invoice_path):
    df = session.read.option("extract_images", True).documents(invoice_path)
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


def test_extract_images_with_page_rows(session, invoice_path):
    df = (
        session.read.option("extract_images", True)
        .option("row_boundary", "page")
        .documents(invoice_path)
    )
    assert "IMAGES" in _unquoted(df) and "PAGE_INDEX" in _unquoted(df)
    assert df.collect()[0]["_document_error"] is None


def test_extract_images_needs_layout_mode(session, invoice_path):
    with pytest.raises(SnowparkDataframeReaderException, match="layout"):
        session.read.option("extract_images", True).option(
            "parse_mode", "ocr"
        ).documents(invoice_path)


def test_non_stage_path_raises(session):
    with pytest.raises(ValueError, match="invalid Snowflake stage location"):
        session.read.documents("/local/path/doc.pdf")


def test_private_documents_alias_still_works(session, invoice_path):
    df = session.read._documents(invoice_path)
    assert _unquoted(df) == [
        "SOURCE_FILE",
        "TOTAL_PAGES",
        "CONTENT",
        "_document_error",
    ]
