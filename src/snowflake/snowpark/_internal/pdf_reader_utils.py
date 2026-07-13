#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Orchestration behind :meth:`DataFrameReader.pdf`.

`read.pdf()` is a thin layer over Cortex AI functions (``AI_PARSE_DOCUMENT`` /
``AI_COMPLETE``) plus an optional pdfplumber UDF. Given a stage of PDFs and an
output schema, it returns a materialized, typed DataFrame. The extraction prompt
is generated from the schema; a caller-supplied prompt is optional extra
guidance.

Pipeline::

    stage of PDFs
      -> parse step    (mode: text | parse | direct | auto)
      -> AI_COMPLETE   (schema -> typed columns)
      -> write to target table (a session temp table if unspecified)
      -> DataFrame over that table

The two Cortex call sites (`_sql_complete`, `_sql_parse`) are isolated so they
can be matched to an account's Cortex signature version in one place.

Deliberate design points (see the PRD / POC notes):
  * Eager. `pdf()` executes and materializes on the call, unlike other read.*
    methods, because the plan holds volatile, expensive AI calls that a lazy
    DataFrame would re-run and re-bill on every action with no result-cache
    reuse.
  * `auto` is a two-pass structure (probe materialized, then OCR only the
    scanned subset) -- NOT a `CASE WHEN needs_ocr THEN AI_PARSE(...)`, because
    short-circuiting is not guaranteed for an expensive volatile function.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from snowflake.snowpark.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
    VariantType,
)

if TYPE_CHECKING:
    from snowflake.snowpark.dataframe import DataFrame
    from snowflake.snowpark.session import Session

MODES = ("text", "parse", "direct", "auto")
DEFAULT_MODEL = "llama3.1-70b"
DEFAULT_TEXT_THRESHOLD = (
    50  # chars-per-page below this in the probe => treat as scanned
)

_TEXT_UDF_NAME = "pdf_extract_text_poc"

# Snowpark type -> (json schema type, optional format hint)
_JSON_TYPE = {
    StringType: ("string", None),
    BooleanType: ("boolean", None),
    IntegerType: ("integer", None),
    LongType: ("integer", None),
    DoubleType: ("number", None),
    DecimalType: ("number", None),
    DateType: ("string", "date"),
    TimestampType: ("string", "date-time"),
}


# --------------------------------------------------------------------------- #
# schema translation
# --------------------------------------------------------------------------- #


def _json_schema_from_structtype(schema: StructType) -> dict:
    """StructType -> JSON schema for AI_COMPLETE structured output.

    Two hard-won constraints (verified against Cortex on prod3):
      * Do NOT emit the JSON-schema ``format`` keyword (e.g. ``"date"``): Cortex
        structured output rejects it and silently returns NULL for the whole row.
        Convey formatting (ISO-8601 dates, etc.) through the prompt instead.
      * Mark all fields ``required``: without it the model omits fields it's
        unsure about even when the value is present in the document. Downstream
        TRY_CAST still nulls a genuinely bad value, so this is safe.
    """
    props: dict = {}
    for field in schema.fields:
        json_type, _fmt = _JSON_TYPE.get(type(field.datatype), ("string", None))
        props[field.name] = {"type": json_type}
    return {
        "type": "object",
        "properties": props,
        "required": [f.name for f in schema.fields],
    }


# Per-type guidance for the generated prompt. This is also where date/timestamp
# formatting lives, since Cortex rejects the JSON-schema `format` keyword.
_PROMPT_TYPE_HINT = {
    StringType: "a string",
    BooleanType: "true or false",
    IntegerType: "an integer",
    LongType: "an integer",
    DoubleType: "a number",
    DecimalType: "a number",
    DateType: "a date in ISO-8601 format (YYYY-MM-DD)",
    TimestampType: "a timestamp in ISO-8601 format (YYYY-MM-DDTHH:MM:SS)",
}


def _build_extraction_prompt(schema: StructType, user_prompt: str | None = None) -> str:
    """Generate the extraction instruction from the output schema.

    The schema is the contract, so the prompt is derived from it rather than
    hand-written by the caller. An optional ``user_prompt`` is appended for extra
    guidance (domain hints, disambiguation) but is never required.
    """
    lines = ["Extract the following fields from the document and return them as JSON:"]
    for f in schema.fields:
        hint = _PROMPT_TYPE_HINT.get(type(f.datatype), "a value")
        lines.append(f"- {f.name}: {hint}")
    lines.append("If a field is not present in the document, return null for it.")
    prompt = "\n".join(lines)
    if user_prompt:
        prompt += f"\n\nAdditional instructions:\n{user_prompt}"
    return prompt


def _cast_expr(json_path: str, datatype) -> str:
    """SQL to extract one response field and cast it, nulling on bad values.

    TRY_CAST/TRY_TO_* keep a single malformed value from failing the whole row;
    the row still carries `_raw_response` for debugging.
    """
    raw = f"{json_path}::string"
    if isinstance(datatype, (StringType, VariantType)):
        return raw
    if isinstance(datatype, DecimalType):
        return f"TRY_CAST({raw} AS DECIMAL({datatype.precision},{datatype.scale}))"
    if isinstance(datatype, (IntegerType, LongType)):
        return f"TRY_CAST({raw} AS NUMBER(38,0))"
    if isinstance(datatype, DoubleType):
        return f"TRY_CAST({raw} AS DOUBLE)"
    if isinstance(datatype, BooleanType):
        return f"TRY_CAST({raw} AS BOOLEAN)"
    if isinstance(datatype, DateType):
        return f"TRY_TO_DATE({raw})"
    if isinstance(datatype, TimestampType):
        return f"TRY_TO_TIMESTAMP({raw})"
    return raw


# --------------------------------------------------------------------------- #
# Cortex call sites -- match these to your account's Cortex signature version.
# --------------------------------------------------------------------------- #


def _sql_str(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


def _sql_json_literal(obj: dict) -> str:
    return "PARSE_JSON(" + _sql_str(json.dumps(obj)) + ")"


def _sql_complete(model: str, prompt_expr: str, schema: StructType | None) -> str:
    """AI_COMPLETE. Structured output via a `response_format` JSON schema."""
    if schema is None:
        return f"AI_COMPLETE({_sql_str(model)}, {prompt_expr})"
    response_format = {"type": "json", "schema": _json_schema_from_structtype(schema)}
    return (
        "AI_COMPLETE("
        f"model => {_sql_str(model)}, "
        f"prompt => {prompt_expr}, "
        f"response_format => {_sql_json_literal(response_format)}"
        ")"
    )


def _sql_parse(stage: str, relative_path_expr: str, ocr: bool = True) -> str:
    """AI_PARSE_DOCUMENT returning extracted text (reads `:content`)."""
    mode = "OCR" if ocr else "LAYOUT"
    file_expr = f"TO_FILE({_sql_str('@' + stage)}, {relative_path_expr})"
    return f"AI_PARSE_DOCUMENT({file_expr}, {_sql_json_literal({'mode': mode})}):content::string"


# --------------------------------------------------------------------------- #
# pdfplumber UDF (registered on demand for text/auto modes)
# --------------------------------------------------------------------------- #


def _register_text_udf(session: Session, threshold: int) -> str:
    """Register a UDF that reads a staged PDF and extracts its text layer.

    Returns {text, pages, needs_ocr}; `needs_ocr` is decided from a shallow probe
    (first 2 pages) so a long scan isn't fully parsed just to be identified.
    pdfplumber must be resolvable in the UDF sandbox (Snowflake Anaconda channel
    or the artifact repository).
    """

    def extract_text(scoped_url: str) -> dict:
        import io

        import pdfplumber

        from snowflake.snowpark.files import SnowflakeFile

        with SnowflakeFile.open(scoped_url, "rb") as f:
            data = f.read()
        parts, probe_chars, probe_pages = [], 0, 0
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            n_pages = len(pdf.pages)
            for i, page in enumerate(pdf.pages):
                t = page.extract_text() or ""
                parts.append(t)
                if i < 2:
                    probe_chars += len(t)
                    probe_pages += 1
        density = probe_chars / probe_pages if probe_pages else 0
        return {
            "text": "\n".join(parts),
            "pages": n_pages,
            "needs_ocr": density < threshold,
        }

    session.udf.register(
        extract_text,
        name=_TEXT_UDF_NAME,
        replace=True,
        is_permanent=False,
        return_type=VariantType(),
        input_types=[StringType()],
        packages=["snowflake-snowpark-python", "pdfplumber"],
    )
    return _TEXT_UDF_NAME


# --------------------------------------------------------------------------- #
# orchestration entry point (called by DataFrameReader.pdf)
# --------------------------------------------------------------------------- #


def _split_stage_path(path: str) -> tuple[str, str]:
    p = path.lstrip("@")
    stage, _, subdir = p.partition("/")
    return stage, subdir


def read_pdf(
    session: Session,
    path: str,
    *,
    schema: StructType,
    prompt: str | None,
    mode: str,
    model: str,
    target: str | None,
    threshold: int,
) -> DataFrame:
    """Execute the PDF read pipeline and return a DataFrame over the result table.

    ``schema`` is required; validation of arguments is done by the caller
    (DataFrameReader.pdf).
    """
    stage, subdir = _split_stage_path(path)
    pattern = f"{subdir}%" if subdir else "%"

    files_sql = f"""
        SELECT relative_path,
               BUILD_SCOPED_FILE_URL('@{stage}', relative_path) AS scoped_url,
               BUILD_STAGE_FILE_URL('@{stage}', relative_path)  AS file_url
        FROM DIRECTORY('@{stage}')
        WHERE relative_path ILIKE '{pattern}' AND relative_path ILIKE '%.pdf'
    """

    if mode == "text":
        udf = _register_text_udf(session, threshold)
        text_sql = f"SELECT file_url, relative_path, {udf}(scoped_url):text::string AS text FROM ({files_sql})"
    elif mode == "parse":
        text_sql = (
            f"SELECT file_url, relative_path, {_sql_parse(stage, 'relative_path')} AS text "
            f"FROM ({files_sql})"
        )
    elif mode == "auto":
        udf = _register_text_udf(session, threshold)
        # Pass 1: probe everything, materialized, so AI_PARSE touches only scans.
        session.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE _pdf_probe_poc AS
            SELECT file_url, relative_path, {udf}(scoped_url) AS r FROM ({files_sql})
        """
        ).collect()
        text_sql = f"""
            SELECT file_url, relative_path, r:text::string AS text
            FROM _pdf_probe_poc WHERE NOT r:needs_ocr::boolean
            UNION ALL
            SELECT file_url, relative_path, {_sql_parse(stage, 'relative_path')} AS text
            FROM _pdf_probe_poc WHERE r:needs_ocr::boolean
        """
    else:  # direct
        text_sql = f"SELECT file_url, relative_path, NULL AS text FROM ({files_sql})"

    gen_prompt = _build_extraction_prompt(schema, prompt)
    if mode == "direct":
        prompt_expr = (
            f"PROMPT({_sql_str(gen_prompt + ' {0}')}, "
            f"TO_FILE({_sql_str('@' + stage)}, relative_path))"
        )
    else:
        prompt_expr = f"CONCAT({_sql_str(gen_prompt)}, '\\n\\n', COALESCE(text, ''))"
    complete = _sql_complete(model, prompt_expr, schema)
    typed_cols = ",\n               ".join(
        f"{_cast_expr('resp:' + f.name, f.datatype)} AS {f.name}" for f in schema.fields
    )
    result_sql = f"""
        WITH raw AS (
            SELECT file_url, {complete} AS _raw_response FROM ({text_sql})
        )
        SELECT file_url, {typed_cols}, _raw_response,
               TRY_PARSE_JSON(_raw_response) IS NULL AS _parse_failed
        FROM (SELECT file_url, _raw_response, TRY_PARSE_JSON(_raw_response) AS resp FROM raw)
    """

    if target is None:
        target = "_pdf_result_poc"
        create = f"CREATE OR REPLACE TEMP TABLE {target} AS {result_sql}"
    else:
        create = f"CREATE OR REPLACE TABLE {target} AS {result_sql}"
    session.sql(create).collect()
    return session.table(target)
