#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import os
import weakref
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import str_to_sql
from snowflake.snowpark._internal.document_reader_options import (
    CONTENT_COLUMN,
    DocumentReaderOptions,
    EXTRACTION_SCORES_COLUMN,
    ExtractionSpec,
    IMAGES_COLUMN,
    PAGE_INDEX_COLUMN,
    RESPONSE_COLUMN,
    SOURCE_FILE_COLUMN,
    TOTAL_PAGES_COLUMN,
    _EXTRACT_ERROR_COLUMN,
    _PARSE_ERROR_COLUMN,
)
from snowflake.snowpark._internal.pdf_reader import PAGE_SEPARATOR
from snowflake.snowpark._internal.udf_utils import get_types_from_type_hints
from snowflake.snowpark._internal.utils import (
    STAGE_PREFIX,
    TempObjectType,
    is_in_stored_procedure,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import (
    ai_complete,
    ai_extract,
    ai_parse_document,
    any_value,
    array_compact,
    array_construct,
    array_size,
    array_to_string,
    coalesce,
    col,
    concat,
    flatten,
    listagg,
    lit,
    max as max_,
    object_construct_keep_null,
    random,
    substring,
    to_file,
    try_cast,
    try_parse_json,
    when,
)
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from snowflake.snowpark.version import VERSION

if TYPE_CHECKING:  # pragma: no cover
    from snowflake.snowpark.dataframe import DataFrame
    from snowflake.snowpark.session import Session


# Models in order of preference. For schema reads the extraction procedure uses the
# preferred until AI_COMPLETE reports it unknown for the account, then the next, and fails
# the read when it knows none; prompt reads choose one up front (prompt_model). Both are cross-region models, so an account or region without them
# can lack either.
# The budgets below were tuned on the preferred model.
_PREFERRED_EXTRACTION_MODEL = "claude-sonnet-5-5"
_FALLBACK_EXTRACTION_MODEL = "claude-opus-5"
_EXTRACTION_MODELS = [_PREFERRED_EXTRACTION_MODEL, _FALLBACK_EXTRACTION_MODEL]

_DEFAULT_EXTRACTION_PROMPT = "Extract the requested fields from this document."

# claude-sonnet-5-5 rejects temperature and response_format (a bare "malformed request"),
# so the schema goes in the prompt and the JSON is parsed out of the text answer. Without
# max_tokens AI_COMPLETE caps output at 4096, truncating long answers mid-JSON; 128000 is
# the model's ceiling.
_MAX_OUTPUT_TOKENS = 128000
# Each call is asked to stop below `target` output tokens and plan the rest of its pages,
# and is hard-capped at `max_tokens`. The model underestimates its own output (real output
# ~1.3x its estimate), so targets stay well below max_tokens.
# - "first": the first round of documents over 50 pages or 10 MB, when they are not planned.
# - "range": ranges planned for smaller documents.
# - "large_range": ranges planned for the large documents, against the same smaller target.
# - "merge_target": adjacent planned ranges are coalesced only up to this estimate.
# - "plan_first": large documents start with planning-only calls (segments of at most
#   50 pages / 10 MB) that choose ranges under range_target, all then extracted at once.
_CALL_BUDGETS = {
    "first": {"target": 80000, "max_tokens": _MAX_OUTPUT_TOKENS},
    "range": {"target": 120000, "max_tokens": _MAX_OUTPUT_TOKENS},
    "large_range": {"target": 80000, "max_tokens": _MAX_OUTPUT_TOKENS},
    "merge_target": 60000,
    "plan_first": {"range_target": 60000, "max_tokens": 64000},
}
# Floor for the per-document call cap; the procedure raises it from the answer size it
# observes, up to its hard maximum.
_MAX_CALLS_PER_FILE = 30


_EXTRACT_PROC_FILE_PATH = os.path.join(
    os.path.dirname(__file__), "document_extract_proc.py"
)
_EXTRACT_PROC_HANDLER = "run"
_EXTRACT_PROC_NAME = "SNOWPARK_TEMP_PROCEDURE_DOCUMENT_EXTRACT"

_PDF_READER_FILE_PATH = os.path.join(os.path.dirname(__file__), "pdf_reader.py")
_PDF_READER_HANDLER = "PDFTextReader"
_PDF_READER_UDTF_NAME = "SNOWPARK_TEMP_TABLE_FUNCTION_PDF_TEXT_READER"
_ERROR_GUARD_UDF_NAME = "SNOWPARK_TEMP_FUNCTION_RAISE_IF_DOCUMENT_ERROR"

_STAGE_PATH_COLUMN = "_DOC_STAGE_PATH"
_PARSED_COLUMN = "_DOC_PARSED"
_EXTRACTED_COLUMN = "_DOC_EXTRACTED"
_UDTF_ERROR_COLUMN = "UDTF_ERROR"


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


def read_documents(
    session: "Session", path: str, options: DocumentReaderOptions
) -> "DataFrame":
    if (
        options.extract_enabled
        and not options.parse_enabled
        and options.extraction_engine == "ai_complete"
    ):
        df = extract_with_split_procedure(session, path, options)
        df = finalize_errors(session, df, options)
        return df.select(output_columns(options))

    df = access(session, path, options)

    if options.parse_enabled:
        df = parse(session, df, options)

    if options.complete_enabled:
        df = extract(df, options)

    df = finalize_errors(session, df, options)
    return df.select(output_columns(options))


def output_columns(options: DocumentReaderOptions) -> List[str]:
    names = [SOURCE_FILE_COLUMN]
    if options.parse_enabled:
        if options.page_rows:
            names.append(PAGE_INDEX_COLUMN)
        names.append(TOTAL_PAGES_COLUMN)
        names.append(CONTENT_COLUMN)
        if options.extract_images:
            names.append(IMAGES_COLUMN)
    if options.extract_enabled:
        names.extend(options.extraction.field_columns)
        if options.scores_enabled:
            names.append(EXTRACTION_SCORES_COLUMN)
    elif options.complete_enabled:
        names.append(RESPONSE_COLUMN)
    if options.has_corrupt_record_column:
        names.append(quote_name_without_upper_casing(options.corrupt_record_column))
    return names


def as_variant(column: Column) -> Column:
    # AI_PARSE_DOCUMENT/AI_EXTRACT/AI_COMPLETE document their output as a JSON string;
    # re-parsing it makes sub-field access (col["field"]) uniform.
    return try_parse_json(column.cast(StringType()))


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def quote_stage_path(path: str) -> str:
    """``path`` as the repo's SQL string literal for LIST, with backslashes, single
    quotes and newlines escaped (see ``str_to_sql``), so nothing in it can add a
    clause to the statement."""
    return str_to_sql(path)


# Trailing (not leading) so Snowpark's own statement-type detection, which looks at
# the start of the text, is unaffected. Only enum-like values go in here -- never a
# path, prompt or schema, which could leak user data into QUERY_HISTORY.
_USAGE_COMMENT_FORMAT = (
    "/* Snowpark:DataFrameReader.documents read_type={read_type} version={version} */"
)


def usage_comment(options: DocumentReaderOptions) -> str:
    """A trailing SQL comment identifying a documents() read's entry query in
    QUERY_HISTORY, server-side counterpart to the read_type telemetry
    dataframe_reader.py adds to the client-side api_call."""
    version = ".".join(str(part) for part in VERSION if part is not None)
    return _USAGE_COMMENT_FORMAT.format(read_type=options.read_type, version=version)


def split_stage(path: str) -> Tuple[str, int]:
    """The stage part of ``path`` (``@`` and everything before the first ``/`` outside
    double quotes), and how many leading characters of a name LIST returns for it are the
    stage's own name. LIST names a file of a named stage '<stage name>/<relative path>',
    with no database or schema and the name lower-cased, but a file of the user stage
    (``@~``) or a table stage (``@%t``) by its relative path alone. A copy of
    ``split_stage`` in ``document_extract_proc.py``, which is uploaded standalone."""
    in_quotes = False
    end = len(path)
    for i, char in enumerate(path):
        if char == '"':
            in_quotes = not in_quotes
        elif char == "/" and not in_quotes:
            end = i
            break
    stage = "@" + path[:end].lstrip("@")
    in_quotes = False
    name_start = 0
    for i, char in enumerate(stage):
        if char == '"':
            in_quotes = not in_quotes
        elif char in "@." and not in_quotes:
            name_start = i + 1
    name = stage[name_start:]
    if name == "~" or name.startswith("%"):
        return stage, 0
    if name.startswith('"') and name.endswith('"') and len(name) > 1:
        name = name[1:-1].replace('""', '"')
    return stage, len(name) + 1


def access(
    session: "Session", path: str, options: DocumentReaderOptions
) -> "DataFrame":
    # The plain string path is kept as its own column because the PDF UDTF takes a
    # path string, not a FILE value. Each file is referenced through the stage as the
    # user wrote it (see split_stage), so it resolves for user and table stages, quoted
    # names and stages outside the current schema.
    stage, listed_prefix = split_stage(path)
    stage_path = concat(lit(f"{stage}/"), substring(col('"name"'), listed_prefix + 1))
    files_df = session.sql(
        f"LIST {quote_stage_path(path)} {usage_comment(options)}"
    ).select(
        to_file(stage_path).alias(SOURCE_FILE_COLUMN),
        stage_path.alias(_STAGE_PATH_COLUMN),
    )
    # LIST, like DIRECTORY(@stage), carries no cardinality statistics for the
    # optimizer, which otherwise defaults every downstream AI_PARSE_DOCUMENT/
    # AI_EXTRACT/AI_COMPLETE call in this plan to a single partition/thread
    # regardless of warehouse size. sort(random()) forces a shuffle that
    # redistributes rows -- and therefore the AI calls made per row -- across
    # every thread the warehouse has.
    return files_df.sort(random())


def parse(
    session: "Session", files_df: "DataFrame", options: DocumentReaderOptions
) -> "DataFrame":
    if options.parse_mode == "text":
        return parse_text(session, files_df, options)
    return parse_ai(files_df, options)


def parse_ai(files_df: "DataFrame", options: DocumentReaderOptions) -> "DataFrame":
    parse_kwargs: Dict[str, Any] = {
        "mode": options.parse_mode.upper(),
        # AI_PARSE_DOCUMENT rejects page_filter unless page_split is also true,
        # even when the document boundary (row_boundary="document") is what
        # collapses the pages back down afterwards in aggregate_pages().
        "page_split": options.row_boundary == "page" or options.page_filter is not None,
    }
    if options.page_filter is not None:
        parse_kwargs["page_filter"] = options.page_filter
    if options.extract_images:
        parse_kwargs["extract_images"] = True

    df = files_df.with_column(
        _PARSED_COLUMN,
        as_variant(
            ai_parse_document(
                col(SOURCE_FILE_COLUMN), return_error_details=True, **parse_kwargs
            )
        ),
    )
    parsed = col(_PARSED_COLUMN)
    # return_error_details=True wraps content/pages/images under `value`, sibling to
    # `error` and `metadata` -- not at the top level.
    payload = parsed["value"]
    df = df.with_column(_PARSE_ERROR_COLUMN, parsed["error"].cast(StringType()))
    if options.extract_images:
        df = df.with_column(IMAGES_COLUMN, payload["images"])

    page_count = parsed["metadata"]["pageCount"].cast(IntegerType())

    # page_filter implies page_split server-side, so the response carries a `pages`
    # array and no top-level `content` whenever either one is set.
    if options.page_filter is None and options.row_boundary != "page":
        return df.with_columns(
            [TOTAL_PAGES_COLUMN, CONTENT_COLUMN],
            [page_count, payload["content"].cast(StringType())],
        )

    # Resolved before the explode so the parsed document isn't referenced above the
    # join and replicated once per page.
    df = df.with_column(
        TOTAL_PAGES_COLUMN, coalesce(page_count, array_size(payload["pages"]))
    )

    # outer=True keeps exactly one row for a file whose parse failed and therefore has
    # no `pages` array to expand.
    exploded = df.join_table_function(
        flatten(payload["pages"], outer=True)
    ).with_columns(
        [PAGE_INDEX_COLUMN, CONTENT_COLUMN],
        [
            col("VALUE")["index"].cast(IntegerType()),
            col("VALUE")["content"].cast(StringType()),
        ],
    )
    if options.row_boundary == "page":
        return exploded
    return aggregate_pages(exploded, options)


def aggregate_pages(
    exploded: "DataFrame", options: DocumentReaderOptions
) -> "DataFrame":
    """Collapse a page-shaped response back to one row per file, needed when a
    page_filter narrows the document but the caller asked for document rows."""
    aggregations = [
        listagg(col(CONTENT_COLUMN), PAGE_SEPARATOR)
        .within_group(col(PAGE_INDEX_COLUMN).asc())
        .alias(CONTENT_COLUMN),
        max_(col(TOTAL_PAGES_COLUMN)).alias(TOTAL_PAGES_COLUMN),
        max_(col(_PARSE_ERROR_COLUMN)).alias(_PARSE_ERROR_COLUMN),
    ]
    if options.extract_images:
        aggregations.append(any_value(col(IMAGES_COLUMN)).alias(IMAGES_COLUMN))

    # SOURCE_FILE is rebuilt rather than aggregated because FILE is not a groupable type.
    return (
        exploded.group_by(_STAGE_PATH_COLUMN)
        .agg(*aggregations)
        .with_column(SOURCE_FILE_COLUMN, to_file(col(_STAGE_PATH_COLUMN)))
    )


def parse_text(
    session: "Session", files_df: "DataFrame", options: DocumentReaderOptions
) -> "DataFrame":
    pdf_udtf = register_pdf_udtf(session)
    # is not None, not truthy: page_filter=[] means "select zero pages" here too,
    # matching parse_ai, rather than falling back to "no filter".
    page_filter_json = (
        json.dumps(options.page_filter) if options.page_filter is not None else ""
    )
    return files_df.join_table_function(
        pdf_udtf(
            col(_STAGE_PATH_COLUMN),
            lit(options.row_boundary),
            lit(page_filter_json),
            lit(options.mode),
        )
    ).with_column(_PARSE_ERROR_COLUMN, col(_UDTF_ERROR_COLUMN))


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def cast_extracted_field(
    value: Column, field: str, extraction: ExtractionSpec
) -> Tuple[Column, Optional[Column]]:
    """Returns (output_column, mismatch), where mismatch is a boolean Column (true when
    the field was populated but didn't match its declared type), or None when there is no
    scalar type to enforce. Uses TRY_CAST via VARCHAR (VARIANT has no direct TRY_CAST
    overload) to degrade a bad field to NULL rather than aborting the row; the mismatch
    signal surfaces that degradation through the existing error column so PERMISSIVE/
    FAILFAST semantics are preserved. Fields with no declared type or a composite type
    (array/object) are returned as-is."""
    target_type = extraction.field_types.get(field)
    if target_type is None:
        return value, None
    text = value.cast(StringType())
    if isinstance(target_type, StringType):
        return text, None
    casted = try_cast(text, target_type)
    return casted, value.is_not_null() & casted.is_null()


def merge_extract_error(
    native_error: Column, field_columns: List[str], mismatches: List[Optional[Column]]
) -> Column:
    tagged = [
        when(mismatch, lit(name))
        for name, mismatch in zip(field_columns, mismatches)
        if mismatch is not None
    ]
    if not tagged:
        return native_error.cast(StringType())
    mismatched_fields = array_compact(array_construct(*tagged))
    mismatch_message = when(
        array_size(mismatched_fields) > 0,
        concat(
            lit("Field(s) did not match their declared type: "),
            array_to_string(mismatched_fields, lit(", ")),
        ),
    )
    # AI_EXTRACT/AI_COMPLETE's own in-band error takes precedence -- a call that
    # already failed has nothing meaningful in `response`/`value` to mismatch-check
    # in the first place, so the two conditions can't both legitimately fire at once.
    return coalesce(native_error.cast(StringType()), mismatch_message)


def extract(df: "DataFrame", options: DocumentReaderOptions) -> "DataFrame":
    input_col = (
        col(CONTENT_COLUMN) if options.parse_enabled else col(SOURCE_FILE_COLUMN)
    )
    if options.extraction_engine == "ai_complete":
        if options.parse_enabled:
            # AI_PARSE_DOCUMENT chained into AI_COMPLETE in one compiled query hits a
            # Snowflake compiler bug: AI_PARSE_DOCUMENT gets rewritten into a private,
            # 4-argument preview function, but that rewrite ends up passing 5 arguments
            # whenever AI_COMPLETE is also present in the same plan (AI_EXTRACT in that
            # same position does not trigger it). Collecting the parse result and
            # re-issuing AI_COMPLETE as a separate query splits the two calls into
            # separate compiled plans, which avoids the buggy rewrite entirely -- this
            # was confirmed by reproducing both the failure and the workaround directly
            # against the account. The tradeoff: this forces read_documents() to execute
            # immediately for this one combination, rather than staying lazy until the
            # caller's own collect().
            df = materialize_parsed_content(df, options)
            input_col = col(CONTENT_COLUMN)
        return extract_with_ai_complete(df, input_col, options)
    return extract_with_ai_extract(df, input_col, options)


def materialize_parsed_content(
    df: "DataFrame", options: DocumentReaderOptions
) -> "DataFrame":
    columns = [_STAGE_PATH_COLUMN, TOTAL_PAGES_COLUMN, CONTENT_COLUMN]
    fields = [
        StructField(_STAGE_PATH_COLUMN, StringType()),
        StructField(TOTAL_PAGES_COLUMN, IntegerType()),
        StructField(CONTENT_COLUMN, StringType()),
    ]
    if options.page_rows:
        columns.append(PAGE_INDEX_COLUMN)
        fields.append(StructField(PAGE_INDEX_COLUMN, IntegerType()))
    if options.extract_images:
        columns.append(IMAGES_COLUMN)
        fields.append(StructField(IMAGES_COLUMN, StringType()))
    columns.append(_PARSE_ERROR_COLUMN)
    fields.append(StructField(_PARSE_ERROR_COLUMN, StringType()))

    rows = [[row[name] for name in columns] for row in df.collect()]
    materialized = df._session.create_dataframe(rows, schema=StructType(fields))
    # SOURCE_FILE (a FILE value) can't round-trip through create_dataframe as a
    # literal, so it is rebuilt from the stage path -- the same trick
    # aggregate_pages() already relies on for the same reason.
    materialized = materialized.with_column(
        SOURCE_FILE_COLUMN, to_file(col(_STAGE_PATH_COLUMN))
    )
    if options.extract_images:
        materialized = materialized.with_column(
            IMAGES_COLUMN, try_parse_json(col(IMAGES_COLUMN))
        )
    # Re-applies the same warehouse-parallelism fix as access(): a literal VALUES
    # source generally carries real cardinality, but shuffle regardless so this
    # doesn't silently regress if that ever isn't true.
    return materialized.sort(random())


def extract_with_ai_extract(
    df: "DataFrame", input_col: Column, options: DocumentReaderOptions
) -> "DataFrame":
    extraction = options.extraction
    df = df.with_column(
        _EXTRACTED_COLUMN,
        as_variant(
            ai_extract(
                input_col, response_format=extraction.ai_extract_format, scores=True
            )
        ),
    )
    extracted = col(_EXTRACTED_COLUMN)
    response = extracted["response"]
    cast_results = [
        cast_extracted_field(response[field], field, extraction)
        for field in extraction.fields
    ]
    names = extraction.field_columns + [EXTRACTION_SCORES_COLUMN, _EXTRACT_ERROR_COLUMN]
    values = [column for column, _ in cast_results] + [
        extracted["scoring"],
        merge_extract_error(
            extracted["error"],
            extraction.field_columns,
            [mismatch for _, mismatch in cast_results],
        ),
    ]
    return df.with_columns(names, values)


# Whether AI_COMPLETE knows the preferred model, per session (see prompt_model).
_PREFERRED_MODEL_KNOWN: "weakref.WeakKeyDictionary[Session, bool]" = (
    weakref.WeakKeyDictionary()
)


def prompt_model(session: "Session") -> str:
    """The model for a prompt read: the preferred one, or the fallback when AI_COMPLETE
    reports the preferred one unknown for the account or region. A prompt read is one
    query, and an unknown model fails the whole query rather than one document, so it is
    checked up front with one call of a single output token, once per session. Any other
    failure of the check keeps the preferred model for this read and is not remembered."""
    known = _PREFERRED_MODEL_KNOWN.get(session)
    if known is None:
        try:
            session.sql(
                "SELECT AI_COMPLETE(?, 'Reply with OK.', {'max_tokens': 1})",
                params=[_PREFERRED_EXTRACTION_MODEL],
            ).collect()
            known = True
        except Exception as exc:
            if "unknown model" not in str(exc).lower():
                return _PREFERRED_EXTRACTION_MODEL
            known = False
        _PREFERRED_MODEL_KNOWN[session] = known
    return _PREFERRED_EXTRACTION_MODEL if known else _FALLBACK_EXTRACTION_MODEL


def build_ai_complete_call(
    input_col: Column,
    options: DocumentReaderOptions,
    model: str = _PREFERRED_EXTRACTION_MODEL,
) -> Column:
    # Parse ran: input_col is parsed CONTENT text, folded into AI_COMPLETE's
    # single-string variant. Parse didn't run: input_col is SOURCE_FILE, passed to
    # AI_COMPLETE's file variant instead -- whose prompt argument must then be a plain
    # string, not a Column, so there is no CONTENT to fold in. With no schema there is
    # no response_format and AI_COMPLETE answers the prompt as free text.
    response_format = (
        options.extraction.ai_complete_format if options.extract_enabled else None
    )
    if options.parse_enabled:
        prompt_text = (
            input_col
            if options.prompt is None
            else concat(lit(options.prompt), lit("\n\n"), input_col)
        )
        return ai_complete(
            model,
            prompt_text,
            model_parameters={"max_tokens": _MAX_OUTPUT_TOKENS},
            response_format=response_format,
            return_error_details=True,
        )
    return ai_complete(
        model,
        options.prompt or _DEFAULT_EXTRACTION_PROMPT,
        file=input_col,
        model_parameters={"max_tokens": _MAX_OUTPUT_TOKENS},
        response_format=response_format,
        return_error_details=True,
    )


def extract_with_ai_complete(
    df: "DataFrame", input_col: Column, options: DocumentReaderOptions
) -> "DataFrame":
    call = build_ai_complete_call(input_col, options, prompt_model(df.session))
    # Unlike AI_PARSE_DOCUMENT/AI_EXTRACT, AI_COMPLETE(..., return_error_details=True)
    # returns a native OBJECT rather than a JSON string -- CAST(... AS VARCHAR), which
    # as_variant() always applies, fails to compile for it. Bracket access works
    # directly on the raw result, so it skips as_variant() here.
    df = df.with_column(_EXTRACTED_COLUMN, call)
    extracted = col(_EXTRACTED_COLUMN)
    value = extracted["value"]
    extraction = options.extraction
    if extraction is None:
        return df.with_columns(
            [RESPONSE_COLUMN, _EXTRACT_ERROR_COLUMN],
            [value.cast(StringType()), extracted["error"].cast(StringType())],
        )
    return project_extracted_fields(df, value, extracted["error"], extraction)


def project_extracted_fields(
    df: "DataFrame", value: Column, error: Column, extraction: ExtractionSpec
) -> "DataFrame":
    cast_results = [
        cast_extracted_field(value[field], field, extraction)
        for field in extraction.fields
    ]
    names = extraction.field_columns + [_EXTRACT_ERROR_COLUMN]
    values = [column for column, _ in cast_results] + [
        merge_extract_error(
            error,
            extraction.field_columns,
            [mismatch for _, mismatch in cast_results],
        )
    ]
    return df.with_columns(names, values)


def extract_with_split_procedure(
    session: "Session", path: str, options: DocumentReaderOptions
) -> "DataFrame":
    """Schema extraction straight from the FILE, splitting the PDF across several
    AI_COMPLETE calls when one response cannot hold the whole answer. The loop needs each
    call's answer before planning the next, so it runs in a stored procedure; the CALL
    still stays lazy because Snowpark defers it to RESULT_SCAN at collect time."""
    proc_name = register_extract_proc(session)
    # In the order of run()'s parameters (document_extract_proc.py) and of
    # register_extract_proc()'s input_types.
    args = [
        path,
        json.dumps(options.extraction.ai_complete_format),
        options.prompt or _DEFAULT_EXTRACTION_PROMPT,
        json.dumps(_EXTRACTION_MODELS),
        json.dumps(_CALL_BUDGETS),
        _MAX_CALLS_PER_FILE,
        session.get_session_stage(),
    ]
    df = session.sql(
        f"CALL {proc_name}({', '.join(['?'] * len(args))}) {usage_comment(options)}",
        params=args,
    ).select(
        to_file(col("STAGE_PATH")).alias(SOURCE_FILE_COLUMN),
        try_parse_json(col("VALUE")).alias(_EXTRACTED_COLUMN),
        col("ERROR").alias(_EXTRACT_ERROR_COLUMN),
        try_parse_json(col("SCORES")).alias(EXTRACTION_SCORES_COLUMN),
    )
    return project_extracted_fields(
        df, col(_EXTRACTED_COLUMN), col(_EXTRACT_ERROR_COLUMN), options.extraction
    )


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def finalize_errors(
    session: "Session", df: "DataFrame", options: DocumentReaderOptions
) -> "DataFrame":
    error_stages = options.error_stages
    if not error_stages:
        return df

    if options.mode == "PERMISSIVE":
        # One entry per phase that actually failed, not just one overall verdict: a
        # single AI SQL call's own error is scoped to that one call, but a row here
        # can go through two independent calls (parse, then extract), so both can
        # fail at once and neither should be dropped in favor of the other.
        tagged = [
            when(
                col(name).is_not_null(),
                object_construct_keep_null(
                    lit("stage"), lit(stage), lit("message"), col(name)
                ),
            )
            for stage, name in error_stages
        ]
        errors = array_compact(array_construct(*tagged))
        return df.with_column(
            quote_name_without_upper_casing(options.corrupt_record_column),
            when(array_size(errors) > 0, errors),
        )

    # FAILFAST: every AI call captures its errors in-band, so aborting the read takes
    # an explicit raise, anchored to a real returned column -- an unused/dropped one
    # could be optimized away and the check with it.
    if options.parse_enabled:
        anchor = CONTENT_COLUMN
    elif options.extract_enabled:
        anchor = options.extraction.field_columns[0]
    else:
        anchor = RESPONSE_COLUMN
    guard = register_error_guard_udf(session)
    raw_error = merge_error_expressions([col(name) for _, name in error_stages])
    return df.with_column(anchor, guard(raw_error, col(anchor)))


def merge_error_expressions(expressions: List[Column]) -> Column:
    # coalesce() requires at least two arguments on this account -- Parse or Extract
    # alone (parse_mode="none", or no schema set) leaves only one error expression,
    # which needs no merging at all.
    return coalesce(*expressions) if len(expressions) > 1 else expressions[0]


# ---------------------------------------------------------------------------
# Runtime object registration
# ---------------------------------------------------------------------------


def register_pdf_udtf(session: "Session"):
    """Register a session-scoped PDF text-extraction UDTF, mirroring
    DataFrameReader._register_xml_udtf."""
    if is_in_stored_procedure():  # pragma: no cover
        import_stage_name = session.get_fully_qualified_name_if_possible(
            "SNOWPARK_TEMP_STAGE_PDFIMPORTS"
        )
        session._run_query(
            f"CREATE TEMPORARY STAGE IF NOT EXISTS {import_stage_name}",
            is_ddl_on_temp_object=True,
        )
        import_stage = f"{STAGE_PREFIX}{import_stage_name}"
        session._conn.upload_file(
            _PDF_READER_FILE_PATH,
            import_stage,
            compress_data=False,
            overwrite=True,
            skip_upload_on_content_match=True,
        )
        python_file_path = f"{import_stage}/{os.path.basename(_PDF_READER_FILE_PATH)}"
    else:
        python_file_path = _PDF_READER_FILE_PATH

    _, input_types = get_types_from_type_hints(
        (_PDF_READER_FILE_PATH, _PDF_READER_HANDLER), TempObjectType.TABLE_FUNCTION
    )

    return session.udtf.register_from_file(
        python_file_path,
        _PDF_READER_HANDLER,
        name=session.get_fully_qualified_name_if_possible(_PDF_READER_UDTF_NAME),
        output_schema=StructType(
            [
                StructField(PAGE_INDEX_COLUMN, IntegerType(), True),
                StructField(TOTAL_PAGES_COLUMN, IntegerType(), True),
                StructField(CONTENT_COLUMN, StringType(), True),
                StructField(_UDTF_ERROR_COLUMN, StringType(), True),
            ]
        ),
        input_types=input_types,
        packages=["snowflake-snowpark-python", "pdfminer.six", "python-docx"],
        if_not_exists=True,
        skip_upload_on_content_match=True,
        _suppress_local_package_warnings=True,
    )


def register_extract_proc(session: "Session") -> str:
    if is_in_stored_procedure():  # pragma: no cover
        import_stage_name = session.get_fully_qualified_name_if_possible(
            "SNOWPARK_TEMP_STAGE_PDFIMPORTS"
        )
        session._run_query(
            f"CREATE TEMPORARY STAGE IF NOT EXISTS {import_stage_name}",
            is_ddl_on_temp_object=True,
        )
        import_stage = f"{STAGE_PREFIX}{import_stage_name}"
        session._conn.upload_file(
            _EXTRACT_PROC_FILE_PATH,
            import_stage,
            compress_data=False,
            overwrite=True,
            skip_upload_on_content_match=True,
        )
        python_file_path = f"{import_stage}/{os.path.basename(_EXTRACT_PROC_FILE_PATH)}"
    else:
        python_file_path = _EXTRACT_PROC_FILE_PATH

    name = session.get_fully_qualified_name_if_possible(_EXTRACT_PROC_NAME)
    session.sproc.register_from_file(
        python_file_path,
        _EXTRACT_PROC_HANDLER,
        return_type=StructType(
            [
                StructField("STAGE_PATH", StringType()),
                StructField("VALUE", StringType()),
                StructField("ERROR", StringType()),
                StructField("SCORES", StringType()),
            ]
        ),
        # In the order of run()'s parameters after the session, and of the args
        # extract_with_split_procedure() passes to the CALL.
        input_types=[
            StringType(),  # path
            StringType(),  # response_format: the JSON schema, as JSON
            StringType(),  # predicate: the prompt
            StringType(),  # models: candidates in order of preference, as JSON
            StringType(),  # call_budgets: as JSON
            IntegerType(),  # max_calls_per_file
            StringType(),  # work_stage: where page ranges are staged
        ],
        name=name,
        packages=["snowflake-snowpark-python", "pypdf"],
        if_not_exists=True,
        # Caller's rights: the proc reads the caller's stages and writes page parts to
        # the caller's session stage.
        execute_as="caller",
        skip_upload_on_content_match=True,
        _suppress_local_package_warnings=True,
    )
    return name


def register_error_guard_udf(session: "Session"):
    def raise_if_document_error(
        error: Optional[str], passthrough: Optional[str]
    ) -> Optional[str]:
        if error is not None:
            raise RuntimeError(f"Document processing failed: {error}")
        return passthrough

    return session.udf.register(
        raise_if_document_error,
        return_type=StringType(),
        input_types=[StringType(), StringType()],
        name=session.get_fully_qualified_name_if_possible(_ERROR_GUARD_UDF_NAME),
        if_not_exists=True,
    )
