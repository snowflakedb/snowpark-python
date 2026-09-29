#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import dataclasses
import json
import os
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.document_reader_options import (
    CONTENT_COLUMN,
    DocumentReaderOptions,
    EXTRACTION_SCORES_COLUMN,
    ExtractionSpec,
    IMAGES_COLUMN,
    PAGE_INDEX_COLUMN,
    SOURCE_FILE_COLUMN,
    TOTAL_PAGES_COLUMN,
    _EXTRACT_ERROR_COLUMN,
    _PARSE_ERROR_COLUMN,
)
from snowflake.snowpark._internal.document_reader_heuristics import MAX_FILE_BYTES
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
    array_agg,
    array_compact,
    array_construct,
    array_flatten,
    array_size,
    array_to_string,
    coalesce,
    col,
    concat,
    flatten,
    get,
    iff,
    is_array,
    listagg,
    lit,
    max as max_,
    object_construct_keep_null,
    random,
    to_file,
    to_json,
    try_cast,
    try_parse_json,
    when,
)
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    VariantType,
)

if TYPE_CHECKING:  # pragma: no cover
    from snowflake.snowpark.dataframe import DataFrame
    from snowflake.snowpark.session import Session


_DEFAULT_EXTRACTION_MODEL = "claude-4-sonnet"
_DEFAULT_EXTRACTION_PROMPT = "Extract the requested fields from this document."

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
    df = build_extraction_plan(session, path, options)
    if escalation_eligible(options):
        df = escalate_failed_documents(session, path, options, df)
    elif collapse_inferred_page_rows(options):
        # Materialised first, for speed rather than correctness: grouping the live
        # parse->extract chain re-walks it for the grouped scan, measured at 312s
        # against 66s for the same aggregation over a cached plan on a W2-27
        # document. Same eager-execution tradeoff materialize_parsed_content() and
        # escalate_failed_documents() already accept, for a different reason.
        df = aggregate_extracted_pages(df.cache_result(), options)
        # The result is one row per document now, so output_columns() must stop
        # asking for PAGE_INDEX -- which it keys off row_boundary.
        options.row_boundary = "document"
    df = finalize_errors(session, df, options)
    return df.select(output_columns(options))


def collapse_inferred_page_rows(options: DocumentReaderOptions) -> bool:
    """True when the planner, not the caller, asked for page rows.

    A caller who sets row_boundary="page" wants one row per page and must get it
    -- that is the shape retrieval chunking is built on. But when
    infer_row_boundary() picks page rows on the caller's behalf for a
    document-level schema, returning N rows hands back something the caller never
    asked for and cannot use directly: each row carries only the fields its own
    page happened to show, so none of them is the answer and the answer is their
    union. Measured on W2-27, the one family in the short split where the planner
    infers page boundary: individual page rows score 0.59-0.79 while the union of
    them scores 0.77-0.88, so roughly 0.17 F1 per document was assembly work left
    to the caller. Mutually exclusive with escalation, which only runs when
    row_boundary is "document".
    """
    return (
        options.row_boundary_inferred and options.page_rows and options.extract_enabled
    )


def build_extraction_plan(
    session: "Session",
    path: str,
    options: DocumentReaderOptions,
    only_stage_paths: Optional[List[str]] = None,
) -> "DataFrame":
    df = access(session, path)
    if only_stage_paths:
        df = df.filter(col(_STAGE_PATH_COLUMN).isin(only_stage_paths))

    if options.parse_enabled:
        df = parse(session, df, options)

    if options.extract_enabled:
        df = extract(df, options)

    return df


def escalation_eligible(options: DocumentReaderOptions) -> bool:
    # There is no static signal in options/schema that predicts which documents
    # need page boundary instead of document boundary -- two ExtractBench
    # families carry byte-identical schema signatures yet have opposite
    # correct row_boundary answers, so shape alone can't drive this choice.
    # The only reliable signal is a document-boundary call that has already
    # failed, which means the decision to retry is made per-document, after
    # the fact, in escalate_failed_documents() below -- this predicate only
    # gates whether that retry machinery runs at all for a given read.
    return (
        options.extract_enabled
        and options.extraction_engine == "ai_complete"
        and options.row_boundary == "document"
        and options.page_filter is None
    )


def escalate_failed_documents(
    session: "Session",
    path: str,
    options: DocumentReaderOptions,
    plan: "DataFrame",
) -> "DataFrame":
    # Which documents failed can only be known by actually running the
    # document-boundary extraction, for the same reason escalation_eligible()
    # can't decide eligibility from schema/options alone: nothing upstream
    # predicts it. That forces this plan to execute here rather than staying
    # lazy until the caller's own collect() -- materialize_parsed_content()
    # (line 350) already accepts the identical eager-execution tradeoff, for
    # an unrelated compiler-bug workaround, and is the precedent for doing so
    # here too.
    cached = plan.cache_result()

    failed = col(_EXTRACT_ERROR_COLUMN).is_not_null()
    failing = [
        row[_STAGE_PATH_COLUMN]
        for row in cached.filter(failed).select(_STAGE_PATH_COLUMN).distinct().collect()
    ]
    if not failing:
        return cached

    # AI_PARSE_DOCUMENT has no page-split under parse_mode="none" to escalate
    # into, so a page retry needs a real parse phase; "layout" is the same
    # default parse_mode uses elsewhere in this file.
    page_options = dataclasses.replace(
        options,
        row_boundary="page",
        parse_mode="layout" if options.parse_mode == "none" else options.parse_mode,
    )
    page_plan = build_extraction_plan(
        session, path, page_options, only_stage_paths=failing
    )
    recovered = aggregate_extracted_pages(page_plan, options, cached)
    return cached.filter(~failed).union_all_by_name(recovered)


def aggregate_extracted_pages(
    page_plan: "DataFrame",
    options: DocumentReaderOptions,
    reference: Optional["DataFrame"] = None,
) -> "DataFrame":
    """Collapse page rows back to one row per document.

    ``reference`` is the DataFrame whose column list and types the result has to
    match. escalate_failed_documents() passes the cached document-boundary plan,
    because the two are union_all_by_name'd together and that only works on
    identical column lists. When the PLANNER chose page rows there is nothing to
    union against, so the page plan is its own reference -- and PAGE_INDEX is
    dropped, since a merged row has no single page index to report.
    """
    reference = page_plan if reference is None else reference
    drop_page_index = reference is page_plan
    schema_by_name = {
        field.name.upper(): field.datatype for field in reference.schema.fields
    }

    aggregations = []
    for field_column in options.extraction.field_columns:
        value = col(field_column)
        # Whether a field is an array can't be read off the declared schema --
        # schema preparation already stringifies arrays, so the declared type
        # is "string" either way -- so this checks the actual per-page value
        # at runtime instead. Pages that returned an array are concatenated in
        # page order; ARRAY_AGG drops NULLs, so a page whose value wasn't
        # itself an array (parsed_arrays is NULL for that row) simply isn't
        # counted as an array page. If no page produced an array at all, this
        # field never held one, and the first non-null page-order value wins.
        parsed_arrays = array_agg(
            iff(
                is_array(try_parse_json(value.cast(StringType()))),
                try_parse_json(value.cast(StringType())),
                lit(None),
            )
        ).within_group(col(PAGE_INDEX_COLUMN).asc())
        first_non_null = get(
            array_agg(value).within_group(col(PAGE_INDEX_COLUMN).asc()),
            lit(0),
        )
        aggregated = iff(
            array_size(parsed_arrays) > 0,
            to_json(array_flatten(parsed_arrays)),
            first_non_null,
        )
        target_type = schema_by_name.get(field_column.upper())
        if isinstance(target_type, VariantType):
            # VARIANT has no direct CAST target the way scalar types do (see
            # cast_extracted_field above) -- round-tripping through
            # try_parse_json(...cast(StringType())) is the same workaround
            # used there.
            aggregated = try_parse_json(aggregated.cast(StringType()))
        elif target_type is not None:
            aggregated = aggregated.cast(target_type)
        aggregations.append(aggregated.alias(field_column))

    aggregations.append(max_(col(_EXTRACT_ERROR_COLUMN)).alias(_EXTRACT_ERROR_COLUMN))
    if _PARSE_ERROR_COLUMN in reference.columns:
        aggregations.append(max_(col(_PARSE_ERROR_COLUMN)).alias(_PARSE_ERROR_COLUMN))
    if CONTENT_COLUMN in reference.columns:
        aggregations.append(
            listagg(col(CONTENT_COLUMN), PAGE_SEPARATOR)
            .within_group(col(PAGE_INDEX_COLUMN).asc())
            .alias(CONTENT_COLUMN)
        )
    if TOTAL_PAGES_COLUMN in reference.columns:
        aggregations.append(max_(col(TOTAL_PAGES_COLUMN)).alias(TOTAL_PAGES_COLUMN))
    if IMAGES_COLUMN in reference.columns:
        aggregations.append(any_value(col(IMAGES_COLUMN)).alias(IMAGES_COLUMN))
    if _EXTRACTED_COLUMN in reference.columns:
        # The raw AI_COMPLETE response blob outlives materialize_parsed_content
        # (it's added afterwards, by extract_with_ai_complete) and is never
        # dropped before this point, even though every field this read cares
        # about has already been pulled out of it into its own column above.
        # output_columns() never selects it either, so which page's copy wins
        # is immaterial -- it only needs to exist so the union_all_by_name in
        # escalate_failed_documents() below sees a matching column list.
        aggregations.append(any_value(col(_EXTRACTED_COLUMN)).alias(_EXTRACTED_COLUMN))

    # SOURCE_FILE is rebuilt rather than aggregated because FILE is not a
    # groupable type -- same precedent as aggregate_pages() above.
    aggregated_df = (
        page_plan.group_by(_STAGE_PATH_COLUMN)
        .agg(*aggregations)
        .with_column(SOURCE_FILE_COLUMN, to_file(col(_STAGE_PATH_COLUMN)))
    )
    out_columns = [
        name
        for name in reference.columns
        if not (drop_page_index and name == PAGE_INDEX_COLUMN)
    ]
    return aggregated_df.select([col(name) for name in out_columns])


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
        if options.extraction_engine == "ai_extract":
            names.append(EXTRACTION_SCORES_COLUMN)
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


def list_stage_file_stats(session: "Session", path: str) -> List[Tuple[str, int]]:
    """(name, size_bytes) from LIST, used by the smart-reader planner."""
    rows = session.sql(f"LIST {path}").collect()
    files: List[Tuple[str, int]] = []
    for row in rows:
        data = row.as_dict() if hasattr(row, "as_dict") else {}
        name = data.get("name", data.get("NAME", row[0] if row else ""))
        size = data.get("size", data.get("SIZE", row[1] if len(row) > 1 else 0))
        try:
            size_int = int(size or 0)
        except (TypeError, ValueError):
            size_int = 0
        files.append((str(name or ""), size_int))
    return files


def access(session: "Session", path: str) -> "DataFrame":
    # The plain string path is kept as its own column because the PDF UDTF takes a
    # path string, not a FILE value.
    stage_path = concat(lit(STAGE_PREFIX), col('"name"'))
    listed = session.sql(f"LIST {path}").filter(
        col('"size"').is_null() | (col('"size"') <= lit(MAX_FILE_BYTES))
    )
    files_df = listed.select(
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
    extract_kwargs: Dict[str, Any] = {
        "response_format": extraction.ai_extract_format,
        "scores": True,
    }
    if options.extract_scale_factor and options.extract_scale_factor != 1.0:
        extract_kwargs["config"] = {"scale_factor": options.extract_scale_factor}
    df = df.with_column(
        _EXTRACTED_COLUMN,
        as_variant(ai_extract(input_col, **extract_kwargs)),
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


# AI_COMPLETE defaults max_tokens to 4096. Extraction output scales with the number of
# cells requested, and a truncated response is discarded whole -- Cortex reports
# "unexpected end of JSON input" (or, on some models, a bare "internal error"), losing
# every correctly read field along with the overflow. Asking for more costs nothing when
# the answer is short: this is a ceiling, not a request.
#
# 8192 IS NOT THE MAXIMUM -- 128000 is. Snowflake documents the 4096 default and no
# maximum, but the server states the real ceiling when exceeded: asking for 262144 returns
# "max_tokens parameter exceeds the maximum possible value (128000)".
#
# Raised 8192 -> 65536. A ceiling is not a request: a short answer bills the same at 65536
# as at 8192, so this costs nothing on documents that already worked. On a 6-page table
# that failed outright at 8192 it returns 333 rows, and it
# was the sole cause of 14 of 20 long-split extraction failures.
#
# ⚠️ Verified on claude-opus-5 only, NOT on _DEFAULT_EXTRACTION_MODEL. A bigger ceiling
# also makes a doomed call SLOWER rather than successful -- a whole-document call at
# 128000 ran 1h45m without returning while a 3-part split of the same document finished in
# ~1400s. Raising this does not remove the need to split very large documents; the reader
# has no splitting, so documents whose output exceeds even 65536 tokens still fail.
_COMPLETE_MAX_OUTPUT_TOKENS = 65536
_COMPLETE_MODEL_PARAMETERS = {
    "temperature": 0,
    "max_tokens": _COMPLETE_MAX_OUTPUT_TOKENS,
}


def build_ai_complete_call(
    model: str, input_col: Column, options: DocumentReaderOptions
) -> Column:
    # Parse ran: input_col is parsed CONTENT text, folded into AI_COMPLETE's
    # single-string variant. Parse didn't run: input_col is SOURCE_FILE, passed to
    # AI_COMPLETE's file variant instead -- whose prompt argument must then be a plain
    # string, not a Column, so there is no CONTENT to fold in.
    response_format = options.extraction.ai_complete_format
    if options.parse_enabled:
        prompt_text = (
            input_col
            if options.prompt is None
            else concat(lit(options.prompt), lit("\n\n"), input_col)
        )
        return ai_complete(
            model,
            prompt_text,
            response_format=response_format,
            model_parameters=_COMPLETE_MODEL_PARAMETERS,
            return_error_details=True,
        )
    return ai_complete(
        model,
        options.prompt or _DEFAULT_EXTRACTION_PROMPT,
        file=input_col,
        response_format=response_format,
        model_parameters=_COMPLETE_MODEL_PARAMETERS,
        return_error_details=True,
    )


def extract_with_ai_complete(
    df: "DataFrame", input_col: Column, options: DocumentReaderOptions
) -> "DataFrame":
    model = options.model or _DEFAULT_EXTRACTION_MODEL
    call = build_ai_complete_call(model, input_col, options)
    # Unlike AI_PARSE_DOCUMENT/AI_EXTRACT, AI_COMPLETE(..., return_error_details=True)
    # returns a native OBJECT rather than a JSON string -- CAST(... AS VARCHAR), which
    # as_variant() always applies, fails to compile for it. Bracket access works
    # directly on the raw result, so it skips as_variant() here.
    df = df.with_column(_EXTRACTED_COLUMN, call)
    extracted = col(_EXTRACTED_COLUMN)
    value = extracted["value"]
    extraction = options.extraction
    cast_results = [
        cast_extracted_field(value[field], field, extraction)
        for field in extraction.fields
    ]
    names = extraction.field_columns + [_EXTRACT_ERROR_COLUMN]
    values = [column for column, _ in cast_results] + [
        merge_extract_error(
            extracted["error"],
            extraction.field_columns,
            [mismatch for _, mismatch in cast_results],
        )
    ]
    return df.with_columns(names, values)


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
    anchor = (
        CONTENT_COLUMN if options.parse_enabled else options.extraction.field_columns[0]
    )
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
