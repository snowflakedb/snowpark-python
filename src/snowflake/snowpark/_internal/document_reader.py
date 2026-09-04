#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import os
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.document_reader_options import (
    CONTENT_COLUMN,
    DocumentReaderOptions,
    EXTRACTION_SCORES_COLUMN,
    IMAGES_COLUMN,
    PAGE_INDEX_COLUMN,
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
    array_size,
    coalesce,
    col,
    concat,
    flatten,
    listagg,
    lit,
    max as max_,
    object_construct_keep_null,
    to_file,
    try_parse_json,
    when,
)
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
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
    df = access(session, path)

    if options.parse_enabled:
        df = parse(session, df, options)

    if options.extract_enabled:
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


def access(session: "Session", path: str) -> "DataFrame":
    # The plain string path is kept as its own column because the PDF UDTF takes a
    # path string, not a FILE value.
    stage_path = concat(lit(STAGE_PREFIX), col('"name"'))
    return session.sql(f"LIST {path}").select(
        to_file(stage_path).alias(SOURCE_FILE_COLUMN),
        stage_path.alias(_STAGE_PATH_COLUMN),
    )


def parse(
    session: "Session", files_df: "DataFrame", options: DocumentReaderOptions
) -> "DataFrame":
    if options.parse_mode == "text":
        return parse_text(session, files_df, options)
    return parse_ai(files_df, options)


def parse_ai(files_df: "DataFrame", options: DocumentReaderOptions) -> "DataFrame":
    parse_kwargs: Dict[str, Any] = {
        "mode": options.parse_mode.upper(),
        "page_split": options.row_boundary == "page",
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


def extract(df: "DataFrame", options: DocumentReaderOptions) -> "DataFrame":
    input_col = (
        col(CONTENT_COLUMN) if options.parse_enabled else col(SOURCE_FILE_COLUMN)
    )
    if options.extraction_engine == "ai_complete":
        return extract_with_ai_complete(df, input_col, options)
    return extract_with_ai_extract(df, input_col, options)


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
    names = extraction.field_columns + [EXTRACTION_SCORES_COLUMN, _EXTRACT_ERROR_COLUMN]
    values = [response[field].cast(StringType()) for field in extraction.fields] + [
        extracted["scoring"],
        extracted["error"].cast(StringType()),
    ]
    return df.with_columns(names, values)


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
            return_error_details=True,
        )
    return ai_complete(
        model,
        options.prompt or _DEFAULT_EXTRACTION_PROMPT,
        file=input_col,
        response_format=response_format,
        return_error_details=True,
    )


def extract_with_ai_complete(
    df: "DataFrame", input_col: Column, options: DocumentReaderOptions
) -> "DataFrame":
    model = options.model or _DEFAULT_EXTRACTION_MODEL
    call = build_ai_complete_call(model, input_col, options)
    df = df.with_column(_EXTRACTED_COLUMN, as_variant(call))
    extracted = col(_EXTRACTED_COLUMN)
    value = extracted["value"]
    extraction = options.extraction
    names = extraction.field_columns + [_EXTRACT_ERROR_COLUMN]
    values = [value[field].cast(StringType()) for field in extraction.fields] + [
        extracted["error"].cast(StringType())
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
        tagged = [
            when(
                col(name).is_not_null(),
                object_construct_keep_null(
                    lit("stage"), lit(stage), lit("message"), col(name)
                ),
            )
            for stage, name in error_stages
        ]
        return df.with_column(
            quote_name_without_upper_casing(options.corrupt_record_column),
            coalesce(*tagged),
        )

    # FAILFAST: every AI call captures its errors in-band, so aborting the read takes
    # an explicit raise, anchored to a real returned column -- an unused/dropped one
    # could be optimized away and the check with it.
    anchor = (
        CONTENT_COLUMN if options.parse_enabled else options.extraction.field_columns[0]
    )
    guard = register_error_guard_udf(session)
    raw_error = coalesce(*[col(name) for _, name in error_stages])
    return df.with_column(anchor, guard(raw_error, col(anchor)))


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
        packages=["snowflake-snowpark-python", "pdfminer.six"],
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
