#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Planner for the document reader: fill unset pipeline options and reshape
schemas to match published Cortex AI_EXTRACT / AI_PARSE_DOCUMENT practices.

Explicit DataFrameReader options always win. See
https://docs.snowflake.com/en/user-guide/snowflake-cortex/document-extraction
https://docs.snowflake.com/en/user-guide/snowflake-cortex/parse-document
"""

from __future__ import annotations

import copy
import os
import re
from typing import Any, Iterable, Sequence

from snowflake.snowpark._internal.document_reader_options import (
    DocumentReaderOptions,
    ExtractionSpec,
)

# Cortex PARSE/EXTRACT reject files larger than 100 MB.
MAX_FILE_BYTES = 100 * 1024 * 1024
# Cheap LIST-size proxy for scanned PDFs (parse-doc: OCR for scans).
SCANNED_PDF_BYTES = 1_000_000
EXTRACT_SCANNED_SCALE = 2.0

_NATIVE_TEXT_EXTS = frozenset({".txt", ".text", ".md", ".html", ".htm"})
_IMAGE_EXTS = frozenset(
    {".png", ".jpeg", ".jpg", ".tif", ".tiff", ".bmp", ".gif", ".webp"}
)
_STRUCTURE_TOKENS = frozenset(
    {
        "table",
        "tables",
        "figure",
        "figures",
        "exhibit",
        "layout",
        "markdown",
        "heading",
        "diagram",
        "chart",
        "has_table",
        "has_figure",
        "tables_on_page",
        "figures_on_page",
    }
)
_IMAGE_TOKENS = frozenset(
    {"signature", "stamp", "logo", "figure", "checkbox", "handwriting", "image"}
)
_PAGE_SCOPE_TOKENS = frozenset(
    {
        "page",
        "on_page",
        "tables_on_page",
        "figures_on_page",
        "has_table",
        "has_figure",
        "header",
        "footer",
    }
)
_DOC_SCOPE_TOKENS = frozenset(
    {
        "vendor",
        "vendor_name",
        "start_date",
        "end_date",
        "termination",
        "termination_clause",
        "contract",
        "contract_category",
        "renewal",
        "customer_id",
        "invoice_number",
        "total_amount",
        "party",
        "agreement",
        "license",
    }
)
_COMPLETE_TOKENS = frozenset(
    {
        "summary",
        "summarize",
        "overview",
        "explanation",
        "rationale",
        "assessment",
        "analysis",
        "recommendation",
        "takeaway",
    }
)
_COMPLETE_PHRASES = ("sentence", "in your own words", "explain", "summar")


def smart_enabled(cur_options: dict[str, Any]) -> bool:
    """SMART defaults on; `.option("smart", False)` disables inference."""
    if "SMART" not in cur_options:
        return True
    return str(cur_options["SMART"]).lower() not in {"false", "0", "no", "off"}


def schema_properties(schema: Any) -> dict[str, Any]:
    if not isinstance(schema, dict):
        return {}
    if isinstance(schema.get("properties"), dict):
        return schema["properties"]
    inner = schema.get("schema")
    if isinstance(inner, dict) and isinstance(inner.get("properties"), dict):
        return inner["properties"]
    return {}


def is_table_property(defn: Any) -> bool:
    if not isinstance(defn, dict) or defn.get("type") != "object":
        return False
    nested = defn.get("properties") or {}
    if not nested:
        return False
    return all(
        isinstance(col_def, dict) and col_def.get("type") == "array"
        for col_def in nested.values()
    )


def _token_blob(schema: Any, prompt: str = "") -> str:
    parts = [prompt or ""]
    for name, defn in schema_properties(schema).items():
        parts.append(str(name))
        if isinstance(defn, dict):
            parts.append(str(defn.get("description", "")))
        elif isinstance(defn, str):
            parts.append(defn)
    return " ".join(parts).lower().replace("-", "_")


def needs_structure(schema: Any, prompt: str = "") -> bool:
    blob = _token_blob(schema, prompt)
    return any(token in blob for token in _STRUCTURE_TOKENS)


def needs_images(schema: Any, prompt: str = "") -> bool:
    blob = _token_blob(schema, prompt)
    return any(token in blob for token in _IMAGE_TOKENS)


def infer_row_boundary(schema: Any) -> str:
    """Page rows for page-local or table schemas; else one row per file.

    Table extraction over 4096 tokens should be split per page
    (AI_EXTRACT table guidance).
    """
    props = schema_properties(schema)
    if not props:
        return "document"
    names = " ".join(props).lower().replace("-", "_")
    if any(token in names for token in _PAGE_SCOPE_TOKENS):
        return "page"
    if any(is_table_property(defn) for defn in props.values()):
        return "page"
    if any(token in names for token in _DOC_SCOPE_TOKENS):
        return "document"
    return "document"


def looks_generative(schema: Any) -> bool:
    for name, defn in schema_properties(schema).items():
        lname = name.lower().replace("-", "_")
        desc = ""
        if isinstance(defn, dict):
            desc = str(defn.get("description") or "").lower()
        elif isinstance(defn, str):
            desc = defn.lower()
        tokens = set(lname.split("_"))
        if tokens & _COMPLETE_TOKENS or any(t in lname for t in _COMPLETE_TOKENS):
            return True
        if any(p in f"{lname} {desc}" for p in _COMPLETE_PHRASES):
            return True
    # Flat {field: question} schemas: only the values are questions.
    if isinstance(schema, dict) and not schema_properties(schema):
        for key, value in schema.items():
            lname = str(key).lower()
            desc = str(value).lower() if isinstance(value, str) else ""
            if any(t in lname for t in _COMPLETE_TOKENS):
                return True
            if any(p in desc for p in _COMPLETE_PHRASES):
                return True
    return False


def skip_parse_for_extract(schema: Any, prompt: str = "") -> bool:
    """True when AI_EXTRACT should run on the FILE (no AI_PARSE_DOCUMENT)."""
    if not schema:
        return False
    if needs_images(schema, prompt):
        return False
    if looks_generative(schema):
        return False
    if infer_row_boundary(schema) == "page":
        return False
    return True


def humanize_field_name(name: str) -> str:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name or "")
    spaced = spaced.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", spaced).strip().lower()


def title_case_header(name: str) -> str:
    return " ".join(part.capitalize() for part in humanize_field_name(name).split())


def question_for_field(name: str, defn: dict[str, Any] | None = None) -> str:
    """Specific, single-value English question (AI_EXTRACT question guidance)."""
    defn = defn or {}
    desc = str(defn.get("description") or "").strip()
    ftype = str(defn.get("type") or "string").lower()
    human = humanize_field_name(name) or "value"
    desc_l = desc.lower()
    looks_like_question = desc.endswith("?") or desc_l.startswith(
        ("what ", "who ", "which ", "is ", "are ", "list:", "does ", "do ")
    )
    if looks_like_question or (desc and len(desc) > 60):
        question = desc
    elif name.lower() == "date" or human == "date":
        question = (
            f"What is the {desc}?"
            if desc
            else "What is the primary date of this document (the agreement or issuing date)?"
        )
    elif desc:
        question = f"What is the {human}?"
        if desc.lower() not in human:
            question = f"{question} ({desc})"
    else:
        question = f"What is the {human}?"
    if ftype == "array" and not question.lower().startswith("list:"):
        if question.lower().startswith("what is "):
            question = "What are " + question[len("What is ") :]
        question = f"List: {question}"
    return question


def rewrite_response_format(schema: Any, prompt: str = "") -> Any:
    """Codify AI_EXTRACT question and table practices without renaming fields."""
    if not schema:
        return schema
    if isinstance(schema, list):
        return schema
    if not isinstance(schema, dict):
        return schema
    fmt = copy.deepcopy(schema)
    props = schema_properties(fmt)
    if not props:
        # Flat {name: prompt} map.
        for key, value in list(fmt.items()):
            if isinstance(value, str) and not value.strip().endswith("?"):
                fmt[key] = question_for_field(str(key), {"description": value})
        return fmt

    context = (prompt or "").strip()
    for name, defn in list(props.items()):
        if not isinstance(defn, dict):
            continue
        if is_table_property(defn):
            nested = defn.get("properties") or {}
            if "column_ordering" not in defn:
                defn["column_ordering"] = list(nested.keys())
            if not str(defn.get("description") or "").strip():
                title = title_case_header(name)
                defn["description"] = (
                    f"{title}. {context}".strip() if context else title
                )
            for col_name, col_def in nested.items():
                if not isinstance(col_def, dict):
                    continue
                col_desc = str(col_def.get("description") or "").strip()
                if not col_desc or "_" in col_desc or col_desc == col_name:
                    col_def["description"] = title_case_header(col_name)
            continue
        question = question_for_field(name, defn)
        if context and context.lower() not in question.lower():
            question = f"{question} Context: {context}"
        defn["description"] = question
    return fmt


def plan_parse_mode(
    files: Sequence[tuple[str, int]],
    *,
    schema: Any = None,
    prompt: str = "",
    extract_images: bool = False,
) -> str:
    """Choose none | ocr | layout | text from file list + schema.

    LAYOUT is the parse-doc default for complex docs; OCR for scans/images;
    none when EXTRACT can read the FILE directly.
    """
    if extract_images or needs_images(schema, prompt):
        return "layout"
    if skip_parse_for_extract(schema, prompt):
        return "none"
    if needs_structure(schema, prompt):
        return "layout"
    if not files:
        return "layout"
    exts = [os.path.splitext(name)[1].lower() for name, _ in files]
    sizes = [size for _, size in files]
    if exts and all(ext in _NATIVE_TEXT_EXTS for ext in exts) and schema:
        return "none" if skip_parse_for_extract(schema, prompt) else "text"
    if any(ext in _IMAGE_EXTS for ext in exts):
        return "ocr"
    if any(
        ext == ".pdf" and size >= SCANNED_PDF_BYTES for ext, size in zip(exts, sizes)
    ):
        return "ocr"
    return "layout"


def plan_extract_scale(files: Sequence[tuple[str, int]]) -> float:
    scale = 1.0
    for name, size in files:
        ext = os.path.splitext(name)[1].lower()
        if ext in _IMAGE_EXTS or size >= SCANNED_PDF_BYTES:
            scale = max(scale, EXTRACT_SCANNED_SCALE)
    return scale


def plan_extraction_engine(schema: Any) -> str:
    return "ai_complete" if looks_generative(schema) else "ai_extract"


def apply_document_heuristics(
    options: DocumentReaderOptions,
    explicit: Iterable[str],
    *,
    schema: Any = None,
    prompt: str = "",
    files: Sequence[tuple[str, int]] | None = None,
) -> DocumentReaderOptions:
    """Fill unset DocumentReaderOptions. ``explicit`` is upper-case option keys."""
    explicit_keys = {str(k).upper() for k in explicit}
    files = list(files or [])
    schema = schema
    prompt = prompt or (options.prompt or "")

    if schema and "SCHEMA" in explicit_keys:
        rewritten = rewrite_response_format(schema, prompt)
        options.extraction = ExtractionSpec.from_response_format(rewritten)

    if "ROW_BOUNDARY" not in explicit_keys and schema:
        options.row_boundary = infer_row_boundary(schema)

    if "EXTRACT_IMAGES" not in explicit_keys and needs_images(schema, prompt):
        options.extract_images = True

    if "EXTRACTION_ENGINE" not in explicit_keys and schema:
        options.extraction_engine = plan_extraction_engine(schema)

    if "PARSE_MODE" not in explicit_keys:
        options.parse_mode = plan_parse_mode(
            files,
            schema=schema,
            prompt=prompt,
            extract_images=options.extract_images,
        )

    if (
        "EXTRACT_SCALE_FACTOR" not in explicit_keys
        and options.extract_enabled
        and options.extraction_engine == "ai_extract"
    ):
        options.extract_scale_factor = plan_extract_scale(files)

    options.validate()
    return options
