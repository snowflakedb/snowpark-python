#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Stored-procedure handler for document extraction that splits a PDF when the answer
does not fit in one AI_COMPLETE response.

Uploaded and registered as a standalone file, so it imports nothing from snowpark's
internals. Every function except ``run`` is pure and unit-tested directly.

Each AI_COMPLETE call is asked to fill an ``extraction_status`` block first: whether
its answer is complete, and if not, the last page it covered and how to split the
pages it did not. Pages are always 1-based positions in the original document.
"""

import hashlib
import io
import json
import os
from typing import Any, Dict, List, Optional, Tuple

from snowflake.snowpark import DataFrame, Session

STATUS_FIELD = "extraction_status"

_STATUS_SCHEMA = {
    "type": "object",
    "description": (
        "Fill this FIRST, before extracting anything else. Describes how much of the "
        "document this response covers."
    ),
    "required": [
        "complete",
        "estimated_total_output_tokens",
        "last_pdf_page_extracted",
        "remaining_page_ranges",
    ],
    "properties": {
        "complete": {
            "type": "boolean",
            "description": "true if every requested value from every assigned page is in this response.",
        },
        "estimated_total_output_tokens": {
            "type": "integer",
            "description": "Estimated output tokens for the COMPLETE answer over all assigned pages.",
        },
        "last_pdf_page_extracted": {
            "type": "integer",
            "description": "1-based position within this file of the last page fully included.",
        },
        "remaining_page_ranges": {
            "type": "array",
            "description": "If complete is false, the remaining assigned pages split into consecutive ranges.",
            "items": {
                "type": "object",
                "required": ["start_pdf_page", "end_pdf_page"],
                "properties": {
                    "start_pdf_page": {"type": "integer"},
                    "end_pdf_page": {"type": "integer"},
                    "estimated_output_tokens": {"type": "integer"},
                },
            },
        },
    },
}


# Keywords Cortex json mode rejects or enforces in ways that discard a whole response.
# ``required`` is enforced as non-null, which a page range lacking some header field always
# fails; ``additionalProperties: false`` rejects the envelope opus-5 sometimes wraps its
# answer in, which unwrap_envelope() would otherwise recover.
_UNSUPPORTED_KEYWORDS = frozenset(
    {
        "required",
        "additionalProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
        "default",
        "$schema",
        "$id",
        "$comment",
        "examples",
        "readOnly",
        "writeOnly",
        "deprecated",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "pattern",
        "format",
        "const",
        "if",
        "then",
        "else",
        "dependentSchemas",
        "dependentRequired",
        "prefixItems",
    }
)


# Keys of these map field/definition names to subschemas, so their own keys are names,
# not keywords, and must never be filtered.
_NAME_MAPS = frozenset({"properties", "$defs", "definitions", "patternProperties"})


def drop_unsupported(schema: Any) -> Any:
    if isinstance(schema, list):
        return [drop_unsupported(v) for v in schema]
    if not isinstance(schema, dict):
        return schema
    out = {}
    for key, value in schema.items():
        if key in _UNSUPPORTED_KEYWORDS:
            continue
        if key in _NAME_MAPS and isinstance(value, dict):
            out[key] = {name: drop_unsupported(sub) for name, sub in value.items()}
        else:
            out[key] = drop_unsupported(value)
    return out


def is_splittable_error(error: str) -> bool:
    """Errors that fewer pages can fix: a response cut off at max_tokens, or an input
    Cortex rejects as too large -- which claude-opus-5 reports only as "malformed
    request", for scanned PDFs well under the documented 22 MB."""
    return "unexpected end of JSON input" in error or "malformed request" in error


# The largest scanned PDF claude-opus-5 accepted as one FILE was 12.63 MB; a 15.19 MB one
# was rejected every time.
MAX_FILE_BYTES = 12_000_000


def initial_ranges(
    total_pages: int, file_bytes: int, max_bytes: int = MAX_FILE_BYTES
) -> List[Tuple[int, int]]:
    """Page ranges for the first round: the whole file when it fits in one call,
    otherwise even ranges sized from the average page size. Each range after the first
    also carries one context page, so it is planned one page short."""
    if file_bytes <= max_bytes or total_pages <= 1:
        return [(1, total_pages)]
    pages_per_range = max(1, int(max_bytes / (file_bytes / total_pages)) - 1)
    return [
        (start, min(start + pages_per_range - 1, total_pages))
        for start in range(1, total_pages + 1, pages_per_range)
    ]


def nullable_root_scalars(properties: Dict[str, Any]) -> Dict[str, Any]:
    """A call covering only some pages cannot see a value printed elsewhere (a total on
    the last page); with a non-null scalar type the model writes a placeholder such as
    '<UNKNOWN>' into a number field and json mode rejects the whole response."""
    out = {}
    for name, prop in properties.items():
        json_type = prop.get("type") if isinstance(prop, dict) else None
        if isinstance(json_type, str) and json_type not in ("object", "array", "null"):
            prop = {**prop, "type": [json_type, "null"]}
        out[name] = prop
    return out


def with_status(response_format: Dict[str, Any]) -> Dict[str, Any]:
    schema = response_format["schema"]
    wrapped = dict(schema)
    wrapped["properties"] = {
        STATUS_FIELD: _STATUS_SCHEMA,
        **nullable_root_scalars(schema.get("properties", {})),
    }
    return {**response_format, "schema": drop_unsupported(wrapped)}


def build_predicate(
    base: str, target: int, start: int, end: int, total_pages: int
) -> str:
    if start == 1 and end == total_pages:
        scope = (
            f"This file is the whole document: document pages 1 to {total_pages}. "
            "Page numbers in extraction_status are 1-based PDF page positions, not the "
            "page numbers printed on the pages."
        )
    elif start == 1:
        scope = (
            f"This file is the first {end} pages of a longer document. Page numbers in "
            "extraction_status are 1-based positions within THIS file, not the page "
            "numbers printed on the pages."
        )
    else:
        # The context page is described only by its position: page numbers printed on
        # the pages can differ from positions, and a number here gets confused with them.
        scope = (
            f"This file is {end - start + 2} consecutive pages from the middle of a longer "
            "document. The FIRST page of this file is context only: do NOT extract any rows "
            "or values from it; use it only to carry forward values that continue onto the "
            "next page (such as a group label printed only on a group's first row). "
            "Extract from the SECOND page of this file onward. Page numbers in "
            "extraction_status are 1-based positions within THIS file (the context page "
            "is position 1), not the page numbers printed on the pages."
        )
    return (
        f"{base}\n\n{scope}\n\n"
        "If a requested single value is not shown on the pages you must extract, return "
        "null for it; never write a placeholder such as '<UNKNOWN>' or 'N/A'.\n\n"
        "Your output is limited, so first estimate how many output tokens the COMPLETE "
        "answer for the pages you must extract would need (count the table rows per page and the "
        f"size of each row as JSON). If that estimate is {target} tokens or less, extract "
        "everything and set extraction_status.complete = true. If it is more, extract only "
        "from the first page you must extract up to the last whole page that keeps this response "
        f"under {target} output tokens, never stopping in the middle of a page. Set "
        "complete = false, set last_pdf_page_extracted to that page, and split the "
        "remaining pages of this file into consecutive remaining_page_ranges that are each "
        f"expected to need under {target} output tokens. Always fill extraction_status first."
    )


def unwrap_envelope(value: Any, schema: Dict[str, Any]) -> Any:
    """claude-opus-5 sometimes nests its whole answer under one undeclared key such as
    ``{"input": {...}}``. json mode accepts it because ``additionalProperties`` cannot be
    sent, and every real field then reads as missing."""
    declared = set((schema.get("properties") or {}).keys())
    if not isinstance(value, dict) or not declared:
        return value
    top = sum(1 for k in declared if not _is_absent(value.get(k)))
    best, best_count = value, top
    for key, inner in value.items():
        if key in declared or not isinstance(inner, dict):
            continue
        count = sum(1 for k in declared if k in inner)
        if count > best_count:
            best, best_count = inner, count
    return best


def file_page_offset(start: int) -> int:
    """Document page = offset + position in the part file; parts after the first carry
    the preceding page as context at position 1."""
    return start - 2 if start > 1 else 0


def _to_document_page(page: Any, start: int, end: int) -> Optional[int]:
    if not isinstance(page, int) or isinstance(page, bool):
        return None
    document_page = page + file_page_offset(start)
    if start - 1 <= document_page <= end:
        return document_page
    return None


def next_ranges(
    start: int, end: int, value: Optional[Dict[str, Any]], error: Optional[str]
) -> Tuple[bool, List[Tuple[int, int]]]:
    """Returns (keep_value, ranges_still_to_extract) for one finished call."""
    if error is not None or not isinstance(value, dict):
        if end > start and (error is None or is_splittable_error(error)):
            mid = (start + end) // 2
            return False, [(start, mid), (mid + 1, end)]
        return False, []
    status = value.get(STATUS_FIELD)
    if not isinstance(status, dict) or status.get("complete") is not False:
        return True, []
    last = _to_document_page(status.get("last_pdf_page_extracted"), start, end)
    if last is None or last < start:
        # No usable progress: re-asking for the same pages would loop, so halve them.
        if end > start:
            mid = (start + end) // 2
            return False, [(start, mid), (mid + 1, end)]
        return True, []
    if last >= end:
        return True, []
    ranges = []
    for planned in status.get("remaining_page_ranges") or []:
        if not isinstance(planned, dict):
            continue
        a = _to_document_page(planned.get("start_pdf_page"), start, end)
        b = _to_document_page(planned.get("end_pdf_page"), start, end)
        if a is None or b is None:
            continue
        a, b = max(a, last + 1), min(b, end)
        if a <= b:
            ranges.append((a, b))
    return True, _cover(sorted(ranges), last + 1, end)


def _cover(
    ranges: List[Tuple[int, int]], first: int, last: int
) -> List[Tuple[int, int]]:
    """Make ranges consecutive, non-overlapping and exactly cover [first, last]."""
    covered: List[Tuple[int, int]] = []
    cursor = first
    for a, b in ranges:
        if b < cursor:
            continue
        if a > cursor:
            covered.append((cursor, a - 1))
        a = max(a, cursor)
        covered.append((a, b))
        cursor = b + 1
    if cursor <= last:
        covered.append((cursor, last))
    return covered


def _is_absent(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def merge_values(parts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge per-range answers given in page order: arrays concatenate, objects merge
    field by field, scalars keep the first present value."""

    def merge(a: Any, b: Any) -> Any:
        if _is_absent(a):
            return b
        if _is_absent(b):
            return a
        if isinstance(a, list) and isinstance(b, list):
            return a + b
        if isinstance(a, dict) and isinstance(b, dict):
            out = dict(a)
            for key, value in b.items():
                out[key] = merge(out.get(key), value)
            return out
        return a

    merged: Dict[str, Any] = {}
    for part in parts:
        part = {k: v for k, v in part.items() if k != STATUS_FIELD}
        merged = merge(merged, part)
    return merged


def qualify_listed_name(path: str, listed_name: str) -> str:
    """LIST returns '<stage name>/<relative path>' without database or schema; rebuild a
    reference that resolves regardless of the session's current schema."""
    stage = path.lstrip("@").split("/", 1)[0]
    relative = listed_name.split("/", 1)[1] if "/" in listed_name else listed_name
    return f"@{stage}/{relative}"


_AI_COMPLETE_SQL = (
    "SELECT AI_COMPLETE(model => ?, predicate => ?, file => TO_FILE(?), "
    "model_parameters => PARSE_JSON(?), response_format => PARSE_JSON(?), "
    "return_error_details => TRUE)"
)


def run(
    session: Session,
    path: str,
    response_format: str,
    predicate: str,
    model: str,
    model_parameters: str,
    output_token_target: int,
    max_calls_per_file: int,
    work_stage: str,
) -> DataFrame:  # pragma: no cover - exercised against a live account
    from pypdf import PdfReader, PdfWriter
    from snowflake.snowpark.files import SnowflakeFile
    from snowflake.snowpark.types import StringType, StructField, StructType

    sent_format = with_status(json.loads(response_format))
    format_with_status = json.dumps(sent_format)
    files = [
        qualify_listed_name(path, row["name"])
        for row in session.sql(f"LIST {path}").collect()
    ]

    readers: Dict[str, Any] = {}
    totals: Dict[str, int] = {}
    sizes: Dict[str, int] = {}
    errors: Dict[str, List[str]] = {f: [] for f in files}
    for file in files:
        with SnowflakeFile.open(file, "rb", require_scoped_url=False) as f:
            content = f.read()
        sizes[file] = len(content)
        try:
            readers[file] = PdfReader(io.BytesIO(content))
            totals[file] = len(readers[file].pages)
        except Exception:  # not a readable PDF: extract it whole, never split
            readers[file] = None
            totals[file] = 1

    def part_file(file: str, start: int, end: int) -> Optional[str]:
        """Stage the pages as their own PDF; None when the part is still too large."""
        if start == 1 and end == totals[file] and sizes[file] <= MAX_FILE_BYTES:
            return file
        if readers[file] is None:
            return file
        writer = PdfWriter()
        for index in range(max(start - 2, 0), end):
            writer.add_page(readers[file].pages[index])
        buffer = io.BytesIO()
        writer.write(buffer)
        if buffer.tell() > MAX_FILE_BYTES and end > start:
            return None
        buffer.seek(0)
        stem = os.path.splitext(os.path.basename(file))[0]
        digest = hashlib.md5(file.encode()).hexdigest()[:12]
        target = f"{work_stage}/document_parts/{digest}/{stem}_p{start}_{end}.pdf"
        session.file.put_stream(buffer, target, auto_compress=False, overwrite=True)
        return target

    calls = {f: 0 for f in files}
    done: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {f: [] for f in files}
    pending = [
        (f, a, b)
        for f in files
        for a, b in (
            initial_ranges(totals[f], sizes[f]) if readers[f] is not None else [(1, 1)]
        )
    ]
    while pending:
        jobs = []
        oversized = []
        for file, start, end in pending:
            if calls[file] >= max_calls_per_file:
                errors[file].append(
                    f"pages {start}-{end} not extracted: reached {max_calls_per_file} calls"
                )
                continue
            staged = part_file(file, start, end)
            if staged is None:
                mid = (start + end) // 2
                oversized.extend([(file, start, mid), (file, mid + 1, end)])
                continue
            calls[file] += 1
            params = [
                model,
                build_predicate(
                    predicate, output_token_target, start, end, totals[file]
                ),
                staged,
                model_parameters,
                format_with_status,
            ]
            jobs.append(
                (
                    file,
                    start,
                    end,
                    session.sql(_AI_COMPLETE_SQL, params=params).collect_nowait(),
                )
            )
        pending = oversized
        for file, start, end, job in jobs:
            try:
                raw = job.result()[0][0]
                result = json.loads(raw) if isinstance(raw, str) else raw
                value, error = result.get("value"), result.get("error")
                value = unwrap_envelope(value, sent_format["schema"])
            except Exception as exc:
                value, error = None, str(exc)
            if readers[file] is None:
                if error is not None:
                    errors[file].append(error)
                else:
                    done[file].append((start, value))
                continue
            keep, remaining = next_ranges(start, end, value, error)
            if keep:
                done[file].append((start, value))
            elif not remaining:
                errors[file].append(f"pages {start}-{end}: {error}")
            pending.extend((file, a, b) for a, b in remaining)

    rows = []
    for file in files:
        parts = [value for _, value in sorted(done[file], key=lambda item: item[0])]
        rows.append(
            [
                file,
                json.dumps(merge_values(parts)) if parts else None,
                "; ".join(errors[file]) if errors[file] else None,
            ]
        )
    schema = StructType(
        [
            StructField("STAGE_PATH", StringType()),
            StructField("VALUE", StringType()),
            StructField("ERROR", StringType()),
        ]
    )
    return session.create_dataframe(rows, schema=schema)
