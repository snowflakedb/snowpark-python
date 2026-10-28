#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Stored-procedure handler for document extraction that splits a PDF when the answer
does not fit in one AI_COMPLETE response.

Where to start reading: ``run`` at the bottom of the file is the entry point; it calls
the steps of ``DocumentExtraction`` just above it, in order:

1. ``load_files``: list the files under the path and read each one's page count.
2. ``plan_large_files``: files over 50 pages or 10 MB get planning-only calls that
   choose the page ranges to extract; every other file starts as one range (or a few,
   when it is over the size limit).
3. ``extract_until_done``: extract every range at once; ``finish`` reads each answer
   and queues what it leaves (the rest of a range that did not fit, or the halves of a
   failed one), until nothing is left. A throttled call is sent again after a short wait
   (``is_throttle_error``). If most documents are failing, no further rounds are
   submitted (``should_abort``): the documents still waiting get an error and the
   finished rows are returned.
4. back in ``run``: merge each file's parts, in page order, into one row.

Calls go to the first of the candidate models. A call AI_COMPLETE reports as
unknown-model is not a document failure: ``model_missing`` moves the read to the next
model, the call is sent again, and the read stops only when no model is left.

Everything above ``read_answer`` is a pure helper for one of these steps.

Each AI_COMPLETE call is asked to fill an ``extraction_status`` block first: whether
its answer is complete, and if not, the last page it covered and how to split the
pages it did not. Pages are always 1-based positions in the original document.

Uploaded and registered as a standalone file, so it imports nothing from snowpark's
internals. Every function except ``run`` and ``DocumentExtraction`` is pure and
unit-tested directly.
"""

import hashlib
import io
import json
import math
import os
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from snowflake.snowpark import DataFrame, Session

STATUS_FIELD = "extraction_status"
SCORES_FIELD = "extraction_scores"

# A field added at the front of the user's schema in every extraction call, which the model
# fills before extracting. It is how one call reports whether its answer is complete, and
# if not, which page it stopped after and how it would split the pages it left; the
# procedure uses that to queue the next calls (next_ranges). It also carries the model's
# output-token estimates, which size those ranges and the per-file call cap. The field is
# never returned to the user: merge_values() drops it from the merged answer.
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


# JSON Schema keywords removed from the user's schema before it is put in the prompt.
#
# The schema is shown to the model so it knows which fields to extract and their types.
# But a long document is read in several calls, each seeing only some of its pages, and
# many keywords state rules about the complete answer that one call cannot satisfy: for
# example ``required: ["invoice_number"]`` when the invoice number is printed on page 1
# and this call reads pages 40-60, or ``minItems: 100`` when this call sees 30 of the
# rows. A model told to meet such a rule may invent a value instead of leaving it out.
#
# The removed keywords are of three kinds:
# - rules on presence, size or value (required, minItems, maximum, pattern, const, ...),
#   which only the merged answer could meet;
# - rules that depend on other fields or positions (if/then/else, dependentRequired,
#   prefixItems, ...), which a partial answer can break the same way;
# - annotations that do not change what to extract ($schema, $id, examples, readOnly,
#   deprecated, ...), which only add prompt tokens.
# ``additionalProperties: false`` goes too: some answers arrive wrapped in one extra key,
# which unwrap_envelope() removes afterwards.
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
    """``schema`` with every key in ``_UNSUPPORTED_KEYWORDS`` dropped, recursively; keys
    under ``_NAME_MAPS`` are names, not keywords, and are never dropped."""
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


# A "malformed request" is the only answer AI_COMPLETE gives for a FILE over its input
# limit (about 15 MB; page count is not a limit), but it is also the answer for a file
# it cannot read at all. Only a call that sent more than this many bytes is split
# for it: splitting a small part that was rejected only multiplies the calls.
MALFORMED_SPLIT_MIN_BYTES = 10_000_000


def is_splittable_error(error: str, sent_bytes: Optional[int] = None) -> bool:
    """Errors that fewer pages can fix: a response cut off at max_tokens; an input
    Cortex rejects as too large, which it reports only as "malformed request" (counted
    only when the call sent more than ``MALFORMED_SPLIT_MIN_BYTES``, or when
    ``sent_bytes`` is unknown); or an answer that does not parse as JSON."""
    malformed = "malformed request" in error and (
        sent_bytes is None or sent_bytes > MALFORMED_SPLIT_MIN_BYTES
    )
    return (
        "unexpected end of JSON input" in error
        or malformed
        or UNPARSEABLE_TEXT_ERROR_PREFIX in error
        or NO_JSON_TEXT_ERROR in error
    )


# Signatures of a call the service refused for load, not for its content: sending it
# again later can succeed. Matched case-insensitively on the error text; the bare codes
# need word boundaries so a query id or a number never matches.
_THROTTLE_PATTERNS = (
    re.compile(r"\b429\b"),
    re.compile(r"too many requests"),
    re.compile(r"throttl"),
    re.compile(r"rate[ -]?limit"),
    re.compile(r"(at|insufficient|no) capacity"),
    re.compile(r"overloaded"),
    re.compile(r"service unavailable"),
    re.compile(r"\b503\b"),
)

# Retries per page range: a range whose answer is throttled is sent again up to this
# many times. Each round waits once before its retries, by THROTTLE_BACKOFF_SECONDS.
MAX_THROTTLE_RETRIES = 3
THROTTLE_BACKOFF_SECONDS = (5, 15, 45)
_sleep = time.sleep  # replaced in tests


def is_throttle_error(error: str) -> bool:
    """True when ``error`` says the service refused the call because of load (rate limit,
    429/503, overloaded, no capacity) rather than because of the document. An unknown
    model and a "malformed request" (an input that is too large) are never throttling."""
    text = error.lower()
    if "unknown model" in text or "malformed request" in text:
        return False
    return any(pattern.search(text) for pattern in _THROTTLE_PATTERNS)


# AI_COMPLETE accepts a FILE up to about 15 MB (claude-sonnet-5-5 took 14.99 MB and
# rejected 15.19 MB with a bare "malformed request"); parts stay well under that.
MAX_FILE_BYTES = 12_000_000

# Every file is read whole into the procedure's memory before it is cut into parts, so a
# file larger than this (by the size LIST reports) is not read: it gets its own error
# row instead of exhausting the procedure's memory.
# File types AI_COMPLETE accepts for Claude models. Other files are rejected before their
# bytes are read, so they are never sent to the model.
SUPPORTED_FILE_EXTENSIONS = frozenset(
    {".txt", ".md", ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".xhtml"}
)
MAX_FILE_READ_BYTES = 300_000_000


# Above these, a first call that ignores its token target can run for many minutes before
# failing, so the first round gets the small "first" budget; below them a whole answer
# usually fits in one call, which the small budget would needlessly split.
BOUNDED_FIRST_CALL_MIN_PAGES = 50
BOUNDED_FIRST_CALL_MIN_BYTES = 10_000_000


def is_large_file(total_pages: int, file_bytes: int) -> bool:
    """True when the file is over ``BOUNDED_FIRST_CALL_MIN_PAGES`` pages or
    ``BOUNDED_FIRST_CALL_MIN_BYTES``: it is planned first (plan_large_files), and its
    ranges use the smaller "large_range" budget."""
    return (
        total_pages > BOUNDED_FIRST_CALL_MIN_PAGES
        or file_bytes > BOUNDED_FIRST_CALL_MIN_BYTES
    )


def first_round_budget(total_pages: int, file_bytes: int) -> str:
    """Budget name for a file's first round: ``"first"`` for a large file, otherwise the
    normal ``"range"`` budget."""
    return "first" if is_large_file(total_pages, file_bytes) else "range"


def range_budget(total_pages: int, file_bytes: int) -> str:
    """Budget for a file's planned ranges: a large file's are sized and run against the
    same smaller target as its first round."""
    return "large_range" if is_large_file(total_pages, file_bytes) else "range"


def initial_ranges(
    total_pages: int, file_bytes: int, max_bytes: int = MAX_FILE_BYTES
) -> List[Tuple[int, int]]:
    """Page ranges for the first round: the whole file when it fits in one call,
    otherwise the fewest equal ranges whose size, estimated from the average page size,
    stays under ``max_bytes``. Each range after the first also carries one context page,
    so it is planned one page short."""
    if file_bytes <= max_bytes or total_pages <= 1:
        return [(1, total_pages)]
    limit = max(1, int(max_bytes / (file_bytes / total_pages)) - 1)
    parts = -(-total_pages // limit)
    pages_per_range = -(-total_pages // parts)
    return [
        (start, min(start + pages_per_range - 1, total_pages))
        for start in range(1, total_pages + 1, pages_per_range)
    ]


_SUBSCHEMA_LISTS = ("anyOf", "oneOf", "allOf", "prefixItems")


def nullable_scalars(schema: Any) -> Any:
    """Make every scalar at any depth nullable. A call covering only some pages cannot see
    a value printed elsewhere -- a document total on the last page, or a fund's totals
    when the range starts mid-schedule -- and a schema that forbids null pushes the
    model to write a placeholder instead."""
    if not isinstance(schema, dict):
        return schema
    out = dict(schema)
    json_type = out.get("type")
    if isinstance(json_type, str) and json_type not in ("object", "array", "null"):
        out["type"] = [json_type, "null"]
    for key in _NAME_MAPS:
        if isinstance(out.get(key), dict):
            out[key] = {name: nullable_scalars(sub) for name, sub in out[key].items()}
    if "items" in out:
        out["items"] = nullable_scalars(out["items"])
    for key in _SUBSCHEMA_LISTS:
        if isinstance(out.get(key), list):
            out[key] = [nullable_scalars(sub) for sub in out[key]]
    return out


def scores_schema(field_names: List[str]) -> Dict[str, Any]:
    """The ``extraction_scores`` property: one nullable number per top-level field of the
    user's schema, the model's confidence in that field's extracted value. It is the last
    property, so the model scores values it has already written."""
    return {
        "type": "object",
        "description": (
            "Fill this LAST, after every field. For each top-level field, your confidence "
            "from 0 to 1 that the value you extracted is correct and complete."
        ),
        "properties": {name: {"type": ["number", "null"]} for name in field_names},
    }


def with_status(response_format: Dict[str, Any]) -> Dict[str, Any]:
    """``response_format`` with every scalar made nullable (see ``nullable_scalars``),
    ``_STATUS_SCHEMA`` added as the leading field and ``extraction_scores`` (one number
    per top-level field) as the last one, and unsupported keywords dropped -- the schema
    actually sent to the model."""
    wrapped = nullable_scalars(response_format["schema"])
    properties = wrapped.get("properties", {})
    wrapped["properties"] = {STATUS_FIELD: _STATUS_SCHEMA, **properties}
    if properties:
        wrapped["properties"][SCORES_FIELD] = scores_schema(list(properties))
    return {**response_format, "schema": drop_unsupported(wrapped)}


def build_predicate(
    base: str,
    target: int,
    start: int,
    end: int,
    total_pages: int,
    plan_target: Optional[int] = None,
) -> str:
    """``target`` bounds this response; ``plan_target`` sizes the ranges it plans for
    the remaining pages (defaults to ``target``)."""
    plan_target = plan_target or target
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
        f"expected to need under {plan_target} output tokens. Always fill extraction_status first."
    )


# Plan-first: a large file is first cut into
# segments that each carry the previous page as context, and one planning call per segment
# returns -- without extracting -- calibration samples, rows per page and the page ranges to
# extract; every planned range then runs at once.
PLAN_SEGMENT_MAX_PAGES = 50  # pages per planning part file, the context page included
PLAN_SEGMENT_MAX_BYTES = 10_000_000


def planning_segments(
    total_pages: int,
    file_bytes: int,
    max_pages: int = PLAN_SEGMENT_MAX_PAGES,
    max_bytes: int = PLAN_SEGMENT_MAX_BYTES,
) -> List[Tuple[int, int]]:
    """The fewest equal page segments whose part files, with one context page in front,
    stay within ``max_pages`` pages and (from the average page size) ``max_bytes``; none
    for a file with no pages."""
    if total_pages <= 0:
        return []
    by_pages = max(1, max_pages - 1)
    by_size = max(1, int(max_bytes / (file_bytes / max(total_pages, 1))) - 1)
    limit = min(by_pages, by_size)
    parts = -(-total_pages // limit)
    size = -(-total_pages // parts)
    return [
        (a, min(a + size - 1, total_pages)) for a in range(1, total_pages + 1, size)
    ]


def build_plan_predicate(
    predicate: str,
    start: int,
    end: int,
    total_pages: int,
    target: int,
    schema: Dict[str, Any],
) -> str:
    """The planning prompt for document pages ``start``..``end``."""
    if start == 1 and end == total_pages:
        scope = (
            f"This file is the whole document: pages 1 to {total_pages}. Report "
            f"positions 1 to {total_pages} of THIS file."
        )
    elif start == 1:
        scope = (
            f"This file is pages 1 to {end} of a {total_pages}-page document. Report "
            f"positions 1 to {end} of THIS file."
        )
    else:
        positions = end - start + 2
        scope = (
            f"This file is {positions} consecutive pages from the middle of a "
            f"{total_pages}-page document. Position 1 is the page BEFORE this segment, "
            "included ONLY as context (for example, the column headers of a table that "
            "continues onto the next page): do NOT report position 1 and do NOT take "
            f"samples from it. Report positions 2 to {positions} of THIS file."
        )
    return (
        "Do NOT extract this document. Your job is to plan its extraction as "
        f"ACCURATELY as possible.\n\nThe extraction instructions are: {predicate}\n\n"
        f"{scope}\n\n"
        "How the plan is used: the pages you report will be split into the page ranges "
        "you choose, and each range is extracted by a separate call, all running at the "
        "same time. Each call can write at most 128000 output tokens; a call that runs "
        "past that is cut off, its whole answer is thrown away and minutes of work are "
        "wasted. So choose consecutive page ranges that each need UNDER "
        f"{target} output tokens for their extracted JSON, as close to {target} as you "
        "can without going over, and never split a page. Estimates from counting alone "
        "have been about 2x too low, because every row repeats all its JSON keys, "
        "quotes, nulls and punctuation. So measure, do not guess.\n\n"
        "The extraction will produce JSON conforming to the JSON Schema below.\n\n"
        "Step 1, calibration: pick 3 pages that are typical of the pages with the MOST "
        "items, from different parts of the document. For each, write the first 3 "
        "items of that page exactly as they would appear in the final JSON: every field "
        "of the schema's array item, with null for missing values, compact JSON.\n\n"
        "Step 2: for every page you must report, give [page, rows]: its 1-based PDF "
        "page position within this file (not the number printed on the page) and how "
        "many items from it would go into the schema's arrays.\n\n"
        "Step 3: split the pages you must report into consecutive ranges [start_page, "
        "end_page, estimated_output_tokens] that together cover all of them, each "
        "estimated from your step 1 measurement (an item's JSON is roughly one token "
        f"per 1.5 characters) and each under {target} output tokens.\n\n"
        "Respond with ONLY one compact JSON object, no prose and no markdown code "
        'fences:\n{"samples": [{"page": 12, "array": "<array field name>", "items": '
        '[ ... ]}, ...], "pages": [[1, 0], [2, 74], ...], "ranges": [[1, 9, 58000], '
        "[10, 17, 57000], ...]}\n\n"
        f"JSON Schema:\n{json.dumps(schema)}"
    )


def ranges_from_plan(
    value: Any, start: int, end: int
) -> Optional[List[Tuple[int, int]]]:
    """Document page ranges from a planning answer for pages ``start``..``end``: positions
    mapped back to document pages, the context page dropped, overlaps trimmed and any page
    the plan left out covered by its own range. None when the plan has no usable range."""
    if not isinstance(value, dict) or not isinstance(value.get("ranges"), list):
        return None
    ranges = []
    for item in value["ranges"]:
        if not (
            isinstance(item, list)
            and len(item) >= 2
            and all(isinstance(x, int) and not isinstance(x, bool) for x in item[:2])
        ):
            continue
        a = _to_document_page(item[0], start, end)
        b = _to_document_page(item[1], start, end)
        if a is None or b is None:
            continue
        a, b = max(a, start), min(b, end)
        if a <= b:
            ranges.append((a, b))
    if not ranges:
        return None
    return _cover(sorted(ranges), start, end)


def unwrap_envelope(value: Any, schema: Dict[str, Any]) -> Any:
    """An answer nested whole under one undeclared key such as ``{"input": {...}}``,
    unwrapped; otherwise every real field would read as missing."""
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
    """``page``, a 1-based position within the part file for pages ``start``..``end``,
    converted to a document page; None when it is not an integer or falls outside the
    part file's range."""
    if not isinstance(page, int) or isinstance(page, bool):
        return None
    document_page = page + file_page_offset(start)
    if start - 1 <= document_page <= end:
        return document_page
    return None


def document_estimate(
    value: Any, start: int, end: int, total_pages: int
) -> Optional[int]:
    """A call's estimate for its own pages scaled to the whole document; None without one."""
    status = value.get(STATUS_FIELD) if isinstance(value, dict) else None
    estimate = (
        status.get("estimated_total_output_tokens")
        if isinstance(status, dict)
        else None
    )
    if not isinstance(estimate, int) or isinstance(estimate, bool) or estimate <= 0:
        return None
    return int(estimate * total_pages / max(end - start + 1, 1))


_PAGE_FIELD_NAMES = frozenset({"page", "page_number", "pdf_page", "page_index"})


def _is_page_field(name: str) -> bool:
    """True when ``name`` looks like a page-number field (see ``_PAGE_FIELD_NAMES``)."""
    lowered = name.lower()
    return lowered in _PAGE_FIELD_NAMES or lowered.endswith("_page")


def shift_page_fields(value: Dict[str, Any], start: int, end: int) -> Dict[str, Any]:
    """Rows in a part file report pages as positions within that file; shift them to
    document pages. Only a page field whose every value in an array lies within the part
    file's content pages (2..file length; position 1 is the context page) is shifted, so
    page numbers printed on the pages -- which run on from the document -- are left alone."""
    offset = file_page_offset(start)
    if offset == 0:
        return value
    file_pages = end - start + 2

    def shift(node: Any) -> Any:
        if isinstance(node, dict):
            return {k: shift(v) for k, v in node.items()}
        if not isinstance(node, list):
            return node
        items = [shift(item) for item in node]
        rows = [item for item in items if isinstance(item, dict)]
        page_keys = {k for row in rows for k in row if _is_page_field(k)}
        for key in page_keys:
            values = [row.get(key) for row in rows if row.get(key) is not None]
            if values and all(
                isinstance(v, int) and not isinstance(v, bool) and 2 <= v <= file_pages
                for v in values
            ):
                for row in rows:
                    if isinstance(row.get(key), int):
                        row[key] += offset
        return items

    return {
        k: (v if k in (STATUS_FIELD, SCORES_FIELD) else shift(v))
        for k, v in value.items()
    }


def halves(start: int, end: int) -> List[Tuple[int, int]]:
    """``start``-``end`` split into two ranges; the first takes the middle page."""
    mid = (start + end) // 2
    return [(start, mid), (mid + 1, end)]


def next_ranges(
    start: int,
    end: int,
    value: Optional[Dict[str, Any]],
    error: Optional[str],
    target: Optional[int] = None,
    salvaged: Optional[Dict[str, Any]] = None,
    sent_bytes: Optional[int] = None,
) -> Tuple[bool, List[Tuple[int, int]]]:
    """Returns (keep_value, ranges_still_to_extract) for one finished call. With a
    ``target``, adjacent planned ranges are coalesced up to ``target`` estimated tokens.
    A failed call's ``salvaged`` status, when usable, re-splits its pages along its own
    plan instead of in halves. ``sent_bytes`` is the size of the file or part the call
    sent, for ``is_splittable_error``.

    A successful answer over more than one page whose ``extraction_status`` is missing
    or not an object is not trusted: the model may have stopped early without saying so,
    so the answer is discarded and its pages retried in halves. On a single page, which
    cannot be split, the answer is kept."""
    if error is not None or not isinstance(value, dict):
        if end > start and (error is None or is_splittable_error(error, sent_bytes)):
            if salvaged is not None:
                ranges = ranges_from_salvaged_status(start, end, salvaged, target)
                if ranges:
                    return False, ranges
            return False, halves(start, end)
        return False, []
    status = value.get(STATUS_FIELD)
    if not isinstance(status, dict):
        if end > start:
            return False, halves(start, end)
        return True, []
    if status.get("complete") is not False:
        return True, []
    last = _to_document_page(status.get("last_pdf_page_extracted"), start, end)
    if last is None or last < start:
        # No usable progress: re-asking for the same pages would loop, so halve them.
        if end > start:
            return False, halves(start, end)
        return True, []
    if last >= end:
        return True, []
    ranges = []
    estimates: Dict[Tuple[int, int], int] = {}
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
            estimate = planned.get("estimated_output_tokens")
            if isinstance(estimate, int) and not isinstance(estimate, bool):
                estimates[(a, b)] = estimate
    covered = _cover(sorted(ranges), last + 1, end)
    if target is None:
        return True, covered
    return True, coalesce_ranges(
        [(a, b, estimates.get((a, b))) for a, b in covered], target
    )


def coalesce_ranges(
    ranges: List[Tuple[int, int, Optional[int]]], target: int
) -> List[Tuple[int, int]]:
    """Merge consecutive ranges while their summed estimate stays within ``target``, so a
    plan finer than the target asks for does not cost extra calls; a range without an
    estimate is never merged."""
    merged: List[Tuple[int, int, Optional[int]]] = []
    for a, b, estimate in ranges:
        if (
            merged
            and estimate is not None
            and merged[-1][2] is not None
            and merged[-1][2] + estimate <= target
        ):
            merged[-1] = (merged[-1][0], b, merged[-1][2] + estimate)
        else:
            merged.append((a, b, estimate))
    return [(a, b) for a, b, _ in merged]


HARD_MAX_CALLS_PER_FILE = 60

# The most AI_COMPLETE calls a read keeps in flight at once: a round with more tasks is
# submitted and collected in chunks of this size, planning calls included.
MAX_CALLS_IN_FLIGHT = 64


def chunks(items: List[Any], size: int) -> List[List[Any]]:
    """``items`` in consecutive lists of at most ``size``."""
    return [items[i : i + size] for i in range(0, len(items), size)]


# Files are read and extracted in batches whose listed sizes add up to at most this, so
# the procedure holds one batch of PDFs in memory at a time rather than the whole path.
# MAX_FILE_READ_BYTES keeps any one file that is read below it.
MAX_BATCH_BYTES = 500_000_000


def batches(
    listed: List[Tuple[str, Optional[int]]], max_bytes: int
) -> List[List[Tuple[str, Optional[int]]]]:
    """``listed`` ``(name, size)`` pairs in consecutive batches whose sizes add up to at
    most ``max_bytes``. A file that is not read (over ``MAX_FILE_READ_BYTES``) or whose
    size is unknown counts as 0; a file larger than ``max_bytes`` is a batch of its own."""
    result: List[List[Tuple[str, Optional[int]]]] = []
    batch: List[Tuple[str, Optional[int]]] = []
    total = 0
    for name, size in listed:
        read = size if size is not None and size <= MAX_FILE_READ_BYTES else 0
        if batch and total + read > max_bytes:
            result.append(batch)
            batch, total = [], 0
        batch.append((name, size))
        total += read
    if batch:
        result.append(batch)
    return result


# A read that is failing across the board stops submitting instead of spending calls on
# the rest: once this many documents have finished, it stops when more than this fraction
# of them failed (see ``should_abort``); what is finished is still returned.
MIN_DOCUMENTS_BEFORE_ABORT = 10
MAX_FAILED_FRACTION = 0.75


def should_abort(finished: int, failed: int) -> bool:
    """True when at least ``MIN_DOCUMENTS_BEFORE_ABORT`` documents have finished and more
    than ``MAX_FAILED_FRACTION`` of the finished ones failed (exactly that fraction does
    not abort)."""
    return (
        finished >= MIN_DOCUMENTS_BEFORE_ABORT
        and failed > MAX_FAILED_FRACTION * finished
    )


def abort_message(finished: int, failed: int, errors: List[str]) -> str:
    """Why a read was stopped by ``should_abort``: how many of the finished documents
    failed, and the most common first error among them."""
    common = Counter(errors).most_common(1)
    reason = f" Most common error: {common[0][0][:200]}" if common else ""
    return (
        f"the read stopped early because {failed} of the {finished} documents finished "
        f"so far failed, which is more than {MAX_FAILED_FRACTION:.0%}.{reason}"
    )


def stop_waiting(tasks: List[Any], reason: str) -> None:
    """End the read for the files that still have page ranges waiting: each gets one error
    naming its ranges that were not extracted and ``reason``. Parts a file already holds
    and the errors it already has are kept, so its row is returned as it stands."""
    ranges: Dict[int, Tuple[Any, List[str]]] = {}
    for file, start, end, *_ in tasks:
        ranges.setdefault(id(file), (file, []))[1].append(f"{start}-{end}")
    for file, spans in ranges.values():
        file.errors.append(f"pages {', '.join(spans)} not extracted: {reason}")


def call_cap(floor: int, estimated_total: Optional[int], range_target: int) -> int:
    """Calls a file may use: twice the number of target-sized ranges its answer needs,
    plus slack, never below ``floor`` nor above HARD_MAX_CALLS_PER_FILE."""
    if not estimated_total:
        return floor
    needed = -(-estimated_total // range_target)
    return min(HARD_MAX_CALLS_PER_FILE, max(floor, 2 * needed + 4))


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
    """True when ``value`` is None, or an empty string, list or dict."""
    return value is None or value == "" or value == [] or value == {}


def continues_across_split(last: Any, first: Any) -> bool:
    """True when the last array item of one range and the first of the next are one item
    cut by the split -- e.g. a fund whose holdings end one range and whose totals open the
    next. They must agree on every value both have, share at least one identifying scalar,
    and each fill something the other lacks; two complete rows are never joined, so a row
    printed twice stays twice."""
    if not isinstance(last, dict) or not isinstance(first, dict):
        return False
    shared_scalar = False
    for key in set(last) & set(first):
        a, b = last[key], first[key]
        if _is_absent(a) or _is_absent(b):
            continue
        if isinstance(a, dict) and isinstance(b, dict):
            if not continues_across_split(a, b) and a != b:
                return False
        elif isinstance(a, list) or isinstance(b, list):
            continue
        elif a != b:
            return False
        else:
            shared_scalar = True
    fills_last = any(
        _is_absent(last.get(k)) and not _is_absent(v) for k, v in first.items()
    )
    fills_first = any(
        _is_absent(first.get(k)) and not _is_absent(v) for k, v in last.items()
    )
    return shared_scalar and (fills_last or fills_first)


def merge_values(parts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge per-range answers given in page order: arrays concatenate (joining the item
    a split cut in two), objects merge field by field, scalars keep the first present
    value."""

    def merge(a: Any, b: Any) -> Any:
        if _is_absent(a):
            return b
        if _is_absent(b):
            return a
        if isinstance(a, list) and isinstance(b, list):
            if continues_across_split(a[-1], b[0]):
                return a[:-1] + [merge(a[-1], b[0])] + b[1:]
            return a + b
        if isinstance(a, dict) and isinstance(b, dict):
            out = dict(a)
            for key, value in b.items():
                out[key] = merge(out.get(key), value)
            return out
        return a

    merged: Dict[str, Any] = {}
    for part in parts:
        part = {k: v for k, v in part.items() if k not in (STATUS_FIELD, SCORES_FIELD)}
        merged = merge(merged, part)
    return merged


def merge_scores(
    parts: List[Dict[str, Any]], fields: Optional[List[str]] = None
) -> Dict[str, float]:
    """One confidence per top-level field from the ``extraction_scores`` of each part (the
    parts of one document, as answered). Conservative rule: a field's score is the
    MINIMUM over the parts where the field has a value; when it is empty in every part,
    the MAXIMUM reported (the confidence that it is really absent). Scores are clamped to
    [0, 1]; non-numbers, booleans and NaN are ignored; a field with no usable score is
    left out. With ``fields``, names that are not top-level fields of the schema are
    ignored."""
    filled: Dict[str, List[float]] = {}
    empty: Dict[str, List[float]] = {}
    for part in parts:
        scores = part.get(SCORES_FIELD)
        if not isinstance(scores, dict):
            continue
        for name, raw in scores.items():
            if fields is not None and name not in fields:
                continue
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                continue
            if not math.isfinite(raw):
                continue
            score = min(max(float(raw), 0.0), 1.0)
            target = empty if _is_absent(part.get(name)) else filled
            target.setdefault(name, []).append(score)
    merged: Dict[str, float] = {}
    for name in list(filled) + [n for n in empty if n not in filled]:
        merged[name] = min(filled[name]) if name in filled else max(empty[name])
    return merged


def quote_stage_path(path: str) -> str:
    """``path`` as a single-quoted SQL string literal for LIST, with backslashes,
    single quotes and newlines escaped, so nothing in it can add a clause to the
    statement. A copy of ``str_to_sql`` in ``_internal/analyzer/datatype_mapper.py``,
    since this file is uploaded standalone and must not import snowpark internals."""
    return (
        "'" + path.replace("\\", "\\\\").replace("'", "''").replace("\n", "\\n") + "'"
    )


def listed_size(row: Any) -> Optional[int]:
    """The ``size`` LIST reports for a file, or None when the row has none."""
    try:
        return int(row["size"])
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def split_stage(path: str) -> Tuple[str, int]:
    """The stage part of ``path`` (``@`` and everything before the first ``/`` outside
    double quotes), and how many leading characters of a name LIST returns for it are the
    stage's own name. LIST names a file of a named stage '<stage name>/<relative path>',
    with no database or schema and the name lower-cased, but a file of the user stage
    (``@~``) or a table stage (``@%t``) by its relative path alone. A copy of
    ``split_stage`` in ``document_reader.py``, which cannot import this file."""
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


def qualify_listed_name(path: str, listed_name: str) -> str:
    """A reference to a file LIST returned for ``path`` (see ``split_stage``) that
    resolves regardless of the session's current database and schema."""
    stage, listed_prefix = split_stage(path)
    return f"{stage}/{listed_name[listed_prefix:]}"


# claude-sonnet-5-5 rejects response_format, so the schema travels in the predicate and
# the answer comes back as text.
_AI_COMPLETE_SQL = (
    "SELECT AI_COMPLETE(model => ?, predicate => ?, file => TO_FILE(?), "
    "model_parameters => PARSE_JSON(?), return_error_details => TRUE)"
)


_SCORES_INSTRUCTION = (
    " Fill extraction_scores last, after every other field: for each top-level field, a "
    "number from 0 to 1 for how confident you are that the value you extracted is "
    "correct and complete (1 is certain, 0 is a guess). For an array or object field "
    "give one score for the whole value. For a field you returned as null or empty, "
    "give how confident you are that it is really absent from the pages you were given."
)


def schema_instruction(schema: Dict[str, Any]) -> str:
    """The instruction telling the model to answer with one JSON object conforming to
    ``schema`` and nothing else, including the confidence scores when the schema has the
    ``extraction_scores`` property."""
    scores = (
        _SCORES_INSTRUCTION if SCORES_FIELD in (schema.get("properties") or {}) else ""
    )
    return (
        "Respond with ONLY one JSON object that conforms to the JSON Schema below: no "
        "prose before or after it and no markdown code fences. Fill extraction_status "
        "first."
        + scores
        + "\n\nJSON Schema:\n"
        + json.dumps(schema, separators=(",", ":"))
    )


TRUNCATED_TEXT_ERROR = "unexpected end of JSON input in a text answer"
UNPARSEABLE_TEXT_ERROR_PREFIX = "answer was not valid JSON"
NO_JSON_TEXT_ERROR = "answer contained no JSON object"
NOT_AN_OBJECT_TEXT_ERROR = "answer JSON was not an object"


def _brackets_close(text: str) -> bool:
    """Whether every { and [ outside a JSON string is closed by the end of ``text``."""
    depth, in_string, escaped = 0, False, False
    for char in text:
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char in "{[":
            depth += 1
        elif char in "}]":
            depth -= 1
    return depth <= 0 and not in_string


def parse_text_answer(text: Any) -> Tuple[Any, Optional[str]]:
    """The JSON object out of a text answer. A JSON document cut off before it closes is
    reported with the truncation signature so its pages get split. An answer that is a
    JSON list instead of an object is an error (``NOT_AN_OBJECT_TEXT_ERROR``), not split."""
    if isinstance(text, dict):
        return text, None
    if not isinstance(text, str):
        return None, "answer was not text"
    body = text.strip()
    if body.startswith("```"):
        body = body.split("\n", 1)[1] if "\n" in body else ""
        if body.rstrip().endswith("```"):
            body = body.rstrip()[:-3]
    first = body.find("{")
    bracket = body.find("[")
    if bracket >= 0 and (first < 0 or bracket < first):
        # A top-level list, e.g. [{...}, {...}] (even cut off): decoding from its first
        # "{" would keep the first element and silently drop the rest. A list counts when
        # the answer starts with it, or when "[" is directly followed by an object; a
        # bracket in prose before the object (e.g. "[1]") does not.
        try:
            listed, _ = json.JSONDecoder(strict=False).raw_decode(body[bracket:])
        except json.JSONDecodeError:
            listed = None  # prose such as "[note]", not JSON: look for an object
        list_of_objects = first > bracket and not body[bracket + 1 : first].strip()
        at_start = not body[:bracket].strip()
        if (isinstance(listed, list) and at_start) or list_of_objects:
            return None, NOT_AN_OBJECT_TEXT_ERROR
    if first < 0:
        return None, NO_JSON_TEXT_ERROR
    candidate = body[first:]
    try:
        # strict=False: a raw tab or newline inside a string is still a usable answer.
        value, _ = json.JSONDecoder(strict=False).raw_decode(candidate)
    except json.JSONDecodeError as exc:
        # An answer cut off by max_tokens usually stops mid-string, and the decoder then
        # points at where that string *started*, not at the end -- so also treat any answer
        # whose brackets never close as truncated.
        if (
            exc.pos >= len(candidate.rstrip()) - 1
            or exc.msg.startswith("Unterminated string")
            or not _brackets_close(candidate)
        ):
            return None, TRUNCATED_TEXT_ERROR
        return None, f"{UNPARSEABLE_TEXT_ERROR_PREFIX}: {exc.msg}"
    return value, None  # decoding started at a "{", so this is an object


def salvage_status(text: Any) -> Optional[Dict[str, Any]]:
    """The extraction_status object of a text answer that does not parse as a whole. The
    status is written first, so an answer cut off later usually still holds it intact."""
    if not isinstance(text, str):
        return None
    key = text.find(f'"{STATUS_FIELD}"')
    brace = text.find("{", key) if key >= 0 else -1
    if brace < 0:
        return None
    try:
        status, _ = json.JSONDecoder(strict=False).raw_decode(text[brace:])
    except json.JSONDecodeError:
        return None
    return status if isinstance(status, dict) else None


def split_evenly(start: int, end: int, parts: int) -> List[Tuple[int, int]]:
    """Split pages ``start``..``end`` into ``parts`` consecutive, evenly sized ranges."""
    parts = max(1, min(parts, end - start + 1))
    size = -(-(end - start + 1) // parts)
    return [(a, min(a + size - 1, end)) for a in range(start, end + 1, size)]


def ranges_from_salvaged_status(
    start: int, end: int, status: Dict[str, Any], target: Optional[int]
) -> Optional[List[Tuple[int, int]]]:
    """Ranges for a failed call's pages from the plan in its salvaged status, instead of
    blind halves. The pages it meant to extract itself overflowed, so they are cut into at
    least two pieces (more if its own estimate for them exceeds ``target``); the pages it
    planned follow as planned. None when the status gives nothing usable."""
    last = _to_document_page(status.get("last_pdf_page_extracted"), start, end)
    if last is None or last < start:
        return None
    planned: List[Tuple[int, int]] = []
    remaining_estimate = 0
    for item in status.get("remaining_page_ranges") or []:
        if not isinstance(item, dict):
            continue
        a = _to_document_page(item.get("start_pdf_page"), start, end)
        b = _to_document_page(item.get("end_pdf_page"), start, end)
        if a is None or b is None:
            continue
        a, b = max(a, last + 1), min(b, end)
        if a <= b:
            planned.append((a, b))
            estimate = item.get("estimated_output_tokens")
            if isinstance(estimate, int) and not isinstance(estimate, bool):
                remaining_estimate += estimate
    total = status.get("estimated_total_output_tokens")
    pieces = 2
    if target and isinstance(total, int) and not isinstance(total, bool):
        pieces = max(pieces, -(-(total - remaining_estimate) // target))
    ranges = split_evenly(start, min(last, end), pieces)
    if last < end:
        ranges += _cover(sorted(planned), last + 1, end)
    if ranges == [(start, end)]:
        return None
    return ranges


def is_unknown_model_error(message: str) -> bool:
    """True when AI_COMPLETE failed because the model does not exist or is not enabled
    for the account or region (``400 unknown model "<name>"``). Only this message ends
    a read: other failures, such as throttling, can pass and are reported per file."""
    return "unknown model" in message.lower()


class NoModelAvailable(RuntimeError):
    """AI_COMPLETE knows none of the models the reader can use. Terminal: it is never
    turned into a per-file error."""


def no_model_message(models: List[str]) -> str:
    """The terminal error when AI_COMPLETE knows none of the models the reader can use."""
    names = " or ".join(f'"{m}"' for m in models)
    return (
        f"No model is available for document extraction: AI_COMPLETE reports {names} as "
        "unknown for this account. Enable cross-region inference "
        "(the CORTEX_ENABLED_CROSS_REGION account parameter) or use a region where one "
        "of these models is available."
    )


def read_answer(raw: Any) -> Tuple[Any, Optional[str], Optional[Dict[str, Any]]]:
    """(value, error, salvaged status) from one ``AI_COMPLETE(..., return_error_details =>
    TRUE)`` result. The status is salvaged only from a text answer that does not parse."""
    result = json.loads(raw) if isinstance(raw, str) else raw
    value, error = result.get("value"), result.get("error")
    if error is not None:
        return value, error, None
    parsed, error = parse_text_answer(value)
    return parsed, error, salvage_status(value) if error is not None else None


@dataclass
class FileState:
    """One document's progress through the procedure. ``reader`` is None for a file that
    is not a readable PDF: it is extracted whole, as one page, and never split
    (``splittable`` is False). The reader is dropped once the file's batch is done, to
    free its bytes; ``splittable`` still says whether it was a PDF. ``failed`` marks a
    file whose bytes could not be read at all: it has its error, no tasks, and still
    gets an output row."""

    path: str
    reader: Any
    pages: int
    size: int
    cap: int
    calls: int = 0
    parts: List[Tuple[int, Dict[str, Any]]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    failed: bool = False
    splittable: bool = field(init=False)

    def __post_init__(self) -> None:
        """Record whether this file can be cut into page ranges, i.e. it parsed as a
        PDF, so that stays known after its reader is dropped."""
        self.splittable = self.reader is not None

    @classmethod
    def failed_with(cls, path: str, size: int, cap: int, error: str) -> "FileState":
        """A ``failed`` file: one page, no tasks, and ``error`` as its only error."""
        return cls(path, None, 1, size, cap, errors=[error], failed=True)

    @property
    def range_budget(self) -> str:
        """This file's budget name for its ranges after the first round."""
        return range_budget(self.pages, self.size)


# A page range waiting for a call: (file, first page, last page, budget name).
Task = Tuple[FileState, int, int, str]


@dataclass
class Submitted:
    """An AI_COMPLETE call in flight: its async job and the model it was sent to."""

    job: Any
    model: str


def result_row(file: FileState, fields: Optional[List[str]] = None) -> List[Any]:
    """The output row ``[STAGE_PATH, VALUE, ERROR, SCORES]`` of one file: its parts merged
    in page order, so a table split across ranges comes back as one table, and the
    per-field confidence scores merged by ``merge_scores`` (None when there are none).
    A failure while merging is that file's error, not the read's."""
    errors = list(file.errors)
    value = scores = None
    parts = [part for _, part in sorted(file.parts, key=lambda item: item[0])]
    if parts:
        try:
            value = json.dumps(merge_values(parts))
            merged = merge_scores(parts, fields)
            scores = json.dumps(merged) if merged else None
        except Exception as exc:
            value = scores = None
            errors.append(f"could not merge the extracted parts: {exc}")
    return [file.path, value, "; ".join(errors) if errors else None, scores]


def document_outcomes(
    files: List[FileState], tasks: List[Task]
) -> Tuple[int, int, List[str]]:
    """``(finished, failed, errors)`` over the documents in ``files`` given the ``tasks``
    still waiting. Only readable PDFs are documents here: a file that is not a PDF
    (extracted whole) and a file that could not be read at all are left out of both
    counts, since they say nothing about whether the model and the schema work. A
    document has finished when none of its page ranges is waiting. It has failed when it
    finished with no successful part, so a document that was only partly extracted counts
    as succeeded (its errors still appear in its own error column). ``errors`` holds the
    first error of each failed document."""
    waiting = {id(task[0]) for task in tasks}
    done = [f for f in files if f.splittable and not f.failed and id(f) not in waiting]
    failed = [f for f in done if not f.parts]
    return len(done), len(failed), [f.errors[0] for f in failed if f.errors]


class DocumentExtraction:
    """The calls for every file under a path: plan large files, extract every range,
    re-split ranges whose answer did not fit, and merge each file's parts."""

    def __init__(
        self,
        session: Session,
        response_format: Dict[str, Any],
        predicate: str,
        models: List[str],
        budgets: Dict[str, Any],
        work_stage: str,
    ) -> None:
        """Precompute the schema sent to the model (with the status field added and
        every scalar made nullable) and its instruction text. ``models`` are the
        candidates in order of preference; calls use the first until AI_COMPLETE reports
        it unknown (see ``model_missing``)."""
        self.session = session
        self.schema = response_format["schema"]
        self.sent_schema = with_status(response_format)["schema"]
        self.instruction = "\n\n" + schema_instruction(self.sent_schema)
        self.predicate = predicate
        self.models = models
        self.model = models[0]
        self.budgets = budgets
        self.work_stage = work_stage
        # Times each page range was sent again after a throttled answer, and the wait
        # before the next round (see ``throttled``).
        self.throttle_retries: Dict[Tuple[int, ...], int] = {}
        self.throttle_wait = 0
        # Bytes of the file or part last staged for each (file, start, end) range.
        self.sent_bytes: Dict[Tuple[int, int, int], int] = {}

    def model_missing(
        self, file: FileState, submitted: Submitted, error: Optional[str]
    ) -> bool:
        """Whether ``error`` says AI_COMPLETE does not know the model the call was sent
        to. Such a call is not a document failure: it is not counted against ``file``'s
        calls, and the caller sends it again. The first report for the model in use
        moves the read on to the next candidate, so the rest of the calls already in
        flight with the old model only need sending again; when no candidate is left the
        read stops with ``NoModelAvailable``. Costs no extra call when the first model
        is available."""
        if error is None or not is_unknown_model_error(error):
            return False
        file.calls -= 1
        if submitted.model == self.model:
            index = self.models.index(self.model) + 1
            if index >= len(self.models):
                raise NoModelAvailable(no_model_message(self.models))
            self.model = self.models[index]
        return True

    def throttled(
        self, file: FileState, key: Tuple[int, ...], error: Optional[str]
    ) -> bool:
        """Whether ``error`` is a throttled answer to send again. Such a call is not a
        document failure and is not counted against ``file``'s calls. Each page range
        (``key``) is sent again at most ``MAX_THROTTLE_RETRIES`` times; the next round
        waits first (``THROTTLE_BACKOFF_SECONDS``, once however many calls were throttled).
        After the last retry the throttled answer is that range's error like any other."""
        if error is None or not is_throttle_error(error):
            return False
        retries = self.throttle_retries.get(key, 0)
        if retries >= MAX_THROTTLE_RETRIES:
            return False
        self.throttle_retries[key] = retries + 1
        file.calls -= 1
        self.throttle_wait = max(self.throttle_wait, THROTTLE_BACKOFF_SECONDS[retries])
        return True

    def wait_if_throttled(self) -> None:
        """Wait before the next round if a call in the last one was throttled."""
        if self.throttle_wait:
            _sleep(self.throttle_wait)
            self.throttle_wait = 0

    @staticmethod
    def read_bytes(name: str) -> bytes:
        """The bytes of the staged file ``name``."""
        from snowflake.snowpark.files import SnowflakeFile

        with SnowflakeFile.open(name, "rb", require_scoped_url=False) as f:
            return f.read()

    def load_file(
        self, name: str, max_calls_per_file: int, listed_size: Optional[int] = None
    ) -> FileState:
        """Read one staged file and parse it as a PDF when possible; a file that is not a
        readable PDF becomes a one-page, unsplittable ``FileState``. A file that cannot
        be read at all, whose ``listed_size`` is over ``MAX_FILE_READ_BYTES`` (it is then
        not read), or that is a PDF with no pages becomes a ``failed`` one carrying the
        error, so it affects only its own output row."""
        from pypdf import PdfReader

        if listed_size is not None and listed_size > MAX_FILE_READ_BYTES:
            return FileState.failed_with(
                name,
                listed_size,
                max_calls_per_file,
                f"file is {listed_size} bytes, larger than the supported maximum "
                f"of {MAX_FILE_READ_BYTES} bytes",
            )
        extension = os.path.splitext(name)[1].lower()
        if extension not in SUPPORTED_FILE_EXTENSIONS:
            return FileState.failed_with(
                name,
                0,
                max_calls_per_file,
                f"file type {extension or '(none)'} is not supported; supported "
                f"types are {', '.join(sorted(SUPPORTED_FILE_EXTENSIONS))}",
            )
        try:
            content = self.read_bytes(name)
        except Exception as exc:
            return FileState.failed_with(
                name, 0, max_calls_per_file, f"could not read file: {exc}"
            )
        try:
            reader = PdfReader(io.BytesIO(content))
            pages = len(reader.pages)
        except Exception:
            reader, pages = None, 1
        if reader is not None and pages == 0:
            return FileState.failed_with(
                name, len(content), max_calls_per_file, "the PDF has no pages"
            )
        return FileState(name, reader, pages, len(content), max_calls_per_file)

    def list_files(self, path: str) -> List[Tuple[str, Optional[int]]]:
        """Every file under ``path`` as ``(stage reference, listed size)``. Listing the
        path itself failing stops the read: there is nothing to report per file."""
        return [
            (qualify_listed_name(path, row["name"]), listed_size(row))
            for row in self.session.sql(f"LIST {quote_stage_path(path)}").collect()
        ]

    def load_files(
        self, listed: List[Tuple[str, Optional[int]]], max_calls_per_file: int
    ) -> List[FileState]:
        """Load each listed file (see ``load_file``)."""
        return [self.load_file(name, max_calls_per_file, size) for name, size in listed]

    def extract_in_batches(
        self, listed: List[Tuple[str, Optional[int]]], max_calls_per_file: int
    ) -> List[FileState]:
        """Every listed file's ``FileState``, extracted one batch at a time (``batches``,
        ``MAX_BATCH_BYTES``): load the batch, plan it, extract it, then drop its readers
        before the next batch is read. The early stop (``should_abort``) counts the
        documents of every batch so far. Once the read has stopped, the files of the
        batches left are not read: each gets an error saying so."""
        files: List[FileState] = []
        stopped: Optional[str] = None
        for batch in batches(listed, MAX_BATCH_BYTES):
            if stopped is None:
                finished, failed, errors = document_outcomes(files, [])
                if should_abort(finished, failed):
                    stopped = abort_message(finished, failed, errors)
            if stopped is not None:
                files += [
                    FileState.failed_with(
                        name, size or 0, max_calls_per_file, f"not extracted: {stopped}"
                    )
                    for name, size in batch
                ]
                continue
            loaded = self.load_files(batch, max_calls_per_file)
            files += loaded
            tasks = self.plan_large_files(loaded)
            stopped = self.extract_until_done(files, tasks, max_calls_per_file)
            for file in loaded:
                file.reader = None
        return files

    def stage_part(self, file: FileState, start: int, end: int) -> Optional[str]:
        """Stage the pages, with the previous page as context, as their own PDF; None
        when the part is still too large."""
        from pypdf import PdfWriter

        if not file.splittable or (
            start == 1 and end == file.pages and file.size <= MAX_FILE_BYTES
        ):
            self.sent_bytes[(id(file), start, end)] = file.size
            return file.path
        writer = PdfWriter()
        for index in range(max(start - 2, 0), end):
            writer.add_page(file.reader.pages[index])
        buffer = io.BytesIO()
        writer.write(buffer)
        if buffer.tell() > MAX_FILE_BYTES and end > start:
            return None
        self.sent_bytes[(id(file), start, end)] = buffer.tell()
        buffer.seek(0)
        stem = os.path.splitext(os.path.basename(file.path))[0]
        digest = hashlib.md5(file.path.encode()).hexdigest()[:12]
        target = f"{self.work_stage}/document_parts/{digest}/{stem}_p{start}_{end}.pdf"
        self.session.file.put_stream(
            buffer, target, auto_compress=False, overwrite=True
        )
        return target

    def submit(
        self, file: FileState, predicate: str, staged: str, max_tokens: int
    ) -> Submitted:
        """Submit one AI_COMPLETE call for ``staged`` to the model in use and return it
        in flight; counts against ``file``'s call budget."""
        file.calls += 1
        model = self.model
        job = self.session.sql(
            _AI_COMPLETE_SQL,
            params=[model, predicate, staged, json.dumps({"max_tokens": max_tokens})],
        ).collect_nowait()
        return Submitted(job, model)

    @staticmethod
    def collect(
        submitted: Submitted,
    ) -> Tuple[Any, Optional[str], Optional[Dict[str, Any]]]:
        """The call's answer via ``read_answer``, or ``(None, str(exc), None)`` if it
        raised."""
        try:
            return read_answer(submitted.job.result()[0][0])
        except Exception as exc:
            return None, str(exc), None

    def plan_large_files(self, files: List[FileState]) -> List[Task]:
        """The first round of tasks. A large file gets planning-only calls on segments of
        it, and its tasks are the ranges they plan; a segment whose plan fails, or that is
        too large to plan, falls back to a first call that extracts and plans its own
        remainder. Every other file starts with its initial ranges. Planning calls count
        against the file's call cap like any other: a segment of a file at its cap gets
        no planning call and goes on as a first call, which the extraction loop reports
        as over the cap. At most ``MAX_CALLS_IN_FLIGHT`` planning calls are in flight."""
        planned = [f for f in files if f.splittable and is_large_file(f.pages, f.size)]
        tasks, segments = self.initial_tasks(files, planned)
        # A segment is planned again only after a throttled answer (at most
        # MAX_THROTTLE_RETRIES times) or an unknown model (at most once per model), so
        # these rounds plan every segment; any left over goes on as a first call.
        for _ in range(1 + MAX_THROTTLE_RETRIES + len(self.models)):
            if not segments:
                break
            self.wait_if_throttled()
            again: List[Tuple[FileState, int, int]] = []
            for chunk in chunks(segments, MAX_CALLS_IN_FLIGHT):
                jobs = self.submit_plans(chunk, tasks)
                again += self.collect_plans(jobs, tasks)
            segments = again
        tasks += [(file, a, b, "first") for file, a, b in segments]
        self.raise_planned_caps(planned, tasks)
        return tasks

    @staticmethod
    def initial_tasks(
        files: List[FileState], planned: List[FileState]
    ) -> Tuple[List[Task], List[Tuple[FileState, int, int]]]:
        """The tasks of every file that is not ``planned`` (its initial ranges), and the
        planning segments of every file that is. A file whose first round cannot be
        worked out gets that as its error, not the read's."""
        tasks: List[Task] = []
        segments: List[Tuple[FileState, int, int]] = []
        for file in files:
            if file.failed:
                continue
            try:
                if file in planned:
                    segments += [
                        (file, a, b)
                        for a, b in planning_segments(file.pages, file.size)
                    ]
                    continue
                first = first_round_budget(file.pages, file.size)
                ranges = (
                    initial_ranges(file.pages, file.size)
                    if file.splittable
                    else [(1, 1)]
                )
            except Exception as exc:
                file.errors.append(f"could not plan the file: {exc}")
                continue
            tasks += [(file, a, b, first) for a, b in ranges]
        return tasks, segments

    def submit_plans(
        self, chunk: List[Tuple[FileState, int, int]], tasks: List[Task]
    ) -> List[Tuple[FileState, int, int, Submitted]]:
        """Submit a planning call for each segment in ``chunk``. A segment that gets no
        call (its file is at its cap, or staging or submitting it fails) goes on to
        ``tasks`` as a first call."""
        plan = self.budgets["plan_first"]
        jobs = []
        for file, a, b in chunk:
            if file.calls >= file.cap:
                # Like a failed plan: the extraction loop gets the segment and records
                # the cap error for it.
                tasks.append((file, a, b, "first"))
                continue
            prompt = build_plan_predicate(
                self.predicate,
                a,
                b,
                file.pages,
                plan["range_target"],
                self.schema,
            )
            try:
                staged = self.stage_part(file, a, b)
                job = (
                    self.submit(file, prompt, staged, plan["max_tokens"])
                    if staged is not None
                    else None
                )
            except Exception:
                job = None  # the fallback below records the error if it recurs
            if job is None:
                tasks.append((file, a, b, "first"))
                continue
            jobs.append((file, a, b, job))
        return jobs

    def collect_plans(
        self, jobs: List[Tuple[FileState, int, int, Submitted]], tasks: List[Task]
    ) -> List[Tuple[FileState, int, int]]:
        """Add the ranges each planning call planned to ``tasks``, or the segment as a
        first call when the plan failed. Returns the segments to plan again: those
        throttled, or sent to a model AI_COMPLETE did not know."""
        again = []
        for file, a, b, submitted in jobs:
            value, error, _ = self.collect(submitted)
            if self.model_missing(file, submitted, error):
                again.append((file, a, b))  # plan it again with the new model
                continue
            if self.throttled(file, (id(file), a, b, 0), error):
                again.append((file, a, b))  # plan it again after the wait
                continue
            ranges = ranges_from_plan(value, a, b) if error is None else None
            if ranges:
                tasks += [(file, x, y, file.range_budget) for x, y in ranges]
            else:
                tasks.append((file, a, b, "first"))
        return again

    @staticmethod
    def raise_planned_caps(planned: List[FileState], tasks: List[Task]) -> None:
        """Raise the call cap of each planned file to cover the tasks its plan queued,
        up to ``HARD_MAX_CALLS_PER_FILE``."""
        for file in planned:
            queued = sum(1 for task in tasks if task[0] is file)
            file.cap = max(
                file.cap, min(HARD_MAX_CALLS_PER_FILE, file.calls + 2 * queued + 4)
            )

    def extract_until_done(
        self, files: List[FileState], tasks: List[Task], max_calls_per_file: int
    ) -> Optional[str]:
        """Run every task, at most ``MAX_CALLS_IN_FLIGHT`` calls at a time
        (``run_chunk``), then the tasks their answers leave (the rest of a range that did
        not fit, or the halves of one that failed) until none are left.
        A failure in staging a part, submitting a call or processing an answer is that
        file's error, and the other files go on; a throttled call is sent again after a
        wait (``throttled``). After each round, while work is still waiting, the read
        stops submitting if ``should_abort`` says too many finished documents failed: the
        files with ranges still waiting get an error for them (``stop_waiting``) and
        everything finished is kept, so the caller still gets one row per file. When
        nothing is waiting there is nothing to stop and the results are complete.
        Returns why the read stopped, or None when it did not."""
        while tasks:
            self.wait_if_throttled()
            next_round: List[Task] = []
            for chunk in chunks(tasks, MAX_CALLS_IN_FLIGHT):
                next_round += self.run_chunk(chunk, max_calls_per_file)
            tasks = next_round
            if tasks:
                finished, failed, errors = document_outcomes(files, tasks)
                if should_abort(finished, failed):
                    reason = abort_message(finished, failed, errors)
                    stop_waiting(tasks, reason)
                    return reason
        return None

    def run_chunk(self, chunk: List[Task], max_calls_per_file: int) -> List[Task]:
        """Submit the calls for ``chunk`` (at most ``MAX_CALLS_IN_FLIGHT`` tasks), collect
        them, and return the tasks they leave for the next round."""
        jobs, oversized = self.submit_calls(chunk)
        return oversized + self.collect_calls(jobs, max_calls_per_file)

    def submit_calls(
        self, chunk: List[Task]
    ) -> Tuple[List[Tuple[FileState, int, int, str, Submitted]], List[Task]]:
        """Submit a call for each task in ``chunk``. Returns the submitted calls, and the
        halves of each range too large to stage. A task whose file is at its cap, or
        whose part cannot be staged or submitted, gets that as its file's error."""
        jobs = []
        oversized: List[Task] = []
        for file, start, end, budget_name in chunk:
            if file.calls >= file.cap:
                file.errors.append(
                    f"pages {start}-{end} not extracted: reached {file.cap} calls"
                )
                continue
            try:
                staged = self.stage_part(file, start, end)
            except Exception as exc:
                file.errors.append(
                    f"pages {start}-{end}: could not stage part file: {exc}"
                )
                continue
            if staged is None:
                oversized += [(file, a, b, budget_name) for a, b in halves(start, end)]
                continue
            budget = self.budgets[budget_name]
            prompt = (
                build_predicate(
                    self.predicate,
                    budget["target"],
                    start,
                    end,
                    file.pages,
                    plan_target=self.budgets[file.range_budget]["target"],
                )
                + self.instruction
            )
            try:
                job = self.submit(file, prompt, staged, budget["max_tokens"])
            except Exception as exc:
                file.errors.append(
                    f"pages {start}-{end}: could not submit the AI_COMPLETE call: {exc}"
                )
                continue
            jobs.append((file, start, end, budget_name, job))
        return jobs, oversized

    def collect_calls(
        self,
        jobs: List[Tuple[FileState, int, int, str, Submitted]],
        max_calls_per_file: int,
    ) -> List[Task]:
        """Record each submitted call's answer (``finish``) and return the tasks they
        leave. A failure in processing an answer is that file's error; running out of
        models stops the read."""
        left: List[Task] = []
        for file, start, end, budget_name, job in jobs:
            try:
                left += self.finish(
                    file, start, end, budget_name, job, max_calls_per_file
                )
            except NoModelAvailable:
                raise
            except Exception as exc:
                file.errors.append(
                    f"pages {start}-{end}: could not process the answer: {exc}"
                )
        return left

    def finish(
        self,
        file: FileState,
        start: int,
        end: int,
        budget_name: str,
        submitted: Submitted,
        max_calls_per_file: int,
    ) -> List[Task]:
        """Record one call's answer and return the tasks it leaves. A call whose model
        AI_COMPLETE does not know, or that was throttled, leaves its own task, to be sent
        again."""
        value, error, salvaged = self.collect(submitted)
        if self.model_missing(file, submitted, error):
            return [(file, start, end, budget_name)]
        if self.throttled(file, (id(file), start, end), error):
            return [(file, start, end, budget_name)]
        value = unwrap_envelope(value, self.sent_schema)
        if not file.splittable:
            if error is not None:
                file.errors.append(error)
            else:
                file.parts.append((start, value))
            return []
        range_target = self.budgets[file.range_budget]["target"]
        if error is None and isinstance(value, dict):
            value = shift_page_fields(value, start, end)
            estimate = document_estimate(value, start, end, file.pages)
            if estimate is not None:
                file.cap = max(
                    file.cap, call_cap(max_calls_per_file, estimate, range_target)
                )
        keep, remaining = next_ranges(
            start,
            end,
            value,
            error,
            target=self.budgets["merge_target"],
            salvaged=salvaged,
            sent_bytes=self.sent_bytes.get((id(file), start, end)),
        )
        if keep:
            file.parts.append((start, value))
        elif not remaining:
            file.errors.append(f"pages {start}-{end}: {error}")
        # Ranges re-split from a failed call (halves, or along its salvaged plan) keep
        # its budget; ranges a successful call planned get the per-range budget.
        next_budget = file.range_budget if keep else budget_name
        return [(file, a, b, next_budget) for a, b in remaining]


def run(
    session: Session,
    path: str,
    response_format: str,
    predicate: str,
    models: str,
    call_budgets: str,
    max_calls_per_file: int,
    work_stage: str,
) -> DataFrame:
    """Extract the schema's fields from every file under ``path``; one output row per
    file, however many AI_COMPLETE calls it took.

    Args:
        path: stage path of a file or folder, e.g. ``@stage/invoices/``.
        response_format: the user's schema as ``{"schema": <JSON Schema>}``, JSON.
        predicate: the extraction prompt (the user's, or the default).
        models: the AI_COMPLETE model names the read may use, in order of preference,
            as a JSON list; the first is used until AI_COMPLETE reports it unknown, then
            the next, with the calls that failed sent again.
        call_budgets: output-token targets and limits per kind of call, JSON (see
            ``_CALL_BUDGETS`` in document_reader.py).
        max_calls_per_file: starting cap on calls per file; raised for files whose
            answers turn out large, up to HARD_MAX_CALLS_PER_FILE.
        work_stage: stage where page-range part files are written (the session stage).

    Raises:
        NoModelAvailable (a RuntimeError): AI_COMPLETE knows none of ``models`` (not
            enabled for the account or region); raised before any extraction is returned.

    Returns:
        STAGE_PATH, VALUE (the merged answer as JSON, or None when no part succeeded),
        ERROR (every failure for the file, joined with "; ", or None) and SCORES (the
        per-field confidence scores as JSON, see ``merge_scores``, or None). A failure
        reading, staging, submitting or merging is a file's own ERROR and does not
        stop the read. When more than ``MAX_FAILED_FRACTION`` of the readable PDFs finished
        so far failed (once ``MIN_DOCUMENTS_BEFORE_ABORT`` have), the read stops
        submitting: files with page ranges still waiting, and files of batches not yet
        read, say so in their ERROR, and every finished row is returned as it is.
    """
    from snowflake.snowpark.types import StringType, StructField, StructType

    extraction = DocumentExtraction(
        session,
        json.loads(response_format),
        predicate,
        json.loads(models),
        json.loads(call_budgets),
        work_stage,
    )
    # 1. List the files under the path, with their sizes.
    listed = extraction.list_files(path)
    # 2. For each batch of files: read them, plan the first page ranges (planned for
    # large files, whole for the rest), and extract until no range is left; each file
    # collects its parts and errors. Only one batch's bytes are held at a time.
    files = extraction.extract_in_batches(listed, max_calls_per_file)
    # 3. One row per file (see result_row), including files that failed.
    fields = list(extraction.schema.get("properties") or {})
    rows = [result_row(file, fields) for file in files]
    schema = StructType(
        [
            StructField("STAGE_PATH", StringType()),
            StructField("VALUE", StringType()),
            StructField("ERROR", StringType()),
            StructField("SCORES", StringType()),
        ]
    )
    return session.create_dataframe(rows, schema=schema)
