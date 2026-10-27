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
   failed one), until nothing is left.
4. back in ``run``: merge each file's parts, in page order, into one row.

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
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from snowflake.snowpark import DataFrame, Session

STATUS_FIELD = "extraction_status"

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


EMPTY_ANSWER_ERROR = "AI_COMPLETE returned an empty object"


def is_splittable_error(error: str) -> bool:
    """Errors that fewer pages can fix: a response cut off at max_tokens; an input
    Cortex rejects as too large, which it reports only as "malformed request"; an empty
    object; or an answer that does not parse as JSON."""
    return (
        "unexpected end of JSON input" in error
        or "malformed request" in error
        or EMPTY_ANSWER_ERROR in error
        or UNPARSEABLE_TEXT_ERROR_PREFIX in error
        or NO_JSON_TEXT_ERROR in error
    )


def is_empty_answer(value: Any) -> bool:
    """True when ``value`` is an object whose every field is absent (see ``_is_absent``)."""
    return isinstance(value, dict) and all(_is_absent(v) for v in value.values())


# AI_COMPLETE accepts a FILE up to about 15 MB (claude-sonnet-5-5 took 14.99 MB and
# rejected 15.19 MB with a bare "malformed request"); parts stay well under that.
MAX_FILE_BYTES = 12_000_000


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


def with_status(response_format: Dict[str, Any]) -> Dict[str, Any]:
    """``response_format`` with every scalar made nullable (see ``nullable_scalars``),
    ``_STATUS_SCHEMA`` added as a leading field, and unsupported keywords dropped -- the
    schema actually sent to the model."""
    wrapped = nullable_scalars(response_format["schema"])
    wrapped["properties"] = {
        STATUS_FIELD: _STATUS_SCHEMA,
        **wrapped.get("properties", {}),
    }
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
    stay within ``max_pages`` pages and (from the average page size) ``max_bytes``."""
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

    return {k: (v if k == STATUS_FIELD else shift(v)) for k, v in value.items()}


def next_ranges(
    start: int,
    end: int,
    value: Optional[Dict[str, Any]],
    error: Optional[str],
    target: Optional[int] = None,
    salvaged: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, List[Tuple[int, int]]]:
    """Returns (keep_value, ranges_still_to_extract) for one finished call. With a
    ``target``, adjacent planned ranges are coalesced up to ``target`` estimated tokens.
    A failed call's ``salvaged`` status, when usable, re-splits its pages along its own
    plan instead of in halves."""
    if error is not None or not isinstance(value, dict):
        if end > start and (error is None or is_splittable_error(error)):
            if salvaged is not None:
                ranges = ranges_from_salvaged_status(start, end, salvaged, target)
                if ranges:
                    return False, ranges
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
        part = {k: v for k, v in part.items() if k != STATUS_FIELD}
        merged = merge(merged, part)
    return merged


def qualify_listed_name(path: str, listed_name: str) -> str:
    """LIST returns '<stage name>/<relative path>' without database or schema; rebuild a
    reference that resolves regardless of the session's current schema."""
    stage = path.lstrip("@").split("/", 1)[0]
    relative = listed_name.split("/", 1)[1] if "/" in listed_name else listed_name
    return f"@{stage}/{relative}"


# claude-sonnet-5-5 rejects response_format, so the schema travels in the predicate and
# the answer comes back as text.
_AI_COMPLETE_SQL = (
    "SELECT AI_COMPLETE(model => ?, predicate => ?, file => TO_FILE(?), "
    "model_parameters => PARSE_JSON(?), return_error_details => TRUE)"
)


# A model-availability probe: no file, one output token.
_AI_COMPLETE_PING_SQL = (
    "SELECT AI_COMPLETE(model => ?, prompt => ?, model_parameters => PARSE_JSON(?))"
)


def schema_instruction(schema: Dict[str, Any]) -> str:
    """The instruction telling the model to answer with one JSON object conforming to
    ``schema`` and nothing else."""
    return (
        "Respond with ONLY one JSON object that conforms to the JSON Schema below: no "
        "prose before or after it and no markdown code fences. Fill extraction_status "
        "first.\n\nJSON Schema:\n" + json.dumps(schema, separators=(",", ":"))
    )


TRUNCATED_TEXT_ERROR = "unexpected end of JSON input in a text answer"
UNPARSEABLE_TEXT_ERROR_PREFIX = "answer was not valid JSON"
NO_JSON_TEXT_ERROR = "answer contained no JSON object"


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
    reported with the truncation signature so its pages get split."""
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
    if not isinstance(value, dict):
        return None, "answer JSON was not an object"
    return value, None


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
    is not a readable PDF: it is extracted whole, as one page, and never split."""

    path: str
    reader: Any
    pages: int
    size: int
    cap: int
    calls: int = 0
    parts: List[Tuple[int, Dict[str, Any]]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def splittable(self) -> bool:
        """Whether this file can be cut into page ranges, i.e. it parsed as a PDF."""
        return self.reader is not None

    @property
    def range_budget(self) -> str:
        """This file's budget name for its ranges after the first round."""
        return range_budget(self.pages, self.size)


# A page range waiting for a call: (file, first page, last page, budget name).
Task = Tuple[FileState, int, int, str]


class DocumentExtraction:  # pragma: no cover - exercised against a live account
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
        candidates in order of preference; ``check_model`` picks the one in use."""
        self.session = session
        self.schema = response_format["schema"]
        self.sent_schema = with_status(response_format)["schema"]
        self.instruction = "\n\n" + schema_instruction(self.sent_schema)
        self.predicate = predicate
        self.models = models
        self.model = models[0]
        self.budgets = budgets
        self.work_stage = work_stage

    def check_model(self) -> None:
        """Choose the model for this read, before any file is touched: the first of
        ``models`` that AI_COMPLETE knows, found with one call each (no file,
        max_tokens 1). When it knows none, every call would fail the same way, so the
        read stops. Any other failure, such as throttling, does not rule a model out."""
        for model in self.models:
            try:
                self.session.sql(
                    _AI_COMPLETE_PING_SQL,
                    params=[model, "Reply with OK.", json.dumps({"max_tokens": 1})],
                ).collect()
            except Exception as exc:
                if is_unknown_model_error(str(exc)):
                    continue
            self.model = model
            return
        raise RuntimeError(no_model_message(self.models))

    def load_files(self, path: str, max_calls_per_file: int) -> List[FileState]:
        """List every file under ``path``, read it, and parse it as a PDF when possible;
        a file that is not a readable PDF becomes a one-page, unsplittable ``FileState``."""
        from pypdf import PdfReader
        from snowflake.snowpark.files import SnowflakeFile

        files = []
        for row in self.session.sql(f"LIST {path}").collect():
            name = qualify_listed_name(path, row["name"])
            with SnowflakeFile.open(name, "rb", require_scoped_url=False) as f:
                content = f.read()
            try:
                reader = PdfReader(io.BytesIO(content))
                pages = len(reader.pages)
            except Exception:
                reader, pages = None, 1
            files.append(
                FileState(name, reader, pages, len(content), max_calls_per_file)
            )
        return files

    def stage_part(self, file: FileState, start: int, end: int) -> Optional[str]:
        """Stage the pages, with the previous page as context, as their own PDF; None
        when the part is still too large."""
        from pypdf import PdfWriter

        if not file.splittable or (
            start == 1 and end == file.pages and file.size <= MAX_FILE_BYTES
        ):
            return file.path
        writer = PdfWriter()
        for index in range(max(start - 2, 0), end):
            writer.add_page(file.reader.pages[index])
        buffer = io.BytesIO()
        writer.write(buffer)
        if buffer.tell() > MAX_FILE_BYTES and end > start:
            return None
        buffer.seek(0)
        stem = os.path.splitext(os.path.basename(file.path))[0]
        digest = hashlib.md5(file.path.encode()).hexdigest()[:12]
        target = f"{self.work_stage}/document_parts/{digest}/{stem}_p{start}_{end}.pdf"
        self.session.file.put_stream(
            buffer, target, auto_compress=False, overwrite=True
        )
        return target

    def submit(self, file: FileState, predicate: str, staged: str, max_tokens: int):
        """Submit one AI_COMPLETE call for ``staged`` and return its async job; counts
        against ``file``'s call budget."""
        file.calls += 1
        return self.session.sql(
            _AI_COMPLETE_SQL,
            params=[
                self.model,
                predicate,
                staged,
                json.dumps({"max_tokens": max_tokens}),
            ],
        ).collect_nowait()

    @staticmethod
    def collect(job) -> Tuple[Any, Optional[str], Optional[Dict[str, Any]]]:
        """``job``'s answer via ``read_answer``, or ``(None, str(exc), None)`` if it
        raised."""
        try:
            return read_answer(job.result()[0][0])
        except Exception as exc:
            return None, str(exc), None

    def plan_large_files(self, files: List[FileState]) -> List[Task]:
        """The first round of tasks. A large file gets planning-only calls on segments of
        it, and its tasks are the ranges they plan; a segment whose plan fails, or that is
        too large to plan, falls back to a first call that extracts and plans its own
        remainder. Every other file starts with its initial ranges."""
        plan = self.budgets["plan_first"]
        tasks: List[Task] = []
        planned = [f for f in files if f.splittable and is_large_file(f.pages, f.size)]
        for file in files:
            if file in planned:
                continue
            first = first_round_budget(file.pages, file.size)
            ranges = (
                initial_ranges(file.pages, file.size) if file.splittable else [(1, 1)]
            )
            tasks += [(file, a, b, first) for a, b in ranges]
        jobs = []
        for file in planned:
            for a, b in planning_segments(file.pages, file.size):
                staged = self.stage_part(file, a, b)
                if staged is None:
                    tasks.append((file, a, b, "first"))
                    continue
                prompt = build_plan_predicate(
                    self.predicate, a, b, file.pages, plan["range_target"], self.schema
                )
                jobs.append(
                    (file, a, b, self.submit(file, prompt, staged, plan["max_tokens"]))
                )
        for file, a, b, job in jobs:
            value, error, _ = self.collect(job)
            ranges = ranges_from_plan(value, a, b) if error is None else None
            if ranges:
                tasks += [(file, x, y, file.range_budget) for x, y in ranges]
            else:
                tasks.append((file, a, b, "first"))
        for file in planned:
            queued = sum(1 for task in tasks if task[0] is file)
            file.cap = max(
                file.cap, min(HARD_MAX_CALLS_PER_FILE, file.calls + 2 * queued + 4)
            )
        return tasks

    def extract_until_done(self, tasks: List[Task], max_calls_per_file: int) -> None:
        """Run every task at once, then the tasks their answers leave (the rest of a
        range that did not fit, or the halves of one that failed) until none are left."""
        while tasks:
            jobs = []
            oversized: List[Task] = []
            for file, start, end, budget_name in tasks:
                if file.calls >= file.cap:
                    file.errors.append(
                        f"pages {start}-{end} not extracted: reached {file.cap} calls"
                    )
                    continue
                staged = self.stage_part(file, start, end)
                if staged is None:
                    mid = (start + end) // 2
                    oversized += [
                        (file, start, mid, budget_name),
                        (file, mid + 1, end, budget_name),
                    ]
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
                jobs.append(
                    (
                        file,
                        start,
                        end,
                        budget_name,
                        self.submit(file, prompt, staged, budget["max_tokens"]),
                    )
                )
            tasks = oversized
            for file, start, end, budget_name, job in jobs:
                tasks += self.finish(
                    file, start, end, budget_name, job, max_calls_per_file
                )

    def finish(
        self,
        file: FileState,
        start: int,
        end: int,
        budget_name: str,
        job,
        max_calls_per_file: int,
    ) -> List[Task]:
        """Record one call's answer and return the tasks it leaves."""
        value, error, salvaged = self.collect(job)
        value = unwrap_envelope(value, self.sent_schema)
        if error is None and is_empty_answer(value):
            value, error = None, EMPTY_ANSWER_ERROR
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
            as a JSON list; the first one AI_COMPLETE knows is used.
        call_budgets: output-token targets and limits per kind of call, JSON (see
            ``_CALL_BUDGETS`` in document_reader.py).
        max_calls_per_file: starting cap on calls per file; raised for files whose
            answers turn out large, up to HARD_MAX_CALLS_PER_FILE.
        work_stage: stage where page-range part files are written (the session stage).

    Raises:
        RuntimeError: AI_COMPLETE knows none of ``models`` (not enabled for the account
            or region); raised before any file is read.

    Returns:
        STAGE_PATH, VALUE (the merged answer as JSON, or None when no part succeeded)
        and ERROR (every failure for the file, joined with "; ", or None).
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
    # 0. Pick the first available model; fail the whole read at once if none is.
    extraction.check_model()
    # 1. Read every file once, for its page count and size.
    files = extraction.load_files(path, max_calls_per_file)
    # 2. Decide the first page ranges: planned for large files, whole for the rest.
    tasks = extraction.plan_large_files(files)
    # 3. Extract until no range is left; each file collects its parts and errors.
    extraction.extract_until_done(tasks, max_calls_per_file)
    # 4. One row per file: its parts merged in page order, so a table split across
    #    ranges comes back as one table.
    rows = []
    for file in files:
        parts = [value for _, value in sorted(file.parts, key=lambda part: part[0])]
        rows.append(
            [
                file.path,
                json.dumps(merge_values(parts)) if parts else None,
                "; ".join(file.errors) if file.errors else None,
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
