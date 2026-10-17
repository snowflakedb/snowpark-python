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


EMPTY_ANSWER_ERROR = "AI_COMPLETE returned an empty object"


def is_splittable_error(error: str) -> bool:
    """Errors that fewer pages can fix: a response cut off at max_tokens; an input
    Cortex rejects as too large, which claude-opus-5 reports only as "malformed
    request"; or an answer too long to generate, which json mode can return as an
    empty object with no error at all."""
    return (
        "unexpected end of JSON input" in error
        or "malformed request" in error
        or EMPTY_ANSWER_ERROR in error
    )


def is_empty_answer(value: Any) -> bool:
    return isinstance(value, dict) and all(_is_absent(v) for v in value.values())


# The largest scanned PDF claude-opus-5 accepted as one FILE was 12.63 MB; a 15.19 MB one
# was rejected every time.
MAX_FILE_BYTES = 12_000_000


# Above these, a first call that ignores its token target can run for many minutes before
# failing, so the first round gets the small "first" budget; below them a whole answer
# usually fits in one call, which the small budget would needlessly split.
BOUNDED_FIRST_CALL_MIN_PAGES = 50
BOUNDED_FIRST_CALL_MIN_BYTES = 10_000_000


def first_round_budget(total_pages: int, file_bytes: int) -> str:
    if (
        total_pages > BOUNDED_FIRST_CALL_MIN_PAGES
        or file_bytes > BOUNDED_FIRST_CALL_MIN_BYTES
    ):
        return "first"
    return "range"


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
    when the range starts mid-schedule -- and json mode rejects the whole response for
    one null in a non-nullable field."""
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
    factor: Optional[float] = None,
    target: Optional[int] = None,
) -> Tuple[bool, List[Tuple[int, int]]]:
    """Returns (keep_value, ranges_still_to_extract) for one finished call. With a
    ``factor`` (actual / estimated output tokens) and ``target``, adjacent planned
    ranges are coalesced up to ``target`` real tokens."""
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
    if factor is None or target is None:
        return True, covered
    return True, coalesce_ranges(
        [(a, b, estimates.get((a, b))) for a, b in covered], factor, target
    )


def coalesce_ranges(
    ranges: List[Tuple[int, int, Optional[int]]], factor: float, target: int
) -> List[Tuple[int, int]]:
    """Merge consecutive ranges while their corrected estimate stays within ``target``.
    The model overestimates its own output (1.2-2.6x observed), so its plan is far finer
    than the target asks for; a range without an estimate is never merged."""
    merged: List[Tuple[int, int, Optional[float]]] = []
    for a, b, estimate in ranges:
        scaled = estimate * factor if estimate is not None else None
        if (
            merged
            and scaled is not None
            and merged[-1][2] is not None
            and merged[-1][2] + scaled <= target
        ):
            merged[-1] = (merged[-1][0], b, merged[-1][2] + scaled)
        else:
            merged.append((a, b, scaled))
    return [(a, b) for a, b, _ in merged]


CHARS_PER_OUTPUT_TOKEN = 2.5  # measured on opus-5 JSON answers: 2.51-2.54


def observed_factor(value: Any) -> Optional[float]:
    """Actual / estimated output tokens for the pages one call extracted, or None when the
    call reported no usable estimate."""
    if not isinstance(value, dict):
        return None
    status = value.get(STATUS_FIELD)
    if not isinstance(status, dict):
        return None
    total = status.get("estimated_total_output_tokens")
    if not isinstance(total, int) or isinstance(total, bool) or total <= 0:
        return None
    remaining = 0
    if status.get("complete") is False:
        for planned in status.get("remaining_page_ranges") or []:
            estimate = (
                planned.get("estimated_output_tokens")
                if isinstance(planned, dict)
                else None
            )
            if isinstance(estimate, int) and not isinstance(estimate, bool):
                remaining += estimate
    done = total - remaining
    if done <= 0:
        return None
    answer = {k: v for k, v in value.items() if k != STATUS_FIELD}
    return len(json.dumps(answer)) / CHARS_PER_OUTPUT_TOKEN / done


def correction_factor(observed: List[float]) -> float:
    """Median of a file's observed factors, clamped; 1.0 (trust the model) until known."""
    if not observed:
        return 1.0
    ordered = sorted(observed)
    middle = (
        ordered[len(ordered) // 2]
        if len(ordered) % 2
        else (ordered[len(ordered) // 2 - 1] + ordered[len(ordered) // 2]) / 2
    )
    return min(max(middle, 0.2), 1.5)


HARD_MAX_CALLS_PER_FILE = 60


def call_cap(
    floor: int, estimated_total: Optional[int], factor: float, range_target: int
) -> int:
    """Calls a file may use: twice the corrected number of target-sized ranges its answer
    needs, plus slack, never below ``floor`` nor above HARD_MAX_CALLS_PER_FILE."""
    if not estimated_total:
        return floor
    needed = -(-int(estimated_total * factor) // range_target)
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


_AI_COMPLETE_SQL = (
    "SELECT AI_COMPLETE(model => ?, predicate => ?, file => TO_FILE(?), "
    "model_parameters => PARSE_JSON(?), response_format => PARSE_JSON(?), "
    "return_error_details => TRUE)"
)
# For models that reject response_format (claude-sonnet-5-5): the schema travels in the
# predicate and the answer comes back as text.
_AI_COMPLETE_TEXT_SQL = (
    "SELECT AI_COMPLETE(model => ?, predicate => ?, file => TO_FILE(?), "
    "model_parameters => PARSE_JSON(?), return_error_details => TRUE)"
)


def schema_instruction(schema: Dict[str, Any]) -> str:
    return (
        "Respond with ONLY one JSON object that conforms to the JSON Schema below: no "
        "prose before or after it and no markdown code fences. Fill extraction_status "
        "first.\n\nJSON Schema:\n" + json.dumps(schema, separators=(",", ":"))
    )


TRUNCATED_TEXT_ERROR = "unexpected end of JSON input in a text answer"


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
    reported with the truncation signature so its pages get split, like json mode's."""
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
        return None, "answer contained no JSON object"
    candidate = body[first:]
    try:
        value, _ = json.JSONDecoder().raw_decode(candidate)
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
        return None, f"answer was not valid JSON: {exc.msg}"
    if not isinstance(value, dict):
        return None, "answer JSON was not an object"
    return value, None


def run(
    session: Session,
    path: str,
    response_format: str,
    predicate: str,
    model: str,
    model_parameters: str,
    call_budgets: str,
    max_calls_per_file: int,
    work_stage: str,
) -> DataFrame:  # pragma: no cover - exercised against a live account
    from pypdf import PdfReader, PdfWriter
    from snowflake.snowpark.files import SnowflakeFile
    from snowflake.snowpark.types import StringType, StructField, StructType

    base_parameters = json.loads(model_parameters)
    budgets = json.loads(call_budgets)
    json_mode = budgets.get("json_mode", True)
    sent_format = with_status(json.loads(response_format))
    format_with_status = json.dumps(sent_format)
    instruction = (
        "" if json_mode else "\n\n" + schema_instruction(sent_format["schema"])
    )
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
    caps = {f: max_calls_per_file for f in files}
    observed: Dict[str, List[float]] = {f: [] for f in files}
    range_target = budgets["range"]["target"]
    # Models whose estimates are not measured as overestimates skip the correction, and
    # planned ranges are only coalesced up to merge_target, leaving headroom under max_tokens.
    correct_estimates = budgets.get("correct_estimates", True)
    merge_target = budgets.get("merge_target", range_target)

    def file_factor(file: str) -> float:
        return correction_factor(observed[file]) if correct_estimates else 1.0

    done: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {f: [] for f in files}
    pending = [
        (f, a, b, first_round_budget(totals[f], sizes[f]))
        for f in files
        for a, b in (
            initial_ranges(totals[f], sizes[f]) if readers[f] is not None else [(1, 1)]
        )
    ]
    while pending:
        jobs = []
        oversized = []
        for file, start, end, budget_name in pending:
            if calls[file] >= caps[file]:
                errors[file].append(
                    f"pages {start}-{end} not extracted: reached {caps[file]} calls"
                )
                continue
            staged = part_file(file, start, end)
            if staged is None:
                mid = (start + end) // 2
                oversized.extend(
                    [(file, start, mid, budget_name), (file, mid + 1, end, budget_name)]
                )
                continue
            calls[file] += 1
            budget = budgets[budget_name]
            # Targets are stated in the model's own (over)estimated units so it stops near
            # the real target; max_tokens stays in real tokens as the hard bound.
            factor = file_factor(file)
            params = [
                model,
                build_predicate(
                    predicate,
                    int(budget["target"] / factor),
                    start,
                    end,
                    totals[file],
                    plan_target=int(range_target / factor),
                )
                + instruction,
                staged,
                json.dumps({**base_parameters, "max_tokens": budget["max_tokens"]}),
            ] + ([format_with_status] if json_mode else [])
            jobs.append(
                (
                    file,
                    start,
                    end,
                    budget_name,
                    session.sql(
                        _AI_COMPLETE_SQL if json_mode else _AI_COMPLETE_TEXT_SQL,
                        params=params,
                    ).collect_nowait(),
                )
            )
        pending = oversized
        for file, start, end, budget_name, job in jobs:
            try:
                raw = job.result()[0][0]
                result = json.loads(raw) if isinstance(raw, str) else raw
                value, error = result.get("value"), result.get("error")
                if error is None and not json_mode:
                    value, error = parse_text_answer(value)
                value = unwrap_envelope(value, sent_format["schema"])
            except Exception as exc:
                value, error = None, str(exc)
            if error is None and is_empty_answer(value):
                value, error = None, EMPTY_ANSWER_ERROR
            if readers[file] is None:
                if error is not None:
                    errors[file].append(error)
                else:
                    done[file].append((start, value))
                continue
            factor = observed_factor(value) if error is None else None
            if factor is not None:
                observed[file].append(factor)
            if error is None and isinstance(value, dict):
                value = shift_page_fields(value, start, end)
                estimate = document_estimate(value, start, end, totals[file])
                if estimate is not None:
                    caps[file] = max(
                        caps[file],
                        call_cap(max_calls_per_file, estimate, 1.0, range_target),
                    )
            keep, remaining = next_ranges(
                start,
                end,
                value,
                error,
                factor=file_factor(file),
                target=merge_target,
            )
            if keep:
                done[file].append((start, value))
            elif not remaining:
                errors[file].append(f"pages {start}-{end}: {error}")
            # Halves of a failed call know no more than it did, so they keep its budget;
            # ranges a call planned get the larger per-range budget.
            next_budget = "range" if keep else budget_name
            pending.extend((file, a, b, next_budget) for a, b in remaining)

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
