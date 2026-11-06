#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Code that runs inside every procedure or UDF that backs a Snowpark MCP tool.

``make_handler`` wraps the user's tool function; the wrapper is what gets registered.
It converts the function's result to JSON-safe values, keeps it under the MCP server's
response limit, and turns a ``ToolError`` into an error result the model can read.

This module is pickled by value together with each tool (see ``mcp_utils``), so the
warehouse never imports it by name: tools keep working where the installed Snowpark is
older than the client that deployed them. For that reason it imports only the standard
library, and recognizes Snowpark and pandas objects by their attributes.
"""

import base64
import datetime
import decimal
import inspect
import json
import logging
import math
import sys
from typing import Any, Callable, Dict, Iterable, List, Optional

DEFAULT_MAX_ROWS = 1000
# Below the 250 KB at which the MCP server truncates GENERIC tool results, so the
# model is told a result was cut instead of receiving broken JSON.
DEFAULT_MAX_RESULT_BYTES = 240_000
PLATFORM_RESULT_LIMIT_BYTES = 250_000

_logger = logging.getLogger("snowflake.snowpark.mcp")


class ToolError(Exception):
    """Raise in a tool to return an error the model can read and act on.

    The tool returns ``{"error": {"type": "ToolError", "message": ..., "hint": ...}}``
    instead of failing. Use it for expected failures, such as an unknown ID; any other
    exception makes the tool call fail.

    Args:
        message: What went wrong, written for the model.
        hint: Optional suggestion for what to try instead.
    """

    # The class is pickled by value with each tool, so on the warehouse it is a copy
    # of this class rather than this class; the runtime recognizes it by this marker.
    _snowpark_mcp_tool_error = True

    def __init__(self, message: str, hint: Optional[str] = None) -> None:
        super().__init__(message)
        self.message = message
        self.hint = hint


def error_result(
    error_type: str, message: str, hint: Optional[str] = None
) -> Dict[str, Any]:
    error = {"type": error_type, "message": message}
    if hint:
        error["hint"] = hint
    return {"error": error}


def to_json_value(value: Any) -> Any:
    """Converts a value returned by a tool to values ``json.dumps`` accepts."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # NaN and infinity are not valid JSON.
        return value if math.isfinite(value) else None
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            return None
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        # pandas NaT is a datetime that is not equal to itself.
        return None if value != value else value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if _is_row(value):
        return {str(k): to_json_value(v) for k, v in value.asDict().items()}
    if isinstance(value, dict):
        return {str(k): to_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_json_value(v) for v in value]
    if hasattr(value, "item") and callable(value.item):
        # numpy scalars
        return to_json_value(value.item())
    raise TypeError(
        f"A tool returned a value of type {type(value).__name__}, which cannot be "
        "converted to JSON. Return a dict, list, str, number, bool, None, a Row, or a "
        "DataFrame."
    )


def _is_row(value: Any) -> bool:
    return isinstance(value, tuple) and callable(getattr(value, "asDict", None))


def _is_snowpark_dataframe(value: Any) -> bool:
    return all(
        callable(getattr(value, attr, None)) for attr in ("collect", "limit")
    ) and hasattr(value, "schema")


def _is_pandas_dataframe(value: Any) -> bool:
    return (
        callable(getattr(value, "to_dict", None))
        and callable(getattr(value, "head", None))
        and hasattr(value, "columns")
        and hasattr(value, "iloc")
    )


def _json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


def table_result(
    columns: List[str],
    rows: Iterable[Dict[str, Any]],
    max_rows: int,
    max_result_bytes: int,
) -> Dict[str, Any]:
    """Builds ``{"columns", "rows", "row_count", "truncated"}`` from at most
    ``max_rows + 1`` rows, keeping the rows that fit in ``max_result_bytes``."""
    result = {"columns": columns, "rows": [], "row_count": 0, "truncated": False}
    # Bytes used by everything except the rows, with "truncated" set to true so the
    # final result is never larger than what was measured.
    size = _json_size({**result, "row_count": max_rows, "truncated": True})
    kept = result["rows"]
    for row in rows:
        if len(kept) >= max_rows:
            result["truncated"] = True
            break
        row = to_json_value(row)
        row_size = _json_size(row) + (1 if kept else 0)  # the separating comma
        if size + row_size > max_result_bytes:
            result["truncated"] = True
            break
        kept.append(row)
        size += row_size
    result["row_count"] = len(kept)
    return result


def to_tool_result(value: Any, max_rows: int, max_result_bytes: int) -> Any:
    """Converts what a tool function returned to the JSON value the tool returns."""
    if _is_snowpark_dataframe(value):
        rows = value.limit(max_rows + 1).collect()
        columns = list(rows[0].asDict()) if rows else list(value.schema.names)
        return table_result(
            columns, (row.asDict() for row in rows), max_rows, max_result_bytes
        )
    if _is_pandas_dataframe(value):
        records = value.head(max_rows + 1).to_dict(orient="records")
        return table_result(
            [str(c) for c in value.columns], records, max_rows, max_result_bytes
        )

    result = to_json_value(value)
    size = _json_size(result)
    if size > max_result_bytes:
        return error_result(
            "RESULT_TOO_LARGE",
            f"The result is {size} bytes, more than the {max_result_bytes} bytes this "
            "tool can return.",
            "Ask for fewer rows or fields, or narrow the request.",
        )
    return result


def run_tool(
    func: Callable,
    args: tuple,
    kwargs: dict,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
    mask_error_details: bool = False,
) -> Any:
    """Calls ``func`` and converts its result. A ``ToolError`` becomes an error
    result. Any other exception is logged and raised, so the MCP server records the
    call as failed; with ``mask_error_details``, it is raised as a generic error."""
    name = getattr(func, "__name__", "tool")
    try:
        return to_tool_result(func(*args, **kwargs), max_rows, max_result_bytes)
    except Exception as exc:
        if getattr(exc, "_snowpark_mcp_tool_error", False):
            return error_result(
                "ToolError",
                getattr(exc, "message", str(exc)),
                getattr(exc, "hint", None),
            )
        # Logged so the traceback reaches the event table.
        _logger.exception("MCP tool %s failed", name)
        if not mask_error_details:
            raise
        # The MCP server sends a failed call's Python error to the client, with the
        # traceback (file paths and code of the tool, Snowpark and the connector)
        # unless the traceback limit is 0.
        sys.tracebacklimit = 0
        raise RuntimeError(
            f"Tool {name} failed with an unexpected error. The details were logged."
        ) from None


def make_handler(
    func: Callable,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
    mask_error_details: bool = False,
) -> Callable:
    """Returns the function to register for a tool.

    It has ``func``'s signature, so Snowpark registers the procedure or UDF with the
    same parameter names and defaults, but no annotations: the registered return type
    is always VARIANT, whatever ``func`` is annotated to return.
    """

    def handler(*args, **kwargs):
        return run_tool(
            func, args, kwargs, max_rows, max_result_bytes, mask_error_details
        )

    handler.__name__ = func.__name__
    handler.__qualname__ = func.__qualname__
    handler.__doc__ = func.__doc__
    handler.__signature__ = inspect.signature(func).replace(
        parameters=[
            p.replace(annotation=inspect.Parameter.empty)
            for p in inspect.signature(func).parameters.values()
        ],
        return_annotation=inspect.Signature.empty,
    )
    return handler
