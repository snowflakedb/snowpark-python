#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Handlers of the ready-made tools in ``snowflake.snowpark.mcp.tools``.

``make_run_sql`` builds the governed SQL tool: one statement per call, read-only
unless configured otherwise, with row and size limits and one structured log record
per call. ``make_log_feedback`` builds the feedback tools that write to a table.

Pickled by value like ``mcp_tool_runtime``, so it imports only the standard library.
Every function except the ``make_*`` handlers is pure and unit-tested directly.
"""

import json
import logging
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

_logger = logging.getLogger("snowflake.snowpark.mcp.governed_sql")

READ_ONLY_KEYWORDS = ("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN")
# Statements whose results Snowpark cannot wrap in a subquery, so their rows are
# limited after they are fetched instead of in the query.
_UNWRAPPABLE_KEYWORDS = ("SHOW", "DESCRIBE", "DESC", "EXPLAIN", "LIST", "LS")


def strip_comments_and_strings(sql: str) -> str:
    """Returns ``sql`` with comments removed and the contents of string literals and
    quoted identifiers replaced by spaces, so keywords and semicolons can be found
    lexically. Raises ``ValueError`` on an unterminated literal or comment."""
    out: List[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        two = sql[i : i + 2]
        if two == "--" or two == "//":
            end = sql.find("\n", i)
            i = n if end < 0 else end
        elif two == "/*":
            end = sql.find("*/", i + 2)
            if end < 0:
                raise ValueError("The query has an unterminated /* comment.")
            out.append(" ")
            i = end + 2
        elif two == "$$":
            end = sql.find("$$", i + 2)
            if end < 0:
                raise ValueError("The query has an unterminated $$ string.")
            out.append("''")
            i = end + 2
        elif ch in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == "\\" and ch == "'":
                    j += 2
                    continue
                if sql[j] == ch:
                    if j + 1 < n and sql[j + 1] == ch:  # doubled quote
                        j += 2
                        continue
                    break
                j += 1
            if j >= n:
                raise ValueError("The query has an unterminated quoted string.")
            out.append(ch + ch)
            i = j + 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def check_statement(sql: str, read_only: bool) -> Tuple[str, str]:
    """Checks that ``sql`` is one statement, and a read-only one if ``read_only``.

    Returns the statement without a trailing semicolon and its first keyword.
    Raises ``ValueError`` with a message for the model otherwise. The check is
    lexical: the role the tool runs with is what prevents writes.
    """
    stripped = strip_comments_and_strings(sql).strip()
    if not stripped:
        raise ValueError("The query is empty.")
    body = stripped.rstrip(";").rstrip()
    if not body:
        raise ValueError("The query is empty.")
    if ";" in body:
        raise ValueError("Send one statement at a time.")
    match = re.match(r"[\s(]*([A-Za-z_]+)", body)
    if not match:
        raise ValueError("The query does not start with a SQL keyword.")
    keyword = match.group(1).upper()
    if read_only and keyword not in READ_ONLY_KEYWORDS:
        raise ValueError(
            f"Only read-only statements are allowed ({', '.join(READ_ONLY_KEYWORDS)}); "
            f"this one starts with {keyword}."
        )
    statement = sql.strip()
    while statement.endswith(";"):
        statement = statement[:-1].rstrip()
    return statement, keyword


def make_run_sql(
    read_only: bool, max_rows: int, max_result_bytes: int, run_tool: Callable
) -> Callable:
    """Returns the tool function of ``tools.governed_sql``; ``run_tool`` is
    ``mcp_tool_runtime.run_tool``, passed in so this module stays standalone."""

    def run_sql(session, query: str) -> Any:
        started = time.monotonic()
        record: Dict[str, Any] = {"query": query}
        try:
            statement, keyword = check_statement(query, read_only)
        except ValueError as e:
            record.update(status="rejected", error=str(e))
            _logger.info(json.dumps(record))
            return {"error": {"type": "QUERY_REJECTED", "message": str(e)}}

        def execute():
            df = session.sql(statement)
            if keyword in _UNWRAPPABLE_KEYWORDS:
                # A list of Rows: run_tool cuts it to max_rows below.
                return _RowList(df.collect())
            return df

        try:
            result = run_tool(lambda: execute(), (), {}, max_rows, max_result_bytes)
        except Exception as e:
            record.update(
                status="failed",
                error=str(e),
                duration_ms=int((time.monotonic() - started) * 1000),
            )
            _logger.info(json.dumps(record))
            return {"error": {"type": "QUERY_FAILED", "message": str(e)}}
        record.update(
            status="ok",
            rows=result.get("row_count") if isinstance(result, dict) else None,
            truncated=result.get("truncated") if isinstance(result, dict) else None,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        _logger.info(json.dumps(record))
        return result

    run_sql.__doc__ = (
        "Run one SQL statement in Snowflake and return the result rows."
        + (" Only read-only statements are allowed." if read_only else "")
        + f" At most {max_rows} rows are returned; the result says when it was cut."
        "\n\nArgs:\n    query: One Snowflake SQL statement."
    )
    return run_sql


class _RowList(list):
    """Rows already fetched, presented like a DataFrame so the runtime builds a
    table result from them."""

    class _NoColumns:
        names: List[str] = []

    schema = _NoColumns()

    def limit(self, n: int) -> "_RowList":
        return _RowList(self[:n])

    def collect(self) -> list:
        return list(self)


def make_log_feedback(table: str, kind: str) -> Callable:
    """Returns the tool function of a feedback tool that inserts into ``table``."""

    def log(session, message: str, details: Optional[str] = None) -> Dict[str, Any]:
        # details is JSON text: the MCP server cannot pass JSON objects to a tool.
        # Text that is not a JSON object is kept as {"text": ...}.
        try:
            parsed = json.loads(details) if details else {}
        except ValueError:
            parsed = None
        if not isinstance(parsed, dict):
            parsed = {"text": details}
        session.sql(
            f"INSERT INTO {table} (KIND, MESSAGE, DETAILS, LOGGED_BY) "
            "SELECT ?, ?, PARSE_JSON(?), CURRENT_USER()",
            params=[kind, message, json.dumps(parsed)],
        ).collect()
        return {"status": "recorded"}

    return log


def feedback_table_ddl(table: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), "
        "KIND VARCHAR, MESSAGE VARCHAR, DETAILS VARIANT, LOGGED_BY VARCHAR)"
    )
