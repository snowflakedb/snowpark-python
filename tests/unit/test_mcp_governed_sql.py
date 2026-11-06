#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect
import json
import logging

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.mcp_governed_sql import (
    check_statement,
    feedback_table_ddl,
    make_log_feedback,
    make_run_sql,
    strip_comments_and_strings,
)
from snowflake.snowpark._internal.mcp_tool_runtime import run_tool

# ---------------------------------------------------------------------------
# strip_comments_and_strings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 1", "SELECT 1"),
        ("SELECT 'a;b'", "SELECT ''"),
        ('SELECT 1 AS "a;b"', 'SELECT 1 AS ""'),
        ("SELECT 1 -- ; drop\nFROM t", "SELECT 1 \nFROM t"),
        ("SELECT 1 // ; drop", "SELECT 1 "),
        ("SELECT /* ; */ 1", "SELECT   1"),
        ("SELECT 'it''s; ok'", "SELECT ''"),
        ('SELECT "a""b;c"', 'SELECT ""'),
        ("SELECT $$a; 'b$$", "SELECT ''"),
        (r"SELECT 'it\'s; ok'", "SELECT ''"),
        ("SELECT 1 /* a */ + /* b */ 2", "SELECT 1   +   2"),
    ],
)
def test_strip_comments_and_strings(sql, expected):
    assert strip_comments_and_strings(sql) == expected


@pytest.mark.parametrize(
    "sql, message",
    [
        ("SELECT 'abc", "unterminated quoted"),
        ('SELECT "abc', "unterminated quoted"),
        ("SELECT /* abc", "unterminated /\\*"),
        ("SELECT $$abc", "unterminated \\$\\$"),
        ("SELECT 'a''", "unterminated quoted"),
    ],
)
def test_strip_comments_and_strings_unterminated(sql, message):
    with pytest.raises(ValueError, match=message):
        strip_comments_and_strings(sql)


# ---------------------------------------------------------------------------
# check_statement
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, keyword",
    [
        ("SELECT 1", "SELECT"),
        ("select 1", "SELECT"),
        ("  SELECT 1  ", "SELECT"),
        ("WITH x AS (SELECT 1) SELECT * FROM x", "WITH"),
        ("SHOW TABLES", "SHOW"),
        ("DESCRIBE TABLE t", "DESCRIBE"),
        ("DESC TABLE t", "DESC"),
        ("EXPLAIN SELECT 1", "EXPLAIN"),
        ("(SELECT 1)", "SELECT"),
        ("((SELECT 1) UNION (SELECT 2))", "SELECT"),
        ("-- comment\nSELECT 1", "SELECT"),
        ("/* c */ SELECT 1", "SELECT"),
        ("SELECT ';'", "SELECT"),
        ('SELECT 1 AS "a;b"', "SELECT"),
        ("SELECT 1 -- ; DROP TABLE t", "SELECT"),
        ("SELECT 1 /* ; DROP TABLE t */", "SELECT"),
        ("SELECT 'it''s; fine'", "SELECT"),
        ("SELECT $$; DROP TABLE t;$$", "SELECT"),
        ("SELECT 1\n-- trailing", "SELECT"),
    ],
)
def test_check_statement_accepts_read_only(sql, keyword):
    statement, found = check_statement(sql, read_only=True)
    assert found == keyword
    assert statement == sql.strip()


def test_check_statement_strips_trailing_semicolons():
    assert check_statement("select 1;", True) == ("select 1", "SELECT")
    assert check_statement("select 1 ; ", True) == ("select 1", "SELECT")
    assert check_statement("select 1;;", True) == ("select 1", "SELECT")


def test_check_statement_keeps_original_text():
    statement, _ = check_statement("SELECT 'a;b' -- c\n;", True)
    assert statement == "SELECT 'a;b' -- c"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; DROP TABLE t",
        "SELECT 1;DROP TABLE t",
        "SELECT 1 /* x */; DROP TABLE t",
        "SELECT 1 -- c\n; DROP TABLE t",
        "SELECT 'a'; SELECT 'b'",
        "SELECT 'it''s'; DROP TABLE t",
        "SELECT $$x$$; DROP TABLE t",
        r"SELECT 'a\\'; DROP TABLE t",
    ],
)
@pytest.mark.parametrize("read_only", [True, False])
def test_check_statement_rejects_multiple_statements(sql, read_only):
    with pytest.raises(ValueError, match="one statement"):
        check_statement(sql, read_only)


@pytest.mark.parametrize(
    "sql, keyword",
    [
        ("-- c\nDELETE FROM t", "DELETE"),
        ("DROP TABLE t", "DROP"),
        ("insert into t values (1)", "INSERT"),
        ("CREATE TABLE t (a int)", "CREATE"),
        ("CALL p()", "CALL"),
        ("LIST @s", "LIST"),
        ("/* SELECT */ UPDATE t SET a = 1", "UPDATE"),
        ("(DELETE FROM t)", "DELETE"),
    ],
)
def test_check_statement_rejects_writes_when_read_only(sql, keyword):
    with pytest.raises(ValueError, match=f"starts with {keyword}"):
        check_statement(sql, read_only=True)


def test_check_statement_accepts_writes_when_not_read_only():
    assert check_statement("DELETE FROM t WHERE a = 1;", False) == (
        "DELETE FROM t WHERE a = 1",
        "DELETE",
    )
    assert check_statement("-- c\nDROP TABLE t", False)[1] == "DROP"


@pytest.mark.parametrize(
    "sql", ["", "   ", "\n", "-- only a comment", "/* c */", "/* a */ -- b\n"]
)
@pytest.mark.parametrize("read_only", [True, False])
def test_check_statement_rejects_empty(sql, read_only):
    with pytest.raises(ValueError, match="empty"):
        check_statement(sql, read_only)


@pytest.mark.parametrize("sql", [";", " ; ", ";;", "-- c\n;"])
def test_check_statement_rejects_only_semicolons(sql):
    with pytest.raises(ValueError):
        check_statement(sql, True)


def test_run_sql_only_semicolon_is_rejected():
    session = FakeSession()
    result = make_run_sql(True, 3, 10_000, run_tool)(session, ";")
    assert result["error"]["type"] == "QUERY_REJECTED"
    assert session.statements == []


@pytest.mark.parametrize("sql", ["SELECT 'abc", "SELECT 1 /* c", "SELECT $$ x"])
def test_check_statement_rejects_unterminated(sql):
    with pytest.raises(ValueError, match="unterminated"):
        check_statement(sql, True)


def test_check_statement_comment_does_not_hide_keyword():
    # A statement that merely mentions SELECT in a comment or string is not a SELECT.
    with pytest.raises(ValueError):
        check_statement("/* SELECT */ 'SELECT' DROP TABLE t", True)


@pytest.mark.parametrize("sql", ["select*from t", "( SELECT 1)", "SELECT(1)"])
def test_check_statement_accepts_valid_select_without_spaces(sql):
    assert check_statement(sql, True)[1] == "SELECT"


# ---------------------------------------------------------------------------
# make_run_sql
# ---------------------------------------------------------------------------


class FakeSchema:
    names = ["A"]


class FakeDataFrame:
    def __init__(self, rows) -> None:
        self._rows = rows
        self.schema = FakeSchema()
        self.limit_calls = []
        self.collect_calls = 0

    def limit(self, n):
        self.limit_calls.append(n)
        limited = FakeDataFrame(self._rows[:n])
        self.limited = limited
        return limited

    def collect(self):
        self.collect_calls += 1
        return list(self._rows)


class FakeSession:
    def __init__(self, rows=None, error=None) -> None:
        self.rows = rows if rows is not None else []
        self.error = error
        self.statements = []
        self.frames = []

    def sql(self, statement, params=None):
        self.statements.append(statement)
        if self.error:
            raise self.error
        frame = FakeDataFrame(self.rows)
        self.frames.append(frame)
        return frame


def _run_sql(read_only=True, max_rows=3, max_result_bytes=10_000):
    return make_run_sql(read_only, max_rows, max_result_bytes, run_tool)


def test_run_sql_select_uses_limit():
    session = FakeSession([Row(A=i) for i in range(10)])
    result = _run_sql(max_rows=3)(session, "SELECT a FROM t;")
    assert session.statements == ["SELECT a FROM t"]
    frame = session.frames[0]
    assert frame.limit_calls == [4]
    assert frame.collect_calls == 0  # only the limited frame is collected
    assert result == {
        "columns": ["A"],
        "rows": [{"A": 0}, {"A": 1}, {"A": 2}],
        "row_count": 3,
        "truncated": True,
    }


def test_run_sql_select_not_truncated():
    session = FakeSession([Row(A=1), Row(A=2)])
    result = _run_sql(max_rows=3)(session, "select a from t")
    assert result["row_count"] == 2
    assert result["truncated"] is False


@pytest.mark.parametrize(
    "query", ["SHOW TABLES", "DESCRIBE TABLE t", "DESC TABLE t", "EXPLAIN SELECT 1"]
)
def test_run_sql_unwrappable_statements_collect_and_cut(query):
    session = FakeSession([Row(A=i) for i in range(10)])
    result = _run_sql(max_rows=3)(session, query)
    frame = session.frames[0]
    assert frame.collect_calls == 1
    assert frame.limit_calls == []  # SHOW cannot be wrapped in a subquery
    assert result["rows"] == [{"A": 0}, {"A": 1}, {"A": 2}]
    assert result["row_count"] == 3
    assert result["truncated"] is True


def test_run_sql_show_with_few_rows():
    session = FakeSession([Row(A=1)])
    result = _run_sql(max_rows=3)(session, "SHOW TABLES")
    assert result["truncated"] is False
    assert result["rows"] == [{"A": 1}]


def test_run_sql_show_without_rows():
    session = FakeSession([])
    result = _run_sql()(session, "SHOW TABLES")
    assert result == {"columns": [], "rows": [], "row_count": 0, "truncated": False}


def test_run_sql_list_is_collected_when_not_read_only():
    session = FakeSession([Row(A=1)])
    result = _run_sql(read_only=False)(session, "LIST @stage")
    assert session.frames[0].collect_calls == 1
    assert result["rows"] == [{"A": 1}]


def test_run_sql_max_result_bytes():
    session = FakeSession([Row(A="x" * 100) for _ in range(10)])
    result = _run_sql(max_rows=10, max_result_bytes=300)(session, "SELECT a FROM t")
    assert result["truncated"] is True
    assert 0 < result["row_count"] < 10
    assert len(json.dumps(result, separators=(",", ":")).encode()) <= 300


@pytest.mark.parametrize(
    "query", ["DROP TABLE t", "SELECT 1; SELECT 2", "", "SELECT 'x", "-- nothing"]
)
def test_run_sql_rejected_query_does_not_call_session(query):
    session = FakeSession()
    result = _run_sql()(session, query)
    assert session.statements == []
    assert set(result) == {"error"}
    assert result["error"]["type"] == "QUERY_REJECTED"
    assert result["error"]["message"]


def test_run_sql_write_allowed_when_not_read_only():
    session = FakeSession([Row(**{"number of rows deleted": 1})])
    result = _run_sql(read_only=False)(session, "DELETE FROM t")
    assert session.statements == ["DELETE FROM t"]
    assert result["rows"] == [{"number of rows deleted": 1}]


def test_run_sql_session_error_is_query_failed():
    session = FakeSession(error=RuntimeError("SQL compilation error: bad"))
    result = _run_sql()(session, "SELECT * FROM missing")
    assert session.statements == ["SELECT * FROM missing"]
    assert result["error"]["type"] == "QUERY_FAILED"
    assert "SQL compilation error: bad" in result["error"]["message"]


def test_run_sql_collect_error_is_query_failed():
    class Boom(FakeDataFrame):
        def limit(self, n):
            raise RuntimeError("warehouse suspended")

    session = FakeSession()
    session.sql = lambda statement, params=None: Boom([])
    result = _run_sql()(session, "SELECT 1")
    assert result["error"]["type"] == "QUERY_FAILED"
    assert "warehouse suspended" in result["error"]["message"]


def test_run_sql_logs_one_record_per_call(caplog):
    run_sql = _run_sql(max_rows=1)
    session = FakeSession([Row(A=1), Row(A=2)])
    with caplog.at_level(logging.INFO, logger="snowflake.snowpark.mcp.governed_sql"):
        run_sql(session, "SELECT a FROM t")
        run_sql(session, "DROP TABLE t")
    records = [
        json.loads(r.getMessage())
        for r in caplog.records
        if r.name == "snowflake.snowpark.mcp.governed_sql"
    ]
    assert len(records) == 2
    assert records[0]["query"] == "SELECT a FROM t"
    assert records[0]["status"] == "ok"
    assert records[0]["rows"] == 1
    assert records[0]["truncated"] is True
    assert "duration_ms" in records[0]
    assert records[1]["status"] == "rejected"
    assert records[1]["query"] == "DROP TABLE t"
    assert "error" in records[1]


def test_run_sql_logs_failure(caplog):
    session = FakeSession(error=RuntimeError("nope"))
    with caplog.at_level(logging.INFO, logger="snowflake.snowpark.mcp.governed_sql"):
        _run_sql()(session, "SELECT 1")
    records = [
        json.loads(r.getMessage())
        for r in caplog.records
        if r.name == "snowflake.snowpark.mcp.governed_sql"
    ]
    assert [r["status"] for r in records] == ["failed"]
    assert records[0]["error"] == "nope"


def test_run_sql_signature_and_docstring():
    read_only = _run_sql(read_only=True, max_rows=7)
    assert list(inspect.signature(read_only).parameters) == ["session", "query"]
    assert "Only read-only statements are allowed." in read_only.__doc__
    assert "At most 7 rows" in read_only.__doc__
    assert "Args:\n    query:" in read_only.__doc__
    writable = _run_sql(read_only=False)
    assert "read-only" not in writable.__doc__


# ---------------------------------------------------------------------------
# make_log_feedback / feedback_table_ddl
# ---------------------------------------------------------------------------


class RecordingSession:
    def __init__(self) -> None:
        self.calls = []
        self.collected = 0

    def sql(self, statement, params=None):
        self.calls.append((statement, params))
        session = self

        class Result:
            def collect(self_inner):
                session.collected += 1
                return []

        return Result()


def test_log_feedback_inserts_with_params():
    session = RecordingSession()
    log = make_log_feedback("APP.MCP.FEEDBACK", "data_issue")
    result = log(session, "wrong total", json.dumps({"table": "t", "n": 1}))
    assert result == {"status": "recorded"}
    assert session.collected == 1
    ((statement, params),) = session.calls
    assert statement.startswith(
        "INSERT INTO APP.MCP.FEEDBACK (KIND, MESSAGE, DETAILS, LOGGED_BY)"
    )
    assert statement.count("?") == 3
    assert params == ["data_issue", "wrong total", json.dumps({"table": "t", "n": 1})]


def test_log_feedback_without_details():
    session = RecordingSession()
    make_log_feedback("T", "feedback")(session, "hi")
    assert session.calls[0][1] == ["feedback", "hi", "{}"]


def test_log_feedback_message_is_not_interpolated_into_sql():
    session = RecordingSession()
    make_log_feedback("T", "feedback")(session, "'); DROP TABLE t; --")
    assert "DROP" not in session.calls[0][0]


def test_log_feedback_signature():
    log = make_log_feedback("T", "feedback")
    assert list(inspect.signature(log).parameters) == ["session", "message", "details"]


def test_feedback_table_ddl():
    ddl = feedback_table_ddl("APP.MCP.FEEDBACK")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS APP.MCP.FEEDBACK (")
    for column in ("LOGGED_AT", "KIND", "MESSAGE", "DETAILS", "LOGGED_BY"):
        assert column in ddl
    # The columns the INSERT writes exist in the DDL.
    assert "DETAILS VARIANT" in ddl


@pytest.mark.parametrize(
    "details, stored",
    [
        (None, {}),
        ("", {}),
        ('{"a": 1}', {"a": 1}),
        ("not json", {"text": "not json"}),
        ("[1, 2]", {"text": "[1, 2]"}),
    ],
)
def test_log_feedback_details_is_json_text(details, stored):
    session = RecordingSession()
    make_log_feedback("APP.MCP.FEEDBACK", "feedback")(session, "m", details)
    ((_, params),) = session.calls
    assert json.loads(params[2]) == stored
