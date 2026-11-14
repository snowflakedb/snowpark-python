#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
import inspect
import json
import types
from typing import List, Literal, Optional
from unittest import mock

import pytest
import yaml

from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal import mcp_tool_runtime, mcp_utils
from snowflake.snowpark.mcp import (
    DeployResult,
    MCPLintError,
    MCPServer,
    PythonTool,
    Tool,
    ToolError,
    _signature_key,
    _Target,
    mcp_tool,
    tools,
)
from snowflake.snowpark.types import (
    LongType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    VariantType,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_session(responses=None, database='"APP"', schema='"MCP"'):
    """A mocked session. ``responses`` is a list of ``(sql prefix, rows)``; the first
    entry whose prefix the statement starts with gives the rows ``collect`` returns
    (``rows`` may be a callable taking the statement). Everything run through
    ``session.sql`` and every registration is recorded in ``session.statements``."""
    session = mock.MagicMock()
    session.get_current_database.return_value = database
    session.get_current_schema.return_value = schema
    session.get_current_role.return_value = '"APP_ROLE"'
    session.connection.host = "my_org-my_acct.snowflakecomputing.com"
    session.responses = list(responses or [])
    session.statements = []

    def sql(statement, params=None):
        session.statements.append(statement)
        result = mock.MagicMock()
        rows = []
        for prefix, candidate in session.responses:
            if statement.startswith(prefix):
                rows = candidate(statement) if callable(candidate) else candidate
                break
        result.collect.return_value = list(rows)
        return result

    session.sql.side_effect = sql
    session.sproc.register.side_effect = lambda **kw: session.statements.append(
        "REGISTER PROCEDURE " + kw["name"]
    )
    session.udf.register.side_effect = lambda **kw: session.statements.append(
        "REGISTER FUNCTION " + kw["name"]
    )
    return session


@mcp_tool(read_only=True)
def get_account(session: Session, account_id: str, days: int = 3) -> dict:
    """Return one account.

    Args:
        account_id: The account ID.
        days: How many days back to look.
    """
    return {"id": account_id, "days": days}


@mcp_tool(read_only=True)
def add_numbers(a: int, b: int = 1) -> int:
    """Add two numbers.

    Args:
        a: First.
        b: Second.
    """
    return a + b


def plain(session, x: str):
    """Describe.

    Args:
        x: The x.
    """


def messages(issues, level=None, tool=None):
    return [
        i.message
        for i in issues
        if (level is None or i.level == level) and (tool is None or i.tool == tool)
    ]


def tagged(server, tool, suffix=""):
    return mcp_utils.object_comment(server, suffix, tool, "1.0.0")


class FakeSchema:
    names = ["A"]


class FakeDataFrame:
    def __init__(self, rows) -> None:
        self._rows = rows
        self.schema = FakeSchema()

    def limit(self, n):
        return FakeDataFrame(self._rows[:n])

    def collect(self):
        return list(self._rows)


def python_server(name="S", *extra):
    return MCPServer(name, [get_account, add_numbers, *extra])


# ---------------------------------------------------------------------------
# mcp_tool
# ---------------------------------------------------------------------------


def test_mcp_tool_requires_read_only():
    def f(x: str):
        """Doc."""

    with pytest.raises(TypeError):
        mcp_tool(f)
    with pytest.raises(TypeError):
        mcp_tool()(f)  # decorator form without read_only


@pytest.mark.parametrize("value", ["yes", None, 1, 0])
def test_mcp_tool_read_only_must_be_bool(value):
    def f(x: str):
        """Doc."""

    with pytest.raises(TypeError, match="read_only"):
        mcp_tool(f, read_only=value)
    with pytest.raises(TypeError, match="read_only"):
        mcp_tool(read_only=value)(f)


def test_mcp_tool_read_only_and_destructive_conflict():
    def f(x: str):
        """Doc."""

    with pytest.raises(ValueError, match="read-only"):
        mcp_tool(f, read_only=True, destructive=True)
    # read_only=True with destructive=False is fine.
    assert (
        mcp_tool(f, read_only=True, destructive=False).annotations["destructiveHint"]
        is False
    )


def test_mcp_tool_kind_auto():
    def proc(session: Session, x: str):
        """Doc."""

    def udf(x: str):
        """Doc."""

    def string_annotation(session: "Session", x: str):  # noqa: F821
        """Doc."""

    assert mcp_tool(proc, read_only=True).kind == "procedure"
    assert mcp_tool(udf, read_only=True).kind == "function"
    assert mcp_tool(string_annotation, read_only=True).kind == "procedure"
    assert mcp_tool(plain, read_only=True).kind == "function"
    assert mcp_tool(plain, read_only=True, kind="procedure").kind == "procedure"


def test_mcp_tool_explicit_procedure_skips_first_parameter():
    tool = mcp_tool(plain, read_only=True, kind="procedure")
    assert [p.name for p in tool.parameters] == ["x"]
    assert messages(tool._lint(), "error") == []


def test_mcp_tool_procedure_without_parameters_is_a_lint_error():
    def f():
        """Doc."""

    tool = mcp_tool(f, read_only=True, kind="procedure")
    assert any("first parameter" in m for m in messages(tool._lint(), "error"))


@pytest.mark.parametrize("name", ["bad-name", "1x", "a b", "x.y"])
def test_mcp_tool_invalid_name(name):
    def f(x: str):
        """Doc."""

    with pytest.raises(ValueError, match="Invalid tool name"):
        mcp_tool(f, read_only=True, name=name)


def test_mcp_tool_empty_name_is_invalid():
    with pytest.raises(ValueError, match="Invalid tool name"):
        mcp_tool(plain, read_only=True, name="")


def test_mcp_tool_other_invalid_arguments():
    with pytest.raises(TypeError, match="expects a function"):
        mcp_tool("not callable", read_only=True)
    with pytest.raises(ValueError, match="object_name"):
        mcp_tool(plain, read_only=True, object_name="a-b")
    with pytest.raises(ValueError, match="kind"):
        mcp_tool(plain, read_only=True, kind="bogus")
    with pytest.raises(ValueError, match="max_rows"):
        mcp_tool(plain, read_only=True, max_rows=0)


def test_mcp_tool_decorated_tool_is_still_callable():
    assert isinstance(get_account, PythonTool)
    assert get_account("session", "x") == {"id": "x", "days": 3}
    assert get_account(None, "x", days=9) == {"id": "x", "days": 9}
    assert add_numbers(2, b=5) == 7
    assert get_account.func.__name__ == "get_account"


def test_mcp_tool_description_title_and_schema_from_function():
    assert get_account.name == "get_account"
    assert get_account.title == "Get account"
    assert get_account.description == "Return one account."
    assert get_account.input_schema == {
        "type": "object",
        "properties": {
            "account_id": {"type": "string", "description": "The account ID."},
            "days": {
                "type": "integer",
                "default": 3,
                "description": "How many days back to look.",
            },
        },
        "required": ["account_id"],
    }
    assert add_numbers.input_schema["required"] == ["a"]


def test_mcp_tool_explicit_name_title_description():
    tool = mcp_tool(
        plain, read_only=True, name="other", title="T", description="Explicit."
    )
    assert (tool.name, tool.title, tool.description) == ("other", "T", "Explicit.")


def test_mcp_tool_annotation_defaults():
    read = mcp_tool(plain, read_only=True)
    assert read.annotations == {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False,
    }
    write = mcp_tool(plain, read_only=False)
    assert write.annotations["readOnlyHint"] is False
    assert write.annotations["destructiveHint"] is True
    assert (
        mcp_tool(plain, read_only=False, destructive=False).annotations[
            "destructiveHint"
        ]
        is False
    )
    assert (
        mcp_tool(plain, read_only=True, idempotent=True).annotations["idempotentHint"]
        is True
    )


def test_mcp_tool_open_world_defaults_to_external_access():
    eai = mcp_tool(plain, read_only=True, external_access_integrations=["EAI"])
    assert eai.annotations["openWorldHint"] is True
    assert (
        mcp_tool(
            plain,
            read_only=True,
            external_access_integrations=["EAI"],
            open_world=False,
        ).annotations["openWorldHint"]
        is False
    )
    assert (
        mcp_tool(plain, read_only=True, open_world=True).annotations["openWorldHint"]
        is True
    )


def test_mcp_tool_decorator_form_with_options():
    @mcp_tool(read_only=False, name="renamed")
    def f(x: str):
        """Doc."""

    assert f.name == "renamed"


# ---------------------------------------------------------------------------
# PythonTool.invoke
# ---------------------------------------------------------------------------


def test_invoke_procedure_tool():
    assert get_account.invoke(object(), account_id="x") == {"id": "x", "days": 3}
    assert get_account.invoke(object(), account_id="x", days=7)["days"] == 7


def test_invoke_udf_tool_needs_no_session():
    assert add_numbers.invoke(a=1, b=2) == 3


def test_invoke_validates_arguments():
    with pytest.raises(ValueError, match="unknown argument 'z'"):
        get_account.invoke(object(), account_id="x", z=1)
    with pytest.raises(ValueError, match="missing required argument 'account_id'"):
        get_account.invoke(object())
    with pytest.raises(ValueError, match="must be of type integer"):
        get_account.invoke(object(), account_id="x", days="3")
    with pytest.raises(ValueError, match="get_account"):
        get_account.invoke(object(), account_id=5)


def test_invoke_procedure_tool_requires_session():
    with pytest.raises(ValueError, match="needs a session"):
        get_account.invoke(account_id="x")


def test_invoke_converts_dataframe_result():
    @mcp_tool(read_only=True, max_rows=2)
    def f(session: Session):
        """Rows."""
        return FakeDataFrame([Row(A=1), Row(A=2), Row(A=3)])

    assert f.invoke(object()) == {
        "columns": ["A"],
        "rows": [{"A": 1}, {"A": 2}],
        "row_count": 2,
        "truncated": True,
    }


def test_invoke_tool_error_becomes_error_result():
    @mcp_tool(read_only=True)
    def f(x: str):
        """Doc."""
        raise ToolError("unknown " + x, hint="try again")

    assert f.invoke(x="a") == {
        "error": {"type": "ToolError", "message": "unknown a", "hint": "try again"}
    }


def test_invoke_other_exception_propagates():
    @mcp_tool(read_only=True)
    def f(x: str):
        """Doc."""
        raise KeyError("boom")

    with pytest.raises(KeyError):
        f.invoke(x="a")


def test_invoke_result_too_large():
    @mcp_tool(read_only=True, max_result_bytes=50)
    def f(x: str):
        """Doc."""
        return "y" * 100

    assert f.invoke(x="a")["error"]["type"] == "RESULT_TOO_LARGE"


def test_invoke_null_for_optional():
    @mcp_tool(read_only=True)
    def f(x: Optional[str] = None):
        """Doc."""
        return {"x": x}

    assert f.invoke(x=None) == {"x": None}
    assert f.invoke() == {"x": None}


# ---------------------------------------------------------------------------
# MCPServer basics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["bad name", "1x", "a-b", "", "a.b"])
def test_server_invalid_name(name):
    with pytest.raises(ValueError, match="Invalid MCP server name"):
        MCPServer(name)


def test_server_name_is_uppercased():
    assert MCPServer("sales_mcp").name == "SALES_MCP"


def test_server_add_flattens_lists():
    server = MCPServer("S", [get_account])

    @mcp_tool(read_only=True)
    def ping(session: Session) -> str:
        """Ping."""
        return "pong"

    extra = [ping]
    result = server.add(extra, add_numbers, [[tools.SqlExecution(name="sx")]])
    assert result is server
    assert [t.name for t in server.tools] == [
        "get_account",
        "ping",
        "add_numbers",
        "sx",
    ]


@pytest.mark.parametrize("bad", ["a string", 5, None, b"bytes", [1]])
def test_server_add_rejects_non_tools(bad):
    with pytest.raises(TypeError, match="Expected an MCP tool"):
        MCPServer("S").add(bad)


def test_server_tool_lookup():
    server = python_server()
    assert server.tool("add_numbers") is add_numbers
    with pytest.raises(KeyError, match="no_such"):
        server.tool("no_such")


def test_server_repr():
    assert "get_account" in repr(python_server())


# ---------------------------------------------------------------------------
# lint
# ---------------------------------------------------------------------------


def test_lint_clean_server():
    assert python_server().lint() == []


def test_lint_duplicate_names():
    server = MCPServer("S", [get_account, get_account])
    errors = messages(server.lint(), "error")
    assert errors == ["Two tools have this name."]


def test_lint_too_many_tools():
    many = [
        mcp_tool(plain, read_only=True, name=f"tool_{i}", description="d")
        for i in range(51)
    ]
    issues = MCPServer("S", many).lint()
    assert any("51 tools" in m for m in messages(issues, "error"))
    assert not any("tools;" in m for m in messages(MCPServer("S", many[:50]).lint()))


def test_lint_missing_description():
    def f(x: str):
        pass

    issues = MCPServer("S", [mcp_tool(f, read_only=True)]).lint()
    assert any("no description" in m for m in messages(issues, "error", "f"))


def test_lint_long_description_warns():
    tool = mcp_tool(plain, read_only=True, description="d" * 1025)
    assert any("1024" in m for m in messages(MCPServer("S", [tool]).lint(), "warning"))


def test_lint_missing_parameter_description_warns():
    def f(x: str, y: str):
        """Doc.

        Args:
            x: Described.
        """

    issues = MCPServer("S", [mcp_tool(f, read_only=True)]).lint()
    assert messages(issues, "error") == []
    warnings = messages(issues, "warning")
    assert len(warnings) == 1 and "Parameter y has no description" in warnings[0]


def test_lint_owner_rights_sql_parameter_warns():
    def f(session: Session, query: str):
        """Doc.

        Args:
            query: The query.
        """

    owner = MCPServer("S", [mcp_tool(f, read_only=True)]).lint()
    assert any("free-form SQL" in m for m in messages(owner, "warning"))
    caller = MCPServer("S", [mcp_tool(f, read_only=True, execute_as="caller")]).lint()
    assert messages(caller) == []


def test_lint_owner_rights_sql_parameter_advice():
    def f(session: Session, query: str):
        """Doc.

        Args:
            query: The query.
        """

    (issue,) = MCPServer("S", [mcp_tool(f, read_only=True)]).lint()
    assert issue.level == "warning"
    assert "execute_as='caller'" in issue.message
    assert "read-only role" in issue.message


def test_lint_udf_sql_parameter_does_not_warn():
    def f(query: str):
        """Doc.

        Args:
            query: The query.
        """

    assert MCPServer("S", [mcp_tool(f, read_only=True)]).lint() == []


def test_lint_read_only_tool_named_like_a_write():
    def write_x(x: str):
        """Doc.

        Args:
            x: X.
        """

    warnings = messages(
        MCPServer("S", [mcp_tool(write_x, read_only=True)]).lint(), "warning"
    )
    assert any("name suggests" in m for m in warnings)
    assert MCPServer("S", [mcp_tool(write_x, read_only=False)]).lint() == []


def test_lint_max_result_bytes_above_platform_limit():
    def f(x: str):
        """Doc.

        Args:
            x: X.
        """

    tool = mcp_tool(f, read_only=True, max_result_bytes=250_001)
    assert any("250000" in m for m in messages(MCPServer("S", [tool]).lint(), "error"))
    ok = mcp_tool(f, read_only=True, max_result_bytes=250_000)
    assert messages(MCPServer("S", [ok]).lint(), "error") == []


def test_lint_udf_with_execute_as_caller_is_an_error():
    def f(x: str):
        """Doc.

        Args:
            x: X.
        """

    tool = mcp_tool(f, read_only=True, execute_as="caller")
    errors = messages(MCPServer("S", [tool]).lint(), "error")
    assert len(errors) == 1 and "execute_as only applies" in errors[0]


def test_lint_unannotated_session_parameter_is_an_error():
    def f(session, x: str):
        """Doc.

        Args:
            x: X.
        """

    tool = mcp_tool(f, read_only=True)
    assert tool.kind == "function"
    errors = messages(MCPServer("S", [tool]).lint(), "error")
    assert any("named 'session'" in m for m in errors)


def test_lint_unsupported_parameters_are_errors():
    def f(x: str, *args, **kwargs):
        """Doc.

        Args:
            x: X.
        """

    errors = messages(MCPServer("S", [mcp_tool(f, read_only=True)]).lint(), "error")
    assert len(errors) == 2


def test_lint_builtin_tools():
    server = MCPServer(
        "S",
        [
            tools.CortexAgent("A.B.AGENT"),  # no description
            tools.CortexSearch("SVC", description="Search."),  # not qualified
        ],
    )
    issues = server.lint()
    assert any("no description" in m for m in messages(issues, "error", "agent"))
    assert any("not fully qualified" in m for m in messages(issues, "warning", "svc"))


# ---------------------------------------------------------------------------
# to_spec / to_yaml
# ---------------------------------------------------------------------------


def test_to_yaml_python_tools():
    server = python_server("SRV")
    parsed = yaml.safe_load(
        server.to_yaml(database="APP", schema="MCP_DEV", suffix="_dev")
    )
    procedure, function = parsed["tools"]
    assert procedure["name"] == "get_account"
    assert procedure["title"] == "Get account"
    assert procedure["type"] == "GENERIC"
    assert procedure["identifier"] == "APP.MCP_DEV.SRV_DEV__GET_ACCOUNT"
    assert procedure["description"] == "Return one account."
    assert procedure["config"]["type"] == "procedure"
    assert procedure["config"]["input_schema"] == get_account.input_schema
    assert procedure["annotations"] == get_account.annotations
    assert function["identifier"] == "APP.MCP_DEV.SRV_DEV__ADD_NUMBERS"
    assert function["config"]["type"] == "function"
    assert function["config"]["input_schema"]["required"] == ["a"]


def test_to_yaml_uses_session_defaults():
    parsed = yaml.safe_load(python_server("SRV").to_yaml(make_session()))
    assert parsed["tools"][0]["identifier"] == "APP.MCP.SRV__GET_ACCOUNT"


def test_to_yaml_requires_database_and_schema():
    with pytest.raises(ValueError, match="database and schema"):
        python_server().to_yaml()
    with pytest.raises(ValueError, match="database and schema"):
        python_server().to_yaml(database="APP")


def test_to_yaml_invalid_suffix():
    with pytest.raises(ValueError, match="Invalid suffix"):
        python_server().to_yaml(database="APP", schema="MCP", suffix="-dev")


def test_to_yaml_object_name_override():
    tool = mcp_tool(plain, read_only=True, object_name="my_proc", description="d")
    spec = MCPServer("S", [tool]).to_spec(database="APP", schema="MCP", suffix="_x")
    assert spec["tools"][0]["identifier"] == "APP.MCP.MY_PROC"


def test_to_yaml_optional_config():
    tool = mcp_tool(
        plain, read_only=True, description="d", warehouse="WH", query_timeout=30
    )
    config = MCPServer("S", [tool]).to_spec(database="A", schema="B")["tools"][0][
        "config"
    ]
    assert config["warehouse"] == "WH" and config["query_timeout"] == 30
    plain_config = python_server().to_spec(database="A", schema="B")["tools"][0][
        "config"
    ]
    assert "warehouse" not in plain_config and "query_timeout" not in plain_config


def test_to_yaml_builtin_tools():
    server = MCPServer(
        "S",
        [
            tools.CortexAgent(
                "APP.AG.SALES_AGENT", description="Agent.", read_only=True
            ),
            tools.CortexAnalyst("APP.SV.REVENUE", description="Revenue."),
            tools.CortexSearch("APP.SS.DOCS", description="Docs.", name="docs_search"),
            tools.SqlExecution(warehouse="WH", query_timeout=10),
            tools.CodingAgent(model="auto", instructions="Be careful."),
        ],
    )
    by_name = {t["name"]: t for t in server.to_spec(database="A", schema="B")["tools"]}
    agent = by_name["sales_agent"]
    assert (agent["type"], agent["identifier"]) == (
        "CORTEX_AGENT_RUN",
        "APP.AG.SALES_AGENT",
    )
    assert agent["annotations"]["readOnlyHint"] is True
    assert "config" not in agent
    analyst = by_name["revenue"]
    assert (analyst["type"], analyst["identifier"]) == (
        "CORTEX_ANALYST_MESSAGE",
        "APP.SV.REVENUE",
    )
    assert "annotations" not in analyst
    search = by_name["docs_search"]
    assert (search["type"], search["identifier"]) == (
        "CORTEX_SEARCH_SERVICE_QUERY",
        "APP.SS.DOCS",
    )
    sql = by_name["sql_execution"]
    assert sql["type"] == "SYSTEM_EXECUTE_SQL"
    assert sql["config"] == {"read_only": True, "warehouse": "WH", "query_timeout": 10}
    assert "identifier" not in sql
    coding = by_name["coding_agent"]
    assert coding["type"] == "CODING_AGENT"
    assert coding["config"] == {"model": "auto", "instructions": "Be careful."}


def test_to_yaml_sql_execution_read_only_false():
    spec = MCPServer("S", [tools.SqlExecution(read_only=False)]).to_spec(
        database="A", schema="B"
    )
    assert spec["tools"][0]["config"]["read_only"] is False


def test_to_spec_unresolved_tool_without_session():
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    with pytest.raises(ValueError, match="session"):
        server.to_spec(database="APP", schema="MCP")


def test_lint_unresolved_existing_tool_is_skipped_without_session():
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    assert server.lint() == []


# ---------------------------------------------------------------------------
# Tool.from_procedure / Tool.from_function
# ---------------------------------------------------------------------------

SHOW_FINANCIALS = Row(
    name="GET_FINANCIALS",
    arguments="GET_FINANCIALS(VARCHAR, DEFAULT NUMBER) RETURN VARCHAR",
    description="Financials for a ticker",
)
DESCRIBE_FINANCIALS = [
    Row(property="signature", value="(TICKER VARCHAR, YEARS NUMBER)"),
    Row(property="returns", value="VARCHAR"),
    Row(property="language", value="PYTHON"),
]


def financials_session(show_rows=None, describe_rows=None):
    return make_session(
        [
            (
                "SHOW PROCEDURES LIKE",
                [SHOW_FINANCIALS] if show_rows is None else show_rows,
            ),
            (
                "DESCRIBE PROCEDURE",
                DESCRIBE_FINANCIALS if describe_rows is None else describe_rows,
            ),
        ]
    )


def test_from_procedure_resolution():
    session = financials_session()
    tool = Tool.from_procedure(
        "APP.FIN.GET_FINANCIALS",
        read_only=True,
        param_descriptions={"Ticker": "Stock ticker."},
    )
    assert tool.name == "get_financials"
    spec = MCPServer("S", [tool]).to_spec(session)["tools"][0]
    assert session.statements == [
        "SHOW PROCEDURES LIKE 'GET_FINANCIALS' IN SCHEMA APP.FIN",
        "DESCRIBE PROCEDURE APP.FIN.GET_FINANCIALS(VARCHAR, NUMBER)",
    ]
    assert spec["type"] == "GENERIC"
    assert spec["identifier"] == "APP.FIN.GET_FINANCIALS"
    assert spec["description"] == "Financials for a ticker"
    assert spec["config"]["type"] == "procedure"
    assert spec["config"]["input_schema"] == {
        "type": "object",
        "properties": {
            "ticker": {"type": "string", "description": "Stock ticker."},
            "years": {"type": "integer"},
        },
        "required": ["ticker"],
    }
    assert spec["annotations"]["readOnlyHint"] is True
    assert [p.name for p in tool.parameters] == ["ticker", "years"]
    assert [p.required for p in tool.parameters] == [True, False]


def test_from_procedure_param_descriptions_are_case_insensitive():
    for key in ("ticker", "TICKER", "Ticker"):
        tool = Tool.from_procedure(
            "APP.FIN.GET_FINANCIALS", read_only=True, param_descriptions={key: "d"}
        )
        MCPServer("S", [tool]).to_spec(financials_session())
        assert tool.parameters[0].description == "d"
        assert tool.parameters[1].description is None


def test_from_procedure_is_resolved_once():
    session = financials_session()
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    server.to_spec(session)
    server.to_spec(session)
    assert len(session.statements) == 2


def test_from_procedure_explicit_description_wins_over_comment():
    session = financials_session()
    tool = Tool.from_procedure(
        "APP.FIN.GET_FINANCIALS", read_only=True, description="Mine.", name="fin"
    )
    spec = MCPServer("S", [tool]).to_spec(session)["tools"][0]
    assert (spec["name"], spec["description"]) == ("fin", "Mine.")


def test_from_procedure_default_comment_is_not_a_description():
    row = Row(
        name="GET_FINANCIALS",
        arguments="GET_FINANCIALS(VARCHAR, DEFAULT NUMBER) RETURN VARCHAR",
        description="user-defined procedure",
    )
    session = financials_session([row])
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    assert any("no description" in m for m in messages(server.lint(session), "error"))


def test_from_procedure_lint_after_resolution():
    describe = [Row(property="signature", value="(TICKER VARCHAR, PAYLOAD VARIANT)")]
    row = Row(
        name="GET_FINANCIALS",
        arguments="GET_FINANCIALS(VARCHAR, VARIANT) RETURN VARCHAR",
        description="Financials.",
    )
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    issues = server.lint(financials_session([row], describe))
    warnings = messages(issues, "warning")
    assert any("PAYLOAD" in m and "no JSON equivalent" in m for m in warnings)
    assert any("Parameter ticker has no description" in m for m in warnings)
    assert messages(issues, "error") == []


def test_from_procedure_unqualified_uses_session_defaults():
    session = financials_session()
    tool = Tool.from_procedure("GET_FINANCIALS", read_only=True)
    MCPServer("S", [tool]).to_spec(session)
    assert session.statements[0] == (
        "SHOW PROCEDURES LIKE 'GET_FINANCIALS' IN SCHEMA APP.MCP"
    )


def test_from_procedure_schema_qualified():
    session = financials_session()
    tool = Tool.from_procedure("FIN.GET_FINANCIALS", read_only=True)
    MCPServer("S", [tool]).to_spec(session)
    assert session.statements[0].endswith("IN SCHEMA APP.FIN")
    assert session.statements[1].startswith(
        "DESCRIBE PROCEDURE APP.FIN.GET_FINANCIALS("
    )


def test_from_procedure_without_session_database_fails():
    session = make_session(database=None)
    server = MCPServer("S", [Tool.from_procedure("GET_FINANCIALS", read_only=True)])
    with pytest.raises(ValueError):
        server.to_spec(session, database="APP", schema="MCP")


OVERLOADED = [
    Row(
        name="GET_FINANCIALS",
        arguments="GET_FINANCIALS(VARCHAR) RETURN VARCHAR",
        description="One argument",
    ),
    Row(
        name="GET_FINANCIALS",
        arguments="GET_FINANCIALS(VARCHAR, NUMBER) RETURN VARCHAR",
        description="Two arguments",
    ),
]


def test_from_procedure_overloads_are_ambiguous_without_types():
    session = financials_session(OVERLOADED)
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    with pytest.raises(ValueError, match="2 overloads") as exc:
        server.to_spec(session)
    assert "GET_FINANCIALS(VARCHAR)" in str(exc.value)


@pytest.mark.parametrize(
    "identifier, describe, description",
    [
        ("APP.FIN.GET_FINANCIALS(STRING)", "(VARCHAR)", "One argument"),
        ("APP.FIN.GET_FINANCIALS(VARCHAR)", "(VARCHAR)", "One argument"),
        (
            "APP.FIN.GET_FINANCIALS(STRING, BIGINT)",
            "(VARCHAR, NUMBER)",
            "Two arguments",
        ),
        (
            "APP.FIN.GET_FINANCIALS(TEXT, NUMBER(38,0))",
            "(VARCHAR, NUMBER)",
            "Two arguments",
        ),
    ],
)
def test_from_procedure_argument_types_pick_the_overload(
    identifier, describe, description
):
    session = financials_session(
        OVERLOADED,
        [
            Row(
                property="signature",
                value=describe.replace("(", "(A ").replace(", ", ", B "),
            )
        ],
    )
    tool = Tool.from_procedure(identifier, read_only=True)
    spec = MCPServer("S", [tool]).to_spec(session)["tools"][0]
    assert spec["description"] == description
    assert session.statements[-1].startswith(
        "DESCRIBE PROCEDURE APP.FIN.GET_FINANCIALS" + describe
    )


def test_from_procedure_not_found():
    session = financials_session(show_rows=[])
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    with pytest.raises(ValueError, match="was not found"):
        server.to_spec(session)


def test_from_procedure_ignores_like_wildcard_matches():
    other = Row(
        name="GETXFINANCIALS",
        arguments="GETXFINANCIALS(VARCHAR) RETURN VARCHAR",
        description="other",
    )
    session = financials_session([other, SHOW_FINANCIALS])
    tool = Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)
    spec = MCPServer("S", [tool]).to_spec(session)["tools"][0]
    assert spec["description"] == "Financials for a ticker"


def test_from_procedure_argument_type_mismatch_is_not_found():
    session = financials_session()
    tool = Tool.from_procedure("APP.FIN.GET_FINANCIALS(BOOLEAN)", read_only=True)
    with pytest.raises(ValueError, match="was not found"):
        MCPServer("S", [tool]).to_spec(session)


def test_from_procedure_without_signature_in_describe():
    session = financials_session(describe_rows=[Row(property="returns", value="X")])
    tool = Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)
    with pytest.raises(ValueError, match="no signature"):
        MCPServer("S", [tool]).to_spec(session)


def test_from_procedure_with_stored_procedure_object():
    sproc = types.SimpleNamespace(
        name="APP.FIN.GET_FINANCIALS", _input_types=[StringType(), LongType()]
    )
    tool = Tool.from_procedure(sproc, read_only=True)
    assert tool.object_identifier == "APP.FIN.GET_FINANCIALS"
    assert tool.arg_types == ["STRING", "BIGINT"]
    session = financials_session(
        OVERLOADED, [Row(property="signature", value="(A VARCHAR, B NUMBER)")]
    )
    spec = MCPServer("S", [tool]).to_spec(session)["tools"][0]
    assert spec["description"] == "Two arguments"


def test_from_procedure_rejects_other_objects():
    with pytest.raises(TypeError, match="Expected a procedure name"):
        Tool.from_procedure(42, read_only=True)
    with pytest.raises(TypeError, match="Expected a function name"):
        Tool.from_function(object(), read_only=True)


def test_from_procedure_requires_read_only():
    with pytest.raises(TypeError):
        Tool.from_procedure("APP.FIN.GET_FINANCIALS")
    with pytest.raises(TypeError, match="read_only"):
        Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only="no")


@pytest.mark.parametrize("identifier", ["", "A.B.C.D", "A..C"])
def test_from_procedure_invalid_identifier(identifier):
    with pytest.raises(ValueError, match="Invalid procedure name"):
        Tool.from_procedure(identifier, read_only=True)


def test_from_function_resolution():
    session = make_session(
        [
            (
                "SHOW USER FUNCTIONS LIKE 'FX'",
                [
                    Row(
                        name="FX",
                        arguments="FX(VARCHAR) RETURN NUMBER",
                        description="Exchange rate",
                    )
                ],
            ),
            ("DESCRIBE FUNCTION", [Row(property="signature", value="(CCY VARCHAR)")]),
        ]
    )
    tool = Tool.from_function("APP.FIN.FX", read_only=True, name="fx_rate")
    spec = MCPServer("S", [tool]).to_spec(session)["tools"][0]
    assert session.statements == [
        "SHOW USER FUNCTIONS LIKE 'FX' IN SCHEMA APP.FIN",
        "DESCRIBE FUNCTION APP.FIN.FX(VARCHAR)",
    ]
    assert spec["name"] == "fx_rate"
    assert spec["config"]["type"] == "function"
    assert spec["config"]["input_schema"]["properties"] == {"ccy": {"type": "string"}}


def test_from_procedure_bracket_optional_syntax():
    row = Row(
        name="GET_FINANCIALS",
        arguments="GET_FINANCIALS(VARCHAR [, NUMBER]) RETURN VARCHAR",
        description="d",
    )
    session = financials_session([row])
    tool = Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)
    MCPServer("S", [tool]).to_spec(session)
    assert [p.required for p in tool.parameters] == [True, False]


def test_from_procedure_grants():
    session = financials_session()
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    statements = server.grant_usage(session, "ANALYST", dry_run=True)
    assert (
        "GRANT USAGE ON PROCEDURE APP.FIN.GET_FINANCIALS(VARCHAR, NUMBER) TO ROLE ANALYST"
        in statements
    )


# ---------------------------------------------------------------------------
# deploy: dry run
# ---------------------------------------------------------------------------


def test_deploy_dry_run():
    session = make_session()
    server = python_server("S")
    result = server.deploy(session, dry_run=True)
    assert isinstance(result, DeployResult)
    assert result.server == "APP.MCP.S"
    assert result.sql[0] == "CREATE STAGE IF NOT EXISTS APP.MCP.SNOWPARK_MCP_STAGE"
    create = result.sql[1]
    assert create.startswith(
        "CREATE OR REPLACE MCP SERVER APP.MCP.S FROM SPECIFICATION $$\n"
    )
    assert create.endswith("$$")
    body = create[
        len("CREATE OR REPLACE MCP SERVER APP.MCP.S FROM SPECIFICATION $$\n") : -2
    ]
    assert yaml.safe_load(body) == server.to_spec(database="APP", schema="MCP")
    assert len(result.sql) == 2
    assert result.objects == [
        "APP.MCP.S__GET_ACCOUNT(STRING, BIGINT)",
        "APP.MCP.S__ADD_NUMBERS(BIGINT, BIGINT)",
    ]
    assert result.pruned == []
    assert result.issues == []
    assert result.endpoint_url == (
        "https://my-org-my-acct.snowflakecomputing.com/api/v2/databases/APP/"
        "schemas/MCP/mcp-servers/S"
    )
    # Nothing ran or was registered.
    session.sproc.register.assert_not_called()
    session.udf.register.assert_not_called()
    # Only the read-only SHOW statements ran.
    assert all(c.args[0].startswith("SHOW") for c in session.sql.call_args_list)
    session._conn._telemetry_client.send_function_usage_telemetry.assert_not_called()


def test_deploy_dry_run_with_suffix_database_and_schema():
    result = python_server("S").deploy(
        make_session(), database="DB", schema="DEV", suffix="_dev", dry_run=True
    )
    assert result.server == "DB.DEV.S_DEV"
    assert result.sql[0] == "CREATE STAGE IF NOT EXISTS DB.DEV.SNOWPARK_MCP_STAGE"
    assert result.sql[1].startswith("CREATE OR REPLACE MCP SERVER DB.DEV.S_DEV ")
    assert result.objects[0] == "DB.DEV.S_DEV__GET_ACCOUNT(STRING, BIGINT)"
    assert result.endpoint_url.endswith("/databases/DB/schemas/DEV/mcp-servers/S_DEV")


def test_deploy_dry_run_stage_location_skips_stage_creation():
    result = python_server().deploy(
        make_session(), stage_location="@X.Y.Z", dry_run=True
    )
    assert not any("CREATE STAGE" in s for s in result.sql)


def test_deploy_dry_run_builtin_only_server_needs_no_stage():
    server = MCPServer("S", [tools.SqlExecution()])
    result = server.deploy(make_session(), dry_run=True)
    assert len(result.sql) == 1 and result.objects == []


def test_deploy_dry_run_reports_warnings():
    def f(x: str):
        """Doc."""

    server = MCPServer("S", [mcp_tool(f, read_only=True)])
    result = server.deploy(make_session(), dry_run=True)
    assert [i.level for i in result.issues] == ["warning"]


def test_deploy_lint_errors_stop_before_any_sql():
    def f(x: str):
        pass  # no description

    session = make_session()
    server = MCPServer("S", [mcp_tool(f, read_only=True)])
    with pytest.raises(MCPLintError, match="no description") as exc:
        server.deploy(session)
    assert any(i.level == "error" for i in exc.value.issues)
    session.sql.assert_not_called()
    session.sproc.register.assert_not_called()
    session.udf.register.assert_not_called()


def test_deploy_lint_errors_stop_dry_run_too():
    server = MCPServer("S", [get_account, get_account])
    with pytest.raises(MCPLintError, match="Two tools"):
        server.deploy(make_session(), dry_run=True)


def test_deploy_spec_with_dollar_quotes_in_description_is_refused():
    def f(x: str):
        """Doc.

        Args:
            x: X.
        """

    tool = mcp_tool(f, read_only=True, description="Costs $$ per call.")
    for dry_run in (True, False):
        session = make_session()
        with pytest.raises(ValueError, match=r"\$\$"):
            MCPServer("S", [tool]).deploy(session, dry_run=dry_run)
        session.sql.assert_not_called()
        session.sproc.register.assert_not_called()
        session.udf.register.assert_not_called()


# ---------------------------------------------------------------------------
# deploy: registration
# ---------------------------------------------------------------------------


def test_deploy_registers_tools():
    session = make_session()
    result = python_server("S").deploy(session)
    assert (
        session.statements[0] == "CREATE STAGE IF NOT EXISTS APP.MCP.SNOWPARK_MCP_STAGE"
    )
    # Objects are created before the server is replaced.
    assert session.statements[1:3] == [
        "REGISTER PROCEDURE APP.MCP.S__GET_ACCOUNT",
        "REGISTER FUNCTION APP.MCP.S__ADD_NUMBERS",
    ]
    assert any(
        s.startswith("CREATE OR REPLACE MCP SERVER APP.MCP.S ")
        for s in session.statements[3:]
    )
    assert result.objects == [
        "APP.MCP.S__GET_ACCOUNT(STRING, BIGINT)",
        "APP.MCP.S__ADD_NUMBERS(BIGINT, BIGINT)",
    ]

    (sproc_call,) = session.sproc.register.call_args_list
    kwargs = sproc_call.kwargs
    assert sproc_call.args == ()
    assert kwargs["name"] == "APP.MCP.S__GET_ACCOUNT"
    assert kwargs["is_permanent"] is True
    assert kwargs["replace"] is True
    assert kwargs["copy_grants"] is True
    assert kwargs["preserve_parameter_names"] is True
    assert kwargs["execute_as"] == "owner"
    assert kwargs["stage_location"] == "@APP.MCP.SNOWPARK_MCP_STAGE"
    assert isinstance(kwargs["return_type"], VariantType)
    assert [type(t) for t in kwargs["input_types"]] == [StringType, LongType]
    assert kwargs["source_code_display"] is False
    assert "snowpark_mcp" in kwargs["comment"]
    comment = mcp_utils.parse_object_comment(kwargs["comment"])
    assert comment["server"] == "S" and comment["tool"] == "get_account"
    assert comment["suffix"] == ""

    (udf_call,) = session.udf.register.call_args_list
    assert udf_call.kwargs["name"] == "APP.MCP.S__ADD_NUMBERS"
    assert "execute_as" not in udf_call.kwargs
    assert udf_call.kwargs["replace"] is True
    assert udf_call.kwargs["copy_grants"] is True
    assert udf_call.kwargs["preserve_parameter_names"] is True
    assert isinstance(udf_call.kwargs["return_type"], VariantType)


def test_deploy_registered_handler_has_tool_signature_and_runs():
    session = make_session()
    python_server("S").deploy(session)
    handler = session.sproc.register.call_args.kwargs["func"]
    spec = inspect.getfullargspec(handler)
    assert spec.args == ["session", "account_id", "days"]
    assert spec.defaults == (3,)
    assert spec.annotations == {}
    assert handler(None, "x") == {"id": "x", "days": 3}
    udf_handler = session.udf.register.call_args.kwargs["func"]
    assert udf_handler(1) == 2


def test_deploy_passes_packages_imports_and_integrations():
    tool = mcp_tool(
        plain,
        read_only=True,
        description="d",
        kind="procedure",
        packages=["pandas"],
        imports=["lib.py"],
        external_access_integrations=["EAI"],
        secrets={"k": "APP.MCP.SECRET"},
        execute_as="caller",
    )
    session = make_session()
    MCPServer("S", [tool]).deploy(session, stage_location="@X.Y.Z")
    kwargs = session.sproc.register.call_args.kwargs
    assert kwargs["packages"] == ["pandas"]
    assert kwargs["imports"] == ["lib.py"]
    assert kwargs["external_access_integrations"] == ["EAI"]
    assert kwargs["secrets"] == {"k": "APP.MCP.SECRET"}
    assert kwargs["execute_as"] == "caller"
    assert kwargs["stage_location"] == "@X.Y.Z"
    assert not any("CREATE STAGE" in s for s in session.statements)


def test_deploy_with_suffix_registers_suffixed_names():
    session = make_session()
    python_server("S").deploy(session, schema="MCP_DEV", suffix="_dev")
    assert session.sproc.register.call_args.kwargs["name"] == (
        "APP.MCP_DEV.S_DEV__GET_ACCOUNT"
    )
    comment = mcp_utils.parse_object_comment(
        session.sproc.register.call_args.kwargs["comment"]
    )
    assert comment["suffix"] == "_DEV"


def test_deploy_sends_telemetry():
    session = make_session()
    server = MCPServer("S", [get_account, add_numbers, tools.SqlExecution()])
    server.deploy(session)
    send = session._conn._telemetry_client.send_function_usage_telemetry
    send.assert_called_once()
    assert send.call_args.args[:2] == ("MCPServer.deploy", "usage")
    (payload,) = send.call_args.kwargs["api_calls"]
    assert json.loads(payload) == {
        "tools": {
            "python_procedure": 1,
            "python_function": 1,
            "SYSTEM_EXECUTE_SQL": 1,
        },
    }


def test_deploy_telemetry_failure_does_not_fail_deploy():
    session = make_session()
    session._conn._telemetry_client.send_function_usage_telemetry.side_effect = (
        RuntimeError("telemetry down")
    )
    result = python_server("S").deploy(session)
    assert isinstance(result, DeployResult)


# ---------------------------------------------------------------------------
# deploy: grants
# ---------------------------------------------------------------------------

GRANT_ROWS = [
    Row(privilege="OWNERSHIP", granted_to="ROLE", grantee_name="OWNER_ROLE"),
    Row(privilege="USAGE", granted_to="ROLE", grantee_name="ANALYST"),
    Row(privilege="USAGE", granted_to="DATABASE_ROLE", grantee_name="APP.READERS"),
]


def test_deploy_preserves_grants():
    session = make_session(
        [
            ("SHOW MCP SERVERS", [Row(name="S")]),
            ("SHOW GRANTS ON MCP SERVER", GRANT_ROWS),
        ]
    )
    python_server("S").deploy(session)
    statements = session.statements
    assert "SHOW MCP SERVERS LIKE 'S' IN SCHEMA APP.MCP" in statements
    assert "SHOW GRANTS ON MCP SERVER APP.MCP.S" in statements
    grants = [s for s in statements if s.startswith("GRANT")]
    assert grants == [
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE "ANALYST"',
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO DATABASE ROLE "APP"."READERS"',
    ]
    create_index = next(
        i for i, s in enumerate(statements) if s.startswith("CREATE OR REPLACE MCP")
    )
    assert all(statements.index(g) > create_index for g in grants)
    # Grants are read before the server is replaced.
    assert statements.index("SHOW GRANTS ON MCP SERVER APP.MCP.S") < create_index


def test_deploy_new_server_has_no_grants_to_preserve():
    session = make_session([("SHOW MCP SERVERS", [])])
    python_server("S").deploy(session)
    assert not any(s.startswith("SHOW GRANTS") for s in session.statements)
    assert not any(s.startswith("GRANT") for s in session.statements)


def test_deploy_preserve_grants_false():
    session = make_session([("SHOW MCP SERVERS", [Row(name="S")])])
    python_server("S").deploy(session, preserve_grants=False)
    assert not any(s.startswith("SHOW MCP") for s in session.statements)
    assert not any(s.startswith("SHOW GRANTS") for s in session.statements)


def test_deploy_dry_run_reports_grants_without_running_them():
    session = make_session(
        [
            ("SHOW MCP SERVERS", [Row(name="S")]),
            ("SHOW GRANTS ON MCP SERVER", GRANT_ROWS),
        ]
    )
    result = python_server("S").deploy(session, dry_run=True)
    assert result.sql[-2:] == [
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE "ANALYST"',
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO DATABASE ROLE "APP"."READERS"',
    ]
    assert not any(s.startswith("GRANT") for s in session.statements)
    assert all(c.args[0].startswith("SHOW") for c in session.sql.call_args_list)


def test_deploy_dry_run_tolerates_unreadable_schema():
    session = make_session()
    session.sql.side_effect = RuntimeError("Schema does not exist")
    result = python_server("S").deploy(session, dry_run=True)
    assert result.pruned == []
    assert result.sql[0].startswith("CREATE STAGE")
    assert not any(s.startswith("GRANT") for s in result.sql)


def grant_session(rows, fail_on=()):
    def run(statement):
        if any(f in statement for f in fail_on):
            raise RuntimeError("insufficient privileges")
        return []

    return make_session(
        [
            ("SHOW MCP SERVERS", [Row(name="S")]),
            ("SHOW GRANTS ON MCP SERVER", rows),
            ("GRANT", run),
        ]
    )


def test_deploy_grants_quote_grantee_names():
    session = grant_session(
        [
            Row(privilege="USAGE", granted_to="ROLE", grantee_name="Analysts"),
            Row(privilege="USAGE", granted_to="ROLE", grantee_name="my role"),
            Row(privilege="USAGE", granted_to="ROLE", grantee_name='odd"name'),
            Row(
                privilege="USAGE", granted_to="DATABASE_ROLE", grantee_name="Db.My Role"
            ),
        ]
    )
    python_server("S").deploy(session)
    assert [s for s in session.statements if s.startswith("GRANT")] == [
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE "Analysts"',
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE "my role"',
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE "odd""name"',
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO DATABASE ROLE "Db"."My Role"',
    ]


@pytest.mark.parametrize("grant_option", ["true", "TRUE", True])
def test_deploy_grants_keep_grant_option(grant_option):
    session = grant_session(
        [
            Row(
                privilege="USAGE",
                granted_to="ROLE",
                grantee_name="A",
                grant_option=grant_option,
            ),
            Row(
                privilege="USAGE",
                granted_to="ROLE",
                grantee_name="B",
                grant_option="false",
            ),
        ]
    )
    python_server("S").deploy(session)
    assert [s for s in session.statements if s.startswith("GRANT")] == [
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE "A" WITH GRANT OPTION',
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE "B"',
    ]


def test_deploy_applies_all_grants_and_reports_the_failures():
    session = grant_session(
        [
            Row(privilege="USAGE", granted_to="ROLE", grantee_name="A"),
            Row(privilege="USAGE", granted_to="ROLE", grantee_name="B"),
            Row(privilege="USAGE", granted_to="ROLE", grantee_name="C"),
        ],
        fail_on=['"B"'],
    )
    with pytest.raises(RuntimeError) as exc_info:
        python_server("S").deploy(session, prune=True)
    message = str(exc_info.value)
    assert "APP.MCP.S was replaced" in message
    assert "1 of its previous grants" in message
    assert 'GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE "B"' in message
    assert "insufficient privileges" in message
    assert '"A"' not in message and '"C"' not in message
    # The grants after the failing one still ran.
    grants = [s for s in session.statements if s.startswith("GRANT")]
    assert len(grants) == 3


def test_deploy_grants_ignore_other_servers_matching_the_like_pattern():
    # LIKE 'S_X' also matches 'SAX'; '_' is a wildcard.
    session = make_session(
        [
            ("SHOW MCP SERVERS", [Row(name="SAX")]),
            ("SHOW GRANTS ON MCP SERVER", GRANT_ROWS),
        ]
    )
    MCPServer("S_X", [get_account]).deploy(session)
    assert not any(s.startswith("SHOW GRANTS") for s in session.statements)
    assert not any(s.startswith("GRANT") for s in session.statements)


def test_deploy_grants_exact_server_name_is_case_insensitive():
    session = make_session(
        [
            ("SHOW MCP SERVERS", [Row(name="SAX"), Row(name="s_x")]),
            ("SHOW GRANTS ON MCP SERVER", GRANT_ROWS),
        ]
    )
    MCPServer("S_X", [get_account]).deploy(session)
    assert any(s.startswith("SHOW GRANTS") for s in session.statements)


# ---------------------------------------------------------------------------
# deploy: prune
# ---------------------------------------------------------------------------


def prune_session(procedures, functions=()):
    return make_session(
        [
            ("SHOW PROCEDURES IN SCHEMA APP.MCP", procedures),
            ("SHOW USER FUNCTIONS IN SCHEMA APP.MCP", list(functions)),
        ]
    )


def show_row(name, types, comment, returns="VARIANT"):
    return Row(
        name=name,
        arguments=f"{name}({types}) RETURN {returns}",
        description=comment,
    )


def test_deploy_prune_drops_only_removed_tools_of_this_server():
    session = prune_session(
        procedures=[
            # A current tool, with SHOW's spelling of the types.
            show_row("S__GET_ACCOUNT", "VARCHAR, NUMBER", tagged("S", "get_account")),
            # A removed tool of this server.
            show_row("S__OLD_TOOL", "VARCHAR, NUMBER", tagged("S", "old_tool")),
            # An old overload of a current tool.
            show_row("S__GET_ACCOUNT", "VARCHAR", tagged("S", "get_account")),
            # Another server's object, another suffix's object, and an untagged one.
            show_row("OTHER__X", "VARCHAR", tagged("OTHER", "x")),
            show_row("S_DEV__OLD", "VARCHAR", tagged("S", "old", "_DEV")),
            show_row("S__MINE", "VARCHAR", "user-defined procedure"),
            show_row("S__JSONISH", "VARCHAR", '{"origin": "else"}'),
        ],
        functions=[
            show_row("S__ADD_NUMBERS", "NUMBER, NUMBER", tagged("S", "add_numbers")),
            show_row("S__OLD_UDF", "NUMBER", tagged("S", "old_udf")),
        ],
    )
    result = python_server("S").deploy(session, prune=True)
    drops = [s for s in session.statements if s.startswith("DROP")]
    assert drops == [
        "DROP PROCEDURE IF EXISTS APP.MCP.S__GET_ACCOUNT(VARCHAR)",
        "DROP PROCEDURE IF EXISTS APP.MCP.S__OLD_TOOL(VARCHAR, NUMBER)",
        "DROP FUNCTION IF EXISTS APP.MCP.S__OLD_UDF(NUMBER)",
    ]
    assert result.pruned == [
        "APP.MCP.S__GET_ACCOUNT(VARCHAR)",
        "APP.MCP.S__OLD_TOOL(VARCHAR, NUMBER)",
        "APP.MCP.S__OLD_UDF(NUMBER)",
    ]
    # Pruning happens after the server is replaced; the old overload of a current
    # tool is dropped before it (see the next tests).
    create_index = next(
        i
        for i, s in enumerate(session.statements)
        if s.startswith("CREATE OR REPLACE MCP")
    )
    assert session.statements.index(drops[0]) < create_index
    assert session.statements.index(drops[1]) > create_index


def test_deploy_prune_keeps_current_tool_with_show_type_spelling():
    session = prune_session(
        [show_row("S__GET_ACCOUNT", "VARCHAR, NUMBER", tagged("S", "get_account"))]
    )
    result = python_server("S").deploy(session, prune=True)
    assert result.pruned == []
    assert not any(s.startswith("DROP") for s in session.statements)


def test_deploy_prune_respects_suffix():
    session = make_session(
        [
            (
                "SHOW PROCEDURES IN SCHEMA APP.MCP",
                [
                    show_row("S_DEV__OLD", "VARCHAR", tagged("S", "old", "_DEV")),
                    show_row("S__OLD", "VARCHAR", tagged("S", "old", "")),
                ],
            )
        ]
    )
    python_server("S").deploy(session, suffix="_dev", prune=True)
    drops = [s for s in session.statements if s.startswith("DROP")]
    assert drops == ["DROP PROCEDURE IF EXISTS APP.MCP.S_DEV__OLD(VARCHAR)"]


def test_deploy_without_prune_keeps_objects_of_removed_tools():
    session = prune_session([show_row("S__OLD", "VARCHAR", tagged("S", "old"))])
    result = python_server("S").deploy(session)
    assert not any(s.startswith("DROP") for s in session.statements)
    assert result.pruned == []


def test_deploy_prune_keeps_datetime_tool_when_show_reports_ntz():
    @mcp_tool(read_only=True)
    def events_since(session: Session, since: datetime.datetime):
        """Events.

        Args:
            since: Start.
        """

    session = prune_session(
        [show_row("S__EVENTS_SINCE", "TIMESTAMP_NTZ", tagged("S", "events_since"))]
    )
    result = MCPServer("S", [events_since]).deploy(session, prune=True)
    assert result.pruned == []
    assert not any(s.startswith("DROP") for s in session.statements)


def test_datetime_parameter_is_registered_as_timestamp_ntz():
    @mcp_tool(read_only=True)
    def events_since(since: datetime.datetime):
        """Events.

        Args:
            since: Start.
        """

    assert events_since.parameters[0].datatype == TimestampType(TimestampTimeZone.NTZ)
    assert events_since._signature(_Target("APP", "MCP", "", "S")).endswith(
        "(TIMESTAMP_NTZ)"
    )
    # An object created under TIMESTAMP_TYPE_MAPPING = TIMESTAMP_LTZ is another
    # overload: it is dropped, and the current one kept.
    session = prune_session(
        [
            show_row("S__EVENTS_SINCE", "TIMESTAMP_LTZ", tagged("S", "events_since")),
            show_row("S_DEV__EVENTS_SINCE", "TIMESTAMP_NTZ", tagged("S", "x", "_DEV")),
        ],
        functions=[
            show_row("S__EVENTS_SINCE", "TIMESTAMP_NTZ", tagged("S", "events_since"))
        ],
    )
    result = MCPServer("S", [events_since]).deploy(session)
    assert result.pruned == ["APP.MCP.S__EVENTS_SINCE(TIMESTAMP_LTZ)"]


def test_deploy_drops_old_overload_when_parameter_types_change_without_prune():
    @mcp_tool(read_only=True)
    def lookup(session: Session, key: str):
        """Look up.

        Args:
            key: The key.
        """

    session = prune_session(
        [
            show_row("S__LOOKUP", "NUMBER", tagged("S", "lookup")),
            show_row("S__LOOKUP", "VARCHAR", tagged("S", "lookup")),
            # Untagged, another server's, another suffix's and a removed tool's
            # objects are left alone without prune.
            show_row("S__LOOKUP", "BOOLEAN", "user-defined procedure"),
            show_row("OTHER__LOOKUP", "NUMBER", tagged("OTHER", "lookup")),
            show_row("S_DEV__LOOKUP", "NUMBER", tagged("S", "lookup", "_DEV")),
            show_row("S__GONE", "NUMBER", tagged("S", "gone")),
        ],
    )
    result = MCPServer("S", [lookup]).deploy(session)
    drops = [s for s in session.statements if s.startswith("DROP")]
    assert drops == ["DROP PROCEDURE IF EXISTS APP.MCP.S__LOOKUP(NUMBER)"]
    assert result.pruned == ["APP.MCP.S__LOOKUP(NUMBER)"]
    assert result.sql.index(drops[0]) < next(
        i for i, s in enumerate(result.sql) if s.startswith("CREATE OR REPLACE MCP")
    )
    # Registered before the old overload is dropped.
    assert session.statements.index("REGISTER PROCEDURE APP.MCP.S__LOOKUP") < (
        session.statements.index(drops[0])
    )


def test_deploy_dry_run_reports_old_overload():
    session = prune_session([show_row("S__GET_ACCOUNT", "NUMBER", tagged("S", "x"))])
    result = python_server("S").deploy(session, dry_run=True)
    assert result.pruned == ["APP.MCP.S__GET_ACCOUNT(NUMBER)"]
    assert "DROP PROCEDURE IF EXISTS APP.MCP.S__GET_ACCOUNT(NUMBER)" in result.sql
    assert not any(s.startswith("DROP") for s in session.statements)


def test_deploy_dry_run_prune_reports_but_does_not_drop():
    session = prune_session([show_row("S__OLD", "VARCHAR", tagged("S", "old"))])
    result = python_server("S").deploy(session, prune=True, dry_run=True)
    assert result.pruned == ["APP.MCP.S__OLD(VARCHAR)"]
    assert result.sql[-1] == "DROP PROCEDURE IF EXISTS APP.MCP.S__OLD(VARCHAR)"
    # Only the two SHOW statements ran.
    assert all(c.args[0].startswith("SHOW") for c in session.sql.call_args_list)


def test_deploy_prune_drops_procedure_replaced_by_function_of_same_signature():
    @mcp_tool(read_only=True)
    def lookup(x: str):
        """Look up.

        Args:
            x: The key.
        """

    session = prune_session([show_row("S__LOOKUP", "VARCHAR", tagged("S", "lookup"))])
    MCPServer("S", [lookup]).deploy(session, prune=True)
    assert "DROP PROCEDURE IF EXISTS APP.MCP.S__LOOKUP(VARCHAR)" in session.statements


# ---------------------------------------------------------------------------
# grant_usage
# ---------------------------------------------------------------------------


def grant_server():
    return MCPServer(
        "S",
        [
            get_account,
            add_numbers,
            tools.CortexAgent("APP.AG.SALES", description="Agent."),
            tools.CortexAgent("APP.AG.OPS", description="Agent."),
            tools.CortexAnalyst("APP.SV.REVENUE", description="Revenue."),
            tools.CortexSearch("APP.SS.DOCS", description="Docs."),
            tools.SqlExecution(),
        ],
    )


def test_grant_usage_dry_run():
    session = make_session()
    statements = grant_server().grant_usage(session, "ANALYST", dry_run=True)
    assert statements == [
        "GRANT USAGE ON DATABASE APP TO ROLE ANALYST",
        "GRANT USAGE ON SCHEMA APP.MCP TO ROLE ANALYST",
        "GRANT USAGE ON MCP SERVER APP.MCP.S TO ROLE ANALYST",
        "GRANT USAGE ON PROCEDURE APP.MCP.S__GET_ACCOUNT(STRING, BIGINT) TO ROLE ANALYST",
        "GRANT USAGE ON FUNCTION APP.MCP.S__ADD_NUMBERS(BIGINT, BIGINT) TO ROLE ANALYST",
        "GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE ANALYST",
        "GRANT USAGE ON SCHEMA APP.AG TO ROLE ANALYST",
        "GRANT USAGE ON AGENT APP.AG.SALES TO ROLE ANALYST",
        "GRANT USAGE ON AGENT APP.AG.OPS TO ROLE ANALYST",
        "GRANT USAGE ON SCHEMA APP.SV TO ROLE ANALYST",
        "GRANT SELECT ON SEMANTIC VIEW APP.SV.REVENUE TO ROLE ANALYST",
        "GRANT USAGE ON SCHEMA APP.SS TO ROLE ANALYST",
        "GRANT USAGE ON CORTEX SEARCH SERVICE APP.SS.DOCS TO ROLE ANALYST",
    ]
    assert len(statements) == len(set(statements))
    session.sql.assert_not_called()


def test_grant_usage_runs_statements():
    session = make_session()
    statements = python_server("S").grant_usage(session, "ANALYST", suffix="_dev")
    assert session.statements == statements
    assert "GRANT USAGE ON MCP SERVER APP.MCP.S_DEV TO ROLE ANALYST" in statements
    assert (
        "GRANT USAGE ON PROCEDURE APP.MCP.S_DEV__GET_ACCOUNT(STRING, BIGINT) TO ROLE ANALYST"
        in statements
    )


def test_grant_usage_quoted_role():
    statements = python_server().grant_usage(make_session(), '"my role"', dry_run=True)
    assert statements[0] == 'GRANT USAGE ON DATABASE APP TO ROLE "my role"'


@pytest.mark.parametrize("role", ["PUBLIC", "public", '"PUBLIC"', " Public "])
def test_grant_usage_refuses_public(role):
    session = make_session()
    with pytest.raises(ValueError, match="PUBLIC"):
        python_server().grant_usage(session, role)
    session.sql.assert_not_called()


@pytest.mark.parametrize(
    "role", ["", "a b", "x; DROP ROLE y", "r-1", '"unterminated', "1role"]
)
def test_grant_usage_refuses_invalid_role(role):
    with pytest.raises(ValueError, match="Invalid role"):
        python_server().grant_usage(make_session(), role, dry_run=True)


def test_grant_usage_resolves_existing_tools():
    session = financials_session()
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    statements = server.grant_usage(session, "R", dry_run=True)
    assert statements[-1] == (
        "GRANT USAGE ON PROCEDURE APP.FIN.GET_FINANCIALS(VARCHAR, NUMBER) TO ROLE R"
    )


# ---------------------------------------------------------------------------
# call_tool
# ---------------------------------------------------------------------------


def call_session(value):
    return make_session(
        [("CALL", [Row(RESULT=value)]), ("SELECT", [Row(RESULT=value)])]
    )


def test_call_tool_procedure():
    session = call_session('{"id": "x", "days": 3}')
    result = python_server("S").call_tool(
        session, "get_account", {"account_id": "x", "days": 3}
    )
    assert result == {"id": "x", "days": 3}
    (statement,) = session.statements
    assert statement == (
        "CALL APP.MCP.S__GET_ACCOUNT(account_id => 'x'::STRING, days => 3::BIGINT)"
    )


def test_call_tool_udf_uses_select():
    session = call_session("3")
    result = python_server("S").call_tool(session, "add_numbers", {"a": 1, "b": 2})
    assert result == 3
    (statement,) = session.statements
    assert statement == "SELECT APP.MCP.S__ADD_NUMBERS(a => 1::BIGINT, b => 2::BIGINT)"


def test_call_tool_with_suffix_and_schema():
    session = call_session('"ok"')
    result = python_server("S").call_tool(
        session, "get_account", {"account_id": "x"}, schema="DEV", suffix="_dev"
    )
    assert result == "ok"
    assert session.statements[0].startswith(
        "CALL APP.DEV.S_DEV__GET_ACCOUNT(account_id => 'x'::STRING)"
    )


def test_call_tool_escapes_strings():
    session = call_session("{}")
    python_server("S").call_tool(session, "get_account", {"account_id": "o'brien; --"})
    assert "account_id => 'o''brien; --'::STRING" in session.statements[0]


def test_call_tool_escapes_quote_and_backslash():
    session = call_session("{}")
    python_server("S").call_tool(session, "get_account", {"account_id": "it's \\ ok"})
    assert session.statements[0] == (
        "CALL APP.MCP.S__GET_ACCOUNT(account_id => 'it''s \\\\ ok'::STRING)"
    )


def test_call_tool_null_list_and_dict_arguments():
    @mcp_tool(read_only=True)
    def f(a: Optional[str], b: List[int], c: dict, d: bool):
        """Doc.

        Args:
            a: A.
            b: B.
            c: C.
            d: D.
        """

    session = call_session("1")
    MCPServer("S", [f]).call_tool(
        session, "f", {"a": None, "b": [1, 2], "c": {"k": "v"}, "d": True}
    )
    assert session.statements[0] == (
        "SELECT APP.MCP.S__F(a => NULL, b => PARSE_JSON('[1, 2]')::ARRAY, "
        'c => PARSE_JSON(\'{"k": "v"}\')::OBJECT, d => TRUE::BOOLEAN)'
    )


def test_call_tool_no_arguments():
    @mcp_tool(read_only=True)
    def nothing(session: Session):
        """Nothing."""

    session = call_session("null")
    assert MCPServer("S", [nothing]).call_tool(session, "nothing") is None
    assert session.statements == ["CALL APP.MCP.S__NOTHING()"]


def test_call_tool_non_string_value_is_returned_as_is():
    session = call_session({"already": "parsed"})
    assert python_server("S").call_tool(
        session, "get_account", {"account_id": "x"}
    ) == {"already": "parsed"}


def test_call_tool_empty_result():
    session = make_session()
    assert (
        python_server("S").call_tool(session, "get_account", {"account_id": "x"})
        is None
    )


def test_call_tool_invalid_arguments():
    session = call_session("{}")
    server = python_server("S")
    with pytest.raises(ValueError, match="missing required argument"):
        server.call_tool(session, "get_account", {})
    with pytest.raises(ValueError, match="unknown argument"):
        server.call_tool(session, "get_account", {"account_id": "x", "nope": 1})
    with pytest.raises(ValueError, match="must be of type integer"):
        server.call_tool(session, "get_account", {"account_id": "x", "days": "3"})
    assert session.statements == []


def test_call_tool_unknown_tool():
    with pytest.raises(KeyError):
        python_server().call_tool(make_session(), "nope")


def test_call_tool_builtin_tool_is_not_callable():
    server = MCPServer("S", [tools.SqlExecution(name="sx")])
    with pytest.raises(NotImplementedError, match="SYSTEM_EXECUTE_SQL"):
        server.call_tool(make_session(), "sx", {})


def test_call_tool_existing_procedure_with_string_argument():
    session = financials_session()
    session.responses.append(("CALL", [Row(R="Revenue: 1")]))
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    assert (
        server.call_tool(session, "get_financials", {"ticker": "SNOW"}) == "Revenue: 1"
    )
    assert (
        session.statements[-1]
        == "CALL APP.FIN.GET_FINANCIALS(TICKER => 'SNOW'::VARCHAR)"
    )


def test_call_tool_existing_procedure_with_number_argument():
    session = financials_session()
    session.responses.append(("CALL", [Row(R="Revenue: 1")]))
    server = MCPServer(
        "S", [Tool.from_procedure("APP.FIN.GET_FINANCIALS", read_only=True)]
    )
    server.call_tool(session, "get_financials", {"ticker": "SNOW", "years": 3})
    assert session.statements[-1] == (
        "CALL APP.FIN.GET_FINANCIALS(TICKER => 'SNOW'::VARCHAR, YEARS => 3::NUMBER)"
    )


def test_call_tool_int_for_float_parameter():
    @mcp_tool(read_only=True)
    def scale(x: float):
        """Scale.

        Args:
            x: Value.
        """

    session = call_session("1")
    MCPServer("S", [scale]).call_tool(session, "scale", {"x": 2})
    assert session.statements[0] == "SELECT APP.MCP.S__SCALE(x => 2::DOUBLE)"


def test_call_tool_date_parameter():
    import datetime

    @mcp_tool(read_only=True)
    def on(day: datetime.date):
        """On a day.

        Args:
            day: The day.
        """

    session = call_session("1")
    MCPServer("S", [on]).call_tool(session, "on", {"day": "2024-01-02"})
    assert "day => '2024-01-02'::DATE" in session.statements[0]


# ---------------------------------------------------------------------------
# drop
# ---------------------------------------------------------------------------


def test_drop_server_only():
    session = make_session()
    statements = python_server("S").drop(session, suffix="_dev")
    assert statements == ["DROP MCP SERVER IF EXISTS APP.MCP.S_DEV"]
    assert session.statements == statements


def test_drop_include_tools():
    session = prune_session(
        [
            show_row("S__GET_ACCOUNT", "VARCHAR, NUMBER", tagged("S", "get_account")),
            show_row("OTHER__X", "VARCHAR", tagged("OTHER", "x")),
        ],
        [show_row("S__ADD_NUMBERS", "NUMBER, NUMBER", tagged("S", "add_numbers"))],
    )
    statements = python_server("S").drop(session, include_tools=True)
    assert statements == [
        "DROP MCP SERVER IF EXISTS APP.MCP.S",
        "DROP PROCEDURE IF EXISTS APP.MCP.S__GET_ACCOUNT(VARCHAR, NUMBER)",
        "DROP FUNCTION IF EXISTS APP.MCP.S__ADD_NUMBERS(NUMBER, NUMBER)",
    ]


# ---------------------------------------------------------------------------
# endpoint_url, _signature_key, _Target
# ---------------------------------------------------------------------------


def test_endpoint_url():
    url = python_server("S").endpoint_url(make_session(), schema="MCP", suffix="_dev")
    assert url == (
        "https://my-org-my-acct.snowflakecomputing.com/api/v2/databases/APP/"
        "schemas/MCP/mcp-servers/S_DEV"
    )


def test_endpoint_url_explicit_database_and_schema():
    url = MCPServer("srv").endpoint_url(make_session(), database="db1", schema="s2")
    assert url.endswith("/databases/DB1/schemas/S2/mcp-servers/SRV")


def test_endpoint_url_quotes_special_names():
    url = MCPServer("S").endpoint_url(make_session(), database='"my db"', schema="MCP")
    assert "/databases/my%20db/schemas/MCP/" in url


def test_signature_key_normalizes_type_synonyms():
    assert _signature_key("A.B.X(STRING, BIGINT)") == _signature_key(
        "A.B.X(VARCHAR, NUMBER)"
    )
    assert _signature_key("A.B.X(TEXT, NUMBER(38,0))") == _signature_key(
        "a.b.x(VARCHAR, INT)"
    )
    assert _signature_key("A.B.X(DOUBLE)") == _signature_key("A.B.X(FLOAT)")
    assert _signature_key("A.B.X(VARCHAR)") != _signature_key("A.B.X(VARCHAR, VARCHAR)")
    assert _signature_key("A.B.X(VARCHAR)") != _signature_key("A.B.Y(VARCHAR)")
    assert _signature_key("A.B.X(VARCHAR)") != _signature_key("A.B.X(BOOLEAN)")
    assert _signature_key("A.B.X()") == ("A.B.X", ())


def test_target_names():
    target = _Target("APP", "MCP", "_DEV", "S")
    assert target.schema_fqn == "APP.MCP"
    assert target.server_fqn == "APP.MCP.S_DEV"
    assert target.object_fqn("X") == "APP.MCP.X"


def test_target_normalizes_identifiers():
    server = MCPServer("S")
    target = server._target(None, "app", '"MCP"', "")
    assert (target.database, target.schema) == ("APP", "MCP")
    target = server._target(None, '"Mixed Case"', "x", "")
    assert target.database == '"Mixed Case"'
    with pytest.raises(ValueError, match="Invalid identifier"):
        server._target(None, "a b", "x", "")


@pytest.mark.parametrize("param", ["start", "order", "select", "Group"])
def test_lint_reserved_parameter_name_is_error(param):
    ns = {}
    exec(
        f"def tool({param}: int) -> int:\n"
        f'    """Doc.\n\n    Args:\n        {param}: A number.\n    """\n'
        f"    return {param}\n",
        ns,
    )
    tool = mcp_tool(ns["tool"], read_only=True)
    issues = MCPServer("S", tools=[tool]).lint()
    assert any(
        i.level == "error" and "reserved word" in i.message and param in i.message
        for i in issues
    )


@pytest.mark.parametrize("param", ["first", "count", "limit", "date", "name"])
def test_lint_allows_non_reserved_parameter_names(param):
    ns = {}
    exec(f'def tool({param}: int) -> int:\n    """Doc."""\n    return 1\n', ns)
    tool = mcp_tool(ns["tool"], read_only=True)
    issues = MCPServer("S", tools=[tool]).lint()
    assert not any("reserved word" in i.message for i in issues)


@pytest.mark.parametrize(
    "annotation", ["typing.List[int]", "dict", "typing.Dict[str, int]", "list"]
)
def test_lint_array_and_object_parameters_are_errors(annotation):
    ns = {"typing": __import__("typing")}
    exec(
        f"def tool(v: {annotation}) -> int:\n"
        '    """Doc.\n\n    Args:\n        v: A value.\n    """\n'
        "    return 1\n",
        ns,
    )
    issues = MCPServer("S", tools=[mcp_tool(ns["tool"], read_only=True)]).lint()
    assert any(
        i.level == "error" and "cannot pass to a procedure or UDF" in i.message
        for i in issues
    )


def test_deploy_refuses_array_parameter_before_running_sql():
    @mcp_tool(read_only=True)
    def tool(session: Session, ids: List[int]) -> int:
        """Doc.

        Args:
            ids: IDs.
        """
        return 1

    session = make_session()
    with pytest.raises(MCPLintError, match="ids"):
        MCPServer("S", tools=[tool]).deploy(session)
    session.sproc.register.assert_not_called()
    assert not any(
        "CREATE" in str(c.args[0]) for c in session.sql.call_args_list if c.args
    )


def test_mask_error_details_defaults_on_and_reaches_handler():
    import sys

    @mcp_tool(read_only=True)
    def boom(session: Session, x: int) -> int:
        """Doc.

        Args:
            x: A number.
        """
        raise ValueError("secret")

    assert boom.mask_error_details is True
    session = make_session()
    MCPServer("S", tools=[boom]).deploy(session)
    handler = session.sproc.register.call_args.kwargs["func"]
    old = getattr(sys, "tracebacklimit", None)
    try:
        with pytest.raises(RuntimeError, match="unexpected error") as exc_info:
            handler(None, 1)
        assert "secret" not in str(exc_info.value)
    finally:
        if old is None:
            if hasattr(sys, "tracebacklimit"):
                del sys.tracebacklimit
        else:
            sys.tracebacklimit = old
    # invoke() keeps the original error for local debugging.
    with pytest.raises(ValueError, match="secret"):
        boom.invoke(object(), x=1)


def test_mask_error_details_off_reraises_original():
    @mcp_tool(read_only=True, mask_error_details=False)
    def boom(x: int) -> int:
        """Doc.

        Args:
            x: A number.
        """
        raise ValueError("secret")

    session = make_session()
    MCPServer("S", tools=[boom]).deploy(session)
    handler = session.udf.register.call_args.kwargs["func"]
    with pytest.raises(ValueError, match="secret"):
        handler(1)


def test_register_passes_input_schema_and_session_flag():
    session = make_session()
    server = python_server("S")
    server.deploy(session)

    handler = session.sproc.register.call_args.kwargs["func"]
    schema = get_account.input_schema
    assert schema["required"] == ["account_id"]
    # The session is skipped, and the check happens before the function is called.
    assert handler(None, None) == {
        "error": {"type": "INVALID_ARGUMENTS", "message": "account_id must not be null"}
    }
    assert handler(None, "x") == {"id": "x", "days": 3}

    udf_handler = session.udf.register.call_args.kwargs["func"]
    assert udf_handler(1) == 2
    assert udf_handler(None)["error"]["message"] == "a must not be null"


def test_registered_handler_checks_enum_and_does_not_call_function():
    called = []

    @mcp_tool(read_only=True)
    def pick(session: Session, kind: Literal["a", "b"]) -> str:
        """Pick one.

        Args:
            kind: Which one.
        """
        called.append(kind)
        return kind

    session = make_session()
    MCPServer("S", tools=[pick]).deploy(session)
    handler = session.sproc.register.call_args.kwargs["func"]
    assert handler(None, "zzz") == {
        "error": {
            "type": "INVALID_ARGUMENTS",
            "message": "kind must be one of ['a', 'b'], got 'zzz'",
            "hint": "Allowed values for kind: a, b.",
        }
    }
    assert called == []
    assert handler(None, kind="a") == "a"


# ---------------------------------------------------------------------------
# Optional parameters without a default
# ---------------------------------------------------------------------------


@mcp_tool(read_only=True)
def maybe(session: Session, key: str, note: Optional[str], limit: Optional[int] = None):
    """Look something up.

    Args:
        key: The key.
        note: A note, or null.
        limit: A limit.
    """
    return {"key": key, "note": note, "limit": limit}


def test_optional_without_default_is_required_in_the_schema():
    assert maybe.input_schema == {
        "type": "object",
        "properties": {
            "key": {"type": "string", "description": "The key."},
            "note": {"type": ["string", "null"], "description": "A note, or null."},
            "limit": {"type": ["integer", "null"], "description": "A limit."},
        },
        "required": ["key", "note"],
    }


def test_optional_without_default_invoke():
    assert maybe.invoke(object(), key="k", note=None) == {
        "key": "k",
        "note": None,
        "limit": None,
    }
    assert maybe.invoke(object(), key="k", note="n", limit=2)["limit"] == 2
    with pytest.raises(ValueError, match="missing required argument 'note'"):
        maybe.invoke(object(), key="k")
    with pytest.raises(ValueError, match="key must not be null"):
        maybe.invoke(object(), key=None, note=None)


def test_optional_without_default_deployed_handler_accepts_null():
    handler = mcp_tool_runtime.make_handler(
        maybe.func, input_schema=maybe.input_schema, takes_session=True
    )
    assert handler(object(), "k", None) == {"key": "k", "note": None, "limit": None}
    assert handler(object(), "k", note=None, limit=None)["note"] is None
    assert handler(object(), None, "n") == {
        "error": {"type": "INVALID_ARGUMENTS", "message": "key must not be null"}
    }


def test_optional_without_default_call_tool_passes_null():
    session = call_session('{"key": "k"}')
    MCPServer("S", [maybe]).call_tool(session, "maybe", {"key": "k", "note": None})
    assert session.statements[0] == (
        "CALL APP.MCP.S__MAYBE(key => 'k'::STRING, note => NULL)"
    )
    with pytest.raises(ValueError, match="missing required argument 'note'"):
        MCPServer("S", [maybe]).call_tool(session, "maybe", {"key": "k"})


def test_optional_container_parameter_is_still_linted():
    @mcp_tool(read_only=True)
    def f(items: Optional[List[int]] = None, sql: Optional[str] = None):
        """Doc.

        Args:
            items: Items.
            sql: SQL.
        """

    issues = MCPServer("S", [f]).lint()
    assert any("takes a JSON array" in i.message for i in issues)


# ---------------------------------------------------------------------------
# invoke converts arguments like the MCP server
# ---------------------------------------------------------------------------


def test_invoke_converts_arguments_to_sql_types():
    @mcp_tool(read_only=True)
    def f(
        d: datetime.date,
        ts: datetime.datetime,
        t: datetime.time,
        amount: decimal.Decimal,
        ratio: float,
        n: int,
        label: str,
    ):
        """Doc.

        Args:
            d: D.
            ts: TS.
            t: T.
            amount: Amount.
            ratio: Ratio.
            n: N.
            label: Label.
        """
        return [type(v).__name__ for v in (d, ts, t, amount, ratio, n, label)], str(
            amount
        )

    types_, amount = f.invoke(
        d="2024-01-02",
        ts="2024-01-02T03:04:05",
        t="03:04:05",
        amount=1.25,
        ratio=2,
        n=3,
        label="2024-01-02",
    )
    assert types_ == ["date", "datetime", "time", "Decimal", "float", "int", "str"]
    assert amount == "1.25"


def test_invoke_leaves_null_arguments_alone():
    @mcp_tool(read_only=True)
    def f(d: Optional[datetime.date] = None):
        """Doc."""
        return {"d": d}

    assert f.invoke(d=None) == {"d": None}
    assert f.invoke(d="2024-01-02") == {"d": "2024-01-02"}


def test_invoke_invalid_date_string():
    @mcp_tool(read_only=True)
    def f(d: datetime.date):
        """Doc."""

    with pytest.raises(ValueError, match="Invalid arguments for tool f: d is not a"):
        f.invoke(d="yesterday")


# ---------------------------------------------------------------------------
# Annotations that cannot be resolved
# ---------------------------------------------------------------------------


def test_unresolvable_session_annotation_falls_back_to_its_name(tmp_path):
    path = tmp_path / "tool_module_session.py"
    path.write_text(
        "from __future__ import annotations\n"
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    import pandas as pd\n"
        "def tool(session: Session, n: int) -> pd.DataFrame:\n"
        "    pass\n"
    )
    import importlib.util

    spec = importlib.util.spec_from_file_location("tool_module_session", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    tool = mcp_tool(module.tool, read_only=True, description="Doc.")
    assert tool.kind == "procedure"
    assert [p.name for p in tool.parameters] == ["n"]
    assert tool._param_problems == []


# ---------------------------------------------------------------------------
# call_tool: existing objects' types and names
# ---------------------------------------------------------------------------


def existing_session(signature, show_arguments):
    return make_session(
        [
            (
                "SHOW PROCEDURES LIKE",
                [Row(name="P", arguments=show_arguments, description="Do p")],
            ),
            ("DESCRIBE PROCEDURE", [Row(property="signature", value=signature)]),
            ("CALL", [Row(x="ok")]),
        ]
    )


def test_call_tool_existing_object_casts_to_the_describe_type():
    session = existing_session("(AMOUNT NUMBER(10,2))", "P(NUMBER) RETURN VARCHAR")
    server = MCPServer("S", [Tool.from_procedure("APP.MCP.P", read_only=True)])
    server.call_tool(session, "p", {"amount": 1.25})
    assert session.statements[-1] == "CALL APP.MCP.P(AMOUNT => 1.25::NUMBER(10,2))"
    # Grants and signature matching use SHOW's type.
    assert server.tool("p")._signature() == "APP.MCP.P(NUMBER)"


def test_call_tool_existing_object_quotes_case_sensitive_parameter_names():
    session = existing_session(
        '("id" VARCHAR, "My ""Col""" NUMBER, PLAIN VARCHAR)',
        "P(VARCHAR, NUMBER, VARCHAR) RETURN VARCHAR",
    )
    server = MCPServer("S", [Tool.from_procedure("APP.MCP.P", read_only=True)])
    server.call_tool(session, "p", {"id": "x", 'My "Col"': 1, "plain": "y"})
    assert session.statements[-1] == (
        'CALL APP.MCP.P("id" => \'x\'::VARCHAR, "My ""Col""" => 1::NUMBER, '
        "PLAIN => 'y'::VARCHAR)"
    )


@pytest.mark.parametrize(
    "value, literal",
    [
        (float("nan"), "'NaN'"),
        (float("inf"), "'inf'"),
        (float("-inf"), "'-inf'"),
    ],
)
def test_call_tool_non_finite_float(value, literal):
    @mcp_tool(read_only=True)
    def scale(x: float):
        """Scale.

        Args:
            x: Value.
        """

    session = call_session("1")
    MCPServer("S", [scale]).call_tool(session, "scale", {"x": value})
    assert session.statements[0] == f"SELECT APP.MCP.S__SCALE(x => {literal}::DOUBLE)"


# ---------------------------------------------------------------------------
# Re-review follow-ups
# ---------------------------------------------------------------------------


def overload_grant_session(grant_rows, fail_on=()):
    def grants(statement):
        return grant_rows if "VARCHAR" in statement else []

    def run(statement):
        if any(f in statement for f in fail_on):
            raise RuntimeError("insufficient privileges")
        return []

    return make_session(
        [
            (
                "SHOW PROCEDURES IN SCHEMA APP.MCP",
                [show_row("S__GET_ACCOUNT", "VARCHAR", tagged("S", "get_account"))],
            ),
            ("SHOW MCP SERVERS", [Row(name="S")]),
            ("SHOW GRANTS ON MCP SERVER", [GRANT_ROWS[1]]),
            ("SHOW GRANTS ON PROCEDURE", grants),
            ("GRANT", run),
        ]
    )


OVERLOAD_GRANTS = [
    Row(privilege="OWNERSHIP", granted_to="ROLE", grantee_name="OWNER_ROLE"),
    Row(
        privilege="USAGE", granted_to="ROLE", grantee_name="Reader", grant_option="true"
    ),
    Row(privilege="USAGE", granted_to="DATABASE_ROLE", grantee_name="APP.READERS"),
]


def test_deploy_moves_grants_of_a_dropped_overload_to_the_new_object():
    session = overload_grant_session(OVERLOAD_GRANTS)
    result = python_server("S").deploy(session)
    new = "PROCEDURE APP.MCP.S__GET_ACCOUNT(STRING, BIGINT)"
    statements = session.statements
    assert "SHOW GRANTS ON PROCEDURE APP.MCP.S__GET_ACCOUNT(VARCHAR)" in statements
    assert statements.index(
        "SHOW GRANTS ON PROCEDURE APP.MCP.S__GET_ACCOUNT(VARCHAR)"
    ) < statements.index("DROP PROCEDURE IF EXISTS APP.MCP.S__GET_ACCOUNT(VARCHAR)")
    grants = [s for s in statements if s.startswith("GRANT") and new in s]
    assert grants == [
        f'GRANT USAGE ON {new} TO ROLE "Reader" WITH GRANT OPTION',
        f'GRANT USAGE ON {new} TO DATABASE ROLE "APP"."READERS"',
    ]
    assert result.pruned == ["APP.MCP.S__GET_ACCOUNT(VARCHAR)"]


def test_deploy_dry_run_reports_grants_of_a_dropped_overload():
    session = overload_grant_session(OVERLOAD_GRANTS)
    result = python_server("S").deploy(session, dry_run=True)
    new = "PROCEDURE APP.MCP.S__GET_ACCOUNT(STRING, BIGINT)"
    assert f'GRANT USAGE ON {new} TO ROLE "Reader" WITH GRANT OPTION' in result.sql
    assert not any(s.startswith("GRANT") for s in session.statements)


def test_deploy_overload_grants_not_read_without_preserve_grants():
    session = overload_grant_session(OVERLOAD_GRANTS)
    python_server("S").deploy(session, preserve_grants=False)
    assert not any(s.startswith("SHOW GRANTS") for s in session.statements)


def test_deploy_prune_runs_when_grants_fail():
    session = overload_grant_session(OVERLOAD_GRANTS, fail_on=['"Reader"'])
    session.responses.insert(
        0,
        (
            "SHOW USER FUNCTIONS IN SCHEMA APP.MCP",
            [show_row("S__OLD_UDF", "NUMBER", tagged("S", "old_udf"))],
        ),
    )
    with pytest.raises(RuntimeError) as exc_info:
        python_server("S").deploy(session, prune=True)
    assert "pruning ran" in str(exc_info.value)
    assert '"Reader"' in str(exc_info.value)
    assert "DROP FUNCTION IF EXISTS APP.MCP.S__OLD_UDF(NUMBER)" in session.statements
    assert any(
        "DATABASE ROLE" in s for s in session.statements if s.startswith("GRANT")
    )


def test_deploy_grants_do_not_requote_quoted_database_role_parts():
    session = grant_session(
        [
            Row(
                privilege="USAGE",
                granted_to="DATABASE_ROLE",
                grantee_name='"my db".READERS',
            ),
            Row(
                privilege="USAGE",
                granted_to="APPLICATION_ROLE",
                grantee_name='APP."my role"',
            ),
        ]
    )
    python_server("S").deploy(session)
    assert [s for s in session.statements if s.startswith("GRANT")] == [
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO DATABASE ROLE "my db"."READERS"',
        'GRANT USAGE ON MCP SERVER APP.MCP.S TO APPLICATION ROLE "APP"."my role"',
    ]
