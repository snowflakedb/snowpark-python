#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import re
from typing import Literal, Optional

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col
from snowflake.snowpark.mcp import MCPServer, Tool, ToolError, mcp_tool

pytestmark = [
    pytest.mark.udf,
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="MCP servers are not supported in Local Testing",
    ),
]


@mcp_tool(read_only=True)
def numbers(session: Session, first: int, count: int = 3) -> object:
    """Return a range of numbers.

    Args:
        first: First number.
        count: How many numbers.
    """
    if count > 100:
        raise ToolError("count is too large", hint="Ask for at most 100 numbers.")
    return session.range(first, first + count).select(col("ID").alias("N"))


@mcp_tool(read_only=True)
def add(a: int, b: Optional[int] = None) -> int:
    """Add two numbers.

    Args:
        a: First number.
        b: Second number, 1 when omitted.
    """
    return a + (1 if b is None else b)


def _schema_name(temp_schema: str) -> str:
    return temp_schema.split(".")[-1]


def test_deploy_and_call_python_tools(session, temp_schema):
    schema = _schema_name(temp_schema)
    server = MCPServer("SNOWPARK_MCP_TEST", tools=[numbers, add])
    result = server.deploy(session, schema=schema, suffix="_DEV")
    try:
        assert result.server.endswith(f"{schema.upper()}.SNOWPARK_MCP_TEST_DEV")
        assert len(result.objects) == 2

        table = server.call_tool(
            session, "numbers", {"first": 5}, schema=schema, suffix="_DEV"
        )
        assert table["columns"] == ["N"]
        assert [r["N"] for r in table["rows"]] == [5, 6, 7]
        assert table["truncated"] is False

        error = server.call_tool(
            session,
            "numbers",
            {"first": 0, "count": 1000},
            schema=schema,
            suffix="_DEV",
        )
        assert error == {
            "error": {
                "type": "ToolError",
                "message": "count is too large",
                "hint": "Ask for at most 100 numbers.",
            }
        }

        assert (
            server.call_tool(session, "add", {"a": 2}, schema=schema, suffix="_DEV")
            == 3
        )
        assert (
            server.call_tool(
                session, "add", {"a": 2, "b": 5}, schema=schema, suffix="_DEV"
            )
            == 7
        )

        servers = session.sql(
            f"SHOW MCP SERVERS LIKE 'SNOWPARK_MCP_TEST_DEV' IN SCHEMA {temp_schema}"
        ).collect()
        assert len(servers) == 1
    finally:
        server.drop(session, schema=schema, suffix="_DEV", include_tools=True)


def test_redeploy_with_prune_drops_removed_tools(session, temp_schema):
    schema = _schema_name(temp_schema)
    server = MCPServer("SNOWPARK_MCP_PRUNE", tools=[numbers, add])
    server.deploy(session, schema=schema)
    try:
        smaller = MCPServer("SNOWPARK_MCP_PRUNE", tools=[add])
        result = smaller.deploy(session, schema=schema, prune=True)
        assert len(result.pruned) == 1
        assert "SNOWPARK_MCP_PRUNE__NUMBERS" in result.pruned[0]
        remaining = session.sql(
            f"SHOW USER FUNCTIONS LIKE 'SNOWPARK_MCP_PRUNE__ADD' IN SCHEMA {temp_schema}"
        ).collect()
        assert len(remaining) == 1
    finally:
        server.drop(session, schema=schema, include_tools=True)


def test_tool_from_existing_sql_procedure(session, temp_schema):
    schema = _schema_name(temp_schema)
    session.sql(
        f"""CREATE OR REPLACE PROCEDURE {temp_schema}.GREET(NAME VARCHAR, TIMES NUMBER DEFAULT 1)
        RETURNS VARCHAR LANGUAGE SQL
        COMMENT = 'Greet someone.'
        AS 'BEGIN RETURN REPEAT(''hi '' || NAME || '' '', TIMES); END'"""
    ).collect()
    greet = Tool.from_procedure(
        f"{temp_schema}.GREET", read_only=True, param_descriptions={"name": "Who"}
    )
    server = MCPServer("SNOWPARK_MCP_EXISTING", tools=[greet])
    server.deploy(session, schema=schema)
    try:
        assert greet.description == "Greet someone."
        assert [p.name for p in greet.parameters] == ["name", "times"]
        assert [p.required for p in greet.parameters] == [True, False]
        assert (
            server.call_tool(session, "greet", {"name": "a", "times": 2}, schema=schema)
            == "hi a hi a "
        )
    finally:
        server.drop(session, schema=schema)


@mcp_tool(read_only=True, mask_error_details=False)
def two_statements(session: Session, x: int) -> int:
    """Run two statements in one call.

    Args:
        x: Unused.
    """
    return session.sql("SELECT 1; SELECT 2").collect()[0][0]


def test_multiple_statements_are_rejected_by_snowflake(session, temp_schema):
    schema = _schema_name(temp_schema)
    server = MCPServer("SNOWPARK_MCP_MULTI", tools=[two_statements])
    server.deploy(session, schema=schema)
    try:
        with pytest.raises(SnowparkSQLException, match="statement count"):
            server.call_tool(session, "two_statements", {"x": 1}, schema=schema)
    finally:
        server.drop(session, schema=schema, include_tools=True)


def test_grant_usage_dry_run(session, temp_schema):
    schema = _schema_name(temp_schema)
    server = MCPServer("SNOWPARK_MCP_GRANTS", tools=[numbers, add])
    statements = server.grant_usage(
        session, "SOME_ROLE", schema=schema, suffix="_DEV", dry_run=True
    )
    assert any(
        s.startswith("GRANT USAGE ON MCP SERVER ")
        and s.endswith(f".{schema.upper()}.SNOWPARK_MCP_GRANTS_DEV TO ROLE SOME_ROLE")
        for s in statements
    )
    assert any(
        "ON PROCEDURE" in s and "__NUMBERS(BIGINT, BIGINT)" in s for s in statements
    )
    assert any("ON FUNCTION" in s and "__ADD(BIGINT, BIGINT)" in s for s in statements)


@mcp_tool(read_only=True)
def broken(session: Session, x: int) -> int:
    """Fail with an unexpected error.

    Args:
        x: Unused.
    """
    raise ValueError("internal detail")


def test_unexpected_error_is_masked(session, temp_schema):
    schema = _schema_name(temp_schema)
    server = MCPServer("SNOWPARK_MCP_MASK", tools=[broken])
    server.deploy(session, schema=schema)
    try:
        with pytest.raises(SnowparkSQLException) as exc_info:
            server.call_tool(session, "broken", {"x": 1}, schema=schema)
        message = str(exc_info.value)
        assert "broken failed with an unexpected error" in message
        assert re.search(r"error ID [0-9a-f]{12}\)", message)
        assert "Retrying with the same arguments will not help" in message
        assert "internal detail" not in message
        assert "Traceback" not in message
    finally:
        server.drop(session, schema=schema, include_tools=True)


@mcp_tool(read_only=True)
def pick(kind: Literal["a", "b"]) -> str:
    """Pick one.

    Args:
        kind: Which one.
    """
    return kind


def test_deployed_tool_checks_enum_arguments(session, temp_schema):
    schema = _schema_name(temp_schema)
    server = MCPServer("SNOWPARK_MCP_ENUM", tools=[pick])
    result = server.deploy(session, schema=schema)
    try:
        fq_name = result.objects[0].split("(")[0]
        # Raw SQL bypasses the client-side check of call_tool.
        rows = session.sql(f"SELECT {fq_name}(kind => 'zzz')").collect()
        assert json.loads(rows[0][0]) == {
            "error": {
                "type": "INVALID_ARGUMENTS",
                "message": "kind must be one of ['a', 'b'], got 'zzz'",
                "hint": "Allowed values for kind: a, b.",
            }
        }
        rows = session.sql(f"SELECT {fq_name}(kind => 'a')").collect()
        assert json.loads(rows[0][0]) == "a"
    finally:
        server.drop(session, schema=schema, include_tools=True)


@mcp_tool(read_only=True, name="lookup")
def lookup_by_number(key: int) -> str:
    """Look up a value by number.

    Args:
        key: The key.
    """
    return f"number {key}"


@mcp_tool(read_only=True, name="lookup")
def lookup_by_text(key: str) -> str:
    """Look up a value by text.

    Args:
        key: The key.
    """
    return f"text {key}"


def test_redeploy_with_changed_parameter_type_leaves_one_overload(session, temp_schema):
    schema = _schema_name(temp_schema)
    first = MCPServer("SNOWPARK_MCP_RETYPE", tools=[lookup_by_number])
    first.deploy(session, schema=schema)
    second = MCPServer("SNOWPARK_MCP_RETYPE", tools=[lookup_by_text])
    try:
        result = second.deploy(session, schema=schema)
        assert len(result.pruned) == 1
        assert result.pruned[0].endswith("SNOWPARK_MCP_RETYPE__LOOKUP(NUMBER)")
        overloads = session.sql(
            "SHOW USER FUNCTIONS LIKE 'SNOWPARK_MCP_RETYPE__LOOKUP' "
            f"IN SCHEMA {temp_schema}"
        ).collect()
        assert len(overloads) == 1
        assert "VARCHAR" in overloads[0]["arguments"]
        assert (
            second.call_tool(session, "lookup", {"key": "a"}, schema=schema) == "text a"
        )
    finally:
        second.drop(session, schema=schema, include_tools=True)


@mcp_tool(read_only=True)
def add_or_keep(a: int, b: Optional[int]) -> int:
    """Add two numbers, or return the first when the second is null.

    Args:
        a: First number.
        b: Second number, or null.
    """
    return a if b is None else a + b


def test_optional_parameter_without_default_can_be_null(session, temp_schema):
    schema = _schema_name(temp_schema)
    server = MCPServer("SNOWPARK_MCP_NULLABLE", tools=[add_or_keep])
    server.deploy(session, schema=schema)
    try:
        assert add_or_keep.input_schema["required"] == ["a", "b"]
        assert (
            server.call_tool(session, "add_or_keep", {"a": 4, "b": None}, schema=schema)
            == 4
        )
        assert (
            server.call_tool(session, "add_or_keep", {"a": 4, "b": 3}, schema=schema)
            == 7
        )
    finally:
        server.drop(session, schema=schema, include_tools=True)
