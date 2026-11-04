#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Optional

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.mcp import MCPServer, Tool, ToolError, mcp_tool, tools

pytestmark = [
    pytest.mark.udf,
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="MCP servers are not supported in Local Testing",
    ),
]


@mcp_tool(read_only=True)
def numbers(session: Session, start: int, count: int = 3) -> object:
    """Return a range of numbers.

    Args:
        start: First number.
        count: How many numbers.
    """
    if count > 100:
        raise ToolError("count is too large", hint="Ask for at most 100 numbers.")
    return session.range(start, start + count).select(col("ID").alias("N"))


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
        assert result.server.endswith(f"{schema}.SNOWPARK_MCP_TEST_DEV")
        assert len(result.objects) == 2

        table = server.call_tool(
            session, "numbers", {"start": 5}, schema=schema, suffix="_DEV"
        )
        assert table["columns"] == ["N"]
        assert [r["N"] for r in table["rows"]] == [5, 6, 7]
        assert table["truncated"] is False

        error = server.call_tool(
            session,
            "numbers",
            {"start": 0, "count": 1000},
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


def test_governed_sql(session, temp_schema):
    schema = _schema_name(temp_schema)
    server = MCPServer("SNOWPARK_MCP_SQL", tools=tools.governed_sql(max_rows=2))
    server.deploy(session, schema=schema)
    try:
        result = server.call_tool(
            session,
            "run_sql",
            {"query": "SELECT SEQ4() AS N FROM TABLE(GENERATOR(ROWCOUNT => 5))"},
            schema=schema,
        )
        assert result["row_count"] == 2
        assert result["truncated"] is True

        shown = server.call_tool(
            session,
            "run_sql",
            {"query": f"SHOW PROCEDURES IN SCHEMA {temp_schema}"},
            schema=schema,
        )
        assert "rows" in shown

        rejected = server.call_tool(
            session, "run_sql", {"query": "DROP TABLE NOT_A_TABLE"}, schema=schema
        )
        assert rejected["error"]["type"] == "QUERY_REJECTED"
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
