#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Build tools and servers for the Snowflake-managed MCP server in Python.

Define tools with :func:`mcp_tool` (from a Python function) or with
:meth:`Tool.from_procedure` / :meth:`Tool.from_function` (from an object that already
exists), add the MCP server's built-in tool types with :meth:`Tool.builtin`,
and put them in an :class:`MCPServer`. ``MCPServer.deploy`` registers the Python
tools as procedures or UDFs and creates the server; ``grant_usage`` grants a role
everything the server's tools need.

Prefer narrow tools: a tool that runs fixed SQL with the model's values bound as
parameters, or filters a DataFrame, can do nothing else, so there is no query for the
model to get wrong or abuse.

Example::

    from snowflake.snowpark import Session
    from snowflake.snowpark.mcp import MCPServer, mcp_tool

    @mcp_tool(read_only=True)
    def get_account(session: Session, account_id: str) -> dict:
        \"\"\"Return one account.

        Args:
            account_id: The account ID.
        \"\"\"
        rows = session.sql(
            "SELECT * FROM CRM.PUBLIC.ACCOUNTS WHERE ACCOUNT_ID = ?", params=[account_id]
        ).collect()
        return rows[0].as_dict() if rows else {}

    server = MCPServer("SALES_MCP", tools=[get_account])
    server.deploy(session, schema="MCP_DEV", suffix="_DEV")
    server.grant_usage(session, role="SALES_MCP_USER", schema="MCP_DEV", suffix="_DEV")
"""

from snowflake.snowpark.mcp._runtime import (  # noqa: F401
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_MAX_ROWS,
    PLATFORM_RESULT_LIMIT_BYTES,
    ToolError,
)
from snowflake.snowpark.mcp._server import DeployResult, MCPLintError, MCPServer
from snowflake.snowpark.mcp._tools import (  # noqa: F401
    ExistingObjectTool,
    PythonTool,
    Tool,
    mcp_tool,
)
from snowflake.snowpark.mcp._utils import LintIssue, ToolParameter  # noqa: F401

__all__ = [
    "DeployResult",
    "LintIssue",
    "MCPLintError",
    "MCPServer",
    "Tool",
    "ToolError",
    "mcp_tool",
]
