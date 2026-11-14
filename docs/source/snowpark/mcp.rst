==============================
MCP Tools and Servers
==============================

.. automodule:: snowflake.snowpark.mcp
   :noindex:

.. currentmodule:: snowflake.snowpark.mcp

.. rubric:: Classes

.. autosummary::
    :toctree: api/

    MCPServer
    MCPTool
    Tool
    ToolError
    DeployResult
    LintIssue
    MCPLintError

.. rubric:: Functions

.. autosummary::
    :toctree: api/

    mcp_tool

.. rubric:: Methods

.. autosummary::
    :toctree: api/

    MCPServer.add
    MCPServer.call_tool
    MCPServer.deploy
    MCPServer.drop
    MCPServer.endpoint_url
    MCPServer.grant_usage
    MCPServer.lint
    MCPServer.to_spec
    MCPServer.to_yaml
    Tool.from_function
    Tool.from_procedure
    PythonTool.invoke

.. currentmodule:: snowflake.snowpark.mcp.tools

.. rubric:: Built-in tools

.. autosummary::
    :toctree: api/

    CodingAgent
    CortexAgent
    CortexAnalyst
    CortexSearch
    SqlExecution
