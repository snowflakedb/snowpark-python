#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Built-in tools for :class:`~snowflake.snowpark.mcp.MCPServer`.

Built-in tools are the MCP server's own tool types: :class:`CortexAgent`,
:class:`CortexAnalyst`, :class:`CortexSearch`, :class:`SqlExecution` and
:class:`CodingAgent`. Prefer :class:`CortexAgent` and :class:`CortexAnalyst` over
procedures that call an agent or Cortex Analyst: they avoid the extra wait of a
procedure call.
"""

from typing import Any, Dict, List, Optional

from snowflake.snowpark._internal import mcp_utils
from snowflake.snowpark._internal.mcp_utils import LintIssue
from snowflake.snowpark.mcp import MCPTool, _annotations, _Target

__all__ = [
    "CodingAgent",
    "CortexAgent",
    "CortexAnalyst",
    "CortexSearch",
    "SqlExecution",
]


def _default_name(identifier: str) -> str:
    return mcp_utils.split_qualified_name(identifier)[-1].strip('"').lower()


def _schema_usage(identifier: str) -> List[str]:
    """Usage on the database and schema of a fully qualified object."""
    parts = mcp_utils.split_qualified_name(identifier)
    if len(parts) != 3:
        return []
    return [f"USAGE ON DATABASE {parts[0]}", f"USAGE ON SCHEMA {parts[0]}.{parts[1]}"]


class _BuiltinTool(MCPTool):
    _object_grant: Optional[str] = None  # e.g. "USAGE ON AGENT"

    def __init__(
        self,
        identifier: Optional[str],
        name: Optional[str],
        title: Optional[str],
        description: Optional[str],
        annotations: Optional[Dict[str, bool]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        if identifier is not None:
            parts = mcp_utils.split_qualified_name(identifier)
            if not 1 <= len(parts) <= 3 or not all(parts):
                raise ValueError(f"Invalid object name {identifier!r}.")
        super().__init__(
            name or (_default_name(identifier) if identifier else self._fallback_name),
            title,
            description,
            annotations,
        )
        self.identifier = identifier
        self.config = {k: v for k, v in (config or {}).items() if v is not None}

    _fallback_name = "tool"

    def _lint(self) -> List[LintIssue]:
        issues = super()._lint()
        if (
            self.identifier
            and len(mcp_utils.split_qualified_name(self.identifier)) != 3
        ):
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    f"{self.identifier} is not fully qualified (DATABASE.SCHEMA.NAME).",
                )
            )
        return issues

    def _spec(self, target: _Target) -> Dict[str, Any]:
        spec = self._spec_base(self.identifier)
        if self.config:
            spec["config"] = dict(self.config)
        if self.annotations:
            spec["annotations"] = dict(self.annotations)
        return spec

    def _grants(self, target: _Target) -> List[str]:
        if not self._object_grant or not self.identifier:
            return []
        return _schema_usage(self.identifier) + [
            f"{self._object_grant} {self.identifier}"
        ]


def _optional_annotations(
    read_only: Optional[bool],
    destructive: Optional[bool],
    idempotent: bool,
    open_world: bool,
) -> Optional[Dict[str, bool]]:
    if read_only is None and destructive is None:
        return None
    return _annotations(bool(read_only), destructive, idempotent, open_world)


class CortexAgent(_BuiltinTool):
    """A Cortex Agent as a tool (``CORTEX_AGENT_RUN``).

    Args:
        agent: Fully qualified agent name.
        name: Tool name. Defaults to the agent name in lowercase.
        title: Human-readable title.
        description: What the agent can answer; the model reads it to choose the tool.
        read_only: Whether the agent only reads data. When omitted, no annotations
            are set and clients treat the tool as one that may change data.
        destructive: Whether the agent may delete or overwrite data.
        idempotent: Whether repeating a call has no further effect.
        open_world: Whether the agent reaches systems outside Snowflake.
    """

    tool_type = "CORTEX_AGENT_RUN"
    _object_grant = "USAGE ON AGENT"

    def __init__(
        self,
        agent: str,
        *,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        read_only: Optional[bool] = None,
        destructive: Optional[bool] = None,
        idempotent: bool = False,
        open_world: bool = False,
    ) -> None:
        super().__init__(
            agent,
            name,
            title,
            description,
            _optional_annotations(read_only, destructive, idempotent, open_world),
        )

    def _grants(self, target: _Target) -> List[str]:
        return ["DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER"] + super()._grants(target)


class CortexAnalyst(_BuiltinTool):
    """Cortex Analyst over a semantic view as a tool (``CORTEX_ANALYST_MESSAGE``).
    It returns generated SQL to the client; to have the SQL run and summarized,
    expose a :class:`CortexAgent` that uses the semantic view instead.

    Args:
        semantic_view: Fully qualified semantic view name.
        name: Tool name. Defaults to the view name in lowercase.
        title: Human-readable title.
        description: What the semantic view covers.
    """

    tool_type = "CORTEX_ANALYST_MESSAGE"
    _object_grant = "SELECT ON SEMANTIC VIEW"

    def __init__(
        self,
        semantic_view: str,
        *,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(semantic_view, name, title, description)


class CortexSearch(_BuiltinTool):
    """A Cortex Search service as a tool (``CORTEX_SEARCH_SERVICE_QUERY``).

    Args:
        service: Fully qualified Cortex Search service name.
        name: Tool name. Defaults to the service name in lowercase.
        title: Human-readable title.
        description: What the service searches.
    """

    tool_type = "CORTEX_SEARCH_SERVICE_QUERY"
    _object_grant = "USAGE ON CORTEX SEARCH SERVICE"

    def __init__(
        self,
        service: str,
        *,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(service, name, title, description)


class SqlExecution(_BuiltinTool):
    """The MCP server's SQL execution tool (``SYSTEM_EXECUTE_SQL``).

    It runs SQL with the role of the MCP client and skips any semantic views and
    agents. Put it on a server of its own with a least-privileged role.

    Args:
        name: Tool name.
        title: Human-readable title.
        description: Description for the model.
        read_only: Only allow SELECT statements.
        warehouse: Warehouse to run queries on.
        query_timeout: Query timeout in seconds.
    """

    tool_type = "SYSTEM_EXECUTE_SQL"
    _fallback_name = "sql_execution"

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = "Run a SQL query in Snowflake.",
        read_only: bool = True,
        warehouse: Optional[str] = None,
        query_timeout: Optional[int] = None,
    ) -> None:
        super().__init__(
            None,
            name,
            title,
            description,
            config={
                "read_only": read_only,
                "warehouse": warehouse,
                "query_timeout": query_timeout,
            },
        )


class CodingAgent(_BuiltinTool):
    """The MCP server's coding agent (``CODING_AGENT``), which runs code in a
    sandbox tied to the conversation.

    Args:
        name: Tool name.
        title: Human-readable title.
        description: Description for the model.
        model: Model the agent uses.
        instructions: Instructions for the agent.
        tools: Additional agent tools, as in the agent specification.
        tool_resources: Resources for those tools, as in the agent specification.
        skills: Skills for the agent, as in the agent specification.
    """

    tool_type = "CODING_AGENT"
    _fallback_name = "coding_agent"

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = "Write and run code to answer a question.",
        model: Optional[str] = None,
        instructions: Optional[str] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_resources: Optional[Dict[str, Any]] = None,
        skills: Optional[List[Any]] = None,
    ) -> None:
        super().__init__(
            None,
            name,
            title,
            description,
            config={
                "model": model,
                "instructions": instructions,
                "tools": tools,
                "tool_resources": tool_resources,
                "skills": skills,
            },
        )

    def _grants(self, target: _Target) -> List[str]:
        return ["DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER"]
