#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Built-in and ready-made tools for :class:`~snowflake.snowpark.mcp.MCPServer`.

Built-in tools are the MCP server's own tool types: :class:`CortexAgent`,
:class:`CortexAnalyst`, :class:`CortexSearch`, :class:`SqlExecution` and
:class:`CodingAgent`. Prefer :class:`CortexAgent` and :class:`CortexAnalyst` over
procedures that call an agent or Cortex Analyst: they avoid the extra wait of a
procedure call.

Ready-made tools are Python tools Snowpark provides: :func:`governed_sql`, and
:func:`governed_sql_role_setup`, which returns the SQL that creates the read-only role
to deploy it with.
"""

from typing import Any, Dict, List, Literal, Optional

from snowflake.snowpark._internal import mcp_governed_sql, mcp_tool_runtime, mcp_utils
from snowflake.snowpark._internal.mcp_tool_runtime import (
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_MAX_ROWS,
)
from snowflake.snowpark._internal.mcp_utils import LintIssue
from snowflake.snowpark._internal.utils import private_preview
from snowflake.snowpark.mcp import (
    _PREVIEW_VERSION,
    MCPTool,
    PythonTool,
    _annotations,
    _is_quoted_identifier,
    _Target,
)

__all__ = [
    "CodingAgent",
    "CortexAgent",
    "CortexAnalyst",
    "CortexSearch",
    "SqlExecution",
    "governed_sql",
    "governed_sql_role_setup",
]

# Roles whose rights are too broad for a tool that runs the model's SQL.
_FORBIDDEN_TOOL_ROLES = frozenset(
    ("PUBLIC",) + tuple(sorted(mcp_utils.ADMIN_ROLE_NAMES))
)


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
    agents. Consider :func:`governed_sql` for row and size limits and per-call
    logging, and put either on a server of its own with a least-privileged role.

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


@private_preview(version=_PREVIEW_VERSION)
def governed_sql(
    *,
    name: str = "run_sql",
    read_only: bool = True,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
    query_timeout: Optional[int] = 60,
    warehouse: Optional[str] = None,
    execute_as: Literal["caller", "owner"] = "owner",
    feedback_tools: bool = False,
    feedback_table: Optional[str] = None,
) -> List[MCPTool]:
    """A ready-made tool that runs one SQL statement per call, with limits. Prefer
    narrow tools that run fixed SQL; use this as an open-ended fallback, on an MCP
    server of its own.

    - One statement per call, enforced by a lexical check and by Snowflake. With
      ``read_only=True``, the statement must start with SELECT, WITH, SHOW, DESCRIBE
      or EXPLAIN. This check is lexical; the role is what prevents writes.
    - Returns at most ``max_rows`` rows and ``max_result_bytes`` bytes, and says
      when the result was cut.
    - Logs one record per call (query, status, row count, duration) through the
      ``snowflake.snowpark.mcp.governed_sql`` logger, which reaches the event table
      when one is set up.

    **Owner's rights** (``execute_as="owner"``, the default and recommended):
    deploy the server with a session whose role is a dedicated read-only role, such
    as ``MCP_SQL_READER`` (see :func:`governed_sql_role_setup`). The procedure is
    owned by that role, so the model's queries can read only what that role can
    read, whatever role the MCP client connects with. Inside the procedure,
    ``CURRENT_ROLE()`` is the owner role and ``CURRENT_USER()`` is the calling user,
    so role-based row access and masking policies apply as for the owner (callers
    may see fewer rows or masked values than they would directly), while policies
    based on ``CURRENT_USER()`` still apply per user. Callers need only USAGE on
    the server and the procedure.

    **Caller's rights** (``execute_as="caller"``): each user's grants apply. This
    is safe only if every user's MCP session role is read-only. The role depends
    on how the client connects, and some clients, Claude included, request
    ``session:role:all``, which means the user's default role.

    With ``feedback_tools=True``, also returns ``log_feedback`` and
    ``log_data_issue`` tools that insert into ``feedback_table`` (created if
    missing), so the model can report problems without write access to anything
    else. They run with the owner's rights.

    Returns a list of tools; pass it to :class:`MCPServer` or ``MCPServer.add``.
    """
    if execute_as not in ("caller", "owner"):
        raise ValueError("execute_as must be 'caller' or 'owner'.")
    run_sql = mcp_governed_sql.make_run_sql(
        read_only, max_rows, max_result_bytes, mcp_tool_runtime.run_tool
    )
    tools: List[MCPTool] = [
        PythonTool(
            run_sql,
            read_only=read_only,
            name=name,
            kind="procedure",
            execute_as=execute_as,
            warehouse=warehouse,
            query_timeout=query_timeout,
            max_rows=max_rows,
            max_result_bytes=max_result_bytes,
            _ready_made="governed_sql",
        )
    ]
    if feedback_tools:
        if not feedback_table:
            raise ValueError("feedback_tools=True requires feedback_table.")
        parts = mcp_utils.split_qualified_name(feedback_table)
        if len(parts) != 3:
            raise ValueError(
                "feedback_table must be fully qualified (DATABASE.SCHEMA.TABLE)."
            )
        for tool_name, kind, doc in (
            (
                "log_feedback",
                "feedback",
                "Record feedback about an answer: what was asked, what was wrong or "
                "missing.\n\nArgs:\n    message: The feedback.\n    details: "
                "Optional JSON object, as text, with details such as the question "
                "and the query.",
            ),
            (
                "log_data_issue",
                "data_issue",
                "Report a problem in the data: wrong, missing or inconsistent "
                "values.\n\nArgs:\n    message: What is wrong, and where.\n    "
                "details: Optional JSON object, as text, with details such as the table "
                "and the query.",
            ),
        ):
            func = mcp_governed_sql.make_log_feedback(feedback_table, kind)
            func.__name__ = func.__qualname__ = tool_name
            func.__doc__ = doc
            func.__annotations__ = {"message": str, "details": Optional[str]}
            tools.append(
                PythonTool(
                    func,
                    read_only=False,
                    destructive=False,
                    name=tool_name,
                    kind="procedure",
                    execute_as="owner",
                    warehouse=warehouse,
                    query_timeout=query_timeout,
                    _setup_sql=lambda target: [
                        mcp_governed_sql.feedback_table_ddl(feedback_table)
                    ],
                    _ready_made="governed_sql_feedback",
                )
            )
    return tools


@private_preview(version=_PREVIEW_VERSION)
def governed_sql_role_setup(
    role: str,
    schema: str,
    read_from: List[str],
    warehouse: str,
    user: Optional[str] = None,
) -> List[str]:
    """Returns the SQL that creates the read-only role to deploy :func:`governed_sql`
    with. It does not run anything: a security administrator reviews and runs it.

    The role can use ``warehouse``, create the server's procedures, stage and MCP
    server in ``schema``, and read what ``read_from`` lists.

    Args:
        role: The role to create, such as ``"MCP_SQL_READER"``. PUBLIC and the
            system roles (ACCOUNTADMIN, SYSADMIN, ...) are refused.
        schema: ``"DATABASE.SCHEMA"`` where the server is deployed.
        read_from: What the model may read: ``"DATABASE.SCHEMA"`` for every table
            and view in a schema, now and in the future, or
            ``"DATABASE.SCHEMA.NAME"`` for one table. Granting a view needs
            ``GRANT SELECT ON VIEW`` instead of ``ON TABLE`` in the statement.
        warehouse: The warehouse the role may use.
        user: Grant the role to this user, too.
    """
    role = role.strip()
    _check_name(role, "role")
    if role.strip('"').upper() in _FORBIDDEN_TOOL_ROLES:
        raise ValueError(
            f"Refusing to use {role} for governed_sql: use a dedicated read-only role."
        )
    _check_name(warehouse, "warehouse")
    if user is not None:
        _check_name(user, "user")
    schema_parts = _qualified_parts(schema, 2, "schema")
    statements = [
        f"CREATE ROLE IF NOT EXISTS {role}",
        f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role}",
        f"GRANT USAGE ON DATABASE {schema_parts[0]} TO ROLE {role}",
        "GRANT USAGE, CREATE PROCEDURE, CREATE STAGE, CREATE MCP SERVER ON SCHEMA "
        f"{'.'.join(schema_parts)} TO ROLE {role}",
    ]
    for entry in read_from:
        parts = _qualified_parts(entry, None, "read_from entry")
        schema_fqn = ".".join(parts[:2])
        statements.append(f"GRANT USAGE ON DATABASE {parts[0]} TO ROLE {role}")
        statements.append(f"GRANT USAGE ON SCHEMA {schema_fqn} TO ROLE {role}")
        if len(parts) == 2:
            for scope in ("ALL", "FUTURE"):
                for kind in ("TABLES", "VIEWS"):
                    statements.append(
                        f"GRANT SELECT ON {scope} {kind} IN SCHEMA {schema_fqn} "
                        f"TO ROLE {role}"
                    )
        else:
            statements.append(f"GRANT SELECT ON TABLE {entry.strip()} TO ROLE {role}")
    if user is not None:
        statements.append(f"GRANT ROLE {role} TO USER {user}")
    return list(dict.fromkeys(statements))


def _check_name(name: str, what: str) -> None:
    if not (mcp_utils.is_identifier(name) or _is_quoted_identifier(name)):
        raise ValueError(f"Invalid {what} name {name!r}.")


def _qualified_parts(name: str, count: Optional[int], what: str) -> List[str]:
    parts = [p.strip() for p in mcp_utils.split_qualified_name(name.strip())]
    if count is None:
        count_ok = len(parts) in (2, 3)
        expected = "DATABASE.SCHEMA or DATABASE.SCHEMA.NAME"
    else:
        count_ok = len(parts) == count
        expected = "DATABASE.SCHEMA"
    if not count_ok:
        raise ValueError(f"Invalid {what} {name!r}: use {expected}.")
    for part in parts:
        _check_name(part, what)
    return parts
