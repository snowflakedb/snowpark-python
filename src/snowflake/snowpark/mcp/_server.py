#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""The MCP server: deploying, granting and pruning."""

import dataclasses
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import quote as url_quote

import yaml

from snowflake.snowpark._internal.utils import private_preview
from snowflake.snowpark.mcp import _utils
from snowflake.snowpark.mcp._tools import (
    ExistingObjectTool,
    PythonTool,
    Tool,
    _Target,
    _quote_identifier,
    _signature_key,
    _sql_argument,
    _sql_param_name,
)
from snowflake.snowpark.mcp._utils import (
    LintIssue,
    _is_quoted_identifier,
    _normalize_identifier,
    _row_dict,
    _sql_type,
)

_logger = logging.getLogger("snowflake.snowpark.mcp")

_DEFAULT_STAGE_NAME = "SNOWPARK_MCP_STAGE"


class MCPLintError(ValueError):
    """Raised by :meth:`MCPServer.deploy` when its checks of the definition find errors."""

    def __init__(self, issues: List[LintIssue]) -> None:
        self.issues = issues
        super().__init__(
            "The MCP server definition has errors:\n"
            + "\n".join(str(i) for i in issues if i.level == "error")
        )


@dataclasses.dataclass
class DeployResult:
    """What :meth:`MCPServer.deploy` did, or would do with ``dry_run=True``."""

    #: Fully qualified name of the MCP server.
    server: str
    #: URL MCP clients connect to.
    endpoint_url: str
    #: Procedures and UDFs created or replaced, with their argument types.
    objects: List[str]
    #: Objects dropped: those of tools no longer in the server (``prune=True``),
    #: and old overloads of current tools whose parameter types changed (always).
    pruned: List[str]
    #: SQL statements run (or, with ``dry_run=True``, that would run), apart from
    #: the procedure and UDF registrations listed in ``objects``.
    sql: List[str]
    #: Warnings and notes from the checks of the definition.
    issues: List[LintIssue]


class MCPServer:
    """A Snowflake-managed MCP server defined in Python.

    The definition is independent of where it is deployed: pass ``database``,
    ``schema`` and ``suffix`` to each method to work with, for example, a dev and a
    prod copy of the same server. ``database`` and ``schema`` default to the
    session's current ones. Pass ``database``, ``schema`` and ``suffix`` the same way
    to ``deploy``, ``grant_usage``, ``call_tool`` and ``drop``: each call targets the
    copy they name.

    Args:
        name: The server name, an unquoted identifier.
        tools: The server's tools.
    """

    def __init__(self, name: str, tools: Iterable[Tool] = ()) -> None:
        if not _utils.is_identifier(name) or "$" in name:
            raise ValueError(
                f"Invalid MCP server name {name!r}: use letters, digits and '_', not "
                "starting with a digit."
            )
        self.name = name.upper()
        self.tools: List[Tool] = []
        self.add(*tools)

    def __repr__(self) -> str:
        return f"MCPServer(name={self.name!r}, tools={[t.name for t in self.tools]})"

    def add(self, *tools: Union[Tool, Iterable[Tool]]) -> "MCPServer":
        """Adds tools. Accepts tools and iterables of tools. Returns the
        server."""
        for tool in tools:
            if isinstance(tool, Tool):
                self.tools.append(tool)
            elif isinstance(tool, Iterable) and not isinstance(tool, (str, bytes)):
                self.add(*tool)
            else:
                raise TypeError(
                    f"Expected an MCP tool, got {type(tool).__name__}. Create tools "
                    "with mcp_tool, Tool.from_procedure, or Tool.builtin."
                )
        return self

    def tool(self, name: str) -> Tool:
        """Returns the tool with this name."""
        for tool in self.tools:
            if tool.name == name:
                return tool
        raise KeyError(f"The server has no tool named {name!r}.")

    # -- definition ---------------------------------------------------------

    def _lint(self, session=None) -> List[LintIssue]:
        """Checks the definition and returns the problems found. Errors stop
        :meth:`deploy`. With a session, tools made from existing objects are read
        from Snowflake first, so they are checked too."""
        if session is not None:
            self._resolve(session, self._target(session, None, None, ""))
        issues = _utils.lint_tool_count_and_names([t.name for t in self.tools])
        issues += _utils.lint_automatic_agent_tool_names(
            [(t.name, getattr(t, "tool_type", "GENERIC")) for t in self.tools]
        )
        for tool in self.tools:
            issues += tool._lint()
        no_warehouse = [
            t.name
            for t in self.tools
            if isinstance(t, (PythonTool, ExistingObjectTool)) and not t.warehouse
        ]
        if no_warehouse:
            issues.append(
                LintIssue(
                    "info",
                    None,
                    "Tools without a warehouse run on the caller's default "
                    "warehouse, which grant_usage doesn't grant: "
                    f"{', '.join(no_warehouse)}.",
                )
            )
        object_owners: Dict[str, str] = {}
        for tool in self.tools:
            if isinstance(tool, PythonTool):
                object_name = tool._object_name(_Target("", "", "", self.name))
                owner = object_owners.setdefault(object_name, tool.name)
                if owner != tool.name:
                    issues.append(
                        LintIssue(
                            "error",
                            tool.name,
                            f"The tool's procedure or UDF is named {object_name}, "
                            f"like the one of tool {owner}, so one would replace the "
                            "other; set object_name on one of them.",
                        )
                    )
        return issues

    def to_spec(
        self,
        session=None,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        suffix: str = "",
    ) -> Dict[str, Any]:
        """Returns the server's specification as a dict."""
        target = self._target(session, database, schema, suffix)
        if session is not None:
            self._resolve(session, target)
        unresolved = [t.name for t in self.tools if not t._is_resolved()]
        if unresolved:
            raise ValueError(
                f"Tools {unresolved} are made from existing objects, whose signatures "
                "are read from Snowflake: pass a session."
            )
        return {"tools": [t._spec(target) for t in self.tools]}

    def to_yaml(
        self,
        session=None,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        suffix: str = "",
    ) -> str:
        """Returns the server's specification as YAML, as ``CREATE MCP SERVER``
        takes it."""
        return yaml.safe_dump(
            self.to_spec(session, database=database, schema=schema, suffix=suffix),
            sort_keys=False,
            allow_unicode=True,
            width=1000,
        )

    def endpoint_url(
        self,
        session,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        suffix: str = "",
    ) -> str:
        """Returns the URL MCP clients connect to."""
        target = self._target(session, database, schema, suffix)
        return _endpoint_url(session, target)

    # -- deployment ---------------------------------------------------------

    @private_preview(version="1.58.0")
    def deploy(
        self,
        session,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        suffix: str = "",
        stage_location: Optional[str] = None,
        prune: bool = False,
        preserve_grants: bool = True,
        dry_run: bool = False,
    ) -> DeployResult:
        """Creates or replaces the server and the procedures and UDFs of its Python
        tools.

        Steps: check the definition (errors stop the deploy); read the tools made from existing
        objects; read the schema's objects, and stop, before creating anything, if
        an overload of a tool's object would make the server's reference to it
        ambiguous; register the Python tools; move the grants of the old overloads of
        the Python tools, whose parameter types changed, to the tools' new objects
        and drop the old overloads; create or replace the server; re-apply the
        server's grants; with ``prune=True``, drop the objects of tools no longer in
        the server. If a grant or a prune drop fails, the remaining grants and drops
        still run, and the error lists the ones that failed; a failure to drop an
        old overload stops the deploy before the server is replaced. The tool
        objects are created before the server is replaced, so the server never
        refers to a missing object. The deploy is not transactional: if replacing
        the server fails, the old server keeps running with the new tool code.

        Args:
            session: The session to deploy with.
            database: Target database. Defaults to the session's.
            schema: Target schema. Defaults to the session's.
            suffix: Appended to the server name and to its tools' object names,
                e.g. ``"_DEV"``.
            stage_location: Stage for the Python tools' code. Defaults to
                ``@<database>.<schema>.SNOWPARK_MCP_STAGE``, created if missing.
            prune: Drop procedures and UDFs this server created for tools it no
                longer has.
            preserve_grants: Re-apply the server's grants after replacing it.
            dry_run: Return what would be done without doing it. The server's
                grants and the existing objects are still read.
        """
        target = self._target(session, database, schema, suffix)
        self._resolve(session, target)
        issues = self._lint()
        if any(i.level == "error" for i in issues):
            raise MCPLintError(issues)
        spec_yaml = self.to_yaml(
            database=target.database, schema=target.schema, suffix=target.suffix
        )
        if "$$" in spec_yaml:
            raise ValueError(
                "A tool description or setting contains '$$', which cannot appear in "
                "a CREATE MCP SERVER specification."
            )

        sql: List[str] = []
        objects: List[str] = []
        pruned: List[str] = []

        def run(statement: str) -> list:
            sql.append(statement)
            if dry_run:
                return []
            return session.sql(statement).collect()

        python_tools = [t for t in self.tools if isinstance(t, PythonTool)]
        keep = {
            (t._grants(target)[0].split()[2], _signature_key(t._signature(target)))
            for t in python_tools
        }
        current = {_signature_key(t._signature(target))[0]: t for t in python_tools}
        tagged, ownership_warnings, listed = (
            self._tagged_objects(session, target, tolerate_errors=dry_run)
            if python_tools or prune
            else ({}, [], [])
        )
        issues += ownership_warnings
        # An old overload of a current tool, from before its parameter types changed,
        # is dropped even without prune: the server refers to a tool's object by name
        # only, which must be unambiguous. Its grants move to the new object first,
        # so a failure to replace the server below does not lose them.
        old_overloads = []
        overload_grants: List[str] = []
        for signature, grant_rows in tagged.items():
            kind, _, sig = signature.partition(" ")
            key = _signature_key(sig)
            if key[0] in current and (kind, key) not in keep:
                old_overloads.append(signature)
                if preserve_grants:
                    new_tool = current[key[0]]
                    overload_grants += _statements_from_grants(
                        grant_rows,
                        new_tool._grants(target)[0].partition(" ON ")[2],
                    )
        # Any other overload of a current tool's object, tagged or not and whatever its
        # owner, would make the server's name-only reference to the object ambiguous.
        ambiguous: Dict[str, List[str]] = {}
        for signature in listed:
            kind, _, sig = signature.partition(" ")
            key = _signature_key(sig)
            tool = current.get(key[0])
            if (
                tool is not None
                and kind == ("PROCEDURE" if tool.kind == "procedure" else "FUNCTION")
                and (kind, key) not in keep
                and signature not in old_overloads
            ):
                ambiguous.setdefault(tool.name, []).append(sig)
        for tool_name, signatures in ambiguous.items():
            message = (
                f"The object of tool {tool_name} also exists as "
                f"{', '.join(signatures)}, which this deploy neither replaces nor "
                "drops. The server refers to a tool's object by name only, so the "
                "call would be ambiguous. Drop those overloads, or set a different "
                "object_name on the tool."
            )
            if not dry_run:
                raise ValueError(message)
            issues.append(LintIssue("error", tool_name, message))

        if python_tools and stage_location is None:
            stage = target.object_fqn(_DEFAULT_STAGE_NAME)
            run(f"CREATE STAGE IF NOT EXISTS {stage}")
            stage_location = f"@{stage}"
        for tool in python_tools:
            objects.append(
                tool._signature(target)
                if dry_run
                else tool._register(session, target, stage_location)
            )

        failed_grants: List[Tuple[str, Exception]] = []
        failed_drops: List[Tuple[str, Exception]] = []

        def apply_grants(statements: List[str]) -> None:
            for statement in statements:
                try:
                    run(statement)
                except Exception as e:
                    failed_grants.append((statement, e))

        grants: List[str] = []
        if preserve_grants:
            grants = _server_grants(session, target, tolerate_errors=dry_run)
        apply_grants(list(dict.fromkeys(overload_grants)))
        for signature in old_overloads:
            pruned.append(signature)
            kind, _, sig = signature.partition(" ")
            run(f"DROP {kind} IF EXISTS {sig}")

        def failed_grants_text() -> str:
            return "\n".join(f"{stmt}  -- {err}" for stmt, err in failed_grants)

        try:
            run(
                f"CREATE OR REPLACE MCP SERVER {target.server_fqn} FROM SPECIFICATION "
                f"$$\n{spec_yaml}$$"
            )
        except Exception as e:
            if failed_grants:
                raise RuntimeError(
                    f"{e}\nThese grants of the old overloads of tools could not be "
                    f"moved to their new objects; grant them again:\n"
                    f"{failed_grants_text()}"
                ) from e
            raise
        apply_grants(grants)
        if prune:
            for signature in tagged:
                if signature in pruned:
                    continue
                kind, _, sig = signature.partition(" ")
                if (kind, _signature_key(sig)) not in keep:
                    pruned.append(signature)
                    try:
                        run(f"DROP {kind} IF EXISTS {sig}")
                    except Exception as e:
                        failed_drops.append((f"DROP {kind} IF EXISTS {sig}", e))

        if failed_grants or failed_drops:
            problems = []
            if failed_grants:
                one = len(failed_grants) == 1
                problems.append(
                    f"{len(failed_grants)} {'grant' if one else 'grants'} could not "
                    f"be applied; grant {'it' if one else 'them'} again:\n"
                    f"{failed_grants_text()}"
                )
            if failed_drops:
                one = len(failed_drops) == 1
                problems.append(
                    f"{len(failed_drops)} unused {'object' if one else 'objects'} "
                    f"could not be dropped; drop {'it' if one else 'them'}:\n"
                    + "\n".join(f"{stmt}  -- {err}" for stmt, err in failed_drops)
                )
            raise RuntimeError(
                f"The server {target.server_fqn} was replaced"
                + (" and pruning ran" if prune else "")
                + ", but "
                + "\n\nAlso, ".join(problems)
            )
        if not dry_run:
            self._send_telemetry(session)
        return DeployResult(
            server=target.server_fqn,
            endpoint_url=_endpoint_url(session, target),
            objects=objects,
            pruned=[s.partition(" ")[2] for s in pruned],
            sql=sql,
            issues=issues,
        )

    def grant_usage(
        self,
        session,
        role: str,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        suffix: str = "",
        dry_run: bool = False,
    ) -> List[str]:
        """Grants ``role`` what it needs to connect to the server and call each of
        its tools: usage on the database, schema and server, usage on every tool's
        procedure, UDF, agent or search service and its database and schema, select
        on semantic views, and usage on every warehouse set on a tool. Returns the
        statements, which are not run when ``dry_run`` is True.

        It does not grant what a tool uses internally: the search services, semantic
        views, tables and warehouses an agent uses, or the objects a procedure or
        UDF reads. Grant those separately.

        Tools without a ``warehouse`` run on the caller's default warehouse, which
        this can't know, so it grants no warehouse for them. It grants
        ``SNOWFLAKE.CORTEX_AGENT_USER`` for agent and coding agent tools and no other
        Cortex database role: the Snowflake documentation lists no Cortex role for
        Cortex Analyst or Cortex Search tools through MCP. If calls to those tools
        fail, grant the Cortex database role your account uses (such as
        ``SNOWFLAKE.CORTEX_USER``).
        """
        role = role.strip()
        if role.strip('"').upper() == "PUBLIC":
            raise ValueError(
                "Refusing to grant MCP tools to PUBLIC; grant them to a dedicated role."
            )
        if not (_utils.is_identifier(role) or _is_quoted_identifier(role)):
            raise ValueError(f"Invalid role name {role!r}.")
        target = self._target(session, database, schema, suffix)
        self._resolve(session, target)
        clauses = [
            f"USAGE ON DATABASE {target.database}",
            f"USAGE ON SCHEMA {target.schema_fqn}",
            f"USAGE ON MCP SERVER {target.server_fqn}",
        ]
        for tool in self.tools:
            clauses += tool._grants(target)
            clauses += [f"USAGE ON WAREHOUSE {w}" for w in tool._warehouses()]
        statements = [
            f"GRANT {clause} TO ROLE {role}" for clause in dict.fromkeys(clauses)
        ]
        if not dry_run:
            for statement in statements:
                session.sql(statement).collect()
        return statements

    def call_tool(
        self,
        session,
        tool_name: str,
        arguments: Optional[Dict[str, Any]] = None,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        suffix: str = "",
    ) -> Any:
        """Calls a deployed tool over ``session`` with tool-call ``arguments``, by
        name, and returns its result. Use it to test tools as deployed without
        setting up an MCP client. Only tools backed by procedures and UDFs can be
        called this way.

        A call here differs from one through the MCP endpoint: it runs with the
        session's role, not the role of the client's OAuth token or PAT; it ignores
        the tool's ``warehouse`` and ``query_timeout``; it doesn't apply the server's
        250 KB result truncation, which matters for :meth:`Tool.from_procedure` and
        :meth:`Tool.from_function` tools, as they have no result cap of their own;
        and it checks argument names and types locally, which the server doesn't
        (enum values and nulls are left to the deployed tool).
        """
        arguments = arguments or {}
        tool = self.tool(tool_name)
        target = self._target(session, database, schema, suffix)
        if isinstance(tool, PythonTool):
            params = tool.parameters
            identifier = target.object_fqn(tool._object_name(target))
            kind = tool.kind
            sql_names = {p.name: p.name for p in params}
            sql_types = {p.name: _sql_type(p.datatype) for p in params}
        elif isinstance(tool, ExistingObjectTool):
            if not tool._is_resolved():
                tool._resolve(session, target)
            params = tool.parameters or []
            identifier = tool._qualified_name
            kind = tool.kind
            sql_names = {p.name: _sql_param_name(p.sql_name or p.name) for p in params}
            sql_types = {p.name: p.cast_type or p.sql_type or "VARIANT" for p in params}
        else:
            raise NotImplementedError(
                f"Tool {tool_name} is a {tool.tool_type} tool; call it through the "
                "MCP endpoint."
            )
        # Enum values and nulls are not rejected: the deployed tool checks them.
        problems = _utils.check_arguments(
            _utils.input_schema(params), arguments, strict_values=False
        )
        if problems:
            raise ValueError(
                f"Invalid arguments for tool {tool_name}: " + "; ".join(problems)
            )
        args_sql = ", ".join(
            f"{sql_names[name]} => {_sql_argument(value, sql_types[name])}"
            for name, value in arguments.items()
        )
        if kind == "procedure":
            rows = session.sql(f"CALL {identifier}({args_sql})").collect()
        else:
            rows = session.sql(f"SELECT {identifier}({args_sql})").collect()
        value = rows[0][0] if rows else None
        if isinstance(tool, PythonTool) and isinstance(value, str):
            return json.loads(value)
        return value

    def drop(
        self,
        session,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        suffix: str = "",
        include_tools: bool = False,
    ) -> List[str]:
        """Drops the server and, with ``include_tools=True``, the procedures and
        UDFs it created, which the session's role owns. Returns the statements
        run."""
        target = self._target(session, database, schema, suffix)
        statements = [f"DROP MCP SERVER IF EXISTS {target.server_fqn}"]
        if include_tools:
            owned, warnings, _ = self._tagged_objects(session, target)
            for warning in warnings:
                _logger.warning("%s", warning.message)
            for signature in owned:
                kind, _, sig = signature.partition(" ")
                statements.append(f"DROP {kind} IF EXISTS {sig}")
        for statement in statements:
            session.sql(statement).collect()
        return statements

    # -- helpers ------------------------------------------------------------

    def _target(
        self, session, database: Optional[str], schema: Optional[str], suffix: str
    ) -> _Target:
        if suffix and (not _utils.is_identifier("X" + suffix) or "$" in suffix):
            raise ValueError(f"Invalid suffix {suffix!r}: use letters, digits and '_'.")
        if session is not None:
            database = database or session.get_current_database()
            schema = schema or session.get_current_schema()
        if not database or not schema:
            raise ValueError(
                "Pass database and schema, or a session with a current database and "
                "schema."
            )
        return _Target(
            _normalize_identifier(database),
            _normalize_identifier(schema),
            suffix.upper(),
            self.name,
        )

    def _resolve(self, session, target: _Target) -> None:
        for tool in self.tools:
            if not tool._is_resolved():
                tool._resolve(session, target)

    def _tagged_objects(
        self, session, target: _Target, tolerate_errors: bool = False
    ) -> Tuple[Dict[str, list], List[LintIssue], List[str]]:
        """``{"PROCEDURE DB.SCH.NAME(TYPES)": grants}`` (or ``"FUNCTION ..."``) for
        every object whose comment says this server and suffix created it and that
        the session's role owns, with the rows of ``SHOW GRANTS`` on it; a
        warning for each tagged object owned by another role, whose comment may be
        copied; and the signatures, in the same form, of every procedure and UDF in
        the schema. With ``tolerate_errors``, a schema that cannot be read, as before
        a first deploy, has none."""
        role = session.get_current_role()
        role = _normalize_identifier(role) if role else None
        found: Dict[str, list] = {}
        warnings: List[LintIssue] = []
        listed: List[str] = []
        for keyword, show in (
            ("PROCEDURE", "PROCEDURES"),
            ("FUNCTION", "USER FUNCTIONS"),
        ):
            try:
                rows = session.sql(
                    f"SHOW {show} IN SCHEMA {target.schema_fqn}"
                ).collect()
            except Exception:
                if not tolerate_errors:
                    raise
                return {}, [], []
            for row in rows:
                values = _row_dict(row)
                tag = _utils.parse_object_comment(values.get("description"))
                ours = (
                    tag
                    and tag.get("server") == target.server_name
                    and tag.get("suffix", "") == target.suffix
                )
                try:
                    name, types_, _ = _utils.parse_arguments_column(
                        str(values.get("arguments", ""))
                    )
                except ValueError:
                    if ours:
                        raise
                    continue
                signature = (
                    f"{target.object_fqn(_sql_param_name(name))}({', '.join(types_)})"
                )
                listed.append(f"{keyword} {signature}")
                if not ours:
                    continue
                try:
                    grant_rows = session.sql(
                        f"SHOW GRANTS ON {keyword} {signature}"
                    ).collect()
                except Exception:
                    if not tolerate_errors:
                        raise
                    grant_rows = []
                owner = next(
                    (
                        str(_row_dict(r).get("grantee_name"))
                        for r in grant_rows
                        if str(_row_dict(r).get("privilege", "")).upper() == "OWNERSHIP"
                    ),
                    None,
                )
                if role is not None and _is_role(owner, role):
                    found[f"{keyword} {signature}"] = grant_rows
                else:
                    warnings.append(
                        LintIssue(
                            "warning",
                            None,
                            f"{signature} has this server's tag but is owned by "
                            f"{owner or 'an unknown role'}, not {role}; it was left alone.",
                        )
                    )
        return found, warnings, listed

    def _send_telemetry(self, session) -> None:
        try:
            counts: Dict[str, int] = {}
            for tool in self.tools:
                key = (
                    f"python_{tool.kind}"
                    if isinstance(tool, PythonTool)
                    else f"existing_{tool.kind}"
                    if isinstance(tool, ExistingObjectTool)
                    else tool.tool_type
                )
                counts[key] = counts.get(key, 0) + 1
            session._conn._telemetry_client.send_function_usage_telemetry(
                "MCPServer.deploy",
                "usage",
                api_calls=[json.dumps({"tools": counts})],
            )
        except Exception:  # telemetry must never fail a deploy
            _logger.debug("Failed to send MCP deploy telemetry", exc_info=True)


def _endpoint_url(session, target: _Target) -> str:
    host = session.connection.host.replace("_", "-")

    def part(name: str) -> str:
        # Only a plain upper-case identifier is addressed unquoted.
        if not (_utils.is_identifier(name) and name == name.upper()):
            name = name if _is_quoted_identifier(name) else '"' + name + '"'
        return url_quote(name, safe="")

    database, schema, name = (
        part(p)
        for p in (target.database, target.schema, target.server_name + target.suffix)
    )
    return f"https://{host}/api/v2/databases/{database}/schemas/{schema}/mcp-servers/{name}"


def _server_grants(
    session, target: _Target, tolerate_errors: bool = False
) -> List[str]:
    """``GRANT`` statements that re-apply the grants on the server other than
    OWNERSHIP, if it exists. With ``tolerate_errors``, a server that cannot be
    read, as before a first deploy, has none."""
    try:
        return _read_server_grants(session, target)
    except Exception:
        if not tolerate_errors:
            raise
        return []


def _read_server_grants(session, target: _Target) -> List[str]:
    server_name = f"{target.server_name}{target.suffix}".upper()
    shown = session.sql(
        f"SHOW MCP SERVERS LIKE '{server_name}' IN SCHEMA {target.schema_fqn}"
    ).collect()
    # LIKE treats '_' as a wildcard, so the rows can include other servers.
    if not any(
        str(_row_dict(row).get("name", "")).strip('"').upper() == server_name
        for row in shown
    ):
        return []
    return _grant_statements(
        session, f"MCP SERVER {target.server_fqn}", f"MCP SERVER {target.server_fqn}"
    )


def _is_role(name: Optional[str], role: str) -> bool:
    """Whether the role name ``SHOW GRANTS`` reports is ``role``, as
    ``_normalize_identifier`` returns it. The comparison is exact: ``SHOW GRANTS``
    reports a role as stored, so an unquoted role is reported in upper case, and
    ``app_role`` is a different role from ``APP_ROLE``."""
    if name is None:
        return False
    if _is_quoted_identifier(role):
        return name == role[1:-1].replace('""', '"')
    return name == role


def _grant_statements(session, source: str, destination: str) -> List[str]:
    """``GRANT`` statements that give ``destination`` (``"<kind> <name>"``) the
    grants on ``source``, other than OWNERSHIP."""
    return _statements_from_grants(
        session.sql(f"SHOW GRANTS ON {source}").collect(), destination
    )


def _statements_from_grants(rows: list, destination: str) -> List[str]:
    """``GRANT`` statements that give ``destination`` the grants in the rows of
    ``SHOW GRANTS``, other than OWNERSHIP."""
    statements = []
    for row in rows:
        values = _row_dict(row)
        privilege = str(values.get("privilege", "")).upper()
        if not privilege or privilege == "OWNERSHIP":
            continue
        granted_to = str(values.get("granted_to", "ROLE")).upper()
        grantee = str(values.get("grantee_name"))
        # A database or application role is reported as DB.ROLE.
        parts = (
            _utils.split_qualified_name(grantee)
            if granted_to.endswith("_ROLE")
            else [grantee]
        )
        statement = (
            f"GRANT {privilege} ON {destination} TO {granted_to.replace('_', ' ')} "
            + ".".join(_quote_grantee_part(part, granted_to) for part in parts)
        )
        if str(values.get("grant_option", "")).lower() == "true":
            statement += " WITH GRANT OPTION"
        statements.append(statement)
    return statements


def _quote_grantee_part(part: str, granted_to: str) -> str:
    if granted_to.endswith("_ROLE") and _is_quoted_identifier(part):
        return part
    return _quote_identifier(part)
