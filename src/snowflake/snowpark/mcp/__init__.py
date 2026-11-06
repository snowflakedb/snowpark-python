#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Build tools and servers for the Snowflake-managed MCP server in Python.

Define tools with :func:`mcp_tool` (from a Python function) or with
:meth:`Tool.from_procedure` / :meth:`Tool.from_function` (from an object that already
exists), add ready-made and built-in tools from :mod:`snowflake.snowpark.mcp.tools`,
and put them in an :class:`MCPServer`. ``MCPServer.deploy`` registers the Python
tools as procedures or UDFs and creates the server; ``grant_usage`` grants a role
everything the server's tools need.

Example::

    from snowflake.snowpark import Session
    from snowflake.snowpark.mcp import MCPServer, mcp_tool, tools

    @mcp_tool(read_only=True)
    def get_account(session: Session, account_id: str) -> dict:
        \"\"\"Return one account.

        Args:
            account_id: The account ID.
        \"\"\"
        return session.table("CRM.PUBLIC.ACCOUNTS").filter(...).first().asDict()

    server = MCPServer("SALES_MCP", tools=[get_account, *tools.governed_sql()])
    server.deploy(session, schema="MCP_DEV", suffix="_DEV")
    server.grant_usage(session, role="SALES_MCP_USER", schema="MCP_DEV", suffix="_DEV")
"""

import dataclasses
import inspect
import json
import logging
import typing
from types import ModuleType
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Tuple, Union
from urllib.parse import quote as url_quote

import yaml

import snowflake.snowpark
from snowflake.snowpark._internal import mcp_governed_sql, mcp_tool_runtime, mcp_utils
from snowflake.snowpark._internal.mcp_tool_runtime import (
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_MAX_ROWS,
    PLATFORM_RESULT_LIMIT_BYTES,
    ToolError,
)
from snowflake.snowpark._internal.mcp_utils import LintIssue, ToolParameter
from snowflake.snowpark._internal.utils import private_preview
from snowflake.snowpark.types import DataType, VariantType

__all__ = [
    "DeployResult",
    "LintIssue",
    "MCPLintError",
    "MCPServer",
    "MCPTool",
    "Tool",
    "ToolError",
    "mcp_tool",
]

_logger = logging.getLogger(__name__)

_PREVIEW_VERSION = "1.56.0"
_DEFAULT_STAGE_NAME = "SNOWPARK_MCP_STAGE"


class MCPLintError(ValueError):
    """Raised by :meth:`MCPServer.deploy` when :meth:`MCPServer.lint` finds errors."""

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
    #: Objects dropped because their tool is no longer in the server.
    pruned: List[str]
    #: SQL statements run (or, with ``dry_run=True``, that would run), apart from
    #: the procedure and UDF registrations listed in ``objects``.
    sql: List[str]
    #: Lint warnings and notes.
    issues: List[LintIssue]


@dataclasses.dataclass(frozen=True)
class _Target:
    """Where a server is deployed: its database, schema, and name suffix."""

    database: str
    schema: str
    suffix: str
    server_name: str

    @property
    def schema_fqn(self) -> str:
        return f"{self.database}.{self.schema}"

    @property
    def server_fqn(self) -> str:
        return f"{self.schema_fqn}.{self.server_name}{self.suffix}"

    def object_fqn(self, object_name: str) -> str:
        return f"{self.schema_fqn}.{object_name}"


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


class MCPTool:
    """A tool of an :class:`MCPServer`. Create tools with :func:`mcp_tool`,
    :meth:`Tool.from_procedure`, :meth:`Tool.from_function`, or the functions in
    :mod:`snowflake.snowpark.mcp.tools`."""

    #: The ``type`` of the tool in the server spec.
    tool_type: str = ""

    def __init__(
        self,
        name: str,
        title: Optional[str],
        description: Optional[str],
        annotations: Optional[Dict[str, bool]],
    ) -> None:
        if not mcp_utils.is_mcp_tool_name(name):
            raise ValueError(
                f"Invalid tool name {name!r}: use 1 to 64 letters, digits, '_' or '-'."
            )
        self.name = name
        self.title = title or mcp_utils.title_from_name(name)
        self.description = description
        self.annotations = annotations

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"

    # Overridden by tool kinds that need them.
    def _lint(self) -> List[LintIssue]:
        issues = []
        if not self.description:
            issues.append(
                LintIssue(
                    "error",
                    self.name,
                    "The tool has no description; the model chooses tools by their "
                    "descriptions.",
                )
            )
        elif len(self.description) > mcp_utils.MAX_DESCRIPTION_CHARS:
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    f"The description is over {mcp_utils.MAX_DESCRIPTION_CHARS} "
                    "characters.",
                )
            )
        return issues

    def _resolve(self, session, target: _Target) -> None:
        """Reads what the tool needs from Snowflake before its spec can be built."""

    def _is_resolved(self) -> bool:
        return True

    def _setup_sql(self, target: _Target) -> List[str]:
        return []

    def _spec(self, target: _Target) -> Dict[str, Any]:
        raise NotImplementedError

    def _grants(self, target: _Target) -> List[str]:
        """Grant clauses (``<privilege> ON <object>``) a role needs to call the tool."""
        return []

    def _spec_base(self, identifier: Optional[str]) -> Dict[str, Any]:
        spec: Dict[str, Any] = {"name": self.name, "title": self.title}
        spec["type"] = self.tool_type
        if identifier:
            spec["identifier"] = identifier
        spec["description"] = self.description or ""
        return spec


def _annotations(
    read_only: bool,
    destructive: Optional[bool],
    idempotent: bool,
    open_world: bool,
) -> Dict[str, bool]:
    if not isinstance(read_only, bool):
        raise TypeError(
            "read_only must be True or False. Set it explicitly: the MCP server "
            "otherwise tells clients every custom tool may change data."
        )
    if read_only and destructive:
        raise ValueError("A read-only tool cannot be destructive.")
    return {
        "readOnlyHint": read_only,
        "destructiveHint": (not read_only) if destructive is None else destructive,
        "idempotentHint": idempotent,
        "openWorldHint": open_world,
    }


def _generic_config(
    kind: str,
    params: List[ToolParameter],
    warehouse: Optional[str],
    query_timeout: Optional[int],
) -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "type": kind,
        "input_schema": mcp_utils.input_schema(params),
    }
    if warehouse:
        config["warehouse"] = warehouse
    if query_timeout is not None:
        config["query_timeout"] = query_timeout
    return config


def _sql_type(datatype: DataType) -> str:
    from snowflake.snowpark._internal.type_utils import convert_sp_to_sf_type

    return convert_sp_to_sf_type(datatype)


class PythonTool(MCPTool):
    """A tool backed by a Python function, created with :func:`mcp_tool`.

    Calling it runs the function unchanged. :meth:`invoke` runs it the way the
    deployed tool does.
    """

    tool_type = "GENERIC"

    def __init__(
        self,
        func: Callable,
        *,
        read_only: bool,
        destructive: Optional[bool] = None,
        idempotent: bool = False,
        open_world: Optional[bool] = None,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        kind: Literal["auto", "procedure", "function"] = "auto",
        object_name: Optional[str] = None,
        execute_as: Literal["owner", "caller", "restricted caller"] = "owner",
        packages: Optional[List[Union[str, ModuleType]]] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        warehouse: Optional[str] = None,
        query_timeout: Optional[int] = None,
        max_rows: int = DEFAULT_MAX_ROWS,
        max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
        mask_error_details: bool = True,
        _setup_sql: Optional[Callable[[_Target], List[str]]] = None,
        _ready_made: Optional[str] = None,
    ) -> None:
        if not callable(func):
            raise TypeError(f"mcp_tool expects a function, got {type(func).__name__}")
        name = func.__name__ if name is None else name
        if not mcp_utils.is_identifier(name):
            raise ValueError(
                f"Invalid tool name {name!r}: the name of a Python tool is part of its "
                "procedure's name, so it must be a valid unquoted identifier "
                "(letters, digits and '_', not starting with a digit)."
            )
        if object_name is not None and not mcp_utils.is_identifier(object_name):
            raise ValueError(f"Invalid object_name {object_name!r}.")
        if kind not in ("auto", "procedure", "function"):
            raise ValueError("kind must be 'auto', 'procedure' or 'function'.")
        if max_rows < 1:
            raise ValueError("max_rows must be at least 1.")

        doc_description, param_docs = mcp_utils.parse_docstring(inspect.getdoc(func))
        super().__init__(
            name,
            title,
            description or doc_description,
            _annotations(
                read_only,
                destructive,
                idempotent,
                bool(external_access_integrations)
                if open_world is None
                else open_world,
            ),
        )
        self.func = func
        self.read_only = read_only
        self.object_name = object_name
        self.execute_as = execute_as
        self.packages = packages
        self.imports = imports
        self.external_access_integrations = external_access_integrations
        self.secrets = secrets
        self.warehouse = warehouse
        self.query_timeout = query_timeout
        self.max_rows = max_rows
        self.max_result_bytes = max_result_bytes
        self.mask_error_details = mask_error_details
        self._setup_sql_func = _setup_sql
        self._ready_made = _ready_made

        first = next(iter(inspect.signature(func).parameters.values()), None)
        try:
            hints = typing.get_type_hints(func)
        except Exception:
            hints = {}
        takes_session = first is not None and mcp_utils.is_session_annotation(
            hints.get(first.name, first.annotation)
        )
        if kind == "auto":
            kind = "procedure" if takes_session else "function"
        self.kind = kind
        self._takes_session = kind == "procedure"
        # With an explicit kind="procedure", the first parameter receives the
        # session whether or not it is annotated.
        self._kind_problem = None
        if kind == "procedure" and first is None:
            self._kind_problem = (
                "A procedure-backed tool's first parameter must be the Session."
            )
        elif kind == "function" and first is not None and first.name == "session":
            self._kind_problem = (
                "The first parameter is named 'session' but is not annotated as "
                "Session, so the tool would be a UDF. Annotate it as "
                "snowflake.snowpark.Session to make the tool a procedure."
            )

        self.parameters, self._param_problems = mcp_utils.parameters_from_function(
            func, skip_first=self._takes_session, param_descriptions=param_docs
        )

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    @property
    def input_schema(self) -> Dict[str, Any]:
        """The JSON schema of the tool's arguments, as the model sees it."""
        return mcp_utils.input_schema(self.parameters)

    def invoke(self, session=None, **arguments) -> Any:
        """Runs the tool locally the way the deployed tool runs: checks
        ``arguments`` against :attr:`input_schema`, calls the function, and converts
        the result to the JSON the tool returns. A :class:`ToolError` becomes an
        error result; any other exception is raised as is, even when
        ``mask_error_details`` is set, so it can be debugged locally.

        Args:
            session: The session passed to a procedure-backed tool. Not used by
                UDF-backed tools.
            arguments: The tool-call arguments, by name.
        """
        problems = mcp_utils.check_arguments(self.input_schema, arguments)
        if problems:
            raise ValueError(
                f"Invalid arguments for tool {self.name}: " + "; ".join(problems)
            )
        if self._takes_session and session is None:
            raise ValueError(f"Tool {self.name} needs a session; pass it to invoke().")
        args = (session,) if self._takes_session else ()
        return mcp_tool_runtime.run_tool(
            self.func, args, arguments, self.max_rows, self.max_result_bytes
        )

    def _object_name(self, target: _Target) -> str:
        if self.object_name:
            return self.object_name.upper()
        return f"{target.server_name}{target.suffix}__{self.name}".upper()

    def _signature(self, target: _Target) -> str:
        types_ = ", ".join(_sql_type(p.datatype) for p in self.parameters)
        return f"{target.object_fqn(self._object_name(target))}({types_})"

    def _lint(self) -> List[LintIssue]:
        issues = super()._lint()
        issues += [LintIssue("error", self.name, p) for p in self._param_problems]
        if self._kind_problem:
            issues.append(LintIssue("error", self.name, self._kind_problem))
        if self.max_result_bytes > PLATFORM_RESULT_LIMIT_BYTES:
            issues.append(
                LintIssue(
                    "error",
                    self.name,
                    f"max_result_bytes is above {PLATFORM_RESULT_LIMIT_BYTES}, where "
                    "the MCP server truncates results.",
                )
            )
        if self.kind == "function" and self.execute_as != "owner":
            issues.append(
                LintIssue(
                    "error",
                    self.name,
                    "execute_as only applies to procedure-backed tools; this tool is "
                    "a UDF because its first parameter is not a Session.",
                )
            )
        for p in self.parameters:
            container_issue = mcp_utils.container_parameter_issue(p)
            if container_issue:
                issues.append(LintIssue("error", self.name, container_issue))
            if p.name.upper() in mcp_utils.RESERVED_PARAMETER_NAMES:
                issues.append(
                    LintIssue(
                        "error",
                        self.name,
                        f"Parameter {p.name} is a Snowflake reserved word, which "
                        "cannot be a procedure or UDF parameter name; rename it.",
                    )
                )
            if not p.description:
                issues.append(
                    LintIssue(
                        "warning",
                        self.name,
                        f"Parameter {p.name} has no description. Add it under Args: "
                        "in the docstring, or with Annotated[type, 'description'].",
                    )
                )
            if (
                self.kind == "procedure"
                and self.execute_as == "owner"
                and mcp_utils.is_sql_parameter(p)
            ):
                issues.append(
                    LintIssue(
                        "warning",
                        self.name,
                        f"Parameter {p.name} looks like free-form SQL, and the tool "
                        "runs with its owner's rights: the model could run any SQL "
                        "the owner can. Use execute_as='caller' or "
                        "tools.governed_sql().",
                    )
                )
        if self.read_only and mcp_utils.name_suggests_write(self.name):
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    "The tool is marked read_only=True but its name suggests it "
                    "changes data.",
                )
            )
        return issues

    def _setup_sql(self, target: _Target) -> List[str]:
        return self._setup_sql_func(target) if self._setup_sql_func else []

    def _spec(self, target: _Target) -> Dict[str, Any]:
        spec = self._spec_base(target.object_fqn(self._object_name(target)))
        spec["config"] = _generic_config(
            self.kind, self.parameters, self.warehouse, self.query_timeout
        )
        spec["annotations"] = dict(self.annotations)
        return spec

    def _grants(self, target: _Target) -> List[str]:
        kind = "PROCEDURE" if self.kind == "procedure" else "FUNCTION"
        return [f"USAGE ON {kind} {self._signature(target)}"]

    def _register(self, session, target: _Target, stage_location: str) -> str:
        """Registers the procedure or UDF behind the tool and returns its signature."""
        _pickle_runtime_by_value()
        handler = mcp_tool_runtime.make_handler(
            self.func, self.max_rows, self.max_result_bytes, self.mask_error_details
        )
        comment = mcp_utils.object_comment(
            target.server_name,
            target.suffix,
            self.name,
            snowflake.snowpark.__version__,
        )
        common = dict(
            func=handler,
            return_type=VariantType(),
            input_types=[p.datatype for p in self.parameters],
            name=target.object_fqn(self._object_name(target)),
            is_permanent=True,
            stage_location=stage_location,
            imports=self.imports,
            packages=self.packages,
            replace=True,
            external_access_integrations=self.external_access_integrations,
            secrets=self.secrets,
            comment=comment,
            copy_grants=True,
            source_code_display=False,
            preserve_parameter_names=True,
            _emit_ast=False,
        )
        if self.kind == "procedure":
            session.sproc.register(execute_as=self.execute_as, **common)
        else:
            session.udf.register(**common)
        return self._signature(target)


class ExistingObjectTool(MCPTool):
    """A tool backed by a procedure or UDF that already exists, created with
    :meth:`Tool.from_procedure` or :meth:`Tool.from_function`. Its signature and
    comment are read from Snowflake when the server is deployed."""

    tool_type = "GENERIC"

    def __init__(
        self,
        kind: Literal["procedure", "function"],
        identifier: str,
        arg_types: Optional[List[str]],
        *,
        read_only: bool,
        destructive: Optional[bool],
        idempotent: bool,
        open_world: bool,
        name: Optional[str],
        title: Optional[str],
        description: Optional[str],
        param_descriptions: Optional[Dict[str, str]],
        warehouse: Optional[str],
        query_timeout: Optional[int],
    ) -> None:
        parts = mcp_utils.split_qualified_name(identifier)
        if not 1 <= len(parts) <= 3 or not all(parts):
            raise ValueError(f"Invalid {kind} name {identifier!r}.")
        default_name = parts[-1].strip('"').lower()
        super().__init__(
            name or default_name,
            title,
            description,
            _annotations(read_only, destructive, idempotent, open_world),
        )
        self.kind = kind
        self.read_only = read_only
        self.object_identifier = identifier
        self.arg_types = arg_types
        self.param_descriptions = {
            k.upper(): v for k, v in (param_descriptions or {}).items()
        }
        self.warehouse = warehouse
        self.query_timeout = query_timeout
        self._qualified_name: Optional[str] = identifier if len(parts) == 3 else None
        self.parameters: Optional[List[ToolParameter]] = None
        self._open_schema_params: List[str] = []

    @property
    def _keyword(self) -> str:
        return "PROCEDURE" if self.kind == "procedure" else "FUNCTION"

    def _is_resolved(self) -> bool:
        return self.parameters is not None

    def _signature(self) -> str:
        return (
            f"{self._qualified_name}("
            + ", ".join(p.sql_type or "" for p in self.parameters or [])
            + ")"
        )

    def _resolve(self, session, target: _Target) -> None:
        parts = mcp_utils.split_qualified_name(self.object_identifier)
        if len(parts) < 3:
            database = session.get_current_database()
            schema = session.get_current_schema()
            if len(parts) == 2:
                schema = parts[0]
            if not database or not schema:
                raise ValueError(
                    f"Cannot resolve {self.object_identifier!r}: use a fully "
                    "qualified name or set the session's database and schema."
                )
            parts = [
                _normalize_identifier(database),
                _normalize_identifier(schema),
                parts[-1],
            ]
        db, sch, obj = parts
        show = "PROCEDURES" if self.kind == "procedure" else "USER FUNCTIONS"
        object_name_literal = obj.strip('"').replace("'", "''")
        rows = session.sql(
            f"SHOW {show} LIKE '{object_name_literal}' IN SCHEMA {db}.{sch}"
        ).collect()
        candidates = []
        for row in rows:
            values = _row_dict(row)
            if str(values.get("name", "")).upper() != obj.strip('"').upper():
                continue
            _, types_, optional = mcp_utils.parse_arguments_column(
                str(values.get("arguments", ""))
            )
            candidates.append((types_, optional, values.get("description")))
        if self.arg_types is not None:
            wanted = [_normalize_sql_type(t) for t in self.arg_types]
            candidates = [
                c
                for c in candidates
                if [_normalize_sql_type(t) for t in c[0]] == wanted
            ]
        if not candidates:
            raise ValueError(
                f"{self._keyword.lower().capitalize()} {self.object_identifier} was "
                "not found, or the role cannot see it."
            )
        if len(candidates) > 1:
            raise ValueError(
                f"{self.object_identifier} has {len(candidates)} overloads; give the "
                f"argument types, e.g. Tool.from_{self.kind}('{db}.{sch}.{obj}(VARCHAR)')."
            )
        types_, optional, comment = candidates[0]
        self._qualified_name = f"{db}.{sch}.{obj}"
        describe = session.sql(
            f"DESCRIBE {self._keyword} {self._qualified_name}({', '.join(types_)})"
        ).collect()
        signature = next(
            (
                str(_row_dict(r).get("value"))
                for r in describe
                if str(_row_dict(r).get("property", "")).lower() == "signature"
            ),
            None,
        )
        if signature is None:
            raise ValueError(f"DESCRIBE {self._keyword} returned no signature.")
        names_and_types = mcp_utils.parse_signature(signature)
        if len(names_and_types) != len(types_):
            raise ValueError(
                f"Could not match the signature {signature} of {self.object_identifier}."
            )
        params = []
        self._open_schema_params = []
        for (name, describe_type), show_type, is_optional in zip(
            names_and_types, types_, optional
        ):
            schema, precise = mcp_utils.sql_type_to_schema(describe_type or show_type)
            if not precise:
                self._open_schema_params.append(name)
            description = self.param_descriptions.get(name.upper())
            if description:
                schema = {**schema, "description": description}
            params.append(
                ToolParameter(
                    name=name.lower() if name.isupper() else name,
                    json_schema=schema,
                    datatype=None,
                    required=not is_optional,
                    description=description,
                    sql_type=show_type,
                )
            )
        self.parameters = params
        if not self.description and not mcp_utils.is_default_comment(comment):
            self.description = str(comment)

    def _lint(self) -> List[LintIssue]:
        if not self._is_resolved():
            # The description may come from the object's comment.
            return []
        issues = super()._lint()
        for name in self._open_schema_params:
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    f"Parameter {name} has a type with no JSON equivalent (such as "
                    "VARIANT), so the model is not told what to pass. The MCP "
                    "server passes only numbers to VARIANT parameters; strings, "
                    "arrays and objects fail.",
                )
            )
        for p in self.parameters or []:
            container_issue = mcp_utils.container_parameter_issue(p)
            if container_issue:
                issues.append(LintIssue("error", self.name, container_issue))
            if not p.description:
                issues.append(
                    LintIssue(
                        "warning",
                        self.name,
                        f"Parameter {p.name} has no description; pass it in "
                        "param_descriptions.",
                    )
                )
        if self.read_only and mcp_utils.name_suggests_write(self.name):
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    "The tool is marked read_only=True but its name suggests it "
                    "changes data.",
                )
            )
        return issues

    def _spec(self, target: _Target) -> Dict[str, Any]:
        spec = self._spec_base(self._qualified_name)
        spec["config"] = _generic_config(
            self.kind, self.parameters or [], self.warehouse, self.query_timeout
        )
        spec["annotations"] = dict(self.annotations)
        return spec

    def _grants(self, target: _Target) -> List[str]:
        return [f"USAGE ON {self._keyword} {self._signature()}"]


def _row_dict(row) -> Dict[str, Any]:
    values = row.asDict() if hasattr(row, "asDict") else dict(row)
    return {str(k).strip('"').lower(): v for k, v in values.items()}


def _normalize_sql_type(sql_type: str) -> str:
    base = sql_type.strip().upper().split("(")[0].strip()
    aliases = {
        "STRING": "VARCHAR",
        "TEXT": "VARCHAR",
        "CHAR": "VARCHAR",
        "CHARACTER": "VARCHAR",
        "NVARCHAR": "VARCHAR",
        "INT": "NUMBER",
        "INTEGER": "NUMBER",
        "BIGINT": "NUMBER",
        "SMALLINT": "NUMBER",
        "TINYINT": "NUMBER",
        "BYTEINT": "NUMBER",
        "DECIMAL": "NUMBER",
        "NUMERIC": "NUMBER",
        "DOUBLE": "FLOAT",
        "REAL": "FLOAT",
        "FLOAT4": "FLOAT",
        "FLOAT8": "FLOAT",
        "DATETIME": "TIMESTAMP_NTZ",
        "TIMESTAMP": "TIMESTAMP_NTZ",
    }
    return aliases.get(base, base)


def _sql_argument(value: Any, sql_type: str) -> str:
    """Renders a JSON tool-call argument as a SQL expression of ``sql_type``."""
    if value is None:
        return "NULL"
    base = sql_type.strip().upper().split("(")[0].strip()
    if base in ("VARIANT", "ARRAY", "OBJECT", "MAP") or isinstance(value, (list, dict)):
        literal = "PARSE_JSON(" + _sql_string(json.dumps(value)) + ")"
        return literal if base == "VARIANT" else f"{literal}::{sql_type}"
    if isinstance(value, bool):
        literal = "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        literal = repr(value)
    else:
        literal = _sql_string(str(value))
    return f"{literal}::{sql_type}"


def _sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def _signature_key(signature: str) -> Tuple[str, Tuple[str, ...]]:
    """Compares signatures across type synonyms: STRING and VARCHAR, BIGINT and
    NUMBER."""
    name, types_ = mcp_utils.split_identifier_and_types(signature)
    return name.upper(), tuple(_normalize_sql_type(t) for t in types_ or [])


_RUNTIME_PICKLED_BY_VALUE = False


def _pickle_runtime_by_value() -> None:
    """Makes cloudpickle copy the runtime modules into each tool instead of
    referencing them by name, so tools do not depend on the warehouse's Snowpark
    version."""
    global _RUNTIME_PICKLED_BY_VALUE
    if _RUNTIME_PICKLED_BY_VALUE:
        return
    import cloudpickle

    register = getattr(cloudpickle, "register_pickle_by_value", None)
    if register is None:
        raise RuntimeError(
            "Deploying MCP tools requires cloudpickle 2.0 or later; installed: "
            f"{getattr(cloudpickle, '__version__', 'unknown')}."
        )
    register(mcp_tool_runtime)
    register(mcp_governed_sql)
    _RUNTIME_PICKLED_BY_VALUE = True


@private_preview(version=_PREVIEW_VERSION)
def mcp_tool(
    func: Optional[Callable] = None,
    *,
    read_only: bool,
    destructive: Optional[bool] = None,
    idempotent: bool = False,
    open_world: Optional[bool] = None,
    name: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    kind: Literal["auto", "procedure", "function"] = "auto",
    object_name: Optional[str] = None,
    execute_as: Literal["owner", "caller", "restricted caller"] = "owner",
    packages: Optional[List[Union[str, ModuleType]]] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    warehouse: Optional[str] = None,
    query_timeout: Optional[int] = None,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
    mask_error_details: bool = True,
) -> Union[PythonTool, Callable[[Callable], PythonTool]]:
    """Turns a Python function into an MCP tool. Use it as a decorator.

    The tool's description comes from the docstring, and its argument schema from
    the type hints and the docstring's ``Args:`` section. A function whose first
    parameter is annotated :class:`~snowflake.snowpark.Session` becomes a stored
    procedure; any other function becomes a UDF. Nothing is created in Snowflake
    until the tool's :class:`MCPServer` is deployed.

    The function can return a dict, list, str, number, bool, None, a
    :class:`~snowflake.snowpark.Row`, or a DataFrame (Snowpark or pandas). A
    DataFrame is returned as ``{"columns", "rows", "row_count", "truncated"}`` with
    at most ``max_rows`` rows. Raise :class:`ToolError` to return an error the model
    can act on.

    Args:
        func: The function. Omit it when passing arguments to the decorator.
        read_only: Whether the tool only reads data. Required, because MCP clients
            otherwise treat every custom tool as one that may change data.
        destructive: Whether the tool may delete or overwrite data. Defaults to
            ``not read_only``.
        idempotent: Whether calling the tool twice with the same arguments has no
            further effect.
        open_world: Whether the tool reaches systems outside Snowflake. Defaults to
            True when ``external_access_integrations`` is set.
        name: The tool name. Defaults to the function name.
        title: A human-readable title. Defaults to one derived from the name.
        description: The description the model reads. Defaults to the docstring,
            without its ``Args:`` and other sections.
        kind: ``"procedure"``, ``"function"`` (UDF), or ``"auto"``.
        object_name: Name of the procedure or UDF. Defaults to
            ``<SERVER><SUFFIX>__<TOOL>``.
        execute_as: For procedure-backed tools: run with the owner's or the
            caller's rights.
        packages: Packages for the procedure or UDF.
        imports: Imports for the procedure or UDF.
        external_access_integrations: External access integrations the tool uses.
        secrets: Secrets the tool uses.
        warehouse: Warehouse the MCP server runs the tool on.
        query_timeout: Timeout, in seconds, of the tool call.
        max_rows: Maximum number of rows returned from a DataFrame result.
        max_result_bytes: Maximum size of the result, below the MCP server's 250 KB
            limit.
        mask_error_details: Send MCP clients a generic error, without the
            traceback, when the tool raises anything other than
            :class:`ToolError`. The full error is logged, and reaches the event
            table when one is set up. Without it, clients receive the traceback,
            with the file paths and code of the tool.

    Example::

        @mcp_tool(read_only=True)
        def get_account(session: Session, account_id: str) -> dict:
            \"\"\"Return one account.

            Args:
                account_id: The 18-character account ID.
            \"\"\"
            row = session.table("CRM.PUBLIC.ACCOUNTS").filter(col("ID") == account_id).first()
            if row is None:
                raise ToolError(f"No account {account_id}.", hint="Use find_account first.")
            return row
    """
    options = dict(
        read_only=read_only,
        destructive=destructive,
        idempotent=idempotent,
        open_world=open_world,
        name=name,
        title=title,
        description=description,
        kind=kind,
        object_name=object_name,
        execute_as=execute_as,
        packages=packages,
        imports=imports,
        external_access_integrations=external_access_integrations,
        secrets=secrets,
        warehouse=warehouse,
        query_timeout=query_timeout,
        max_rows=max_rows,
        max_result_bytes=max_result_bytes,
        mask_error_details=mask_error_details,
    )
    if func is None:
        return lambda f: PythonTool(f, **options)
    return PythonTool(func, **options)


class Tool:
    """Creates tools from procedures and UDFs that already exist, in any language."""

    @staticmethod
    @private_preview(version=_PREVIEW_VERSION)
    def from_procedure(
        procedure: Union[str, "snowflake.snowpark.stored_procedure.StoredProcedure"],
        *,
        read_only: bool,
        destructive: Optional[bool] = None,
        idempotent: bool = False,
        open_world: bool = False,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        param_descriptions: Optional[Dict[str, str]] = None,
        warehouse: Optional[str] = None,
        query_timeout: Optional[int] = None,
    ) -> ExistingObjectTool:
        """Makes a tool from an existing stored procedure.

        Its parameter names, types and defaults are read with ``SHOW PROCEDURES``
        and ``DESCRIBE PROCEDURE`` when the server is deployed, and its description
        defaults to the procedure's comment.

        Args:
            procedure: ``"DB.SCHEMA.NAME(ARG_TYPES)"``, or a
                :class:`~snowflake.snowpark.stored_procedure.StoredProcedure`. The
                argument types can be omitted when the procedure is not overloaded.
            read_only: Whether the tool only reads data. Required.
            destructive: Whether it may delete or overwrite data. Defaults to
                ``not read_only``.
            idempotent: Whether repeating a call has no further effect.
            open_world: Whether it reaches systems outside Snowflake.
            name: The tool name. Defaults to the procedure name in lowercase.
            title: A human-readable title.
            description: The description the model reads.
            param_descriptions: Descriptions of the parameters, by name.
            warehouse: Warehouse the MCP server runs the tool on.
            query_timeout: Timeout, in seconds, of the tool call.
        """
        return _existing_object_tool("procedure", procedure, locals())

    @staticmethod
    @private_preview(version=_PREVIEW_VERSION)
    def from_function(
        function: Union[str, "snowflake.snowpark.udf.UserDefinedFunction"],
        *,
        read_only: bool,
        destructive: Optional[bool] = None,
        idempotent: bool = False,
        open_world: bool = False,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        param_descriptions: Optional[Dict[str, str]] = None,
        warehouse: Optional[str] = None,
        query_timeout: Optional[int] = None,
    ) -> ExistingObjectTool:
        """Makes a tool from an existing scalar UDF. Takes the same arguments as
        :meth:`from_procedure`."""
        return _existing_object_tool("function", function, locals())


def _existing_object_tool(
    kind: str, obj: Any, args: Dict[str, Any]
) -> ExistingObjectTool:
    if isinstance(obj, str):
        identifier, arg_types = mcp_utils.split_identifier_and_types(obj)
    elif hasattr(obj, "name") and hasattr(obj, "_input_types"):
        identifier = obj.name
        arg_types = [_sql_type(t) for t in obj._input_types]
    else:
        raise TypeError(
            f"Expected a {kind} name or a registered {kind} object, got "
            f"{type(obj).__name__}."
        )
    options = {k: v for k, v in args.items() if k not in ("procedure", "function")}
    return ExistingObjectTool(kind, identifier, arg_types, **options)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


class MCPServer:
    """A Snowflake-managed MCP server defined in Python.

    The definition is independent of where it is deployed: pass ``database``,
    ``schema`` and ``suffix`` to each method to work with, for example, a dev and a
    prod copy of the same server. ``database`` and ``schema`` default to the
    session's current ones.

    Args:
        name: The server name, an unquoted identifier.
        tools: The server's tools.
    """

    def __init__(self, name: str, tools: Iterable[MCPTool] = ()) -> None:
        if not mcp_utils.is_identifier(name):
            raise ValueError(
                f"Invalid MCP server name {name!r}: use an unquoted identifier."
            )
        self.name = name.upper()
        self.tools: List[MCPTool] = []
        self.add(*tools)

    def __repr__(self) -> str:
        return f"MCPServer(name={self.name!r}, tools={[t.name for t in self.tools]})"

    def add(self, *tools: Union[MCPTool, Iterable[MCPTool]]) -> "MCPServer":
        """Adds tools. Accepts tools and lists of tools (as returned by
        ``tools.governed_sql``). Returns the server."""
        for tool in tools:
            if isinstance(tool, MCPTool):
                self.tools.append(tool)
            elif isinstance(tool, Iterable) and not isinstance(tool, (str, bytes)):
                self.add(*tool)
            else:
                raise TypeError(
                    f"Expected an MCP tool, got {type(tool).__name__}. Create tools "
                    "with mcp_tool, Tool.from_procedure, or snowflake.snowpark.mcp.tools."
                )
        return self

    def tool(self, name: str) -> MCPTool:
        """Returns the tool with this name."""
        for tool in self.tools:
            if tool.name == name:
                return tool
        raise KeyError(f"The server has no tool named {name!r}.")

    # -- definition ---------------------------------------------------------

    def lint(self, session=None) -> List[LintIssue]:
        """Checks the definition and returns the problems found. Errors stop
        :meth:`deploy`. With a session, tools made from existing objects are read
        from Snowflake first, so they are checked too."""
        if session is not None:
            self._resolve(session, self._target(session, None, None, ""))
        issues = mcp_utils.lint_tool_count_and_names([t.name for t in self.tools])
        for tool in self.tools:
            issues += tool._lint()
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

    @private_preview(version=_PREVIEW_VERSION)
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

        Steps: lint (errors stop the deploy); read the tools made from existing
        objects; register the Python tools; create or replace the server; re-apply
        the server's grants; with ``prune=True``, drop the objects of tools no
        longer in the server. The tool objects are created before the server is
        replaced, so the server never refers to a missing object. The deploy is not
        transactional: if replacing the server fails, the old server keeps running
        with the new tool code.

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
            dry_run: Return what would be done without doing it.
        """
        target = self._target(session, database, schema, suffix)
        self._resolve(session, target)
        issues = self.lint()
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
        if python_tools and stage_location is None:
            stage = target.object_fqn(_DEFAULT_STAGE_NAME)
            run(f"CREATE STAGE IF NOT EXISTS {stage}")
            stage_location = f"@{stage}"
        setup = [st for tool in self.tools for st in tool._setup_sql(target)]
        for statement in dict.fromkeys(setup):
            run(statement)
        for tool in python_tools:
            objects.append(
                tool._signature(target)
                if dry_run
                else tool._register(session, target, stage_location)
            )

        grants: List[Tuple[str, str, str]] = []
        if preserve_grants and not dry_run:
            grants = _server_grants(session, target)
        run(
            f"CREATE OR REPLACE MCP SERVER {target.server_fqn} FROM SPECIFICATION "
            f"$$\n{spec_yaml}$$"
        )
        for privilege, grantee_type, grantee in grants:
            run(
                f"GRANT {privilege} ON MCP SERVER {target.server_fqn} TO "
                f"{grantee_type} {grantee}"
            )

        if prune:
            keep = {
                (t._grants(target)[0].split()[2], _signature_key(t._signature(target)))
                for t in python_tools
            }
            for signature in self._tagged_objects(session, target):
                kind, _, sig = signature.partition(" ")
                if (kind, _signature_key(sig)) not in keep:
                    pruned.append(signature)
                    kind, _, sig = signature.partition(" ")
                    run(f"DROP {kind} IF EXISTS {sig}")

        if not dry_run:
            self._send_telemetry(session)
        return DeployResult(
            server=target.server_fqn,
            endpoint_url=_endpoint_url(session, target),
            objects=objects,
            pruned=[s.partition(" ")[2] for s in pruned],
            sql=sql,
            issues=[i for i in issues if i.level != "error"],
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
        procedure, UDF, agent or search service, and select on semantic views.
        Returns the statements, which are not run when ``dry_run`` is True.

        Tools made from agents also need the agent's own resources (search services,
        semantic views, warehouses) granted; those are not covered.
        """
        role = role.strip()
        if role.strip('"').upper() == "PUBLIC":
            raise ValueError(
                "Refusing to grant MCP tools to PUBLIC; grant them to a dedicated role."
            )
        if not (mcp_utils.is_identifier(role) or _is_quoted_identifier(role)):
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
        """
        arguments = arguments or {}
        tool = self.tool(tool_name)
        target = self._target(session, database, schema, suffix)
        if isinstance(tool, PythonTool):
            params = tool.parameters
            identifier = target.object_fqn(tool._object_name(target))
            kind = tool.kind
            sql_types = {p.name: _sql_type(p.datatype) for p in params}
        elif isinstance(tool, ExistingObjectTool):
            tool._resolve(session, target)
            params = tool.parameters or []
            identifier = tool._qualified_name
            kind = tool.kind
            sql_types = {p.name: p.sql_type or "VARIANT" for p in params}
        else:
            raise NotImplementedError(
                f"Tool {tool_name} is a {tool.tool_type} tool; call it through the "
                "MCP endpoint."
            )
        problems = mcp_utils.check_arguments(mcp_utils.input_schema(params), arguments)
        if problems:
            raise ValueError(
                f"Invalid arguments for tool {tool_name}: " + "; ".join(problems)
            )
        args_sql = ", ".join(
            f"{name} => {_sql_argument(value, sql_types[name])}"
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
        UDFs it created. Returns the statements run."""
        target = self._target(session, database, schema, suffix)
        statements = [f"DROP MCP SERVER IF EXISTS {target.server_fqn}"]
        if include_tools:
            for signature in self._tagged_objects(session, target):
                kind, _, sig = signature.partition(" ")
                statements.append(f"DROP {kind} IF EXISTS {sig}")
        for statement in statements:
            session.sql(statement).collect()
        return statements

    # -- helpers ------------------------------------------------------------

    def _target(
        self, session, database: Optional[str], schema: Optional[str], suffix: str
    ) -> _Target:
        if suffix and not mcp_utils.is_identifier("X" + suffix):
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

    def _tagged_objects(self, session, target: _Target) -> List[str]:
        """``"PROCEDURE DB.SCH.NAME(TYPES)"`` / ``"FUNCTION ..."`` for every object
        whose comment says this server and suffix created it."""
        found = []
        for keyword, show in (
            ("PROCEDURE", "PROCEDURES"),
            ("FUNCTION", "USER FUNCTIONS"),
        ):
            rows = session.sql(f"SHOW {show} IN SCHEMA {target.schema_fqn}").collect()
            for row in rows:
                values = _row_dict(row)
                tag = mcp_utils.parse_object_comment(values.get("description"))
                if not tag or tag.get("server") != target.server_name:
                    continue
                if tag.get("suffix", "") != target.suffix:
                    continue
                name, types_, _ = mcp_utils.parse_arguments_column(
                    str(values.get("arguments", ""))
                )
                found.append(
                    f"{keyword} {target.object_fqn(name)}({', '.join(types_)})"
                )
        return found

    def _send_telemetry(self, session) -> None:
        try:
            counts: Dict[str, int] = {}
            ready_made = []
            for tool in self.tools:
                key = (
                    f"python_{tool.kind}"
                    if isinstance(tool, PythonTool)
                    else f"existing_{tool.kind}"
                    if isinstance(tool, ExistingObjectTool)
                    else tool.tool_type
                )
                counts[key] = counts.get(key, 0) + 1
                if getattr(tool, "_ready_made", None):
                    ready_made.append(tool._ready_made)
            session._conn._telemetry_client.send_function_usage_telemetry(
                "MCPServer.deploy",
                "usage",
                api_calls=[json.dumps({"tools": counts, "ready_made": ready_made})],
            )
        except Exception:  # telemetry must never fail a deploy
            _logger.debug("Failed to send MCP deploy telemetry", exc_info=True)


def _is_quoted_identifier(name: str) -> bool:
    return (
        len(name) >= 2
        and name.startswith('"')
        and name.endswith('"')
        and '"' not in name[1:-1].replace('""', "")
    )


def _normalize_identifier(name: str) -> str:
    """Unquotes ``"NAME"`` when it is a plain upper-case identifier, as
    ``Session.get_current_database`` returns it, and upper-cases unquoted names."""
    name = name.strip()
    if _is_quoted_identifier(name):
        inner = name[1:-1]
        if mcp_utils.is_identifier(inner) and inner == inner.upper():
            return inner
        return name
    if not mcp_utils.is_identifier(name):
        raise ValueError(f"Invalid identifier {name!r}.")
    return name.upper()


def _endpoint_url(session, target: _Target) -> str:
    host = session.connection.host.replace("_", "-")
    parts = [
        p.strip('"').replace('""', '"')
        for p in (target.database, target.schema, target.server_name + target.suffix)
    ]
    database, schema, name = (url_quote(p, safe="") for p in parts)
    return f"https://{host}/api/v2/databases/{database}/schemas/{schema}/mcp-servers/{name}"


def _server_grants(session, target: _Target) -> List[Tuple[str, str, str]]:
    """Grants on the server other than OWNERSHIP, if it exists."""
    exists = session.sql(
        f"SHOW MCP SERVERS LIKE '{target.server_name}{target.suffix}' IN SCHEMA "
        f"{target.schema_fqn}"
    ).collect()
    if not exists:
        return []
    grants = []
    for row in session.sql(f"SHOW GRANTS ON MCP SERVER {target.server_fqn}").collect():
        values = _row_dict(row)
        privilege = str(values.get("privilege", "")).upper()
        if not privilege or privilege == "OWNERSHIP":
            continue
        grantee_type = str(values.get("granted_to", "ROLE")).upper().replace("_", " ")
        grants.append((privilege, grantee_type, str(values.get("grantee_name"))))
    return grants
