#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Tool definitions for ``snowflake.snowpark.mcp``."""

import copy
import dataclasses
import inspect
import json
import math
import re
from types import ModuleType
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union


import snowflake.snowpark
from snowflake.snowpark._internal.utils import private_preview
from snowflake.snowpark.mcp import _runtime, _utils
from snowflake.snowpark.mcp._runtime import (
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_MAX_ROWS,
    PLATFORM_RESULT_LIMIT_BYTES,
)
from snowflake.snowpark.mcp._utils import (
    LintIssue,
    ToolParameter,
    _normalize_identifier,
    _row_dict,
    _sql_type,
)
from snowflake.snowpark.types import DecimalType, DoubleType, VariantType

_PREVIEW_VERSION = "1.58.0"


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


class Tool:
    """A tool of an :class:`MCPServer`. Create tools with :func:`mcp_tool`, from
    procedures and UDFs that already exist, in any language, with
    :meth:`from_procedure` and :meth:`from_function`, or add one of the MCP server's
    built-in tool types with :meth:`builtin`."""

    #: The ``type`` of the tool in the server spec.
    tool_type: str = ""

    def __init__(
        self,
        name: str,
        title: Optional[str],
        description: Optional[str],
        annotations: Optional[Dict[str, bool]],
    ) -> None:
        if not _utils.is_mcp_tool_name(name):
            raise ValueError(
                f"Invalid tool name {name!r}: use 1 to 64 letters, digits, '_' or '-'."
            )
        self.name = name
        self.title = title or _utils.title_from_name(name)
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
        elif len(self.description) > _utils.MAX_DESCRIPTION_CHARS:
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    f"The description is over {_utils.MAX_DESCRIPTION_CHARS} "
                    "characters.",
                )
            )
        return issues

    def _resolve(self, session, target: _Target) -> None:
        """Reads what the tool needs from Snowflake before its spec can be built."""

    def _is_resolved(self) -> bool:
        return True

    def _spec(self, target: _Target) -> Dict[str, Any]:
        raise NotImplementedError

    def _grants(self, target: _Target) -> List[str]:
        """Grant clauses (``<privilege> ON <object>``) a role needs to call the tool."""
        return []

    def _warehouses(self) -> List[str]:
        """Warehouses the MCP server runs the tool on, which a role needs usage on."""
        return []

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
    ) -> "ExistingObjectTool":
        """Makes a tool from an existing stored procedure.

        Its parameter names, types and defaults are read with ``SHOW PROCEDURES``
        and ``DESCRIBE PROCEDURE`` when the server is deployed, and its description
        defaults to the procedure's comment. A name without a database or schema
        resolves against the session's current ones when the server is first
        deployed.

        Args:
            procedure: ``"DB.SCHEMA.NAME(ARG_TYPES)"``, or a
                :class:`~snowflake.snowpark.stored_procedure.StoredProcedure`. The
                argument types can be omitted when the procedure is not overloaded.
                The server refers to the procedure by name only, so with overloads
                the types only choose which signature is read here, and a call may
                resolve to another overload.
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
    ) -> "ExistingObjectTool":
        """Makes a tool from an existing scalar UDF. Takes the same arguments as
        :meth:`from_procedure`."""
        return _existing_object_tool("function", function, locals())

    @staticmethod
    @private_preview(version=_PREVIEW_VERSION)
    def builtin(
        type: str,
        identifier: Optional[str] = None,
        *,
        description: str,
        name: Optional[str] = None,
        title: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        read_only: Optional[bool] = None,
        destructive: Optional[bool] = None,
        idempotent: bool = False,
        open_world: bool = False,
    ) -> "Tool":
        """Makes a tool of one of the MCP server's built-in types, which it runs
        itself: ``CORTEX_AGENT_RUN`` (a Cortex Agent), ``CORTEX_ANALYST_MESSAGE``
        (Cortex Analyst over a semantic view), ``CORTEX_SEARCH_SERVICE_QUERY`` (a
        Cortex Search service), ``SYSTEM_EXECUTE_SQL`` (runs SQL with the client's
        role; put it on a server of its own with a least-privileged role) and
        ``CODING_AGENT`` (runs code in a sandbox). Prefer ``CORTEX_AGENT_RUN`` and
        ``CORTEX_ANALYST_MESSAGE`` over procedures that call them: they avoid the
        extra wait of a procedure call. Other type names are passed to the server as
        they are, so types added later work without a new release; ``GENERIC``
        tools are made with :func:`mcp_tool`, :meth:`from_procedure` and
        :meth:`from_function`.

        Args:
            type: The tool type, in upper case, such as ``"CORTEX_AGENT_RUN"``.
            identifier: The fully qualified object (``DATABASE.SCHEMA.NAME``) for
                the types that use one. Required for ``CORTEX_AGENT_RUN``,
                ``CORTEX_ANALYST_MESSAGE`` and ``CORTEX_SEARCH_SERVICE_QUERY``, and
                not allowed for ``SYSTEM_EXECUTE_SQL`` and ``CODING_AGENT``.
            description: What the tool does; the model reads it to choose the tool.
                Required, and not empty.
            name: The tool name. Defaults to the object name in lowercase, and is
                required when there is no identifier.
            title: A human-readable title.
            config: The type's configuration, written to the server spec as is. The
                platform defines its contents, so they are not checked here beyond
                being JSON values. Common keys: ``read_only``, ``warehouse`` and
                ``query_timeout`` for ``SYSTEM_EXECUTE_SQL``; ``model``,
                ``instructions``, ``tools``, ``tool_resources`` and ``skills`` for
                ``CODING_AGENT``. See ``CREATE MCP SERVER`` in the Snowflake
                documentation for the full list.
            read_only: Whether the tool only reads data. This only sets the
                annotation clients see; it doesn't restrict what the tool does. The
                MCP server accepts annotations only on ``CORTEX_AGENT_RUN`` among the
                built-in types, so ``read_only``, ``destructive``, ``idempotent`` and
                ``open_world`` raise :class:`ValueError` for the other known types.
                ``SYSTEM_EXECUTE_SQL`` is read-only by default; pass
                ``config={"read_only": False}`` to allow writes. When neither ``read_only`` nor
                ``destructive`` is set, the spec has no annotations.
            destructive: Whether it may delete or overwrite data.
            idempotent: Whether repeating a call has no further effect.
            open_world: Whether it reaches systems outside Snowflake.
        """
        if type in _KNOWN_BUILTIN_TYPES - {"CORTEX_AGENT_RUN"} and (
            read_only is not None or destructive is not None or idempotent or open_world
        ):
            raise ValueError(
                f"The MCP server does not accept annotations on {type} tools: "
                "read_only, destructive, idempotent and open_world apply only to "
                "CORTEX_AGENT_RUN among the built-in types."
            )
        return _BuiltinTool(
            type,
            identifier,
            name,
            title,
            description,
            config,
            _optional_annotations(read_only, destructive, idempotent, open_world),
        )

    def _spec_base(self, identifier: Optional[str]) -> Dict[str, Any]:
        spec: Dict[str, Any] = {"name": self.name, "title": self.title}
        spec["type"] = self.tool_type
        if identifier:
            spec["identifier"] = identifier
        spec["description"] = self.description or ""
        return spec


def _deepcopy_sharing_modules(value: Any, memo: dict) -> Any:
    """``copy.deepcopy`` that shares modules, which cannot be copied, such as the
    ones a tool's ``packages`` may list."""
    if isinstance(value, ModuleType):
        return value
    if isinstance(value, (list, tuple)):
        return type(value)(_deepcopy_sharing_modules(v, memo) for v in value)
    return copy.deepcopy(value, memo)


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


def _optional_annotations(
    read_only: Optional[bool],
    destructive: Optional[bool],
    idempotent: bool,
    open_world: bool,
) -> Optional[Dict[str, bool]]:
    for arg, value in (("read_only", read_only), ("destructive", destructive)):
        if value is not None and not isinstance(value, bool):
            raise TypeError(f"{arg} must be True, False or None.")
    if read_only is None and destructive is None:
        return None
    return _annotations(bool(read_only), destructive, idempotent, open_world)


def _generic_config(
    kind: str,
    params: List[ToolParameter],
    warehouse: Optional[str],
    query_timeout: Optional[int],
) -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "type": kind,
        "input_schema": _utils.input_schema(params),
    }
    if warehouse:
        config["warehouse"] = warehouse
    if query_timeout is not None:
        config["query_timeout"] = query_timeout
    return config


class PythonTool(Tool):
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
    ) -> None:
        if not callable(func):
            raise TypeError(f"mcp_tool expects a function, got {type(func).__name__}")
        name = func.__name__ if name is None else name
        if not _utils.is_identifier(name):
            raise ValueError(
                f"Invalid tool name {name!r}: the name of a Python tool is part of its "
                "procedure's name, so it must be a valid unquoted identifier "
                "(letters, digits and '_', not starting with a digit)."
            )
        if object_name is not None and not _utils.is_identifier(object_name):
            raise ValueError(f"Invalid object_name {object_name!r}.")
        if kind not in ("auto", "procedure", "function"):
            raise ValueError("kind must be 'auto', 'procedure' or 'function'.")
        if max_rows < 1:
            raise ValueError("max_rows must be at least 1.")

        doc_description, param_docs = _utils.parse_docstring(inspect.getdoc(func))
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
        # Not functools.update_wrapper, which would copy the function's __dict__.
        self.__name__ = func.__name__
        self.__qualname__ = func.__qualname__
        self.__doc__ = func.__doc__
        self.__module__ = func.__module__
        self.__wrapped__ = func
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

        first = next(iter(inspect.signature(func).parameters.values()), None)
        first_hint = first.annotation if first is not None else None
        if first is not None:
            try:
                first_hint = _utils.resolve_annotation(func, first)
            except Exception:  # reported as a lint error for the parameter
                pass
        takes_session = first is not None and _utils.is_session_annotation(first_hint)
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
        elif (
            kind == "procedure"
            and first.annotation is not inspect.Parameter.empty
            and not takes_session
        ):
            self._kind_problem = (
                f"The first parameter of a procedure-backed tool receives the Session, "
                f"but is annotated {first.annotation!r}; annotate it as "
                "snowflake.snowpark.Session or leave it unannotated."
            )
        elif kind == "function" and first is not None and first.name == "session":
            self._kind_problem = (
                "The first parameter is named 'session' but is not annotated as "
                "Session, so the tool would be a UDF. Annotate it as "
                "snowflake.snowpark.Session to make the tool a procedure."
            )

        self.parameters, self._param_problems = _utils.parameters_from_function(
            func, skip_first=self._takes_session, param_descriptions=param_docs
        )

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __reduce__(self):
        # A tool that calls another tool refers to it as a global, so the tool is
        # pickled with the deployed one. It is pickled as the plain function, so the
        # warehouse does not import this module, which its Snowpark may not have.
        return (_runtime._return_value, (self.func,))

    # __reduce__ would make copies plain functions, so copy as a PythonTool.
    def __copy__(self):
        clone = object.__new__(type(self))
        clone.__dict__.update(self.__dict__)
        return clone

    def __deepcopy__(self, memo):
        clone = object.__new__(type(self))
        memo[id(self)] = clone
        for name, value in self.__dict__.items():
            clone.__dict__[name] = (
                value
                if name in ("func", "__wrapped__")
                else _deepcopy_sharing_modules(value, memo)
            )
        return clone

    @property
    def input_schema(self) -> Dict[str, Any]:
        """The JSON schema of the tool's arguments, as the model sees it."""
        return _utils.input_schema(self.parameters)

    def invoke(self, session=None, /, **arguments) -> Any:
        """Runs the tool locally the way the deployed tool runs: checks
        ``arguments`` against :attr:`input_schema`, calls the function, and converts
        the result to the JSON the tool returns. Like the MCP server, it converts
        arguments to the parameters' SQL types first: ISO strings to ``date``,
        ``datetime`` and ``time``, numbers to ``Decimal`` for ``decimal.Decimal``
        parameters and to ``float`` for ``float`` parameters. A :class:`ToolError`
        becomes an error result; any other exception is raised as is, even when
        ``mask_error_details`` is set, so it can be debugged locally.

        A missing, unknown, wrongly typed or out-of-range argument raises
        :class:`ValueError`. A value outside an ``enum``, or a null for an argument
        that does not allow null, returns the ``INVALID_ARGUMENTS`` error result the
        deployed tool returns, because the MCP server passes such arguments to the
        tool.

        Args:
            session: The session passed to a procedure-backed tool. Not used by
                UDF-backed tools. It is positional-only, so a tool argument can be
                named ``session``: pass it by keyword.
            arguments: The tool-call arguments, by name.
        """
        # Enum values and nulls are not rejected: the deployed tool returns an error
        # result for them, as the MCP server does not validate arguments.
        schema = self.input_schema
        problems = _utils.check_arguments(schema, arguments, strict_values=False)
        if problems:
            raise ValueError(
                f"Invalid arguments for tool {self.name}: " + "; ".join(problems)
            )
        if self._takes_session and session is None:
            raise ValueError(f"Tool {self.name} needs a session; pass it to invoke().")
        # The deployed tool receives an omitted argument's SQL default, converted to
        # the parameter's SQL type.
        defaults = inspect.signature(self.func).parameters
        arguments = dict(arguments)
        for p in self.parameters:
            default = defaults[p.name].default if p.name in defaults else None
            if (
                p.name not in arguments
                and default is not inspect.Parameter.empty
                and isinstance(p.datatype, (DecimalType, DoubleType))
            ):
                arguments[p.name] = _utils.sql_default(p.datatype, default)
        try:
            arguments = {
                p.name: _utils.to_sql_value(p, arguments[p.name])
                for p in self.parameters
                if p.name in arguments
            }
        except ValueError as e:
            raise ValueError(f"Invalid arguments for tool {self.name}: {e}") from None
        invalid = _runtime.invalid_arguments_result(schema, arguments)
        if invalid is not None:
            return invalid
        args = (session,) if self._takes_session else ()
        return _runtime.run_tool(
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
        seen_names: Dict[str, str] = {}
        for p in self.parameters:
            container_issue = _utils.container_parameter_issue(p)
            if container_issue:
                issues.append(LintIssue("error", self.name, container_issue))
            if not _utils.is_identifier(p.name):
                issues.append(
                    LintIssue(
                        "error",
                        self.name,
                        f"Parameter {p.name} is not a plain identifier (letters A-Z, "
                        "digits and '_'), which a procedure or UDF parameter name "
                        "must be; rename it.",
                    )
                )
            other = seen_names.setdefault(p.name.upper(), p.name)
            if other != p.name:
                issues.append(
                    LintIssue(
                        "error",
                        self.name,
                        f"Parameters {other} and {p.name} differ only in case, which "
                        "Snowflake does not distinguish; rename one.",
                    )
                )
            if p.name.upper() in _utils.RESERVED_PARAMETER_NAMES:
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
                and _utils.is_sql_parameter(p)
            ):
                issues.append(
                    LintIssue(
                        "warning",
                        self.name,
                        f"Parameter {p.name} looks like free-form SQL, and the "
                        "tool runs with its owner's rights: the model could run "
                        "any SQL the owner can. Use execute_as='caller', or "
                        "deploy with a dedicated read-only role.",
                    )
                )
        if self.read_only and _utils.name_suggests_write(self.name):
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
        spec = self._spec_base(target.object_fqn(self._object_name(target)))
        spec["config"] = _generic_config(
            self.kind, self.parameters, self.warehouse, self.query_timeout
        )
        spec["annotations"] = dict(self.annotations)
        return spec

    def _grants(self, target: _Target) -> List[str]:
        kind = "PROCEDURE" if self.kind == "procedure" else "FUNCTION"
        return [f"USAGE ON {kind} {self._signature(target)}"]

    def _warehouses(self) -> List[str]:
        return [self.warehouse] if self.warehouse else []

    def _register(self, session, target: _Target, stage_location: str) -> str:
        """Registers the procedure or UDF behind the tool and returns its signature."""
        _pickle_runtime_by_value()
        handler = _runtime.make_handler(
            self.func,
            self.max_rows,
            self.max_result_bytes,
            self.mask_error_details,
            input_schema=self.input_schema,
            takes_session=self._takes_session,
        )
        # Snowpark registers no defaults at all if it cannot render one, such as 0 for
        # a FLOAT parameter.
        types_by_name = {p.name: p.datatype for p in self.parameters}
        handler.__signature__ = handler.__signature__.replace(
            parameters=[
                p.replace(default=_utils.sql_default(types_by_name[p.name], p.default))
                if p.name in types_by_name and p.default is not p.empty
                else p
                for p in handler.__signature__.parameters.values()
            ]
        )
        comment = _utils.object_comment(
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


class ExistingObjectTool(Tool):
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
        parts = _utils.split_qualified_name(identifier)
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
        self._overloaded = False

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
        parts = _utils.split_qualified_name(self.object_identifier)
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
        quoted = obj.startswith('"')
        object_name = obj.strip('"').replace('""', '"') if quoted else obj
        rows = session.sql(
            f"SHOW {show} LIKE {_sql_string(object_name)} IN SCHEMA {db}.{sch}"
        ).collect()
        candidates = []
        for row in rows:
            values = _row_dict(row)
            shown_name = str(values.get("name", ""))
            if (
                shown_name != object_name
                if quoted
                else shown_name.upper() != object_name.upper()
            ):
                continue
            _, types_, optional = _utils.parse_arguments_column(
                str(values.get("arguments", ""))
            )
            unsupported = (
                "a table function"
                if str(values.get("is_table_function", "")).upper() == "Y"
                else "an aggregate function"
                if str(values.get("is_aggregate", "")).upper() == "Y"
                else None
            )
            candidates.append(
                (types_, optional, values.get("description"), unsupported)
            )
        shown_signatures = [f"{obj}({', '.join(c[0])})" for c in candidates]
        overloaded = len(candidates) > 1
        if self.arg_types is not None:
            wanted = [_normalize_sql_type(t) for t in self.arg_types]
            candidates = [
                c
                for c in candidates
                if [_normalize_sql_type(t) for t in c[0]] == wanted
            ]
        if not candidates:
            message = (
                f"{self._keyword.lower().capitalize()} {self.object_identifier} was "
                "not found, or the role cannot see it."
            )
            if shown_signatures and self.arg_types is not None:
                message += (
                    " Argument types are compared with what SHOW reports; found: "
                    + ", ".join(shown_signatures)
                )
            raise ValueError(message)
        scalar = [c for c in candidates if not c[3]]
        if not scalar:
            raise ValueError(
                f"{self.object_identifier} is {candidates[0][3]}; a tool can call "
                "only a scalar function."
            )
        candidates = scalar
        if len(candidates) > 1:
            raise ValueError(
                f"{self.object_identifier} has {len(candidates)} overloads; give the "
                f"argument types, e.g. Tool.from_{self.kind}('{db}.{sch}.{obj}(VARCHAR)')."
            )
        types_, optional, comment, _ = candidates[0]
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
        names_and_types = _utils.parse_signature(signature)
        if len(names_and_types) != len(types_):
            raise ValueError(
                f"Could not match the signature {signature} of {self.object_identifier}."
            )
        params = []
        self._open_schema_params = []
        for (name, describe_type), show_type, is_optional in zip(
            names_and_types, types_, optional
        ):
            schema, precise = _utils.sql_type_to_schema(describe_type or show_type)
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
                    cast_type=describe_type or show_type,
                    sql_name=name,
                )
            )
        self.parameters = params
        self._overloaded = overloaded
        if not self.description and not _utils.is_default_comment(comment):
            self.description = str(comment)

    def _lint(self) -> List[LintIssue]:
        if not self._is_resolved():
            # The description may come from the object's comment.
            return []
        issues = super()._lint()
        if self._overloaded:
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    f"{self._qualified_name} has more than one overload. The server "
                    "refers to it by name only, so a call may resolve to another "
                    "overload than the one read here.",
                )
            )
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
            container_issue = _utils.container_parameter_issue(p)
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
        if self.read_only and _utils.name_suggests_write(self.name):
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
        return _utils.schema_usage_grants(self._qualified_name) + [
            f"USAGE ON {self._keyword} {self._signature()}"
        ]

    def _warehouses(self) -> List[str]:
        return [self.warehouse] if self.warehouse else []


_BUILTIN_TYPE_PATTERN = re.compile(r"[A-Z][A-Z0-9_]*")
_IDENTIFIER_REQUIRED = frozenset(
    {"CORTEX_AGENT_RUN", "CORTEX_ANALYST_MESSAGE", "CORTEX_SEARCH_SERVICE_QUERY"}
)
_IDENTIFIER_REFUSED = frozenset({"SYSTEM_EXECUTE_SQL", "CODING_AGENT"})
_KNOWN_BUILTIN_TYPES = _IDENTIFIER_REQUIRED | _IDENTIFIER_REFUSED
# What a role needs on the identifier of a known type, besides usage on its
# database and schema.
_OBJECT_GRANTS = {
    "CORTEX_AGENT_RUN": "USAGE ON AGENT",
    "CORTEX_ANALYST_MESSAGE": "SELECT ON SEMANTIC VIEW",
    "CORTEX_SEARCH_SERVICE_QUERY": "USAGE ON CORTEX SEARCH SERVICE",
}
_AGENT_ROLE_GRANT = "DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER"


class _BuiltinTool(Tool):
    """A tool of one of the MCP server's built-in types. Create it with
    :meth:`Tool.builtin`."""

    def __init__(
        self,
        type: str,
        identifier: Optional[str],
        name: Optional[str],
        title: Optional[str],
        description: str,
        config: Optional[Dict[str, Any]],
        annotations: Optional[Dict[str, bool]],
    ) -> None:
        if not isinstance(type, str) or not _BUILTIN_TYPE_PATTERN.fullmatch(type):
            raise ValueError(
                f"Invalid tool type {type!r}: use upper-case letters, digits and "
                "'_', starting with a letter, such as 'CORTEX_AGENT_RUN'."
            )
        if type == "GENERIC":
            raise ValueError(
                "GENERIC is not a built-in tool type. Make tools from Python "
                "functions with mcp_tool, and from existing procedures and UDFs with "
                "Tool.from_procedure and Tool.from_function."
            )
        if identifier is None and type in _IDENTIFIER_REQUIRED:
            raise ValueError(f"A {type} tool needs an identifier.")
        if identifier is not None and type in _IDENTIFIER_REFUSED:
            raise ValueError(f"A {type} tool does not take an identifier.")
        if identifier is not None:
            parts = (
                _utils.split_qualified_name(identifier)
                if isinstance(identifier, str)
                else []
            )
            if not 1 <= len(parts) <= 3 or not all(parts):
                raise ValueError(f"Invalid object name {identifier!r}.")
        if name is None:
            if identifier is None:
                raise ValueError(f"A {type} tool without an identifier needs a name.")
            name = parts[-1].strip('"').lower()
        if not isinstance(description, str) or not description.strip():
            raise ValueError(
                f"The description of tool {name!r} must be a non-empty string: the "
                "model chooses tools by their descriptions."
            )
        if config is not None and not isinstance(config, dict):
            raise ValueError(f"config must be a dict, got {config.__class__.__name__}.")
        if config:
            try:
                json.dumps(config)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"config of tool {name!r} must hold only JSON values (str, "
                    f"number, bool, None, list, dict): {e}"
                ) from None
        super().__init__(name, title, description, annotations)
        self.tool_type = type
        self.identifier = identifier
        self.config: Dict[str, Any] = copy.deepcopy(config) if config else {}

    def _lint(self) -> List[LintIssue]:
        issues = super()._lint()
        if self.identifier and len(_utils.split_qualified_name(self.identifier)) != 3:
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    f"{self.identifier} is not fully qualified (DATABASE.SCHEMA.NAME).",
                )
            )
        if (
            self.tool_type == "SYSTEM_EXECUTE_SQL"
            and self.config.get("read_only") is False
        ):
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    "The SQL tool allows statements that change data (config "
                    "read_only is False).",
                )
            )
        if self.tool_type == "CODING_AGENT" and _always_allows(self.config):
            issues.append(
                LintIssue(
                    "warning",
                    self.name,
                    "The coding agent runs commands and changes data without asking "
                    "(permission_policy always_allow); use it only for trusted "
                    "automation.",
                )
            )
        if self.tool_type not in _KNOWN_BUILTIN_TYPES:
            issues.append(
                LintIssue(
                    "info",
                    self.name,
                    f"grant_usage does not know what tool type {self.tool_type} "
                    "needs beyond usage on its database and schema.",
                )
            )
        return issues

    def _warehouses(self) -> List[str]:
        warehouse = self.config.get("warehouse")
        if self.tool_type == "SYSTEM_EXECUTE_SQL" and warehouse:
            return [warehouse]
        return []

    def _spec(self, target: _Target) -> Dict[str, Any]:
        spec = self._spec_base(self.identifier)
        if self.config:
            spec["config"] = copy.deepcopy(self.config)
        if self.annotations:
            spec["annotations"] = dict(self.annotations)
        return spec

    def _grants(self, target: _Target) -> List[str]:
        grants = []
        if self.tool_type in ("CORTEX_AGENT_RUN", "CODING_AGENT"):
            grants.append(_AGENT_ROLE_GRANT)
        if not self.identifier:
            return grants
        grants += _utils.schema_usage_grants(self.identifier)
        if self.tool_type in _OBJECT_GRANTS:
            grants.append(f"{_OBJECT_GRANTS[self.tool_type]} {self.identifier}")
        return grants


def _always_allows(config: Dict[str, Any]) -> bool:
    node: Any = config
    for key in ("tool_resources", "code_toolset_all", "permission_policy"):
        node = node.get(key) if isinstance(node, dict) else None
    return isinstance(node, dict) and node.get("type") == "always_allow"


def _normalize_sql_type(sql_type: str) -> str:
    base = sql_type.strip().upper().split("(")[0].strip()
    aliases = {
        "STRING": "VARCHAR",
        "TEXT": "VARCHAR",
        "CHAR": "VARCHAR",
        "CHARACTER": "VARCHAR",
        "NVARCHAR": "VARCHAR",
        "NCHAR": "VARCHAR",
        "VARBINARY": "BINARY",
        "INT": "NUMBER",
        "INTEGER": "NUMBER",
        "BIGINT": "NUMBER",
        "SMALLINT": "NUMBER",
        "TINYINT": "NUMBER",
        "BYTEINT": "NUMBER",
        "DECIMAL": "NUMBER",
        "NUMERIC": "NUMBER",
        "DOUBLE": "FLOAT",
        "DOUBLE PRECISION": "FLOAT",
        "REAL": "FLOAT",
        "FLOAT4": "FLOAT",
        "FLOAT8": "FLOAT",
        "DATETIME": "TIMESTAMP_NTZ",
        "TIMESTAMP": "TIMESTAMP_NTZ",
    }
    return aliases.get(base, base)


def _sql_param_name(name: str) -> str:
    """A parameter name as it must appear in SQL: quoted unless it is a plain
    upper-case identifier."""
    if _utils.is_identifier(name) and name == name.upper():
        return name
    return _quote_identifier(name)


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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
    elif isinstance(value, float) and not math.isfinite(value):
        literal = "'NaN'" if math.isnan(value) else "'inf'" if value > 0 else "'-inf'"
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
    name, types_ = _utils.split_identifier_and_types(signature)
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
    register(_runtime)
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
    the type hints and the docstring's ``Args:`` section. A parameter is required
    unless it has a default; ``Optional[T]`` without a default is required but
    accepts null. A function whose first parameter is annotated
    :class:`~snowflake.snowpark.Session` becomes a stored procedure; any other
    function becomes a UDF. Nothing is created in Snowflake
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
            ``<SERVER><SUFFIX>__<TOOL>``. It does not include the suffix, so
            deployments of the same server to one schema with different suffixes,
            such as dev and prod, overwrite each other's objects.
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
            :class:`ToolError`. The error says to tell the user that the tool
            failed and gives an error ID. The full error is logged with the same
            ID, and reaches the event table when one is set up. Without it,
            clients receive the traceback, with the file paths and code of the
            tool.

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


def _existing_object_tool(
    kind: str, obj: Any, args: Dict[str, Any]
) -> ExistingObjectTool:
    if isinstance(obj, str):
        identifier, arg_types = _utils.split_identifier_and_types(obj)
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
