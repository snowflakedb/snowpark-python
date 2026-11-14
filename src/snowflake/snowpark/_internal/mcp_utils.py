#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Helpers for ``snowflake.snowpark.mcp``: JSON schemas from type hints, docstring
parsing, SQL signature parsing, spec building, and lint rules.

Everything here is pure: no session, no queries. ``snowflake.snowpark.mcp`` runs the
queries and calls these.
"""

import dataclasses
import datetime
import decimal
import inspect
import json
import re
import types
import typing
from typing import Any, Dict, List, Optional, Tuple

from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
)

# Tool and object names are kept to unquoted Snowflake identifiers, so a Python tool's
# name can be part of its procedure's name without quoting.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
# The MCP spec's rule for tool names.
_MCP_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

MAX_TOOLS_PER_SERVER = 50
MAX_DESCRIPTION_CHARS = 1024
OBJECT_COMMENT_ORIGIN = "snowpark_mcp"

# Words in a tool name that suggest it changes data.
_WRITE_WORDS = (
    "write",
    "insert",
    "update",
    "delete",
    "merge",
    "drop",
    "create",
    "alter",
    "truncate",
    "ddl",
    "dml",
    "record",
    "submit",
    "finalize",
    "log",
    "set",
    "upsert",
)
# Words Snowflake rejects as procedure and UDF parameter names. Snowpark does not
# quote preserved parameter names, so a tool parameter cannot be one of these. Found
# by creating a function with each reserved keyword as its parameter name.
RESERVED_PARAMETER_NAMES = frozenset(
    """ALL ALTER AND ANY AS BETWEEN BY CHECK COLUMN CONNECT CREATE CURRENT DELETE
    DISTINCT DROP ELSE EXISTS FOLLOWING FOR FROM GRANT GROUP HAVING ILIKE IN INCREMENT
    INSERT INTERSECT INTO IS LIKE MINUS NOT NULL OF ON OR ORDER QUALIFY REGEXP REVOKE
    RLIKE ROW ROWS SAMPLE SELECT SET SOME START TABLE TABLESAMPLE THEN TO TRIGGER UNION
    UNIQUE UPDATE VALUES WHENEVER WHERE WITH""".split()
)
# Parameter names that suggest the model passes free-form SQL.
_SQL_PARAMETER_NAMES = ("sql", "query", "statement", "stmt", "sql_text", "sql_query")


class UnsupportedTypeError(TypeError):
    pass


@dataclasses.dataclass
class ToolParameter:
    """One argument of a tool: its JSON schema for the model, and SQL type for the
    procedure or UDF."""

    name: str
    json_schema: Dict[str, Any]
    datatype: Optional[DataType]
    required: bool
    description: Optional[str] = None
    # For procedures and UDFs that already exist: the SQL type as SHOW reports it
    # (no precision or scale), the full type from DESCRIBE, and the name as DESCRIBE
    # reports it, which may be case-sensitive.
    sql_type: Optional[str] = None
    cast_type: Optional[str] = None
    sql_name: Optional[str] = None


@dataclasses.dataclass
class LintIssue:
    """A problem :meth:`MCPServer.lint` found. ``level`` is ``"error"``,
    ``"warning"`` or ``"info"``; errors stop a deploy."""

    level: str
    tool: Optional[str]
    message: str

    def __str__(self) -> str:
        where = f"{self.tool}: " if self.tool else ""
        return f"[{self.level}] {where}{self.message}"


def is_identifier(name: str) -> bool:
    return bool(_IDENTIFIER_RE.match(name))


def is_mcp_tool_name(name: str) -> bool:
    return bool(_MCP_TOOL_NAME_RE.match(name))


def title_from_name(name: str) -> str:
    words = re.split(r"[_\-\s]+", name.strip("_-"))
    text = " ".join(w for w in words if w).lower()
    return text[:1].upper() + text[1:]


# ---------------------------------------------------------------------------
# Docstrings
# ---------------------------------------------------------------------------

_SECTION_RE = re.compile(
    r"^(Args|Arguments|Parameters|Params|Returns|Return|Yields|Raises|Examples|Example|"
    r"Notes|Note|See Also|References|Attributes)\s*:?\s*$"
)


def parse_docstring(doc: Optional[str]) -> Tuple[Optional[str], Dict[str, str]]:
    """Returns the description (the text before the first section) and the
    parameter descriptions from a Google-style ``Args:`` or NumPy-style
    ``Parameters`` section."""
    if not doc:
        return None, {}
    lines = inspect.cleandoc(doc).splitlines()

    description_lines: List[str] = []
    i = 0
    while i < len(lines) and not _SECTION_RE.match(lines[i].strip()):
        description_lines.append(lines[i])
        i += 1
    description = "\n".join(description_lines).strip() or None

    params: Dict[str, str] = {}
    while i < len(lines):
        header = lines[i].strip().rstrip(":")
        i += 1
        numpy_style = i < len(lines) and set(lines[i].strip()) == {"-"}
        if numpy_style:
            i += 1
        body: List[str] = []
        while i < len(lines) and not (
            _SECTION_RE.match(lines[i].strip())
            and (
                lines[i].strip().endswith(":")
                or (i + 1 < len(lines) and set(lines[i + 1].strip()) == {"-"})
            )
        ):
            body.append(lines[i])
            i += 1
        if header in ("Args", "Arguments", "Parameters", "Params"):
            params.update(
                _parse_numpy_params(body) if numpy_style else _parse_google_params(body)
            )
    return description, params


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def _parse_google_params(body: List[str]) -> Dict[str, str]:
    params: Dict[str, List[str]] = {}
    base = min((_indent(line) for line in body if line.strip()), default=0)
    current = None
    for line in body:
        if not line.strip():
            continue
        match = re.match(r"^\s*\*{0,2}(\w+)\s*(\([^)]*\))?\s*:\s*(.*)$", line)
        if _indent(line) == base and match:
            current = match.group(1)
            params[current] = [match.group(3).strip()]
        elif current:
            params[current].append(line.strip())
    return {k: " ".join(p for p in v if p) for k, v in params.items()}


def _parse_numpy_params(body: List[str]) -> Dict[str, str]:
    params: Dict[str, List[str]] = {}
    base = min((_indent(line) for line in body if line.strip()), default=0)
    current = None
    for line in body:
        if not line.strip():
            continue
        if _indent(line) == base:
            current = re.split(r"\s*:\s*", line.strip(), maxsplit=1)[0]
            params[current] = []
        elif current:
            params[current].append(line.strip())
    return {k: " ".join(v) for k, v in params.items()}


# ---------------------------------------------------------------------------
# Python type hints -> JSON schema and SQL type
# ---------------------------------------------------------------------------

_NONE_TYPE = type(None)
_UNION_TYPES = (typing.Union,) + (
    (types.UnionType,) if hasattr(types, "UnionType") else ()
)


def _is_typeddict(tp: Any) -> bool:
    is_typeddict = getattr(typing, "is_typeddict", None)
    if is_typeddict is not None and is_typeddict(tp):
        return True
    return (
        isinstance(tp, type)
        and issubclass(tp, dict)
        and hasattr(tp, "__annotations__")
        and hasattr(tp, "__total__")
    )


def type_hint_to_schema(tp: Any) -> Tuple[Dict[str, Any], DataType, bool]:
    """Maps a parameter's type hint to its JSON schema, its SQL type, and whether it
    may be null. Raises ``UnsupportedTypeError`` for types a tool cannot take."""
    if tp is inspect.Parameter.empty:
        raise UnsupportedTypeError("it has no type hint")

    origin = typing.get_origin(tp)
    args = typing.get_args(tp)

    if origin is typing.Annotated:
        schema, datatype, nullable = type_hint_to_schema(args[0])
        descriptions = [m for m in args[1:] if isinstance(m, str)]
        if descriptions:
            schema = {**schema, "description": " ".join(descriptions)}
        return schema, datatype, nullable

    if origin in _UNION_TYPES:
        non_none = [a for a in args if a is not _NONE_TYPE]
        if len(non_none) == 1 and len(args) == 2:
            schema, datatype, _ = type_hint_to_schema(non_none[0])
            return schema, datatype, True
        raise UnsupportedTypeError(f"{tp} is a union; only Optional[T] is supported")

    if origin is typing.Literal:
        if all(isinstance(a, str) for a in args):
            return {"type": "string", "enum": list(args)}, StringType(), False
        if all(isinstance(a, int) and not isinstance(a, bool) for a in args):
            return {"type": "integer", "enum": list(args)}, LongType(), False
        raise UnsupportedTypeError("Literal values must be all strings or all integers")

    if tp is str:
        return {"type": "string"}, StringType(), False
    if tp is bool:
        return {"type": "boolean"}, BooleanType(), False
    if tp is int:
        return {"type": "integer"}, LongType(), False
    if tp is float:
        return {"type": "number"}, DoubleType(), False
    if tp is decimal.Decimal:
        return {"type": "number"}, DecimalType(38, 18), False
    # datetime is a subclass of date, so it is checked first.
    # Explicitly NTZ: a bare TIMESTAMP is created as whatever TIMESTAMP_TYPE_MAPPING says.
    if tp is datetime.datetime:
        return (
            {"type": "string", "format": "date-time"},
            TimestampType(TimestampTimeZone.NTZ),
            False,
        )
    if tp is datetime.date:
        return {"type": "string", "format": "date"}, DateType(), False
    if tp is datetime.time:
        return {"type": "string", "format": "time"}, TimeType(), False

    if tp in (list, tuple, typing.List, typing.Tuple) or origin in (list, tuple):
        schema: Dict[str, Any] = {"type": "array"}
        item_args = [a for a in args if a is not Ellipsis]
        if len(item_args) == 1:
            schema["items"] = _nested_schema(item_args[0])
        return schema, ArrayType(), False

    if tp in (dict, typing.Dict) or origin is dict:
        schema = {"type": "object"}
        if len(args) == 2:
            schema["additionalProperties"] = _nested_schema(args[1])
        return schema, MapType(), False

    if _is_typeddict(tp):
        hints = typing.get_type_hints(tp, include_extras=True)
        properties = {k: _nested_schema(v) for k, v in hints.items()}
        schema = {"type": "object", "properties": properties}
        required = sorted(getattr(tp, "__required_keys__", hints.keys()))
        if required:
            schema["required"] = list(required)
        return schema, MapType(), False

    name = getattr(tp, "__name__", str(tp))
    hint = ""
    if dataclasses.is_dataclass(tp):
        hint = "; use a TypedDict for structured arguments"
    elif tp in (bytes, bytearray):
        hint = "; pass binary data as a base64 string"
    raise UnsupportedTypeError(f"type {name} is not supported{hint}")


def _nested_schema(tp: Any) -> Dict[str, Any]:
    """The schema of a list item, dict value or TypedDict key: there, unlike for a
    top-level argument, Optional[T] means the value may be null."""
    schema, _, nullable = type_hint_to_schema(tp)
    return _with_null(schema) if nullable else schema


def _with_null(schema: Dict[str, Any]) -> Dict[str, Any]:
    if "type" in schema and isinstance(schema["type"], str):
        return {**schema, "type": [schema["type"], "null"]}
    return schema


def base_json_type(schema: Dict[str, Any]) -> Optional[str]:
    """The JSON type of a schema, ignoring ``"null"`` in a nullable type."""
    expected = schema.get("type")
    if isinstance(expected, list):
        return next((t for t in expected if t != "null"), None)
    return expected


def allows_null(schema: Dict[str, Any]) -> bool:
    return isinstance(schema.get("type"), list) and "null" in schema["type"]


def is_session_annotation(tp: Any) -> bool:
    if isinstance(tp, str):
        return tp.split(".")[-1] == "Session"
    return getattr(tp, "__name__", None) == "Session" and getattr(
        tp, "__module__", ""
    ).startswith("snowflake.snowpark")


def resolve_annotation(func: Any, parameter: inspect.Parameter) -> Any:
    """Resolves the annotation of one parameter, which is a string under ``from
    __future__ import annotations``, in the namespace of ``func``'s module. Unlike
    ``typing.get_type_hints(func)``, it does not need the other parameters' or the
    return annotations to resolve. Raises when this one cannot be resolved."""
    if parameter.annotation is inspect.Parameter.empty:
        return parameter.annotation
    holder = types.SimpleNamespace(
        __annotations__={parameter.name: parameter.annotation},
        __globals__=getattr(inspect.unwrap(func), "__globals__", {}),
    )
    return typing.get_type_hints(holder, include_extras=True)[parameter.name]


def parameters_from_function(
    func: Any, skip_first: bool, param_descriptions: Dict[str, str]
) -> Tuple[List[ToolParameter], List[str]]:
    """Returns the tool parameters of ``func`` and the problems found, one message per
    parameter that cannot be a tool argument."""
    signature = inspect.signature(func)
    params: List[ToolParameter] = []
    problems: List[str] = []
    items = list(signature.parameters.values())[1 if skip_first else 0 :]
    for p in items:
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            problems.append(f"parameter {p.name}: *args and **kwargs are not supported")
            continue
        if p.kind is p.KEYWORD_ONLY:
            problems.append(
                f"parameter {p.name}: keyword-only parameters are not supported"
            )
            continue
        try:
            hint = resolve_annotation(func, p)
        except Exception as e:
            problems.append(
                f"parameter {p.name}: could not resolve the type hint "
                f"{p.annotation!r} ({e})"
            )
            continue
        try:
            schema, datatype, nullable = type_hint_to_schema(hint)
        except UnsupportedTypeError as e:
            problems.append(f"parameter {p.name}: {e}")
            continue
        has_default = p.default is not inspect.Parameter.empty
        if nullable:
            schema = _with_null(schema)
        if has_default and p.default is not None:
            try:
                schema = {**schema, "default": json.loads(json.dumps(p.default))}
            except (TypeError, ValueError):
                problems.append(
                    f"parameter {p.name}: the default value is not JSON-serializable"
                )
        description = schema.get("description") or param_descriptions.get(p.name)
        if description:
            schema = {**schema, "description": description}
        params.append(
            ToolParameter(
                name=p.name,
                json_schema=schema,
                datatype=datatype,
                required=not has_default,
                description=description,
            )
        )
    return params, problems


def to_sql_value(param: ToolParameter, value: Any) -> Any:
    """Converts a JSON argument the way the MCP server does before calling a
    procedure or UDF: to the Python value of the parameter's SQL type."""
    datatype = param.datatype
    if value is None or datatype is None:
        return value
    parser = None
    if isinstance(datatype, DateType):
        parser, kind = datetime.date.fromisoformat, "date"
    elif isinstance(datatype, TimestampType):
        parser, kind = datetime.datetime.fromisoformat, "datetime"
    elif isinstance(datatype, TimeType):
        parser, kind = datetime.time.fromisoformat, "time"
    if parser is not None:
        try:
            return parser(value)
        except ValueError as e:
            raise ValueError(
                f"{param.name} is not a valid ISO {kind} string ({e})"
            ) from None
    if isinstance(datatype, DecimalType):
        return decimal.Decimal(str(value))
    if isinstance(datatype, DoubleType):
        return float(value)
    return value


def input_schema(params: List[ToolParameter]) -> Dict[str, Any]:
    schema: Dict[str, Any] = {
        "type": "object",
        "properties": {p.name: p.json_schema for p in params},
    }
    required = [p.name for p in params if p.required]
    if required:
        schema["required"] = required
    return schema


# ---------------------------------------------------------------------------
# SQL types and signatures of procedures and UDFs that already exist
# ---------------------------------------------------------------------------


def split_top_level(text: str, sep: str = ",") -> List[str]:
    parts, depth, current, quote = [], 0, [], None
    for ch in text:
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ('"', "'"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == sep and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def sql_type_to_schema(sql_type: str) -> Tuple[Dict[str, Any], bool]:
    """Maps a Snowflake type name to a JSON schema. The flag is False when the type
    has no precise JSON equivalent (VARIANT, GEOGRAPHY, ...) and the schema is open."""
    t = sql_type.strip().upper()
    base = re.split(r"[\s(]", t, maxsplit=1)[0]
    if base in ("VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER", "NVARCHAR", "NCHAR"):
        return {"type": "string"}, True
    if base in ("NUMBER", "NUMERIC", "DECIMAL"):
        match = re.search(r"\(\s*\d+\s*,\s*(\d+)\s*\)", t)
        scale = int(match.group(1)) if match else 0
        return ({"type": "integer"} if scale == 0 else {"type": "number"}), True
    if base in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"):
        return {"type": "integer"}, True
    if base in ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL"):
        return {"type": "number"}, True
    if base == "BOOLEAN":
        return {"type": "boolean"}, True
    if base == "DATE":
        return {"type": "string", "format": "date"}, True
    if base.startswith("TIMESTAMP") or base == "DATETIME":
        return {"type": "string", "format": "date-time"}, True
    if base == "TIME":
        return {"type": "string", "format": "time"}, True
    if base == "ARRAY":
        return {"type": "array"}, True
    if base in ("OBJECT", "MAP"):
        return {"type": "object"}, True
    return {}, False


def parse_signature(signature: str) -> List[Tuple[str, str]]:
    """Parses ``DESCRIBE PROCEDURE``'s signature, e.g. ``(TICKER VARCHAR, YEARS
    NUMBER)``, into ``[(name, type), ...]``."""
    text = signature.strip()
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1]
    result = []
    for part in split_top_level(text):
        part = part.strip()
        if part.startswith('"'):
            end = 1
            while end < len(part):
                if part[end] == '"':
                    if part[end + 1 : end + 2] == '"':  # doubled quote
                        end += 2
                        continue
                    break
                end += 1
            name, sql_type = part[1:end].replace('""', '"'), part[end + 1 :]
        else:
            name, _, sql_type = part.partition(" ")
        result.append((name, sql_type.strip()))
    return result


def parse_arguments_column(arguments: str) -> Tuple[str, List[str], List[bool]]:
    """Parses the ``arguments`` column of ``SHOW PROCEDURES`` / ``SHOW USER
    FUNCTIONS``, e.g. ``GET_FINANCIALS(VARCHAR, DEFAULT NUMBER) RETURN VARCHAR``,
    into the name, the argument types, and which arguments have a default."""
    match = re.match(r"^\s*(.*?)\((.*)\)\s*RETURN\s", arguments, re.DOTALL)
    if not match:
        match = re.match(r"^\s*(.*?)\((.*)\)\s*$", arguments, re.DOTALL)
    if not match:
        raise ValueError(f"Cannot parse procedure arguments: {arguments}")
    name, args_text = match.group(1).strip(), match.group(2)
    # Older output marks optional arguments with brackets: NAME(VARCHAR [, NUMBER]).
    bracket = args_text.find("[")
    types_, optional = [], []
    for part in split_top_level(args_text.replace("[", "").replace("]", "")):
        is_default = part.upper().startswith("DEFAULT ")
        types_.append(part[len("DEFAULT ") :].strip() if is_default else part.strip())
        optional.append(is_default)
    if bracket >= 0:
        n_before = len(split_top_level(args_text[:bracket]))
        optional = [opt or i >= n_before for i, opt in enumerate(optional)]
    return name, types_, optional


def split_identifier_and_types(identifier: str) -> Tuple[str, Optional[List[str]]]:
    """Splits ``DB.SCH.NAME(VARCHAR, NUMBER)`` into the name and the argument
    types; the types are None when the identifier has no parentheses."""
    identifier = identifier.strip()
    if not identifier.endswith(")") or "(" not in identifier:
        return identifier, None
    name, _, args = identifier[:-1].partition("(")
    return name.strip(), [a for a in split_top_level(args) if a]


def split_qualified_name(name: str) -> List[str]:
    """Splits ``DB.SCH.NAME`` on dots outside double quotes."""
    return split_top_level(name, sep=".")


def is_default_comment(comment: Optional[str]) -> bool:
    return not comment or comment.strip().lower() in (
        "user-defined procedure",
        "user-defined function",
    )


# ---------------------------------------------------------------------------
# Object comments: how deploy recognizes the objects it created
# ---------------------------------------------------------------------------


def object_comment(server: str, suffix: str, tool: str, version: str) -> str:
    return json.dumps(
        {
            "origin": OBJECT_COMMENT_ORIGIN,
            "v": 1,
            "server": server,
            "suffix": suffix,
            "tool": tool,
            "snowpark_version": version,
        },
        separators=(",", ":"),
    )


def parse_object_comment(comment: Optional[str]) -> Optional[Dict[str, Any]]:
    if not comment or not comment.lstrip().startswith("{"):
        return None
    try:
        data = json.loads(comment)
    except ValueError:
        return None
    if isinstance(data, dict) and data.get("origin") == OBJECT_COMMENT_ORIGIN:
        return data
    return None


# ---------------------------------------------------------------------------
# Lint
# ---------------------------------------------------------------------------


def name_suggests_write(name: str) -> bool:
    words = [w for w in re.split(r"[_\-]+", name.lower()) if w]
    return any(w in _WRITE_WORDS for w in words)


def container_parameter_issue(param: ToolParameter) -> Optional[str]:
    """The MCP server cannot pass a JSON array or object to a GENERIC tool, whatever
    the parameter's SQL type: the call fails before the tool runs."""
    json_type = base_json_type(param.json_schema)
    if json_type in ("array", "object"):
        return (
            f"Parameter {param.name} takes a JSON {json_type}, which "
            "the MCP server cannot pass to a procedure or UDF. Take it as a str "
            "holding JSON text instead, and parse it in the tool."
        )
    return None


def is_sql_parameter(param: ToolParameter) -> bool:
    return (
        base_json_type(param.json_schema) == "string"
        and "enum" not in param.json_schema
        and param.name.lower() in _SQL_PARAMETER_NAMES
    )


def lint_tool_count_and_names(tool_names: List[str]) -> List[LintIssue]:
    issues = []
    if len(tool_names) > MAX_TOOLS_PER_SERVER:
        issues.append(
            LintIssue(
                "error",
                None,
                f"The server has {len(tool_names)} tools; an MCP server can have at "
                f"most {MAX_TOOLS_PER_SERVER}.",
            )
        )
    seen = set()
    for name in tool_names:
        key = name.lower()
        if key in seen:
            issues.append(LintIssue("error", name, "Two tools have this name."))
        seen.add(key)
    return issues


# ---------------------------------------------------------------------------
# Checking arguments against an input schema
# ---------------------------------------------------------------------------

_JSON_TYPE_CHECKS = {
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
}


def _check_value(value: Any, schema: Dict[str, Any], path: str) -> List[str]:
    if value is None and allows_null(schema):
        return []
    expected = base_json_type(schema)
    if expected and not _JSON_TYPE_CHECKS[expected](value):
        return [f"{path} must be of type {expected}, got {type(value).__name__}"]
    problems = []
    if "enum" in schema and value not in schema["enum"]:
        problems.append(f"{path} must be one of {schema['enum']}")
    if expected == "array" and "items" in schema:
        for i, item in enumerate(value):
            problems += _check_value(item, schema["items"], f"{path}[{i}]")
    if expected == "object":
        for key in schema.get("required", []):
            if key not in value:
                problems.append(f"{path} is missing required key {key!r}")
        properties = schema.get("properties", {})
        extra = schema.get("additionalProperties")
        for key, item in value.items():
            if key in properties:
                problems += _check_value(item, properties[key], f"{path}.{key}")
            elif isinstance(extra, dict):
                problems += _check_value(item, extra, f"{path}.{key}")
    return problems


def check_arguments(schema: Dict[str, Any], arguments: Dict[str, Any]) -> List[str]:
    """Checks tool-call arguments against an input schema built by
    ``input_schema``. Returns one message per problem."""
    properties = schema.get("properties", {})
    problems = [
        f"unknown argument {name!r}" for name in arguments if name not in properties
    ]
    problems += [
        f"missing required argument {name!r}"
        for name in schema.get("required", [])
        if name not in arguments
    ]
    for name, value in arguments.items():
        if name in properties and value is not None:
            problems += _check_value(value, properties[name], name)
        elif (
            name in properties
            and name in schema.get("required", [])
            and not allows_null(properties[name])
        ):
            problems.append(f"{name} must not be null")
    return problems
