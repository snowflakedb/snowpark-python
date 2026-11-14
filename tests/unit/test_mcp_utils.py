#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import dataclasses
import datetime
import decimal
import inspect
from typing import Annotated, Dict, List, Literal, Optional, Tuple, TypedDict, Union

import pytest

from snowflake.snowpark._internal import mcp_utils
from snowflake.snowpark._internal.mcp_utils import (
    LintIssue,
    ToolParameter,
    UnsupportedTypeError,
    check_arguments,
    input_schema,
    lint_tool_count_and_names,
    name_suggests_write,
    object_comment,
    parameters_from_function,
    parse_arguments_column,
    parse_docstring,
    parse_object_comment,
    parse_signature,
    split_identifier_and_types,
    sql_type_to_schema,
    to_sql_value,
    type_hint_to_schema,
)
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    TimestampType,
    TimeType,
)

# ---------------------------------------------------------------------------
# type_hint_to_schema
# ---------------------------------------------------------------------------


class Point(TypedDict):
    x: int
    label: str


class PartialPoint(TypedDict, total=False):
    x: int


@dataclasses.dataclass
class DataPoint:
    x: int


@pytest.mark.parametrize(
    "hint, schema, datatype",
    [
        (str, {"type": "string"}, StringType),
        (int, {"type": "integer"}, LongType),
        (float, {"type": "number"}, DoubleType),
        (bool, {"type": "boolean"}, BooleanType),
        (decimal.Decimal, {"type": "number"}, DecimalType),
        (datetime.date, {"type": "string", "format": "date"}, DateType),
        (datetime.datetime, {"type": "string", "format": "date-time"}, TimestampType),
        (datetime.time, {"type": "string", "format": "time"}, TimeType),
        (List[int], {"type": "array", "items": {"type": "integer"}}, ArrayType),
        (list, {"type": "array"}, ArrayType),
        (list[str], {"type": "array", "items": {"type": "string"}}, ArrayType),
        (
            Dict[str, int],
            {"type": "object", "additionalProperties": {"type": "integer"}},
            MapType,
        ),
        (dict, {"type": "object"}, MapType),
        (Literal["a", "b"], {"type": "string", "enum": ["a", "b"]}, StringType),
        (Literal[1, 2], {"type": "integer", "enum": [1, 2]}, LongType),
        (
            Annotated[str, "the name"],
            {"type": "string", "description": "the name"},
            StringType,
        ),
    ],
)
def test_type_hint_to_schema(hint, schema, datatype):
    result_schema, result_type, nullable = type_hint_to_schema(hint)
    assert result_schema == schema
    assert type(result_type) is datatype
    assert nullable is False


def test_type_hint_decimal_precision():
    _, datatype, _ = type_hint_to_schema(decimal.Decimal)
    assert (datatype.precision, datatype.scale) == (38, 18)


def test_type_hint_datetime_is_not_date():
    assert type_hint_to_schema(datetime.datetime)[0]["format"] == "date-time"


def test_type_hint_optional():
    schema, datatype, nullable = type_hint_to_schema(Optional[int])
    assert schema == {"type": "integer"}
    assert isinstance(datatype, LongType)
    assert nullable is True


def test_type_hint_pep604_optional():
    schema, datatype, nullable = type_hint_to_schema(int | None)
    assert schema == {"type": "integer"}
    assert nullable is True
    assert type_hint_to_schema(None | str)[2] is True


def test_type_hint_optional_list():
    schema, _, nullable = type_hint_to_schema(Optional[List[str]])
    assert schema == {"type": "array", "items": {"type": "string"}}
    assert nullable


def test_type_hint_annotated_multiple_descriptions_and_optional():
    schema, _, nullable = type_hint_to_schema(
        Annotated[Optional[int], "first", 5, "second"]
    )
    assert schema == {"type": "integer", "description": "first second"}
    assert nullable


def test_type_hint_nested_list_of_dicts():
    schema, _, _ = type_hint_to_schema(List[Dict[str, str]])
    assert schema == {
        "type": "array",
        "items": {"type": "object", "additionalProperties": {"type": "string"}},
    }


def test_type_hint_typeddict():
    schema, datatype, nullable = type_hint_to_schema(Point)
    assert schema == {
        "type": "object",
        "properties": {"x": {"type": "integer"}, "label": {"type": "string"}},
        "required": ["label", "x"],
    }
    assert isinstance(datatype, MapType)
    assert nullable is False


def test_type_hint_typeddict_not_total_has_no_required():
    schema, _, _ = type_hint_to_schema(PartialPoint)
    assert schema == {"type": "object", "properties": {"x": {"type": "integer"}}}


def test_type_hint_dataclass_suggests_typeddict():
    with pytest.raises(UnsupportedTypeError, match="TypedDict"):
        type_hint_to_schema(DataPoint)


def test_type_hint_bytes_suggests_base64():
    with pytest.raises(UnsupportedTypeError, match="base64"):
        type_hint_to_schema(bytes)


def test_type_hint_empty():
    with pytest.raises(UnsupportedTypeError, match="no type hint"):
        type_hint_to_schema(inspect.Parameter.empty)


@pytest.mark.parametrize(
    "hint",
    [
        Union[int, str],
        int | str,
        Union[int, str, None],
        Literal["a", 1],
        Literal[True, False],
        object,
        set,
        List[bytes],
    ],
)
def test_type_hint_unsupported(hint):
    with pytest.raises(UnsupportedTypeError):
        type_hint_to_schema(hint)


def test_unsupported_type_error_is_type_error():
    assert issubclass(UnsupportedTypeError, TypeError)


def test_type_hint_tuple_is_array():
    assert type_hint_to_schema(Tuple[int, ...])[0] == {
        "type": "array",
        "items": {"type": "integer"},
    }


def test_nested_optional_accepts_null():
    class WithOptional(TypedDict):
        x: Optional[int]

    schema = input_schema(
        [ToolParameter("p", type_hint_to_schema(WithOptional)[0], None, True)]
    )
    assert check_arguments(schema, {"p": {"x": None}}) == []


# ---------------------------------------------------------------------------
# parse_docstring
# ---------------------------------------------------------------------------


def test_parse_docstring_google():
    doc = """Return one account.

    Looks it up by ID, and
    returns nothing when it is missing.

    Args:
        account_id (str): The 18-character
            account ID.
        days: How many days
            back to look.
        *extra: Ignored by the tool.

    Returns:
        dict: The account.

    Raises:
        ValueError: when bad.
    """
    description, params = parse_docstring(doc)
    assert description == (
        "Return one account.\n\nLooks it up by ID, and\nreturns nothing when it is missing."
    )
    assert params == {
        "account_id": "The 18-character account ID.",
        "days": "How many days back to look.",
        "extra": "Ignored by the tool.",
    }
    assert "dict" not in params.values()


def test_parse_docstring_google_returns_not_a_param():
    _, params = parse_docstring(
        "Desc.\n\nArgs:\n    a: A.\n\nReturns:\n    b: not a param"
    )
    assert params == {"a": "A."}


def test_parse_docstring_numpy():
    doc = """Return one account.

    Parameters
    ----------
    account_id : str
        The account ID,
        18 characters.
    days : int, optional
        Days back.

    Returns
    -------
    dict
        The account.
    """
    description, params = parse_docstring(doc)
    assert description == "Return one account."
    assert params == {
        "account_id": "The account ID, 18 characters.",
        "days": "Days back.",
    }


def test_parse_docstring_no_sections():
    description, params = parse_docstring("Just a description.\nOn two lines.")
    assert description == "Just a description.\nOn two lines."
    assert params == {}


@pytest.mark.parametrize("doc", [None, "", "   "])
def test_parse_docstring_empty(doc):
    assert parse_docstring(doc) == (None, {})


def test_parse_docstring_only_args_has_no_description():
    description, params = parse_docstring("Args:\n    a: A.")
    assert description is None
    assert params == {"a": "A."}


def test_parse_docstring_inspect_getdoc_indentation():
    def f():
        """Summary.

        Args:
            a: Described.
        """

    assert parse_docstring(inspect.getdoc(f)) == ("Summary.", {"a": "Described."})


# ---------------------------------------------------------------------------
# parameters_from_function
# ---------------------------------------------------------------------------


def test_parameters_skip_first_for_session():
    def f(session, a: str, b: int = 5):
        pass

    params, problems = parameters_from_function(f, True, {})
    assert problems == []
    assert [p.name for p in params] == ["a", "b"]


def test_parameters_no_skip():
    def f(session: str, a: str):
        pass

    params, _ = parameters_from_function(f, False, {})
    assert [p.name for p in params] == ["session", "a"]


def test_parameters_defaults_and_required():
    def f(
        a: str,
        b: int = 5,
        c: Optional[str] = None,
        d: Optional[int] = 3,
        e: Optional[str] = None,
    ):
        pass

    params, problems = parameters_from_function(f, False, {})
    assert problems == []
    by_name = {p.name: p for p in params}
    assert by_name["a"].required is True
    assert by_name["b"].required is False
    assert by_name["b"].json_schema == {"type": "integer", "default": 5}
    # Optional with a None default: not required, and no "default" key.
    assert by_name["c"].required is False
    assert "default" not in by_name["c"].json_schema
    assert by_name["d"].json_schema == {"type": ["integer", "null"], "default": 3}
    assert by_name["c"].json_schema == {"type": ["string", "null"]}
    schema = input_schema(params)
    assert schema["required"] == ["a"]
    assert list(schema["properties"]) == ["a", "b", "c", "d", "e"]


def test_parameters_optional_without_default_is_required_but_nullable():
    def f(a: Optional[str]):
        pass

    params, _ = parameters_from_function(f, False, {})
    assert params[0].required is True
    assert params[0].json_schema == {"type": ["string", "null"]}
    schema = input_schema(params)
    assert schema["required"] == ["a"]
    assert check_arguments(schema, {"a": None}) == []
    assert check_arguments(schema, {"a": "x"}) == []
    assert check_arguments(schema, {"a": 1}) == ["a must be of type string, got int"]
    assert check_arguments(schema, {}) == ["missing required argument 'a'"]


def test_check_arguments_null_for_required_non_optional_parameter():
    def f(a: str):
        pass

    schema = input_schema(parameters_from_function(f, False, {})[0])
    assert check_arguments(schema, {"a": None}) == ["a must not be null"]


def test_parameters_none_default_without_optional_hint_is_not_required():
    def f(a: str = None):
        pass

    params, _ = parameters_from_function(f, False, {})
    assert params[0].required is False
    assert "default" not in params[0].json_schema


def test_parameters_all_required_schema():
    def f(a: str, b: int):
        pass

    params, _ = parameters_from_function(f, False, {})
    assert input_schema(params) == {
        "type": "object",
        "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
        "required": ["a", "b"],
    }


def test_parameters_no_required_key_when_nothing_required():
    def f(a: str = "x"):
        pass

    params, _ = parameters_from_function(f, False, {})
    assert "required" not in input_schema(params)


def test_parameters_var_args_kwargs_and_keyword_only_are_problems():
    def f(a: str, *args: int, k: int = 1, **kwargs: int):
        pass

    params, problems = parameters_from_function(f, False, {})
    assert [p.name for p in params] == ["a"]
    assert len(problems) == 3
    assert any("args" in p and "*args" in p for p in problems)
    assert any("kwargs" in p for p in problems)
    assert any("k:" in p and "keyword-only" in p for p in problems)


def test_parameters_non_json_default_is_a_problem():
    def f(a: datetime.date = datetime.date(2024, 1, 1)):  # noqa: B008
        pass

    params, problems = parameters_from_function(f, False, {})
    assert len(problems) == 1
    assert "parameter a" in problems[0] and "JSON" in problems[0]
    assert "default" not in params[0].json_schema


def test_parameters_missing_hint_is_a_problem():
    def f(a, b: str):
        pass

    params, problems = parameters_from_function(f, False, {})
    assert [p.name for p in params] == ["b"]
    assert len(problems) == 1 and problems[0].startswith("parameter a:")


def test_parameters_descriptions_from_docstring_and_annotated():
    def f(
        a: str,
        b: Annotated[int, "from annotated"],
        c: Annotated[int, "annotated wins"] = 1,
    ):
        """Desc.

        Args:
            a: from docstring
            c: ignored
        """

    _, docs = parse_docstring(inspect.getdoc(f))
    params, problems = parameters_from_function(f, False, docs)
    assert problems == []
    assert [p.description for p in params] == [
        "from docstring",
        "from annotated",
        "annotated wins",
    ]
    assert params[0].json_schema["description"] == "from docstring"
    assert params[2].json_schema == {
        "type": "integer",
        "description": "annotated wins",
        "default": 1,
    }


def test_parameters_datatypes():
    def f(a: str, b: List[int], c: Optional[float] = None):
        pass

    params, _ = parameters_from_function(f, False, {})
    assert [type(p.datatype) for p in params] == [StringType, ArrayType, DoubleType]


def test_parameters_unresolvable_forward_reference():
    def f(a: "NoSuchType", b: str):  # noqa: F821
        pass

    params, problems = parameters_from_function(f, False, {})
    # The unresolved annotation is a string, which is not a supported type.
    assert [p.name for p in params] == ["b"]
    assert len(problems) == 1


# ---------------------------------------------------------------------------
# sql_type_to_schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql_type, schema, precise",
    [
        ("VARCHAR", {"type": "string"}, True),
        ("VARCHAR(100)", {"type": "string"}, True),
        ("string", {"type": "string"}, True),
        ("TEXT", {"type": "string"}, True),
        ("NUMBER", {"type": "integer"}, True),
        ("NUMBER(38,0)", {"type": "integer"}, True),
        ("NUMBER(38)", {"type": "integer"}, True),
        ("NUMBER(10,2)", {"type": "number"}, True),
        ("NUMBER(10, 2)", {"type": "number"}, True),
        ("DECIMAL(5,1)", {"type": "number"}, True),
        ("NUMERIC(5,0)", {"type": "integer"}, True),
        ("INT", {"type": "integer"}, True),
        ("BIGINT", {"type": "integer"}, True),
        ("FLOAT", {"type": "number"}, True),
        ("DOUBLE", {"type": "number"}, True),
        ("BOOLEAN", {"type": "boolean"}, True),
        ("DATE", {"type": "string", "format": "date"}, True),
        ("TIMESTAMP_NTZ(9)", {"type": "string", "format": "date-time"}, True),
        ("TIMESTAMP_LTZ", {"type": "string", "format": "date-time"}, True),
        ("TIMESTAMP_TZ", {"type": "string", "format": "date-time"}, True),
        ("TIME", {"type": "string", "format": "time"}, True),
        ("TIME(9)", {"type": "string", "format": "time"}, True),
        ("ARRAY", {"type": "array"}, True),
        ("OBJECT", {"type": "object"}, True),
        ("MAP", {"type": "object"}, True),
        ("VARIANT", {}, False),
        ("GEOGRAPHY", {}, False),
        ("BINARY", {}, False),
    ],
)
def test_sql_type_to_schema(sql_type, schema, precise):
    assert sql_type_to_schema(sql_type) == (schema, precise)


# ---------------------------------------------------------------------------
# Signatures
# ---------------------------------------------------------------------------


def test_parse_signature():
    assert parse_signature("(TICKER VARCHAR, YEARS NUMBER)") == [
        ("TICKER", "VARCHAR"),
        ("YEARS", "NUMBER"),
    ]


def test_parse_signature_precision_and_quotes():
    assert parse_signature('("A" NUMBER(38,0), B VARCHAR(10))') == [
        ("A", "NUMBER(38,0)"),
        ("B", "VARCHAR(10)"),
    ]


def test_parse_signature_quoted_name_with_space():
    assert parse_signature('("My Arg" NUMBER)') == [("My Arg", "NUMBER")]


def test_parse_signature_empty_and_without_parens():
    assert parse_signature("()") == []
    assert parse_signature("A VARCHAR") == [("A", "VARCHAR")]


def test_parse_arguments_column_default():
    assert parse_arguments_column("NAME(VARCHAR, DEFAULT NUMBER) RETURN VARCHAR") == (
        "NAME",
        ["VARCHAR", "NUMBER"],
        [False, True],
    )


def test_parse_arguments_column_brackets():
    assert parse_arguments_column("NAME(VARCHAR [, NUMBER]) RETURN VARCHAR") == (
        "NAME",
        ["VARCHAR", "NUMBER"],
        [False, True],
    )


def test_parse_arguments_column_no_arguments():
    assert parse_arguments_column("NAME() RETURN VARIANT") == ("NAME", [], [])


def test_parse_arguments_column_precision_and_table_return():
    assert parse_arguments_column(
        "F(NUMBER(38,0), DEFAULT VARCHAR) RETURN TABLE (A NUMBER, B VARCHAR)"
    ) == ("F", ["NUMBER(38,0)", "VARCHAR"], [False, True])


def test_parse_arguments_column_without_return():
    assert parse_arguments_column("F(VARCHAR)") == ("F", ["VARCHAR"], [False])


def test_parse_arguments_column_invalid():
    with pytest.raises(ValueError, match="Cannot parse"):
        parse_arguments_column("not a signature")


@pytest.mark.parametrize(
    "identifier, expected",
    [
        ("DB.SCH.NAME", ("DB.SCH.NAME", None)),
        ("NAME", ("NAME", None)),
        ("DB.SCH.NAME()", ("DB.SCH.NAME", [])),
        ("DB.SCH.NAME(VARCHAR, NUMBER)", ("DB.SCH.NAME", ["VARCHAR", "NUMBER"])),
        (
            "DB.SCH.NAME(VARCHAR, NUMBER(38,0))",
            ("DB.SCH.NAME", ["VARCHAR", "NUMBER(38,0)"]),
        ),
        ("  NAME( STRING )  ", ("NAME", ["STRING"])),
    ],
)
def test_split_identifier_and_types(identifier, expected):
    assert split_identifier_and_types(identifier) == expected


def test_split_qualified_name_respects_quotes():
    assert mcp_utils.split_qualified_name('DB."a.b".NAME') == ["DB", '"a.b"', "NAME"]


# ---------------------------------------------------------------------------
# Object comments
# ---------------------------------------------------------------------------


def test_object_comment_round_trip():
    comment = object_comment("SRV", "_DEV", "get_x", "1.56.0")
    parsed = parse_object_comment(comment)
    assert parsed == {
        "origin": "snowpark_mcp",
        "v": 1,
        "server": "SRV",
        "suffix": "_DEV",
        "tool": "get_x",
        "snowpark_version": "1.56.0",
    }


@pytest.mark.parametrize(
    "comment",
    [
        None,
        "",
        "user-defined procedure",
        "{not json",
        "[1, 2]",
        '{"origin": "someone_else"}',
        "{}",
        '{"server": "SRV"}',
    ],
)
def test_parse_object_comment_not_ours(comment):
    assert parse_object_comment(comment) is None


# ---------------------------------------------------------------------------
# Lint helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name, expected",
    [
        ("write_report", True),
        ("get_settings", False),
        ("log_feedback", True),
        ("get_logs", False),
        ("Delete-Account", True),
        ("update", True),
        ("get_account", False),
        ("dataset", False),
    ],
)
def test_name_suggests_write(name, expected):
    assert name_suggests_write(name) is expected


def test_lint_tool_count_and_names():
    assert lint_tool_count_and_names(["a", "b"]) == []
    issues = lint_tool_count_and_names(["a", "A", "b"])
    assert [(i.level, i.tool) for i in issues] == [("error", "A")]
    issues = lint_tool_count_and_names([f"t{i}" for i in range(51)])
    assert len(issues) == 1 and issues[0].level == "error" and "51" in issues[0].message
    assert lint_tool_count_and_names([f"t{i}" for i in range(50)]) == []


def test_lint_issue_str():
    assert str(LintIssue("error", "t", "msg")) == "[error] t: msg"
    assert str(LintIssue("warning", None, "msg")) == "[warning] msg"


@pytest.mark.parametrize(
    "name, expected",
    [
        ("get_x", True),
        ("a-b", True),
        ("a" * 64, True),
        ("a" * 65, False),
        ("a b", False),
        ("", False),
    ],
)
def test_is_mcp_tool_name(name, expected):
    assert mcp_utils.is_mcp_tool_name(name) is expected


@pytest.mark.parametrize(
    "name, expected",
    [("get_x", True), ("_x1$", True), ("1x", False), ("a-b", False), ("", False)],
)
def test_is_identifier(name, expected):
    assert mcp_utils.is_identifier(name) is expected


def test_title_from_name():
    assert mcp_utils.title_from_name("get_account-info") == "Get account info"


def test_is_sql_parameter():
    def make(name, schema):
        return ToolParameter(name, schema, None, True)

    assert mcp_utils.is_sql_parameter(make("query", {"type": "string"}))
    assert mcp_utils.is_sql_parameter(make("SQL", {"type": "string"}))
    assert not mcp_utils.is_sql_parameter(make("query", {"type": "integer"}))
    assert not mcp_utils.is_sql_parameter(
        make("query", {"type": "string", "enum": ["a"]})
    )
    assert not mcp_utils.is_sql_parameter(make("name", {"type": "string"}))


def test_is_session_annotation():
    from snowflake.snowpark import Session

    assert mcp_utils.is_session_annotation(Session)
    assert mcp_utils.is_session_annotation("Session")
    assert mcp_utils.is_session_annotation("snowflake.snowpark.Session")
    assert not mcp_utils.is_session_annotation(str)
    assert not mcp_utils.is_session_annotation(inspect.Parameter.empty)

    class Session_:  # noqa: N801
        pass

    Session_.__name__ = "Session"
    assert not mcp_utils.is_session_annotation(Session_)  # not from snowflake.snowpark


# ---------------------------------------------------------------------------
# check_arguments
# ---------------------------------------------------------------------------


def _schema(**props):
    required = [k for k, v in props.items() if v.pop("_required", False)]
    schema = {"type": "object", "properties": props}
    if required:
        schema["required"] = required
    return schema


def test_check_arguments_ok():
    schema = _schema(a={"type": "string", "_required": True}, b={"type": "integer"})
    assert check_arguments(schema, {"a": "x", "b": 1}) == []
    assert check_arguments(schema, {"a": "x"}) == []


def test_check_arguments_unknown():
    schema = _schema(a={"type": "string"})
    problems = check_arguments(schema, {"z": 1})
    assert problems == ["unknown argument 'z'"]


def test_check_arguments_missing_required():
    schema = _schema(a={"type": "string", "_required": True})
    assert check_arguments(schema, {}) == ["missing required argument 'a'"]


def test_check_arguments_wrong_type():
    schema = _schema(a={"type": "string"}, b={"type": "boolean"}, c={"type": "array"})
    problems = check_arguments(schema, {"a": 1, "b": "yes", "c": {}})
    assert len(problems) == 3
    assert "a must be of type string, got int" in problems[0]
    assert "b must be of type boolean" in problems[1]
    assert "c must be of type array" in problems[2]


@pytest.mark.parametrize("value", [True, False, 1.5, "1"])
def test_check_arguments_integer_rejects(value):
    schema = _schema(n={"type": "integer"})
    assert len(check_arguments(schema, {"n": value})) == 1


def test_check_arguments_number_accepts_int_and_float_but_not_bool():
    schema = _schema(n={"type": "number"})
    assert check_arguments(schema, {"n": 1}) == []
    assert check_arguments(schema, {"n": 1.5}) == []
    assert len(check_arguments(schema, {"n": True})) == 1


def test_check_arguments_enum():
    schema = _schema(mode={"type": "string", "enum": ["a", "b"]})
    assert check_arguments(schema, {"mode": "a"}) == []
    problems = check_arguments(schema, {"mode": "c"})
    assert len(problems) == 1 and "one of ['a', 'b']" in problems[0]


def test_check_arguments_integer_enum_rejects_bool():
    schema = _schema(n={"type": "integer", "enum": [0, 1]})
    assert check_arguments(schema, {"n": 1}) == []
    assert len(check_arguments(schema, {"n": True})) == 1


def test_check_arguments_nested_array_items():
    schema = _schema(xs={"type": "array", "items": {"type": "integer"}})
    assert check_arguments(schema, {"xs": [1, 2]}) == []
    problems = check_arguments(schema, {"xs": [1, "b", 3, "d"]})
    assert len(problems) == 2
    assert problems[0].startswith("xs[1] must be of type integer")
    assert problems[1].startswith("xs[3] must be of type integer")


def test_check_arguments_nested_object():
    point = type_hint_to_schema(Point)[0]
    schema = _schema(p=point)
    assert check_arguments(schema, {"p": {"x": 1, "label": "l"}}) == []
    problems = check_arguments(schema, {"p": {"x": "no"}})
    assert any("missing required key 'label'" in p for p in problems)
    assert any(p.startswith("p.x must be of type integer") for p in problems)


def test_check_arguments_additional_properties():
    schema = _schema(m=type_hint_to_schema(Dict[str, int])[0])
    assert check_arguments(schema, {"m": {"a": 1}}) == []
    problems = check_arguments(schema, {"m": {"a": "x"}})
    assert problems and problems[0].startswith("m.a must be of type integer")


def test_check_arguments_null_for_optional_is_ok():
    schema = _schema(a={"type": "string"})
    assert check_arguments(schema, {"a": None}) == []


def test_check_arguments_null_for_required_is_a_problem():
    schema = _schema(a={"type": "string", "_required": True})
    assert check_arguments(schema, {"a": None}) == ["a must not be null"]


def test_check_arguments_schema_with_no_type_accepts_anything():
    schema = _schema(v={})
    assert check_arguments(schema, {"v": {"anything": [1]}}) == []


def test_check_arguments_round_trip_with_parameters_from_function():
    def f(a: str, b: Literal["x", "y"] = "x", c: Optional[List[int]] = None):
        pass

    params, _ = parameters_from_function(f, False, {})
    schema = input_schema(params)
    assert check_arguments(schema, {"a": "s", "b": "y", "c": [1]}) == []
    assert len(check_arguments(schema, {"a": "s", "b": "z"})) == 1
    assert check_arguments(schema, {"a": "s", "c": None}) == []


# ---------------------------------------------------------------------------
# Annotations that cannot be resolved
# ---------------------------------------------------------------------------


def _function_from_source(source, tmp_path):
    import importlib.util

    path = tmp_path / "tool_module.py"
    path.write_text(source)
    spec = importlib.util.spec_from_file_location("tool_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.tool


def test_unresolvable_return_annotation_does_not_break_parameters(tmp_path):
    func = _function_from_source(
        "from __future__ import annotations\n"
        "from typing import TYPE_CHECKING, Optional\n"
        "if TYPE_CHECKING:\n"
        "    import pandas as pd\n"
        "def tool(n: int, region: Optional[str] = None) -> pd.DataFrame:\n"
        "    pass\n",
        tmp_path,
    )
    params, problems = parameters_from_function(func, False, {})
    assert problems == []
    assert [(p.name, p.json_schema["type"]) for p in params] == [
        ("n", "integer"),
        ("region", ["string", "null"]),
    ]


def test_unresolvable_parameter_annotation_is_reported_per_parameter(tmp_path):
    func = _function_from_source(
        "from __future__ import annotations\n"
        "def tool(n: int, other: Missing, name: str) -> int:\n"
        "    pass\n",
        tmp_path,
    )
    params, problems = parameters_from_function(func, False, {})
    assert [p.name for p in params] == ["n", "name"]
    assert len(problems) == 1
    assert problems[0].startswith(
        "parameter other: could not resolve the type hint 'Missing' ("
    )
    assert "Missing" in problems[0]


# ---------------------------------------------------------------------------
# Conversion of arguments to SQL values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "datatype, value, expected",
    [
        (DateType(), "2024-01-02", datetime.date(2024, 1, 2)),
        (
            TimestampType(),
            "2024-01-02T03:04:05",
            datetime.datetime(2024, 1, 2, 3, 4, 5),
        ),
        (TimeType(), "03:04:05", datetime.time(3, 4, 5)),
        (DecimalType(38, 18), 1.5, decimal.Decimal("1.5")),
        (DecimalType(38, 18), 2, decimal.Decimal("2")),
        (DoubleType(), 2, 2.0),
        (LongType(), 2, 2),
        (StringType(), "2024-01-02", "2024-01-02"),
        (DateType(), None, None),
    ],
)
def test_to_sql_value(datatype, value, expected):
    param = ToolParameter("p", {}, datatype, True)
    result = to_sql_value(param, value)
    assert result == expected and type(result) is type(expected)


def test_to_sql_value_invalid_date():
    param = ToolParameter("day", {"format": "date"}, DateType(), True)
    with pytest.raises(ValueError, match="day is not a valid ISO date string"):
        to_sql_value(param, "yesterday")


def test_parse_signature_strips_a_default():
    assert parse_signature("(TIMES NUMBER DEFAULT 1, NAME VARCHAR default 'a b')") == [
        ("TIMES", "NUMBER"),
        ("NAME", "VARCHAR"),
    ]
    assert parse_signature('("my col" NUMBER(10,2) DEFAULT 1.5)') == [
        ("my col", "NUMBER(10,2)")
    ]


def test_to_sql_value_timestamp_with_offset_is_naive():
    param = ToolParameter("ts", {}, TimestampType(), True)
    assert to_sql_value(param, "2024-01-02T03:04:05Z") == datetime.datetime(
        2024, 1, 2, 3, 4, 5
    )
    result = to_sql_value(param, "2024-01-02T03:04:05+02:00")
    assert result == datetime.datetime(2024, 1, 2, 3, 4, 5) and result.tzinfo is None
    time_param = ToolParameter("t", {}, TimeType(), True)
    assert to_sql_value(time_param, "03:04:05Z") == datetime.time(3, 4, 5)


def test_nullable_enum_includes_null():
    def f(
        kind: Optional[Literal["a", "b"]] = None,
        items: List[Optional[Literal[1, 2]]] = (),
    ):
        pass

    params, _ = parameters_from_function(f, False, {})
    assert params[0].json_schema == {
        "type": ["string", "null"],
        "enum": ["a", "b", None],
    }
    assert params[1].json_schema["items"] == {
        "type": ["integer", "null"],
        "enum": [1, 2, None],
    }
    schema = input_schema(params[:1])
    assert check_arguments(schema, {"kind": None}) == []
    assert check_arguments(schema, {"kind": "c"}) != []
