#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import base64
import datetime
import decimal
import inspect
import json
import logging
import re
import subprocess
import sys
import textwrap

import pytest

from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.mcp_tool_runtime import (
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_MAX_ROWS,
    ToolError,
    check_received_arguments,
    error_result,
    make_handler,
    run_tool,
    table_result,
    to_json_value,
    to_tool_result,
)


def _size(value) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


class FakeSchema:
    def __init__(self, names) -> None:
        self.names = names


class FakeDataFrame:
    """Just enough of a Snowpark DataFrame for the runtime to recognize it."""

    def __init__(self, rows, names=None) -> None:
        self._rows = rows
        self.schema = FakeSchema(names or [])
        self.limit_calls = []
        self.collect_calls = 0

    def limit(self, n):
        self.limit_calls.append(n)
        return FakeDataFrame(self._rows[:n], self.schema.names)

    def collect(self):
        self.collect_calls += 1
        return list(self._rows)


# ---------------------------------------------------------------------------
# to_json_value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        (True, True),
        ("text", "text"),
        (7, 7),
        (1.5, 1.5),
        (decimal.Decimal("3"), 3),
        (decimal.Decimal("3.00"), 3),
        (decimal.Decimal("1E+2"), 100),
        (decimal.Decimal("3.25"), 3.25),
        (decimal.Decimal("-0.5"), -0.5),
        (float("nan"), None),
        (float("inf"), None),
        (float("-inf"), None),
        (decimal.Decimal("NaN"), None),
        (decimal.Decimal("Infinity"), None),
        (decimal.Decimal("-Infinity"), None),
        (datetime.date(2024, 5, 6), "2024-05-06"),
        (datetime.datetime(2024, 5, 6, 7, 8, 9), "2024-05-06T07:08:09"),
        (datetime.time(7, 8, 9), "07:08:09"),
        (datetime.timedelta(minutes=2, milliseconds=500), 120.5),
        (b"\x00\xffab", base64.b64encode(b"\x00\xffab").decode()),
        (bytearray(b"hi"), base64.b64encode(b"hi").decode()),
        (memoryview(b"hi"), base64.b64encode(b"hi").decode()),
    ],
)
def test_to_json_value_scalars(value, expected):
    result = to_json_value(value)
    assert result == expected
    assert type(result) is type(expected)


def test_to_json_value_decimal_integral_is_int_and_fractional_is_float():
    assert type(to_json_value(decimal.Decimal("10"))) is int
    assert type(to_json_value(decimal.Decimal("10.5"))) is float


def test_to_json_value_nested_containers():
    value = {
        "a": [1, (2, decimal.Decimal("3.5"))],
        "b": {"c": {datetime.date(2024, 1, 1)}, 5: float("nan")},
        "d": frozenset([decimal.Decimal("1")]),
    }
    result = to_json_value(value)
    assert result == {
        "a": [1, [2, 3.5]],
        "b": {"c": ["2024-01-01"], "5": None},
        "d": [1],
    }
    json.dumps(result)  # must be serializable


def test_to_json_value_row():
    row = Row(A=1, B=decimal.Decimal("2.5"), C=datetime.date(2024, 1, 2))
    assert to_json_value(row) == {"A": 1, "B": 2.5, "C": "2024-01-02"}
    assert to_json_value([row, row]) == [to_json_value(row)] * 2


def test_to_json_value_numpy_scalar():
    np = pytest.importorskip("numpy")
    result = to_json_value(np.int64(4))
    assert result == 4 and type(result) is int


@pytest.mark.parametrize(
    "value", [object(), {1, object()}, [lambda: 1], Exception("x")]
)
def test_to_json_value_unsupported(value):
    with pytest.raises(TypeError, match="cannot be converted to JSON"):
        to_json_value(value)


# ---------------------------------------------------------------------------
# table_result
# ---------------------------------------------------------------------------


def test_table_result_fits():
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    result = table_result(["a", "b"], rows, 10, 10_000)
    assert result == {
        "columns": ["a", "b"],
        "rows": rows,
        "row_count": 2,
        "truncated": False,
    }


def test_table_result_exactly_max_rows_is_not_truncated():
    rows = [{"a": i} for i in range(3)]
    result = table_result(["a"], rows, 3, 10_000)
    assert result["row_count"] == 3
    assert result["truncated"] is False


def test_table_result_max_rows_cut():
    # The caller passes at most max_rows + 1 rows; the extra one proves truncation.
    rows = [{"a": i} for i in range(4)]
    result = table_result(["a"], rows, 3, 10_000)
    assert result["rows"] == rows[:3]
    assert result["row_count"] == 3
    assert result["truncated"] is True


def test_table_result_empty():
    result = table_result(["a"], [], 3, 10_000)
    assert result == {"columns": ["a"], "rows": [], "row_count": 0, "truncated": False}


def test_table_result_converts_values():
    result = table_result(
        ["d", "n"],
        [{"d": datetime.date(2024, 1, 1), "n": decimal.Decimal("1.5")}],
        10,
        10_000,
    )
    assert result["rows"] == [{"d": "2024-01-01", "n": 1.5}]


@pytest.mark.parametrize("limit", [150, 200, 257, 400, 1000])
def test_table_result_max_result_bytes_cut(limit):
    rows = [{"id": i, "name": "naïve-" + "é" * (i % 7)} for i in range(200)]
    result = table_result(["id", "name"], rows, 1000, limit)
    assert result["truncated"] is True
    assert 0 < result["row_count"] < len(rows)
    assert result["rows"] == rows[: result["row_count"]]
    assert _size(result) <= limit
    # No row that would still have fit was dropped. The header is sized for
    # row_count = max_rows, so measure the next row with that count.
    bigger = dict(result)
    bigger["rows"] = rows[: result["row_count"] + 1]
    bigger["row_count"] = 1000
    assert _size(bigger) > limit


def test_table_result_no_truncation_when_it_fits_in_bytes():
    rows = [{"id": i} for i in range(5)]
    result = table_result(["id"], rows, 100, 10_000)
    assert result["truncated"] is False
    assert _size(result) <= 10_000


def test_table_result_row_larger_than_limit_returns_no_rows():
    result = table_result(["a"], [{"a": "x" * 1000}], 10, 200)
    assert result["rows"] == []
    assert result["row_count"] == 0
    assert result["truncated"] is True


@pytest.mark.parametrize("extra", range(0, 14))
def test_table_result_never_exceeds_limit_when_row_count_gains_digits(extra):
    rows = [{"a": 1}] * 20
    base = _size({"columns": ["a"], "rows": [], "row_count": 0, "truncated": True})
    limit = base + 10 * _size(rows[0]) + 9 + extra
    result = table_result(["a"], rows, 1000, limit)
    assert _size(result) <= limit
    assert result["truncated"] is True
    # Adding the next row would go over the limit.
    bigger = dict(result)
    bigger["rows"] = rows[: result["row_count"] + 1]
    bigger["row_count"] = 1000
    assert _size(bigger) > limit


# ---------------------------------------------------------------------------
# to_tool_result
# ---------------------------------------------------------------------------


def test_to_tool_result_snowpark_dataframe():
    rows = [Row(ID=i, NAME=f"n{i}") for i in range(5)]
    df = FakeDataFrame(rows, names=["ID", "NAME"])
    result = to_tool_result(df, max_rows=3, max_result_bytes=10_000)
    assert len(df.limit_calls) == 1 and df.limit_calls[0] == 4  # max_rows + 1
    assert result["columns"] == ["ID", "NAME"]
    assert result["rows"] == [{"ID": i, "NAME": f"n{i}"} for i in range(3)]
    assert result["row_count"] == 3
    assert result["truncated"] is True


def test_to_tool_result_snowpark_dataframe_not_truncated():
    df = FakeDataFrame([Row(ID=1), Row(ID=2)], names=["ID"])
    result = to_tool_result(df, max_rows=3, max_result_bytes=10_000)
    assert result["truncated"] is False
    assert result["row_count"] == 2


def test_to_tool_result_empty_snowpark_dataframe_uses_schema_names():
    df = FakeDataFrame([], names=["ID", "NAME"])
    result = to_tool_result(df, max_rows=3, max_result_bytes=10_000)
    assert result == {
        "columns": ["ID", "NAME"],
        "rows": [],
        "row_count": 0,
        "truncated": False,
    }


def test_to_tool_result_pandas_dataframe():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": ["w", "x", "y", None]})
    result = to_tool_result(df, max_rows=3, max_result_bytes=10_000)
    assert result["columns"] == ["a", "b"]
    assert result["rows"] == [
        {"a": 1, "b": "w"},
        {"a": 2, "b": "x"},
        {"a": 3, "b": "y"},
    ]
    assert result["row_count"] == 3
    assert result["truncated"] is True


def test_to_tool_result_pandas_non_string_columns_and_nan():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({0: [1.0, float("nan")], "t": pd.to_datetime(["2024-01-01"] * 2)})
    result = to_tool_result(df, max_rows=10, max_result_bytes=10_000)
    assert result["columns"] == ["0", "t"]
    assert result["rows"][1]["0"] is None
    json.dumps(result)


def test_to_tool_result_empty_pandas_dataframe():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"a": [], "b": []})
    result = to_tool_result(df, max_rows=3, max_result_bytes=10_000)
    assert result == {
        "columns": ["a", "b"],
        "rows": [],
        "row_count": 0,
        "truncated": False,
    }


def test_to_tool_result_pandas_nat_becomes_null():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"t": pd.to_datetime(["2024-01-01", None])})
    result = to_tool_result(df, max_rows=10, max_result_bytes=10_000)
    assert result["rows"][1] == {"t": None}


def test_to_tool_result_plain_values():
    assert to_tool_result({"a": decimal.Decimal("1.5")}, 10, 1000) == {"a": 1.5}
    assert to_tool_result(None, 10, 1000) is None
    assert to_tool_result("s", 10, 1000) == "s"
    assert to_tool_result(Row(A=1), 10, 1000) == {"A": 1}


def test_to_tool_result_too_large():
    result = to_tool_result({"blob": "x" * 500}, max_rows=10, max_result_bytes=100)
    assert set(result) == {"error"}
    error = result["error"]
    assert error["type"] == "RESULT_TOO_LARGE"
    assert "100" in error["message"]
    assert error["hint"]


def test_to_tool_result_exact_size_is_allowed():
    value = {"k": "x" * 10}
    assert to_tool_result(value, 10, _size(value)) == value
    assert to_tool_result(value, 10, _size(value) - 1)["error"]["type"] == (
        "RESULT_TOO_LARGE"
    )


def test_to_tool_result_unsupported_type_raises():
    with pytest.raises(TypeError):
        to_tool_result(object(), 10, 1000)


# ---------------------------------------------------------------------------
# error_result and run_tool
# ---------------------------------------------------------------------------


def test_error_result():
    assert error_result("T", "m") == {"error": {"type": "T", "message": "m"}}
    assert error_result("T", "m", "h") == {
        "error": {"type": "T", "message": "m", "hint": "h"}
    }


def test_tool_error_attributes():
    err = ToolError("nope", hint="try this")
    assert str(err) == "nope"
    assert err.message == "nope" and err.hint == "try this"
    assert ToolError("x").hint is None


def test_run_tool_returns_converted_result():
    def func(a, b=2):
        return {"sum": decimal.Decimal(a + b)}

    assert run_tool(func, (1,), {"b": 3}) == {"sum": 4}


def test_run_tool_tool_error():
    def func():
        raise ToolError("No such account.", hint="Use find_account first.")

    assert run_tool(func, (), {}) == {
        "error": {
            "type": "ToolError",
            "message": "No such account.",
            "hint": "Use find_account first.",
        }
    }


def test_run_tool_tool_error_without_hint():
    def func():
        raise ToolError("No such account.")

    assert run_tool(func, (), {}) == {
        "error": {"type": "ToolError", "message": "No such account."}
    }


def test_run_tool_recognizes_tool_error_by_marker_not_class():
    # On the warehouse the ToolError class is a pickled copy, not this class.
    class OtherToolError(Exception):
        _snowpark_mcp_tool_error = True

        def __init__(self, message, hint=None) -> None:
            super().__init__(message)
            self.message = message
            self.hint = hint

    def func():
        raise OtherToolError("copy", "h")

    assert run_tool(func, (), {})["error"] == {
        "type": "ToolError",
        "message": "copy",
        "hint": "h",
    }


def test_run_tool_other_exception_is_logged_and_reraised(caplog):
    def broken():
        raise RuntimeError("boom")

    with caplog.at_level(logging.ERROR, logger="snowflake.snowpark.mcp"):
        with pytest.raises(RuntimeError, match="boom"):
            run_tool(broken, (), {})
    records = [r for r in caplog.records if r.name == "snowflake.snowpark.mcp"]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert "broken" in records[0].getMessage()
    assert records[0].exc_info[1].args == ("boom",)
    assert "RuntimeError: boom" in _formatted(records[0])


def test_run_tool_tool_error_is_not_logged(caplog):
    def func():
        raise ToolError("x")

    with caplog.at_level(logging.DEBUG, logger="snowflake.snowpark.mcp"):
        run_tool(func, (), {})
    assert not caplog.records


def test_run_tool_applies_limits():
    df = FakeDataFrame([Row(A=i) for i in range(10)], names=["A"])
    result = run_tool(lambda: df, (), {}, max_rows=2, max_result_bytes=10_000)
    assert result["row_count"] == 2 and result["truncated"] is True
    assert df.limit_calls == [3]


def test_default_limits_are_below_platform_limit():
    assert DEFAULT_MAX_ROWS == 1000
    assert DEFAULT_MAX_RESULT_BYTES < 250_000


# ---------------------------------------------------------------------------
# make_handler
# ---------------------------------------------------------------------------


def _annotated(
    session: "Session", name: str, count: int = 3, *, flag: bool = True
) -> dict:
    """Doc."""
    return {"name": name, "count": decimal.Decimal(count), "flag": flag}


def test_make_handler_keeps_signature_without_annotations():
    handler = make_handler(_annotated)
    original = inspect.getfullargspec(_annotated)
    spec = inspect.getfullargspec(handler)
    assert spec.args == original.args == ["session", "name", "count"]
    assert spec.defaults == original.defaults == (3,)
    assert spec.kwonlyargs == original.kwonlyargs == ["flag"]
    assert spec.kwonlydefaults == original.kwonlydefaults == {"flag": True}
    assert spec.annotations == {}
    assert not handler.__annotations__
    assert handler.__name__ == "_annotated"
    assert handler.__doc__ == "Doc."


def test_make_handler_does_not_touch_original():
    make_handler(_annotated)
    assert _annotated.__annotations__["name"] is str
    assert inspect.signature(_annotated).return_annotation is dict


def test_make_handler_returns_converted_result():
    handler = make_handler(_annotated)
    assert handler(None, "x") == {"name": "x", "count": 3, "flag": True}
    assert handler(None, "x", 4, flag=False) == {
        "name": "x",
        "count": 4,
        "flag": False,
    }


def test_make_handler_applies_limits():
    def func(n):
        return [{"v": "x" * 50} for _ in range(n)]

    handler = make_handler(func, max_rows=2, max_result_bytes=60)
    assert handler(1) == [{"v": "x" * 50}]
    assert handler(3)["error"]["type"] == "RESULT_TOO_LARGE"


def test_make_handler_tool_error():
    def func(x):
        raise ToolError("bad " + x, hint="h")

    assert make_handler(func)("y")["error"]["message"] == "bad y"


# ---------------------------------------------------------------------------
# Pickle by value
# ---------------------------------------------------------------------------

_UNPICKLE_SCRIPT = textwrap.dedent(
    """
    import importlib.abc
    import sys

    class Blocker(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname.startswith("snowflake.snowpark"):
                raise ImportError("blocked import of " + fullname)
            return None

    sys.meta_path.insert(0, Blocker())

    import cloudpickle

    with open(sys.argv[1], "rb") as f:
        handler = cloudpickle.loads(f.read())

    import json

    out = {
        "invalid": handler("zzz"),
        "ok": handler("fine"),
        "tool_error": handler("bad"),
        "snowpark_imported": any(m.startswith("snowflake.snowpark") for m in sys.modules),
    }
    try:
        handler("crash")
    except RuntimeError as e:
        out["crash"] = str(e)
    print("RESULT" + json.dumps(out))
    """
)


def test_pickled_tool_does_not_import_snowpark(tmp_path):
    cloudpickle = pytest.importorskip("cloudpickle")
    from snowflake.snowpark.mcp import _pickle_runtime_by_value

    _pickle_runtime_by_value()

    def tool(x: str) -> dict:
        if x == "bad":
            raise ToolError("bad input", hint="use a good one")
        if x == "crash":
            raise RuntimeError("kaboom")
        return {"value": decimal.Decimal("2.5"), "x": x}

    schema = {
        "type": "object",
        "properties": {"x": {"type": "string", "enum": ["fine", "bad", "crash"]}},
        "required": ["x"],
    }
    handler = make_handler(tool, input_schema=schema)
    payload = tmp_path / "handler.pkl"
    payload.write_bytes(cloudpickle.dumps(handler))

    proc = subprocess.run(
        [sys.executable, "-c", _UNPICKLE_SCRIPT, str(payload)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    line = next(ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT"))
    out = json.loads(line[len("RESULT") :])
    assert out["ok"] == {"value": 2.5, "x": "fine"}
    assert out["tool_error"] == {
        "error": {
            "type": "ToolError",
            "message": "bad input",
            "hint": "use a good one",
        }
    }
    assert out["crash"] == "kaboom"
    assert out["invalid"] == {
        "error": {
            "type": "INVALID_ARGUMENTS",
            "message": "x must be one of ['fine', 'bad', 'crash'], got 'zzz'",
            "hint": "Allowed values for x: fine, bad, crash.",
        }
    }
    assert out["snowpark_imported"] is False


def test_runtime_module_imports_only_stdlib():
    import ast

    from snowflake.snowpark._internal import mcp_tool_runtime

    tree = ast.parse(inspect.getsource(mcp_tool_runtime))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add((node.module or "").split(".")[0])
    assert names <= set(sys.stdlib_module_names), names - set(sys.stdlib_module_names)


# ---------------------------------------------------------------------------
# mask_error_details
# ---------------------------------------------------------------------------


@pytest.fixture
def restore_tracebacklimit():
    had = hasattr(sys, "tracebacklimit")
    old = getattr(sys, "tracebacklimit", None)
    yield
    if had:
        sys.tracebacklimit = old
    elif hasattr(sys, "tracebacklimit"):
        del sys.tracebacklimit


def _formatted(record):
    return logging.Formatter().format(record)


def _secret_failure():
    raise ValueError("secret detail /home/me/tool.py")


def test_run_tool_masked_raises_generic_error(caplog, restore_tracebacklimit):
    with caplog.at_level(logging.ERROR, logger="snowflake.snowpark.mcp"):
        with pytest.raises(RuntimeError) as exc_info:
            run_tool(_secret_failure, (), {}, mask_error_details=True)
    message = str(exc_info.value)
    assert "_secret_failure failed with an unexpected error" in message
    assert "Retrying with the same arguments will not help" in message
    assert "tell the user that the tool failed" in message
    assert "secret detail" not in message
    error_id = re.search(r"error ID ([0-9a-f]{12})\)", message).group(1)
    # The same ID is in the log record.
    assert any(
        r.exc_info and f"(error ID {error_id})" in r.getMessage()
        for r in caplog.records
    )
    # The original error is not chained into what the client sees...
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True
    # ...and the traceback is suppressed when the warehouse formats the error.
    assert sys.tracebacklimit == 0
    # The full error is still logged.
    assert any(
        r.exc_info and "ValueError: secret detail" in _formatted(r)
        for r in caplog.records
    )


def test_run_tool_masked_failures_all_log_a_traceback(caplog, restore_tracebacklimit):
    # The first masked failure sets sys.tracebacklimit to 0 for the process.
    with caplog.at_level(logging.ERROR, logger="snowflake.snowpark.mcp"):
        for _ in range(2):
            with pytest.raises(RuntimeError):
                run_tool(_secret_failure, (), {}, mask_error_details=True)
    assert len(caplog.records) == 2
    assert sys.tracebacklimit == 0
    for record in caplog.records:
        assert record.exc_info is not None
        message = _formatted(record)
        assert "Traceback (most recent call last)" in message
        assert "in _secret_failure" in message
        assert "ValueError: secret detail" in message


def test_run_tool_masked_formats_without_traceback(restore_tracebacklimit):
    import traceback

    try:
        run_tool(_secret_failure, (), {}, mask_error_details=True)
    except RuntimeError:
        formatted = traceback.format_exc()
    assert "Traceback" not in formatted
    assert "secret detail" not in formatted
    assert formatted.startswith("RuntimeError: Tool _secret_failure failed")


def test_run_tool_masked_covers_result_conversion(restore_tracebacklimit):
    with pytest.raises(RuntimeError, match="failed with an unexpected error"):
        run_tool(lambda: object(), (), {}, mask_error_details=True)


def test_run_tool_masked_still_returns_tool_error():
    def f():
        raise ToolError("expected", hint="h")

    assert run_tool(f, (), {}, mask_error_details=True) == error_result(
        "ToolError", "expected", "h"
    )


def test_run_tool_unmasked_reraises_original(restore_tracebacklimit):
    with pytest.raises(ValueError, match="secret detail"):
        run_tool(_secret_failure, (), {})
    assert getattr(sys, "tracebacklimit", None) != 0


def test_make_handler_passes_mask_error_details(restore_tracebacklimit):
    with pytest.raises(RuntimeError, match="unexpected error"):
        make_handler(_secret_failure, mask_error_details=True)()
    with pytest.raises(ValueError):
        make_handler(_secret_failure)()


def test_run_tool_masked_error_ids_differ(restore_tracebacklimit):
    ids = []
    for _ in range(2):
        with pytest.raises(RuntimeError) as exc_info:
            run_tool(_secret_failure, (), {}, mask_error_details=True)
        ids.append(re.search(r"error ID ([0-9a-f]{12})\)", str(exc_info.value))[1])
    assert ids[0] != ids[1]


def test_run_tool_unmasked_logs_error_id(caplog):
    with caplog.at_level(logging.ERROR, logger="snowflake.snowpark.mcp"):
        with pytest.raises(ValueError):
            run_tool(_secret_failure, (), {})
    assert re.search(r"error ID [0-9a-f]{12}\)", caplog.records[0].getMessage())


# ---------------------------------------------------------------------------
# Argument checks
# ---------------------------------------------------------------------------

_ARGS_SCHEMA = {
    "type": "object",
    "properties": {
        "kind": {"type": "string", "enum": ["a", "b"]},
        "level": {"type": "integer", "enum": [1, 2]},
        "day": {"type": "string", "format": "date"},
        "note": {"type": "string"},
    },
    "required": ["kind"],
}


def test_check_received_arguments_valid():
    assert check_received_arguments(_ARGS_SCHEMA, {"kind": "a", "level": 2}) == []


def test_check_received_arguments_enum_violation():
    assert check_received_arguments(_ARGS_SCHEMA, {"kind": "zzz"}) == [
        "kind must be one of ['a', 'b'], got 'zzz'"
    ]


def test_check_received_arguments_int_enum():
    assert check_received_arguments(_ARGS_SCHEMA, {"kind": "a", "level": 3}) == [
        "level must be one of [1, 2], got 3"
    ]


def test_check_received_arguments_required_none():
    assert check_received_arguments(_ARGS_SCHEMA, {"kind": None}) == [
        "kind must not be null"
    ]


def test_check_received_arguments_optional_none_is_ok():
    assert (
        check_received_arguments(
            _ARGS_SCHEMA, {"kind": "a", "level": None, "note": None}
        )
        == []
    )


def test_check_received_arguments_does_not_check_types():
    arguments = {"kind": "a", "day": datetime.date(2025, 1, 2), "note": 5}
    assert check_received_arguments(_ARGS_SCHEMA, arguments) == []


def test_check_received_arguments_joins_all_problems_in_handler():
    called = []

    def tool(kind: str, level: int = 1):
        called.append(kind)

    handler = make_handler(tool, input_schema=_ARGS_SCHEMA)
    result = handler("zzz", level=3)
    assert result["error"]["type"] == "INVALID_ARGUMENTS"
    assert result["error"]["message"] == (
        "kind must be one of ['a', 'b'], got 'zzz'; level must be one of [1, 2], got 3"
    )
    assert (
        result["error"]["hint"] == "Allowed values for kind: a, b. "
        "Allowed values for level: 1, 2."
    )
    assert called == []


def test_handler_required_none_returns_invalid_arguments():
    called = []

    def tool(kind: str):
        called.append(kind)

    result = make_handler(tool, input_schema=_ARGS_SCHEMA)(None)
    assert result == {
        "error": {"type": "INVALID_ARGUMENTS", "message": "kind must not be null"}
    }
    assert called == []


def test_handler_valid_arguments_call_function():
    def tool(kind: str, day=None):
        return {"kind": kind, "day": day}

    handler = make_handler(tool, input_schema=_ARGS_SCHEMA)
    assert handler("a", day=datetime.date(2025, 1, 2)) == {
        "kind": "a",
        "day": "2025-01-02",
    }


@pytest.mark.parametrize("positional", [True, False])
def test_procedure_handler_skips_session_and_maps_names(positional):
    called = []

    def tool(session, kind: str):
        called.append(kind)

    handler = make_handler(tool, input_schema=_ARGS_SCHEMA, takes_session=True)
    result = handler(object(), "zzz") if positional else handler(object(), kind="zzz")
    assert result["error"]["message"] == "kind must be one of ['a', 'b'], got 'zzz'"
    assert called == []
    handler(object(), "a")
    assert called == ["a"]


def test_handler_with_session_named_like_a_property_is_not_checked():
    schema = {"properties": {"kind": {"enum": ["a"]}}, "required": ["kind"]}

    def tool(kind, other):
        return "ok"

    assert make_handler(tool, input_schema=schema, takes_session=True)("x", 1) == "ok"


def test_udf_handler_maps_positional_and_keyword_names():
    def tool(kind: str, note: str = ""):
        return "ok"

    handler = make_handler(tool, input_schema=_ARGS_SCHEMA)
    assert handler("a", "n") == "ok"
    assert handler(kind="a") == "ok"
    assert handler("q")["error"]["type"] == "INVALID_ARGUMENTS"
    assert handler(kind="q", note="n")["error"]["type"] == "INVALID_ARGUMENTS"


def test_handler_without_input_schema_does_not_validate():
    def tool(kind: str):
        return kind

    assert make_handler(tool)("zzz") == "zzz"
    assert make_handler(tool)(None) is None


def test_handler_validates_when_masking_errors():
    result = make_handler(
        lambda kind: kind, mask_error_details=True, input_schema=_ARGS_SCHEMA
    )("zzz")
    assert result["error"]["type"] == "INVALID_ARGUMENTS"


def test_handler_with_wrong_arguments_lets_the_function_raise():
    def f(a: int) -> int:
        return a

    handler = make_handler(f, input_schema={"type": "object", "properties": {}})
    with pytest.raises(TypeError):
        handler(1, 2)


# ---------------------------------------------------------------------------
# Nullable arguments, numpy arrays and duplicate column names
# ---------------------------------------------------------------------------


def test_handler_null_for_required_nullable_argument_is_accepted():
    schema = {
        "type": "object",
        "properties": {"a": {"type": ["string", "null"]}, "b": {"type": "string"}},
        "required": ["a", "b"],
    }

    def tool(a, b):
        return {"a": a}

    handler = make_handler(tool, input_schema=schema)
    assert handler(None, "x") == {"a": None}
    assert handler("x", None) == {
        "error": {"type": "INVALID_ARGUMENTS", "message": "b must not be null"}
    }


def test_to_json_value_numpy_array():
    np = pytest.importorskip("numpy")
    result = to_json_value(np.array([1, 2]))
    assert result == [1, 2] and type(result[0]) is int
    assert to_json_value(np.array([[1.5, 2.5], [3.5, 4.5]])) == [
        [1.5, 2.5],
        [3.5, 4.5],
    ]
    assert to_json_value({"a": np.array([np.float64(1.0)])}) == {"a": [1.0]}


def test_to_tool_result_snowpark_dataframe_with_duplicate_column_names():
    row = Row._builder.build("x", "y", "z").to_row()
    row.__dict__["_fields"] = ("A", "A", "B")
    df = FakeDataFrame([row], names=["A", "A", "B"])
    result = to_tool_result(df, max_rows=10, max_result_bytes=10_000)
    assert result["columns"] == ["A", "A_2", "B"]
    assert result["rows"] == [{"A": "x", "A_2": "y", "B": "z"}]
    empty = to_tool_result(
        FakeDataFrame([], names=["A", "A"]), max_rows=10, max_result_bytes=10_000
    )
    assert empty["columns"] == ["A", "A_2"]


def test_to_tool_result_pandas_dataframe_with_duplicate_column_names():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame([[1, 2, 3]], columns=["A", "A", "A_2"])
    result = to_tool_result(df, max_rows=10, max_result_bytes=10_000)
    assert result["columns"] == ["A", "A_2", "A_2_2"]
    assert result["rows"] == [{"A": 1, "A_2": 2, "A_2_2": 3}]
    # The caller's DataFrame is not renamed.
    assert list(df.columns) == ["A", "A", "A_2"]


def test_run_tool_restores_a_traceback_limit_it_lifted_for_logging():
    old = getattr(sys, "tracebacklimit", None)
    sys.tracebacklimit = 5
    try:
        with pytest.raises(ValueError):
            run_tool(_secret_failure, (), {})
        assert sys.tracebacklimit == 5
    finally:
        if old is None:
            del sys.tracebacklimit
        else:
            sys.tracebacklimit = old


def test_to_json_value_pandas_na():
    pd = pytest.importorskip("pandas")
    assert to_json_value({"a": pd.NA, "b": 1}) == {"a": None, "b": 1}
    df = pd.DataFrame({"a": pd.array([1, None], dtype="Int64")})
    result = to_tool_result(df, max_rows=10, max_result_bytes=10_000)
    assert result["rows"] == [{"a": 1}, {"a": None}]


def test_enum_error_for_nullable_parameter_does_not_show_none():
    schema = {
        "type": "object",
        "properties": {"mode": {"type": ["string", "null"], "enum": ["a", "b", None]}},
        "required": ["mode"],
    }

    def f(mode):
        return mode

    result = make_handler(f, input_schema=schema)("zzz")
    assert result["error"]["type"] == "INVALID_ARGUMENTS"
    assert result["error"]["message"] == "mode must be one of ['a', 'b'], got 'zzz'"
    assert result["error"]["hint"] == "Allowed values for mode: a, b."
    assert make_handler(f, input_schema=schema)(None) is None
