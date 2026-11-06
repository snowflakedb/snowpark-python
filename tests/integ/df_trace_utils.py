#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Helpers of the DataFrame trace tests (``test_df_trace_*.py``).

The tests build dataframes whose action fails on the server and check the debug output of the
error. ``src`` gives the expected trace entry of a call from the source of the test file, so the
position of every entry (line and, on Python 3.11+, columns) is checked exactly.
"""

import ast
import inspect
import os
import re
import sys
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple


HAS_COLUMNS = sys.version_info >= (3, 11)

# The column the dataframes of the tests fail on.
BAD = "DOES_NOT_EXIST"


# ------------------------------------------------------------------ dataframes of the tests
def base(session):
    return session.create_dataframe(
        [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)], schema=["ID", "NAME", "V"]
    )


def other(session):
    return session.create_dataframe([(1, "x"), (2, "y")], schema=["ID", "TAG"])


def fail(df):
    return df.select(BAD)


# ------------------------------------------------------------------ source positions of calls
_SOURCES: Dict[str, Tuple[List[str], List[ast.Call], Dict[int, int]]] = {}


def _parse(path: str):
    if path not in _SOURCES:
        with open(path) as f:
            source = f.read()
        tree = ast.parse(source)
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
        calls.sort(key=lambda n: (n.lineno, n.col_offset))
        decorated_line = {
            id(decorator): node.lineno
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.ClassDef))
            for decorator in node.decorator_list
        }
        _SOURCES[path] = (source.split("\n"), calls, decorated_line)
    return _SOURCES[path]


def _position(
    call: ast.Call, decorated_line: Dict[int, int]
) -> Tuple[int, int, int, int]:
    """The position Python reports for the frame of a call, which the trace records. On Python
    3.11+ it is the extent of the call, starting at the method name when the method is on a later
    line than the receiver. Before 3.11 only the line is known: the line of the method name for a
    method call without keyword or star arguments, of the decorated definition for a decorator
    and else the first line of the call."""
    func = call.func
    if HAS_COLUMNS:
        if isinstance(func, ast.Attribute) and func.end_lineno > call.lineno:
            start = (func.end_lineno, func.end_col_offset - len(func.attr))
        else:
            start = (call.lineno, call.col_offset)
        return (*start, call.end_lineno, call.end_col_offset)
    if id(call) in decorated_line:
        line = decorated_line[id(call)]
    elif (
        isinstance(func, ast.Attribute)
        and not call.keywords
        and not any(isinstance(a, ast.Starred) for a in call.args)
    ):
        line = func.end_lineno
    else:
        line = call.lineno
    return (line, 0, line, 0)


def _text(lines: List[str], position: Tuple[int, int, int, int]) -> str:
    line, column, end_line, end_column = position
    text = "\n".join(lines[line - 1 : end_line])
    return text[column : len(text) - len(lines[end_line - 1]) + end_column]


_SPACES = re.compile(r"\s+")


class Src:
    """A call of a source file, found by its source text."""

    def __init__(
        self,
        snippet: str,
        path: str,
        nth: Optional[int],
        scope: Optional[Tuple[int, int]],
    ) -> None:
        lines, calls, decorated_line = _parse(path)
        wanted = _SPACES.sub("", snippet)
        prefix = wanted.endswith("...")
        if prefix:
            wanted = wanted[:-3]
        found = []
        for call in calls:
            if scope is not None and not scope[0] <= call.lineno <= scope[1]:
                continue
            full = (call.lineno, call.col_offset, call.end_lineno, call.end_col_offset)
            text = _SPACES.sub("", _text(lines, full))
            if text == wanted or (prefix and text.startswith(wanted)):
                found.append(_position(call, decorated_line))
        assert found, f"no call {snippet!r} in {path}"
        assert (
            nth is not None or len(found) == 1
        ), f"several calls {snippet!r}, pass nth"
        nth = nth or 1
        assert len(found) >= nth, f"fewer than {nth} calls {snippet!r} in {path}"
        self.path = path
        self.position = found[nth - 1]
        self.lines = lines

    @property
    def entry(self) -> str:
        line, column, end_line, end_column = self.position
        if not HAS_COLUMNS:
            return f"{self.path}|{line}: {self.lines[line - 1]}"
        return (
            f"{self.path}|{line}:{column}-{end_line}:{end_column}: "
            f"{_text(self.lines, self.position)}"
        )

    @property
    def location(self) -> str:
        line, _, end_line, _ = self.position
        lines = f"line {line}" if line == end_line else f"lines {line}-{end_line}"
        return f"{self.path}: {lines}"

    @property
    def line(self) -> int:
        return self.position[0]


def _function_range(path: str, line: int) -> Optional[Tuple[int, int]]:
    """The lines of the outermost function of ``path`` that contains ``line``."""
    with open(path) as f:
        tree = ast.parse(f.read())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.lineno <= line <= node.end_lineno:
            return (node.lineno, node.end_lineno)
    return None


def src(snippet: str, *, nth: Optional[int] = None, path: Optional[str] = None) -> Src:
    """The call whose source text is ``snippet`` (whitespace is ignored; a trailing
    ``...`` matches the start of the call) in the calling test function, or in the file
    ``path``. Use ``nth`` when several calls match, in the order of the source."""
    if path is not None:
        return Src(snippet, os.path.abspath(path), nth, None)
    caller = inspect.stack()[1]
    path = os.path.abspath(caller.filename)
    return Src(snippet, path, nth, _function_range(path, caller.lineno))


_HERE = os.path.abspath(__file__.replace(".pyc", ".py"))
BASE = src(
    'session.create_dataframe([(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)], schema=["ID", "NAME", "V"])',
    path=_HERE,
)
OTHER = src(
    'session.create_dataframe([(1, "x"), (2, "y")], schema=["ID", "TAG"])', path=_HERE
)
FAIL = src("df.select(BAD)", path=_HERE)


# ------------------------------------------------------------------ the debug output of an error
_HEADER = "Trace of the most recent dataframe operations associated with the error"
_MAPPED = "SQL compilation error corresponds to Python source"
_MISSING = re.compile(
    r"^Missing object '([^']+)' corresponds to Python source at (.+)\.$"
)
_SECTION_START = re.compile(
    r"\n\n(?=Trace of |SQL compilation error corresponds|Missing object |Object ')"
)
_EXISTING = re.compile(r"^Object '([^']+)' was first referenced at (.+)\.$")


def debug_output(exc_info) -> SimpleNamespace:
    """The sections of the debug information of a raised error."""
    out = SimpleNamespace(
        lineage="", mapped=[], missing_object=None, existing_object=None
    )
    text = exc_info.value.debug_context or ""
    sections = text.split("--- Additional Debug Information ---")[-1].strip("\n")
    for section in _SECTION_START.split(sections):
        section = section.strip("\n")
        if not section:
            continue
        if section.startswith(_HEADER):
            out.lineage = section
        elif section.startswith(_MAPPED):
            if section.endswith(":") or "\n" in section:
                out.mapped = [line[len("  - ") :] for line in section.split("\n")[1:]]
            else:
                out.mapped = [section.split(" at ", 1)[1].rstrip(".")]
        elif _MISSING.match(section):
            out.missing_object = _MISSING.match(section).groups()
        elif _EXISTING.match(section):
            out.existing_object = _EXISTING.match(section).groups()
        else:
            raise AssertionError(f"unexpected debug information: {section!r}")
    return out


def assert_debug_output(
    exc_info,
    trace: List[Src],
    mapped: Optional[List[Src]] = None,
    missing_object: Optional[Tuple[str, Src]] = None,
    existing_object: Optional[Tuple[str, Src]] = None,
) -> None:
    """Checks every part of the debug output of an error. The lineage is compared as text: the
    header, the order of the operations and the position and source text of each one."""
    out = debug_output(exc_info)
    entries = [s.entry for s in trace]
    if not HAS_COLUMNS:
        # Without columns, the operations of a line have the same entry and are shown once.
        entries = list(dict.fromkeys(entries))
    expected_lineage = (
        f"{_HEADER} (total {len(entries)}):\n\n" + "\n".join(entries) if entries else ""
    )
    assert out.lineage == expected_lineage

    locations = [s.location for s in mapped or []]
    if not HAS_COLUMNS:
        locations = list(dict.fromkeys(locations))
    assert out.mapped == locations

    for actual, expected in (
        (out.missing_object, missing_object),
        (out.existing_object, existing_object),
    ):
        if expected is None:
            assert actual is None
        else:
            name, call = expected
            assert actual is not None and re.fullmatch(name, actual[0]), actual
            assert actual[1] == f"{call.path}: line {call.line}"


def random_name(prefix: str) -> str:
    import uuid

    return f"{prefix}_{uuid.uuid4().hex[:12].upper()}"
