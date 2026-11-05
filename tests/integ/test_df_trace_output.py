#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Tests of the debug output of the error experience on every workload of the corpus.

For every workload and setting, the lineage trace, the SQL error to Python source mapping and the
missing and existing object context must be the expected ones of the workload. The expectations
of a workload refer to its calls by label (see ``df_trace_corpus``); the source position of each
call (line and, on Python 3.11+, columns) and its source text are computed here from the parsed
source of the corpus, so every entry of the output is compared to the position of the call in
the source.
"""

import ast
import os
import re
import sys
from collections import defaultdict
from typing import Dict, List, Tuple

import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from tests.integ import df_trace_corpus
from tests.integ.df_trace_corpus import WORKLOADS, Ctx
from tests.integ.df_trace_harness import DebugFeatures, normalize_output, observe
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SnowparkSQLException is not raised in local testing mode",
        run=False,
    ),
]

CORPUS_PATH = df_trace_corpus.__file__.replace(".pyc", ".py")
# The path in the output, relative to the repository (see normalize_output).
CORPUS_FILE = normalize_output(os.path.abspath(CORPUS_PATH))
HELPERS = ("_base", "_other", "_fail")
HAS_COLUMNS = sys.version_info >= (3, 11)

_MARKER = re.compile(r"^\s*# (@\S.*)$")
_LABEL = re.compile(r"@([A-Za-z_]\w*)(?:=(\w+)(?:#(\d+))?)?")


# ------------------------------------------------------------------ source positions of the calls
def _method_name(call: ast.Call) -> str:
    func = call.func
    return func.attr if isinstance(func, ast.Attribute) else func.id


def _method_line(call: ast.Call) -> int:
    func = call.func
    return func.end_lineno if isinstance(func, ast.Attribute) else func.lineno


def _method_column(call: ast.Call) -> int:
    func = call.func
    if isinstance(func, ast.Attribute):
        return func.end_col_offset - len(func.attr)
    return func.col_offset


def _position(
    call: ast.Call, decorated_line: Dict[int, int]
) -> Tuple[int, int, int, int]:
    """The position the trace records for a call: the one Python reports for the frame of the
    call. On Python 3.11+ it is the extent of the call, starting at the method name when the
    method is on a later line than the receiver. Before 3.11 only the line is known: the line of
    the method name for a method call without keyword or star arguments, of the decorated
    definition for a decorator and else the first line of the call."""
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


class Corpus:
    """The labeled calls of the corpus and their source positions."""

    def __init__(self, path: str) -> None:
        with open(path) as f:
            self.source = f.read()
        self.lines = self.source.split("\n")
        tree = ast.parse(self.source)
        decorated_line = {
            id(decorator): node.lineno
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.ClassDef))
            for decorator in node.decorator_list
        }
        calls_by_method = defaultdict(list)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(
                node.func, (ast.Attribute, ast.Name)
            ):
                calls_by_method[(_method_line(node), _method_name(node))].append(node)
        for calls in calls_by_method.values():
            calls.sort(key=_method_column)

        # The label comments apply to the next line that is not a label comment.
        self.positions: Dict[int, Tuple[str, Tuple[int, int, int, int]]] = {}
        labels_at: Dict[int, Dict[str, ast.Call]] = {}
        pending = []
        for lineno, line in enumerate(self.lines, start=1):
            marker = _MARKER.match(line)
            if marker:
                pending.extend(_LABEL.findall(marker.group(1)))
                continue
            found = {}
            for label, method, nth in pending:
                calls = calls_by_method.get((lineno, method or label), [])
                assert len(calls) >= int(
                    nth or 1
                ), f"line {lineno}: no call of {method or label} for the label {label}"
                if not nth:
                    assert (
                        len(calls) == 1
                    ), f"line {lineno}: several calls of {method or label}, use #n"
                found[label] = calls[int(nth or 1) - 1]
            if found:
                labels_at[lineno] = found
            pending = []

        # A workload's labels are the ones in its declaration and the ones of the helpers.
        self.scopes: Dict[str, Dict[str, ast.Call]] = {}
        helper_labels: Dict[str, ast.Call] = {}
        for node in tree.body:
            declaration = None
            if isinstance(node, ast.FunctionDef):
                if node.name in HELPERS:
                    helper_labels.update(self._labels_in(labels_at, node))
                for decorator in node.decorator_list:
                    if (
                        isinstance(decorator, ast.Call)
                        and getattr(decorator.func, "id", None) == "workload"
                    ):
                        declaration = decorator
            elif (
                isinstance(node, ast.Expr)
                and isinstance(node.value, ast.Call)
                and getattr(node.value.func, "id", None) == "_action_workload"
            ):
                declaration = node.value
            if declaration is not None:
                name = declaration.args[0].value
                self.scopes[name] = self._labels_in(labels_at, node)
        for name, labels in self.scopes.items():
            shadowed = set(labels) & set(helper_labels)
            assert not shadowed, f"{name}: labels {shadowed} are labels of the helpers"
            labels.update(helper_labels)
        self.decorated_line = decorated_line

    @staticmethod
    def _labels_in(labels_at, node) -> Dict[str, ast.Call]:
        start = min(
            [node.lineno] + [d.lineno for d in getattr(node, "decorator_list", [])]
        )
        labels = {}
        for lineno in range(start, node.end_lineno + 1):
            for label, call in labels_at.get(lineno, {}).items():
                assert label not in labels, f"line {lineno}: duplicate label {label}"
                labels[label] = call
        return labels

    def call(self, workload: str, label: str) -> ast.Call:
        scope = self.scopes[workload]
        assert label in scope, f"{workload}: unknown label {label}"
        return scope[label]

    def position(self, workload: str, label: str) -> Tuple[int, int, int, int]:
        return _position(self.call(workload, label), self.decorated_line)

    def entry(self, workload: str, label: str) -> str:
        """The trace entry of a labeled call."""
        line, column, end_line, end_column = self.position(workload, label)
        if not HAS_COLUMNS:
            return f"{CORPUS_FILE}|{line}: {self.lines[line - 1]}"
        text = "\n".join(self.lines[line - 1 : end_line])
        # the source of the call, from its first to its last column
        text = text[column : len(text) - len(self.lines[end_line - 1]) + end_column]
        return f"{CORPUS_FILE}|{line}:{column}-{end_line}:{end_column}: {text}"

    def mapped_location(self, workload: str, label: str) -> str:
        line, _, end_line, _ = self.position(workload, label)
        return f"line {line}" if line == end_line else f"lines {line}-{end_line}"


CORPUS = Corpus(CORPUS_PATH)


# ------------------------------------------------------------------ expected output
def expected_lineage(workload: str, labels: List[str]) -> str:
    entries = [CORPUS.entry(workload, label) for label in labels]
    if not HAS_COLUMNS:
        # Without columns, the operations of a line have the same entry and are shown once.
        entries = list(dict.fromkeys(entries))
    if not entries:
        return ""
    return (
        "Trace of the most recent dataframe operations associated with the error "
        f"(total {len(entries)}):\n\n" + "\n".join(entries)
    )


_MAPPED = re.compile(r"^(?:  - )?(?:.* at )?(\S+): (lines? \d+(?:-\d+)?)\.?$")


def mapped_locations(text: str) -> List[str]:
    """The source locations of a SQL error to Python source mapping, in their order."""
    locations = []
    if not text.strip():
        return locations
    for line in text.strip().split("\n"):
        if line.endswith("Python sources at:"):
            continue
        match = _MAPPED.match(line.strip() if line.startswith("SQL") else line)
        assert match, f"unexpected line in the mapping: {line!r}"
        assert match.group(1) == CORPUS_FILE, line
        locations.append(match.group(2))
    return locations


def expected_object_context(workload: str, kind: str, name: str, label: str) -> str:
    line = CORPUS.position(workload, label)[0]
    if kind == "missing_object":
        return f"\nMissing object '{name}' corresponds to Python source at {CORPUS_FILE}: line {line}.\n"
    return f"\nObject '{name}' was first referenced at {CORPUS_FILE}: line {line}.\n"


# ------------------------------------------------------------------ fixtures
SETTINGS = [
    pytest.param(
        dict(simplifier=True, cte=False, multiline=True),
        id="simplifier-nocte-multiline",
    ),
    pytest.param(
        dict(simplifier=True, cte=True, multiline=True), id="simplifier-cte-multiline"
    ),
    pytest.param(
        dict(simplifier=False, cte=False, multiline=True),
        id="nosimplifier-nocte-multiline",
    ),
    pytest.param(
        dict(simplifier=True, cte=False, multiline=False),
        id="simplifier-nocte-singleline",
    ),
]


@pytest.fixture(scope="module")
def ctx(session):
    table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    session.sql(f"create temp table {table} (ID int, NAME string, V float)").collect()
    session.sql(f"create temp stage {stage}").collect()
    df_trace_corpus.CURRENT_CTX[0] = Ctx(table=table, stage=stage)
    yield df_trace_corpus.CURRENT_CTX[0]
    session.sql(f"drop table if exists {table}").collect()
    session.sql(f"drop stage if exists {stage}").collect()


@pytest.fixture()
def configured(session, settings):
    old = (
        session.sql_simplifier_enabled,
        session.cte_optimization_enabled,
        session._generate_multiline_queries,
    )
    session.sql_simplifier_enabled = settings["simplifier"]
    session.cte_optimization_enabled = settings["cte"]
    (
        session._enable_multiline_queries
        if settings["multiline"]
        else session._disable_multiline_queries
    )()
    yield
    session.sql_simplifier_enabled = old[0]
    session.cte_optimization_enabled = old[1]
    (
        session._enable_multiline_queries
        if old[2]
        else session._disable_multiline_queries
    )()


def test_every_workload_has_labels_for_its_expectations():
    for workload in WORKLOADS:
        labels = [
            *workload.trace,
            *workload.mapped,
            *(workload.mapped_cte or []),
            *(workload.trace_nosimplifier or []),
            *(workload.mapped_nosimplifier or []),
        ]
        for kind in ("missing_object", "existing_object"):
            if getattr(workload, kind) is not None:
                labels.append(getattr(workload, kind)[1])
        for label in labels:
            CORPUS.call(workload.name, label)


# ------------------------------------------------------------------ the test
@pytest.mark.parametrize("settings", SETTINGS)
@pytest.mark.parametrize("workload", WORKLOADS, ids=lambda w: w.name)
def test_debug_output(session, ctx, configured, settings, workload):
    if workload.needs_simplifier and not settings["simplifier"]:
        pytest.skip("the API requires the SQL simplifier")
    with DebugFeatures():
        observed = observe(workload.build, session, ctx, workload.action)
    out = observed.output

    trace, mapped = workload.trace, workload.mapped
    if settings["cte"] and workload.mapped_cte is not None:
        mapped = workload.mapped_cte
    if not settings["simplifier"]:
        trace = workload.trace_nosimplifier or trace
        if workload.mapped_nosimplifier is not None:
            mapped = workload.mapped_nosimplifier
        elif "fail" in mapped:
            mapped = ["fail"]

    lineage = normalize_output(out.lineage, ctx)
    assert lineage == expected_lineage(workload.name, trace)

    expected_mapped = [CORPUS.mapped_location(workload.name, label) for label in mapped]
    if not HAS_COLUMNS:
        expected_mapped = list(dict.fromkeys(expected_mapped))
    assert mapped_locations(normalize_output(out.mapped, ctx)) == expected_mapped

    for kind in ("missing_object", "existing_object"):
        expected = getattr(workload, kind)
        text = normalize_output(getattr(out, kind), ctx)
        if expected is None:
            assert text == ""
        else:
            assert text == expected_object_context(workload.name, kind, *expected)

    # The message of the raised exception carries the output.
    if lineage:
        context_text = normalize_output(observed.exception.debug_context or "", ctx)
        assert lineage in context_text
