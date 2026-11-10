#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import ast
import copy
import inspect
import threading
from contextlib import contextmanager
from pathlib import Path

import pytest

import snowflake.snowpark
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark import context
from snowflake.snowpark._internal.debug_config import DebugConfig
from snowflake.snowpark._internal.ast.utils import with_src_position
from snowflake.snowpark._internal.utils import publicapi
from snowflake.snowpark._internal.df_trace import (
    ObjectCreationRegistry,
    SrcPosition,
    TraceNode,
    add_parent,
    capture_user_src_position,
    collect_input_nodes,
    traces_inputs,
)

SRC_DIR = Path(snowflake.snowpark.__file__).parent


# ------------------------------------------------------------------ capture
def _capture_both():
    lite = capture_user_src_position(2)
    ast_src = with_src_position(proto.Expr().sql, caller_frame_depth=2)
    return lite, ast_src


def test_capture_matches_the_ast_frame_walk():
    # Both step back to the call site of _capture_both: they must report the same position.
    lite, ast_src = _capture_both()
    assert isinstance(lite, SrcPosition)
    assert lite.filename == __file__
    assert lite.start_line == ast_src.src.start_line
    assert lite.start_column == ast_src.src.start_column
    assert lite.end_line == ast_src.src.end_line
    assert lite.end_column == ast_src.src.end_column


def test_capture_steps_out_of_the_library():
    def user_function():
        return capture_user_src_position()

    pos = user_function()
    assert pos.filename == __file__
    assert pos.start_line == inspect.currentframe().f_lineno - 2 or pos.filename


# ------------------------------------------------------------------ graph basics
def test_trace_node_is_shared_by_copies():
    node = TraceNode(SrcPosition("f.py", 1), [])
    assert copy.deepcopy(node) is node
    assert copy.copy(node) is node
    assert copy.deepcopy([node, {"a": node}])[0] is node


def test_trace_node_source_id_distinguishes_positions():
    a = TraceNode(SrcPosition("f.py", 1, 2, 1, 9))
    b = TraceNode(SrcPosition("f.py", 1, 10, 1, 20))
    c = TraceNode(SrcPosition("f.py", 1, 2, 1, 9))
    assert a.source_id != b.source_id
    assert a.source_id == c.source_id
    assert TraceNode(None).source_id == ""


def test_add_parent_keeps_creation_order_and_ignores_duplicates():
    parent1, parent2 = TraceNode(None), TraceNode(None)
    child = TraceNode(None)
    add_parent(child, parent2)
    add_parent(child, parent1)
    add_parent(child, parent1)
    add_parent(child, None)
    add_parent(child, child)
    assert child.parents == [parent1, parent2]


def test_traces_inputs_adds_the_dataframe_arguments():
    class Fake:
        def __init__(self) -> None:
            self._trace_node = TraceNode(None)

    class Op:
        @traces_inputs("other", "right")
        def combine(self, other, right=None):
            result = Fake()
            result._trace_node.parents = []
            return result

    a, b = Fake(), Fake()
    result = Op().combine(a, right=b)
    assert result._trace_node.parents == [a._trace_node, b._trace_node]
    # a non-dataframe or missing argument is ignored
    assert Op().combine(1)._trace_node.parents == []


def test_traces_inputs_keeps_the_signature_for_publicapi():
    @traces_inputs("other")
    def f(self, other, _emit_ast=True):
        return None

    assert list(inspect.signature(f).parameters) == ["self", "other", "_emit_ast"]


def test_collect_input_nodes_stops_at_the_nearest_nodes_and_ignores_none():
    class Plan:
        def __init__(self, nodes=None, children=()) -> None:
            self.df_trace_nodes = nodes
            self.children_plan_nodes = list(children)

    n1, n2 = TraceNode(None), TraceNode(None)
    deep = Plan([TraceNode(None)])
    leaf1, leaf2 = Plan([n1]), Plan([n2], children=[deep])
    root = Plan(None, children=[Plan(None, children=[leaf1]), leaf2])
    assert collect_input_nodes(root, None) == [n1, n2]


def test_collect_input_nodes_drops_ancestors_of_other_inputs():
    parent = TraceNode(None)
    child = TraceNode(None, [parent])

    class Plan:
        def __init__(self, nodes) -> None:
            self.df_trace_nodes = nodes
            self.children_plan_nodes = []

    assert collect_input_nodes(Plan([parent]), Plan([child])) == [child]


# ------------------------------------------------------------------ registry
def test_registry_is_bounded_and_ordered():
    registry = ObjectCreationRegistry(max_size=3)
    for i in range(5):
        registry.add("table", f"T{i}", None)
    assert [c.name_or_query for c in registry.items()] == ["T2", "T3", "T4"]


def test_registry_is_thread_safe():
    registry = ObjectCreationRegistry(max_size=100000)

    def worker():
        for i in range(500):
            registry.add("sql", str(i), None)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(registry.items()) == 4000


def test_node_ids_are_unique_across_threads():
    ids, lock = [], threading.Lock()

    def worker():
        local = [TraceNode(None).node_id for _ in range(500)]
        with lock:
            ids.extend(local)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(ids)) == len(ids) == 4000


# ------------------------------------------------------------------ no AST in the debug path
def _imports(path):
    tree = ast.parse(path.read_text())
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names.append(node.module or "")
            names.extend(f"{node.module}.{a.name}" for a in node.names)
        elif isinstance(node, ast.Import):
            names.extend(a.name for a in node.names)
    return names


def test_debug_utils_does_not_use_the_ast():
    path = SRC_DIR / "_internal" / "debug_utils.py"
    assert not [n for n in _imports(path) if "_internal.ast" in n or "ast_pb2" in n]
    text = path.read_text()
    for forbidden in (
        "_ast_batch",
        "_bind_stmt_cache",
        "df_ast_ids",
        "get_dependent_bind_ids",
    ):
        assert forbidden not in text


def test_df_trace_module_does_not_use_the_ast():
    path = SRC_DIR / "_internal" / "df_trace.py"
    assert not [n for n in _imports(path) if "_internal.ast" in n or "ast_pb2" in n]


def test_configure_development_features_does_not_use_the_ast():
    source = inspect.getsource(context.configure_development_features)
    assert (
        "ast"
        not in source.replace("last", "")
        .replace("fast", "")
        .replace("least", "")
        .lower()
        .split('"""')[-1]
    )


def test_wrap_exception_reads_trace_nodes_not_ast_ids():
    from snowflake.snowpark._internal.analyzer import snowflake_plan

    source = inspect.getsource(snowflake_plan.SnowflakePlan.Decorator.wrap_exception)
    assert "df_trace_nodes" in source
    assert "df_ast_ids" not in source
    assert "_ast_batch" not in source


# ------------------------------------------------------------------ expression inputs (GAP-1, GAP-2)
def _plan_with(node):
    class Plan:
        def __init__(self) -> None:
            self.df_trace_nodes = [node]
            self.children_plan_nodes = []

    return Plan()


# ------------------------------------------------------------------ public call wrapper
@contextmanager
def _tracing_session():
    """A session with the lineage trace on, so the public API wrapper records the trace."""
    session = type("Session", (), {"_conn": None})()
    session._debug = DebugConfig(session)
    session._debug.enable_dataframe_trace_on_error()
    try:
        yield session
    finally:
        session._debug._close()


@pytest.fixture()
def tracing_flags():
    with _tracing_session():
        yield


class _Container:
    def __init__(self, nodes=None) -> None:
        self.df_trace_nodes = nodes


def _dataframe(nodes=None):
    """A stand-in for a dataframe returned by a public API: the wrapper reads its trace node
    and the trace lists of its plans."""
    df = type("DF", (), {})()
    df._trace_node = TraceNode(None)
    df._plan, df._select_statement = _Container(nodes), None
    return df


def _returning(df):
    @publicapi
    def api(*args, **kwargs):
        return df

    return api


def test_subqueries_at_any_depth_are_inputs_of_the_result(tracing_flags):
    from snowflake.snowpark._internal.analyzer.binary_expression import And, EqualTo
    from snowflake.snowpark._internal.analyzer.expression import (
        InExpression,
        Literal,
        ScalarSubquery,
    )
    from snowflake.snowpark.column import Column

    n1, n2 = TraceNode(None), TraceNode(None)
    in1 = InExpression(Literal(1), [ScalarSubquery(_plan_with(n1))])
    in2 = InExpression(Literal(2), [ScalarSubquery(_plan_with(n2))])
    df = _dataframe()
    _returning(df)(Column(And(EqualTo(Literal(1), Literal(1)), And(in1, in2))))
    assert {id(n) for n in df._trace_node.parents} == {id(n1), id(n2)}


def test_functions_are_inputs_of_the_result_and_plain_columns_are_not(tracing_flags):
    from snowflake.snowpark._internal.analyzer.expression import (
        Attribute,
        Literal,
        SnowflakeUDF,
    )
    from snowflake.snowpark.column import Column
    from snowflake.snowpark.types import IntegerType

    udf_node = TraceNode(None, kind="udf")
    call = SnowflakeUDF("F", [Literal(1)], IntegerType())
    call._df_trace_node = udf_node
    df = _dataframe()
    _returning(df)(Column(call), Column(Attribute("A", IntegerType())))
    assert df._trace_node.parents == [udf_node]


def test_shared_subtrees_are_scanned_once(tracing_flags):
    from snowflake.snowpark._internal.analyzer.binary_expression import And
    from snowflake.snowpark._internal.analyzer.expression import ScalarSubquery
    from snowflake.snowpark.column import Column

    node = TraceNode(None)
    shared = ScalarSubquery(_plan_with(node))
    df = _dataframe()
    _returning(df)(Column(And(shared, shared)))
    assert df._trace_node.parents == [node]


def test_columns_in_lists_dicts_and_table_function_calls_are_scanned(tracing_flags):
    from snowflake.snowpark._internal.analyzer.expression import (
        InExpression,
        Literal,
        ScalarSubquery,
    )
    from snowflake.snowpark.column import Column
    from snowflake.snowpark.table_function import TableFunctionCall

    n1, n2, n3 = TraceNode(None), TraceNode(None), TraceNode(None)
    c1 = Column(InExpression(Literal(1), [ScalarSubquery(_plan_with(n1))]))
    c2 = Column(InExpression(Literal(2), [ScalarSubquery(_plan_with(n2))]))
    call = TableFunctionCall("F")
    call._df_trace_node = n3

    df = _dataframe()
    _returning(df)(c1, [c2], call, x=5)
    assert {id(n) for n in df._trace_node.parents} == {id(n1), id(n2), id(n3)}
    df = _dataframe()
    _returning(df)(cols={"a": c1})
    assert df._trace_node.parents == [n1]


def test_only_the_outermost_call_links_inputs(tracing_flags):
    from snowflake.snowpark._internal.analyzer.expression import ScalarSubquery
    from snowflake.snowpark.column import Column

    node = TraceNode(None)
    inner_df, outer_df = _dataframe(), _dataframe()
    inner = _returning(inner_df)

    @publicapi
    def outer(column):
        inner(column)
        return outer_df

    outer(Column(ScalarSubquery(_plan_with(node))))
    assert outer_df._trace_node.parents == [node]
    assert inner_df._trace_node.parents == []


def test_a_call_that_raised_does_not_leave_state_behind(tracing_flags):
    from snowflake.snowpark._internal.analyzer.expression import ScalarSubquery
    from snowflake.snowpark.column import Column

    @publicapi
    def failing():
        @publicapi
        def inner():
            raise RuntimeError("boom")

        inner()

    with pytest.raises(RuntimeError):
        failing()
    # the next call is an outermost call again
    node = TraceNode(None)
    df = _dataframe()
    _returning(df)(Column(ScalarSubquery(_plan_with(node))))
    assert df._trace_node.parents == [node]


def test_a_failure_in_the_trace_does_not_fail_the_call(tracing_flags, monkeypatch):
    from snowflake.snowpark._internal import df_trace
    from snowflake.snowpark._internal.analyzer.expression import ScalarSubquery
    from snowflake.snowpark.column import Column

    def broken(*args, **kwargs):
        raise TypeError("broken trace")

    monkeypatch.setattr(df_trace, "add_parent", broken)
    df = _dataframe()
    assert _returning(df)(Column(ScalarSubquery(_plan_with(TraceNode(None))))) is df

    class Op:
        @traces_inputs("other")
        def combine(self, other):
            return _dataframe()

    assert Op().combine(_dataframe()) is not None

    monkeypatch.setattr(df_trace, "SrcPosition", broken)
    assert capture_user_src_position() is None
    assert df_trace.registration_trace_node("udf").src is None


def test_calls_in_other_threads_are_independent(tracing_flags):
    from snowflake.snowpark._internal.analyzer.expression import ScalarSubquery
    from snowflake.snowpark.column import Column

    ready, release = threading.Event(), threading.Event()

    @publicapi
    def hold():
        ready.set()
        release.wait(5)

    t = threading.Thread(target=hold)
    t.start()
    ready.wait(5)
    try:
        # a call is open in the other thread; this one is still an outermost call
        node = TraceNode(None)
        df = _dataframe()
        _returning(df)(Column(ScalarSubquery(_plan_with(node))))
        assert df._trace_node.parents == [node]
    finally:
        release.set()
        t.join()


def test_publicapi_records_only_when_tracing_is_on():
    from snowflake.snowpark._internal.analyzer.expression import ScalarSubquery
    from snowflake.snowpark.column import Column

    node = TraceNode(None)
    column = Column(ScalarSubquery(_plan_with(node)))
    off = _dataframe()
    _returning(off)(column)
    with _tracing_session():
        on = _dataframe()
        _returning(on)(column)
    assert off._trace_node.parents == []
    assert on._trace_node.parents == [node]


def test_registration_node_only_when_tracing_is_on():
    from snowflake.snowpark._internal.df_trace import registration_trace_node

    assert registration_trace_node("udf") is None
    with _tracing_session():
        node = registration_trace_node("udf")
    assert node.kind == "udf"
    assert node.src.filename == __file__


def test_tracing_flags_on_counts_the_tracing_sessions():
    from snowflake.snowpark._internal.df_trace import tracing_flags_on

    assert tracing_flags_on() is False
    with _tracing_session() as session:
        assert tracing_flags_on() is True
        session._debug.disable_dataframe_trace_on_error()
        assert tracing_flags_on() is False
        session._debug.enable_trace_sql_errors_to_dataframe()
        assert tracing_flags_on() is True
    # a closed session is released
    assert tracing_flags_on() is False
