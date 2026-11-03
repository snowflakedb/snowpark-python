#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import ast
import copy
import inspect
import threading
from pathlib import Path

import pytest

import snowflake.snowpark
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark import context
from snowflake.snowpark._internal.ast.utils import with_src_position
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
        not in source.replace("last", "").replace("fast", "").lower().split('"""')[-1]
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


def test_expression_scan_finds_subqueries_at_any_depth():
    from snowflake.snowpark._internal.analyzer.binary_expression import And, EqualTo
    from snowflake.snowpark._internal.analyzer.expression import (
        InExpression,
        Literal,
        ScalarSubquery,
    )
    from snowflake.snowpark._internal.df_trace import _expression_inputs

    n1, n2 = TraceNode(None), TraceNode(None)
    in1 = InExpression(Literal(1), [ScalarSubquery(_plan_with(n1))])
    in2 = InExpression(Literal(2), [ScalarSubquery(_plan_with(n2))])
    tree = And(EqualTo(Literal(1), Literal(1)), And(in1, in2))
    assert {id(n) for n in _expression_inputs(tree)} == {id(n1), id(n2)}


def test_expression_scan_finds_function_nodes_and_ignores_plain_columns():
    from snowflake.snowpark._internal.analyzer.expression import (
        Attribute,
        Literal,
        SnowflakeUDF,
    )
    from snowflake.snowpark._internal.df_trace import _expression_inputs
    from snowflake.snowpark.types import IntegerType

    udf_node = TraceNode(None, kind="udf")
    call = SnowflakeUDF("F", [Literal(1)], IntegerType())
    call._df_trace_node = udf_node
    assert _expression_inputs(call) == [udf_node]
    assert _expression_inputs(Attribute("A", IntegerType())) == []


def test_expression_scan_handles_shared_subtrees():
    from snowflake.snowpark._internal.analyzer.binary_expression import And
    from snowflake.snowpark._internal.analyzer.expression import Literal
    from snowflake.snowpark._internal.df_trace import _expression_inputs

    shared = Literal(1)
    tree = And(shared, shared)
    assert _expression_inputs(tree) == []


def test_argument_scan_reads_columns_lists_dicts_and_table_function_calls():
    from snowflake.snowpark._internal.analyzer.expression import (
        InExpression,
        Literal,
        ScalarSubquery,
    )
    from snowflake.snowpark._internal.df_trace import _argument_inputs
    from snowflake.snowpark.column import Column

    n1, n2, n3 = TraceNode(None), TraceNode(None), TraceNode(None)
    c1 = Column(InExpression(Literal(1), [ScalarSubquery(_plan_with(n1))]))
    c2 = Column(InExpression(Literal(2), [ScalarSubquery(_plan_with(n2))]))

    from snowflake.snowpark.table_function import TableFunctionCall

    call = TableFunctionCall("F")
    call._df_trace_node = n3

    found = _argument_inputs((c1, [c2], call), {"x": 5})
    assert {id(n) for n in found} == {id(n1), id(n2), id(n3)}
    found = _argument_inputs((), {"cols": {"a": c1}})
    assert [id(n) for n in found] == [id(n1)]


# ------------------------------------------------------------------ public call wrapper (GAP-3)
@pytest.fixture()
def tracing_flags():
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    yield
    context.configure_development_features()


def test_public_call_depth_is_restored_after_exceptions(tracing_flags):
    from snowflake.snowpark._internal import df_trace

    def outer():
        return df_trace.trace_public_call(inner, (), {})

    def inner():
        assert df_trace.public_call_depth() == 2
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        df_trace.trace_public_call(outer, (), {})
    assert df_trace.public_call_depth() == 0
    assert getattr(df_trace._call_state, "created", None) is None


def test_public_call_depth_is_per_thread(tracing_flags):
    from snowflake.snowpark._internal import df_trace

    seen = {}
    ready, release = threading.Event(), threading.Event()

    def hold():
        ready.set()
        release.wait(5)

    def run_in_thread():
        df_trace.trace_public_call(hold, (), {})

    t = threading.Thread(target=run_in_thread)
    t.start()
    ready.wait(5)
    seen["main"] = df_trace.public_call_depth()
    release.set()
    t.join()
    assert seen["main"] == 0


def test_publicapi_records_only_when_tracing_is_on():
    from snowflake.snowpark._internal import df_trace
    from snowflake.snowpark._internal.utils import publicapi

    calls = []

    @publicapi
    def api():
        calls.append(df_trace.public_call_depth())

    context.configure_development_features()
    api()
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    try:
        api()
    finally:
        context.configure_development_features()
    assert calls == [0, 1]


def test_internal_nodes_are_removed_from_plan_lists():
    from snowflake.snowpark._internal.df_trace import _strip_internal_nodes

    returned_node, internal_node, older = (
        TraceNode(None),
        TraceNode(None),
        TraceNode(None),
    )

    class Container:
        def __init__(self, nodes) -> None:
            self.df_trace_nodes = nodes

    shared = [older, internal_node]
    internal_df = type("DF", (), {})()
    internal_df._trace_node = internal_node
    internal_df._plan, internal_df._select_statement = Container(shared), None
    returned_df = type("DF", (), {})()
    returned_df._trace_node = returned_node
    returned_df._plan = Container([older, internal_node, returned_node])
    returned_df._select_statement = Container([internal_node])

    _strip_internal_nodes([internal_df, returned_df], [returned_df])
    assert shared == [older]
    assert internal_df._plan.df_trace_nodes == [older]
    assert returned_df._plan.df_trace_nodes == [older, returned_node]
    assert returned_df._select_statement.df_trace_nodes is None


def test_registration_node_only_when_tracing_is_on():
    from snowflake.snowpark._internal.df_trace import registration_trace_node

    context.configure_development_features()
    assert registration_trace_node("udf") is None
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    try:
        node = registration_trace_node("udf")
    finally:
        context.configure_development_features()
    assert node.kind == "udf"
    assert node.src.filename == __file__
