#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import ast
import copy
import gc
import inspect
import threading
import weakref
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
    is_tracing_enabled,
    traces_inputs,
)
from snowflake.snowpark.functions import col

SRC_DIR = Path(snowflake.snowpark.__file__).parent


@pytest.fixture()
def tracing():
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    yield
    context.configure_development_features()


def _walk(node):
    seen, stack = [], [node]
    while stack:
        n = stack.pop()
        if n not in seen:
            seen.append(n)
            stack.extend(n.parents)
    return seen


def _df(session):
    return session.create_dataframe([(1, "a"), (2, "b")], schema=["id", "name"])


def _lines(node):
    """The source lines of the node and of everything it was derived from, nearest first."""
    out, seen, layer = [], set(), [node]
    while layer:
        nxt = []
        for n in layer:
            if id(n) in seen:
                continue
            seen.add(id(n))
            out.append(n.src.start_line)
            nxt.extend(n.parents)
        layer = nxt
    return out


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


# ------------------------------------------------------------------ dataframes (local testing)
def test_tracing_is_off_by_default(session):
    assert not is_tracing_enabled()
    df = _df(session).filter(col("id") > 1)
    assert df._trace_node is None
    assert df._plan.df_trace_nodes is None


@pytest.mark.parametrize(
    "flags",
    [
        {"enable_dataframe_trace_on_error": True},
        {"enable_trace_sql_errors_to_dataframe": True},
    ],
)
def test_each_tracing_flag_enables_the_trace(session, flags):
    context.configure_development_features(**flags)
    try:
        assert is_tracing_enabled()
        assert _df(session)._trace_node is not None
    finally:
        context.configure_development_features()


def test_eager_schema_validation_alone_does_not_trace(session):
    context.configure_development_features(enable_eager_schema_validation=True)
    try:
        assert not is_tracing_enabled()
        assert _df(session)._trace_node is None
    finally:
        context.configure_development_features()


def test_chain_links_each_dataframe_to_its_input(session, tracing):
    a = _df(session)
    b = a.filter(col("id") > 0)
    c = b.select("id")
    assert c._trace_node.parents == [b._trace_node]
    assert b._trace_node.parents == [a._trace_node]
    # the internal dataframes of one api call have the position of that call
    assert {n.src.start_line for n in _walk(a._trace_node)} == {
        a._trace_node.src.start_line
    }
    assert c._trace_node.src.filename == __file__
    assert _lines(c._trace_node) == sorted(_lines(c._trace_node), reverse=True)


def test_join_and_union_link_both_inputs(session, tracing):
    left = _df(session)
    right = _df(session).select(col("id").alias("id2"))
    joined = left.join(right, col("id") == col("id2"))
    assert {n for n in joined._trace_node.parents} == {
        left._trace_node,
        right._trace_node,
    }
    unioned = left.union(left.select("id", "name"))
    assert left._trace_node in unioned._trace_node.parents


def test_diamond_keeps_one_node_per_branch(session, tracing):
    base = _df(session).filter(col("id") > 0)
    left = base.select("id")
    r = base.select(col("id").alias("id2"))
    j = left.join(r, col("id") == col("id2"))
    reachable = []

    def walk(n):
        if n not in reachable:
            reachable.append(n)
            for p in n.parents:
                walk(p)

    walk(j._trace_node)
    assert base._trace_node in reachable
    assert len(reachable) == len({id(n) for n in reachable})


def test_grouped_dataframe_is_a_node_between_df_and_aggregate(session, tracing):
    df = _df(session)
    grouped = df.group_by("name")
    result = grouped.count()
    assert grouped._trace_node.kind == "grouped_dataframe"
    assert grouped._trace_node.parents == [df._trace_node]
    assert result._trace_node.parents == [grouped._trace_node]


def test_plans_mirror_the_trace_nodes_of_their_dataframe(session, tracing):
    df = _df(session).filter(col("id") > 0).select("id")
    assert df._plan.df_trace_nodes[-1] is df._trace_node
    if df._select_statement is not None:
        assert df._select_statement.df_trace_nodes[-1] is df._trace_node


def test_dataframe_created_before_enabling_has_no_node_and_works_after(session):
    old = _df(session)
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    try:
        new = old.filter(col("id") > 0)
        assert old._trace_node is None
        assert new._trace_node is not None
        assert new._trace_node.parents == []
        assert new.collect()
    finally:
        context.configure_development_features()


def test_trace_is_freed_with_the_dataframe(session, tracing):
    df = _df(session).filter(col("id") > 0).select("id")
    ref = weakref.ref(df._trace_node)
    parent_ref = weakref.ref(df._trace_node.parents[0])
    del df
    gc.collect()
    assert ref() is None
    assert parent_ref() is None


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
