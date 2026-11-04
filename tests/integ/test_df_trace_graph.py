#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Shape of the lightweight trace graph built for real DataFrames."""

import gc
import weakref

import pytest

from snowflake.snowpark import context
from snowflake.snowpark._internal.df_trace import is_tracing_enabled
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="the trace is not supported for local testing sessions",
        run=False,
    ),
]


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


def _df(session):
    return session.create_dataframe([(1, "a"), (2, "b")], schema=["id", "name"])


def _walk(node):
    seen, stack = [], [node]
    while stack:
        n = stack.pop()
        if n not in seen:
            seen.append(n)
            stack.extend(n.parents)
    return seen


@pytest.fixture()
def tracing():
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    yield
    context.configure_development_features()


def test_tracing_is_off_by_default(session):
    assert not is_tracing_enabled(session)
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
        assert is_tracing_enabled(session)
        assert _df(session)._trace_node is not None
    finally:
        context.configure_development_features()


def test_eager_schema_validation_alone_does_not_trace(session):
    context.configure_development_features(enable_eager_schema_validation=True)
    try:
        assert not is_tracing_enabled(session)
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


def test_trace_is_freed_with_the_dataframe(session, tracing):
    df = _df(session).filter(col("id") > 0).select("id")
    ref = weakref.ref(df._trace_node)
    parent_ref = weakref.ref(df._trace_node.parents[0])
    del df
    gc.collect()
    assert ref() is None
    assert parent_ref() is None


def test_subquery_dataframe_is_an_input_of_the_call_that_uses_it(session, tracing):
    a = _df(session)
    sub = _df(session).select("id")
    filtered = a.filter(col("id").isin(sub))
    assert sub._trace_node in filtered._trace_node.parents
    assert a._trace_node in filtered._trace_node.parents
    later = filtered.select("id")
    # the subquery belongs to the filter call, not to later operations
    assert later._trace_node.parents == [filtered._trace_node]


def test_udf_registration_is_an_input_of_the_dataframe_applying_it(session, tracing):
    from snowflake.snowpark.functions import udf
    from snowflake.snowpark.types import IntegerType

    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    assert f._trace_node is not None and f._trace_node.kind == "udf"
    df = _df(session).with_column("u", f(col("id")))
    assert f._trace_node in df._trace_node.parents


def test_internal_dataframes_are_not_in_the_plan_lists(session, tracing):
    df = _df(session).describe()
    for plan in (df._plan, df._select_statement):
        if plan is not None and plan.df_trace_nodes:
            assert plan.df_trace_nodes[-1] is df._trace_node
    # the dataframes describe() creates internally (same position as the call) are not listed
    same_call = [n for n in df._plan.df_trace_nodes if n.src == df._trace_node.src]
    assert same_call == [df._trace_node]


def test_calls_after_errors_still_link_their_inputs(session, tracing):
    _df(session).filter(col("id") > 0).collect()
    with pytest.raises(SnowparkSQLException):
        _df(session).select("does_not_exist").collect()
    # a failed call leaves no open call behind: the next call is an outermost call again
    sub = _df(session).select("id")
    filtered = _df(session).filter(col("id").isin(sub))
    assert sub._trace_node in filtered._trace_node.parents
