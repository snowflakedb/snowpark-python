#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Turning the tracing features on must not change what a successful program does.

Goal B of the lightweight trace test plan: for pipelines that succeed, rows, schema and the
generated SQL are the same with the features off and on, and turning them on and off restores
the session and process state (goal C).
"""

import re

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark import GroupingSets, Window
from snowflake.snowpark._internal.analyzer import analyzer_utils
from snowflake.snowpark._internal.utils import (
    AstFlagSource,
    TempObjectType,
    is_ast_enabled,
    set_ast_state,
)
from snowflake.snowpark.functions import col, parse_json, sum as sum_
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="needs a live connection",
        run=False,
    ),
]

_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_TEMP_NAME = re.compile(r"SNOWPARK_TEMP_(CTE|TABLE|VIEW|FUNCTION)_[A-Z0-9]+")
_WHITESPACE = re.compile(r"\s+")


def _normalize(query, strip_comments, collapse_whitespace):
    """Temporary object names are random per run; the sql error tracing also adds comments and line breaks."""
    query = _TEMP_NAME.sub(r"SNOWPARK_TEMP_\1_X", query)
    if strip_comments:
        query = _COMMENT.sub("", query)
    if collapse_whitespace:
        query = _WHITESPACE.sub("", query)
    return query


def _a(s):
    return s.create_dataframe(
        [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)], schema=["ID", "NAME", "V"]
    )


def _b(s):
    return s.create_dataframe([(1, "x"), (2, "y")], schema=["ID", "TAG"])


def _plus_one():
    from snowflake.snowpark.functions import udf
    from snowflake.snowpark.types import IntegerType

    return udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])


PIPELINES = {
    "select_filter_sort": lambda s, t: _a(s)
    .select("ID", "V")
    .filter(col("ID") > 1)
    .sort("ID"),
    "with_columns_chain": lambda s, t: _a(s)
    .with_column("X", col("ID") + 1)
    .with_column("Y", col("X") * 2)
    .drop("V"),
    "group_agg": lambda s, t: _a(s)
    .group_by("NAME")
    .agg(sum_("V").alias("S"))
    .sort("NAME"),
    "pivot": lambda s, t: _a(s)
    .select("ID", "NAME", "V")
    .pivot("NAME", ["a", "b"])
    .sum("V"),
    "join_using": lambda s, t: _a(s).join(_b(s), on="ID").sort("ID"),
    "join_condition": lambda s, t: _a(s)
    .join(_b(s).select(col("ID").alias("I2"), "TAG"), col("ID") == col("I2"))
    .sort("ID"),
    "self_join_diamond": lambda s, t: (
        lambda base: base.select("ID", "NAME")
        .join(base.select(col("ID").alias("I2"), "V"), col("ID") == col("I2"))
        .sort("ID")
    )(_a(s).filter(col("ID") > 0)),
    "union_family": lambda s, t: _b(s).union(_b(s)).union_all(_b(s)).sort("ID", "TAG"),
    "intersect_except": lambda s, t: _b(s).union_all(_b(s)).except_(_b(s)).sort("ID"),
    "window": lambda s, t: _a(s)
    .with_column("R", sum_("V").over(Window.partition_by("NAME").order_by("ID")))
    .sort("ID"),
    "rollup": lambda s, t: _a(s).rollup("NAME").agg(sum_("V")).sort("NAME"),
    "grouping_sets": lambda s, t: _a(s)
    .group_by_grouping_sets(GroupingSets([col("NAME")], [col("ID")]))
    .agg(sum_("V"))
    .sort("NAME", "ID"),
    "flatten": lambda s, t: s.create_dataframe([("[1,2]",)], schema=["T"])
    .select(parse_json(col("T")).alias("J"))
    .flatten(col("J"))
    .select("VALUE"),
    "sql_source": lambda s, t: s.sql("select 1 as id union all select 2").sort("ID"),
    "table_source": lambda s, t: s.table(t).select("ID").sort("ID"),
    "range_limit": lambda s, t: s.range(10).sort("ID").limit(3, offset=2),
    "large_create_dataframe": lambda s, t: s.create_dataframe(
        [(i, str(i)) for i in range(20000)], schema=["ID", "NAME"]
    )
    .filter(col("ID") < 5)
    .sort("ID"),
    "cache_result": lambda s, t: _a(s).filter(col("ID") > 1).cache_result().sort("ID"),
    "isin_subquery": lambda s, t: _a(s)
    .filter(col("ID").isin(_b(s).select("ID")))
    .sort("ID"),
    "udf_applied": lambda s, t: _a(s)
    .with_column("U", _plus_one()(col("ID")))
    .sort("ID"),
    "describe": lambda s, t: _a(s).describe().sort("SUMMARY"),
    "self_union": lambda s, t: (lambda d: d.union_all(d).sort("ID"))(
        _b(s).filter(col("ID") > 0)
    ),
    "na_and_sample_free": lambda s, t: _a(s).na.fill(0).dropna().distinct().sort("ID"),
}


@pytest.fixture(scope="module")
def table(session):
    name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.sql(f"create temp table {name} (ID int)").collect()
    session.sql(f"insert into {name} values (1), (2)").collect()
    yield name
    session.sql(f"drop table if exists {name}").collect()


def _snapshot(session):
    return {
        "ast_enabled": is_ast_enabled(),
        "session_ast_enabled": session.ast_enabled,
        "auto_clean_up_temp_table_enabled": session.auto_clean_up_temp_table_enabled,
        "multiline": session._generate_multiline_queries,
        "new_line": analyzer_utils.NEW_LINE,
        "tab": analyzer_utils.TAB,
        "simplifier": session.sql_simplifier_enabled,
        "cte": session.cte_optimization_enabled,
        "eager_schema_validation": session.debug.eager_schema_validation,
        "trace_on_error": context._enable_dataframe_trace_on_error,
        "trace_sql_errors": context._enable_trace_sql_errors_to_dataframe,
    }


def _run(session, name, table, queries_without_comments):
    df = PIPELINES[name](session, table)
    queries = [
        _normalize(q, queries_without_comments, queries_without_comments)
        for q in df.queries["queries"]
    ]
    rows = sorted(df.collect(), key=str)
    return rows, [(f.name, str(f.datatype)) for f in df.schema.fields], queries


@pytest.mark.parametrize("name", list(PIPELINES))
@pytest.mark.parametrize("cte", [True, False], ids=["cte", "nocte"])
@pytest.mark.parametrize(
    "simplifier", [True, False], ids=["simplifier", "nosimplifier"]
)
def test_results_schema_and_sql_unchanged(session, table, name, simplifier, cte):
    old = session.sql_simplifier_enabled
    old_cte = session.cte_optimization_enabled
    session.sql_simplifier_enabled = simplifier
    session.cte_optimization_enabled = cte
    try:
        context.configure_development_features()
        before = _run(session, name, table, queries_without_comments=False)

        # the dataframe trace changes nothing in the generated sql
        context.configure_development_features(enable_dataframe_trace_on_error=True)
        trace_on = _run(session, name, table, queries_without_comments=False)
        assert trace_on[0] == before[0]
        assert trace_on[1] == before[1]
        assert trace_on[2] == before[2]

        # the sql error tracing adds comments to the sql, nothing else
        context.configure_development_features(
            enable_dataframe_trace_on_error=True,
            enable_trace_sql_errors_to_dataframe=True,
        )
        sql_trace_on = _run(session, name, table, queries_without_comments=True)
        assert sql_trace_on[0] == before[0]
        assert sql_trace_on[1] == before[1]
        assert sql_trace_on[2] == [_normalize(q, True, True) for q in before[2]]
    finally:
        context.configure_development_features()
        session.sql_simplifier_enabled = old
        session.cte_optimization_enabled = old_cte


def test_session_and_process_state_is_restored(session):
    context.configure_development_features()
    before = _snapshot(session)
    context.configure_development_features(
        enable_dataframe_trace_on_error=True,
        enable_trace_sql_errors_to_dataframe=True,
    )
    during = _snapshot(session)
    # Only the two tracing flags change; the AST and the temp table clean-up are untouched.
    expected = dict(before, trace_on_error=True, trace_sql_errors=True)
    assert during == expected
    context.configure_development_features()
    assert _snapshot(session) == before


def test_features_do_not_enable_ast_or_change_cleaner(session):
    set_ast_state(AstFlagSource.TEST, False)
    session.auto_clean_up_temp_table_enabled = True
    try:
        context.configure_development_features(
            enable_dataframe_trace_on_error=True,
            enable_trace_sql_errors_to_dataframe=True,
        )
        assert not is_ast_enabled()
        assert session.auto_clean_up_temp_table_enabled is True
        context.configure_development_features()
        assert not is_ast_enabled()
        assert session.auto_clean_up_temp_table_enabled is True
    finally:
        session.auto_clean_up_temp_table_enabled = False


def test_dataframes_created_before_enabling_still_work(session, table):
    df_old = _a(session).filter(
        col("ID") > 1
    )  # built with the features off, no trace node
    context.configure_development_features(
        enable_dataframe_trace_on_error=True,
        enable_trace_sql_errors_to_dataframe=True,
    )
    try:
        mixed = df_old.join(_b(session), on="ID").sort("ID")  # old and new plans
        assert [r["ID"] for r in mixed.collect()] == [2]
        with pytest.raises(Exception) as ex:
            mixed.select("DOES_NOT_EXIST").collect()
        assert "DOES_NOT_EXIST" in str(ex.value)
    finally:
        context.configure_development_features()
