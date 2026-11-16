#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""SNOW-4412736: SQL compilation errors of the queries large query breakdown splits a plan into
map to the DataFrame operations of the query that failed."""

import re

import pytest

from snowflake.snowpark._internal.analyzer.snowflake_plan import PlanQueryType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import call_function, col, lit
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SQL error tracing is not supported in local testing mode",
        run=False,
    ),
]

_MAPPED_LINE = re.compile(r"line[s]? (\d+)")


@pytest.fixture(autouse=True)
def setup(session):
    old = (
        session._generate_multiline_queries,
        session._query_compilation_stage_enabled,
        session._cte_optimization_enabled,
        session._large_query_breakdown_enabled,
        session._large_query_breakdown_complexity_bounds,
    )
    session._enable_multiline_queries()
    session._query_compilation_stage_enabled = True
    session._cte_optimization_enabled = False
    session._large_query_breakdown_complexity_bounds = (300, 600)
    session.debug.set(trace_sql_errors_to_dataframe=True)
    yield
    (
        session._enable_multiline_queries
        if old[0]
        else session._disable_multiline_queries
    )()
    session._query_compilation_stage_enabled = old[1]
    session._cte_optimization_enabled = old[2]
    session._large_query_breakdown_enabled = old[3]
    session._large_query_breakdown_complexity_bounds = old[4]


def _mapped_lines(df):
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    debug_context = str(ex.value.debug_context)
    if "corresponds to Python source" not in debug_context:
        return None
    mapping = debug_context.split("corresponds to Python source")[-1]
    return {int(n) for n in _MAPPED_LINE.findall(mapping)}


def _two_branches(session):
    base = session.sql("select 1 as A, 2 as B")
    d1 = base.with_column("A", col("A") + lit(1))
    d2 = base.with_column("B", col("B") + lit(1))
    for i in range(160):
        d1 = d1.with_column("A", col("A") + lit(i))
        d2 = d2.with_column("B", col("B") + lit(i))
    return d1.union_all(d2)


@pytest.mark.parametrize("large_query_breakdown_enabled", [False, True])
def test_error_in_final_query(session, large_query_breakdown_enabled):
    session._large_query_breakdown_enabled = large_query_breakdown_enabled
    u = _two_branches(session)
    bad_line = Utils.get_current_line_number_sys() + 1
    bad = u.with_column("C", call_function("abs", col("A"), col("A")))
    num_queries = len(bad._plan.execution_queries[PlanQueryType.QUERIES])
    assert (num_queries > 1) == large_query_breakdown_enabled
    lines = _mapped_lines(bad)
    assert lines is not None and bad_line in lines


def test_error_in_partition(session):
    # The invalid column is deep in the plan, so it fails in a partition query that runs
    # before the last query.
    if not session.sql_simplifier_enabled:
        # without the simplifier, with_column resolves the schema eagerly and the error
        # is raised while the DataFrame is built, before any partition runs
        pytest.skip("SQL simplifier must be enabled for this test")
    session._large_query_breakdown_enabled = True
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    bad_line = Utils.get_current_line_number_sys() + 1
    df = df.filter(col("no_such_col") > 0)
    for i in range(12):
        df = df.with_column("a", col("a") + lit(i)).filter(col("a") > i)
        df = df.union_all(df.select("a", "b"))
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) > 1
    lines = _mapped_lines(df)
    assert lines is not None and bad_line in lines
    # the operations after the partition are not reported
    assert bad_line + 3 not in lines


def test_sql_is_unchanged_by_the_feature(session):
    session._large_query_breakdown_enabled = True
    on = _two_branches(session)._plan.execution_queries
    session.debug.set(trace_sql_errors_to_dataframe=False)
    off = _two_branches(session)._plan.execution_queries
    strip_names = re.compile(r"SNOWPARK_TEMP_TABLE_[A-Z0-9]+")
    for kind in (PlanQueryType.QUERIES, PlanQueryType.POST_ACTIONS):
        assert [strip_names.sub("T", q.sql) for q in on[kind]] == [
            strip_names.sub("T", q.sql) for q in off[kind]
        ]
