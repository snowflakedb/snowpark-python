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
    session._large_query_breakdown_complexity_bounds = (100, 200)
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
    assert ex.value.query is not None
    assert not ex.value.query.lstrip().upper().startswith("CREATE")
    debug_context = str(ex.value.debug_context)
    if "corresponds to Python source" not in debug_context:
        return None
    mapping = debug_context.split("corresponds to Python source")[-1]
    return {int(n) for n in _MAPPED_LINE.findall(mapping)}


def _two_branches(session):
    base = session.sql("select 1 as A, 2 as B")
    d1 = base.with_column("A", col("A") + lit(1))
    d2 = base.with_column("B", col("B") + lit(1))
    for i in range(40):
        d1 = d1.with_column("A", col("A") + lit(i))
        d2 = d2.with_column("B", col("B") + lit(i))
        # Real pipeline breakers work without the simplifier too. Keep the
        # tree shallow enough that compilation does not fall back on recursion.
        if i % 10 == 9:
            d1 = d1.sort("A")
            d2 = d2.sort("B")
    union_line = Utils.get_current_line_number_sys() + 1
    return d1.union_all(d2), union_line


@pytest.mark.parametrize("multiline", [False, True])
@pytest.mark.parametrize("large_query_breakdown_enabled", [False, True])
def test_error_in_final_query(session, large_query_breakdown_enabled, multiline):
    (
        session._enable_multiline_queries
        if multiline
        else session._disable_multiline_queries
    )()
    session._large_query_breakdown_enabled = large_query_breakdown_enabled
    u, union_line = _two_branches(session)
    bad_line = Utils.get_current_line_number_sys() + 1
    bad = u.with_column("C", call_function("abs", col("A"), col("A")))
    num_queries = len(bad._plan.execution_queries[PlanQueryType.QUERIES])
    assert (num_queries > 1) == large_query_breakdown_enabled
    lines = _mapped_lines(bad)
    # The final query is compiled in one mode, so its children are rendered
    # verbatim at the lines of their intervals, in single-line mode too.
    # With the simplifier, with_column is a projection of the set statement in
    # the same SELECT, so the union_all trace is reported as well.
    expected = {bad_line, union_line} if session.sql_simplifier_enabled else {bad_line}
    assert lines == expected


@pytest.mark.parametrize("multiline", [False, True])
@pytest.mark.parametrize("literal_newline", [False, True])
def test_error_in_partition(session, multiline, literal_newline):
    # The invalid column is deep in the plan, so it fails in a partition query that runs
    # before the last query.
    if not session.sql_simplifier_enabled:
        # without the simplifier, with_column resolves the schema eagerly and the error
        # is raised while the DataFrame is built, before any partition runs
        pytest.skip("SQL simplifier must be enabled for this test")
    session._large_query_breakdown_enabled = True
    # Tuned to split this shape into exactly two queries; the tests assert that, so a
    # change in complexity scoring fails them loudly instead of testing another shape.
    session._large_query_breakdown_complexity_bounds = (300, 600)
    (
        session._enable_multiline_queries
        if multiline
        else session._disable_multiline_queries
    )()
    # A nonblank literal newline reproduces the line drift without requiring #4514.
    df = (
        session.sql("SELECT 'first\nlast' AS S, 1 AS A, 2 AS B")
        if literal_newline
        else session.sql("SELECT 'first' AS S, 1 AS A, 2 AS B")
    )
    bad_line = Utils.get_current_line_number_sys() + 1
    df = df.filter(col("no_such_col") > 0)
    for i in range(12):
        df = df.with_column("a", col("a") + lit(i)).filter(col("a") > i)
        df = df.union_all(df.select("s", "a", "b"))
    downstream_line = Utils.get_current_line_number_sys() + 1
    df = df.with_column("b", col("b") + lit(1))
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) > 1
    with pytest.raises(SnowparkSQLException) as error:
        df.collect()
    assert error.value.query.lstrip().upper().startswith("CREATE")
    context = str(error.value.debug_context)
    if not multiline:
        # The single-line SQL of the children is not at the lines of their
        # intervals in the partition: no attribution rather than a wrong one.
        assert "corresponds to Python source" not in context
    else:
        mapping = context.split("corresponds to Python source")[-1]
        lines = {int(n) for n in _MAPPED_LINE.findall(mapping)}
        assert bad_line in lines, context
        # The operations after the partition are not reported.
        assert downstream_line not in lines, context


@pytest.mark.parametrize(
    "build_multiline, compile_multiline",
    [(False, True), (True, False), (True, True), (False, False)],
)
def test_cached_layout_toggle(session, build_multiline, compile_multiline):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    session._large_query_breakdown_enabled = True
    session._large_query_breakdown_complexity_bounds = (30, 80)
    (
        session._enable_multiline_queries
        if build_multiline
        else session._disable_multiline_queries
    )()
    base = session.sql("SELECT 1 AS A")
    bad_line = Utils.get_current_line_number_sys() + 1
    df = base.filter(col("bad") > 0)
    for index in range(7):
        df = df.union_all(df.filter(col("A") > index)).sort("A")
    (
        session._enable_multiline_queries
        if compile_multiline
        else session._disable_multiline_queries
    )()
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) == 6
    with pytest.raises(SnowparkSQLException) as error:
        df.collect()
    assert error.value.query.lstrip().upper().startswith("CREATE")
    context = str(error.value.debug_context)
    if build_multiline and compile_multiline:
        mapping = context.split("corresponds to Python source")[-1]
        lines = {int(n) for n in _MAPPED_LINE.findall(mapping)}
        # The source of the filter is in its SELECT; no union/sort is reported.
        assert bad_line in lines and max(lines) == bad_line, context
    else:
        assert "corresponds to Python source" not in context


@pytest.mark.parametrize("late_error", [False, True])
@pytest.mark.parametrize(
    "build_multiline, compile_multiline",
    [(False, True), (True, False), (True, True), (False, False)],
)
def test_union_partition_cached_diagnostics(
    session, late_error, build_multiline, compile_multiline
):
    if not session.sql_simplifier_enabled:
        pytest.skip(
            "Union describes its invalid operand before LQB without the simplifier"
        )
    session._large_query_breakdown_enabled = True
    session._large_query_breakdown_complexity_bounds = (30, 80)
    (
        session._enable_multiline_queries
        if build_multiline
        else session._disable_multiline_queries
    )()
    df = session.sql("SELECT 1 AS A")
    if not late_error:
        bad_line = Utils.get_current_line_number_sys() + 1
        df = df.filter(col("BAD") > 0)
    for index in range(7):
        if late_error and index == 6:
            bad_line = Utils.get_current_line_number_sys() + 1
            df = df.filter(col("BAD") > 0)
        df = df.union_all(df.filter(col("A") > index)).sort("A")
    (
        session._enable_multiline_queries
        if compile_multiline
        else session._disable_multiline_queries
    )()
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) > 1
    if late_error and session.sql_simplifier_enabled:
        assert len(queries) == 5
        assert all('"BAD"' not in query.sql for query in queries[:4])
    with pytest.raises(SnowparkSQLException) as error:
        df.collect()
    context = str(error.value.debug_context)
    if late_error and session.sql_simplifier_enabled:
        assert not error.value.query.lstrip().upper().startswith("CREATE")
    elif not late_error:
        assert error.value.query.lstrip().upper().startswith("CREATE")
    if compile_multiline and (build_multiline or late_error):
        # A late filter is compiled into the last query in multiline mode.
        mapping = context.split("corresponds to Python source")[-1]
        lines = {int(n) for n in _MAPPED_LINE.findall(mapping)}
        # The simplifier merges a filter on the bad filter into its SELECT, and the
        # SELECT has the source of the filter. No union/sort is reported.
        assert bad_line in lines, context
        assert max(lines) == bad_line + late_error, context
    else:
        assert "corresponds to Python source" not in context


def test_nonsimplifier_unary_partition(session):
    if session.sql_simplifier_enabled:
        pytest.skip("Exercises non-simplifier copy intervals")
    session._large_query_breakdown_enabled = True
    session._large_query_breakdown_complexity_bounds = (30, 80)
    df = session.sql("SELECT 1 AS A")
    bad_line = Utils.get_current_line_number_sys() + 1
    df = df.filter(col("BAD") > 0)
    for index in range(12):
        df = df.filter(col("A") > index).sort("A")
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) == 2
    # collect() describes the invalid filter before execution in this mode.
    # Execute the real compiled partition to test its server error coordinates.
    from snowflake.snowpark._internal.debug_utils import (
        get_python_source_from_sql_error,
    )

    with pytest.raises(SnowparkSQLException) as error:
        session.sql(queries[0].sql).collect()
    assert error.value.query.lstrip().upper().startswith("CREATE")
    mapping = get_python_source_from_sql_error(
        df._plan, str(error.value), error.value.query
    )
    assert {int(n) for n in _MAPPED_LINE.findall(mapping)} == {bad_line}


@pytest.mark.parametrize("multiline", [False, True])
def test_sql_is_unchanged_by_the_feature(session, multiline):
    # In single-line mode, removing the UUID marker of a child that is on one line
    # with its parent leaves a new line, for every traced query. The partition
    # CTAS must not add its own marker.
    (
        session._enable_multiline_queries
        if multiline
        else session._disable_multiline_queries
    )()
    session._large_query_breakdown_enabled = True
    on = _two_branches(session)[0]._plan.execution_queries
    session.debug.set(trace_sql_errors_to_dataframe=False)
    off = _two_branches(session)[0]._plan.execution_queries
    assert len(on[PlanQueryType.QUERIES]) > 1
    assert len(off[PlanQueryType.QUERIES]) > 1
    strip_names = re.compile(r"SNOWPARK_TEMP_TABLE_[A-Z0-9]+")
    for kind in (PlanQueryType.QUERIES, PlanQueryType.POST_ACTIONS):
        on_sql = [strip_names.sub("T", q.sql) for q in on[kind]]
        off_sql = [strip_names.sub("T", q.sql) for q in off[kind]]
        if multiline:
            assert on_sql == off_sql
            continue
        assert [sql.replace("\n", "") for sql in on_sql] == off_sql
        for traced, untraced in zip(on_sql, off_sql):
            if traced.lstrip().startswith("CREATE"):
                # The CTAS wrapper and the first byte of its child are identical.
                child_start = untraced.index(" AS  SELECT  *  FROM ( ") + 23
                assert traced[: child_start + 1] == untraced[: child_start + 1]


def _cte_breakdown(session, bad):
    # The shape of test_cte.py::test_large_query_breakdown_with_cte_optimization: a
    # CTE (df1) referenced by the final query and a partition (df3) that is split
    # into a CREATE TABLE AS SELECT, which defines its own CTE (x3).
    from snowflake.snowpark.functions import sum_distinct

    lines = {}
    df0 = session.sql("select 2 as b, 32 as c")
    df1 = session.sql("select 1 as a, 2 as b").filter(col("a") == 1)
    df1 = df1.join(df0, on=["b"], how="inner")
    df2 = df1.filter(col("b") == 2).union_all(df1)
    x3 = session.sql("select 3 as b, 4 as c").with_column("a", col("b") + 1)
    if bad == "partition":
        lines["bad"] = Utils.get_current_line_number_sys() + 1
        x3 = x3.filter(col("no_such_col") > 0)
    df3 = x3.filter(col("b") > 0).union_all(x3)
    lines["loop"] = Utils.get_current_line_number_sys() + 2
    for i in range(7):
        df2 = df2.with_column("a", col("a") + i + col("a"))
        df3 = df3.with_column("b", col("b") + i + col("b"))
    lines["select1"] = Utils.get_current_line_number_sys() + 1
    df2 = df2.select("b", "a")
    if bad == "final":
        lines["bad"] = Utils.get_current_line_number_sys() + 1
        df2 = df2.filter(col("no_such_col") > 0)
    lines["select2"] = Utils.get_current_line_number_sys() + 1
    df2 = df2.select("b", "a")
    df3 = df3.group_by("b").agg(sum_distinct(col("a")).alias("a"))
    df = df2.union_all(df3).filter(col("a") > 2).with_column("a", col("a") + 1)
    return df, lines


@pytest.fixture
def no_resolution_during_diagnosis(monkeypatch):
    """Records plan resolution, SQL generation and queries during SQL error diagnosis."""
    from snowflake.snowpark._internal import server_connection
    from snowflake.snowpark._internal.analyzer import snowflake_plan
    from snowflake.snowpark._internal.analyzer.select_statement import (
        Selectable,
        SelectStatement,
        SetStatement,
    )

    calls = []
    inside = [False]

    def record(name, original, condition=lambda self, *a, **k: True):
        def wrapper(self, *args, **kwargs):
            if inside[0] and condition(self, *args, **kwargs):
                calls.append((name, type(self).__name__))
            return original(self, *args, **kwargs)

        return wrapper

    monkeypatch.setattr(
        Selectable,
        "get_snowflake_plan",
        record(
            "resolve",
            Selectable.get_snowflake_plan,
            lambda self, *a, **k: self._snowflake_plan is None,
        ),
    )
    for cls in (SelectStatement, SetStatement):
        monkeypatch.setattr(
            cls, "_generate_sql", record("_generate_sql", cls._generate_sql)
        )
    for name in ("get_result_attributes", "run_query"):
        original = getattr(server_connection.ServerConnection, name)
        monkeypatch.setattr(
            server_connection.ServerConnection, name, record(name, original)
        )
    diagnose = snowflake_plan.get_python_source_from_sql_error

    def diagnose_wrapper(*args, **kwargs):
        inside[0] = True
        try:
            return diagnose(*args, **kwargs)
        finally:
            inside[0] = False

    monkeypatch.setattr(
        snowflake_plan, "get_python_source_from_sql_error", diagnose_wrapper
    )
    return calls


def _cte_lqb_setup(session, multiline):
    if not session.sql_simplifier_enabled:
        pytest.skip("The invalid filter is described before LQB without the simplifier")
    (
        session._enable_multiline_queries
        if multiline
        else session._disable_multiline_queries
    )()
    session._cte_optimization_enabled = True
    session._large_query_breakdown_enabled = True
    session._large_query_breakdown_complexity_bounds = (300, 600)


@pytest.mark.parametrize("multiline", [False, True])
def test_cte_partition_error_is_not_misattributed(
    session, multiline, no_resolution_during_diagnosis
):
    # The partition CTAS defines a CTE in its own sql, so its lines are not
    # located: it must not be attributed to the partition root (group_by().agg()).
    _cte_lqb_setup(session, multiline)
    df, lines = _cte_breakdown(session, "partition")
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) == 2
    assert queries[0].sql.lstrip().upper().startswith("CREATE")
    assert queries[0].cte_segments is None
    assert "WITH" in queries[0].sql.upper()
    with pytest.raises(SnowparkSQLException) as error:
        df.collect()
    assert error.value.query.lstrip().upper().startswith("CREATE")
    assert "corresponds to Python source" not in str(error.value.debug_context)
    assert no_resolution_during_diagnosis == []


@pytest.mark.parametrize("multiline", [False, True])
def test_cte_final_query_error_reads_cached_sql(
    session, multiline, no_resolution_during_diagnosis
):
    _cte_lqb_setup(session, multiline)
    df, lines = _cte_breakdown(session, "final")
    queries = df._plan.execution_queries[PlanQueryType.QUERIES]
    assert len(queries) == 2
    assert queries[-1].sql.lstrip().upper().startswith("WITH")
    assert queries[-1].cte_segments
    assert all(segment.cached_only for segment in queries[-1].cte_segments)
    mapped = _mapped_lines(df)
    if multiline:
        # The bad filter and the operations of df2 it is merged with.
        assert mapped == {
            lines["loop"],
            lines["select1"],
            lines["bad"],
            lines["select2"],
        }
    else:
        assert mapped is None
    assert no_resolution_during_diagnosis == []


@pytest.mark.parametrize("bad", ["partition", "final"])
@pytest.mark.parametrize("multiline", [False, True])
def test_cte_sql_is_unchanged_by_the_feature(session, multiline, bad):
    _cte_lqb_setup(session, multiline)
    on = _cte_breakdown(session, bad)[0]._plan.execution_queries
    session.debug.set(trace_sql_errors_to_dataframe=False)
    off = _cte_breakdown(session, bad)[0]._plan.execution_queries
    strip_names = re.compile(r"SNOWPARK_TEMP_(TABLE|CTE)_[A-Z0-9]+")
    for kind in (PlanQueryType.QUERIES, PlanQueryType.POST_ACTIONS):
        on_sql = [strip_names.sub("T", q.sql) for q in on[kind]]
        off_sql = [strip_names.sub("T", q.sql) for q in off[kind]]
        if multiline:
            assert on_sql == off_sql
        else:
            assert [sql.replace("\n", "") for sql in on_sql] == off_sql
