#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
import sys

import pytest
import snowflake.snowpark.context as context
from snowflake.snowpark import Row, Session

from snowflake.snowpark._internal.utils import (
    AstFlagSource,
    is_ast_enabled,
    set_ast_state,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    length,
    lit,
    sql_expr,
    sum,
    when,
)
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark._internal.analyzer.snowflake_plan import PlanQueryType
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.window import Window
from tests.utils import IS_IN_STORED_PROC, Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    ),
    pytest.mark.skipif(
        sys.version_info < (3, 11),
        reason="Line numbers are flaky before Python 3.11",
        run=False,
    ),
    pytest.mark.skipif(
        "FIPS_TEST" in os.environ,
        reason="SNOW-2204213: Reading source file location is not correct in FIPS mode",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    # The SQL error tracing does not use the AST collection; keep it off to make sure.
    set_ast_state(AstFlagSource.TEST, False)
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    yield
    assert not is_ast_enabled(), "the debug features must not turn the AST on"
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=False)
    set_ast_state(AstFlagSource.TEST, original)


def test_python_source_location_in_sql_error(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.select(col("a").alias("x"), col("b").alias("y"))
    df2 = df1.select(col("x") + col("a"))
    with pytest.raises(SnowparkSQLException) as ex:
        df2.collect()
    assert "SQL compilation error corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 6}" in str(ex.value.debug_context)


def _mapped_lines(ex) -> list:
    """The lines of this file that the debug context maps the SQL error to."""
    return [
        int(line)
        for line in re.findall(
            re.escape(os.path.basename(__file__)) + r": line (\d+)",
            str(ex.value.debug_context),
        )
    ]


@pytest.fixture(params=[True, False], ids=["multiline", "singleline"])
def multiline(request, session):
    original = session._generate_multiline_queries
    if request.param:
        session._enable_multiline_queries()
    else:
        session._disable_multiline_queries()
    yield request.param
    if original:
        session._enable_multiline_queries()
    else:
        session._disable_multiline_queries()


def test_sql_error_maps_to_operation_of_its_subquery(session, multiline):
    """An operation that wraps the previous ones in a subquery is reported alone, not
    with the operations of the subquery (SNOW-4412764)."""
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    bad = sql_expr("TO_DATE(a, b, a, b)")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.with_column("c", col("a") + 1)
    df = df.with_column("d", bad)
    line = Utils.get_current_line_number_sys() - 1
    df = df.with_column("e", col("a") * 2)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


def test_sql_error_in_case_expression(session, multiline):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    bad = sql_expr("TO_DATE(a, b, a, b)")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.with_column("c", col("a") + 1)
    df = df.with_column("k", when(col("a") > 0, bad).otherwise(lit(None)))
    line = Utils.get_current_line_number_sys() - 1
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


def test_sql_error_in_flattened_operations(session, multiline):
    """Operations that the SQL simplifier flattens into one SELECT share the statement; the
    error is mapped to the one that wrote the part of it with the error (SNOW-2688933).
    """
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    bad = sql_expr("TO_DATE(a, b, a, b)")
    first = Utils.get_current_line_number_sys() + 1
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.select("a", "b")
    df = df.filter(col("a") > 0)
    df = df.filter(bad.is_not_null())
    df = df.sort(col("a"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [first + 3]


def test_sql_error_in_flattened_projection(session, multiline):
    """A column added after a filter that was flattened into the same SELECT (SNOW-2688933)."""
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.filter(col("a") > 0)
    df = df.with_column("c", sql_expr("UPPER(b, a)"))
    line = Utils.get_current_line_number_sys() - 1
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


def test_sql_error_in_flattened_unchanged_column(session, multiline):
    """A column kept by a later select is mapped to the operation that added it."""
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.with_column("c", sql_expr("UPPER(b, a)"))
    line = Utils.get_current_line_number_sys() - 1
    df = df.select("a", "c")
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


def test_sql_error_in_flattened_filters(session, multiline):
    """The filter with the error among the filters flattened into one WHERE clause."""
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.filter(col("a") > 0)
    df = df.filter(sql_expr("UPPER(b, a)") == "x")
    line = Utils.get_current_line_number_sys() - 1
    df = df.filter(col("a") < 9)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


def test_sql_error_in_flattened_sort(session, multiline):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.filter(col("a") > 0)
    df = df.sort(sql_expr("UPPER(b, a)"))
    line = Utils.get_current_line_number_sys() - 1
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


def test_clause_metadata_disabled_with_another_traced_session(session, monkeypatch):
    from snowflake.snowpark._internal import debug_config

    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    session.debug.disable_trace_sql_errors_to_dataframe()
    try:
        with monkeypatch.context() as patch:
            # Model another tracing session without opening another connection.
            patch.setattr(debug_config, "_tracing_sessions", 1)
            df = session.create_dataframe([(1, "a")], schema=["a", "b"])
            projected = df.select("a", "b")
            filtered = projected.filter(col("a") > 0)
            sorted_df = filtered.sort("b")
            for result in (df, projected, filtered, sorted_df):
                statement = result._select_statement
                assert statement._projection_nodes is None
                assert statement._where_nodes is None
                assert statement._order_by_nodes is None
    finally:
        session.debug.enable_trace_sql_errors_to_dataframe()


@pytest.mark.parametrize("text", ["ascii", "\u00e9", "\u4e2d", "\U00010400"])
def test_flattened_projection_mapping_after_unicode(session, multiline, text):
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.with_column("text", lit(text))
    df = df.with_column("bad", sql_expr("UPPER(b, a)"))
    line = Utils.get_current_line_number_sys() - 1
    df = df.with_column("last", lit(2))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


@pytest.mark.parametrize("operation", ["copy", "star", "reorder", "filter", "sort"])
@pytest.mark.parametrize("cache_before", [False, True])
def test_flattened_projection_owner_survives_later_operations(
    session, multiline, operation, cache_before
):
    from copy import copy

    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.with_column("c", sql_expr("UPPER(b, a)"))
    line = Utils.get_current_line_number_sys() - 1
    if cache_before:
        _ = df.queries
    if operation == "copy":
        df = copy(df)
    elif operation == "star":
        df = df.select("*")
    elif operation == "reorder":
        df = df.select("c", "a")
    elif operation == "filter":
        df = df.filter(col("a") > 0)
    else:
        df = df.sort("a")
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


@pytest.mark.parametrize("clause", ["projection", "where", "order_by"])
@pytest.mark.parametrize("trace_during_copy", [False, True])
def test_clause_mapping_after_debug_and_layout_toggle(
    session, multiline, clause, trace_during_copy
):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    if clause == "projection":
        df = df.with_column("c", sql_expr("UPPER(b, a)"))
        line = Utils.get_current_line_number_sys() - 1
    elif clause == "where":
        df = df.filter(sql_expr("UPPER(b, a)") == "x")
        line = Utils.get_current_line_number_sys() - 1
    else:
        df = df.sort(sql_expr("UPPER(b, a)"))
        line = Utils.get_current_line_number_sys() - 1
    _ = df.queries
    if not trace_during_copy:
        session.debug.disable_trace_sql_errors_to_dataframe()
    try:
        df = df.limit(5)
    finally:
        session.debug.enable_trace_sql_errors_to_dataframe()
    if multiline:
        session._disable_multiline_queries()
    else:
        session._enable_multiline_queries()
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    # A plan built while tracing is off has no line intervals. Enabling tracing
    # later must preserve the SQL exception, without inventing a location.
    assert _mapped_lines(ex) == ([line] if trace_during_copy else [])


@pytest.mark.parametrize("operation", ["alias", "star", "bound_star", "quoted_alias"])
def test_filter_owner_survives_projection_changes(session, multiline, operation):
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.filter(sql_expr("UPPER(b, a)") == "x")
    line = Utils.get_current_line_number_sys() - 1
    if operation == "alias":
        df = df.alias("T").select(col("T", "a"))
    elif operation == "star":
        df = df.select("*")
    elif operation == "bound_star":
        df = df.select(df["*"])
    else:
        df = df.select(col("a").alias('"WHERE, FROM"'), "b")
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


@pytest.mark.parametrize(
    "parser_error", [ValueError, TypeError, RecursionError, RuntimeError]
)
def test_clause_parser_failure_preserves_sql_exception(
    session, multiline, monkeypatch, parser_error
):
    from snowflake.snowpark._internal import debug_utils

    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.with_column("bad", sql_expr("UPPER(b, a)"))
    parsed_sql = []

    def broken_parser(sql):
        parsed_sql.append(sql)
        raise parser_error("diagnostic parser failed")

    monkeypatch.setattr(debug_utils, "_clause_tokens", broken_parser)
    with pytest.raises(SnowparkSQLException, match="too many arguments") as ex:
        df.collect()
    assert parsed_sql
    assert ex.value.sql_error_code == 939


def test_reused_filter_predicate_keeps_broader_mapping(session, multiline):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    predicate = sql_expr("UPPER(b, a)") == "x"
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.filter(predicate)
    first = Utils.get_current_line_number_sys() - 1
    df = df.filter(predicate)
    second = Utils.get_current_line_number_sys() - 1
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert {first, second}.issubset(_mapped_lines(ex))


def test_lifted_sort_keeps_original_trace_owner(session, multiline, monkeypatch):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", True)
    monkeypatch.setattr(context, "_snowpark_connect_flatten_select_after_sort", True)
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.sort(col("a"))
    sort_line = Utils.get_current_line_number_sys() - 1
    owner = df._select_statement._order_by_nodes[1][0]
    selected = df.select((col("a") + 1).alias("a"), "b")
    statement = selected._select_statement
    assert statement.order_by is not None
    assert statement._order_by_nodes[1][0] is owner
    assert statement._order_by_nodes[1][0] is not selected._trace_node

    from snowflake.snowpark._internal.debug_utils import (
        get_python_source_from_sql_error,
    )

    # Use the actual generated SQL: the server error is synthetic, not the lifted sort.
    plan = selected._plan
    executed_sql = plan.queries[-1].sql.strip()
    assert executed_sql.upper().count("SELECT") >= 2
    outer_order_by = executed_sql.upper().rindex("ORDER BY")
    offset = executed_sql.index('"A"', outer_order_by)
    sql_line = executed_sql[:offset].count("\n")
    position = offset - executed_sql.rfind("\n", 0, offset) - 1
    message = get_python_source_from_sql_error(
        plan,
        f"SQL compilation error: error line {sql_line + 1} at position {position}\ninvalid identifier 'A'",
        executed_sql,
    )
    assert [
        int(line)
        for line in re.findall(
            re.escape(os.path.basename(__file__)) + r": line (\d+)", message
        )
    ] == [sort_line]


def test_sql_error_of_another_query_reports_all_operations(session, multiline):
    """The schema query run when the join is created has other lines than the sql of the
    plan, so all the operations of the matched plan are reported."""
    bad = sql_expr("TO_DATE(a, b, a, b)")
    first = Utils.get_current_line_number_sys() + 1
    left = session.create_dataframe([(1, "a")], schema=["a", "b"])
    left = left.with_column("x", bad)
    right = session.create_dataframe([(1, "z")], schema=["a", "y"])
    with pytest.raises(SnowparkSQLException) as ex:
        left.join(right, left["a"] == right["a"])
    if session.sql_simplifier_enabled:
        assert _mapped_lines(ex) == [first, first + 1, first + 4]
    else:
        # Without the simplifier the join resolves the schema of ``left`` with the sql
        # of its own plan, so the line maps precisely to the failing operation.
        assert _mapped_lines(ex) == [first + 1]


def test_python_source_location_in_session_sql(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df3 = session.sql(
        "SELECT a, b, c + '5' AS invalid_operation FROM (select 1 as a, 2 as b, array_construct(1, 2, 3) as c) WHERE a > 0"
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df3.show()
    assert "SQL compilation error corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"lines {line_number - 8}-{line_number - 6}" in str(ex.value.debug_context)


def test_join_ambiguous_column_error(session):
    df1 = session.create_dataframe([[1, "a"], [2, "b"]], schema=["id", "value"])
    df2 = session.create_dataframe([[1, "x"], [2, "y"]], schema=["id", "data"])
    joined = df1.join(df2, df1["id"] == df2["id"])
    df_error = joined.select(col("id"))
    with pytest.raises(SnowparkSQLException) as ex:
        df_error.collect()
    assert "SQL compilation error corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 6}" in str(ex.value.debug_context)


def test_window_function_error(session):
    df = session.create_dataframe(
        [[1, 10], [2, 20], [3, 30]], schema=["group_id", "value"]
    )
    window_spec = Window.partition_by("nonexistent_column").order_by("value")
    df_error = df.select(col("value"), sum(col("value")).over(window_spec))
    with pytest.raises(SnowparkSQLException) as ex:
        df_error.collect()
    assert "SQL compilation error corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 6}" in str(ex.value.debug_context)


def test_invalid_identifier_error_message(session):
    # we reuse tests from test_snowflake_plan_suite.py since AST is not enabled in that test suite
    df = session.create_dataframe([[1, 2, 3]], schema=['"abc"', '"abd"', '"def"'])
    with pytest.raises(SnowparkSQLException) as ex:
        df.select("abc").collect()
    assert ex.value.sql_error_code == 904
    assert "invalid identifier 'ABC'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abc\"'?" in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 9}" in str(ex.value)

    with pytest.raises(SnowparkSQLException) as ex:
        df.select("_ab").collect()
    assert "invalid identifier '_AB'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abd\"' or '\"abc\"'?" in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 8}" in str(ex.value)

    with pytest.raises(SnowparkSQLException) as ex:
        df.select('"abC"').collect()
    assert "invalid identifier '\"abC\"'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean" not in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 8}" in str(ex.value)

    df = session.create_dataframe([list(range(20))], schema=[str(i) for i in range(20)])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("20").collect()
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 2}" in str(ex.value)

    df = session.create_dataframe([1, 2, 3], schema=["A"])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("B").schema
    assert "There are existing quoted column identifiers: ['\"A\"']" in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 3}" in str(ex.value)


@pytest.fixture(params=[True, False], ids=["simplifier", "no_simplifier"])
def simplifier(request, session):
    original = session.sql_simplifier_enabled
    session.sql_simplifier_enabled = request.param
    yield
    session.sql_simplifier_enabled = original


def _line_less_error(ex) -> str:
    assert "SQL compilation error:" in str(ex.value)
    assert "error line" not in str(ex.value)
    return str(ex.value)


def _assert_maps_to(df, line):
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    _line_less_error(ex)
    assert _mapped_lines(ex) == [line]


def test_line_less_error_join_using(session, simplifier):
    """A join USING a column that its left side does not have reports no error line: the
    join is reported, not the operations before it that removed or renamed the column.
    """
    lookup = session.create_dataframe([(1, "Animal")], schema=["type", "category"])

    # an unrelated dataframe dropped its own TYPE column
    other = session.create_dataframe([(1, 2)], schema=["ID", "TYPE"]).select("ID")
    base = session.create_dataframe([(1, 3)], schema=["ID", "KIND"]).join(other, "ID")
    line = Utils.get_current_line_number_sys() + 1
    _assert_maps_to(base.join(lookup, on="TYPE"), line)

    # TYPE was aggregated away
    df = session.create_dataframe([(1, 2)], schema=["ID", "TYPE"])
    agg = df.group_by("ID").agg(sum("TYPE").alias("S"))
    line = Utils.get_current_line_number_sys() + 1
    _assert_maps_to(agg.join(lookup, on="TYPE"), line)

    # the join renamed the TYPE columns of both sides
    a = session.create_dataframe([(1, 2)], schema=["ID", "TYPE"])
    b = session.create_dataframe([(1, 3)], schema=["ID", "TYPE"])
    joined = a.join(b, a["ID"] == b["ID"])
    line = Utils.get_current_line_number_sys() + 1
    _assert_maps_to(joined.join(lookup, on="TYPE"), line)


def test_line_less_error_ticket_example(session):
    """The cast removed TYPE. With the SQL simplifier the join USING reports no error line."""
    session.sql_simplifier_enabled, original = True, session.sql_simplifier_enabled
    try:
        df = session.create_dataframe([("cat", "1")], schema=["object", "type"])
        lookup = session.create_dataframe([(1, "Animal")], schema=["type", "category"])
        normalized = df.select("OBJECT", df.TYPE.cast(IntegerType()))
        filtered = normalized.where(col("TYPE") < 3)
        line = Utils.get_current_line_number_sys() + 1
        _assert_maps_to(filtered.join(lookup, on="TYPE"), line)
    finally:
        session.sql_simplifier_enabled = original


@pytest.mark.parametrize("wrap_join", [False, True], ids=["top", "child"])
def test_line_less_error_with_bind_parameters(session, simplifier, wrap_join):
    left = session.sql("select ? as ID", params=[7])
    right = session.sql("select ? as TYPE", params=[7])
    join_line = Utils.get_current_line_number_sys() + 1
    joined = left.join(right, on="TYPE")
    result = joined.select("ID") if wrap_join else joined
    _assert_maps_to(result, join_line)


def test_line_less_error_in_child(session, simplifier):
    """The failing join is wrapped by later operations: only the join is reported."""
    lookup = session.create_dataframe([(1, "Animal")], schema=["type", "category"])
    df = session.create_dataframe([(1, 2)], schema=["ID", "V"])
    join_line = Utils.get_current_line_number_sys() + 1
    joined = df.join(lookup, on="TYPE")
    result = joined.select("ID").filter(col("ID") > 0)
    with pytest.raises(SnowparkSQLException) as ex:
        result.collect()
    _line_less_error(ex)
    assert _mapped_lines(ex) == [join_line]


def test_line_less_error_set_operation(session, simplifier):
    """A set operation whose branches have different numbers of columns reports no error
    line: the union is reported. With the SQL simplifier, the select and filter on top of it
    are part of the same SQL statement and are reported too."""
    a = session.create_dataframe([(1,)], schema=["A"])
    b = session.create_dataframe([(1, 2)], schema=["A", "B"]).filter(col("A") > 0)
    union_line = Utils.get_current_line_number_sys() + 1
    union = a.union_all(b)
    result = union.select("A").filter(col("A") > 0)
    with pytest.raises(SnowparkSQLException) as ex:
        result.collect()
    assert "invalid number of result columns" in _line_less_error(ex)
    expected = (
        [union_line, union_line + 1] if session.sql_simplifier_enabled else [union_line]
    )
    assert _mapped_lines(ex) == expected


def test_line_less_error_while_building(session):
    """The error is raised while the union is built, by the schema query of its first branch:
    no other line than the failing join is reported."""
    lookup = session.create_dataframe([(1, "Animal")], schema=["type", "category"])
    first_line = Utils.get_current_line_number_sys() + 1
    first = session.create_dataframe([(1, 2)], schema=["ID", "V"]).join(
        lookup, on="TYPE"
    )
    second = session.create_dataframe([(3, 4)], schema=["ID", "V"])
    with pytest.raises(SnowparkSQLException) as ex:
        first.union_all(second).collect()
    _line_less_error(ex)
    assert set(_mapped_lines(ex)) <= {first_line}


def test_line_less_error_not_verified(session):
    """A child that needs queries that run before it cannot be compiled on its own: nothing
    is reported, and the lineage trace is still added."""
    lookup = session.create_dataframe([(1, "Animal")], schema=["type", "category"])
    # Large enough to be loaded into a temporary table by queries that run before it.
    large = session.create_dataframe([(i, i) for i in range(1000)], schema=["ID", "V"])
    joined = large.join(lookup, on="TYPE")
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    try:
        with pytest.raises(SnowparkSQLException) as ex:
            joined.collect()
    finally:
        context.configure_development_features(enable_dataframe_trace_on_error=False)
    _line_less_error(ex)
    assert _mapped_lines(ex) == []
    assert "Trace of the most recent dataframe operations" in str(ex.value)


def test_line_less_error_describe_limit(session, monkeypatch):
    from snowflake.snowpark._internal import debug_utils

    monkeypatch.setattr(debug_utils, "_MAX_LINE_LESS_ERROR_DESCRIBES", 0)
    lookup = session.create_dataframe([(1, "Animal")], schema=["type", "category"])
    joined = session.create_dataframe([(1, 2)], schema=["ID", "V"]).join(
        lookup, on="TYPE"
    )
    with pytest.raises(SnowparkSQLException) as ex:
        joined.collect()
    _line_less_error(ex)
    assert ex.value.debug_context in (None, "")


def test_line_less_error_feature_off_issues_no_describe(session):
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=False)
    lookup = session.create_dataframe([(1, "Animal")], schema=["type", "category"])
    joined = session.create_dataframe([(1, 2)], schema=["ID", "V"]).join(
        lookup, on="TYPE"
    )
    with session.query_history(include_describe=True, include_error=True) as history:
        with pytest.raises(SnowparkSQLException) as ex:
            joined.collect()
    _line_less_error(ex)
    assert "Additional Debug Information" not in str(ex.value)
    # only the failing query itself
    assert len(history.queries) == 1


def test_invalid_identifier_in_filter_on_table(session):
    table_name = Utils.random_table_name()
    session.create_dataframe([(1, 2)], schema=["a", "b"]).write.save_as_table(
        table_name, table_type="temp"
    )
    try:
        filter_line = Utils.get_current_line_number_sys() + 1
        df = session.table(table_name).filter(col("c") > 1)
        with pytest.raises(SnowparkSQLException) as ex:
            df.collect()
        assert "invalid identifier 'C'" in str(ex.value)
        assert _mapped_lines(ex) == [filter_line]
    finally:
        Utils.drop_table(session, table_name)


def test_invalid_identifier_select_of_dropped_column(session):
    df = session.create_dataframe([(1, 2)], schema=["a", "b"])
    select_line = Utils.get_current_line_number_sys() + 1
    df = df.drop("b").select("b")
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert "invalid identifier 'B'" in str(ex.value)
    assert _mapped_lines(ex) == [select_line]


def test_missing_table_with_session_table(session):
    with pytest.raises(SnowparkSQLException) as ex:
        session.table("NON_EXISTENT_TABLE").collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 5}" in str(ex.value)


def test_missing_table_context_with_session_sql(session):
    with pytest.raises(SnowparkSQLException) as ex:
        session.sql("SELECT * FROM NON_EXISTENT_TABLE").collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 5}" in str(ex.value.debug_context)


@pytest.mark.parametrize(
    "operation_name,operation_func",
    [
        (
            "select",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .select(col("a")),
        ),
        (
            "filter",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .filter(col("a") > 0),
        ),
        (
            "sort",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .sort(col("a")),
        ),
        (
            "group_by",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .group_by(col("a")),
        ),
        (
            "join",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .join(
                session.create_dataframe([[1, 2]], schema=["x", "y"]),
                col("a") == col("x"),
            ),
        ),
        (
            "union",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .union(session.table("ANOTHER_NON_EXISTENT_TABLE")),
        ),
    ],
)
def test_missing_table_with_dataframe_operations(
    session, operation_name, operation_func
):
    """Test that missing table errors are traced properly across various DataFrame operations."""

    with pytest.raises(SnowparkSQLException) as ex:
        df = operation_func(session)
        df.collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    ), f"Missing object trace not found for operation: {operation_name}"


@pytest.mark.parametrize(
    "operation_name,operation_func",
    [
        # from_ is a SelectableEntity (table)
        ("table_select", lambda t, session: t.select("A")),
        ("table_select_expr", lambda t, session: t.select((col("A") + 1).alias("B"))),
        ("table_select_chain", lambda t, session: t.select("A").select("A")),
        ("table_filter", lambda t, session: t.filter(col("A") > 0)),
        ("table_sort", lambda t, session: t.sort("A")),
        (
            "table_filter_then_select",
            lambda t, session: t.filter(col("A") > 0).select("A"),
        ),
        ("table_with_column", lambda t, session: t.with_column("B", col("A") + 1)),
        ("table_drop", lambda t, session: t.drop("A")),
        # from_ is a SetStatement
        ("union_then_select", lambda t, session: t.union(t).select("A")),
        # from_ is a SelectSnowflakePlan
        (
            "join_then_select",
            lambda t, session: t.join(session.table("NON_EXISTENT_TABLE"), "A").select(
                "A"
            ),
        ),
        ("limit_then_select", lambda t, session: t.limit(1).select("A")),
        # from_ is a SelectSQL
        (
            "sql_select",
            lambda t, session: session.sql("select * from NON_EXISTENT_TABLE").select(
                "A"
            ),
        ),
        (
            "sql_filter",
            lambda t, session: session.sql("select * from NON_EXISTENT_TABLE").filter(
                col("A") > 0
            ),
        ),
    ],
)
def test_missing_table_context_when_error_is_raised_during_construction(
    session, operation_name, operation_func
):
    """The describe that fails while building the dataframe must carry the same debug
    context as an error raised at collect(), whatever the kind of the inner `from_`."""
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    with pytest.raises(SnowparkSQLException) as ex:
        operation_func(session.table("NON_EXISTENT_TABLE"), session)

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    ), f"Missing object context not found for operation: {operation_name}"


def test_missing_table_context_points_to_table_line_when_error_is_raised_during_construction(
    session,
):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    with pytest.raises(SnowparkSQLException) as ex:
        line_number = Utils.get_current_line_number_sys()
        session.table("NON_EXISTENT_TABLE").select("A")

    assert f"line {line_number + 1}" in str(ex.value.debug_context)


def test_existing_table_with_save_as_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([{"a": 1, "b": 2}])
    df.write.save_as_table(table_name, mode="overwrite")
    with pytest.raises(SnowparkSQLException) as ex:
        df.write.save_as_table(table_name)

    assert f"Object '{table_name}' was first referenced" in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 5}" in str(ex.value.debug_context)

    Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "create_sql_template,conflicting_sql_template,object_type",
    [
        (
            "CREATE TABLE {name} (a INT, b INT)",
            "CREATE TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE OR REPLACE TABLE {name} (a INT, b INT)",
            "CREATE TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE TEMP TABLE {name} (a INT, b INT)",
            "CREATE TEMP TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE TEMPORARY TABLE {name} (a INT, b INT)",
            "CREATE TEMPORARY TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE TABLE IF NOT EXISTS {name} (a INT, b INT)",
            "CREATE TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE VIEW {name} AS SELECT 1 AS a, 2 AS b",
            "CREATE VIEW {name} AS SELECT 3 AS a, 4 AS b",
            "VIEW",
        ),
        (
            "CREATE OR REPLACE VIEW {name} AS SELECT 1 AS a, 2 AS b",
            "CREATE VIEW {name} AS SELECT 3 AS a, 4 AS b",
            "VIEW",
        ),
        (
            "CREATE TEMP VIEW {name} AS SELECT 1 AS a, 2 AS b",
            "CREATE TEMP VIEW {name} AS SELECT 3 AS a, 4 AS b",
            "VIEW",
        ),
        (
            "CREATE VIEW IF NOT EXISTS {name} AS SELECT 1 AS a, 2 AS b",
            "CREATE VIEW {name} AS SELECT 3 AS a, 4 AS b",
            "VIEW",
        ),
        # Quoted table names
        (
            'CREATE TABLE "{name}" (a INT, b INT)',
            'CREATE TABLE "{name}" (a INT, b INT)',
            "TABLE",
        ),
        # Mixed quoted/unquoted (should still match)
        (
            'CREATE TABLE "{name}" (a INT, b INT)',
            "CREATE TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
    ],
)
def test_existing_object_with_session_sql_create_statements(
    session, create_sql_template, conflicting_sql_template, object_type
):
    """Test that get_existing_object_context properly identifies objects created via session.sql() with various CREATE statement patterns."""
    if object_type == "TABLE":
        object_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    else:
        object_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)

    session.sql(create_sql_template.format(name=object_name)).collect()
    # Second CREATE statement should fail with "already exists" error
    with pytest.raises(SnowparkSQLException) as ex:
        session.sql(conflicting_sql_template.format(name=object_name)).collect()

    expected_message = f"Object '{object_name}' was first referenced"
    assert expected_message in str(
        ex.value.debug_context
    ), f"Expected message '{expected_message}' not found in debug context: {ex.value.debug_context}"
    if object_type == "TABLE":
        Utils.drop_table(session, object_name)
    else:
        Utils.drop_view(session, object_name)


def test_existing_object_with_schema_qualified_names(session):
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    db = session.get_current_database()
    sc = session.get_current_schema()
    df = session.create_dataframe([{"a": 1, "b": 2}])
    df.write.save_as_table([db, sc, temp_table_name], mode="overwrite")
    df2 = session.create_dataframe([{"a": 3, "b": 4}])
    with pytest.raises(SnowparkSQLException) as ex:
        df2.write.save_as_table([db, sc, temp_table_name], mode="errorifexists")

    db = db.strip('"')
    sc = sc.strip('"')
    expected_message = f"Object '{db}.{sc}.{temp_table_name}' was first referenced"
    assert expected_message in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 9}" in str(ex.value.debug_context)
    Utils.drop_table(session, temp_table_name)


def test_existing_object_with_schema_qualified_names_using_session_sql(session):
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    db = session.get_current_database()
    sc = session.get_current_schema()
    session.sql(f"CREATE TABLE {db}.{sc}.{temp_table_name} (a INT, b INT)").collect()

    with pytest.raises(SnowparkSQLException) as ex:
        session.sql(
            f"CREATE TABLE {db}.{sc}.{temp_table_name} (a INT, b INT)"
        ).collect()

    db = db.strip('"')
    sc = sc.strip('"')
    expected_message = f"Object '{db}.{sc}.{temp_table_name}' was first referenced"
    assert expected_message in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 11}" in str(ex.value.debug_context)
    Utils.drop_table(session, temp_table_name)


def test_existing_view_with_schema_qualified_names_using_session_sql(session):
    temp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    db = session.get_current_database()
    sc = session.get_current_schema()
    session.sql(
        f"CREATE VIEW {db}.{sc}.{temp_view_name} AS SELECT 1 AS a, 2 AS b"
    ).collect()

    with pytest.raises(SnowparkSQLException) as ex:
        session.sql(
            f"CREATE VIEW {db}.{sc}.{temp_view_name} AS SELECT 3 AS a, 4 AS b"
        ).collect()

    db = db.strip('"')
    sc = sc.strip('"')
    expected_message = f"Object '{db}.{sc}.{temp_view_name}' was first referenced"
    assert expected_message in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"lines {line_number - 13}-{line_number - 11}" in str(ex.value.debug_context)
    Utils.drop_view(session, temp_view_name)


def test_existing_view_with_schema_qualified_names_using_dataframe_methods(session):
    temp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    db = session.get_current_database()
    sc = session.get_current_schema()
    df = session.create_dataframe([{"a": 1, "b": 2}])
    df.create_temp_view([db, sc, temp_view_name])
    df2 = session.create_dataframe([{"a": 3, "b": 4}])
    with pytest.raises(SnowparkSQLException) as ex:
        df2.create_temp_view([db, sc, temp_view_name])

    db = db.strip('"')
    sc = sc.strip('"')
    expected_message = f"Object '{db}.{sc}.{temp_view_name}' was first referenced"
    assert expected_message in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 9}" in str(ex.value.debug_context)
    Utils.drop_view(session, temp_view_name)


def test_existing_table_with_dataframe_write_operations(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df1 = session.create_dataframe([{"a": 1, "b": 2}])
    df1.write.save_as_table(table_name, mode="overwrite")
    df2 = session.create_dataframe([{"a": 3, "b": 4}])
    with pytest.raises(SnowparkSQLException) as ex:
        df2.write.save_as_table(table_name, mode="errorifexists")

    assert f"Object '{table_name}' was first referenced" in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 6}" in str(ex.value.debug_context)
    Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "view_method",
    [
        "create_temp_view",
        "create_or_replace_view",
        "create_or_replace_temp_view",
    ],
)
def test_existing_view_with_all_dataframe_methods(session, view_method):
    """Test that get_existing_object_context works with all DataFrame view creation methods."""
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    df1 = session.create_dataframe([{"a": 1, "b": 2}])
    df2 = session.create_dataframe([{"a": 3, "b": 4}])
    getattr(df1, view_method)(view_name)

    if view_method == "create_or_replace_view":
        with pytest.raises(SnowparkSQLException) as ex:
            session.sql(f"CREATE VIEW {view_name} AS SELECT 5 AS a, 6 AS b").collect()
    elif view_method == "create_or_replace_temp_view":
        with pytest.raises(SnowparkSQLException) as ex:
            session.sql(
                f"CREATE TEMP VIEW {view_name} AS SELECT 5 AS a, 6 AS b"
            ).collect()
    else:
        with pytest.raises(SnowparkSQLException) as ex:
            getattr(df2, view_method)(view_name)

    assert f"Object '{view_name}' was first referenced" in str(
        ex.value.debug_context
    ), f"Expected object reference not found in debug context for {view_method}: {ex.value.debug_context}"

    Utils.drop_view(session, view_name)


def _create_temp_table_sql(column_lines):
    """A multi-line non-select statement, so session.sql() runs it as a pre-action."""
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    return f"create or replace temp table {table_name} as\nselect\n" + ",\n".join(
        column_lines
    )


# Fails at SQL line 10, after the last line of the main query that RESULT_SCANs it.
_LONG_BAD_SQL_COLUMNS = [f"{i} as c{i}" for i in range(7)] + ["no_such_col as bad"]


def _pre_action_beyond_main_query_lines(session):
    sql = _create_temp_table_sql(_LONG_BAD_SQL_COLUMNS)
    sql_line = Utils.get_current_line_number_sys() + 1
    df = session.sql(sql)
    return df.filter(col("c0") > 1), [sql_line], []


def _pre_action_within_main_query_lines(session):
    # Fails at SQL line 5, which also exists in the main query.
    sql = _create_temp_table_sql(["1 as a", "2 as b", "no_such_col as bad"])
    sql_line = Utils.get_current_line_number_sys() + 1
    df = session.sql(sql)
    return df.filter(col("a") > 1), [sql_line], []


def _second_of_two_pre_actions(session):
    sql_ok = _create_temp_table_sql(["1 as a"])
    sql_bad = _create_temp_table_sql(["1 as b", "no_such_col as bad"])
    ok_line = Utils.get_current_line_number_sys() + 1
    df_ok = session.sql(sql_ok)
    bad_line = Utils.get_current_line_number_sys() + 1
    df_bad = session.sql(sql_bad)
    df = df_ok.cross_join(df_bad)
    assert len(df._plan.queries) == 3
    return df, [bad_line], [ok_line]


def _pre_action_repeated_by_cte(session):
    sql = _create_temp_table_sql(_LONG_BAD_SQL_COLUMNS)
    sql_line = Utils.get_current_line_number_sys() + 1
    df = session.sql(sql).filter(col("c0") > 1)
    return df.union_all(df), [sql_line], []


def _main_query_after_session_sql_pre_action(session):
    df = session.sql(_create_temp_table_sql(["1 as a"]))
    filter_line = Utils.get_current_line_number_sys() + 1
    df = df.filter(col("no_such_col") > 1)
    return df, [filter_line], []


def _main_query_after_large_local_data_pre_actions(session):
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
    select_line = Utils.get_current_line_number_sys() + 1
    df = df.select(col("a"), col("no_such_col"))
    assert len(df._plan.queries) > 1
    return df, [select_line], []


def _large_query_breakdown_partition(session):
    # The invalid column is deep in the plan, so it fails in a partition query that
    # runs before the last query. The partition is not owned by a node of the original
    # plan, so no source location is reported rather than a wrong one.
    if not session.sql_simplifier_enabled:
        # without the simplifier, with_column resolves the schema eagerly and the
        # error is raised while the DataFrame is built, before any partition runs
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df = df.filter(col("no_such_col") > 0)
    for i in range(12):
        df = df.with_column("a", col("a") + lit(i)).filter(col("a") > i)
        df = df.union_all(df.select("a", "b"))
    assert len(df._plan.execution_queries[PlanQueryType.QUERIES]) > 1
    return df, None, []


@pytest.mark.parametrize(
    "build_df, session_settings",
    [
        (_pre_action_beyond_main_query_lines, {}),
        (_pre_action_beyond_main_query_lines, {"sql_simplifier_enabled": False}),
        (_pre_action_within_main_query_lines, {}),
        (_second_of_two_pre_actions, {}),
        (_pre_action_repeated_by_cte, {"cte_optimization_enabled": True}),
        (_main_query_after_session_sql_pre_action, {}),
        (_main_query_after_large_local_data_pre_actions, {}),
        (
            _large_query_breakdown_partition,
            {
                # with CTE optimization the plan is compiled into one query instead
                "cte_optimization_enabled": False,
                "large_query_breakdown_enabled": True,
                "_large_query_breakdown_complexity_bounds": (300, 600),
            },
        ),
    ],
)
def test_sql_error_in_query_before_or_after_pre_actions(
    session, build_df, session_settings
):
    # SNOW-4412768: an error raised by a query that runs before the last query of the
    # plan maps to the DataFrame operation that produced it.
    from snowflake.snowpark._internal.analyzer import analyzer

    original_settings = {name: getattr(session, name) for name in session_settings}
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        for name, value in session_settings.items():
            setattr(session, name, value)
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df, expected_lines, unexpected_lines = build_df(session)
        with pytest.raises(SnowparkSQLException) as ex:
            df.collect()
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold
        for name, value in original_settings.items():
            setattr(session, name, value)
    debug_context = str(ex.value.debug_context)
    if expected_lines is None:
        assert "SQL compilation error corresponds to Python source" not in debug_context
        return
    assert "SQL compilation error corresponds to Python source" in debug_context
    for line in expected_lines:
        assert f"line {line}" in debug_context
    for line in unexpected_lines:
        assert f"line {line}" not in debug_context


def test_pre_action_owner_carries_session_sql_trace_node(session):
    # The source location of a pre-action error comes from the trace nodes of the plan
    # that owns the pre-action, which must include the session.sql() call.
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    from snowflake.snowpark._internal.utils import get_plan_from_pre_query_line_number

    df = session.sql(_create_temp_table_sql(["1 as a"]))
    filtered = df.filter(col("a") > 1)
    owner = get_plan_from_pre_query_line_number(
        filtered._plan, filtered._plan.queries[0], 1
    )
    assert any(node is df._trace_node for node in owner.df_trace_nodes)


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_sql_error_tracing_is_per_session(session, db_parameters):
    def build(s):
        df = s.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        return df.select(col("a").alias("x")).select(col("x") + col("b"))

    with Session.builder.configs(db_parameters).create() as other:
        other.sql_simplifier_enabled = session.sql_simplifier_enabled
        other.debug.disable_trace_sql_errors_to_dataframe()
        assert session.debug.trace_sql_errors_to_dataframe is True
        on, off = build(session), build(other)
        # the session with the feature off sends the same SQL and records no line intervals
        assert off.queries == on.queries
        assert not any(q.query_line_intervals for q in off._plan.queries)
        with pytest.raises(SnowparkSQLException) as ex_on:
            on.collect()
        with pytest.raises(SnowparkSQLException) as ex_off:
            off.collect()
    assert "SQL compilation error corresponds to Python source" in str(
        ex_on.value.debug_context
    )
    assert "SQL compilation error corresponds to Python source" not in str(
        ex_off.value.debug_context or ""
    )


def test_empty_lines_in_literals_are_kept(session, multiline):
    """The sql sent with the feature on keeps the empty lines of string literals."""

    def run():
        df = session.sql("select 'a\n\nb' as s").select(
            "s", length(col("s")).alias("n")
        )
        return df.collect(), df.queries["queries"][-1]

    rows_on, sql_on = run()
    session.debug.disable_trace_sql_errors_to_dataframe()
    try:
        rows_off, sql_off = run()
    finally:
        session.debug.enable_trace_sql_errors_to_dataframe()
    assert rows_on == rows_off == [Row(S="a\n\nb", N=4)]
    if multiline:
        assert sql_on == sql_off
