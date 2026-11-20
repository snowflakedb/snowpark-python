#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from copy import copy, deepcopy
from unittest import mock
import pytest

from snowflake.snowpark._internal.analyzer.select_statement import (
    SelectableEntity,
    SelectSQL,
    SelectStatement,
    SelectTableFunction,
    SelectSnowflakePlan,
    SetOperand,
    SetStatement,
    ColumnChangeState,
    ColumnState,
    ColumnStateDict,
    _check_expressions_for_types,
    can_clause_dependent_columns_flatten,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SnowflakeTable
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    COLUMN_DEPENDENCY_ALL,
    COLUMN_DEPENDENCY_DOLLAR,
)
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan, Query
from snowflake.snowpark.types import StringType
from snowflake.snowpark._internal.df_trace import SrcPosition, TraceNode
from snowflake.snowpark._internal.debug_utils import _select_clause_spans
from tests.utils import Utils
import snowflake.snowpark.context as context
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    yield
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=False)
    set_ast_state(AstFlagSource.TEST, original)


@pytest.fixture
def traced_statement(mock_session, mock_analyzer):
    source = SelectableEntity(
        SnowflakeTable("BASE_TABLE", session=mock_session), analyzer=mock_analyzer
    )
    with mock.patch(
        "snowflake.snowpark._internal.analyzer.select_statement.tracing_flags_on",
        return_value=True,
    ), mock.patch(
        "snowflake.snowpark._internal.analyzer.select_statement.is_tracing_enabled",
        return_value=True,
    ), mock.patch.object(
        mock_analyzer, "analyze", side_effect=lambda expr, *_: expr.name
    ):
        yield SelectStatement(
            projection=[Attribute("A", StringType())],
            from_=source,
            where=Attribute("PREDICATE", StringType()),
            order_by=[Attribute("SORT_KEY", StringType())],
            analyzer=mock_analyzer,
            schema_query="SELECT A FROM BASE_TABLE",
        )


def test_clause_token_binds_once_and_excludes_unknown_slots(traced_statement):
    statement = traced_statement
    token = statement._pending_trace_node
    assert statement._projection_nodes[1] == [token]
    assert statement._where_nodes[1][0][1] is token
    assert statement._order_by_nodes[1] == [token]
    original_sql = statement.sql_query
    assert _select_clause_spans(statement)[0] == []
    first = TraceNode(SrcPosition("app.py", 4, 0, 4, 10))
    statement.add_df_trace_node(first)
    statement.add_df_trace_node(TraceNode(SrcPosition("app.py", 8, 0, 8, 10)))
    spans = _select_clause_spans(statement)[0]
    assert len(spans) == 3
    assert all(node is first for _, _, node in spans)
    assert statement.sql_query == original_sql

    # None means unknown, not an unbound slot owned by this statement.
    statement._projection_nodes = (statement.projection, [None])
    statement.add_df_trace_node(first)
    assert statement._projection_nodes[1] == [None]
    assert len(_select_clause_spans(statement)[0]) == 2


@pytest.mark.parametrize("copy_statement", [copy, deepcopy], ids=["copy", "deepcopy"])
@pytest.mark.parametrize("bound", [False, True], ids=["unbound", "bound"])
def test_clause_token_copy_preserves_ownership(traced_statement, copy_statement, bound):
    original = traced_statement
    node = TraceNode(SrcPosition("app.py", 4, 0, 4, 10))
    if bound:
        original.add_df_trace_node(node)
    copied = copy_statement(original)
    assert copied._pending_trace_node is not original._pending_trace_node
    copied.add_df_trace_node(TraceNode(SrcPosition("app.py", 8, 0, 8, 10)))
    inherited = node if bound else original._pending_trace_node
    assert copied._projection_nodes[1][0] is inherited
    assert copied._where_nodes[1][0][1] is inherited
    assert copied._order_by_nodes[1][0] is inherited
    assert original._projection_nodes[1][0] is inherited
    assert copied.sql_query == original.sql_query
    spans = _select_clause_spans(copied)[0]
    assert len(spans) == (3 if bound else 0)
    assert all(owner is node for _, _, owner in spans)
    assert copied.sql_query == original.sql_query

    # A clause introduced on the copy binds to the copy, not to the original.
    copied._order_by_nodes = (copied.order_by, [copied._pending_trace_node])
    new_node = TraceNode(SrcPosition("app.py", 9, 0, 9, 10))
    copied.add_df_trace_node(new_node)
    assert copied._order_by_nodes[1][0] is new_node
    assert original._order_by_nodes[1][0] is inherited


@pytest.mark.parametrize("other_session_tracing", [False, True])
def test_clause_metadata_is_disabled_for_untraced_session(
    mock_session, mock_analyzer, other_session_tracing
):
    from snowflake.snowpark._internal import debug_config
    from snowflake.snowpark._internal.df_trace import is_tracing_enabled

    source = SelectableEntity(
        SnowflakeTable("BASE_TABLE", session=mock_session), analyzer=mock_analyzer
    )
    # An unrelated session may be tracing; this session has no debug features enabled.
    with mock.patch.object(
        debug_config, "_tracing_sessions", int(other_session_tracing)
    ), mock.patch(
        "snowflake.snowpark._internal.analyzer.select_statement.is_tracing_enabled",
        wraps=is_tracing_enabled,
    ) as tracing_enabled:
        statement = SelectStatement(
            projection=[Attribute("A", StringType())],
            from_=source,
            where=Attribute("PREDICATE", StringType()),
            order_by=[Attribute("SORT_KEY", StringType())],
            analyzer=mock_analyzer,
        )
    assert statement._projection_nodes is None
    assert statement._where_nodes is None
    assert statement._order_by_nodes is None
    assert tracing_enabled.call_count == int(other_session_tracing)


def test_select_statement_sql_query(mock_session, mock_analyzer):
    mock_from = mock.create_autospec(SelectableEntity)
    mock_from.sql_query = "SELECT * FROM BASE_TABLE"
    mock_from.uuid = "test-uuid-123"
    mock_from.analyzer = mock_analyzer
    mock_from.query_params = None
    mock_from.pre_actions = None
    mock_from.post_actions = None
    mock_from.expr_to_alias = {}
    mock_from.df_aliased_col_name_to_real_col_name = {}
    mock_from.df_ast_ids = None
    mock_from.df_trace_nodes = None

    select_statement = SelectStatement(
        from_=mock_from,
        analyzer=mock_analyzer,
    )
    assert (
        select_statement.commented_sql
        == f"\n-- {mock_from.uuid}\nSELECT * FROM BASE_TABLE\n-- {mock_from.uuid}\n"
    )
    assert select_statement.sql_query == "SELECT * FROM BASE_TABLE"


def test_select_statement_sql_query_with_projection(mock_session, mock_analyzer):
    mock_from = mock.create_autospec(SelectableEntity)
    mock_from.sql_query = "SELECT * FROM BASE_TABLE"
    mock_from.uuid = "test-uuid-789"
    mock_from.analyzer = mock_analyzer
    mock_from.query_params = None
    mock_from.sql_in_subquery = "(SELECT * FROM BASE_TABLE)"
    mock_from.sql_in_subquery_with_uuid = (
        f"\n-- {mock_from.uuid}\nSELECT * FROM BASE_TABLE\n-- {mock_from.uuid}"
    )
    mock_from.pre_actions = None
    mock_from.post_actions = None
    mock_from.expr_to_alias = {}
    mock_from.df_aliased_col_name_to_real_col_name = {}
    mock_from.df_ast_ids = None
    mock_from.df_trace_nodes = None

    def mock_analyze(expr, df_alias_map, parse_local_name=False):
        if hasattr(expr, "name"):
            return expr.name
        return "A"

    mock_analyzer.analyze.side_effect = mock_analyze

    select_statement = SelectStatement(
        projection=[Attribute("A", StringType())],
        from_=mock_from,
        analyzer=mock_analyzer,
    )
    assert Utils.normalize_sql(select_statement.commented_sql) == Utils.normalize_sql(
        f"SELECT A FROM -- {mock_from.uuid}\nSELECT * FROM BASE_TABLE\n-- {mock_from.uuid}"
    )
    assert Utils.normalize_sql(select_statement.sql_query) == Utils.normalize_sql(
        "SELECT A FROM (SELECT * FROM BASE_TABLE)"
    )


def test_selectable_entity_sql_query(mock_session, mock_analyzer):
    entity = SnowflakeTable("TEST_TABLE", session=mock_session)
    selectable_entity = SelectableEntity(entity, analyzer=mock_analyzer)

    expected = "SELECT * FROM TEST_TABLE"
    assert Utils.normalize_sql(selectable_entity.sql_query) == Utils.normalize_sql(
        expected
    )
    assert Utils.normalize_sql(selectable_entity.commented_sql) == Utils.normalize_sql(
        expected
    )


def test_select_sql_sql_query(mock_session, mock_analyzer):
    sql = "SELECT 1 AS A, 2 AS B"
    select_sql = SelectSQL(sql, analyzer=mock_analyzer)

    assert select_sql.sql_query == sql
    assert select_sql.commented_sql == sql


def test_set_statement_sql_query_no_multiline(mock_session, mock_analyzer):
    mock_selectable1 = mock.create_autospec(SelectableEntity)
    mock_selectable1.sql_query = "SELECT 1 AS A"
    mock_selectable1.uuid = "uuid-1"
    mock_selectable1.pre_actions = None
    mock_selectable1.post_actions = None

    mock_selectable2 = mock.create_autospec(SelectableEntity)
    mock_selectable2.sql_query = "SELECT 2 AS A"
    mock_selectable2.uuid = "uuid-2"
    mock_selectable2.pre_actions = None
    mock_selectable2.post_actions = None

    operand1 = SetOperand(mock_selectable1, "UNION")
    operand2 = SetOperand(mock_selectable2, "UNION")
    set_statement = SetStatement(operand1, operand2, analyzer=mock_analyzer)
    assert Utils.normalize_sql(set_statement.commented_sql) == Utils.normalize_sql(
        "(-- uuid-1 SELECT 1 AS A -- uuid-1)UNION(-- uuid-2 SELECT 2 AS A -- uuid-2)"
    )
    assert Utils.normalize_sql(set_statement.sql_query) == Utils.normalize_sql(
        "(SELECT 1 AS A)UNION(SELECT 2 AS A)"
    )


def test_select_table_function_commented_sql(mock_session, mock_analyzer):
    mock_snowflake_plan = mock.create_autospec(SnowflakePlan)
    mock_query = mock.create_autospec(Query)
    mock_query.sql = "SELECT * FROM TABLE(test_func())"
    mock_snowflake_plan.queries = [mock_query]
    mock_snowflake_plan.post_actions = None
    mock_snowflake_plan.api_calls = []
    mock_snowflake_plan.uuid = "table-func-uuid-456"
    func_expr = TableFunctionExpression("test_func")
    select_table_function = SelectTableFunction(
        func_expr=func_expr,
        snowflake_plan=mock_snowflake_plan,
        analyzer=mock_analyzer,
    )

    expected_sql = "SELECT * FROM TABLE(test_func())"
    assert select_table_function.sql_query == expected_sql
    assert select_table_function.commented_sql == expected_sql


def test_select_snowflake_plan_commented_sql(mock_session, mock_analyzer):
    mock_snowflake_plan = mock.create_autospec(SnowflakePlan)
    mock_query = mock.create_autospec(Query)
    mock_query.sql = "SELECT A, B FROM test_table WHERE A > 10"
    mock_query.params = None
    mock_snowflake_plan.queries = [mock_query]
    mock_snowflake_plan.post_actions = None
    mock_snowflake_plan.api_calls = []
    mock_snowflake_plan.uuid = "snowflake-plan-uuid-789"
    mock_snowflake_plan.schema_query = "SELECT A, B FROM test_table WHERE A > 10"
    mock_snowflake_plan.expr_to_alias = {}
    mock_snowflake_plan.df_aliased_col_name_to_real_col_name = {}
    mock_snowflake_plan.df_ast_ids = None
    mock_snowflake_plan.df_trace_nodes = None
    select_snowflake_plan = SelectSnowflakePlan(
        snowflake_plan=mock_snowflake_plan,
        analyzer=mock_analyzer,
    )

    expected_sql = "SELECT A, B FROM test_table WHERE A > 10"
    assert select_snowflake_plan.sql_query == expected_sql
    assert select_snowflake_plan.commented_sql == expected_sql


def test_check_expressions_for_types_skips_none_expressions():
    """_check_expressions_for_types should skip None entries in the list."""
    result = _check_expressions_for_types(
        [None, None], check_window=True, check_data_gen=True, check_aggregation=True
    )
    assert result is False


def test_check_expressions_for_types_skips_none_among_real_expressions():
    """_check_expressions_for_types should skip None entries interspersed with real expressions."""
    expr = Attribute('"A"', StringType())
    result = _check_expressions_for_types([None, expr, None], check_window=True)
    assert result is False


@pytest.mark.parametrize(
    "dep_columns",
    [COLUMN_DEPENDENCY_DOLLAR, COLUMN_DEPENDENCY_ALL],
)
def test_can_clause_dependent_columns_flatten_sort_new_dollar_or_all(
    dep_columns, monkeypatch
):
    """Sort on a NEW column with DOLLAR or ALL dependencies must not flatten."""
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", True)
    col_states = ColumnStateDict()
    state = ColumnState(
        col_name='"X"',
        change_state=ColumnChangeState.NEW,
        state_dict=col_states,
    )
    state.dependent_columns = dep_columns
    col_states['"X"'] = state

    result = can_clause_dependent_columns_flatten(
        frozenset({'"X"'}), col_states, "sort"
    )
    assert result is False


@pytest.mark.parametrize(
    "compat_mode, flatten_flag, subquery_has_limit_or_offset, expected",
    [
        # Legacy mode: NEW column + filter is never flattened, regardless of limit.
        (False, True, True, False),
        (False, True, False, False),
        # Compat mode with kill-switch OFF: falls back to legacy behavior.
        (True, False, True, False),
        (True, False, False, False),
        # Compat mode ON: the loosening from SNOW-2203826 applies, except we
        # must still block flattening across LIMIT/OFFSET (otherwise WHERE
        # would move in front of LIMIT and change the result set).
        (True, True, True, False),
        (True, True, False, True),
    ],
)
def test_can_clause_dependent_columns_flatten_filter_new_limit(
    compat_mode, flatten_flag, subquery_has_limit_or_offset, expected, monkeypatch
):
    """Filter on a NEW column must not flatten across LIMIT/OFFSET, even when
    the Snowpark Connect compatible-mode loosening is active."""
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", compat_mode)
    monkeypatch.setattr(
        context, "_snowpark_connect_flatten_select_after_sort", flatten_flag
    )
    col_states = ColumnStateDict()
    col_states['"X"'] = ColumnState(
        col_name='"X"',
        change_state=ColumnChangeState.NEW,
        state_dict=col_states,
    )

    result = can_clause_dependent_columns_flatten(
        frozenset({'"X"'}),
        col_states,
        "filter",
        subquery_has_limit_or_offset=subquery_has_limit_or_offset,
    )
    assert result is expected
