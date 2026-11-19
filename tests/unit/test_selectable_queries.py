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
    initiate_column_states,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SnowflakeTable
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    COLUMN_DEPENDENCY_ALL,
    COLUMN_DEPENDENCY_DOLLAR,
    COLUMN_DEPENDENCY_EMPTY,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan, Query
from snowflake.snowpark import DataFrame, Row, Session, Window
from snowflake.snowpark.functions import (
    col,
    lit,
    row_number,
    sql_expr,
    when,
    uuid_string,
)
from snowflake.snowpark.types import LongType, StringType
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)
from snowflake.snowpark._internal.compiler.cte_utils import find_duplicate_subtrees
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.compiler.utils import (
    replace_child,
    update_resolvable_node,
)
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


@pytest.fixture
def qualify_base(mock_server_connection):
    sql_session = Session(mock_server_connection)
    entity = SelectableEntity(
        SnowflakeTable("BASE_TABLE", session=sql_session),
        analyzer=sql_session._analyzer,
    )
    entity.column_states = initiate_column_states(
        [Attribute('"A"', LongType()), Attribute('"B"', LongType())],
        sql_session._analyzer,
        {},
    )
    return SelectStatement(from_=entity, analyzer=sql_session._analyzer)


def test_qualify_common_case_and_clause_order(qualify_base):
    window = row_number().over(Window.order_by("a"))
    predicate = ((col("a") > 1) & (window == 1))._expression
    qualified = qualify_base.filter(predicate)
    assert qualified.from_ is qualify_base.from_
    assert qualified.qualify is predicate
    assert qualified.where is None
    assert qualified.has_clause and qualified.has_clause_using_columns
    assert Utils.normalize_sql(qualified.sql_query) == Utils.normalize_sql(
        'SELECT * FROM BASE_TABLE QUALIFY (("A" > 1 :: INT) AND '
        '(row_number() OVER ( ORDER BY "A" ASC NULLS FIRST ) = 1 :: INT))'
    )
    projected = qualify_base.select([col("a")._expression]).filter(predicate)
    assert projected.from_ is qualify_base.from_
    assert projected.sql_query.count("SELECT") == 1
    result = qualify_base.filter((col("a") > 0)._expression).filter(predicate)
    result = result.sort([col("a").asc()._expression]).limit(2, offset=1)
    sql = result.sql_query
    assert sql.index("WHERE") < sql.index("QUALIFY") < sql.rindex("ORDER BY")
    assert sql.rindex("ORDER BY") < sql.index("LIMIT") < sql.index("OFFSET")
    assert "QUALIFY" in result.commented_sql


@pytest.mark.parametrize("compatible", [False, True])
@pytest.mark.parametrize(
    "boundary",
    [
        "limit",
        "limit_zero",
        "offset",
        "offset_zero",
        "distinct",
        "alias",
        "window",
        "sort_window",
        "qualify",
    ],
)
def test_qualify_preserves_boundaries(qualify_base, monkeypatch, compatible, boundary):
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", compatible)
    monkeypatch.setattr(
        context, "_snowpark_connect_flatten_select_after_sort", compatible
    )
    window = row_number().over(Window.order_by("a"))
    predicate = (window == 1)._expression
    base = qualify_base
    if boundary == "limit":
        base = base.limit(1)
    elif boundary == "limit_zero":
        base = base.limit(0)
    elif boundary in ("offset", "offset_zero"):
        base = copy(base)
        base.offset = 1 if boundary == "offset" else 0
    elif boundary == "distinct":
        base = base.distinct()
    elif boundary == "alias":
        base = base.select([(col("a") + 1).alias("a")._expression])
    elif boundary == "sort_window":
        base = base.sort([window.asc()._expression])
    elif boundary == "window":
        base = base.select([col("a")._expression, window.alias("rn")._expression])
    else:
        base = base.filter(predicate)
    result = base.filter(predicate)
    assert result.from_ is base
    assert result.qualify is predicate


@pytest.mark.parametrize("compatible", [False, True])
@pytest.mark.parametrize(
    "projection, dependency, flatten",
    [
        ([col("a")], col("a"), True),
        ([col("a")], col("b"), False),
        ([col("a")], col("missing"), False),
        ([col("a").alias("renamed")], col("a"), False),
        ([col("a").alias("renamed")], col("renamed"), False),
        ([col("a").alias("b")], col("b"), False),
    ],
    ids=["surviving", "dropped", "missing", "alias-source", "alias", "shadowed"],
)
def test_qualify_dependency_visibility(
    qualify_base, monkeypatch, compatible, projection, dependency, flatten
):
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", compatible)
    monkeypatch.setattr(
        context, "_snowpark_connect_flatten_select_after_sort", compatible
    )
    base = qualify_base.select([column._expression for column in projection])
    predicate = (row_number().over(Window.order_by(dependency)) == 1)._expression
    result = base.filter(predicate)
    assert result.qualify is predicate
    assert result.where is None
    if flatten:
        assert result.from_ is base.from_
        assert result.sql_query.count("SELECT") == 1
    else:
        assert result.from_ is base
        assert Utils.normalize_sql(result.sql_query) == Utils.normalize_sql(
            f"SELECT * FROM ({base.sql_query}) QUALIFY "
            f"{base.analyzer.analyze(predicate, base.df_aliased_col_name_to_real_col_name)}"
        )


@pytest.mark.parametrize("compatible", [False, True])
@pytest.mark.parametrize("keep_parameters", [False, True], ids=["dropped", "surviving"])
def test_qualify_regexp_parameter_visibility(
    qualify_base, monkeypatch, compatible, keep_parameters
):
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", compatible)
    monkeypatch.setattr(
        context, "_snowpark_connect_flatten_select_after_sort", compatible
    )
    projection = [col("a"), col("b")] if keep_parameters else [col("a")]
    base = qualify_base.select([column._expression for column in projection])
    predicate = (
        (row_number().over(Window.order_by("a")) == 1) & lit("a").regexp("A", col("b"))
    )._expression
    result = base.filter(predicate)
    assert result.qualify is predicate
    assert result.where is None
    assert result.from_ is (base.from_ if keep_parameters else base)
    assert result.sql_query.count("SELECT") == (1 if keep_parameters else 2)
    if not keep_parameters:
        assert Utils.normalize_sql(result.sql_query) == Utils.normalize_sql(
            f"SELECT * FROM ({base.sql_query}) QUALIFY "
            f"{base.analyzer.analyze(predicate, base.df_aliased_col_name_to_real_col_name)}"
        )


@pytest.mark.parametrize("compatible", [False, True])
@pytest.mark.parametrize(
    "ordering, dependencies, flatten",
    [
        (sql_expr("b"), COLUMN_DEPENDENCY_ALL, False),
        (sql_expr("$1"), COLUMN_DEPENDENCY_DOLLAR, False),
        (lit(1), COLUMN_DEPENDENCY_EMPTY, True),
    ],
    ids=["unknown", "positional", "empty"],
)
def test_qualify_dependency_sentinels(
    qualify_base, monkeypatch, compatible, ordering, dependencies, flatten
):
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", compatible)
    monkeypatch.setattr(
        context, "_snowpark_connect_flatten_select_after_sort", compatible
    )
    base = qualify_base.select([col("a")._expression])
    predicate = (row_number().over(Window.order_by(ordering)) == 1)._expression
    assert derive_dependent_columns(predicate) == dependencies
    result = base.filter(predicate)
    assert result.from_ is (base.from_ if flatten else base)
    assert result.qualify is predicate


@pytest.mark.parametrize("compatible", [False, True])
@pytest.mark.parametrize("dependency", [col("b"), sql_expr("b")])
def test_where_dependency_visibility_unchanged(
    qualify_base, monkeypatch, compatible, dependency
):
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", compatible)
    monkeypatch.setattr(
        context, "_snowpark_connect_flatten_select_after_sort", compatible
    )
    base = qualify_base.select([col("a")._expression])
    predicate = (dependency > 1)._expression
    result = base.filter(predicate)
    assert result.from_ is base.from_
    assert result.where is predicate
    assert result.qualify is None
    assert result.sql_query.count("SELECT") == 1


def test_qualify_following_operations(qualify_base):
    window = row_number().over(Window.order_by("a"))
    qualified = qualify_base.filter((window > 1)._expression)
    assert qualified.filter((col("a") > 1)._expression).from_ is qualified
    assert qualified.select([col("a")._expression]).from_ is qualified
    assert qualified.select([window.alias("rn")._expression]).from_ is qualified
    assert qualified.sort([window.asc()._expression]).from_ is qualified
    assert qualified.exclude(['"B"'], ['"A"']).from_ is qualified
    assert qualified.ilike("A").from_ is qualified
    assert qualified.sort([col("a").asc()._expression]).from_ is qualified.from_


def test_qualify_copies_complexity_and_nondeterminism(qualify_base):
    predicate = (row_number().over(Window.order_by("a")) == 1)._expression
    qualified = qualify_base.filter(predicate)
    for clone in (copy(qualified), deepcopy(qualified)):
        assert clone.qualify is not None
        assert clone.sql_query == qualified.sql_query
        assert clone.commented_sql == qualified.commented_sql
        assert clone.individual_node_complexity == sum_node_complexities(
            {PlanNodeCategory.FILTER: 1}, predicate.cumulative_node_complexity
        )
    assert deepcopy(qualified).qualify is not predicate
    assert not qualified.contains_data_generation
    nondeterministic = qualify_base.filter(
        (
            row_number().over(Window.order_by(when(col("a") > 0, uuid_string()))) == 1
        )._expression
    )
    assert nondeterministic.contains_data_generation


def test_qualify_cte_candidates(qualify_base):
    window = row_number().over(Window.order_by("a"))
    qualified = qualify_base.filter((window == 1)._expression)
    repeated = qualified.set_operator(qualified, operator=" UNION ALL ")
    duplicates, _ = find_duplicate_subtrees(repeated)
    assert qualified.encoded_node_id_with_query in duplicates

    nondeterministic = qualify_base.filter(
        ((window == 1) & uuid_string().is_not_null())._expression
    )
    repeated = nondeterministic.set_operator(nondeterministic, operator=" UNION ALL ")
    duplicates, _ = find_duplicate_subtrees(repeated)
    assert nondeterministic.encoded_node_id_with_query not in duplicates


def test_qualify_compiler_child_replacement(qualify_base):
    qualified = deepcopy(
        qualify_base.filter((row_number().over(Window.order_by("a")) == 1)._expression)
    )
    original_sql = qualified.sql_query
    original_commented_sql = qualified.commented_sql
    complexity = qualified.cumulative_node_complexity
    replacement = SelectableEntity(
        SnowflakeTable("REPLACEMENT", session=qualify_base._session),
        analyzer=qualify_base.analyzer,
    )
    generator = QueryGenerator(qualify_base._session)
    replace_child(qualified, qualified.from_, replacement, generator)
    update_resolvable_node(qualified, generator)
    assert "QUALIFY" in qualified.sql_query
    assert "REPLACEMENT" in qualified.sql_query
    assert "BASE_TABLE" not in qualified.sql_query
    assert qualified.sql_query != original_sql
    assert qualified.commented_sql != original_commented_sql
    assert qualified.cumulative_node_complexity == complexity


@pytest.mark.parametrize("simplifier", [False, True])
def test_dataframe_qualify_dispatch(qualify_base, monkeypatch, simplifier):
    sql_session = qualify_base._session
    sql_session.sql_simplifier_enabled = simplifier
    base = DataFrame(
        sql_session, qualify_base if simplifier else qualify_base.snowflake_plan
    )
    monkeypatch.setattr(context, "_is_snowpark_connect_compatible_mode", True)
    base._ops_after_agg = set()
    with mock.patch.object(base, "_build_post_agg_df") as build_having:
        result = base.filter(
            row_number().over(Window.order_by("a")) == 1, _emit_ast=False
        )
        build_having.assert_not_called()
    assert "QUALIFY" in result._plan.queries[-1].sql
    assert "HAVING" not in result._plan.queries[-1].sql
    if not simplifier:
        assert result._plan.source_plan.is_qualify
    else:
        assert result._select_statement.qualify is not None
    assert result._ops_after_agg is None
    for ordinary in (col("a") > 1, "row_number() over () = 1"):
        with mock.patch.object(base, "_build_post_agg_df") as build_having:
            base.filter(ordinary, _emit_ast=False)
            build_having.assert_called_once()


def test_qualify_mixed_predicates_and_plain_filters(qualify_base):
    window = row_number().over(Window.order_by("a"))
    for predicate in (
        (col("a") > 1) | (window == 1),
        ~((col("a") > 1) & (window == 1)),
        window.isin([1]),
        when(window == 1, True).otherwise(False),
    ):
        result = qualify_base.filter(predicate._expression)
        assert result.qualify is predicate._expression
        assert result.where is None
    raw = qualify_base.filter(col("a")._expression)
    assert raw.qualify is None
    assert raw.where is not None


def test_local_logical_window_filter_is_explicitly_unsupported(session):
    base = session.create_dataframe([[1], [2]], schema=["a"])
    # The local executor only supports filters through MockSelectStatement.
    logical_base = DataFrame(session, base._plan)
    with pytest.raises(NotImplementedError, match="Filter"):
        logical_base.filter(
            row_number().over(Window.order_by("a")) == 1, _emit_ast=False
        ).collect(_emit_ast=False)


@pytest.mark.parametrize("method", ["filter", "where"])
def test_local_window_filter_boundaries(session, method):
    base = session.create_dataframe([[1], [2]], schema=["a"])
    window = row_number().over(Window.order_by("a"))
    apply_filter = getattr(base, method)
    assert apply_filter((col("a") > 1) & (window == 1)).collect() == []
    assert base.filter(col("a") > 1).filter(window == 1).collect() == [Row(2)]
    assert apply_filter(window == 1).filter(col("a") > 1).collect() == []
    assert apply_filter(window > 1).filter(window == 1).collect() == [Row(2)]
    assert apply_filter(window > 1).select(window.alias("rn")).collect() == [Row(1)]
    assert base.limit(0).filter(window == 1).collect() == []
    assert base.limit(1, offset=1).filter(window == 1).collect() == [Row(2)]
    assert base.select(window.alias("a")).filter(window == 1).collect() == [Row(1)]
    duplicates = session.create_dataframe([[1], [1], [2]], schema=["a"])
    assert duplicates.distinct().filter(window == 2).collect() == [Row(2)]
    assert base.filter(window.isin([1])).collect() == [Row(1)]
    assert base.filter(when(window == 1, True).otherwise(False)).collect() == [Row(1)]
    assert base.filter(window > 1).sort(window.desc()).collect() == [Row(2)]
    assert base.filter((col("a") > 1) | (window == 1)).collect() == [Row(1), Row(2)]
    assert base.filter(~(window == 1)).collect() == [Row(2)]


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
