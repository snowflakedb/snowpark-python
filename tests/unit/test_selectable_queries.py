#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

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
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SnowflakeTable
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan, Query
from snowflake.snowpark.types import StringType
from tests.utils import Utils
import snowflake.snowpark.context as context


@pytest.fixture(autouse=True)
def setup(request):
    original = context._enable_trace_sql_errors_to_dataframe
    context.configure_development_features(
        enable_trace_sql_errors_to_dataframe=True, enable_eager_schema_validation=False
    )
    yield
    context.configure_development_features(
        enable_trace_sql_errors_to_dataframe=original,
        enable_eager_schema_validation=False,
    )


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
    select_snowflake_plan = SelectSnowflakePlan(
        snowflake_plan=mock_snowflake_plan,
        analyzer=mock_analyzer,
    )

    expected_sql = "SELECT A, B FROM test_table WHERE A > 10"
    assert select_snowflake_plan.sql_query == expected_sql
    assert select_snowflake_plan.commented_sql == expected_sql
