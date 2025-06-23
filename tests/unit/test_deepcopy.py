#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import copy
import uuid
from unittest import mock

from snowflake.snowpark import Session, functions as F, types as T
from snowflake.snowpark._internal.analyzer.analyzer_utils import UNION
from snowflake.snowpark._internal.analyzer.select_statement import (
    ColumnStateDict,
    Selectable,
    SelectableEntity,
    SelectSnowflakePlan,
    SelectSQL,
    SelectStatement,
    SelectTableFunction,
    SetOperand,
    SetStatement,
    TableFunctionExpression,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    Attribute,
    Query,
    SnowflakePlan,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SnowflakeTable
from snowflake.snowpark.types import IntegerType, StringType


def init_snowflake_plan(session: Session) -> SnowflakePlan:
    return SnowflakePlan(
        queries=[Query("FAKE PLAN QUERY", params=["a", 1, "b"])],
        schema_query="FAKE SCHEMA QUERY",
        post_actions=[Query("FAKE POST ACTION")],
        expr_to_alias={uuid.uuid4(): "PLAN_EXPR1", uuid.uuid4(): "PLAN_EXPR2"},
        source_plan=SnowflakeTable("TEST_TABLE", session=session),
        is_ddl_on_temp_object=False,
        api_calls=None,
        df_aliased_col_name_to_real_col_name={"df_alias": {"A": "A", "B": "B1"}},
        session=session,
    )


def init_selectable_fields(node: Selectable, init_plan: bool) -> None:
    node.pre_actions = [Query("DUMMY PRE ACTION")]
    node.post_actions = [Query("DUMMY POST ACTION")]
    node.flatten_disabled = True
    dummy_column_dict = ColumnStateDict()
    dummy_column_dict.projection = [
        Attribute("A", IntegerType()),
        Attribute("B", StringType()),
    ]
    # The following are useful aggregate information of all columns. Used to quickly rule if a query can be flattened.
    dummy_column_dict.has_changed_columns = False
    dummy_column_dict.has_new_columns = True
    dummy_column_dict.dropped_columns = ["C"]
    dummy_column_dict.active_columns = ["A", "B"]
    dummy_column_dict.columns_referencing_all_columns = set()
    node._column_states = dummy_column_dict

    node.expr_to_alias = {uuid.uuid4(): "EXPR1", uuid.uuid4(): "EXPR2"}
    node.df_aliased_col_name_to_real_col_name = {"df_alias": {"A": "A", "B": "B1"}}
    if init_plan:
        session = mock.create_autospec(Session)
        node._snowflake_plan = init_snowflake_plan(session)


def verify_copied_selectable(
    copied_selectable: Selectable,
    original_selectable: Selectable,
    expect_plan_copied: bool = False,
) -> None:
    # verify _snowflake_plan is never copied
    if expect_plan_copied:
        assert copied_selectable._snowflake_plan is not None
    else:
        assert copied_selectable._snowflake_plan is None
    assert copied_selectable.pre_actions == original_selectable.pre_actions
    assert copied_selectable.post_actions == original_selectable.post_actions
    assert copied_selectable.flatten_disabled == original_selectable.flatten_disabled
    assert copied_selectable.expr_to_alias == original_selectable.expr_to_alias
    assert (
        copied_selectable.df_aliased_col_name_to_real_col_name
        == copied_selectable.df_aliased_col_name_to_real_col_name
    )
    assert (
        copied_selectable._cumulative_node_complexity
        == original_selectable._cumulative_node_complexity
    )

    copied_state = copied_selectable._column_states
    original_state = original_selectable._column_states
    if (copied_state is not None) and (original_state is not None):
        assert copied_state.active_columns == original_state.active_columns
        assert copied_state.has_new_columns == original_state.has_new_columns
        # verify the column projection
        # direct compare should fail because the attribute objects are different
        assert copied_state.projection != original_state.projection
        for copied_attribute, original_attribute in zip(
            copied_state.projection, original_state.projection
        ):
            assert copied_attribute.name == original_attribute.name
            assert copied_attribute.datatype == original_attribute.datatype
            assert copied_attribute.nullable == original_attribute.nullable
    else:
        assert copied_state is None
        assert original_state is None

    # verify memoization works correctly
    original_id = id(original_selectable)
    memo = {original_id: copied_selectable}
    assert copy.deepcopy(original_selectable, memo) is copied_selectable


def test_selectable_entity(mock_session, mock_analyzer):
    selectable_entity = SelectableEntity(
        SnowflakeTable("TEST_TABLE", session=mock_session), analyzer=mock_analyzer
    )
    init_selectable_fields(selectable_entity, init_plan=False)
    copied_selectable = copy.deepcopy(selectable_entity)
    verify_copied_selectable(copied_selectable, selectable_entity)
    assert copied_selectable.entity.name == selectable_entity.entity.name


def test_select_sql(mock_session, mock_analyzer):
    # none-select sql
    sql = "show tables limit 10"
    select_sql = SelectSQL(
        sql, convert_to_select=False, analyzer=mock_analyzer, params=[1, "a", 2, "b"]
    )

    init_selectable_fields(select_sql, init_plan=True)
    assert select_sql.convert_to_select is False
    assert select_sql._snowflake_plan is not None
    copied_selectable = copy.deepcopy(select_sql)
    verify_copied_selectable(copied_selectable, select_sql)

    # set convert_to_select to True to test the copy
    select_sql.convert_to_select = True
    copied_selectable = copy.deepcopy(select_sql)
    verify_copied_selectable(copied_selectable, select_sql)
    assert copied_selectable.convert_to_select == select_sql.convert_to_select
    assert copied_selectable.commented_sql == select_sql.commented_sql
    assert copied_selectable.original_sql == select_sql.original_sql


def test_select_snowflake_plan(mock_session, mock_analyzer):
    snowflake_plan = init_snowflake_plan(mock_session)
    select_snowflake_plan = SelectSnowflakePlan(
        snowflake_plan=snowflake_plan,
        analyzer=mock_analyzer,
    )
    init_selectable_fields(select_snowflake_plan, init_plan=False)
    copied_selectable = copy.deepcopy(select_snowflake_plan)
    verify_copied_selectable(
        copied_selectable, select_snowflake_plan, expect_plan_copied=True
    )
    assert copied_selectable._query_params == copied_selectable._query_params


def test_select_statement(mock_session, mock_analyzer):
    projection = [
        F.cast(F.col("B"), T.IntegerType())._expression,
        (F.col("A") + F.col("B")).alias("A")._expression,
    ]
    where = [(F.col("B") > 10)._expression]
    order_by = [F.col("B")._expression]
    limit_ = 5
    offset = 2
    from_ = SelectableEntity(
        SnowflakeTable("TEST_TABLE", session=mock_session), analyzer=mock_analyzer
    )
    select_snowflake_plan = SelectStatement(
        projection=projection,
        from_=from_,
        where=where,
        order_by=order_by,
        limit_=limit_,
        offset=offset,
        analyzer=mock_analyzer,
    )
    init_selectable_fields(select_snowflake_plan, init_plan=False)
    copied_selectable = copy.deepcopy(select_snowflake_plan)
    verify_copied_selectable(
        copied_selectable, select_snowflake_plan, expect_plan_copied=False
    )
    # verify select statement fields
    assert copied_selectable.limit_ == select_snowflake_plan.limit_
    assert copied_selectable.offset == select_snowflake_plan.offset
    assert copied_selectable._query_params == select_snowflake_plan._query_params


def test_select_table_function(mock_session, mock_analyzer):
    select_table_function_plan = SelectTableFunction(
        func_expr=TableFunctionExpression(func_name="test_table_function"),
        snowflake_plan=init_snowflake_plan(mock_session),
        analyzer=mock_analyzer,
    )
    init_selectable_fields(select_table_function_plan, init_plan=False)
    copied_selectable = copy.deepcopy(select_table_function_plan)
    verify_copied_selectable(
        copied_selectable, select_table_function_plan, expect_plan_copied=True
    )
    assert (
        copied_selectable.func_expr.func_name
        == select_table_function_plan.func_expr.func_name
    )


def test_set_statement(mock_session, mock_analyzer):
    # construct the SetOperands
    select_entity1 = SelectableEntity(
        SnowflakeTable("TEST_TABLE", session=mock_session), analyzer=mock_analyzer
    )
    select_entity2 = SelectableEntity(
        SnowflakeTable("TEST_TABLE2", session=mock_session), analyzer=mock_analyzer
    )
    operand1 = SetOperand(selectable=select_entity1, operator=UNION)
    operand2 = SetOperand(selectable=select_entity2, operator=UNION)

    # construct the set statement
    set_statement = SetStatement(*[operand1, operand2], analyzer=mock_analyzer)
    init_selectable_fields(set_statement, init_plan=False)
    copied_selectable = copy.deepcopy(set_statement)
    verify_copied_selectable(copied_selectable, set_statement, expect_plan_copied=False)

    # verify the set operands
    for copied_operand, original_operand in zip(
        copied_selectable._nodes, set_statement._nodes
    ):
        verify_copied_selectable(
            copied_operand, original_operand, expect_plan_copied=False
        )
