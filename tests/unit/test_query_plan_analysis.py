#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    EXCEPT,
    INTERSECT,
    UNION,
    UNION_ALL,
)
from snowflake.snowpark._internal.analyzer.expression import Expression, NamedExpression
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectableEntity,
    SelectSnowflakePlan,
    SelectSQL,
    SelectStatement,
    SelectTableFunction,
    SetOperand,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.unary_plan_node import Project


def test_selectable_entity_individual_node_complexity(mock_analyzer):
    plan_node = SelectableEntity(entity_name="dummy entity", analyzer=mock_analyzer)
    assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN: 1}


def test_select_sql_individual_node_complexity(mock_analyzer):
    plan_node = SelectSQL("non-select statement", analyzer=mock_analyzer)
    assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN: 1}

    plan_node = SelectSQL("select 1 as A, 2 as B", analyzer=mock_analyzer)
    assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN: 1}


def test_select_snowflake_plan_individual_node_complexity(
    mock_session, mock_analyzer, mock_query
):
    source_plan = Project([NamedExpression(), NamedExpression()], LogicalPlan())
    snowflake_plan = SnowflakePlan(
        [mock_query], "", source_plan=source_plan, session=mock_session
    )
    plan_node = SelectSnowflakePlan(snowflake_plan, analyzer=mock_analyzer)
    assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN: 2}


@pytest.mark.parametrize(
    "attribute,value,expected_stat",
    [
        ("projection", [NamedExpression()], {PlanNodeCategory.COLUMN: 1}),
        ("projection", [Expression()], {PlanNodeCategory.OTHERS: 1}),
        (
            "order_by",
            [Expression()],
            {PlanNodeCategory.OTHERS: 1, PlanNodeCategory.ORDER_BY: 1},
        ),
        (
            "where",
            Expression(),
            {PlanNodeCategory.OTHERS: 1, PlanNodeCategory.FILTER: 1},
        ),
        ("limit_", 10, {PlanNodeCategory.LOW_IMPACT: 1}),
        ("offset", 2, {PlanNodeCategory.LOW_IMPACT: 1}),
    ],
)
def test_select_statement_individual_node_complexity(
    mock_analyzer, attribute, value, expected_stat
):
    from_ = mock.create_autospec(Selectable)
    from_.pre_actions = None
    from_.post_actions = None
    from_.expr_to_alias = {}
    from_.df_aliased_col_name_to_real_col_name = {}

    plan_node = SelectStatement(from_=from_, analyzer=mock_analyzer)
    setattr(plan_node, attribute, value)
    assert plan_node.individual_node_complexity == expected_stat


def test_select_table_function_individual_node_complexity(
    mock_analyzer, mock_session, mock_query
):
    func_expr = mock.create_autospec(TableFunctionExpression)
    source_plan = Project([NamedExpression(), NamedExpression()], LogicalPlan())
    snowflake_plan = SnowflakePlan(
        [mock_query], "", source_plan=source_plan, session=mock_session
    )

    def mocked_resolve(*args, **kwargs):
        return snowflake_plan

    with mock.patch.object(mock_analyzer, "resolve", side_effect=mocked_resolve):
        plan_node = SelectTableFunction(func_expr, analyzer=mock_analyzer)
        assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN: 2}


@pytest.mark.parametrize("set_operator", [UNION, UNION_ALL, INTERSECT, EXCEPT])
def test_set_statement_individual_node_complexity(mock_analyzer, set_operator):
    mock_selectable = mock.create_autospec(Selectable)
    mock_selectable.pre_actions = None
    mock_selectable.post_actions = None
    mock_selectable.expr_to_alias = {}
    mock_selectable.df_aliased_col_name_to_real_col_name = {}
    set_operands = [
        SetOperand(mock_selectable, set_operator),
        SetOperand(mock_selectable, set_operator),
    ]
    plan_node = SetStatement(*set_operands, analyzer=mock_analyzer)

    assert plan_node.individual_node_complexity == {PlanNodeCategory.SET_OPERATION: 1}
