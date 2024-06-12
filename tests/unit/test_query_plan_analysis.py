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


@pytest.mark.parametrize("node_type", [LogicalPlan, SnowflakePlan, Selectable])
def test_assign_custom_cumulative_node_complexity(
    mock_session, mock_analyzer, mock_query, node_type
):
    def get_node_for_type(node_type):
        if node_type == LogicalPlan:
            return LogicalPlan()
        if node_type == SnowflakePlan:
            return SnowflakePlan(
                [mock_query], "", source_plan=LogicalPlan(), session=mock_session
            )
        return SelectSnowflakePlan(
            SnowflakePlan(
                [mock_query], "", source_plan=LogicalPlan(), session=mock_session
            ),
            analyzer=mock_analyzer,
        )

    def set_children(node, node_type, children):
        if node_type == LogicalPlan:
            node.children = children
        elif node_type == SnowflakePlan:
            node.source_plan.children = children
        else:
            node.snowflake_plan.source_plan.children = children

    nodes = [get_node_for_type(node_type) for _ in range(7)]

    """
                            o                       o
                           / \\                     / \
                          o   o                   x   o
                         /|\
                        o o o       ->
                          |
                          o
    """
    set_children(nodes[0], node_type, [nodes[1], nodes[2]])
    set_children(nodes[1], node_type, [nodes[3], nodes[4], nodes[5]])
    set_children(nodes[2], node_type, [])
    set_children(nodes[3], node_type, [])
    set_children(nodes[4], node_type, [nodes[6]])
    set_children(nodes[5], node_type, [])
    set_children(nodes[6], node_type, [])

    assert nodes[0].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 7}
    assert nodes[1].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 5}
    assert nodes[2].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 1}
    assert nodes[3].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 1}
    assert nodes[4].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 2}
    assert nodes[5].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 1}
    assert nodes[6].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 1}

    nodes[1].cumulative_node_complexity = {PlanNodeCategory.COLUMN.value: 1}

    # assert that only value that is reset is changed
    assert nodes[0].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 7}
    assert nodes[1].cumulative_node_complexity == {PlanNodeCategory.COLUMN.value: 1}
    assert nodes[2].cumulative_node_complexity == {PlanNodeCategory.OTHERS.value: 1}


def test_selectable_entity_individual_node_complexity(mock_analyzer):
    plan_node = SelectableEntity(entity_name="dummy entity", analyzer=mock_analyzer)
    assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN.value: 1}


def test_select_sql_individual_node_complexity(mock_session, mock_analyzer):
    plan_node = SelectSQL(
        "non-select statement", convert_to_select=True, analyzer=mock_analyzer
    )
    assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN.value: 1}

    plan_node = SelectSQL("select 1 as A, 2 as B", analyzer=mock_analyzer)
    assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN.value: 1}


def test_select_snowflake_plan_individual_node_complexity(
    mock_session, mock_analyzer, mock_query
):
    source_plan = Project([NamedExpression(), NamedExpression()], LogicalPlan())
    snowflake_plan = SnowflakePlan(
        [mock_query], "", source_plan=source_plan, session=mock_session
    )
    plan_node = SelectSnowflakePlan(snowflake_plan, analyzer=mock_analyzer)
    assert plan_node.individual_node_complexity == {PlanNodeCategory.COLUMN.value: 2}


@pytest.mark.parametrize(
    "attribute,value,expected_stat",
    [
        ("projection", [NamedExpression()], {PlanNodeCategory.COLUMN.value: 1}),
        ("projection", [Expression()], {PlanNodeCategory.OTHERS.value: 1}),
        (
            "order_by",
            [Expression()],
            {PlanNodeCategory.OTHERS.value: 1, PlanNodeCategory.ORDER_BY.value: 1},
        ),
        (
            "where",
            Expression(),
            {PlanNodeCategory.OTHERS.value: 1, PlanNodeCategory.FILTER.value: 1},
        ),
        ("limit_", 10, {PlanNodeCategory.LOW_IMPACT.value: 1}),
        ("offset", 2, {PlanNodeCategory.LOW_IMPACT.value: 1}),
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
        assert plan_node.individual_node_complexity == {
            PlanNodeCategory.COLUMN.value: 2
        }


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

    assert plan_node.individual_node_complexity == {
        PlanNodeCategory.SET_OPERATION.value: 1
    }
