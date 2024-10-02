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
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    Expression,
    NamedExpression,
)
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    subtract_complexities,
)
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
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeTable,
)
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.unary_plan_node import Project
from snowflake.snowpark.dataframe import StringType


@pytest.mark.parametrize("node_type", [LogicalPlan, SnowflakePlan, Selectable])
def test_reset_cumulative_node_complexity(
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

    nodes = [get_node_for_type(node_type) for _ in range(8)]

    """
                            0                       0
                           / \\                    / \
                          1   2                   1   2
                         /|\\                     |
                        3 4 5       ->            7
                          |
                          6
    """
    set_children(nodes[0], node_type, nodes[1:3])
    set_children(nodes[1], node_type, nodes[3:6])
    set_children(nodes[2], node_type, [])
    set_children(nodes[3], node_type, [])
    set_children(nodes[4], node_type, [nodes[6]])
    set_children(nodes[5], node_type, [])
    set_children(nodes[6], node_type, [])
    set_children(nodes[7], node_type, [])

    assert nodes[0].cumulative_node_complexity == {PlanNodeCategory.OTHERS: 7}
    assert nodes[1].cumulative_node_complexity == {PlanNodeCategory.OTHERS: 5}
    assert nodes[2].cumulative_node_complexity == {PlanNodeCategory.OTHERS: 1}
    assert nodes[3].cumulative_node_complexity == {PlanNodeCategory.OTHERS: 1}
    assert nodes[4].cumulative_node_complexity == {PlanNodeCategory.OTHERS: 2}
    assert nodes[5].cumulative_node_complexity == {PlanNodeCategory.OTHERS: 1}
    assert nodes[6].cumulative_node_complexity == {PlanNodeCategory.OTHERS: 1}

    set_children(nodes[1], node_type, [nodes[7]])
    nodes[1].cumulative_node_complexity = {PlanNodeCategory.COLUMN: 1}
    nodes[0].reset_cumulative_node_complexity()

    # assert that only value that is reset is changed
    assert nodes[0].cumulative_node_complexity == {
        PlanNodeCategory.COLUMN: 1,
        PlanNodeCategory.OTHERS: 2,
    }
    assert nodes[1].cumulative_node_complexity == {PlanNodeCategory.COLUMN: 1}
    assert nodes[2].cumulative_node_complexity == {PlanNodeCategory.OTHERS: 1}


def test_selectable_entity_individual_node_complexity(mock_session, mock_analyzer):
    logical_plan = SnowflakeTable("dummy entity", session=mock_session)
    plan_node = SelectableEntity(entity=logical_plan, analyzer=mock_analyzer)
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
    if attribute == "projection" and isinstance(value[0], NamedExpression):
        # NamedExpression is not a valid projection expression for selectStatement,
        # and there is no individual_node_complexity or cumulative_node_complexity
        # attributes associated with it
        with pytest.raises(AttributeError):
            plan_node.individual_node_complexity
    else:
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


@pytest.mark.parametrize(
    "complexity1, complexity2, expected_result",
    [
        (
            {
                PlanNodeCategory.COLUMN: 20,
                PlanNodeCategory.LITERAL: 5,
                PlanNodeCategory.FUNCTION: 3,
            },
            {
                PlanNodeCategory.COLUMN: 11,
                PlanNodeCategory.LITERAL: 4,
                PlanNodeCategory.FUNCTION: 1,
            },
            {
                PlanNodeCategory.COLUMN: 9,
                PlanNodeCategory.LITERAL: 1,
                PlanNodeCategory.FUNCTION: 2,
            },
        ),
        (
            {
                PlanNodeCategory.COLUMN: 20,
                PlanNodeCategory.LITERAL: 5,
                PlanNodeCategory.FUNCTION: 3,
            },
            {PlanNodeCategory.COLUMN: 11, PlanNodeCategory.FUNCTION: 1},
            {
                PlanNodeCategory.COLUMN: 9,
                PlanNodeCategory.LITERAL: 5,
                PlanNodeCategory.FUNCTION: 2,
            },
        ),
        (
            {
                PlanNodeCategory.COLUMN: 20,
                PlanNodeCategory.LITERAL: 5,
                PlanNodeCategory.FUNCTION: 3,
            },
            {PlanNodeCategory.LITERAL: 11, PlanNodeCategory.FUNCTION: 1},
            {
                PlanNodeCategory.COLUMN: 20,
                PlanNodeCategory.LITERAL: -6,
                PlanNodeCategory.FUNCTION: 2,
            },
        ),
        (
            {
                PlanNodeCategory.COLUMN: 20,
                PlanNodeCategory.LITERAL: 5,
                PlanNodeCategory.FUNCTION: 3,
            },
            {
                PlanNodeCategory.COLUMN: 11,
                PlanNodeCategory.LITERAL: 1,
                PlanNodeCategory.FILTER: 1,
                PlanNodeCategory.CASE_WHEN: 2,
            },
            {
                PlanNodeCategory.COLUMN: 9,
                PlanNodeCategory.LITERAL: 4,
                PlanNodeCategory.FUNCTION: 3,
                PlanNodeCategory.FILTER: -1,
                PlanNodeCategory.CASE_WHEN: -2,
            },
        ),
    ],
)
def test_subtract_complexities(complexity1, complexity2, expected_result):
    assert subtract_complexities(complexity1, complexity2) == expected_result


def test_select_statement_get_complexity_map_no_column_state(mock_analyzer):
    mock_from = mock.create_autospec(Selectable)
    mock_from.pre_actions = None
    mock_from.post_actions = None
    mock_from.expr_to_alias = {}
    mock_from.df_aliased_col_name_to_real_col_name = {}
    select_statement = SelectStatement(analyzer=mock_analyzer, from_=mock_from)

    assert select_statement.get_projection_name_complexity_map() is None
    assert select_statement.projection_complexities == []

    select_statement._column_states = mock.create_autospec(ColumnStateDict)
    select_statement.projection = [Expression()]
    mock_from._column_states = None

    assert select_statement.get_projection_name_complexity_map() is None


def test_select_statement_get_complexity_map_mismatch_projection_length(mock_analyzer):
    mock_from = mock.create_autospec(Selectable)
    mock_from.pre_actions = None
    mock_from.post_actions = None
    mock_from.expr_to_alias = {}
    mock_from.df_aliased_col_name_to_real_col_name = {}

    # create a select_statement with 2 projections
    select_statement = SelectStatement(
        analyzer=mock_analyzer, projection=[Expression(), Expression()], from_=mock_from
    )
    column_states = ColumnStateDict()
    column_states.projection = [Attribute("A", StringType())]
    select_statement._column_states = column_states

    assert select_statement.get_projection_name_complexity_map() is None

    # update column states projection length to match the projection length
    column_states.projection = [
        Attribute("A", StringType()),
        Attribute("B", StringType()),
    ]
    select_statement._projection_complexities = []

    assert select_statement.get_projection_name_complexity_map() is None
