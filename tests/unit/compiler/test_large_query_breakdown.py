#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
TODO:
* test query complexity score calculation is correct -- unit: put in replace_child test
"""

from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.binary_plan_node import Union
from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_UNION,
    SelectSnowflakePlan,
    SelectSQL,
    SelectStatement,
    SetOperand,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    Pivot,
    Sample,
    Sort,
    Unpivot,
)
from snowflake.snowpark._internal.compiler.large_query_breakdown import (
    LargeQueryBreakdown,
)

empty_logical_plan = LogicalPlan()
empty_expression = Expression()
empty_selectable = SelectSQL("dummy_query", analyzer=mock.create_autospec(Analyzer))


@pytest.mark.parametrize(
    "node_generator",
    [
        lambda _: Pivot([], empty_expression, [], [], None, empty_logical_plan),
        lambda _: Unpivot("value_col", "name_col", [], empty_logical_plan),
        lambda _: Sort([], empty_logical_plan),
        lambda _: Aggregate([], [], empty_logical_plan),
        lambda _: Sample(empty_logical_plan, None, 2, None),
        lambda _: Union(empty_logical_plan, empty_logical_plan, is_all=False),
        lambda x: SelectStatement(
            from_=empty_selectable, order_by=[empty_expression], analyzer=x
        ),
        lambda x: SetStatement(
            SetOperand(empty_selectable),
            SetOperand(empty_selectable, SET_UNION),
            analyzer=x,
        ),
    ],
)
def test_pipeline_breaker_node(mock_session, mock_analyzer, node_generator):
    large_query_breakdown = LargeQueryBreakdown(mock_session, mock_analyzer, [])
    node = node_generator(mock_analyzer)

    assert large_query_breakdown._is_node_pipeline_breaker(
        node
    ), f"Node {type(node)} is not detected as a pipeline breaker node"

    resolved_node = mock_analyzer.resolve(node)
    assert isinstance(resolved_node, SnowflakePlan)
    assert large_query_breakdown._is_node_pipeline_breaker(
        resolved_node
    ), f"Resolved node of {type(node)} is not detected as a pipeline breaker node"

    select_snowflake_plan = SelectSnowflakePlan(resolved_node, analyzer=mock_analyzer)
    assert large_query_breakdown._is_node_pipeline_breaker(
        select_snowflake_plan
    ), "SelectSnowflakePlan node is not detected as a pipeline breaker node"


def test_query_complexity_update():
    pass
