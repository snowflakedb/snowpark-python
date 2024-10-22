#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    Except,
    Intersect,
    Union,
)
from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_EXCEPT,
    SET_INTERSECT,
    SET_UNION,
    SET_UNION_ALL,
    SelectSnowflakePlan,
    SelectSQL,
    SelectStatement,
    SetOperand,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    WithQueryBlock,
)
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
    "node_generator,expected_strict,expected_relaxed",
    [
        (
            lambda _: Pivot([], empty_expression, [], [], None, empty_logical_plan),
            True,
            True,
        ),
        (
            lambda _: Unpivot("value_col", "name_col", [], empty_logical_plan),
            True,
            True,
        ),
        (lambda _: Sort([], empty_logical_plan), True, True),
        (lambda _: Aggregate([], [], empty_logical_plan), True, True),
        (lambda _: Sample(empty_logical_plan, None, 2, None), True, True),
        (
            lambda _: Union(empty_logical_plan, empty_logical_plan, is_all=False),
            True,
            True,
        ),
        (
            lambda _: Union(empty_logical_plan, empty_logical_plan, is_all=True),
            False,
            False,
        ),
        (lambda _: Except(empty_logical_plan, empty_logical_plan), True, True),
        (lambda _: Intersect(empty_logical_plan, empty_logical_plan), True, True),
        (lambda _: WithQueryBlock("dummy_cte", empty_logical_plan), True, True),
        (
            lambda x: SelectStatement(
                from_=empty_selectable, order_by=[empty_expression], analyzer=x
            ),
            True,
            True,
        ),
        (
            lambda x: SelectStatement(from_=empty_selectable, analyzer=x),
            False,
            True,
        ),
        (
            lambda x: SetStatement(
                SetOperand(empty_selectable),
                SetOperand(empty_selectable, SET_UNION),
                analyzer=x,
            ),
            True,
            True,
        ),
        (
            lambda x: SetStatement(
                SetOperand(empty_selectable),
                SetOperand(empty_selectable, SET_INTERSECT),
                analyzer=x,
            ),
            True,
            True,
        ),
        (
            lambda x: SetStatement(
                SetOperand(empty_selectable),
                SetOperand(empty_selectable, SET_EXCEPT),
                analyzer=x,
            ),
            True,
            True,
        ),
        (
            lambda x: SetStatement(
                SetOperand(empty_selectable),
                SetOperand(empty_selectable, SET_UNION_ALL),
                SetOperand(empty_selectable, SET_UNION),
                analyzer=x,
            ),
            True,
            True,
        ),
        (
            lambda x: SetStatement(
                SetOperand(empty_selectable),
                SetOperand(empty_selectable, SET_UNION_ALL),
                SetOperand(empty_selectable, SET_INTERSECT),
                analyzer=x,
            ),
            False,
            False,
        ),
    ],
)
def test_pipeline_breaker_node(
    mock_session,
    mock_analyzer,
    node_generator,
    expected_strict,
    expected_relaxed,
):
    large_query_breakdown = LargeQueryBreakdown(
        mock_session,
        mock_analyzer,
        [],
        mock_session.large_query_breakdown_complexity_bounds,
    )
    node = node_generator(mock_analyzer)

    for is_relaxed, expected in [
        (True, expected_relaxed),
        (False, expected_strict),
    ]:
        assert (
            large_query_breakdown._is_node_pipeline_breaker(node, is_relaxed)
            is expected
        ), f"Node {type(node)} is not detected as a pipeline breaker node"

        resolved_node = mock_analyzer.resolve(node)
        assert isinstance(resolved_node, SnowflakePlan)
        assert (
            large_query_breakdown._is_node_pipeline_breaker(resolved_node, is_relaxed)
            is expected
        ), f"Resolved node of {type(node)} is not detected as a pipeline breaker node"

        select_snowflake_plan = SelectSnowflakePlan(
            resolved_node, analyzer=mock_analyzer
        )
        assert (
            large_query_breakdown._is_node_pipeline_breaker(
                select_snowflake_plan, is_relaxed
            )
            is expected
        ), "SelectSnowflakePlan node is not detected as a pipeline breaker node"
