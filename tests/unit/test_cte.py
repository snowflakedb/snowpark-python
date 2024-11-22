#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    SelectSQL,
    SelectStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.compiler.cte_utils import (
    encode_node_id_with_query,
    find_duplicate_subtrees,
)


def test_case1():
    nodes = [mock.create_autospec(SnowflakePlan) for _ in range(7)]
    for i, node in enumerate(nodes):
        node.encoded_node_id_with_query = f"{i}_{i}"
        node.source_plan = None
        if i == 5:
            node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 80000}
        elif i == 2:
            node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 600000}
        else:
            node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 3}
    nodes[0].children_plan_nodes = [nodes[1], nodes[3]]
    nodes[1].children_plan_nodes = [nodes[2], nodes[2]]
    nodes[2].children_plan_nodes = [nodes[4]]
    nodes[3].children_plan_nodes = [nodes[5], nodes[6]]
    nodes[4].children_plan_nodes = [nodes[5]]
    nodes[5].children_plan_nodes = []
    nodes[6].children_plan_nodes = []

    expected_duplicate_subtree_ids = {"2_2", "5_5"}
    expected_repeated_node_complexity = [0, 3, 0, 2, 0, 0, 0]
    return nodes[0], expected_duplicate_subtree_ids, expected_repeated_node_complexity


def test_case2():
    nodes = [mock.create_autospec(SnowflakePlan) for _ in range(7)]
    for i, node in enumerate(nodes):
        node.encoded_node_id_with_query = f"{i}_{i}"
        node.source_plan = None
        if i == 2:
            node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 2000000}
        elif i == 4:
            node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 7000000}
        elif i == 6:
            node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 15000000}
        else:
            node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 10}
    nodes[0].children_plan_nodes = [nodes[1], nodes[3]]
    nodes[1].children_plan_nodes = [nodes[2], nodes[2]]
    nodes[2].children_plan_nodes = [nodes[4], nodes[4]]
    nodes[3].children_plan_nodes = [nodes[6], nodes[6]]
    nodes[4].children_plan_nodes = [nodes[5]]
    nodes[5].children_plan_nodes = []
    nodes[6].children_plan_nodes = [nodes[4], nodes[4]]

    expected_duplicate_subtree_ids = {"2_2", "4_4", "6_6"}
    expected_repeated_node_complexity = [0, 0, 0, 0, 2, 8, 2]
    return nodes[0], expected_duplicate_subtree_ids, expected_repeated_node_complexity


@pytest.mark.parametrize("test_case", [test_case1(), test_case2()])
def test_find_duplicate_subtrees(test_case):
    plan, expected_duplicate_subtree_ids, expected_repeated_node_complexity = test_case
    duplicate_subtrees_ids, repeated_node_complexity = find_duplicate_subtrees(plan)
    assert duplicate_subtrees_ids == expected_duplicate_subtree_ids
    assert repeated_node_complexity is None

    duplicate_subtrees_ids, repeated_node_complexity = find_duplicate_subtrees(
        plan, propagate_complexity_hist=True
    )
    assert duplicate_subtrees_ids == expected_duplicate_subtree_ids
    assert repeated_node_complexity == expected_repeated_node_complexity


def test_encode_node_id_with_query_select_sql(mock_analyzer):
    sql_text = "select 1 as a, 2 as b"
    select_sql_node = SelectSQL(
        sql=sql_text,
        convert_to_select=False,
        analyzer=mock_analyzer,
    )
    expected_hash = "bf156ae77e"
    assert encode_node_id_with_query(select_sql_node) == f"{expected_hash}_SelectSQL"

    select_statement_node = SelectStatement(
        from_=select_sql_node,
        analyzer=mock_analyzer,
    )
    select_statement_node._sql_query = sql_text
    assert (
        encode_node_id_with_query(select_statement_node)
        == f"{expected_hash}_SelectStatement"
    )
