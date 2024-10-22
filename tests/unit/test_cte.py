#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

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
        node.encoded_node_id_with_query = i
        node.source_plan = None
    nodes[0].children_plan_nodes = [nodes[1], nodes[3]]
    nodes[1].children_plan_nodes = [nodes[2], nodes[2]]
    nodes[2].children_plan_nodes = [nodes[4]]
    nodes[3].children_plan_nodes = [nodes[5], nodes[6]]
    nodes[4].children_plan_nodes = [nodes[5]]
    nodes[5].children_plan_nodes = []
    nodes[6].children_plan_nodes = []

    expected_duplicate_subtree_ids = {2, 5}
    return nodes[0], expected_duplicate_subtree_ids


def test_case2():
    nodes = [mock.create_autospec(SnowflakePlan) for _ in range(7)]
    for i, node in enumerate(nodes):
        node.encoded_node_id_with_query = i
        node.source_plan = None
    nodes[0].children_plan_nodes = [nodes[1], nodes[3]]
    nodes[1].children_plan_nodes = [nodes[2], nodes[2]]
    nodes[2].children_plan_nodes = [nodes[4], nodes[4]]
    nodes[3].children_plan_nodes = [nodes[6], nodes[6]]
    nodes[4].children_plan_nodes = [nodes[5]]
    nodes[5].children_plan_nodes = []
    nodes[6].children_plan_nodes = [nodes[4], nodes[4]]

    expected_duplicate_subtree_ids = {2, 4, 6}
    return nodes[0], expected_duplicate_subtree_ids


@pytest.mark.parametrize("test_case", [test_case1(), test_case2()])
def test_find_duplicate_subtrees(test_case):
    plan, expected_duplicate_subtree_ids = test_case
    duplicate_subtrees_ids = find_duplicate_subtrees(plan)
    assert duplicate_subtrees_ids == expected_duplicate_subtree_ids


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
