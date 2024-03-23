#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.cte_utils import find_duplicate_subtrees
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan


def test_case1():
    nodes = [mock.create_autospec(SnowflakePlan) for _ in range(7)]
    for i, node in enumerate(nodes):
        node._id = i
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
        node._id = i
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
    duplicate_subtrees = find_duplicate_subtrees(plan)
    assert {node._id for node in duplicate_subtrees} == expected_duplicate_subtree_ids
