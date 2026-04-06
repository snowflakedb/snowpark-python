#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import hashlib
from types import SimpleNamespace
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


def create_test_case1():
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


def create_test_case2():
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


@pytest.mark.parametrize("test_case", [create_test_case1(), create_test_case2()])
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


def _create_mock_node(encoded_id, complexity=3):
    """Helper to create a mock SnowflakePlan node for CTE tests."""
    node = mock.create_autospec(SnowflakePlan)
    node.encoded_node_id_with_query = encoded_id
    node.source_plan = None
    node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: complexity}
    node.children_plan_nodes = []
    return node


def test_connect_mode_same_object_still_deduplicated():
    """When the same Python object is referenced multiple times (e.g. df.union_all(df)),
    it should still be detected as a duplicate even in connect-compatible mode."""
    root = _create_mock_node("root_R")
    shared_child = _create_mock_node("child_C")
    leaf = _create_mock_node("leaf_L")
    root.children_plan_nodes = [shared_child, shared_child]
    shared_child.children_plan_nodes = [leaf]

    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        duplicated_ids, _ = find_duplicate_subtrees(root)
    assert "child_C" in duplicated_ids


def test_connect_mode_different_objects_same_id_not_deduplicated():
    """When two different Python objects have the same encoded_node_id_with_query
    (e.g. df1.union_all(df2) where df1 and df2 produce the same SQL),
    find_duplicate_subtrees flags the encoded ID (raw count > 1), but
    the per-object filtering in _replace_duplicate_node_with_cte will
    skip them since each object appears only once."""
    root = _create_mock_node("root_R")
    child_a = _create_mock_node("same_S")
    child_b = _create_mock_node("same_S")
    leaf_a = _create_mock_node("leaf_L")
    leaf_b = _create_mock_node("leaf_L")
    root.children_plan_nodes = [child_a, child_b]
    child_a.children_plan_nodes = [leaf_a]
    child_b.children_plan_nodes = [leaf_b]

    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        duplicated_ids, _ = find_duplicate_subtrees(root)
    # The encoded IDs are flagged by raw count; per-object filtering
    # happens downstream in _replace_duplicate_node_with_cte.
    assert "same_S" in duplicated_ids


def test_connect_mode_mixed_shared_and_distinct_objects():
    """A tree with both shared objects (same ref) and distinct objects (different refs,
    same encoded id).

    Tree:   root
           /    \\
        left    right    (different objects, same encoded id "branch_B")
          |       |
        shared  shared   (same object, appears twice → should be deduplicated)

    find_duplicate_subtrees flags both encoded IDs by raw count.
    The per-object filtering in _replace_duplicate_node_with_cte will
    skip left/right (each appears once) and only CTE-ify shared.
    """
    root = _create_mock_node("root_R")
    left = _create_mock_node("branch_B")
    right = _create_mock_node("branch_B")
    shared = _create_mock_node("shared_S")
    root.children_plan_nodes = [left, right]
    left.children_plan_nodes = [shared]
    right.children_plan_nodes = [shared]

    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        duplicated_ids, _ = find_duplicate_subtrees(root)
    # branch_B has raw count 2 (left + right) → flagged as duplicate.
    # shared_S has raw count 2 but its only parent (branch_B) is also
    # duplicated, so it's not the root of a duplicate subtree.
    assert "branch_B" in duplicated_ids
    assert "shared_S" not in duplicated_ids


def test_connect_mode_distinct_objects_each_duplicated():
    """When multiple distinct objects share the same encoded id AND each object
    itself appears more than once, each should still be CTE-deduplicated.

    Tree:       root
               /    \\
           union1   union2
            / \\      / \\
          df1 df1  df2 df2   ← df1 and df2 are different objects, same encoded id
                               df1 appears twice, df2 appears twice

    Both df1 and df2 should be deduplicated individually.
    """
    root = _create_mock_node("root_R")
    union1 = _create_mock_node("union1_U")
    union2 = _create_mock_node("union2_U2")
    df1 = _create_mock_node("same_S")
    df2 = _create_mock_node("same_S")
    root.children_plan_nodes = [union1, union2]
    union1.children_plan_nodes = [df1, df1]
    union2.children_plan_nodes = [df2, df2]

    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        duplicated_ids, _ = find_duplicate_subtrees(root)
    assert "same_S" in duplicated_ids


def test_existing_cases_unchanged_in_connect_mode():
    """Existing test cases use the same object referenced multiple times,
    so results should be the same even in connect-compatible mode."""
    for create_fn in [create_test_case1, create_test_case2]:
        plan, expected_ids, expected_complexity = create_fn()
        with mock.patch(
            "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
        ):
            dup_ids, _ = find_duplicate_subtrees(plan)
        assert dup_ids == expected_ids


def test_connect_mode_with_propagate_complexity_hist():
    """Verify that propagate_complexity_hist still works correctly in connect mode."""
    root = _create_mock_node("root_R")
    shared = _create_mock_node("shared_S", complexity=50000)
    root.children_plan_nodes = [shared, shared]

    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        dup_ids, complexity_hist = find_duplicate_subtrees(
            root, propagate_complexity_hist=True
        )
    assert "shared_S" in dup_ids
    assert complexity_hist is not None
    assert complexity_hist[1] == 2  # 50000 falls in bin 1 (> 10,000, <= 100,000)


def test_connect_mode_mixed_duplicated_and_unique_objects():
    """When multiple distinct objects share the same encoded id but only some
    appear more than once, the encoded ID should still be flagged as
    duplicated (because at least one object is genuinely duplicated).

    Tree:       root
               /    \\
           union1   union2
            / \\      / \\
          df1 df1  df2 df3   ← df1 appears 2x (duplicated), df2 and df3 appear 1x each

    The encoded ID "same_S" should be in duplicated_node_ids because df1
    appears twice. The per-object filtering (df2/df3 not replaced) is
    handled downstream in _replace_duplicate_node_with_cte.
    """
    root = _create_mock_node("root_R")
    union1 = _create_mock_node("union1_U")
    union2 = _create_mock_node("union2_U2")
    df1 = _create_mock_node("same_S")
    df2 = _create_mock_node("same_S")
    df3 = _create_mock_node("same_S")
    root.children_plan_nodes = [union1, union2]
    union1.children_plan_nodes = [df1, df1]
    union2.children_plan_nodes = [df2, df3]

    with mock.patch(
        "snowflake.snowpark.context._is_snowpark_connect_compatible_mode", True
    ):
        duplicated_ids, _ = find_duplicate_subtrees(root)
    assert "same_S" in duplicated_ids


def test_encode_node_id_with_query_select_sql(mock_session, mock_analyzer):
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


def test_encode_node_id_with_query_includes_aliases():
    node = SimpleNamespace(
        sql_query="select col1 from t",
        query_params=(("p1", 1), ("p2", "x")),
        expr_to_alias={"uuid1": "ALIAS1"},
        df_aliased_col_name_to_real_col_name={"ALIAS1": "col1"},
    )

    def stringify_dict(d: dict) -> str:
        key_value_pairs = list(d.items())
        key_value_pairs.sort(key=lambda x: x[0])
        return str(key_value_pairs)

    expected_string = node.sql_query
    if node.query_params:
        expected_string = f"{expected_string}#{node.query_params}"
    if node.expr_to_alias:
        expected_string = f"{expected_string}#{stringify_dict(node.expr_to_alias)}"
    if node.df_aliased_col_name_to_real_col_name:
        expected_string = f"{expected_string}#{stringify_dict(node.df_aliased_col_name_to_real_col_name)}"

    expected_hash = hashlib.sha256(expected_string.encode()).hexdigest()[:10]
    assert encode_node_id_with_query(node) == f"{expected_hash}_SimpleNamespace"
