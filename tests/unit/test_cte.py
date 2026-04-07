#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import hashlib
from types import SimpleNamespace
from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.expression import FunctionExpression
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


def _make_mock_nodes(count, *, source_plans=None):
    """Helper to create mock SnowflakePlan nodes for CTE dedup tests."""
    nodes = [mock.create_autospec(SnowflakePlan) for _ in range(count)]
    for i, node in enumerate(nodes):
        node.encoded_node_id_with_query = f"{i}_{i}"
        node.source_plan = source_plans[i] if source_plans else None
        node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 3}
    return nodes


def test_find_duplicate_subtrees_excludes_data_generation_nodes():
    """Duplicate nodes that contain data generation expressions (e.g., uuid_string())
    should be excluded from CTE deduplication because Snowflake materializes CTEs,
    causing all references to share the same generated values."""
    nodes = _make_mock_nodes(5)
    # Tree:  0 -> [1, 2], 1 -> [3, 3], 2 -> [3, 3]
    # Node 3 appears 4 times with 2 different parents => duplicate
    nodes[0].children_plan_nodes = [nodes[1], nodes[2]]
    nodes[1].children_plan_nodes = [nodes[3], nodes[3]]
    nodes[2].children_plan_nodes = [nodes[3], nodes[3]]
    nodes[3].children_plan_nodes = [nodes[4]]
    nodes[4].children_plan_nodes = []

    # Without data generation: node 3 should be deduplicated
    duplicate_ids, _ = find_duplicate_subtrees(nodes[0])
    assert "3_3" in duplicate_ids

    # Now make node 3's source_plan a SelectStatement with a data generation expression
    mock_select = mock.create_autospec(SelectStatement)
    mock_select.contains_data_generation = True
    mock_select.projection = [mock.MagicMock()]
    nodes[3].source_plan = mock_select

    duplicate_ids, _ = find_duplicate_subtrees(nodes[0])
    assert (
        "3_3" not in duplicate_ids
    ), "Node with data generation expressions should not be deduplicated"


def test_find_duplicate_subtrees_propagates_data_gen_to_parents():
    """Parents of data-generation nodes should also be excluded from deduplication
    because deduplicating a parent would transitively share the generated values."""
    nodes = _make_mock_nodes(5)
    # Tree:  0 -> [1, 2], 1 -> [3], 2 -> [3]
    # Both node 1 and 2 reference node 3 (duplicate)
    # Node 1 and 2 also have the same encoded_id (both are "duplicates")
    nodes[1].encoded_node_id_with_query = "shared_shared"
    nodes[2].encoded_node_id_with_query = "shared_shared"
    nodes[0].children_plan_nodes = [nodes[1], nodes[2]]
    nodes[1].children_plan_nodes = [nodes[3]]
    nodes[2].children_plan_nodes = [nodes[3]]
    nodes[3].children_plan_nodes = []
    nodes[4].children_plan_nodes = []

    # Make node 3 a data generation node
    mock_select = mock.create_autospec(SelectStatement)
    mock_select.contains_data_generation = True
    mock_select.projection = [mock.MagicMock()]
    nodes[3].source_plan = mock_select

    duplicate_ids, _ = find_duplicate_subtrees(nodes[0])
    assert "3_3" not in duplicate_ids
    assert (
        "shared_shared" not in duplicate_ids
    ), "Parents of data-generation nodes should also be excluded"


def test_select_statement_contains_data_generation_projection(
    mock_session, mock_analyzer
):
    """SelectStatement.contains_data_generation detects data gen in projection."""
    base = SelectSQL(sql="select 1", analyzer=mock_analyzer)
    uuid_expr = FunctionExpression("uuid_string", [], is_distinct=False)

    stmt = SelectStatement(
        projection=[uuid_expr],
        from_=base,
        analyzer=mock_analyzer,
    )
    assert stmt.contains_data_generation is True


def test_select_statement_contains_data_generation_flagged_zero_args(
    mock_session, mock_analyzer
):
    """is_data_generator with zero args is non-deterministic and should be flagged."""
    base = SelectSQL(sql="select 1", analyzer=mock_analyzer)
    expr = FunctionExpression("my_func", [], is_distinct=False, is_data_generator=True)

    stmt = SelectStatement(
        projection=[expr],
        from_=base,
        analyzer=mock_analyzer,
    )
    assert stmt.contains_data_generation is True


def test_select_statement_data_generation_with_args_not_flagged(
    mock_session, mock_analyzer
):
    """Deterministic data generation functions with arguments (seq1(0), uniform(...))
    should NOT be flagged -- they produce the same results given the same input."""
    from snowflake.snowpark._internal.analyzer.expression import Literal

    base = SelectSQL(sql="select 1", analyzer=mock_analyzer)
    seq_expr = FunctionExpression(
        "seq1", [Literal(0)], is_distinct=False, is_data_generator=True
    )

    stmt = SelectStatement(
        projection=[seq_expr],
        from_=base,
        analyzer=mock_analyzer,
    )
    assert stmt.contains_data_generation is False


def test_select_statement_contains_data_generation_where(mock_session, mock_analyzer):
    """SelectStatement.contains_data_generation detects data gen in where clause."""
    base = SelectSQL(sql="select 1", analyzer=mock_analyzer)
    uuid_expr = FunctionExpression("uuid_string", [], is_distinct=False)

    stmt = SelectStatement(
        from_=base,
        where=uuid_expr,
        analyzer=mock_analyzer,
    )
    assert stmt.contains_data_generation is True


def test_select_statement_no_data_generation(mock_session, mock_analyzer):
    """SelectStatement.contains_data_generation is False for deterministic queries."""
    base = SelectSQL(sql="select 1", analyzer=mock_analyzer)

    stmt = SelectStatement(
        from_=base,
        analyzer=mock_analyzer,
    )
    assert stmt.contains_data_generation is False


def test_select_statement_contains_data_generation_cached(mock_session, mock_analyzer):
    """The contains_data_generation property should be lazily cached."""
    base = SelectSQL(sql="select 1", analyzer=mock_analyzer)
    uuid_expr = FunctionExpression("uuid_string", [], is_distinct=False)

    stmt = SelectStatement(
        projection=[uuid_expr],
        from_=base,
        analyzer=mock_analyzer,
    )
    assert stmt._contains_data_generation is None
    result = stmt.contains_data_generation
    assert result is True
    assert stmt._contains_data_generation is True
