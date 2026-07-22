#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import hashlib
from types import SimpleNamespace
from unittest import mock

import pytest

from snowflake.snowpark.functions import (
    uuid_string,
    col,
    upper,
    seq1,
    uniform,
    normal,
    random as random_,
    builtin,
)
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
from snowflake.snowpark._internal.compiler.repeated_subquery_elimination import (
    RepeatedSubqueryElimination,
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
    # expr_to_alias is hashed by sorted(set(values())) so two nodes with the
    # same alias values but different UUID keys (e.g. deep-copied self-join
    # branches) produce the same hash.
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
        # Values-only sort (no UUID keys) normalizes away UUID differences
        expected_string = (
            f"{expected_string}#{sorted(set(node.expr_to_alias.values()))}"
        )
    if node.df_aliased_col_name_to_real_col_name:
        expected_string = f"{expected_string}#{stringify_dict(node.df_aliased_col_name_to_real_col_name)}"

    expected_hash = hashlib.sha256(expected_string.encode()).hexdigest()[:10]
    assert encode_node_id_with_query(node) == f"{expected_hash}_SimpleNamespace"

    # Two nodes with the same SQL and same alias values but different UUID keys
    # must hash identically — this is the Q39 self-join case.
    node_same_values_diff_keys = SimpleNamespace(
        sql_query="select col1 from t",
        query_params=(("p1", 1), ("p2", "x")),
        expr_to_alias={"uuid_different": "ALIAS1"},
        df_aliased_col_name_to_real_col_name={"ALIAS1": "col1"},
    )
    assert encode_node_id_with_query(node) == encode_node_id_with_query(
        node_same_values_diff_keys
    )

    # Two nodes with the same SQL but different alias values must hash
    # differently — this preserves the SNOW-2261400 join-suffix fix.
    node_different_values = SimpleNamespace(
        sql_query="select col1 from t",
        query_params=(("p1", 1), ("p2", "x")),
        expr_to_alias={"uuid1": "ALIAS1_WITH_SUFFIX"},
        df_aliased_col_name_to_real_col_name={"ALIAS1": "col1"},
    )
    assert encode_node_id_with_query(node) != encode_node_id_with_query(
        node_different_values
    )


def test_has_alias_conflict():
    # encode_query_id hashes expr_to_alias by alias values only, so two nodes can
    # share a CTE while carrying different expr_id keys. _has_alias_conflict guards
    # the only unsafe case: the same expr_id mapping to a *different* alias, where
    # merging would silently drop an entry and corrupt parent column resolution.
    has_conflict = RepeatedSubqueryElimination._has_alias_conflict

    node = SimpleNamespace(expr_to_alias={"uuid1": "ALIAS1"})

    # No existing CTE yet (first occurrence) -> nothing to conflict with.
    assert has_conflict(node, None) is False

    # Same expr_id mapped to the same alias -> safe to merge.
    existing_same = SimpleNamespace(expr_to_alias={"uuid1": "ALIAS1"})
    assert has_conflict(node, existing_same) is False

    # Disjoint expr_id keys (the normal self-join case: same alias values, fresh
    # UUIDs) -> no conflict, the entries simply coexist after merge.
    existing_disjoint = SimpleNamespace(expr_to_alias={"uuid2": "ALIAS1"})
    assert has_conflict(node, existing_disjoint) is False

    # Same expr_id mapped to a *different* alias -> conflict, must not share CTE.
    existing_conflict = SimpleNamespace(expr_to_alias={"uuid1": "ALIAS2"})
    assert has_conflict(node, existing_conflict) is True

    # A conflict on any one key is enough, even when other keys agree.
    node_multi = SimpleNamespace(expr_to_alias={"uuid1": "ALIAS1", "uuid2": "ALIAS2"})
    existing_partial_conflict = SimpleNamespace(
        expr_to_alias={"uuid1": "ALIAS1", "uuid2": "DIFFERENT"}
    )
    assert has_conflict(node_multi, existing_partial_conflict) is True

    # Node without any expr_to_alias entries can never conflict.
    node_empty = SimpleNamespace(expr_to_alias={})
    assert has_conflict(node_empty, existing_conflict) is False


def test_select_statement_contains_data_generation(mock_session, mock_analyzer):
    """SelectStatement.contains_data_generation should detect zero-arg
    nondeterministic functions in projection, where, and order_by."""
    select_sql = SelectSQL(
        sql="select 1", convert_to_select=False, analyzer=mock_analyzer
    )

    # No data generation — regular function
    stmt = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
    stmt.projection = [upper(col("a"))._expression]
    assert stmt.contains_data_generation is False

    # uuid_string() zero-arg in projection — nondeterministic
    stmt2 = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
    stmt2.projection = [uuid_string()._expression]
    assert stmt2.contains_data_generation is True

    # uuid_string(uuid, name) with args — deterministic, not flagged
    stmt3 = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
    stmt3.projection = [uuid_string(col("uuid"), col("name"))._expression]
    assert stmt3.contains_data_generation is False

    # seq1(1) — deterministic row-local counter, safe to dedup
    stmt4 = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
    stmt4.projection = [seq1(1)._expression]
    assert stmt4.contains_data_generation is False

    # Snowpark's random() always generates a seed internally (line 1903 of
    # functions.py), so the FunctionExpression always has 1 child — it's never
    # truly zero-arg.  This makes it deterministic per-query and safe to dedup.
    # uniform/normal with random() gen inherit this behavior.
    stmt5 = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
    stmt5.projection = [uniform(1, 100, random_())._expression]
    assert stmt5.contains_data_generation is False

    stmt6 = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
    stmt6.projection = [normal(0, 1, random_())._expression]
    assert stmt6.contains_data_generation is False

    # builtin("random")() produces a true zero-arg FunctionExpression —
    # caught by name in NONDETERMINISTIC_DATA_GENERATION via child recursion
    stmt7 = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
    bare_random = builtin("random")()
    stmt7.projection = [uniform(1, 100, bare_random)._expression]
    assert stmt7.contains_data_generation is True


def test_find_duplicate_subtrees_excludes_data_gen_nodes():
    """Duplicate subtrees with data-generation expressions should NOT be
    deduplicated, and the invalidation should propagate to parents."""
    nodes = [mock.create_autospec(SnowflakePlan) for _ in range(5)]
    for i, node in enumerate(nodes):
        node.encoded_node_id_with_query = f"{i}_{i}"
        node.source_plan = None
        node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 3}

    # root -> [a, b], a -> [mid], b -> [mid], mid -> [leaf]
    nodes[0].children_plan_nodes = [nodes[1], nodes[2]]
    nodes[1].children_plan_nodes = [nodes[3]]
    nodes[2].children_plan_nodes = [nodes[3]]
    nodes[3].children_plan_nodes = [nodes[4]]
    nodes[4].children_plan_nodes = []

    # Without data gen: mid (3_3) is a dedup candidate
    dup_ids, _ = find_duplicate_subtrees(nodes[0])
    assert "3_3" in dup_ids

    # Make leaf a SelectStatement with data generation
    leaf = mock.create_autospec(SelectStatement)
    leaf.encoded_node_id_with_query = "4_4"
    leaf.source_plan = None
    leaf.children_plan_nodes = []
    leaf.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 3}
    leaf.contains_data_generation = True
    leaf.projection = None
    leaf.from_ = mock.MagicMock()

    nodes[3].children_plan_nodes = [leaf]

    dup_ids, _ = find_duplicate_subtrees(nodes[0])
    # Both mid (3_3) and leaf (4_4) should be excluded via propagation
    assert "3_3" not in dup_ids
    assert "4_4" not in dup_ids
