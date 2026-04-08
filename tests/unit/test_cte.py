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
    has_nondeterministic_data_generation_exp,
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


# ---------------------------------------------------------------------------
# Tests for non-deterministic data-generation detection (CTE safety)
# ---------------------------------------------------------------------------


class TestHasNondeterministicDataGenerationExp:
    """Tests for has_nondeterministic_data_generation_exp."""

    def test_none_returns_false(self):
        assert has_nondeterministic_data_generation_exp(None) is False

    def test_empty_list_returns_false(self):
        assert has_nondeterministic_data_generation_exp([]) is False

    def test_zero_arg_uuid_string_flagged(self):
        expr = FunctionExpression(
            "uuid_string", [], is_distinct=False, is_data_generator=True
        )
        assert has_nondeterministic_data_generation_exp([expr]) is True

    def test_zero_arg_uuid_string_by_name(self):
        """Even without is_data_generator, a zero-arg call whose name is in the
        constant set should be flagged."""
        expr = FunctionExpression(
            "uuid_string", [], is_distinct=False, is_data_generator=False
        )
        assert has_nondeterministic_data_generation_exp([expr]) is True

    def test_two_arg_uuid_string_not_flagged(self):
        """uuid_string(uuid, name) is deterministic — UUID5."""
        arg1 = FunctionExpression("col", [], is_distinct=False)
        arg2 = FunctionExpression("col", [], is_distinct=False)
        expr = FunctionExpression(
            "uuid_string", [arg1, arg2], is_distinct=False, is_data_generator=False
        )
        assert has_nondeterministic_data_generation_exp([expr]) is False

    def test_seq1_with_args_not_flagged(self):
        """seq1(sign) has 1 arg → should NOT be flagged by the narrow check."""
        from snowflake.snowpark._internal.analyzer.expression import Literal

        sign_arg = Literal(1)
        expr = FunctionExpression(
            "seq1", [sign_arg], is_distinct=False, is_data_generator=True
        )
        assert has_nondeterministic_data_generation_exp([expr]) is False

    def test_nested_nondeterministic_child(self):
        """A parent function with args that wraps a zero-arg nondeterministic
        child should be caught by child recursion."""
        uuid_expr = FunctionExpression(
            "uuid_string", [], is_distinct=False, is_data_generator=True
        )
        wrapper = FunctionExpression(
            "upper", [uuid_expr], is_distinct=False, is_data_generator=False
        )
        assert has_nondeterministic_data_generation_exp([wrapper]) is True

    def test_nested_random_is_known_limitation(self):
        """random() without is_data_generator and not in the constant set
        is a known limitation — it won't be caught."""
        from snowflake.snowpark._internal.analyzer.expression import Literal

        random_expr = FunctionExpression(
            "random", [], is_distinct=False, is_data_generator=False
        )
        lo = Literal(1)
        hi = Literal(100)
        uniform_expr = FunctionExpression(
            "uniform", [lo, hi, random_expr], is_distinct=False, is_data_generator=True
        )
        assert has_nondeterministic_data_generation_exp([uniform_expr]) is False

    def test_zero_arg_randstr_flagged(self):
        expr = FunctionExpression(
            "randstr", [], is_distinct=False, is_data_generator=False
        )
        assert has_nondeterministic_data_generation_exp([expr]) is True


class TestContainsDataGeneration:
    """Tests for SelectStatement.contains_data_generation property."""

    def test_cached_property(self, mock_session, mock_analyzer):
        select_sql = SelectSQL(
            sql="select 1", convert_to_select=False, analyzer=mock_analyzer
        )
        stmt = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
        stmt.projection = None
        stmt.where = None
        stmt.order_by = None

        result1 = stmt.contains_data_generation
        result2 = stmt.contains_data_generation
        assert result1 is False
        assert result1 is result2

    def test_true_when_projection_has_uuid_string(self, mock_session, mock_analyzer):
        select_sql = SelectSQL(
            sql="select 1", convert_to_select=False, analyzer=mock_analyzer
        )
        stmt = SelectStatement(from_=select_sql, analyzer=mock_analyzer)
        stmt.projection = [
            FunctionExpression(
                "uuid_string", [], is_distinct=False, is_data_generator=True
            )
        ]
        stmt.where = None
        stmt.order_by = None
        assert stmt.contains_data_generation is True


class TestFindDuplicateSubtreesDataGen:
    """find_duplicate_subtrees should exclude nodes that contain
    non-deterministic data-generation expressions."""

    def test_data_gen_node_excluded_from_dedup(self):
        """Duplicate subtrees with data-generation expressions should NOT
        appear in the result set."""
        nodes = [mock.create_autospec(SnowflakePlan) for _ in range(4)]
        for i, node in enumerate(nodes):
            node.encoded_node_id_with_query = f"{i}_{i}"
            node.source_plan = None
            node.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 3}

        # Build tree:  root -> [left, right], left -> [leaf], right -> [leaf]
        # leaf is duplicated and would normally be a CTE candidate.
        nodes[0].children_plan_nodes = [nodes[1], nodes[2]]
        nodes[1].children_plan_nodes = [nodes[3]]
        nodes[2].children_plan_nodes = [nodes[3]]
        nodes[3].children_plan_nodes = []

        # Without data generation → leaf is dedup candidate
        dup_ids, _ = find_duplicate_subtrees(nodes[0])
        assert "3_3" in dup_ids

        # Now make leaf a SelectStatement with data generation
        select_stmt = mock.create_autospec(SelectStatement)
        select_stmt.encoded_node_id_with_query = "3_3"
        select_stmt.source_plan = None
        select_stmt.children_plan_nodes = []
        select_stmt.cumulative_node_complexity = {PlanNodeCategory.COLUMN: 3}
        select_stmt.contains_data_generation = True
        select_stmt.projection = None
        select_stmt.from_ = mock.MagicMock()

        nodes[1].children_plan_nodes = [select_stmt]
        nodes[2].children_plan_nodes = [select_stmt]

        dup_ids, _ = find_duplicate_subtrees(nodes[0])
        assert "3_3" not in dup_ids

    def test_data_gen_propagates_to_parents(self):
        """When a child is invalid due to data generation, its parents should
        also be marked invalid (same as SelectFromFileNode behaviour)."""
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

        # Make leaf a data-gen SelectStatement
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
        # Both mid (3_3) and leaf (4_4) should be excluded
        assert "3_3" not in dup_ids
        assert "4_4" not in dup_ids
