#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from typing import TYPE_CHECKING, Set

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    SPACE,
    cte_statement,
    project_statement,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan


def find_duplicate_subtrees(root: "SnowflakePlan") -> Set["SnowflakePlan"]:
    """
    Returns a set containing all duplicate subtrees in query plan tree.
    The root of a duplicate subtree is defined as a duplicate node, if
        - it appears more than once in the tree, AND
        - one of its parent is unique (only appear once) in the tree, OR
        - it has multiple different parents

    For example,
                      root
                     /    \
                   df5   df6
                /   |     |   \
              df3  df3   df4  df4
               |    |     |    |
              df2  df2   df2  df2
               |    |     |    |
              df1  df1   df1  df1

    df4, df3 and df2 are duplicate subtrees.

    This function is used to only include nodes that should be converted to CTEs.
    """
    node_count_map = defaultdict(int)
    node_parents_map = defaultdict(set)

    def traverse(node: "SnowflakePlan") -> None:
        node_count_map[node] += 1
        if node.source_plan and node.source_plan.children:
            for child in node.source_plan.children:
                node_parents_map[child].add(node)
                traverse(child)

    def is_duplicate_subtree(node: "SnowflakePlan") -> bool:
        is_duplicate_node = node_count_map[node] > 1
        if is_duplicate_node:
            is_any_parent_unique_node = any(
                node_count_map[n] == 1 for n in node_parents_map[node]
            )
            if is_any_parent_unique_node:
                return True
            else:
                has_multi_parents = len(node_parents_map[node]) > 1
                if has_multi_parents:
                    return True
        return False

    traverse(root)
    return {node for node in node_count_map if is_duplicate_subtree(node)}


def create_cte_query(
    node: "SnowflakePlan", duplicate_plan_set: Set["SnowflakePlan"]
) -> str:
    plan_to_query_map = {}
    duplicate_plan_to_cte_map = {}
    duplicate_plan_to_table_name_map = {}

    def build_plan_to_query_map_in_post_order(node: "SnowflakePlan") -> None:
        """
        Builds a mapping from query plans to queries that are optimized with CTEs,
        in post-traversal order. We can get the final query from the mapping value of the root node.
        The reason of using poster-traversal order is that chained CTEs have to be built
        from bottom (innermost subquery) to top (outermost query).
        """
        if not node.source_plan or node in plan_to_query_map:
            return

        for child in node.source_plan.children:
            build_plan_to_query_map_in_post_order(child)

        if not node.placeholder_query:
            plan_to_query_map[node] = node.queries[-1].sql
        else:
            plan_to_query_map[node] = node.placeholder_query
            for child in node.source_plan.children:
                # replace the placeholder (id) with child query
                plan_to_query_map[node] = plan_to_query_map[node].replace(
                    child._id, plan_to_query_map[child]
                )

        # duplicate subtrees will be converted CTEs
        if node in duplicate_plan_set:
            # when a subquery is converted a CTE to with clause,
            # it will be replaced by `SELECT * from TEMP_TABLE` in the original query
            table_name = random_name_for_temp_object(TempObjectType.CTE)
            select_stmt = project_statement([], table_name)
            duplicate_plan_to_table_name_map[node] = table_name
            duplicate_plan_to_cte_map[node] = plan_to_query_map[node]
            plan_to_query_map[node] = select_stmt

    build_plan_to_query_map_in_post_order(node)

    # construct with clause
    with_stmt = cte_statement(
        list(duplicate_plan_to_cte_map.values()),
        list(duplicate_plan_to_table_name_map.values()),
    )
    final_query = with_stmt + SPACE + plan_to_query_map[node]
    return final_query
