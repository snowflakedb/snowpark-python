#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import hashlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Optional, Sequence, Set, Union

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
    from snowflake.snowpark._internal.analyzer.select_statement import Selectable
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan

    TreeNode = Union[SnowflakePlan, Selectable]


def find_duplicate_subtrees(root: "TreeNode") -> Set["TreeNode"]:
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
    from snowflake.snowpark._internal.analyzer.select_statement import Selectable

    node_count_map = defaultdict(int)
    node_parents_map = defaultdict(set)

    def traverse(root: "TreeNode") -> None:
        """
        This function uses an iterative approach to avoid hitting Python's maximum recursion depth limit.
        """
        current_level = [root]
        while len(current_level) > 0:
            next_level = []
            for node in current_level:
                # all subqueries under dynamic pivot node will not be optimized now
                # due to a server side bug
                # TODO: SNOW-1413967 Remove it when the bug is fixed
                if is_dynamic_pivot_node(node):
                    continue
                node_count_map[node] += 1
                for child in node.children_plan_nodes:
                    # converting non-SELECT child query to SELECT query here,
                    # so we can further optimize
                    if isinstance(child, Selectable):
                        child = child.to_subqueryable()
                    node_parents_map[child].add(node)
                    next_level.append(child)
            current_level = next_level

    def is_duplicate_subtree(node: "TreeNode") -> bool:
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

    def is_dynamic_pivot_node(node: "TreeNode") -> bool:
        from snowflake.snowpark._internal.analyzer.select_statement import (
            SelectSnowflakePlan,
        )
        from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
        from snowflake.snowpark._internal.analyzer.unary_plan_node import Pivot

        if isinstance(node, SelectSnowflakePlan):
            source_plan = node.snowflake_plan.source_plan
        elif isinstance(node, SnowflakePlan):
            source_plan = node.source_plan
        else:
            return False
        return isinstance(source_plan, Pivot) and source_plan.pivot_values is None

    traverse(root)
    return {node for node in node_count_map if is_duplicate_subtree(node)}


def create_cte_query(root: "TreeNode", duplicate_plan_set: Set["TreeNode"]) -> str:
    from snowflake.snowpark._internal.analyzer.select_statement import Selectable

    plan_to_query_map = {}
    duplicate_plan_to_cte_map = {}
    duplicate_plan_to_table_name_map = {}

    def build_plan_to_query_map_in_post_order(root: "TreeNode") -> None:
        """
        Builds a mapping from query plans to queries that are optimized with CTEs,
        in post-traversal order. We can get the final query from the mapping value of the root node.
        The reason of using poster-traversal order is that chained CTEs have to be built
        from bottom (innermost subquery) to top (outermost query).
        This function uses an iterative approach to avoid hitting Python's maximum recursion depth limit.
        """
        stack1, stack2 = [root], []

        while stack1:
            node = stack1.pop()
            stack2.append(node)
            for child in reversed(node.children_plan_nodes):
                stack1.append(child)

        while stack2:
            node = stack2.pop()
            if node in plan_to_query_map:
                continue

            if not node.children_plan_nodes or not node.placeholder_query:
                plan_to_query_map[node] = (
                    node.sql_query
                    if isinstance(node, Selectable)
                    else node.queries[-1].sql
                )
            else:
                plan_to_query_map[node] = node.placeholder_query
                for child in node.children_plan_nodes:
                    if isinstance(child, Selectable):
                        child = child.to_subqueryable()
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

    build_plan_to_query_map_in_post_order(root)

    # construct with clause
    with_stmt = cte_statement(
        list(duplicate_plan_to_cte_map.values()),
        list(duplicate_plan_to_table_name_map.values()),
    )
    final_query = with_stmt + SPACE + plan_to_query_map[root]
    return final_query


def encode_id(
    query: str, query_params: Optional[Sequence[Any]] = None
) -> Optional[str]:
    string = f"{query}#{query_params}" if query_params else query
    try:
        return hashlib.sha256(string.encode()).hexdigest()[:10]
    except Exception as ex:
        logging.warning(f"Encode SnowflakePlan ID failed: {ex}")
        return None
