#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import hashlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Optional, Set, Union

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    SPACE,
    cte_statement,
    project_statement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import WithQueryBlock
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    is_sql_select_statement,
    random_name_for_temp_object,
)

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.select_statement import Selectable
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan

    TreeNode = Union[SnowflakePlan, Selectable]


def find_duplicate_subtrees(root: "TreeNode") -> Set[str]:
    """
    Returns a set of TreeNode encoded_id that indicates all duplicate subtrees in query plan tree.
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
    id_count_map = defaultdict(int)
    id_parents_map = defaultdict(set)

    def traverse(root: "TreeNode") -> None:
        """
        This function uses an iterative approach to avoid hitting Python's maximum recursion depth limit.
        """
        current_level = [root]
        while len(current_level) > 0:
            next_level = []
            for node in current_level:
                id_count_map[node.encoded_node_id_with_query] += 1
                for child in node.children_plan_nodes:
                    id_parents_map[child.encoded_node_id_with_query].add(
                        node.encoded_node_id_with_query
                    )
                    next_level.append(child)
            current_level = next_level

    def is_duplicate_subtree(encoded_node_id_with_query: str) -> bool:
        is_duplicate_node = id_count_map[encoded_node_id_with_query] > 1
        if is_duplicate_node:
            is_any_parent_unique_node = any(
                id_count_map[id] == 1
                for id in id_parents_map[encoded_node_id_with_query]
            )
            if is_any_parent_unique_node:
                return True
            else:
                has_multi_parents = len(id_parents_map[encoded_node_id_with_query]) > 1
                if has_multi_parents:
                    return True
        return False

    traverse(root)
    duplicated_node = {
        encoded_node_id_with_query
        for encoded_node_id_with_query in id_count_map
        if is_duplicate_subtree(encoded_node_id_with_query)
    }
    return duplicated_node


def create_cte_query(root: "TreeNode", duplicated_node_ids: Set[str]) -> str:
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
            if node.encoded_node_id_with_query in plan_to_query_map:
                continue

            if not node.children_plan_nodes or not node.placeholder_query:
                plan_to_query_map[node.encoded_node_id_with_query] = (
                    node.sql_query
                    if isinstance(node, Selectable)
                    else node.queries[-1].sql
                )
            else:
                plan_to_query_map[
                    node.encoded_node_id_with_query
                ] = node.placeholder_query
                for child in node.children_plan_nodes:
                    # replace the placeholder (id) with child query
                    plan_to_query_map[
                        node.encoded_node_id_with_query
                    ] = plan_to_query_map[node.encoded_node_id_with_query].replace(
                        child.encoded_query_id,
                        plan_to_query_map[child.encoded_node_id_with_query],
                    )

            # duplicate subtrees will be converted CTEs
            if node.encoded_node_id_with_query in duplicated_node_ids:
                # when a subquery is converted a CTE to with clause,
                # it will be replaced by `SELECT * from TEMP_TABLE` in the original query
                table_name = random_name_for_temp_object(TempObjectType.CTE)
                select_stmt = project_statement([], table_name)
                duplicate_plan_to_table_name_map[
                    node.encoded_node_id_with_query
                ] = table_name
                duplicate_plan_to_cte_map[
                    node.encoded_node_id_with_query
                ] = plan_to_query_map[node.encoded_node_id_with_query]
                plan_to_query_map[node.encoded_node_id_with_query] = select_stmt

    build_plan_to_query_map_in_post_order(root)

    # construct with clause
    with_stmt = cte_statement(
        list(duplicate_plan_to_cte_map.values()),
        list(duplicate_plan_to_table_name_map.values()),
    )
    final_query = with_stmt + SPACE + plan_to_query_map[root.encoded_node_id_with_query]
    return final_query


def merge_referenced_ctes(
    ref1: Dict[WithQueryBlock, int], ref2: Dict[WithQueryBlock, int]
) -> Dict[WithQueryBlock, int]:
    """Utility function to merge two referenced_cte dictionaries"""
    merged = ref1.copy()
    for with_query_block, value in ref2.items():
        if with_query_block in merged:
            merged[with_query_block] += value
        else:
            merged[with_query_block] = value
    return merged


def encoded_query_id(node) -> Optional[str]:
    """
    Encode the query and its query parameter into an id using sha256.


    Returns:
        If encode succeed, return the first 10 encoded value.
        Otherwise, return None
    """
    from snowflake.snowpark._internal.analyzer.select_statement import SelectSQL
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan

    if isinstance(node, SnowflakePlan):
        query = node.queries[-1].sql
        query_params = node.queries[-1].params
    elif isinstance(node, SelectSQL):
        # For SelectSql, The original SQL is used to encode its ID,
        # which might be a non-select SQL.
        query = node.original_sql
        query_params = node.query_params
    else:
        query = node.sql_query
        query_params = node.query_params

    if not is_sql_select_statement(query):
        # common subquery elimination only supports eliminating
        # subquery that is select statement. Skip encoding the query
        # to avoid being detected as a common subquery.
        return None

    string = f"{query}#{query_params}" if query_params else query
    try:
        return hashlib.sha256(string.encode()).hexdigest()[:10]
    except Exception as ex:
        logging.warning(f"Encode SnowflakePlan ID failed: {ex}")
        return None


def encode_node_id_with_query(node: "TreeNode") -> str:
    """
    Encode a for the given TreeNode.

    If query and query parameters can be encoded successfully using sha256,
    return the encoded query id + node_type_name.
    Otherwise, return the original node id.
    """
    query_id = encoded_query_id(node)
    if query_id is not None:
        node_type_name = type(node).__name__
        return f"{query_id}_{node_type_name}"
    else:
        return str(id(node))
