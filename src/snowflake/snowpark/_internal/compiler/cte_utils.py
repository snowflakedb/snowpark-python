#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import hashlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import WithQueryBlock
from snowflake.snowpark._internal.utils import is_sql_select_statement

if TYPE_CHECKING:
    from snowflake.snowpark._internal.compiler.utils import TreeNode  # pragma: no cover


def find_duplicate_subtrees(
    root: "TreeNode", propagate_complexity_hist: bool = False
) -> Tuple[Set[str], Optional[List[int]]]:
    """
    Returns a set of TreeNode encoded_id that indicates all duplicate subtrees in query plan tree,
    and the distribution of duplicated node complexity in the tree if propagate_complexity_hist is true.


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
    id_complexity_map = defaultdict(int)

    def traverse(root: "TreeNode") -> None:
        """
        This function uses an iterative approach to avoid hitting Python's maximum recursion depth limit.
        """
        current_level = [root]
        while len(current_level) > 0:
            next_level = []
            for node in current_level:
                id_count_map[node.encoded_node_id_with_query] += 1
                if propagate_complexity_hist and (
                    node.encoded_node_id_with_query not in id_complexity_map
                ):
                    # if propagate_complexity_hist is true, and the complexity score is not
                    # recorded for the current node id, record the complexity
                    id_complexity_map[
                        node.encoded_node_id_with_query
                    ] = get_complexity_score(node)
                for child in node.children_plan_nodes:
                    id_parents_map[child.encoded_node_id_with_query].add(
                        node.encoded_node_id_with_query
                    )
                    next_level.append(child)
            current_level = next_level

    def is_duplicate_subtree(encoded_node_id_with_query: str) -> bool:
        # when a sql query is a select statement, its encoded_node_id_with_query
        # contains _, which is used to separate the query id and node type name.
        is_valid_candidate = "_" in encoded_node_id_with_query
        if not is_valid_candidate:
            return False

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
    duplicated_node_ids = {
        encoded_node_id_with_query
        for encoded_node_id_with_query in id_count_map
        if is_duplicate_subtree(encoded_node_id_with_query)
    }

    if propagate_complexity_hist:
        return (
            duplicated_node_ids,
            get_duplicated_node_complexity_distribution(
                duplicated_node_ids, id_complexity_map, id_count_map
            ),
        )
    else:
        return (duplicated_node_ids, None)


def get_duplicated_node_complexity_distribution(
    duplicated_node_id_set: Set[str],
    id_complexity_map: Dict[str, int],
    id_count_map: Dict[str, int],
) -> List[int]:
    """
    Calculate the complexity distribution for the detected repeated node. The complexity are categorized as following:
    1) low complexity
        bin 0: <= 10,000; bin 1: > 10,000, <= 100,000; bin 2: > 100,000, <= 500,000
    2) medium complexity
        bin 3: > 500,000, <= 1,000,000;  bin 4: > 1,000,000, <= 5,000,000
    4) large complexity
        bin 5: > 5,000,000, <= 10,000,000;  bin 6: > 10,000,000

    Returns:
        A list with size 7, each element corresponds number of repeated nodes with complexity falls into the bin.
    """
    node_complexity_dist = [0] * 7
    for node_id in duplicated_node_id_set:
        complexity_score = id_complexity_map[node_id]
        repeated_count = id_count_map[node_id]
        if complexity_score <= 10000:
            node_complexity_dist[0] += repeated_count
        elif 10000 < complexity_score <= 100000:
            node_complexity_dist[1] += repeated_count
        elif 100000 < complexity_score <= 500000:
            node_complexity_dist[2] += repeated_count
        elif 500000 < complexity_score <= 1000000:
            node_complexity_dist[3] += repeated_count
        elif 1000000 < complexity_score <= 5000000:
            node_complexity_dist[4] += repeated_count
        elif 5000000 < complexity_score <= 10000000:
            node_complexity_dist[5] += repeated_count
        elif complexity_score > 10000000:
            node_complexity_dist[6] += repeated_count

    return node_complexity_dist


def encode_query_id(node) -> Optional[str]:
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
    query_id = encode_query_id(node)
    if query_id is not None:
        node_type_name = type(node).__name__
        return f"{query_id}_{node_type_name}"
    else:
        return str(id(node))


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
