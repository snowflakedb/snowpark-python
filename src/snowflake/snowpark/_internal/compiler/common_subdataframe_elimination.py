#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from typing import Dict, List, Set, Union

from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectSQL,
    SelectStatement,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    WithObjectRef,
    WithQueryBlock,
)
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)

# common sub-dataframe elimination only operates on top
TreeNode = Union[SnowflakePlan, Selectable]


class CommonSubDataframeElimination:
    _logical_plans: List[LogicalPlan]
    _node_count_map: Dict[TreeNode, int]
    _node_parents_map: Dict[TreeNode, Set[TreeNode]]
    _duplicated_nodes: Set[TreeNode]
    _query_generator: QueryGenerator

    def __init__(
        self, logical_plans: List[LogicalPlan], query_generator: QueryGenerator
    ) -> None:
        self._logical_plans = logical_plans
        self._query_generator = query_generator
        self._node_count_map = defaultdict(int)
        self._node_parents_map = defaultdict(set)
        self._duplicated_nodes = set()

    def run(self) -> List[LogicalPlan]:
        final_logical_plans = []
        for logical_plan in self._logical_plans:
            # clear the node_count and parents map
            self._node_count_map = defaultdict(int)
            self._node_parents_map = defaultdict(set)

            self._duplicated_nodes.update(self._find_duplicate_subtrees(logical_plan))
            deduplicated_plan = self._deduplicate_with_cte(logical_plan)
            final_logical_plans.append(deduplicated_plan)

        return final_logical_plans

    def _find_duplicate_subtrees(self, root: "TreeNode") -> Set["TreeNode"]:
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

        def _traverse(root: "TreeNode") -> None:
            """
            This function uses an iterative approach to avoid hitting Python's maximum recursion depth limit.
            """
            current_level = [root]
            while len(current_level) > 0:
                next_level = []
                for node in current_level:
                    self._node_count_map[node] += 1
                    for child in node.children_plan_nodes:
                        self._node_parents_map[child].add(node)
                        next_level.append(child)
                current_level = next_level

        def _is_duplicate_subtree(node: "TreeNode") -> bool:
            is_duplicate_node = self._node_count_map[node] > 1
            if is_duplicate_node:
                is_any_parent_unique_node = any(
                    self._node_count_map[parent] == 1
                    for parent in self._node_parents_map[node]
                )
                if is_any_parent_unique_node:
                    return True
                else:
                    has_multi_parents = len(self._node_parents_map[node]) > 1
                    if has_multi_parents:
                        return True
            return False

        _traverse(root)
        return {node for node in self._node_count_map if _is_duplicate_subtree(node)}

    def _reset_node(self, node: LogicalPlan) -> None:
        def reset_selectable(selectable_node: Selectable) -> None:
            # clear the cache to allow it re-calculate the _snowflake_plan and _sql_query
            if not isinstance(node, (SelectSnowflakePlan, SelectSQL)):
                selectable_node._snowflake_plan = None
            if isinstance(node, (SelectStatement, SetStatement)):
                selectable_node._sql_query = None

        if isinstance(node, SnowflakePlan):
            # do not reset leaf snowflake plan
            if node.source_plan is not None:
                node.queries = None
                node.post_actions = None
                if isinstance(node.source_plan, Selectable):
                    reset_selectable(node.source_plan)
        elif isinstance(node, Selectable):
            reset_selectable(node)

    def _update_parents(self, node: "TreeNode", new_node: "TreeNode") -> None:
        parents = self._node_parents_map[node]
        for parent in parents:
            parent.update_child(node, new_node)

            # reset the parent to resolve it later
            # self._reset_node(parent)

    def _deduplicate_with_cte(self, root: "TreeNode") -> LogicalPlan:
        """
        Transform

        Parameters
        ----------
        root

        Returns
        -------

        """
        stack1, stack2 = [root], []

        while stack1:
            node = stack1.pop()
            stack2.append(node)
            for child in reversed(node.children_plan_nodes):
                stack1.append(child)

        visited_nodes = set()
        while stack2:
            node = stack2.pop()
            if node in visited_nodes:
                continue
            # if the node is a duplicated node and deduplication is not done for the node,
            # start the deduplication transformation use CTE
            if node in self._duplicated_nodes:
                # create a WithQueryBlock node
                with_block = WithQueryBlock(
                    name=random_name_for_temp_object(TempObjectType.CTE), child=node
                )
                # add a projection on top of it
                project_plan = WithObjectRef(child=with_block)
                # resolve the project_plan to get a snowflake plan
                resolved_project = self._query_generator.resolve(project_plan)

                self._update_parents(node, resolved_project)

            elif node not in self._duplicated_nodes:
                # self._reset_node(node)
                if isinstance(node, SelectSnowflakePlan):
                    # resolve the snowflake plan attached
                    # self._reset_node(node._snowflake_plan)
                    new_snowflake_plan = self._query_generator.resolve(
                        node._snowflake_plan
                    )
                    node._snowflake_plan = new_snowflake_plan

                elif isinstance(node, SnowflakePlan):
                    if node.source_plan is not None:
                        # update the snowflake plan to make it a valid plan node
                        new_snowflake_plan = self._query_generator.resolve(
                            node.source_plan
                        )
                        if node == root:
                            root = new_snowflake_plan
                        else:
                            self._update_parents(node, new_snowflake_plan)
                else:
                    self._reset_node(node)

            visited_nodes.add(node)

        return root
