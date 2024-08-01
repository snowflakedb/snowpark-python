#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from typing import Dict, List, Set, Union

from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectTableFunction,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    WithQueryBlock,
)
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.compiler.utils import (
    replace_child_and_reset_node,
    reset_node,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)

# common sub-dataframe elimination only operates on top
TreeNode = Union[SnowflakePlan, Selectable]


class CommonSubDataframeElimination:
    """
    Optimization that used eliminate duplicated queries that is generated for the
    common sub-dataframes.

    When the same dataframe is used at multiple places of the plan, the same subquery
    will be generated at each place where it is used, this lead to repeated evaluation
    of the same subquery, and causes extra performance overhead. This optimization targets
    for detecting the common sub-dataframes, and uses CTE to eliminate the repeated
    subquery generated.
    For example:
       df = session.table("test_table")
       df1 = df1.select("a", "b")
       df2 = df1.union_all(df1)
    originally the generated query for df2 is
        (select "a", "b" from "test_table") union all (select "a", "b" from "test_table")
    after the optimization, the generated query becomes
        with temp_cte_xxx as (select "a", "b" from "test_table")
        (select * from temp_cte_xxx) union all (select * from select * from temp_cte_xxx)
    """

    # original logical plans to apply the optimization on
    _logical_plans: List[LogicalPlan]
    # record the occurrence of each node in a logical plan tree
    _node_count_map: Dict[TreeNode, int]
    # record a map between a node and its parents for a logical plan tree
    _node_parents_map: Dict[TreeNode, Set[TreeNode]]
    # duplicated node detected for a logical plan
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

    def apply(self) -> List[LogicalPlan]:
        """
        Applies Common SubDataframe elimination on the set of logical plans one after another.

        Returns
        -------
        A set of the new LogicalPlans with common sub dataframe deduplicated with CTE node.
        """
        final_logical_plans: List[LogicalPlan] = []
        for logical_plan in self._logical_plans:
            # clear the node_count and parents map
            self._node_count_map = defaultdict(int)
            self._node_parents_map = defaultdict(set)
            self._duplicated_nodes.clear()

            # NOTE: the current common sub-dataframe elimination relies on the
            # fact that all intermediate steps are resolved properly. Here we
            # do a pass of resolve of the logical plan to make sure we get a valid
            # resolved plan to start the process.
            # If the plan is already a resolved plan, this step will be a no-op.
            logical_plan = self._query_generator.resolve(logical_plan)

            # apply the CTE optimization on the resolved plan
            self._find_duplicate_subtrees(logical_plan)
            if len(self._duplicated_nodes) > 0:
                deduplicated_plan = self._deduplicate_with_cte(logical_plan)
                final_logical_plans.append(deduplicated_plan)
            else:
                final_logical_plans.append(logical_plan)

        # TODO (SNOW-1566363): Add telemetry for CTE
        return final_logical_plans

    def _find_duplicate_subtrees(self, root: TreeNode) -> None:
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

        def _traverse(root: TreeNode) -> None:
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

        duplicated_nodes = {
            node for node in self._node_count_map if _is_duplicate_subtree(node)
        }
        self._duplicated_nodes.update(duplicated_nodes)

    def _update_parents(self, node: TreeNode, new_node: TreeNode) -> None:
        parents = self._node_parents_map[node]
        for parent in parents:
            replace_child_and_reset_node(parent, node, new_node, self._query_generator)

    def _resolve_node(self, node: LogicalPlan) -> SnowflakePlan:
        resolved_node = self._query_generator.resolve(node)
        resolved_node._is_valid_for_replacement = True

        return resolved_node

    def _deduplicate_with_cte(self, root: "TreeNode") -> LogicalPlan:
        """
        Replace all duplicated nodes with a WithQueryBlock (CTE node), to enable
        query generation with CTEs.
        """

        stack1, stack2 = [root], []

        while stack1:
            node = stack1.pop()
            stack2.append(node)
            for child in reversed(node.children_plan_nodes):
                stack1.append(child)

        # tack node that is already visited to avoid repeated operation on the same node
        visited_nodes: Set[TreeNode] = set()
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
                with_block._is_valid_for_replacement = True

                resolved_with_block = self._resolve_node(with_block)
                self._update_parents(node, resolved_with_block)

            elif node not in self._duplicated_nodes:
                if isinstance(node, (SelectSnowflakePlan, SelectTableFunction)):
                    # resolve the snowflake plan attached to make sure the
                    # select SnowflakePlan is a valid plan
                    if node._snowflake_plan.source_plan is not None:
                        new_snowflake_plan = self._resolve_node(
                            node._snowflake_plan.source_plan
                        )
                        node._snowflake_plan = new_snowflake_plan

                elif isinstance(node, SnowflakePlan):
                    if node.source_plan is not None:
                        # update the snowflake plan to make it a valid plan node
                        new_snowflake_plan = self._resolve_node(node.source_plan)
                        if node == root:
                            root = new_snowflake_plan
                        else:
                            self._update_parents(node, new_snowflake_plan)
                else:
                    reset_node(node)

            visited_nodes.add(node)

        return root
