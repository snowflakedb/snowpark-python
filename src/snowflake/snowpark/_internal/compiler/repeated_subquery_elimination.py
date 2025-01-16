#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from typing import Dict, List, Optional, Set

from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    WithQueryBlock,
)
from snowflake.snowpark._internal.compiler.cte_utils import find_duplicate_subtrees
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.compiler.utils import (
    TreeNode,
    replace_child,
    update_resolvable_node,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)


class RepeatedSubqueryEliminationResult:
    # the result logical plans after repeated subquery elimination
    logical_plans: List[LogicalPlan]
    # total number of cte nodes created the transformation
    total_num_of_ctes: int

    def __init__(
        self,
        logical_plans: List[LogicalPlan],
        total_num_ctes: int,
    ) -> None:
        self.logical_plans = logical_plans
        self.total_num_of_ctes = total_num_ctes


class RepeatedSubqueryElimination:
    """
    Optimization that used eliminate duplicated queries in the plan.

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
    _query_generator: QueryGenerator
    _total_number_ctes: int

    def __init__(
        self,
        logical_plans: List[LogicalPlan],
        query_generator: QueryGenerator,
    ) -> None:
        self._logical_plans = logical_plans
        self._query_generator = query_generator
        self._total_number_ctes = 0

    def apply(self) -> RepeatedSubqueryEliminationResult:
        """
        Applies Common SubDataframe elimination on the set of logical plans one after another.

        Returns:
            A set of the new LogicalPlans with common sub dataframe deduplicated with CTE node.
        """
        final_logical_plans: List[LogicalPlan] = []
        for logical_plan in self._logical_plans:
            # NOTE: the current common sub-dataframe elimination relies on the
            # fact that all intermediate steps are resolved properly. Here we
            # do a pass of resolve of the logical plan to make sure we get a valid
            # resolved plan to start the process.
            # If the plan is already a resolved plan, this step will be a no-op.
            logical_plan = self._query_generator.resolve(logical_plan)

            # apply the CTE optimization on the resolved plan
            duplicated_node_ids, _ = find_duplicate_subtrees(logical_plan)
            if len(duplicated_node_ids) > 0:
                deduplicated_plan = self._replace_duplicate_node_with_cte(
                    logical_plan, duplicated_node_ids
                )
                final_logical_plans.append(deduplicated_plan)
            else:
                final_logical_plans.append(logical_plan)

        return RepeatedSubqueryEliminationResult(
            logical_plans=final_logical_plans,
            total_num_ctes=self._total_number_ctes,
        )

    def _replace_duplicate_node_with_cte(
        self,
        root: TreeNode,
        duplicated_node_ids: Set[str],
    ) -> LogicalPlan:
        """
        Replace all duplicated nodes with a WithQueryBlock (CTE node), to enable
        query generation with CTEs.

        NOTE, we use stack to perform a post-order traversal instead of recursive call.
        The reason of using the stack approach is that chained CTEs have to be built
        from bottom (innermost subquery) to top (outermost query).
        This function uses an iterative approach to avoid hitting Python's maximum recursion depth limit.
        """

        node_parents_map: Dict[TreeNode, Set[TreeNode]] = defaultdict(set)
        stack1, stack2 = [root], []

        while stack1:
            node = stack1.pop()
            stack2.append(node)
            for child in reversed(node.children_plan_nodes):
                node_parents_map[child].add(node)
                stack1.append(child)

        # track node that is already visited to avoid repeated operation on the same node
        visited_nodes: Set[TreeNode] = set()
        updated_nodes: Set[TreeNode] = set()
        # track the resolved WithQueryBlock node has been created for each duplicated node
        resolved_with_block_map: Dict[str, SnowflakePlan] = {}

        def _update_parents(
            node: TreeNode,
            should_replace_child: bool,
            new_child: Optional[TreeNode] = None,
        ) -> None:
            parents = node_parents_map[node]
            for parent in parents:
                if should_replace_child:
                    assert (
                        new_child is not None
                    ), "no new child is provided for replacement"
                    replace_child(parent, node, new_child, self._query_generator)
                update_resolvable_node(parent, self._query_generator)
                updated_nodes.add(parent)

        while stack2:
            node = stack2.pop()
            if node in visited_nodes:
                continue

            # if the node is a duplicated node and deduplication is not done for the node,
            # start the deduplication transformation use CTE
            if node.encoded_node_id_with_query in duplicated_node_ids:
                if node.encoded_node_id_with_query in resolved_with_block_map:
                    # if the corresponding CTE block has been created, use the existing
                    # one.
                    resolved_with_block = resolved_with_block_map[
                        node.encoded_node_id_with_query
                    ]
                else:
                    # create a WithQueryBlock node
                    with_block = WithQueryBlock(
                        name=random_name_for_temp_object(TempObjectType.CTE), child=node
                    )
                    with_block._is_valid_for_replacement = True

                    resolved_with_block = self._query_generator.resolve(with_block)
                    resolved_with_block_map[
                        node.encoded_node_id_with_query
                    ] = resolved_with_block
                    self._total_number_ctes += 1
                _update_parents(
                    node, should_replace_child=True, new_child=resolved_with_block
                )
            elif node in updated_nodes:
                # if the node is updated, make sure all nodes up to parent is updated
                _update_parents(node, should_replace_child=False)

            visited_nodes.add(node)

        return root
