#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from typing import Dict, List, Set, Union

from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectStatement,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    WithObjectRef,
    WithQueryBlock,
)
from snowflake.snowpark._internal.optimizer.query_generator import QueryGenerator
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)

TreeNode = Union[SnowflakePlan, Selectable]

"""
class CommonSubDataframeEliminationStatus:
    optimization_applied: bool
    num_duplicated_nodes: int
    result_plans: List[LogicalPlan]

    def __int__(self, optimization_applied: bool, num_duplicated_nodes: int, result_plans: List[LogicalPlan]) -> None:
        self.optimization_applied = optimization_applied
        self.num_duplicated_nodes = num_duplicated_nodes
        self.result_plans = result_plans
"""


class CommonSubDataframeElimination:

    _plan: LogicalPlan
    _node_count_map: Dict[LogicalPlan, int]
    _node_parents_map: Dict[LogicalPlan, Set[LogicalPlan]]
    _duplicated_nodes: Set[LogicalPlan]
    _analyzer: QueryGenerator

    def __init__(
        self, logical_plan: LogicalPlan, query_generator: QueryGenerator
    ) -> None:
        self._plan = logical_plan
        self._analyzer = query_generator
        self._node_count_map = defaultdict(int)
        self._node_parents_map = defaultdict(set)
        self._duplicated_nodes = set()
        # self._analyzer = Analyzer(plan.session, skip_schema_query=True)
        # self._analyzer = QueryGenerator(plan.session)
        # self._analyzer.alias_maps_to_use = {}

    def common_subdataframe_elimination(self) -> List[LogicalPlan]:
        self._find_duplicate_subtrees()
        if len(self._duplicated_nodes) > 0:
            return [self._cte_transformation()]
            # return self._cte_transformation()
        else:
            return [self._plan]
            # return self._plan

    def _find_duplicate_subtrees(self) -> None:
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

        def traverse(root: TreeNode) -> None:
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
                    self._node_count_map[node] += 1
                    for child in node.children_plan_nodes:
                        # converting non-SELECT child query to SELECT query here,
                        # so we can further optimize
                        if isinstance(child, Selectable):
                            child = child.to_subqueryable()
                        self._node_parents_map[child].add(node)
                        next_level.append(child)
                current_level = next_level

        def is_duplicate_subtree(node: LogicalPlan) -> bool:
            is_duplicate_node = self._node_count_map[node] > 1
            if is_duplicate_node:
                is_any_parent_unique_node = any(
                    self._node_count_map[n] == 1 for n in self._node_parents_map[node]
                )
                if is_any_parent_unique_node:
                    return True
                else:
                    has_multi_parents = len(self._node_parents_map[node]) > 1
                    if has_multi_parents:
                        return True
            return False

        def is_dynamic_pivot_node(node: LogicalPlan) -> bool:
            from snowflake.snowpark._internal.analyzer.select_statement import (
                SelectSnowflakePlan,
            )
            from snowflake.snowpark._internal.analyzer.snowflake_plan import (
                SnowflakePlan,
            )
            from snowflake.snowpark._internal.analyzer.unary_plan_node import Pivot

            if isinstance(node, SelectSnowflakePlan):
                source_plan = node.snowflake_plan.source_plan
            elif isinstance(node, SnowflakePlan):
                source_plan = node.source_plan
            else:
                return False
            return isinstance(source_plan, Pivot) and source_plan.pivot_values is None

        traverse(self._plan)
        self._duplicated_nodes = {
            node for node in self._node_count_map if is_duplicate_subtree(node)
        }

    def _update_cte_parent(
        self, parent: LogicalPlan, node: "TreeNode", new_node: LogicalPlan
    ) -> None:
        if isinstance(parent, SelectStatement):
            parent._from = new_node
        elif isinstance(parent, SetStatement):
            parent._nodes.clear()
            for operand in parent.set_operands:
                if operand.selectable == node:
                    # with_object_ref_node = WithObjectRef(with_block)
                    with_object_plan = self._analyzer.resolve(new_node)
                    new_selectable = SelectSnowflakePlan(
                        snowflake_plan=with_object_plan,
                        analyzer=self._analyzer,
                    )
                    operand.selectable = new_selectable
                    parent._nodes.append(new_selectable)
                else:
                    parent._nodes.append(operand.selectable)
        elif isinstance(parent, SnowflakePlan):
            parent.source_plan.update_child_node(node, new_node)

    def _cte_transformation(self) -> LogicalPlan:
        stack1, stack2 = [self._plan], []

        while stack1:
            node = stack1.pop()
            stack2.append(node)
            for child in reversed(node.children_plan_nodes):
                stack1.append(child)

        # self._analyzer.table_create_child_attribute_map.update(self._plan.table_create_child_attribute_map)
        eliminated_nodes = set()
        while stack2:
            node = stack2.pop()
            if node in self._duplicated_nodes:
                if node not in eliminated_nodes:
                    parents = self._node_parents_map[node]
                    # convert the node into WithQueryBlock
                    with_block = WithQueryBlock(
                        name=random_name_for_temp_object(TempObjectType.CTE), child=node
                    )
                    with_object_ref_node = WithObjectRef(with_block)
                    for parent in parents:
                        self._update_cte_parent(parent, node, with_object_ref_node)
                    eliminated_nodes.add(node)
            else:
                self.reset_node(node)

        # do one path to resolve the whole tree again
        # final_plan = self._analyzer.resolve(self._plan.source_plan)
        return self._plan

    def reset_node(self, node: LogicalPlan) -> None:
        if isinstance(node, SelectSnowflakePlan):
            node._snowflake_plan = self._analyzer.resolve(
                node._snowflake_plan.source_plan
            )
        elif isinstance(node, Selectable):
            node._snowflake_plan = None
            node._sql_query = None

    def _cte_transformation_old(self) -> "SnowflakePlan":
        stack1, stack2 = [self._plan], []

        while stack1:
            node = stack1.pop()
            stack2.append(node)
            for child in reversed(node.children_plan_nodes):
                stack1.append(child)

        # bottom up visitor, and re-do the resolving of whole plan tree node
        resolved_nodes = {}
        # there should be no change to alias map
        df_aliased_col_name_to_real_col_name = (
            self._plan.df_aliased_col_name_to_real_col_name
        )
        while stack2:
            node = stack2.pop()
            if node in resolved_nodes:
                continue
            if node in self._duplicated_nodes:
                parents = self._node_parents_map[node]
                # resolve current node first
                resolved_node = self._analyzer.do_resolve_with_resolved_children(
                    node, resolved_nodes, df_aliased_col_name_to_real_col_name
                )
                resolved_nodes[node] = resolved_node
                # convert the node into WithQueryBlock
                with_block = WithQueryBlock(
                    name=random_name_for_temp_object(TempObjectType.CTE), child=node
                )
                # resolve with block
                resolved_with_block = self._analyzer.do_resolve_with_resolved_children(
                    with_block, resolved_nodes, df_aliased_col_name_to_real_col_name
                )
                resolved_nodes[with_block] = resolved_with_block
                for parent in parents:
                    if isinstance(parent, SelectStatement):
                        parent._from = WithObjectRef(with_block)
                        parent._snowflake_plan = resolved_with_block
                    elif isinstance(parent, SetStatement):
                        parent._nodes.clear()
                        for operand in parent.set_operands:
                            if operand.selectable == node:
                                with_object_ref_node = WithObjectRef(with_block)
                                with_object_plan = self._analyzer.do_resolve_with_resolved_children(
                                    with_object_ref_node,
                                    resolved_nodes,
                                    resolved_with_block.df_aliased_col_name_to_real_col_name,
                                )
                                new_selectable = SelectSnowflakePlan(
                                    snowflake_plan=with_object_plan,
                                    analyzer=self._analyzer,
                                )
                                operand.selectable = new_selectable
                                parent._nodes.append(new_selectable)
                            else:
                                parent._nodes.append(operand.selectable)

                        # refresh the sql query
                        parent._sql_query = None
                        parent._snowflake_plan = None

                    elif isinstance(parent, SnowflakePlan):
                        for i in range(len(parent.source_plan.children)):
                            try:
                                if parent.source_plan.children[i] == node:
                                    parent.source_plan.children[i] = WithObjectRef(
                                        with_block
                                    )
                            except Exception:
                                pass

            else:
                if isinstance(node, SnowflakePlan):
                    res = self._analyzer.do_resolve_with_resolved_children(
                        node.source_plan,
                        resolved_nodes,
                        df_aliased_col_name_to_real_col_name,
                    )
                    node.schema_query = res.schema_query
                    node.queries = res.queries
                    node.with_query_block_plans = res.with_query_block_plans
                    resolved_nodes[node] = node
                else:
                    if isinstance(node, Selectable):
                        node._snowflake_pan = None
                        node._sql_query = None
                    res = self._analyzer.do_resolve_with_resolved_children(
                        node, resolved_nodes, df_aliased_col_name_to_real_col_name
                    )
                    resolved_nodes[node] = res

        final_plan = self._plan
        return final_plan
