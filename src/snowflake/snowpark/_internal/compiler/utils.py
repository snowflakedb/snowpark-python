#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import copy
from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.binary_plan_node import BinaryNode
from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectStatement,
    SelectTableFunction,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    PlanQueryType,
    Query,
    SnowflakePlan,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoLocationNode,
    Limit,
    LogicalPlan,
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    TableDelete,
    TableMerge,
    TableUpdate,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import UnaryNode
from snowflake.snowpark._internal.compiler.query_generator import (
    QueryGenerator,
    SnowflakeCreateTablePlanInfo,
)


def create_query_generator(plan: SnowflakePlan) -> QueryGenerator:
    """
    Helper function to construct the query generator for a given valid SnowflakePlan.
    """
    snowflake_create_table_plan_info: Optional[SnowflakeCreateTablePlanInfo] = None
    # When the root node of source plan is SnowflakeCreateTable, we need to extract the
    # child attributes for the table that is used for later query generation process.
    # This relies on the fact that SnowflakeCreateTable is an eager evaluation, and
    # SnowflakeCreateTable is always the root node of the logical plan.
    if plan.source_plan is not None and isinstance(
        plan.source_plan, SnowflakeCreateTable
    ):
        create_table_node = plan.source_plan
        # resolve the node child to get the child attribute that is needed for later code
        # generation. Typically, the query attached to the create_table_node is already a
        # resolved plan, and the resolve will be a no-op.
        # NOTE that here we rely on the fact that the SnowflakeCreateTable node is the root
        # of a source plan. Test will fail if that assumption is broken.
        resolved_child = plan.session._analyzer.resolve(create_table_node.query)
        snowflake_create_table_plan_info = SnowflakeCreateTablePlanInfo(
            create_table_node.table_name, resolved_child.attributes
        )

    return QueryGenerator(plan.session, snowflake_create_table_plan_info)


def resolve_and_update_snowflake_plan(
    node: SnowflakePlan, query_generator: QueryGenerator
) -> None:
    """
    Re-resolve the current snowflake plan if it has a source plan attached, and update the fields with
    newly resolved value.
    """

    if node.source_plan is None:
        return

    new_snowflake_plan = query_generator.resolve(node.source_plan)

    # copy over the newly resolved fields to make it an in-place update
    node.queries = new_snowflake_plan.queries
    node.post_actions = new_snowflake_plan.post_actions
    node.expr_to_alias = new_snowflake_plan.expr_to_alias
    node.is_ddl_on_temp_object = new_snowflake_plan.is_ddl_on_temp_object
    node._output_dict = new_snowflake_plan._output_dict
    node.df_aliased_col_name_to_real_col_name = (
        new_snowflake_plan.df_aliased_col_name_to_real_col_name
    )
    node.placeholder_query = new_snowflake_plan.placeholder_query
    node.referred_ctes = new_snowflake_plan.referred_ctes
    node._cumulative_node_complexity = new_snowflake_plan._cumulative_node_complexity


def resolve_node(node: LogicalPlan, query_generator: QueryGenerator) -> SnowflakePlan:
    resolved_node = query_generator.resolve(node)
    resolved_node._is_valid_for_replacement = True

    return resolved_node


def replace_child_and_update_parent(
    parent: LogicalPlan,
    old_child: LogicalPlan,
    new_child: LogicalPlan,
    query_generator: QueryGenerator,
) -> None:
    """
    Helper function to
        1) replace the child node in the plan with a new child
        2) re-resolve the parent node properly if its parent node is a SnowflakePlan or Selectable

    Whenever necessary, we convert the new_child into a Selectable or SnowflakePlan
    based on the parent node type.

    Note the update is done recursively to make sure all nodes in between parent and child are updated
    correctly. For example, with the following plan
    #          SelectSnowflakePlan
    #                  |
    #             SnowflakePlan
    #                  |
    #                JOIN
    All nodes includes SelectSnowflakePlan, SnowflakePlan, JOIN are updated accordingly
    """

    def to_selectable(plan: LogicalPlan, query_generator: QueryGenerator) -> Selectable:
        """Given a LogicalPlan, convert it to a Selectable."""
        if isinstance(plan, Selectable):
            return plan

        snowflake_plan = resolve_node(plan, query_generator)
        return SelectSnowflakePlan(snowflake_plan, analyzer=query_generator)

    if not parent._is_valid_for_replacement:
        raise ValueError(f"parent node {parent} is not valid for replacement.")

    if old_child not in getattr(parent, "children_plan_nodes", parent.children):
        raise ValueError(f"old_child {old_child} is not a child of parent {parent}.")

    if isinstance(parent, SnowflakePlan):
        assert parent.source_plan is not None
        replace_child_and_update_parent(
            parent.source_plan, old_child, new_child, query_generator
        )
        resolve_and_update_snowflake_plan(parent, query_generator)

    elif isinstance(parent, SelectStatement):
        parent.from_ = to_selectable(new_child, query_generator)
        parent._sql_query = None
        parent._snowflake_plan = None
        parent.analyzer = query_generator

    elif isinstance(parent, SetStatement):
        new_child_as_selectable = to_selectable(new_child, query_generator)
        parent._nodes = [
            node if node != old_child else new_child_as_selectable
            for node in parent._nodes
        ]
        for operand in parent.set_operands:
            if operand.selectable == old_child:
                operand.selectable = new_child_as_selectable

        parent._sql_query = None
        parent._snowflake_plan = None
        parent.analyzer = query_generator

    elif isinstance(parent, (SelectSnowflakePlan, SelectTableFunction)):
        assert parent.snowflake_plan is not None
        replace_child_and_update_parent(
            parent.snowflake_plan, old_child, new_child, query_generator
        )
        parent.analyzer = query_generator

    elif isinstance(parent, Selectable):
        assert parent.snowflake_plan is not None
        replace_child_and_update_parent(
            parent.snowflake_plan, old_child, new_child, query_generator
        )
        parent.analyzer = query_generator

    elif isinstance(parent, (UnaryNode, Limit, CopyIntoLocationNode)):
        parent.children = [new_child]
        parent.child = new_child

    elif isinstance(parent, BinaryNode):
        parent.children = [
            node if node != old_child else new_child for node in parent.children
        ]
        if parent.left == old_child:
            parent.left = new_child
        if parent.right == old_child:
            parent.right = new_child

    elif isinstance(parent, SnowflakeCreateTable):
        parent.children = [new_child]
        parent.query = new_child

    elif isinstance(parent, (TableUpdate, TableDelete)):
        snowflake_plan = resolve_node(new_child, query_generator)
        parent.children = [snowflake_plan]
        parent.source_data = snowflake_plan

    elif isinstance(parent, TableMerge):
        snowflake_plan = resolve_node(new_child, query_generator)
        parent.children = [snowflake_plan]
        parent.source = snowflake_plan

    elif isinstance(parent, LogicalPlan):
        parent.children = [
            node if node != old_child else new_child for node in parent.children
        ]

    else:
        raise ValueError(f"parent type {type(parent)} not supported")


def get_snowflake_plan_queries(
    plan: SnowflakePlan, resolved_with_query_blocks: Dict[str, str]
) -> Dict[PlanQueryType, List[Query]]:

    from snowflake.snowpark._internal.analyzer.analyzer_utils import cte_statement

    plan_queries = plan.queries
    post_action_queries = plan.post_actions
    if len(plan.referred_ctes) > 0:
        # make a copy of the original query to avoid any update to the
        # original query object
        plan_queries = copy.deepcopy(plan.queries)
        post_action_queries = copy.deepcopy(plan.post_actions)
        table_names = []
        definition_queries = []
        for name, definition_query in resolved_with_query_blocks.items():
            if name in plan.referred_ctes:
                table_names.append(name)
                definition_queries.append(definition_query)
        with_query = cte_statement(definition_queries, table_names)
        plan_queries[-1].sql = with_query + plan_queries[-1].sql

    return {
        PlanQueryType.QUERIES: plan_queries,
        PlanQueryType.POST_ACTIONS: post_action_queries,
    }
