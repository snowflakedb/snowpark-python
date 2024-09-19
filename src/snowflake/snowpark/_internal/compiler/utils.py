#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import copy
from typing import Dict, List, Optional, Union

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
    TableCreationSource,
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

TreeNode = Union[SnowflakePlan, Selectable]


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
        # we ensure that the source plan is not from large query breakdown because we
        # do not cache attributes for this case.
        assert (
            create_table_node.creation_source
            != TableCreationSource.LARGE_QUERY_BREAKDOWN
        ), "query generator is not supported for large query breakdown as creation source"

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
    node.referenced_ctes = new_snowflake_plan.referenced_ctes
    node._cumulative_node_complexity = new_snowflake_plan._cumulative_node_complexity


def replace_child(
    parent: LogicalPlan,
    old_child: LogicalPlan,
    new_child: LogicalPlan,
    query_generator: QueryGenerator,
) -> None:
    """
    Helper function to replace the child node of a plan node with a new child.

    Whenever necessary, we convert the new_child into a Selectable or SnowflakePlan
    based on the parent node type.
    """

    def to_selectable(plan: LogicalPlan, query_generator: QueryGenerator) -> Selectable:
        """Given a LogicalPlan, convert it to a Selectable."""
        if isinstance(plan, Selectable):
            return plan

        snowflake_plan = query_generator.resolve(plan)
        return SelectSnowflakePlan(snowflake_plan, analyzer=query_generator)

    if not parent._is_valid_for_replacement:
        raise ValueError(f"parent node {parent} is not valid for replacement.")

    if old_child not in getattr(parent, "children_plan_nodes", parent.children):
        raise ValueError(f"old_child {old_child} is not a child of parent {parent}.")

    if isinstance(parent, SnowflakePlan):
        assert parent.source_plan is not None
        replace_child(parent.source_plan, old_child, new_child, query_generator)

    elif isinstance(parent, SelectStatement):
        parent.from_ = to_selectable(new_child, query_generator)

    elif isinstance(parent, SetStatement):
        new_child_as_selectable = to_selectable(new_child, query_generator)
        parent._nodes = [
            node if node != old_child else new_child_as_selectable
            for node in parent._nodes
        ]
        for operand in parent.set_operands:
            if operand.selectable == old_child:
                operand.selectable = new_child_as_selectable

    elif isinstance(parent, Selectable):
        assert parent.snowflake_plan is not None
        replace_child(parent.snowflake_plan, old_child, new_child, query_generator)

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
        snowflake_plan = query_generator.resolve(new_child)
        parent.children = [snowflake_plan]
        parent.source_data = snowflake_plan

    elif isinstance(parent, TableMerge):
        snowflake_plan = query_generator.resolve(new_child)
        parent.children = [snowflake_plan]
        parent.source = snowflake_plan

    elif isinstance(parent, LogicalPlan):
        parent.children = [
            node if node != old_child else new_child for node in parent.children
        ]

    else:
        raise ValueError(f"parent type {type(parent)} not supported")


def update_resolvable_node(
    node: TreeNode,
    query_generator: QueryGenerator,
):
    """
    Helper function to make an in-place update for a node that has had its child updated.
    It works in two parts:
      1. Re-resolve the proper fields based on the child.
      2. Resets re-calculable fields such as _sql_query, _snowflake_plan, _cumulative_node_complexity.

    The re-resolve is only needed for SnowflakePlan node and Selectable node, because only those nodes
    are resolved node with sql query state.

    Note the update is done recursively until it reach to the child to the children_plan_nodes,
    this is to make sure all nodes in between current node and child are updated
    correctly. For example, with the following plan
                  SelectSnowflakePlan
                          |
                     SnowflakePlan
                          |
                        JOIN
    resolve_node(SelectSnowflakePlan, query_generator) will resolve both SelectSnowflakePlan and SnowflakePlan nodes.
    """

    if not node._is_valid_for_replacement:
        raise ValueError(f"node {node} is not valid for update.")

    if not isinstance(node, (SnowflakePlan, Selectable)):
        raise ValueError(f"It is not valid to update node with type {type(node)}.")

    # reset the cumulative_node_complexity for all nodes
    node.reset_cumulative_node_complexity()

    if isinstance(node, SnowflakePlan):
        assert node.source_plan is not None
        if isinstance(node.source_plan, (SnowflakePlan, Selectable)):
            update_resolvable_node(node.source_plan, query_generator)
        resolve_and_update_snowflake_plan(node, query_generator)

    elif isinstance(node, SelectStatement):
        # clean up the cached sql query and snowflake plan to allow
        # re-calculation of the sql query and snowflake plan
        node._sql_query = None
        node._snowflake_plan = None
        # make sure we also clean up the cached _projection_in_str, so that
        # the projection expression can be re-analyzed during code generation
        node._projection_in_str = None
        node.analyzer = query_generator

        # update the pre_actions and post_actions for the select statement
        node.pre_actions = node.from_.pre_actions
        node.post_actions = node.from_.post_actions
        node.expr_to_alias = node.from_.expr_to_alias
        node.df_aliased_col_name_to_real_col_name.clear()
        node.df_aliased_col_name_to_real_col_name.update(
            node.from_.df_aliased_col_name_to_real_col_name
        )

    elif isinstance(node, SetStatement):
        # clean up the cached sql query and snowflake plan to allow
        # re-calculation of the sql query and snowflake plan
        node._sql_query = None
        node._snowflake_plan = None
        node.analyzer = query_generator

        # update the pre_actions and post_actions for the set statement
        node.pre_actions, node.post_actions = None, None
        for operand in node.set_operands:
            if operand.selectable.pre_actions:
                if node.pre_actions is None:
                    node.pre_actions = []
                for action in operand.selectable.pre_actions:
                    if action not in node.pre_actions:
                        node.pre_actions.append(action)

            if operand.selectable.post_actions:
                if node.post_actions is None:
                    node.post_actions = []
                for action in operand.selectable.post_actions:
                    if action not in node.post_actions:
                        node.post_actions.append(action)

    elif isinstance(node, (SelectSnowflakePlan, SelectTableFunction)):
        assert node.snowflake_plan is not None
        update_resolvable_node(node.snowflake_plan, query_generator)
        node.analyzer = query_generator

        node.pre_actions = node._snowflake_plan.queries[:-1]
        node.post_actions = node._snowflake_plan.post_actions
        node._api_calls = node._snowflake_plan.api_calls

        if isinstance(node, SelectSnowflakePlan):
            node.expr_to_alias.update(node._snowflake_plan.expr_to_alias)
            node.df_aliased_col_name_to_real_col_name.update(
                node._snowflake_plan.df_aliased_col_name_to_real_col_name
            )
            node._query_params = []
            for query in node._snowflake_plan.queries:
                if query.params:
                    node._query_params.extend(query.params)

    elif isinstance(node, Selectable):
        node.analyzer = query_generator


def get_snowflake_plan_queries(
    plan: SnowflakePlan, resolved_with_query_blocks: Dict[str, Query]
) -> Dict[PlanQueryType, List[Query]]:

    from snowflake.snowpark._internal.analyzer.analyzer_utils import cte_statement

    plan_queries = plan.queries
    post_action_queries = plan.post_actions
    if len(plan.referenced_ctes) > 0:
        # make a copy of the original query to avoid any update to the
        # original query object
        plan_queries = copy.deepcopy(plan.queries)
        post_action_queries = copy.deepcopy(plan.post_actions)
        table_names = []
        definition_queries = []
        final_query_params = []
        for name, definition_query in resolved_with_query_blocks.items():
            if name in plan.referenced_ctes:
                table_names.append(name)
                definition_queries.append(definition_query.sql)
                final_query_params.extend(definition_query.params)
        with_query = cte_statement(definition_queries, table_names)
        plan_queries[-1].sql = with_query + plan_queries[-1].sql
        final_query_params.extend(plan_queries[-1].params)
        plan_queries[-1].params = final_query_params

    return {
        PlanQueryType.QUERIES: plan_queries,
        PlanQueryType.POST_ACTIONS: post_action_queries,
    }


def is_active_transaction(session):
    """Check is the session has an active transaction."""
    return session._run_query("SELECT CURRENT_TRANSACTION()")[0][0] is not None
