#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Optional, Union as UnionType

from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.binary_plan_node import BinaryNode
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectStatement,
    SelectTableFunction,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
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


def is_active_transaction(session):
    """Check is the session has an active transaction."""
    return session._run_query("SELECT CURRENT_TRANSACTION()")[0][0] is not None


def re_resolve_plan(
    plan: UnionType[SnowflakePlan, Selectable], analyzer: Analyzer
) -> None:
    """Reset the snowflake plan associated with given plan."""
    if isinstance(plan, SnowflakePlan):
        if plan.source_plan is None:
            return
        if isinstance(plan.source_plan, Selectable):
            re_resolve_plan(plan.source_plan, analyzer)

        resolved_plan = analyzer.resolve(plan.source_plan)
        plan.queries = resolved_plan.queries
        plan.post_actions = resolved_plan.post_actions
        plan.expr_to_alias = resolved_plan.expr_to_alias
        plan.is_ddl_on_temp_object = resolved_plan.is_ddl_on_temp_object
        plan._output_dict = resolved_plan._output_dict
        plan.df_aliased_col_name_to_real_col_name = (
            resolved_plan.df_aliased_col_name_to_real_col_name
        )
        plan.placeholder_query = resolved_plan.placeholder_query
        plan._cumulative_node_complexity = resolved_plan._cumulative_node_complexity

    elif isinstance(plan, (SelectSnowflakePlan, SelectTableFunction)):
        if plan.snowflake_plan.source_plan:
            plan._snowflake_plan = analyzer.resolve(plan.snowflake_plan)
        return

    elif isinstance(plan, SelectStatement):
        plan._sql_query = None
        plan._snowflake_plan = None
        re_resolve_plan(plan.from_, analyzer)

    elif isinstance(plan, SetStatement):
        plan._sql_query = None
        plan._snowflake_plan = None
        for operand in plan.set_operands:
            re_resolve_plan(operand.selectable, analyzer)

    elif isinstance(plan, Selectable):
        plan._snowflake_plan = None
    else:
        raise ValueError(
            f"plan type {type(plan)} not supported to have its snowflake plan reset."
        )


def replace_child(
    parent: LogicalPlan,
    old_child: LogicalPlan,
    new_child: LogicalPlan,
    analyzer: Analyzer,
) -> None:
    """
    Helper function to replace the child node in the plan with a new child.

    Whenever necessary, we convert the new_child into a Selectable or SnowflakePlan
    based on the parent node type.
    """

    def to_selectable(plan: LogicalPlan, analyzer: Analyzer) -> Selectable:
        """Given a LogicalPlan, convert it to a Selectable."""
        if isinstance(plan, Selectable):
            return plan

        snowflake_plan = analyzer.resolve(plan)
        snowflake_plan._is_valid_for_replacement = plan._is_valid_for_replacement
        return SelectSnowflakePlan(snowflake_plan, analyzer=analyzer)

    if not parent._is_valid_for_replacement:
        raise ValueError(f"parent node {parent} is not valid for replacement.")

    if old_child not in getattr(parent, "children_plan_nodes", parent.children):
        raise ValueError(f"old_child {old_child} is not a child of parent {parent}.")

    if isinstance(parent, SnowflakePlan):
        assert parent.source_plan is not None
        replace_child(parent.source_plan, old_child, new_child, analyzer)
        return

    if isinstance(parent, SelectStatement):
        parent.from_ = to_selectable(new_child, analyzer)
        return

    if isinstance(parent, SetStatement):
        new_child_as_selectable = to_selectable(new_child, analyzer)
        parent._nodes = [
            node if node != old_child else new_child_as_selectable
            for node in parent._nodes
        ]
        for operand in parent.set_operands:
            if operand.selectable == old_child:
                operand.selectable = new_child_as_selectable
        return

    if isinstance(parent, Selectable):
        assert parent.snowflake_plan.source_plan is not None
        replace_child(parent.snowflake_plan.source_plan, old_child, new_child, analyzer)
        return

    if isinstance(parent, (UnaryNode, Limit, CopyIntoLocationNode)):
        parent.children = [new_child]
        parent.child = new_child
        return

    if isinstance(parent, BinaryNode):
        parent.children = [
            node if node != old_child else new_child for node in parent.children
        ]
        if parent.left == old_child:
            parent.left = new_child
        if parent.right == old_child:
            parent.right = new_child
        return

    if isinstance(parent, SnowflakeCreateTable):
        parent.children = [new_child]
        parent.query = new_child
        return

    if isinstance(parent, (TableUpdate, TableDelete)):
        snowflake_plan = analyzer.resolve(new_child)
        parent.children = [snowflake_plan]
        parent.source_data = snowflake_plan
        return

    if isinstance(parent, TableMerge):
        snowflake_plan = analyzer.resolve(new_child)
        parent.children = [snowflake_plan]
        parent.source = snowflake_plan
        return

    if isinstance(parent, LogicalPlan):
        parent.children = [
            node if node != old_child else new_child for node in parent.children
        ]
        return

    raise ValueError(f"parent type {type(parent)} not supported")


def _plot_plan_if_enabled(root, path) -> None:
    """A helper function to plot the query plan tree using graphviz useful for debugging."""
    import os

    if not os.environ.get("ENABLE_SNOWFLAKE_OPTIMIZATION_PLAN_PLOTTING"):
        return

    import graphviz  # pyright: ignore[reportMissingImports]

    def get_stat(node):
        def get_name(node) -> str:
            addr = hex(id(node))
            name = str(type(node)).split(".")[-1].split("'")[0]
            return f"{name}({addr})"

        name = get_name(node)
        if isinstance(node, SnowflakePlan):
            name = f"{name} :: ({get_name(node.source_plan)})"
        elif isinstance(node, SelectSnowflakePlan):
            name = f"{name} :: ({get_name(node.snowflake_plan.source_plan)})"

        score = get_complexity_score(node.cumulative_node_complexity)
        sql_size = (
            len(node.queries[-1].sql)  # pyright: ignore[reportGeneralTypeIssues]
            if hasattr(node, "queries")
            else len(node.sql_query)  # pyright: ignore[reportGeneralTypeIssues]
        )
        sql_preview = (
            node.queries[-1].sql[:50]  # pyright: ignore[reportGeneralTypeIssues]
            if hasattr(node, "queries")
            else node.sql_query[:50]  # pyright: ignore[reportGeneralTypeIssues]
        )

        return f"{name=}\n" f"{score=}, {sql_size=}\n" f"{sql_preview=}"

    g = graphviz.Graph(format="png")

    g.node(root._id, get_stat(root))

    curr_level = [root]
    while curr_level:
        next_level = []
        for node in curr_level:
            for child in node.children_plan_nodes:
                g.node(child._id, get_stat(child))
                g.edge(node._id, child._id, dir="back")
                next_level.append(child)
        curr_level = next_level

    os.makedirs(os.path.dirname(path), exist_ok=True)
    g.render(path, format="png", cleanup=True)
