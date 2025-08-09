#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import copy
import tempfile
from typing import Dict, List, Optional, Set, Union

from snowflake.snowpark._internal.analyzer.binary_plan_node import BinaryNode
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectStatement,
    SelectTableFunction,
    SelectableEntity,
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
    WithQueryBlock,
)
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    TableDelete,
    TableMerge,
    TableUpdate,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    CreateViewCommand,
    UnaryNode,
)
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
    node.df_aliased_col_name_to_real_col_name.update(  # type: ignore
        new_snowflake_plan.df_aliased_col_name_to_real_col_name  # type: ignore
    )
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

    if not parent._is_valid_for_replacement:
        raise ValueError(f"parent node {parent} is not valid for replacement.")

    if old_child not in getattr(parent, "children_plan_nodes", parent.children):
        if new_child in getattr(parent, "children_plan_nodes", parent.children):
            # the child has already been updated
            return
        else:
            raise ValueError(
                f"old_child {old_child} is not a child of parent {parent}."
            )

    if isinstance(parent, SnowflakePlan):
        assert parent.source_plan is not None
        replace_child(parent.source_plan, old_child, new_child, query_generator)

    elif isinstance(parent, SelectStatement):
        parent.from_ = query_generator.to_selectable(new_child)
        # once the subquery is updated, set _merge_projection_complexity_with_subquery to False to
        # disable the projection complexity merge
        parent._merge_projection_complexity_with_subquery = False

    elif isinstance(parent, SetStatement):
        new_child_as_selectable = query_generator.to_selectable(new_child)
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
        node._commented_sql = None
        node._snowflake_plan = None
        # make sure we also clean up the cached _projection_in_str, so that
        # the projection expression can be re-analyzed during code generation
        node._projection_in_str = None
        node.analyzer = query_generator
        # reset the _projection_complexities fields to re-calculate the complexities
        node._projection_complexities = None

        # update the pre_actions and post_actions for the select statement
        node.pre_actions = node.from_.pre_actions
        node.post_actions = node.from_.post_actions
        node.expr_to_alias = node.from_.expr_to_alias.copy()

        # df_aliased_col_name_to_real_col_name is updated at the frontend api
        # layer when alias is called, not produced during code generation. Should
        # always retain the original value of the map.
        node.df_aliased_col_name_to_real_col_name.update(
            node.from_.df_aliased_col_name_to_real_col_name
        )
        # projection_in_str for SelectStatement runs a analyzer.analyze() which
        # needs the correct expr_to_alias map setup. This map is setup during
        # snowflake plan generation and cached for later use. Calling snowflake_plan
        # here to get the map setup correctly.
        node.get_snowflake_plan(skip_schema_query=True)
        node.expr_to_alias.update(node.snowflake_plan.expr_to_alias)

    elif isinstance(node, SetStatement):
        # clean up the cached sql query and snowflake plan to allow
        # re-calculation of the sql query and snowflake plan
        node._sql_query = None
        node._commented_sql = None
        node._snowflake_plan = None
        node.analyzer = query_generator

        # update the pre_actions and post_actions for the set statement
        node.pre_actions, node.post_actions = None, None
        for operand in node.set_operands:
            if operand.selectable.pre_actions:
                for action in operand.selectable.pre_actions:
                    node.merge_into_pre_action(action)

            if operand.selectable.post_actions:
                for action in operand.selectable.post_actions:
                    node.merge_into_post_action(action)

    elif isinstance(node, (SelectSnowflakePlan, SelectTableFunction)):
        assert node.snowflake_plan is not None
        update_resolvable_node(node.snowflake_plan, query_generator)
        node.analyzer = query_generator

        node.pre_actions = node._snowflake_plan.queries[:-1]
        node.post_actions = node._snowflake_plan.post_actions
        node._api_calls = node._snowflake_plan.api_calls

        if isinstance(node, SelectSnowflakePlan):
            node.expr_to_alias.update(node._snowflake_plan.expr_to_alias)
            node.df_aliased_col_name_to_real_col_name.update(  # type: ignore
                node._snowflake_plan.df_aliased_col_name_to_real_col_name  # type: ignore
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
    # If the plan has referenced ctes, we need to add the cte definition before
    # the final query. This is done for all source plan except for the following
    # cases:
    # - SnowflakeCreateTable
    # - CreateViewCommand
    # - TableUpdate
    # - TableDelete
    # - TableMerge
    # - CopyIntoLocationNode
    # because the generated_queries by QueryGenerator for these nodes already include the cte
    # definition. Adding the cte definition before the query again will cause a syntax error.
    if len(plan.referenced_ctes) > 0 and not isinstance(
        plan.source_plan,
        (
            SnowflakeCreateTable,
            CreateViewCommand,
            TableUpdate,
            TableDelete,
            TableMerge,
            CopyIntoLocationNode,
        ),
    ):
        # make a copy of the original query to avoid any update to the
        # original query object
        plan_queries = copy.copy(plan.queries)
        post_action_queries = copy.copy(plan.post_actions)
        table_names = []
        definition_queries = []
        final_query_params = []
        plan_referenced_cte_names = {
            with_query_block.name for with_query_block in plan.referenced_ctes
        }
        for name, definition_query in resolved_with_query_blocks.items():
            if name in plan_referenced_cte_names:
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


def extract_child_from_with_query_block(child: LogicalPlan) -> TreeNode:
    """Given a WithQueryBlock node, or a node that contains a WithQueryBlock node, this method
    extracts the child node from the WithQueryBlock node and returns it."""
    if isinstance(child, WithQueryBlock):
        return child.children[0]
    if isinstance(child, SnowflakePlan) and child.source_plan is not None:
        return extract_child_from_with_query_block(child.source_plan)
    if isinstance(child, SelectSnowflakePlan):
        return extract_child_from_with_query_block(child.snowflake_plan)

    raise ValueError(
        f"Invalid node type {type(child)} for partitioning."
    )  # pragma: no cover


def is_with_query_block(node: LogicalPlan) -> bool:
    """Given a node, this method checks if the node is a WithQueryBlock node or contains a
    WithQueryBlock node."""
    if isinstance(node, WithQueryBlock):
        return True
    if isinstance(node, SnowflakePlan) and node.source_plan is not None:
        return is_with_query_block(node.source_plan)
    if isinstance(node, SelectSnowflakePlan):
        return is_with_query_block(node.snowflake_plan)

    return False


def replace_child_and_update_ancestors(
    child: LogicalPlan,
    new_child: LogicalPlan,
    parent_map: Dict[LogicalPlan, Set[TreeNode]],
    query_generator: QueryGenerator,
):
    """
    For the given child, this helper function updates all its parents with the new
    child provided and updates all the ancestor nodes.
    """
    parents = parent_map[child]

    for parent in parents:
        replace_child(parent, child, new_child, query_generator)

    nodes_to_reset = list(parents)
    while nodes_to_reset:
        node = nodes_to_reset.pop()

        update_resolvable_node(node, query_generator)

        parents = parent_map[node]
        nodes_to_reset.extend(parents)


def plot_plan_if_enabled(root: LogicalPlan, filename: str) -> None:
    """A helper function to plot the query plan tree using graphviz useful for debugging.
    It plots the plan if the environment variable ENABLE_SNOWPARK_LOGICAL_PLAN_PLOTTING
    is set to true.

    The plots are saved in the temp directory of the system which is obtained using
    https://docs.python.org/3/library/tempfile.html#tempfile.gettempdir. Setting env variable
    TMPDIR to your desired location is recommended. Within the temp directory, the plots are
    saved in the directory `snowpark_query_plan_plots` with the given `filename`. For example,
    we can set the environment variables as follows:

        $ export ENABLE_SNOWPARK_LOGICAL_PLAN_PLOTTING=true
        $ export TMPDIR="/tmp"
        $ ls /tmp/snowpark_query_plan_plots/  # to see the plots

    Args:
        root: root TreeNode of the plan to plot.
        filename: name of the file to save the image plot.
    """
    import os

    if (
        os.environ.get("ENABLE_SNOWPARK_LOGICAL_PLAN_PLOTTING", "false").lower()
        != "true"
    ):
        return

    if int(
        os.environ.get("SNOWPARK_LOGICAL_PLAN_PLOTTING_COMPLEXITY_THRESHOLD", 0)
    ) > get_complexity_score(root):
        return

    import graphviz  # pyright: ignore[reportMissingImports]

    def get_stat(node: LogicalPlan):
        def get_name(node: Optional[LogicalPlan]) -> str:  # pragma: no cover
            if node is None:
                return "EMPTY_SOURCE_PLAN"  # pragma: no cover
            addr = hex(id(node))
            name = str(type(node)).split(".")[-1].split("'")[0]
            suffix = ""
            if isinstance(node, SnowflakeCreateTable):
                # get the table name from the full qualified name
                table_name = node.table_name[-1].split(".")[-1]  # pyright: ignore
                suffix = f" :: {table_name}"
            if isinstance(node, WithQueryBlock):
                # get the CTE identifier excluding SNOWPARK_TEMP_CTE_
                suffix = f" :: {node.name[18:]}"

            return f"{name}({addr}){suffix}"

        name = get_name(node)
        if isinstance(node, SnowflakePlan):
            name = f"{name} :: ({get_name(node.source_plan)})"
        elif isinstance(node, SelectSnowflakePlan):
            name = f"{name} :: ({get_name(node.snowflake_plan)}) :: ({get_name(node.snowflake_plan.source_plan)})"
        elif isinstance(node, SetStatement):
            name = f"{name} :: ({node.set_operands[1].operator})"
        elif isinstance(node, SelectStatement):
            properties = []
            if node.projection:
                properties.append("Proj")  # pragma: no cover
            if node.where:
                properties.append("Filter")  # pragma: no cover
            if node.order_by:
                properties.append("Order")  # pragma: no cover
            if node.limit_:
                properties.append("Limit")  # pragma: no cover
            if node.offset:
                properties.append("Offset")  # pragma: no cover
            name = f"{name} :: ({'| '.join(properties)})"
        elif isinstance(node, SelectableEntity):
            # get the table name from the full qualified name
            name = f"{name} :: ({node.entity.name.split('.')[-1]})"

        def get_sql_text(node: LogicalPlan) -> str:  # pragma: no cover
            if isinstance(node, Selectable):
                return node.sql_query
            if isinstance(node, SnowflakePlan):
                return node.queries[-1].sql
            return ""

        score = get_complexity_score(node)
        sql_text = get_sql_text(node)
        sql_size = len(sql_text)
        ref_ctes = None
        if isinstance(node, (SnowflakePlan, Selectable)):
            ref_ctes = list(
                map(
                    lambda node, cnt: f"{node.name[18:]}:{cnt}",
                    node.referenced_ctes.keys(),
                    node.referenced_ctes.values(),
                )
            )
            for with_query_block in node.referenced_ctes:  # pragma: no cover
                sql_size += len(get_sql_text(with_query_block.children[0]))
        sql_preview = sql_text[:50]

        return f"{name=}\n{score=}, {ref_ctes=}, {sql_size=}\n{sql_preview=}"

    g = graphviz.Graph(format="png")

    curr_level = [root]
    edges = set()  # add edges to set for de-duplication
    while curr_level:
        next_level = []
        for node in curr_level:
            node_id = hex(id(node))
            color = "lightblue" if node._is_valid_for_replacement else "red"
            fillcolor = "lightgray" if is_with_query_block(node) else "white"
            g.node(
                node_id,
                get_stat(node),
                color=color,
                style="filled",
                fillcolor=fillcolor,
            )
            if isinstance(node, (Selectable, SnowflakePlan)):
                children = node.children_plan_nodes
                if isinstance(node, SnowflakePlan) and isinstance(
                    node.source_plan, Selectable
                ):
                    children.append(node.source_plan)
            else:
                children = node.children  # pragma: no cover
            for child in children:
                child_id = hex(id(child))
                edges.add((node_id, child_id))
                next_level.append(child)
        curr_level = next_level
    for edge in edges:
        g.edge(*edge, dir="back")

    tempdir = tempfile.gettempdir()
    path = os.path.join(tempdir, "snowpark_query_plan_plots", filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    g.render(path, format="png", cleanup=True)
