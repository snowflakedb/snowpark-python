#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
from collections import defaultdict
from logging import getLogger
from typing import List

from sortedcontainers import SortedList

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    drop_table_if_exists_statement,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import Union
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_UNION,
    SelectSnowflakePlan,
    SelectStatement,
    SelectTableFunction,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SaveMode,
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    Pivot,
    Sample,
    Sort,
    Unpivot,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    is_active_transaction,
    random_name_for_temp_object,
)

_logger = getLogger(__name__)


class LargeQueryBreakdown:
    # COMPLEXITY_SCORE_LOWER_BOUND = 2_500_000
    # COMPLEXITY_SCORE_UPPER_BOUND = 5_000_000
    COMPLEXITY_SCORE_LOWER_BOUND = 300
    COMPLEXITY_SCORE_UPPER_BOUND = 800

    def __init__(self, session) -> None:
        self.session = session

    def _plot_plan(self, root: SnowflakePlan, path: str) -> None:
        """A helper function to plot the query plan tree using graphviz useful for debugging."""
        import os

        import graphviz

        def get_stat(node):
            def get_name(node):
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
                len(node.queries[-1].sql)
                if hasattr(node, "queries")
                else len(node.sql_query)
            )
            sql_preview = (
                node.queries[-1].sql[:50]
                if hasattr(node, "queries")
                else node.sql_query[:50]
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

    def breakdown_if_large_plan(self, root: SnowflakePlan) -> List[SnowflakePlan]:
        """
        Breaks down a large SnowflakePlan into smaller plans based on complexity score.

        Args:
            root (SnowflakePlan): The root node of the SnowflakePlan.

        Returns:
            List[SnowflakePlan]: A list of smaller SnowflakePlans.

        """

        # We use CTAS (DDL query) to break down the query plan only if the large query
        # breakdown is enabled. Running DDL queries inside a transaction commits current
        # transaction implicitly and executes DDL separately.
        # See https://docs.snowflake.com/en/sql-reference/transactions#ddl
        self.use_ctas = not is_active_transaction(self.session)

        if not self.session._large_query_breakdown_enabled:
            return [root]

        complexity_score = get_complexity_score(root.cumulative_node_complexity)
        if complexity_score < self.COMPLEXITY_SCORE_UPPER_BOUND:
            return [root]

        plans = []
        _logger.debug(f"Pre breakdown {complexity_score=} for root node")
        root = copy.copy(root)
        while complexity_score > self.COMPLEXITY_SCORE_UPPER_BOUND:
            partition = self.get_partitioned_plan(root)
            if partition is None:
                _logger.debug("No appropriate node found to break down the query plan")
                break
            plans.append(partition)
            complexity_score = get_complexity_score(root.cumulative_node_complexity)
            _logger.debug(f"Post breakdown {complexity_score=} for root node")

        plans.append(root)
        return plans

    def get_partitioned_plan(self, root: SnowflakePlan) -> SnowflakePlan:
        """
        This function finds an appropriate node to break the plan into two parts and returns the root of the partitioned tree.
        It also modifies the original root passed into the function.
        If no appropriate nodes are found, it returns None.

        Args:
            root (SnowflakePlan): The root node of the plan.

        Returns:
            SnowflakePlan: The root node of the partitioned tree.

        """

        # Initialize variables
        current_level = [root]
        pipeline_breaker_list = SortedList(key=lambda x: x[0])
        parent_map = defaultdict(set)

        # Traverse the plan tree
        while current_level:
            next_level = []
            for node in current_level:
                for child in node.children_plan_nodes:
                    next_level.append(child)
                    parent_map[child].add(node)
                    score = get_complexity_score(node.cumulative_node_complexity)
                    if self.is_node_valid_to_breakdown(score, child):
                        pipeline_breaker_list.add((score, child))

            current_level = next_level

        # Return if we don't find a valid not to break down the query plan
        if not pipeline_breaker_list:
            return None

        # Find the appropriate child break off from the plan
        child = pipeline_breaker_list[-1][1]

        # Create a temporary table and replace the child node with the temporary table reference
        temp_table_name = self.get_temp_table_name()
        temp_table_plan = self.session._analyzer.resolve(
            SnowflakeCreateTable(
                [temp_table_name],
                None,
                SaveMode.OVERWRITE if self.use_ctas else SaveMode.APPEND,
                child,
                table_type="temp",
                is_generated=True,
            )
        )

        self.update_ancestors(parent_map, child, temp_table_name)
        self.update_root_query(root, temp_table_name)

        return temp_table_plan

    def get_temp_table_name(self) -> str:
        return self.session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )

    def is_node_valid_to_breakdown(self, score: int, node: LogicalPlan) -> bool:
        """Check if the node is valid to break down based on the complexity score and pipeline breaker category."""

        valid_score_range = (
            score > self.COMPLEXITY_SCORE_LOWER_BOUND
            and score <= self.COMPLEXITY_SCORE_UPPER_BOUND + 20
        )

        return valid_score_range and self.is_node_pipeline_breaker(node)

    def is_node_pipeline_breaker(self, node: LogicalPlan) -> bool:
        """Check if the node is a pipeline breaker based on the node type."""
        if isinstance(node, (Pivot, Unpivot, Sort, Aggregate)):
            return True

        if isinstance(node, Sample):
            return node.row_count is not None

        if isinstance(node, Union):
            return not node.is_all

        if isinstance(node, SelectStatement):
            return node.order_by is not None

        if isinstance(node, SetStatement):
            return any(map(lambda x: x.operator == SET_UNION, node.set_operands))

        if isinstance(node, SnowflakePlan):
            return node.source_plan is not None and self.is_node_pipeline_breaker(
                node.source_plan
            )

        if isinstance(node, (SelectSnowflakePlan, SelectTableFunction)):
            return self.is_node_pipeline_breaker(node.snowflake_plan)

        return False

    def update_ancestors(self, parent_map, child, temp_table_name):
        """For the replaced child node, update the direct parent(s) with the temporary table
        reference. For all the ancestors of the child node, update the SnowflakePlan and
        complexity of the nodes.
        """
        temp_table_ref_node = self.session._analyzer.create_selectable_entity(
            temp_table_name, analyzer=self.session._analyzer
        )

        # Replace the child node with the temporary table reference
        parents = parent_map[child]
        for parent in parents:
            parent.replace_child(child, temp_table_ref_node)

        # Update the SQL and complexity of the ancestors nodes
        nodes_to_reset = list(parents)
        while nodes_to_reset:
            node = nodes_to_reset.pop()
            nodes_to_reset.extend(parent_map[node])

            node.reset_snowflake_plan()
            node.reset_cumulative_node_complexity()

    def update_root_query(
        self, root: SnowflakePlan, temp_table_name: str
    ) -> SnowflakePlan:
        """Update the final query in the root node and add a post action to drop the temporary table."""
        # Update the final query in the root node
        updated_root_plan = self.session._analyzer.resolve(root.source_plan)
        root.queries[-1] = updated_root_plan.queries[-1]
        # Drop the temporary table after the root node is executed
        root.post_actions.append(
            Query(
                drop_table_if_exists_statement(temp_table_name),
                is_ddl_on_temp_object=True,
            )
        )
