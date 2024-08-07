#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
from collections import defaultdict
from typing import List, Optional

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
    Selectable,
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
    SnowflakeTable,
    TableCreationSource,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    CreateDynamicTableCommand,
    CreateViewCommand,
    Pivot,
    Sample,
    Sort,
    Unpivot,
)
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.compiler.utils import (
    TreeNode,
    is_active_transaction,
    plot_plan_if_enabled,
    replace_child,
    update_resolvable_node,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.session import Session

COMPLEXITY_SCORE_LOWER_BOUND = 250_000
COMPLEXITY_SCORE_UPPER_BOUND = 500_000

_logger = logging.getLogger(__name__)


class LargeQueryBreakdown:
    """Optimization to break down large query plans into smaller partitions based on
    estimated complexity score of the plan nodes.

    This optimization works by analyzing computed query complexity score for each input
    plan and breaking down the plan into smaller partitions if we detect valid node
    candidates for partitioning. The partitioning is done by creating temp tables for the
    partitioned nodes and replacing the partitioned subtree with the temp table selectable.
    """

    def __init__(
        self,
        session: Session,
        query_generator: QueryGenerator,
        logical_plans: List[LogicalPlan],
    ) -> None:
        self.session = session
        self._query_generator = query_generator
        self.logical_plans = logical_plans

    def apply(self) -> List[LogicalPlan]:
        if is_active_transaction(self.session):
            # Skip optimization if the session is in an active transaction.
            return self.logical_plans

        resulting_plans = []
        for i, logical_plan in enumerate(self.logical_plans):
            # Similar to the repeated subquery elimination, we rely on
            # nodes of the plan to be SnowflakePlan or Selectable. Here,
            # we resolve the plan to make sure we get a valid plan tree.
            resolved_plan = self._query_generator.resolve(logical_plan)
            partition_plans = self._breakdown_plan(i, resolved_plan)
            resulting_plans.extend(partition_plans)

        return resulting_plans

    def _breakdown_plan(self, plan_index: int, root: TreeNode) -> List[LogicalPlan]:
        """Method to breakdown a single TreeNode into smaller partitions based on
        cumulative complexity score and node type.
        """
        if (
            isinstance(root, SnowflakePlan)
            and root.source_plan is not None
            and isinstance(
                root.source_plan, (CreateViewCommand, CreateDynamicTableCommand)
            )
        ):
            # Skip optimization if the root is a view or a dynamic table.
            return [root]

        complexity_score = get_complexity_score(root.cumulative_node_complexity)
        _logger.debug(f"Complexity score for root[{plan_index}] is: {complexity_score}")
        if complexity_score <= COMPLEXITY_SCORE_UPPER_BOUND:
            # Skip optimization if the complexity score is within the upper bound.
            return [root]

        plans = []

        plot_plan_if_enabled(root, f"/tmp/plots/root_{plan_index}")
        partition_index = 0

        while complexity_score > COMPLEXITY_SCORE_UPPER_BOUND:
            partition = self._get_partitioned_plan(root)
            if partition is None:
                _logger.debug("Could not find a valid node for partitioning.")
                break

            plans.append(partition)
            plot_plan_if_enabled(
                partition, f"/tmp/plots/partition_{plan_index}_{partition_index}"
            )
            partition_index += 1
            complexity_score = get_complexity_score(root.cumulative_node_complexity)

        plot_plan_if_enabled(root, f"/tmp/plots/final_root_{plan_index}")
        plans.append(root)
        return plans

    def _get_partitioned_plan(self, root: TreeNode) -> Optional[SnowflakePlan]:
        current_level = [root]
        pipeline_breaker_list = SortedList(key=lambda x: x[0])
        parent_map = defaultdict(set)

        while current_level:
            next_level = []
            for node in current_level:
                children = (
                    node.children_plan_nodes
                    if isinstance(node, (SnowflakePlan, Selectable))
                    else node.children
                )
                for child in children:
                    parent_map[child].add(node)
                    next_level.append(child)
                    score = get_complexity_score(node.cumulative_node_complexity)
                    if self._is_node_valid_to_breakdown(score, child):
                        # Append score and child to the pipeline breaker sorted list
                        # so that the valid child with the highest complexity score
                        # is at the end of the list.
                        pipeline_breaker_list.add((score, child))

            current_level = next_level

        if not pipeline_breaker_list:
            # Return None if no valid node is found for partitioning.
            return None

        # Get the node with the highest complexity score
        _, child = pipeline_breaker_list.pop()

        # Create a temp table for the partitioned node
        temp_table_name = self._get_temp_table_name()
        temp_table_plan = self._query_generator.resolve(
            SnowflakeCreateTable(
                [temp_table_name],
                None,
                SaveMode.ERROR_IF_EXISTS,
                child,
                table_type="temp",
                creation_source=TableCreationSource.LARGE_QUERY_BREAKDOWN,
            )
        )

        # Update the ancestors with the temp table selectable
        self._update_ancestors(parent_map, child, temp_table_name)
        self._update_root_post_actions(root, temp_table_name)

        return temp_table_plan

    def _is_node_valid_to_breakdown(self, score: int, node: LogicalPlan) -> bool:
        """Method to check if a node is valid to breakdown based on complexity score and node type."""
        return (
            COMPLEXITY_SCORE_LOWER_BOUND < score < COMPLEXITY_SCORE_UPPER_BOUND
        ) and self._is_node_pipeline_breaker(node)

    def _is_node_pipeline_breaker(self, node: LogicalPlan) -> bool:
        """Method to check if a node is a pipeline breaker based on the node type.

        If the node contains a SnowflakePlan, we check its source plan recursively.
        """
        if isinstance(node, (Pivot, Unpivot, Sort, Aggregate)):
            return True

        if isinstance(node, Sample):
            return node.row_count is not None

        if isinstance(node, Union):
            return not node.is_all

        if isinstance(node, SelectStatement):
            return node.order_by is not None

        if isinstance(node, SetStatement):
            return any(operand.operator == SET_UNION for operand in node.set_operands)

        if isinstance(node, SnowflakePlan):
            return node.source_plan is not None and self._is_node_pipeline_breaker(
                node.source_plan
            )

        if isinstance(node, (SelectSnowflakePlan, SelectTableFunction)):
            return self._is_node_pipeline_breaker(node.snowflake_plan)

        return False

    def _update_ancestors(
        self, parent_map: defaultdict, child: LogicalPlan, temp_table_name: str
    ) -> None:
        """This method replaces the child node with a temp table selectable, resets
        the snowflake plan and cumulative complexity score for the ancestors, and
        updates the ancestors with the correct snowflake query corresponding to the
        new plan tree.
        """
        temp_table_node = SnowflakeTable(temp_table_name, session=self.session)
        temp_table_selectable = self._query_generator.create_selectable_entity(
            temp_table_node, analyzer=self._query_generator
        )

        parents = parent_map[child]
        updated_nodes = set()
        for parent in parents:
            replace_child(parent, child, temp_table_selectable, self._query_generator)

        nodes_to_reset = list(parents)
        while nodes_to_reset:
            node = nodes_to_reset.pop()
            if node in updated_nodes:
                # Skip if the node is already updated.
                continue
            parents = parent_map[node]
            nodes_to_reset.extend(parents)
            update_resolvable_node(node, self._query_generator)
            updated_nodes.add(node)

    def _update_root_post_actions(self, root: TreeNode, temp_table_name: str) -> None:
        """Method to updates the root node by adding new temp table that was added to
        materialize a partition of the plan."""
        drop_table_query = Query(
            drop_table_if_exists_statement(temp_table_name), is_ddl_on_temp_object=True
        )
        if root.post_actions is None:
            root.post_actions = []
        root.post_actions.append(drop_table_query)

    def _get_temp_table_name(self) -> str:
        return self.session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )
