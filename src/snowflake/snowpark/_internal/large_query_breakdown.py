#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
from collections import defaultdict
from logging import getLogger
from typing import List

from sortedcontainers import SortedList

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PipelineBreakerCategory,
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.analyzer.unary_plan_node import CreateTempTableCommand
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)

_logger = getLogger(__name__)


class LargeQueryBreakdown:
    # COMPLEXITY_SCORE_LOWER_BOUND = 2_500_000
    # COMPLEXITY_SCORE_UPPER_BOUND = 5_000_000
    COMPLEXITY_SCORE_LOWER_BOUND = 300
    COMPLEXITY_SCORE_UPPER_BOUND = 410

    def __init__(self, session) -> None:
        self.session = session

    def breakdown_if_large_plan(self, root: SnowflakePlan) -> List[SnowflakePlan]:
        """
        Breaks down a large SnowflakePlan into smaller plans based on complexity score.

        Args:
            root (SnowflakePlan): The root node of the SnowflakePlan.

        Returns:
            List[SnowflakePlan]: A list of smaller SnowflakePlans.

        """
        plans = []
        complexity_score = get_complexity_score(root.cumulative_node_complexity)
        root = copy.copy(root)
        while complexity_score > self.COMPLEXITY_SCORE_UPPER_BOUND:
            partition = self.get_partitioned_node(root)
            if partition is None:
                _logger.debug("No appropriate node found to break down the query plan")
                break
            _logger.debug(
                f"Partition complexity breakdown: {partition.cumulative_node_complexity}"
            )
            plans.append(partition)
            complexity_score = get_complexity_score(root.cumulative_node_complexity)
            _logger.debug(f"Post breakdown {complexity_score=} for root node")

        plans.append(root)
        return plans

    def get_partitioned_node(self, root: SnowflakePlan) -> SnowflakePlan:
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
        current_level = [root.source_plan]
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
                    if self.is_node_valid_to_breakdown(
                        score, child.pipeline_breaker_category
                    ):
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
            CreateTempTableCommand(child, temp_table_name)
        )

        self.update_ancestors(parent_map, child, temp_table_name)
        self.update_root_query(root, temp_table_name)

        return temp_table_plan

    def get_temp_table_name(self) -> str:
        return self.session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )

    def is_node_valid_to_breakdown(
        self, score: int, pipeline_breaker_category: PipelineBreakerCategory
    ) -> bool:
        """Check if the node is valid to break down based on the complexity score and pipeline breaker category."""

        valid_score_range = (
            score > self.COMPLEXITY_SCORE_LOWER_BOUND
            and score <= self.COMPLEXITY_SCORE_UPPER_BOUND + 20
        )

        valid_pipeline_breaker_category = (
            pipeline_breaker_category == PipelineBreakerCategory.PIPELINE_BREAKER
        )

        return valid_score_range and valid_pipeline_breaker_category

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
        root.post_actions.append(Query(f"DROP TABLE IF EXISTS {temp_table_name}"))
