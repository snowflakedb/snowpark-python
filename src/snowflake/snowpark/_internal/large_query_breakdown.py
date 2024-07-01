
import copy
from typing import List, Optional

from sortedcontainers import SortedList

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import PipelineBreakerCategory, get_complexity_score
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import TempTableReference
from snowflake.snowpark._internal.analyzer.unary_plan_node import CreateTempTableCommand
from snowflake.snowpark._internal.utils import TempObjectType, random_name_for_temp_object


class LargeQueryBreakdown:
    COMPLEXITY_SCORE_LOWER_BOUND = 2_500_000
    COMPLEXITY_SCORE_UPPER_BOUND = 5_000_000

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
        root = copy.deepcopy(root)
        complexity_score = get_complexity_score(root.cumulative_node_complexity)
        while complexity_score > self.COMPLEXITY_SCORE_UPPER_BOUND:
            partition = self.get_partitioned_node(root)
            if partition is None:
                break
            plans.append(partition)
            complexity_score = get_complexity_score(root.cumulative_node_complexity)

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
        current_level = [root]
        pipeline_breaker_list = SortedList()
        parent_map = {}
        parent_map[root] = None

        # Traverse the plan tree
        while current_level:
            next_level = []
            for node in current_level:
                for child in node.children_plan_nodes:
                    next_level.append(child)
                    parent_map[child] = node
                    score = get_complexity_score(child.cumulative_node_complexity)

                    # Categorize the child node based on its complexity score and pipeline breaker category
                    if score > self.COMPLEXITY_SCORE_LOWER_BOUND and score < self.COMPLEXITY_SCORE_UPPER_BOUND:
                        if child.pipeline_breaker_category == PipelineBreakerCategory.PIPELINE_BREAKER:
                            pipeline_breaker_list.append((score, child))

            current_level = next_level

        # Return if we don't find a valid not to break down the query plan
        if not pipeline_breaker_list:
            return None

        # Find the appropriate child and parent nodes to break the plan
        child: Optional[SnowflakePlan] = None
        parent: Optional[SnowflakePlan] = None
        child = pipeline_breaker_list[-1][1]
        parent = parent_map[child]

        # Create a temporary table and replace the child node with the temporary table reference
        temp_table_name = self.session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )
        temp_table_node = self.session._analyzer.resolve(CreateTempTableCommand(child, temp_table_name))
        temp_table_reference = self.session._analyzer.resolve(TempTableReference(temp_table_name))
        parent.replace_child(child, temp_table_reference)

        # Update the SQL and complexity of the parent nodes
        child_sql = child.queries[-1].sql
        while parent is not None:
            parent.queries[-1].sql.replace(child_sql, f"SELECT * FROM {temp_table_name}")
            parent.reset_cumulative_node_complexity()
            parent = parent_map[parent_map]

        return temp_table_node
