#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from typing import TYPE_CHECKING, List, Optional, Union

from sortedcontainers import SortedList

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.select_statement import Selectable
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    SaveMode,
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.compiler.utils import is_active_transaction
from snowflake.snowpark.session import Session

if TYPE_CHECKING:
    PlanTreeNode = Union[SnowflakePlan, Selectable]


class LargeQueryBreakdown:
    COMPLEXITY_SCORE_LOWER_BOUND = 300
    COMPLEXITY_SCORE_UPPER_BOUND = 1000

    def __init__(self, session: Session) -> None:
        self.session = session

    def breakdown_plans(self, plans: List[PlanTreeNode]) -> List[PlanTreeNode]:
        self.use_ctas = not is_active_transaction(self.session)

        resulting_plans = []
        for plan in plans:
            resulting_plans.extend(self.breakdown_plan(plan))

        return resulting_plans

    def breakdown_plan(self, root: PlanTreeNode) -> List[PlanTreeNode]:
        complexity_score = get_complexity_score(root.cumulative_node_complexity)
        if complexity_score < self.COMPLEXITY_SCORE_UPPER_BOUND:
            return [root]

        plans = []

        while complexity_score > self.COMPLEXITY_SCORE_UPPER_BOUND:
            partition = self.get_partitioned_plan(root)
            if partition is None:
                break
            plans.append(partition)
            complexity_score = get_complexity_score(root.cumulative_node_complexity)

        plans.append(root)
        return plans

    def get_partitioned_plan(self, root: PlanTreeNode) -> Optional[PlanTreeNode]:
        current_level = [root]
        pipeline_breaker_list = SortedList(key=lambda x: x[0])
        parent_map = defaultdict(set)

        while current_level:
            next_level = []
            for node in current_level:
                for child in node.children_plan_nodes:
                    parent_map[child].add(node)
                    next_level.append(child)
                    score = get_complexity_score(node.cumulative_node_complexity)
                    if self.is_node_valid_to_breakdown(score, child):
                        pipeline_breaker_list.add((score, child))

            current_level = next_level

        if not pipeline_breaker_list:
            return None

        _, child = pipeline_breaker_list.pop()

        temp_table_name = self.get_temp_table_name()
        temp_table_plan = SnowflakeCreateTable(
            [temp_table_name],
            None,
            SaveMode.ERROR_IF_EXISTS,
            child,
            table_type="temp",
            is_generated=True,
        )

        self.update_ancestors(parent_map, child, temp_table_name)

        return temp_table_plan

    def is_node_valid_to_breakdown(self, score: int, node: PlanTreeNode) -> bool:
        return (
            self.COMPLEXITY_SCORE_LOWER_BOUND
            < score
            < self.COMPLEXITY_SCORE_UPPER_BOUND
        )

    def update_ancestors(
        self, parent_map: defaultdict, child: PlanTreeNode, temp_table_name: str
    ) -> None:
        temp_table_node = self.session._analyzer.create_selectable_entity(
            temp_table_name, analyzer=self.session._analyzer
        )

        parents = parent_map[child]
        for parent in parents:
            parent.replace_child(child, temp_table_node)

        nodes_to_reset = list(parents)
        while nodes_to_reset:
            node = nodes_to_reset.pop()
            nodes_to_reset.extend(parent_map[node])

            # TODO: reset SnowflakePlan for node
            # reset_snowflake_plan(node)
            node.reset_cumulative_node_complexity()
