#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from typing import TYPE_CHECKING, List, Optional, Union as UnionType

from sortedcontainers import SortedList

from snowflake.snowpark._internal.analyzer.binary_plan_node import Union
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_UNION,
    Selectable,
    SelectStatement,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
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
from snowflake.snowpark._internal.compiler.utils import (
    is_active_transaction,
    replace_child,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.session import Session

if TYPE_CHECKING:
    PlanTreeNode = UnionType[SnowflakePlan, Selectable]

COMPLEXITY_SCORE_LOWER_BOUND = 300
COMPLEXITY_SCORE_UPPER_BOUND = 1000


class LargeQueryBreakdown:
    """This class is used to break down large query plans into smaller partitions based on
    estimated complexity score of the plan nodes.
    """

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
        if complexity_score <= COMPLEXITY_SCORE_UPPER_BOUND:
            return [root]

        plans = []

        while complexity_score > COMPLEXITY_SCORE_UPPER_BOUND:
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
        """Method to check if a node is valid to breakdown based on complexity score and node type."""
        return (
            COMPLEXITY_SCORE_LOWER_BOUND < score < COMPLEXITY_SCORE_UPPER_BOUND
        ) and self.is_node_pipeline_breaker(node)

    def is_node_pipeline_breaker(self, node: PlanTreeNode) -> bool:
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
            return node.source_plan is not None and self.is_node_pipeline_breaker(
                node.source_plan
            )

    def update_ancestors(
        self, parent_map: defaultdict, child: PlanTreeNode, temp_table_name: str
    ) -> None:
        temp_table_node = self.session._analyzer.create_selectable_entity(
            temp_table_name, analyzer=self.session._analyzer
        )

        parents = parent_map[child]
        for parent in parents:
            replace_child(parent, child, temp_table_node)

        nodes_to_reset = list(parents)
        while nodes_to_reset:
            node = nodes_to_reset.pop()
            nodes_to_reset.extend(parent_map[node])

            # TODO: reset SnowflakePlan for node
            # reset_snowflake_plan(node)
            node.reset_cumulative_node_complexity()

    def get_temp_table_name(self) -> str:
        return self.session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )
