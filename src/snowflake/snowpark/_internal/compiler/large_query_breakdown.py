#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
from collections import defaultdict
from typing import List, Optional, Tuple

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
    _plot_plan_if_enabled,
    is_active_transaction,
    re_resolve_plan,
    replace_child,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.session import Session

COMPLEXITY_SCORE_LOWER_BOUND = 300
COMPLEXITY_SCORE_UPPER_BOUND = 1000

_logger = logging.getLogger(__name__)


class LargeQueryBreakdown:
    """This class is used to break down large query plans into smaller partitions based on
    estimated complexity score of the plan nodes.
    """

    def __init__(
        self,
        session: Session,
        query_generator: QueryGenerator,
        plans: List[LogicalPlan],
    ) -> None:
        self.session = session
        self._query_generator = query_generator
        self.plans = plans

    def apply(self) -> List[LogicalPlan]:
        if is_active_transaction(self.session):
            return self.plans

        resulting_plans, resulting_drops_queries = [], []
        for i, plan in enumerate(self.plans):
            partition_plan, drop_queries = self._breakdown_plan(i, plan)
            resulting_plans.extend(partition_plan)
            resulting_drops_queries.extend(drop_queries)

        # Add the drop queries to the last plan
        resulting_plans[-1].post_actions.extend(resulting_drops_queries)
        return resulting_plans

    def _breakdown_plan(
        self, plan_index: int, root: LogicalPlan
    ) -> Tuple[List[LogicalPlan], List[Query]]:
        if (
            isinstance(root, SnowflakePlan)
            and root.source_plan is not None
            and isinstance(
                root.source_plan, (CreateViewCommand, CreateDynamicTableCommand)
            )
        ):
            return [root], []
        if isinstance(root, (CreateViewCommand, CreateDynamicTableCommand)):
            return [root], []

        complexity_score = get_complexity_score(root.cumulative_node_complexity)
        _logger.debug(f"Complexity score for root is: {complexity_score}")
        if complexity_score <= COMPLEXITY_SCORE_UPPER_BOUND:
            return [root], []

        plans = []

        _plot_plan_if_enabled(root, f"/tmp/plots/root_{plan_index}")
        partition_index = 0
        drop_queries = []

        while complexity_score > COMPLEXITY_SCORE_UPPER_BOUND:
            partition, drop_query = self._get_partitioned_plan(root)
            if partition is None:
                _logger.debug("Could not find a valid node for partitioning.")
                break
            plans.append(partition)
            drop_queries.append(drop_query)
            _plot_plan_if_enabled(
                partition, f"/tmp/plots/partition_{plan_index}_{partition_index}"
            )
            partition_index += 1
            complexity_score = get_complexity_score(root.cumulative_node_complexity)

        _plot_plan_if_enabled(root, f"/tmp/plots/final_root_{plan_index}")
        plans.append(root)
        return plans, drop_queries

    def _get_partitioned_plan(
        self, root: LogicalPlan
    ) -> Tuple[Optional[SnowflakePlan], Optional[Query]]:
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
                        pipeline_breaker_list.add((score, child))

            current_level = next_level

        if not pipeline_breaker_list:
            return None, None

        _, child = pipeline_breaker_list.pop()

        temp_table_name = self._get_temp_table_name()
        temp_table_plan = self._query_generator.resolve(
            SnowflakeCreateTable(
                [temp_table_name],
                None,
                SaveMode.OVERWRITE,
                child,
                table_type="temp",
                is_generated=True,
            )
        )

        self._update_ancestors(parent_map, child, temp_table_name)
        drop_table_query = Query(
            drop_table_if_exists_statement(temp_table_name), is_ddl_on_temp_object=True
        )
        return temp_table_plan, drop_table_query

    def _is_node_valid_to_breakdown(self, score: int, node: LogicalPlan) -> bool:
        """Method to check if a node is valid to breakdown based on complexity score and node type."""
        return (
            COMPLEXITY_SCORE_LOWER_BOUND < score < COMPLEXITY_SCORE_UPPER_BOUND
        ) and self._is_node_pipeline_breaker(node)

    def _is_node_pipeline_breaker(self, node: LogicalPlan) -> bool:
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
                continue
            parents = parent_map[node]
            nodes_to_reset.extend(parents)
            re_resolve_plan(node, self._query_generator)
            updated_nodes.add(node)

    def _get_temp_table_name(self) -> str:
        return self.session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )
