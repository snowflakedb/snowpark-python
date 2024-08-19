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
    replace_child,
    update_resolvable_node,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.session import Session

# The complexity score lower bound is set to match COMPILATION_MEMORY_LIMIT
# in Snowflake. This is the limit where we start seeing compilation errors.
COMPLEXITY_SCORE_LOWER_BOUND = 10_000_000
COMPLEXITY_SCORE_UPPER_BOUND = 12_000_000

_logger = logging.getLogger(__name__)


class LargeQueryBreakdown:
    r"""Optimization to break down large query plans into smaller partitions based on
    estimated complexity score of the plan nodes.

    This optimization works by analyzing computed query complexity score for each input
    plan and breaking down the plan into smaller partitions if we detect valid node
    candidates for partitioning. The partitioning is done by creating temp tables for the
    partitioned nodes and replacing the partitioned subtree with the temp table selectable.

    Example:
        For a data pipeline with a large query plan created like so:

            >>> base_df = session.sql("select 1 as A, 2 as B")
            >>> df1 = base_df.with_column("A", F.col("A") + F.lit(1))
            >>> x = 100
            >>> for i in range(x):
            >>>     df1 = df1.with_column("A", F.col("A") + F.lit(i))
            >>> df1 = df1.group_by(F.col("A")).agg(F.sum(F.col("B")).alias("B"))

            >>> df2 = base_df.with_column("B", F.col("B") + F.lit(1))
            >>> for i in range(x):
            >>>     df2 = df2.with_column("B", F.col("B") + F.lit(i))
            >>> df2 = df2.group_by(F.col("B")).agg(F.sum(F.col("A")).alias("A"))

            >>> union_df = df1.union_all(df2)
            >>> final_df = union_df.with_column("A", F.col("A") + F.lit(1))

        The corresponding query plan has the following structure:

                                 projection on result
                                           |
                                       UNION ALL
        Groupby + Agg (A) ---------------/   \------------------ Groupby + Agg (B)
        with columns set 1                                      with columns set 2



        Given the right complexity bounds, large query breakdown optimization will break down
        the plan into smaller partition and give us the following plan:


           Create Temp table (T1)                                    projection on result
                    |                      ,                                   |
            Groupby + Agg (A)                                              UNION ALL
            with columns set 1                  Select * from T1 -----------/  \-----------  Groupby + Agg (B)
                                                                                             with columns set 2
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
        self._parent_map = defaultdict(set)

    def apply(self) -> List[LogicalPlan]:
        if is_active_transaction(self.session):
            # Skip optimization if the session is in an active transaction.
            _logger.debug(
                "Skipping large query breakdown optimization due to active transaction."
            )
            return self.logical_plans

        resulting_plans = []
        for i, logical_plan in enumerate(self.logical_plans):
            # Similar to the repeated subquery elimination, we rely on
            # nodes of the plan to be SnowflakePlan or Selectable. Here,
            # we resolve the plan to make sure we get a valid plan tree.
            resolved_plan = self._query_generator.resolve(logical_plan)
            partition_plans = self._try_to_breakdown_plan(i, resolved_plan)
            resulting_plans.extend(partition_plans)

        return resulting_plans

    def _try_to_breakdown_plan(
        self, plan_index: int, root: TreeNode
    ) -> List[LogicalPlan]:
        """Method to breakdown a single TreeNode into smaller partitions based on
        cumulative complexity score and node type.

        This method tried to breakdown the root plan into smaller partitions until the root complexity
        score is within the upper bound. To do this, we follow these steps until the root complexity is
        above the upper bound:

        1. Find a valid node for partitioning.
        2. If not node if found, break the partitioning loop and return all partitioned plans.
        3. For each valid node, cut the node out from the root and create a temp table plan for the partition.
        4. Update the ancestors snowflake plans to generate the correct queries.
        """
        if (
            isinstance(root, SnowflakePlan)
            and root.source_plan is not None
            and isinstance(
                root.source_plan, (CreateViewCommand, CreateDynamicTableCommand)
            )
        ):
            # Skip optimization if the root is a view or a dynamic table.
            _logger.debug(
                "Skipping large query breakdown optimization for view/dynamic table plan."
            )
            return [root]

        complexity_score = get_complexity_score(root.cumulative_node_complexity)
        _logger.debug(f"Complexity score for root[{plan_index}] is: {complexity_score}")

        if complexity_score <= COMPLEXITY_SCORE_UPPER_BOUND:
            # Skip optimization if the complexity score is within the upper bound.
            return [root]

        plans = []
        # TODO: SNOW-1617634 Have a one pass algorithm to find the valid node for partitioning
        while complexity_score > COMPLEXITY_SCORE_UPPER_BOUND:
            child = self._find_node_to_breakdown(root)
            if child is None:
                _logger.debug(
                    f"Could not find a valid node for partitioning. Skipping with root {complexity_score=}"
                )
                break

            partition = self._get_partitioned_plan(root, child)
            plans.append(partition)
            complexity_score = get_complexity_score(root.cumulative_node_complexity)

        plans.append(root)
        return plans

    def _find_node_to_breakdown(self, root: TreeNode) -> Optional[TreeNode]:
        """This method traverses the plan tree and partitions the plan based if a valid partition node
        if found. The steps involved are:

        1. Traverse the plan tree and find the valid nodes for partitioning.
        2. If no valid node is found, return None.
        3. Keep valid nodes in a sorted list based on the complexity score.
        4. Return the node with the highest complexity score.
        """
        current_level = [root]
        pipeline_breaker_list = SortedList(key=lambda x: x[0])

        while current_level:
            next_level = []
            for node in current_level:
                children = (
                    node.children_plan_nodes
                    if isinstance(node, (SnowflakePlan, Selectable))
                    else node.children
                )
                for child in children:
                    self._parent_map[child].add(node)
                    score = get_complexity_score(child.cumulative_node_complexity)
                    if self._is_node_valid_to_breakdown(score, child):
                        # Append score and child to the pipeline breaker sorted list
                        # so that the valid child with the highest complexity score
                        # is at the end of the list.
                        pipeline_breaker_list.add((score, child))
                    else:
                        # don't traverse subtrees if parent is a valid candidate
                        next_level.append(child)

            current_level = next_level

        if not pipeline_breaker_list:
            # Return None if no valid node is found for partitioning.
            return None

        # Get the node with the highest complexity score
        _, child = pipeline_breaker_list.pop()
        return child

    def _get_partitioned_plan(self, root: TreeNode, child: TreeNode) -> SnowflakePlan:
        """This method takes cuts the child out from the root, creates a temp table plan for the
        partitioned child and returns the plan. The steps involved are:

        1. Create a temp table for the partition.
        2. Update the parent with the temp table selectable
        3. Reset snowflake plans for all ancestors so they contain correct queries.
        3. Return the temp table plan.
        """

        # Create a temp table for the partitioned node
        temp_table_name = self.session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )
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
        self._replace_child_and_update_ancestors(child, temp_table_name)

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
        # Pivot/Unpivot, Sort, and GroupBy+Aggregate are pipeline breakers.
        if isinstance(node, (Pivot, Unpivot, Sort, Aggregate)):
            return True

        if isinstance(node, Sample):
            # Row sampling is a pipeline breaker
            return node.row_count is not None

        if isinstance(node, Union):
            # Union is a pipeline breaker since it is a UNION ALL + group by aggregation
            return not node.is_all

        if isinstance(node, SelectStatement):
            # SelectStatement is a pipeline breaker if it contains an order by clause since sorting
            # is a pipeline breaker.
            return node.order_by is not None

        if isinstance(node, SetStatement):
            # SetStatement is a pipeline breaker if it has a UNION operator.
            return any(operand.operator == SET_UNION for operand in node.set_operands)

        if isinstance(node, SnowflakePlan):
            return node.source_plan is not None and self._is_node_pipeline_breaker(
                node.source_plan
            )

        if isinstance(node, (SelectSnowflakePlan)):
            return self._is_node_pipeline_breaker(node.snowflake_plan)

        return False

    def _replace_child_and_update_ancestors(
        self, child: LogicalPlan, temp_table_name: str
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

        # add drop table in post action and see if it propagates to the root
        drop_table_query = Query(
            drop_table_if_exists_statement(temp_table_name), is_ddl_on_temp_object=True
        )
        temp_table_selectable.post_actions = [drop_table_query]

        parents = self._parent_map[child]
        updated_nodes = set()
        for parent in parents:
            replace_child(parent, child, temp_table_selectable, self._query_generator)

        nodes_to_reset = list(parents)
        while nodes_to_reset:
            node = nodes_to_reset.pop()
            if node in updated_nodes:
                # Skip if the node is already updated.
                continue

            update_resolvable_node(node, self._query_generator)
            updated_nodes.add(node)

            parents = self._parent_map[node]
            nodes_to_reset.extend(parents)
