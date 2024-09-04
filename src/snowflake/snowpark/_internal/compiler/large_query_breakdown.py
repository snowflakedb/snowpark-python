#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
from collections import defaultdict
from typing import List, Optional, Tuple

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    drop_table_if_exists_statement,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    Except,
    Intersect,
    Union,
)
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_INTERSECT,
    SET_UNION_ALL,
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
from snowflake.snowpark._internal.compiler.telemetry_constants import (
    SkipLargeQueryBreakdownCategory,
)
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

            base_df = session.sql("select 1 as A, 2 as B")
            df1 = base_df.with_column("A", F.col("A") + F.lit(1))
            df2 = base_df.with_column("B", F.col("B") + F.lit(1))

            for i in range(100):
                df1 = df1.with_column("A", F.col("A") + F.lit(i))
                df2 = df2.with_column("B", F.col("B") + F.lit(i))

            df1 = df1.group_by(F.col("A")).agg(F.sum(F.col("B")).alias("B"))
            df2 = df2.group_by(F.col("B")).agg(F.sum(F.col("A")).alias("A"))

            union_df = df1.union_all(df2)
            final_df = union_df.with_column("A", F.col("A") + F.lit(1))

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
            self.session._conn._telemetry_client.send_large_query_optimization_skipped_telemetry(
                self.session.session_id,
                SkipLargeQueryBreakdownCategory.ACTIVE_TRANSACTION.value,
            )
            return self.logical_plans

        resulting_plans = []
        for logical_plan in self.logical_plans:
            # Similar to the repeated subquery elimination, we rely on
            # nodes of the plan to be SnowflakePlan or Selectable. Here,
            # we resolve the plan to make sure we get a valid plan tree.
            resolved_plan = self._query_generator.resolve(logical_plan)
            partition_plans = self._try_to_breakdown_plan(resolved_plan)
            resulting_plans.extend(partition_plans)

        return resulting_plans

    def _try_to_breakdown_plan(self, root: TreeNode) -> List[LogicalPlan]:
        """Method to breakdown a single plan into smaller partitions based on
        cumulative complexity score and node type.

        This method tried to breakdown the root plan into smaller partitions until the root complexity
        score is within the upper bound. To do this, we follow these steps until the root complexity is
        above the upper bound:

        1. Find a valid node for partitioning.
        2. If not node if found, break the partitioning loop and return all partitioned plans.
        3. For each valid node, cut the node out from the root and create a temp table plan for the partition.
        4. Update the ancestors snowflake plans to generate the correct queries.
        """
        _logger.debug(
            f"Applying large query breakdown optimization for root of type {type(root)}"
        )
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
            self.session._conn._telemetry_client.send_large_query_optimization_skipped_telemetry(
                self.session.session_id,
                SkipLargeQueryBreakdownCategory.VIEW_DYNAMIC_TABLE.value,
            )
            return [root]

        complexity_score = get_complexity_score(root.cumulative_node_complexity)
        _logger.debug(f"Complexity score for root {type(root)} is: {complexity_score}")

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
            3. Return the node with the highest complexity score.
        """
        current_level = [root]
        candidate_node = None
        candidate_score = -1  # start with -1 since score is always > 0

        while current_level:
            next_level = []
            for node in current_level:
                assert isinstance(node, (Selectable, SnowflakePlan))
                for child in node.children_plan_nodes:
                    self._parent_map[child].add(node)
                    valid_to_breakdown, score = self._is_node_valid_to_breakdown(child)
                    if valid_to_breakdown:
                        # If the score for valid node is higher than the last candidate,
                        # update the candidate node and score.
                        if score > candidate_score:
                            candidate_score = score
                            candidate_node = child
                    else:
                        # don't traverse subtrees if parent is a valid candidate
                        next_level.append(child)

            current_level = next_level

        # If no valid node is found, candidate_node will be None.
        # Otherwise, return the node with the highest complexity score.
        return candidate_node

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

    def _is_node_valid_to_breakdown(self, node: LogicalPlan) -> Tuple[bool, int]:
        """Method to check if a node is valid to breakdown based on complexity score and node type.

        Returns:
            A tuple of boolean indicating if the node is valid for partitioning and the complexity score.
        """
        score = get_complexity_score(node.cumulative_node_complexity)
        valid_node = (
            COMPLEXITY_SCORE_LOWER_BOUND < score < COMPLEXITY_SCORE_UPPER_BOUND
        ) and self._is_node_pipeline_breaker(node)
        if valid_node:
            _logger.debug(
                f"Added node of type {type(node)} with score {score} to pipeline breaker list."
            )
        return valid_node, score

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
            # Union is a pipeline breaker since it is a UNION ALL + distinct
            return not node.is_all

        if isinstance(node, (Except, Intersect)):
            # Except and Intersect are pipeline breakers since they are join + distinct
            return True

        if isinstance(node, SelectStatement):
            # SelectStatement is a pipeline breaker if it contains an order by clause since sorting
            # is a pipeline breaker.
            return node.order_by is not None

        if isinstance(node, SetStatement):
            # If the last operator applied in the SetStatement is a pipeline breaker, then the
            # SetStatement is a pipeline breaker. We determine the last operator by checking the
            # operands in the operator list. The last operator is the last operator to be executed
            # in the query based on precedence. Since INTERSECT has the highest precedence and
            # other operators have equal precedence, we make a list of non-INTERSECT operators
            # to determine the last operator.

            # operands[0].operator is ignored in generating the query
            operators = [operand.operator for operand in node.set_operands[1:]]

            # INTERSECT has the highest precedence. EXCEPT, UNION, UNION ALL have the same precedence.
            non_intersect_operators = list(
                filter(lambda x: x != SET_INTERSECT, operators)
            )
            if len(non_intersect_operators) == 0:
                # If all operators are INTERSECT, then the SetStatement is a pipeline breaker.
                return True

            return non_intersect_operators[-1] != SET_UNION_ALL

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

        # add drop table in post action since the temp table created here
        # is only used for the current query.
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
