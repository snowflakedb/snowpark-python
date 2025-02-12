#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

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
    WithQueryBlock,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    CreateDynamicTableCommand,
    CreateViewCommand,
    Distinct,
    Pivot,
    Sample,
    Sort,
    Unpivot,
)
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.compiler.telemetry_constants import (
    CompilationStageTelemetryField,
    NodeBreakdownCategory,
    SkipLargeQueryBreakdownCategory,
)
from snowflake.snowpark._internal.compiler.utils import (
    TreeNode,
    extract_child_from_with_query_block,
    is_active_transaction,
    is_with_query_block,
    replace_child,
    update_resolvable_node,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.session import Session

_logger = logging.getLogger(__name__)


class LargeQueryBreakdownResult:
    # the resulting logical plans after large query breakdown
    logical_plans: List[LogicalPlan]
    # breakdown summary for each root plan
    breakdown_summary: List[Dict[str, int]]
    # skipped summary for each root plan
    skipped_summary: Dict[str, int]

    def __init__(
        self,
        logical_plans: List[LogicalPlan],
        breakdown_summary: List[dict],
        skipped_summary: Dict[str, int],
    ) -> None:
        self.logical_plans = logical_plans
        self.breakdown_summary = breakdown_summary
        self.skipped_summary = skipped_summary


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
        complexity_bounds: Tuple[int, int],
    ) -> None:
        self.session = session
        self._query_generator = query_generator
        self.logical_plans = logical_plans
        self._parent_map = defaultdict(set)
        self.complexity_score_lower_bound = complexity_bounds[0]
        self.complexity_score_upper_bound = complexity_bounds[1]
        # This is used to track the breakdown summary for each root plan.
        # It contains the statistics for number of partitions made. If the final
        # partition could not proceed, it contains how the nodes in this partitions
        # were classified.
        self._breakdown_summary: list = list()
        # This is used to track the summary of reason why the optimization was skipped
        # on a root plan.
        self._skipped_summary: dict = defaultdict(int)

    def apply(self) -> LargeQueryBreakdownResult:
        reason = self._should_skip_optimization_for_session()
        if reason is not None:
            return LargeQueryBreakdownResult(self.logical_plans, [], {reason.value: 1})

        resulting_plans = []
        for logical_plan in self.logical_plans:
            # Similar to the repeated subquery elimination, we rely on
            # nodes of the plan to be SnowflakePlan or Selectable. Here,
            # we resolve the plan to make sure we get a valid plan tree.
            resolved_plan = self._query_generator.resolve(logical_plan)
            partition_plans = self._try_to_breakdown_plan(resolved_plan)
            resulting_plans.extend(partition_plans)

        return LargeQueryBreakdownResult(
            resulting_plans, self._breakdown_summary, self._skipped_summary
        )

    def _should_skip_optimization_for_session(
        self,
    ) -> Optional[SkipLargeQueryBreakdownCategory]:
        """Method to check if the optimization should be skipped based on the session state.

        Returns:
            SkipLargeQueryBreakdownCategory: enum indicating the reason for skipping the optimization.
                if the optimization should be skipped, otherwise None.
        """
        if self.session.get_current_database() is None:
            # Skip optimization if there is no active database.
            _logger.debug(
                "Skipping large query breakdown optimization since there is no active database."
            )
            return SkipLargeQueryBreakdownCategory.NO_ACTIVE_DATABASE

        if self.session.get_current_schema() is None:
            # Skip optimization if there is no active schema.
            _logger.debug(
                "Skipping large query breakdown optimization since there is no active schema."
            )
            return SkipLargeQueryBreakdownCategory.NO_ACTIVE_SCHEMA

        if is_active_transaction(self.session):
            # Skip optimization if the session is in an active transaction.
            _logger.debug(
                "Skipping large query breakdown optimization due to active transaction."
            )
            return SkipLargeQueryBreakdownCategory.ACTIVE_TRANSACTION

        return None

    def _should_skip_optimization_for_root(
        self, root: TreeNode
    ) -> Optional[SkipLargeQueryBreakdownCategory]:
        """Method to check if the optimization should be skipped based on the root node type.

        Returns:
            SkipLargeQueryBreakdownCategory enum indicating the reason for skipping the optimization
                if the optimization should be skipped, otherwise None.
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
            return SkipLargeQueryBreakdownCategory.VIEW_DYNAMIC_TABLE

        return None

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
        reason = self._should_skip_optimization_for_root(root)
        if reason is not None:
            self._skipped_summary[reason.value] += 1
            return [root]

        complexity_score = get_complexity_score(root)
        _logger.debug(f"Complexity score for root {type(root)} is: {complexity_score}")

        if complexity_score <= self.complexity_score_upper_bound:
            # Skip optimization if the complexity score is within the upper bound.
            return [root]

        plans = []
        self._current_breakdown_summary: Dict[str, Any] = {
            CompilationStageTelemetryField.NUM_PARTITIONS_MADE.value: 0,
            CompilationStageTelemetryField.NUM_PIPELINE_BREAKER_USED.value: 0,
            CompilationStageTelemetryField.NUM_RELAXED_BREAKER_USED.value: 0,
        }
        while complexity_score > self.complexity_score_upper_bound:
            child, validity_statistics = self._find_node_to_breakdown(root)
            self._update_current_breakdown_summary(validity_statistics)

            if child is None:
                _logger.debug(
                    f"Could not find a valid node for partitioning. "
                    f"Skipping with root {complexity_score=} {self._current_breakdown_summary=}"
                )
                break

            partition = self._get_partitioned_plan(child)
            plans.append(partition)
            complexity_score = get_complexity_score(root)

        self._breakdown_summary.append(self._current_breakdown_summary)
        plans.append(root)
        return plans

    def _update_current_breakdown_summary(
        self, validity_statistics: Dict[NodeBreakdownCategory, int]
    ) -> None:
        """Method to update the breakdown summary based on the validity statistics of the current root."""
        if validity_statistics.get(NodeBreakdownCategory.VALID_NODE, 0) > 0:
            self._current_breakdown_summary[
                CompilationStageTelemetryField.NUM_PARTITIONS_MADE.value
            ] += 1
            self._current_breakdown_summary[
                CompilationStageTelemetryField.NUM_PIPELINE_BREAKER_USED.value
            ] += 1
        elif validity_statistics.get(NodeBreakdownCategory.VALID_NODE_RELAXED, 0) > 0:
            self._current_breakdown_summary[
                CompilationStageTelemetryField.NUM_PARTITIONS_MADE.value
            ] += 1
            self._current_breakdown_summary[
                CompilationStageTelemetryField.NUM_RELAXED_BREAKER_USED.value
            ] += 1
        else:  # no valid nodes found
            self._current_breakdown_summary[
                CompilationStageTelemetryField.FAILED_PARTITION_SUMMARY.value
            ] = {k.value: validity_statistics.get(k, 0) for k in NodeBreakdownCategory}

    def _find_node_to_breakdown(
        self, root: TreeNode
    ) -> Tuple[Optional[TreeNode], Dict[NodeBreakdownCategory, int]]:
        """This method traverses the plan tree and partitions the plan based if a valid partition node
        if found. The steps involved are:

            1. Traverse the plan tree and find the valid nodes for partitioning.
            2. If no valid node is found, return None.
            3. Return the node with the highest complexity score.
            4. Return the statistics of partition for the current root.
        """
        current_level = [root]
        candidate_node, relaxed_candidate_node = None, None
        # start with -1 since score is always > 0
        candidate_score, relaxed_candidate_score = -1, -1
        current_node_validity_statistics = defaultdict(int)

        while current_level:
            next_level = []
            for node in current_level:
                assert isinstance(node, (Selectable, SnowflakePlan))
                for child in node.children_plan_nodes:
                    self._parent_map[child].add(node)
                    validity_status, score = self._is_node_valid_to_breakdown(
                        child, root
                    )
                    if validity_status == NodeBreakdownCategory.VALID_NODE:
                        # If the score for valid node is higher than the last candidate,
                        # update the candidate node and score.
                        if score > candidate_score:
                            candidate_score = score
                            candidate_node = child
                    else:
                        # don't traverse subtrees if parent is a valid candidate
                        next_level.append(child)

                    if validity_status == NodeBreakdownCategory.VALID_NODE_RELAXED:
                        # Update the relaxed candidate node and score.
                        if score > relaxed_candidate_score:
                            relaxed_candidate_score = score
                            relaxed_candidate_node = child

                    # Update the statistics for the current node.
                    current_node_validity_statistics[validity_status] += 1

            current_level = next_level

        # If no valid node is found, candidate_node will be None.
        # Otherwise, return the node with the highest complexity score.
        return (
            candidate_node or relaxed_candidate_node,
            current_node_validity_statistics,
        )

    def _get_partitioned_plan(self, child: TreeNode) -> SnowflakePlan:
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
                extract_child_from_with_query_block(child)
                if is_with_query_block(child)
                else child,
                table_type="temp",
                creation_source=TableCreationSource.LARGE_QUERY_BREAKDOWN,
            )
        )

        # Update the ancestors with the temp table selectable
        self._replace_child_and_update_ancestors(child, temp_table_name)

        return temp_table_plan

    def _is_node_valid_to_breakdown(
        self, node: TreeNode, root: TreeNode
    ) -> Tuple[NodeBreakdownCategory, int]:
        """Method to check if a node is valid to breakdown based on complexity score and node type.

        Returns:
            A tuple of =>
                InvalidNodesInBreakdownCategory: indicating the primary reason
                    for invalidity if the node is invalid.
                int: the complexity score of the node.
        """
        score = get_complexity_score(node)
        is_valid = True
        validity_status = NodeBreakdownCategory.VALID_NODE

        # check score bounds
        if score < self.complexity_score_lower_bound:
            is_valid = False
            validity_status = NodeBreakdownCategory.SCORE_BELOW_LOWER_BOUND

        if score > self.complexity_score_upper_bound:
            is_valid = False
            validity_status = NodeBreakdownCategory.SCORE_ABOVE_UPPER_BOUND

        # check pipeline breaker condition
        if is_valid and not self._is_node_pipeline_breaker(node):
            if self._is_relaxed_pipeline_breaker(node):
                validity_status = NodeBreakdownCategory.VALID_NODE_RELAXED
            else:
                is_valid = False
                validity_status = NodeBreakdownCategory.NON_PIPELINE_BREAKER

        # check external CTE ref condition
        if is_valid and self._contains_external_cte_ref(node, root):
            is_valid = False
            validity_status = NodeBreakdownCategory.EXTERNAL_CTE_REF

        if is_valid:
            _logger.debug(
                f"Added node of type {type(node)} with score {score} to pipeline breaker list."
            )

        return validity_status, score

    def _contains_external_cte_ref(self, node: TreeNode, root: TreeNode) -> bool:
        """Method to check if a node contains a CTE in its subtree that is also referenced
        by a different node that lies outside the subtree. An example situation is:

                                   root
                                /       \
                            node1       node5
                            /    \
                        node2    node3
                       /    |      |
                   node4  SelectSnowflakePlan
                                |
                           SnowflakePlan
                                |
                           WithQueryBlock
                                |
                              node6

        In this example, node2 contains a WithQueryBlock node that is also referenced
        externally by node3.
        Similarly, node3 contains a WithQueryBlock node that is also referenced externally
        by node2.
        However, node1 contains WithQueryBlock node that is not referenced externally.

        If we compare the count of WithQueryBlock for different nodes, we get:
          NODE:                 COUNT:    Externally Referenced:
          ======================================================
          node1                 2         False
          node2                 1         True
          node3                 1         True
          root                  2         False
          SelectSnowflakePlan   1         False
          SnowflakePlan         1         False

        We determine if a node contains an externally referenced CTE by comparing the
        number of times each unique WithQueryBlock node is referenced in the subtree compared
        to the number of times it is referenced in the root node.
        """

        # Checks for SnowflakePlan and SelectSnowflakePlan is to prevent marking a WithQueryBlock, which is a pipeline breaker
        # node as an external CTE ref.
        if isinstance(node, SelectSnowflakePlan):
            return self._contains_external_cte_ref(node.snowflake_plan, root)

        if isinstance(node, SnowflakePlan) and isinstance(
            node.source_plan, WithQueryBlock
        ):
            ignore_with_query_block = node.source_plan
        else:
            ignore_with_query_block = None

        for with_query_block, node_count in node.referenced_ctes.items():
            if with_query_block is ignore_with_query_block:
                continue
            root_count = root.referenced_ctes[with_query_block]
            if node_count != root_count:
                return True

        return False

    def _is_relaxed_pipeline_breaker(self, node: LogicalPlan) -> bool:
        """Method to check if a node is a relaxed pipeline breaker based on the node type."""
        if isinstance(node, SelectStatement):
            return True

        if isinstance(node, SnowflakePlan):
            return node.source_plan is not None and self._is_relaxed_pipeline_breaker(
                node.source_plan
            )

        if isinstance(node, SelectSnowflakePlan):
            return self._is_relaxed_pipeline_breaker(node.snowflake_plan)

        return False

    def _is_node_pipeline_breaker(self, node: LogicalPlan) -> bool:
        """Method to check if a node is a pipeline breaker based on the node type.

        If the node contains a SnowflakePlan, we check its source plan recursively.
        """
        # Pivot/Unpivot, Sort, and GroupBy+Aggregate are pipeline breakers.
        if isinstance(
            node, (Pivot, Unpivot, Sort, Aggregate, WithQueryBlock, Distinct)
        ):
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
            # SelectStatement is a pipeline breaker if
            # - it contains an order by clause since sorting is a pipeline breaker.
            # - it contains a distinct clause since distinct is a pipeline breaker.
            return node.order_by is not None or node.distinct_

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
        for parent in parents:
            replace_child(parent, child, temp_table_selectable, self._query_generator)

        nodes_to_reset = list(parents)
        while nodes_to_reset:
            node = nodes_to_reset.pop()

            update_resolvable_node(node, self._query_generator)

            parents = self._parent_map[node]
            nodes_to_reset.extend(parents)
