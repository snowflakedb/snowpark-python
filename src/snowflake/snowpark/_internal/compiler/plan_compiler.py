#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
import time
from typing import Dict, List

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    PlanQueryType,
    Query,
    SnowflakePlan,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.compiler.large_query_breakdown import (
    COMPLEXITY_SCORE_LOWER_BOUND,
    COMPLEXITY_SCORE_UPPER_BOUND,
    LargeQueryBreakdown,
)
from snowflake.snowpark._internal.compiler.repeated_subquery_elimination import (
    RepeatedSubqueryElimination,
)
from snowflake.snowpark._internal.compiler.telemetry_constants import (
    CompilationStageTelemetryField,
)
from snowflake.snowpark._internal.compiler.utils import create_query_generator
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark.mock._connection import MockServerConnection


class PlanCompiler:
    """
    This class is responsible for compiling a SnowflakePlan to list of queries and post actions that
    will be sent over to the server for execution.

    The entry point function is compile(), which applies the following steps:
    1) Run pre-check for the Snowflake plan, which mainly checks if optimizations can be applied.
    2) Run pre-process step if optimization can be applied, which extracts and copies the set of
        logical plans associated with the original plan to apply optimizations on.
    3) Applies steps of optimizations. Each optimization takes a set of logical plan and produces a
        new set of logical plans. Note that the optimizations will not maintain schema/attributes,
        so none of the optimizations should rely on the schema/attributes.
    4) Generate queries.
    """

    def __init__(self, plan: SnowflakePlan) -> None:
        self._plan = plan
        current_session = self._plan.session
        self.cte_optimization_enabled = current_session.cte_optimization_enabled
        self.large_query_breakdown_enabled = (
            current_session.large_query_breakdown_enabled
        )
        self.query_compilation_stage_enabled = (
            current_session._query_compilation_stage_enabled
        )

    def should_start_query_compilation(self) -> bool:
        """
        Whether optimization should be applied to the plan or not.
        Optimization can be applied if
        1) there is source logical plan attached to the current snowflake plan
        2) the query compilation stage is enabled
        3) optimizations are enabled in the current session, such as cte_optimization_enabled


        Returns
        -------
        True if optimization should be applied. Otherwise, return False.
        """

        current_session = self._plan.session
        return (
            not isinstance(current_session._conn, MockServerConnection)
            and (self._plan.source_plan is not None)
            and self.query_compilation_stage_enabled
            and (self.cte_optimization_enabled or self.large_query_breakdown_enabled)
        )

    def compile(self) -> Dict[PlanQueryType, List[Query]]:
        if self.should_start_query_compilation():
            # preparation for compilation
            # 1. make a copy of the original plan
            start_time = time.time()
            complexity_score_before_compilation = get_complexity_score(
                self._plan.cumulative_node_complexity
            )
            logical_plans: List[LogicalPlan] = [copy.deepcopy(self._plan)]
            deep_copy_end_time = time.time()

            # 2. create a code generator with the original plan
            query_generator = create_query_generator(self._plan)

            # 3. apply each optimizations if needed
            # CTE optimization
            cte_start_time = time.time()
            if self.cte_optimization_enabled:
                repeated_subquery_eliminator = RepeatedSubqueryElimination(
                    logical_plans, query_generator
                )
                logical_plans = repeated_subquery_eliminator.apply()

            cte_end_time = time.time()
            complexity_scores_after_cte = [
                get_complexity_score(logical_plan.cumulative_node_complexity)
                for logical_plan in logical_plans
            ]

            # Large query breakdown
            if self.large_query_breakdown_enabled:
                large_query_breakdown = LargeQueryBreakdown(
                    self._plan.session, query_generator, logical_plans
                )
                logical_plans = large_query_breakdown.apply()

            large_query_breakdown_end_time = time.time()
            complexity_scores_after_large_query_breakdown = [
                get_complexity_score(logical_plan.cumulative_node_complexity)
                for logical_plan in logical_plans
            ]

            # 4. do a final pass of code generation
            queries = query_generator.generate_queries(logical_plans)

            # log telemetry data
            deep_copy_time = deep_copy_end_time - start_time
            cte_time = cte_end_time - cte_start_time
            large_query_breakdown_time = large_query_breakdown_end_time - cte_end_time
            total_time = time.time() - start_time
            session = self._plan.session
            summary_value = {
                TelemetryField.CTE_OPTIMIZATION_ENABLED.value: self.cte_optimization_enabled,
                TelemetryField.LARGE_QUERY_BREAKDOWN_ENABLED.value: self.large_query_breakdown_enabled,
                CompilationStageTelemetryField.COMPLEXITY_SCORE_BOUNDS.value: (
                    COMPLEXITY_SCORE_LOWER_BOUND,
                    COMPLEXITY_SCORE_UPPER_BOUND,
                ),
                CompilationStageTelemetryField.TIME_TAKEN_FOR_COMPILATION.value: total_time,
                CompilationStageTelemetryField.TIME_TAKEN_FOR_DEEP_COPY_PLAN.value: deep_copy_time,
                CompilationStageTelemetryField.TIME_TAKEN_FOR_CTE_OPTIMIZATION.value: cte_time,
                CompilationStageTelemetryField.TIME_TAKEN_FOR_LARGE_QUERY_BREAKDOWN.value: large_query_breakdown_time,
                CompilationStageTelemetryField.COMPLEXITY_SCORE_BEFORE_COMPILATION.value: complexity_score_before_compilation,
                CompilationStageTelemetryField.COMPLEXITY_SCORE_AFTER_CTE_OPTIMIZATION.value: complexity_scores_after_cte,
                CompilationStageTelemetryField.COMPLEXITY_SCORE_AFTER_LARGE_QUERY_BREAKDOWN.value: complexity_scores_after_large_query_breakdown,
            }
            session._conn._telemetry_client.send_query_compilation_summary_telemetry(
                session_id=session.session_id,
                plan_uuid=self._plan.uuid,
                compilation_stage_summary=summary_value,
            )
            return queries
        else:
            final_plan = self._plan
            final_plan = final_plan.replace_repeated_subquery_with_cte(
                self.cte_optimization_enabled, self.query_compilation_stage_enabled
            )
            return {
                PlanQueryType.QUERIES: final_plan.queries,
                PlanQueryType.POST_ACTIONS: final_plan.post_actions,
            }
