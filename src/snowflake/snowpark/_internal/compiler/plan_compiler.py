#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import logging
from typing import Any, Dict, List

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
    LargeQueryBreakdown,
)
from snowflake.snowpark._internal.compiler.repeated_subquery_elimination import (
    RepeatedSubqueryElimination,
)
from snowflake.snowpark._internal.compiler.telemetry_constants import (
    CompilationStageTelemetryField,
)
from snowflake.snowpark._internal.compiler.utils import (
    create_query_generator,
    plot_plan_if_enabled,
)
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark._internal.utils import measure_time, random_name_for_temp_object
from snowflake.snowpark.mock._connection import MockServerConnection

_logger = logging.getLogger(__name__)


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
            and current_session._query_compilation_stage_enabled
            and (
                current_session.cte_optimization_enabled
                or current_session.large_query_breakdown_enabled
            )
        )

    def compile(self) -> Dict[PlanQueryType, List[Query]]:
        # initialize the queries with the original queries without optimization
        final_plan = self._plan
        queries = {
            PlanQueryType.QUERIES: final_plan.queries,
            PlanQueryType.POST_ACTIONS: final_plan.post_actions,
        }

        if self.should_start_query_compilation():
            session = self._plan.session
            try:
                with measure_time() as total_time:
                    # preparation for compilation
                    # 1. make a copy of the original plan
                    with measure_time() as deep_copy_time:
                        complexity_score_before_compilation = get_complexity_score(
                            self._plan
                        )
                        logical_plans: List[LogicalPlan] = [copy.deepcopy(self._plan)]
                        plot_plan_if_enabled(self._plan, "original_plan")
                        plot_plan_if_enabled(logical_plans[0], "deep_copied_plan")

                    # 2. create a code generator with the original plan
                    query_generator = create_query_generator(self._plan)

                    extra_optimization_status: Dict[str, Any] = {}
                    # 3. apply each optimizations if needed
                    # CTE optimization
                    with measure_time() as cte_time:
                        if session.cte_optimization_enabled:
                            repeated_subquery_eliminator = RepeatedSubqueryElimination(
                                logical_plans, query_generator
                            )
                            elimination_result = repeated_subquery_eliminator.apply()
                            logical_plans = elimination_result.logical_plans
                            # add the extra repeated subquery elimination status
                            extra_optimization_status[
                                CompilationStageTelemetryField.CTE_NODE_CREATED.value
                            ] = elimination_result.total_num_of_ctes
                    complexity_scores_after_cte = [
                        get_complexity_score(logical_plan)
                        for logical_plan in logical_plans
                    ]
                    for i, plan in enumerate(logical_plans):
                        plot_plan_if_enabled(plan, f"cte_optimized_plan_{i}")

                    # Large query breakdown
                    breakdown_summary, skipped_summary = {}, {}
                    with measure_time() as large_query_breakdown_time:
                        if session.large_query_breakdown_enabled:
                            large_query_breakdown = LargeQueryBreakdown(
                                session,
                                query_generator,
                                logical_plans,
                                session.large_query_breakdown_complexity_bounds,
                            )
                            breakdown_result = large_query_breakdown.apply()
                            logical_plans = breakdown_result.logical_plans
                            breakdown_summary = breakdown_result.breakdown_summary
                            skipped_summary = breakdown_result.skipped_summary

                    complexity_scores_after_large_query_breakdown = [
                        get_complexity_score(logical_plan)
                        for logical_plan in logical_plans
                    ]
                    for i, plan in enumerate(logical_plans):
                        plot_plan_if_enabled(plan, f"large_query_breakdown_plan_{i}")

                    # 4. do a final pass of code generation
                    queries = query_generator.generate_queries(logical_plans)

                # log telemetry data
                summary_value = {
                    TelemetryField.CTE_OPTIMIZATION_ENABLED.value: session.cte_optimization_enabled,
                    TelemetryField.LARGE_QUERY_BREAKDOWN_ENABLED.value: session.large_query_breakdown_enabled,
                    CompilationStageTelemetryField.COMPLEXITY_SCORE_BOUNDS.value: session.large_query_breakdown_complexity_bounds,
                    CompilationStageTelemetryField.TIME_TAKEN_FOR_COMPILATION.value: total_time(),
                    CompilationStageTelemetryField.TIME_TAKEN_FOR_DEEP_COPY_PLAN.value: deep_copy_time(),
                    CompilationStageTelemetryField.TIME_TAKEN_FOR_CTE_OPTIMIZATION.value: cte_time(),
                    CompilationStageTelemetryField.TIME_TAKEN_FOR_LARGE_QUERY_BREAKDOWN.value: large_query_breakdown_time(),
                    CompilationStageTelemetryField.COMPLEXITY_SCORE_BEFORE_COMPILATION.value: complexity_score_before_compilation,
                    CompilationStageTelemetryField.COMPLEXITY_SCORE_AFTER_CTE_OPTIMIZATION.value: complexity_scores_after_cte,
                    CompilationStageTelemetryField.COMPLEXITY_SCORE_AFTER_LARGE_QUERY_BREAKDOWN.value: complexity_scores_after_large_query_breakdown,
                    CompilationStageTelemetryField.BREAKDOWN_SUMMARY.value: breakdown_summary,
                    CompilationStageTelemetryField.LARGE_QUERY_BREAKDOWN_OPTIMIZATION_SKIPPED.value: skipped_summary,
                }
                # add the extra optimization status
                summary_value.update(extra_optimization_status)
                session._conn._telemetry_client.send_query_compilation_summary_telemetry(
                    session_id=session.session_id,
                    plan_uuid=self._plan.uuid,
                    compilation_stage_summary=summary_value,
                )
            except Exception as e:
                # if any error occurs during the compilation, we should fall back to the original plan
                _logger.debug(f"Skipping optimization due to error: {e}")
                session._conn._telemetry_client.send_query_compilation_stage_failed_telemetry(
                    session_id=session.session_id,
                    plan_uuid=self._plan.uuid,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )

        return self.replace_temp_obj_placeholders(queries)

    def replace_temp_obj_placeholders(
        self, queries: Dict[PlanQueryType, List[Query]]
    ) -> Dict[PlanQueryType, List[Query]]:
        """
        When thread-safe session is enabled, we use temporary object name placeholders instead of a temporary name
        when generating snowflake plan. We replace the temporary object name placeholders with actual temporary object
        names here. This is done to prevent the following scenario:

        1. A dataframe is created and resolved in main thread.
        2. The resolve plan contains queries that create and drop temp objects.
        3. If the plan with same temp object names is executed my multiple threads, the temp object names will conflict.
           One thread can drop the object before another thread finished using it.

        To prevent this, we generate queries with temp object name placeholders and replace them with actual temp object
        here.
        """
        session = self._plan.session
        if not session._conn._thread_safe_session_enabled:
            return queries
        # This dictionary will store the mapping between placeholder name and actual temp object name.
        placeholders = {}
        # Final execution queries
        execution_queries = {}
        for query_type, query_list in queries.items():
            execution_queries[query_type] = []
            for query in query_list:
                # If the query contains a temp object name placeholder, we generate a random
                # name for the temp object and add it to the placeholders dictionary.
                if query.temp_obj_name_placeholder:
                    (
                        placeholder_name,
                        temp_obj_type,
                    ) = query.temp_obj_name_placeholder
                    placeholders[placeholder_name] = random_name_for_temp_object(
                        temp_obj_type
                    )

                copied_query = copy.copy(query)
                for placeholder_name, target_temp_name in placeholders.items():
                    # Copy the original query and replace all the placeholder names with the
                    # actual temp object names.
                    copied_query.sql = copied_query.sql.replace(
                        placeholder_name, target_temp_name
                    )

                execution_queries[query_type].append(copied_query)
        return execution_queries
