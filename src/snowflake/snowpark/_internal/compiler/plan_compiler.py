#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
from typing import Dict, List

from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    PlanQueryType,
    Query,
    SnowflakePlan,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.compiler.large_query_breakdown import (
    LargeQueryBreakdown,
)
from snowflake.snowpark._internal.compiler.utils import create_query_generator
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

    def should_apply_optimizations(self) -> bool:
        """
        Whether optimization should be applied to the plan or not.
        Optimization can be applied if
        1) there is source logical plan attached to the current snowflake plan
        2) optimizations are enabled in the current session, such as cte_optimization_enabled


        Returns
        -------
        True if optimization should be applied. Otherwise, return False.
        """

        current_session = self._plan.session
        return (
            (self._plan.source_plan is not None)
            and (
                current_session.cte_optimization_enabled
                or current_session.large_query_breakdown_enabled
            )
            and not isinstance(current_session, MockServerConnection)
        )

    def should_apply_cte_optimization(self) -> bool:
        session = self._plan.session
        return session.cte_optimization_enabled

    def should_apply_large_query_breakdown(self) -> bool:
        session = self._plan.session
        return session.large_query_breakdown_enabled

    def compile(self) -> Dict[PlanQueryType, List[Query]]:
        final_plan = self._plan
        if self.should_apply_optimizations():
            plans: List[LogicalPlan] = [copy.deepcopy(final_plan)]
            # apply optimizations
            query_generator = create_query_generator(final_plan)

            if self.should_apply_cte_optimization():
                plans = [plan.replace_repeated_subquery_with_cte() for plan in plans]
            if self.should_apply_large_query_breakdown():
                large_query_breakdown = LargeQueryBreakdown(
                    final_plan.session, query_generator, plans
                )
                plans = large_query_breakdown.apply()

            return query_generator.generate_queries(plans)

        return {
            PlanQueryType.QUERIES: final_plan.queries,
            PlanQueryType.POST_ACTIONS: final_plan.post_actions,
        }
