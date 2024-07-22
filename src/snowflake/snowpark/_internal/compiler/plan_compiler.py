#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List

from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    PlanQueryType,
    Query,
    SnowflakePlan,
)


class PlanCompiler:
    """
    This class is common point of entry from compiling SnowflakePlan to list of queries and post actions.
    We run pre-check and pre-process steps for optimization steps, and apply the activated optimizations.
    """

    def __init__(self, plan: SnowflakePlan) -> None:
        self._plan = plan

    def can_apply_optimizations(self) -> None:
        current_session = self._plan.session
        return (
            self._plan.source_plan is not None
        ) and current_session.cte_optimization_enabled

    def compile(self) -> Dict[PlanQueryType, List[Query]]:
        final_plan = self._plan
        if self.can_apply_optimizations():
            # apply optimizations
            final_plan = final_plan.replace_repeated_subquery_with_cte()

        return {
            PlanQueryType.QUERIES: final_plan.queries,
            PlanQueryType.POST_ACTIONS: final_plan.post_actions,
        }
