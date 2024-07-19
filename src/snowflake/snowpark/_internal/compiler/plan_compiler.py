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
    """This class is common point of entry from compiling SnowflakePlan to list of queries and post actions.
    We run pre-check and pre-process steps for optimization steps, and apply the activated optimizations.
    """

    def __init__(self, plan: SnowflakePlan) -> None:
        self.plan = plan

    def compile(self) -> Dict[PlanQueryType, List[Query]]:
        # apply optimizations
        final_plan = self.replace_repeated_subquery_with_cte()
        return {
            PlanQueryType.QUERIES: final_plan.queries,
            PlanQueryType.POST_ACTIONS: final_plan.post_actions,
        }
