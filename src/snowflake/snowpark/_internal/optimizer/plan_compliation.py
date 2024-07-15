#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import copy
from typing import Dict, List

from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.optimizer.query_generator import QueryGenerator


class PlanCompiler:
    # original snowflake plan
    _plan: SnowflakePlan

    def __init__(self, plan: SnowflakePlan) -> None:
        self._plan = plan

    def _optimization_enabled(self):
        return self._plan.session.cte_optimization_enabled

    def _get_plan_queries(self, plan: SnowflakePlan) -> Dict[str, List[Query]]:
        return {
            "queries": plan.queries,
            "post_actions": plan.post_actions,
        }

    def compile(self) -> Dict[str, List[Query]]:
        from snowflake.snowpark._internal.optimizer.common_subdataframe_elimination import (
            CommonSubDataframeElimination,
        )

        if (self._plan.source_plan is None) or (not self._optimization_enabled()):
            return self._get_plan_queries(self._plan)

        query_generator = QueryGenerator(self._plan.session)
        query_generator.table_create_child_attribute_map.update(
            self._plan.table_create_child_attribute_map
        )
        copied_plan = copy.deepcopy(self._plan.source_plan)

        # Optimization stage
        # get the logical plan context
        # logical_plan_context = LogicalPlanContext()
        sub_dataframe_eliminator = CommonSubDataframeElimination(
            copied_plan, query_generator
        )
        sub_dataframe_elimination_result = (
            sub_dataframe_eliminator.common_subdataframe_elimination()
        )

        # Query generation stage
        final_plan = query_generator.resolve(sub_dataframe_elimination_result[0])
        return self._get_plan_queries(final_plan)
