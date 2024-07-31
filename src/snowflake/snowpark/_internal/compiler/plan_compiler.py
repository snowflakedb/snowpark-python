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
from snowflake.snowpark._internal.compiler.common_subdataframe_elimination import (
    CommonSubDataframeElimination,
)
from snowflake.snowpark._internal.compiler.utils import (
    create_query_generator,
    get_snowflake_plan_queries,
)
from snowflake.snowpark.context import _enable_new_compilation_stage


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
            self._plan.source_plan is not None
        ) and current_session.cte_optimization_enabled

    def compile(self) -> Dict[PlanQueryType, List[Query]]:
        # final_plan = self._plan
        if self.should_apply_optimizations():
            # preparation for compilation
            # 1. make a copy of the original plan
            logical_plans = [copy.deepcopy(self._plan)]
            # 2. create a code generator with the original plan
            query_generator = create_query_generator(self._plan)

            if _enable_new_compilation_stage:
                if self._plan.session.cte_optimization_enabled:
                    common_sub_dataframe_eliminator = CommonSubDataframeElimination(
                        logical_plans, query_generator
                    )
                    logical_plans = common_sub_dataframe_eliminator.apply()

            # do a final pass of code generation
            return query_generator.generate_queries(logical_plans)

        else:
            final_plan = self._plan
            # apply optimizations
            if self._plan.session.cte_optimization_enabled:
                final_plan = final_plan.replace_repeated_subquery_with_cte()

            return get_snowflake_plan_queries(final_plan, {})
