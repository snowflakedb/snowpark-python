#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import DefaultDict, Dict, List

from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    SnowflakePlan,
    SnowflakePlanBuilder,
    PlanQueryType,
    Query
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeCreateTable,
)
from snowflake.snowpark.session import Session


class QueryGenerator(Analyzer):
    """
    Query Generation class that is used re-build the sql query for given logical plans
    during the compilation stage.

    Note that this query generator only rebuild the sql query, and do not rebuild the
    """
    def __init__(
        self,
        session: Session,
        table_create_child_attribute_map: Dict[str, List[Attribute]]
    ) -> None:
        super().__init__(session)
        # overwrite the plan_builder initiated in the super to skip the building of schema query
        self.plan_builder = SnowflakePlanBuilder(self.session, skip_schema_query=True)
        self.table_create_child_attribute_map: Dict[str, List[Attribute]] = table_create_child_attribute_map

    def generate_queries(self, logical_plans: List[LogicalPlan]) -> Dict[PlanQueryType, List[Query]]:
        """
        Parameters
        ----------
        logical_plans

        Returns
        -------

        """
        snowflake_plans = [self.resolve(logical_plan) for logical_plan in logical_plans]
        # merge all results
        queries = []
        post_actions = []
        for snowflake_plan in snowflake_plans:
            queries.extend(snowflake_plan.queries)
            post_actions.extend(snowflake_plan.post_actions)

        return {
            PlanQueryType.QUERIES: queries,
            PlanQueryType.POST_ACTIONS: post_actions
        }

    def do_resolve_with_resolved_children(
        self,
        logical_plan: LogicalPlan,
        resolved_children: Dict[LogicalPlan, SnowflakePlan],
        df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
    ) -> SnowflakePlan:
        if isinstance(logical_plan, SnowflakePlan):
            res = self.do_resolve_with_resolved_children(
                logical_plan.source_plan,
                resolved_children,
                df_aliased_col_name_to_real_col_name,
            )
            resolved_children[logical_plan] = res
            return res

        if isinstance(logical_plan, SnowflakeCreateTable):
            child_plan = resolved_children[logical_plan.children[0]]
            full_table_name = ".".join(logical_plan.table_name)
            return self.plan_builder.save_as_table(
                logical_plan.table_name,
                logical_plan.column_names,
                logical_plan.mode,
                logical_plan.table_type,
                [
                    self.analyze(x, df_aliased_col_name_to_real_col_name)
                    for x in logical_plan.clustering_exprs
                ],
                logical_plan.comment,
                child_plan,
                logical_plan,
                self.table_create_child_attribute_map[full_table_name],
            )

        return super().do_resolve_with_resolved_children(
            logical_plan, resolved_children, df_aliased_col_name_to_real_col_name
        )
