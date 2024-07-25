#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import DefaultDict, Dict, Iterable, List, NamedTuple, Optional

from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.select_statement import Selectable
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    PlanQueryType,
    Query,
    SnowflakePlan,
    SnowflakePlanBuilder,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeCreateTable,
)
from snowflake.snowpark.session import Session


class SnowflakeCreateTablePlanInfo(NamedTuple):
    """
    Cached information that can be used resolve the plan for SnowflakeCreateTable.
    """

    table_name: Iterable[str]
    child_attributes: List[Attribute]


class QueryGenerator(Analyzer):
    """
    Query Generation class that is used re-build the sql query for given logical plans
    during the compilation stage.

    Note that this query generator only rebuild the sql query, not the schema queries.
    """

    def __init__(
        self,
        session: Session,
        snowflake_create_table_plan_info: Optional[SnowflakeCreateTablePlanInfo] = None,
    ) -> None:
        super().__init__(session)
        # overwrite the plan_builder initiated in the super to skip the building of schema query
        self.plan_builder = SnowflakePlanBuilder(self.session, skip_schema_query=True)
        # cached information that can be used resolve the SnowflakeCreateTable node
        self._snowflake_create_table_plan_info: Optional[
            SnowflakeCreateTablePlanInfo
        ] = snowflake_create_table_plan_info

    def generate_queries(
        self, logical_plans: List[LogicalPlan]
    ) -> Dict[PlanQueryType, List[Query]]:
        """
        Generate final queries for the given set of logical plans.

        Returns
        -------

        """
        # generate queries for each logical plan
        snowflake_plans = [self.resolve(logical_plan) for logical_plan in logical_plans]
        # merge all results into final set of queries
        queries = []
        post_actions = []
        for snowflake_plan in snowflake_plans:
            queries.extend(snowflake_plan.queries)
            post_actions.extend(snowflake_plan.post_actions)

        return {
            PlanQueryType.QUERIES: queries,
            PlanQueryType.POST_ACTIONS: post_actions,
        }

    def do_resolve_with_resolved_children(
        self,
        logical_plan: LogicalPlan,
        resolved_children: Dict[LogicalPlan, SnowflakePlan],
        df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
    ) -> SnowflakePlan:
        if isinstance(logical_plan, SnowflakePlan):
            if logical_plan.queries is None:
                # when encounter a SnowflakePlan with no queries, try to re-resolve
                # the source plan to construct the result
                res = self.resolve(logical_plan.source_plan)
                resolved_children[logical_plan] = res
                return res
            else:
                return logical_plan

        if isinstance(logical_plan, SnowflakeCreateTable):
            # overwrite the SnowflakeCreateTable resolving, because the child
            # attribute will be pulled directly from the cache
            resolved_child = resolved_children[logical_plan.children[0]]
            # when the plan is for SnowflakeCreateTable, there must be a snowflake_create_table_plan_info
            # associated with the current query generator.
            # TODO: this check need to be relaxed when large query breakdown with temp table
            #       is implemented, because the child attributes are not necessary to create
            #       the temp table
            assert self._snowflake_create_table_plan_info is not None
            assert (
                self._snowflake_create_table_plan_info.table_name
                == logical_plan.table_name
            )

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
                resolved_child,
                logical_plan,
                self.session._use_scoped_temp_objects,
                logical_plan.is_generated,
                self._snowflake_create_table_plan_info.child_attributes,
            )

        if isinstance(logical_plan, Selectable):
            # overwrite the Selectable resolving to make sure we are triggering
            # any schema query build
            return logical_plan.get_snowflake_plan(skip_schema_query=True)

        return super().do_resolve_with_resolved_children(
            logical_plan, resolved_children, df_aliased_col_name_to_real_col_name
        )
