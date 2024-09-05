#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import copy
from typing import DefaultDict, Dict, Iterable, List, NamedTuple, Optional

from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.select_statement import Selectable
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    CreateViewCommand,
    PlanQueryType,
    Query,
    SnowflakePlan,
    SnowflakePlanBuilder,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoLocationNode,
    LogicalPlan,
    SnowflakeCreateTable,
    TableCreationSource,
    WithQueryBlock,
)
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    TableDelete,
    TableMerge,
    TableUpdate,
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
        # Records the definition of all the with query blocks encountered during the code generation.
        # This information will be used to generate the final query of a SnowflakePlan with the
        # correct CTE definition.
        # NOTE: the dict used here is an ordered dict, all with query block definition is recorded in the
        # order of when the with query block is visited. The order is important to make sure the dependency
        # between the CTE definition is satisfied.
        self.resolved_with_query_block: Dict[str, Query] = {}

    def generate_queries(
        self, logical_plans: List[LogicalPlan]
    ) -> Dict[PlanQueryType, List[Query]]:
        """
        Generate final queries for the given set of logical plans.

        Returns
        -------

        """
        from snowflake.snowpark._internal.compiler.utils import (
            get_snowflake_plan_queries,
        )

        # generate queries for each logical plan
        snowflake_plans = [self.resolve(logical_plan) for logical_plan in logical_plans]
        # merge all results into final set of queries
        queries = []
        post_actions = []
        for snowflake_plan in snowflake_plans:
            plan_queries = get_snowflake_plan_queries(
                snowflake_plan, self.resolved_with_query_block
            )
            # we deduplicate the queries and post actions generated across the logical
            # plans because it is possible for large query breakdown to partition
            # original plan into multiple plans that may contain the same nodes which
            # generate the same queries and post actions.
            for query in plan_queries[PlanQueryType.QUERIES]:
                if query not in queries:
                    queries.append(query)
            for action in plan_queries[PlanQueryType.POST_ACTIONS]:
                if action not in post_actions:
                    post_actions.append(action)

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
                assert logical_plan.source_plan is not None
                # when encounter a SnowflakePlan with no queries, try to re-resolve
                # the source plan to construct the result
                res = self.do_resolve(logical_plan.source_plan)
                resolved_children[logical_plan] = res
                resolved_plan = res
            else:
                resolved_plan = logical_plan

        elif isinstance(logical_plan, SnowflakeCreateTable):
            from snowflake.snowpark._internal.compiler.utils import (
                get_snowflake_plan_queries,
            )

            # overwrite the SnowflakeCreateTable resolving, because the child
            # attribute will be pulled directly from the cache
            resolved_child = resolved_children[logical_plan.children[0]]

            # when creating a table during query compilation stage, if the
            # table being created is the same as the one that is cached, we
            # pull the child attributes directly from the cache. Otherwise, we
            # use the child attributes as None. This will be for the case when
            # table creation source is temp table from large query breakdown.
            child_attributes = None
            if (
                logical_plan.creation_source
                != TableCreationSource.LARGE_QUERY_BREAKDOWN
            ):
                assert self._snowflake_create_table_plan_info is not None
                assert (
                    self._snowflake_create_table_plan_info.table_name
                    == logical_plan.table_name
                )
                child_attributes = (
                    self._snowflake_create_table_plan_info.child_attributes
                )

            # update the resolved child
            copied_resolved_child = copy.copy(resolved_child)
            final_queries = get_snowflake_plan_queries(
                copied_resolved_child, self.resolved_with_query_block
            )
            copied_resolved_child.queries = final_queries[PlanQueryType.QUERIES]
            resolved_plan = self.plan_builder.save_as_table(
                table_name=logical_plan.table_name,
                column_names=logical_plan.column_names,
                mode=logical_plan.mode,
                table_type=logical_plan.table_type,
                clustering_keys=[
                    self.analyze(x, df_aliased_col_name_to_real_col_name)
                    for x in logical_plan.clustering_exprs
                ],
                comment=logical_plan.comment,
                enable_schema_evolution=logical_plan.enable_schema_evolution,
                data_retention_time=logical_plan.data_retention_time,
                max_data_extension_time=logical_plan.max_data_extension_time,
                change_tracking=logical_plan.change_tracking,
                copy_grants=logical_plan.copy_grants,
                child=copied_resolved_child,
                source_plan=logical_plan,
                use_scoped_temp_objects=self.session._use_scoped_temp_objects,
                creation_source=logical_plan.creation_source,
                child_attributes=child_attributes,
                iceberg_config=logical_plan.iceberg_config,
            )

        elif isinstance(
            logical_plan,
            (
                CreateViewCommand,
                TableUpdate,
                TableDelete,
                TableMerge,
                CopyIntoLocationNode,
            ),
        ):
            from snowflake.snowpark._internal.compiler.utils import (
                get_snowflake_plan_queries,
            )

            # for CreateViewCommand, TableUpdate, TableDelete, TableMerge and CopyIntoLocationNode,
            # the with definition must be generated before create, update, delete, merge and copy into
            # query.
            resolved_child = resolved_children[logical_plan.children[0]]
            copied_resolved_child = copy.copy(resolved_child)
            final_queries = get_snowflake_plan_queries(
                copied_resolved_child, self.resolved_with_query_block
            )
            copied_resolved_child.queries = final_queries[PlanQueryType.QUERIES]
            resolved_children[logical_plan.children[0]] = copied_resolved_child
            resolved_plan = super().do_resolve_with_resolved_children(
                logical_plan, resolved_children, df_aliased_col_name_to_real_col_name
            )

        elif isinstance(logical_plan, Selectable):
            # overwrite the Selectable resolving to make sure we are triggering
            # any schema query build
            resolved_plan = logical_plan.get_snowflake_plan(skip_schema_query=True)

        elif isinstance(logical_plan, WithQueryBlock):
            resolved_child = resolved_children[logical_plan.children[0]]
            # record the CTE definition of the current block
            if logical_plan.name not in self.resolved_with_query_block:
                self.resolved_with_query_block[
                    logical_plan.name
                ] = resolved_child.queries[-1]

            resolved_plan = self.plan_builder.with_query_block(
                logical_plan.name,
                resolved_child,
                logical_plan,
            )

        else:
            resolved_plan = super().do_resolve_with_resolved_children(
                logical_plan, resolved_children, df_aliased_col_name_to_real_col_name
            )

        resolved_plan._is_valid_for_replacement = True

        return resolved_plan
