#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import uuid
from typing import DefaultDict, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    SnowflakePlan,
    SnowflakePlanBuilder,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeCreateTable,
    WithObjectRef,
    WithQueryBlock,
)
from snowflake.snowpark.session import Session


class QueryGenerator(Analyzer):
    def __init__(
        self,
        session: Session,
        # df_aliased_col_name_to_real_col_name: Dict[str, Dict[str, str]],
    ) -> None:
        self.session = session
        self.plan_builder = SnowflakePlanBuilder(self.session, skip_schema_query=True)
        # re-propogate
        self.generated_alias_maps = {}
        # subquery plans nees to be repropogated
        self.subquery_plans = []
        self.alias_maps_to_use: Optional[Dict[uuid.UUID, str]] = None
        # self.df_aliased_col_name_to_real_col_name = df_aliased_col_name_to_real_col_name
        self.table_create_child_attribute_map: Dict[str, List[Attribute]] = {}

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
                self.table_create_child_attribute_map[full_table_name],
                logical_plan,
            )

        if isinstance(logical_plan, WithObjectRef):
            return self.plan_builder.with_object_ref(
                logical_plan.children[0].name,
                resolved_children[logical_plan.children[0]],
                logical_plan,
            )

        if isinstance(logical_plan, WithQueryBlock):
            return self.plan_builder.with_query_block(
                logical_plan.name,
                resolved_children[logical_plan.children[0]],
                logical_plan,
            )

        return super().do_resolve_with_resolved_children(
            logical_plan, resolved_children, df_aliased_col_name_to_real_col_name
        )
