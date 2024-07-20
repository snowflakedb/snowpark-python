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
)
from snowflake.snowpark.session import Session


class QueryGenerator(Analyzer):
    def __init__(
        self,
        session: Session,
    ) -> None:
        self.session = session
        self.plan_builder = SnowflakePlanBuilder(self.session, skip_schema_query=True)
        self.generated_alias_maps = {}
        self.subquery_plans = []
        self.alias_maps_to_use: Optional[Dict[uuid.UUID, str]] = None
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

        return super().do_resolve_with_resolved_children(
            logical_plan, resolved_children, df_aliased_col_name_to_real_col_name
        )
