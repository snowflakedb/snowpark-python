#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Optional

from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.compiler.query_generator import (
    QueryGenerator,
    SnowflakeCreateTablePlanInfo,
)


def create_query_generator(plan: SnowflakePlan) -> QueryGenerator:
    """
    Helper function to construct the query generator for a given valid SnowflakePlan.
    """
    snowflake_create_table_plan_info: Optional[SnowflakeCreateTablePlanInfo] = None
    # When the root node of source plan is SnowflakeCreateTable, we need to extract the
    # child attributes for the table that is used for later query generation process.
    # This relies on the fact that SnowflakeCreateTable is an eager evaluation, and
    # SnowflakeCreateTable is always the root node of the logical plan.
    if plan.source_plan is not None and isinstance(
        plan.source_plan, SnowflakeCreateTable
    ):
        create_table_node = plan.source_plan
        # resolve the node child to get the child attribute that is needed for later code
        # generation. Typically, the query attached to the create_table_node is already a
        # resolved plan, and the resolve will be a no-op.
        # NOTE that here we rely on the fact that the SnowflakeCreateTable node is the root
        # of a source plan. Test will fail if that assumption is broken.
        resolved_child = plan.session._analyzer.resolve(create_table_node.query)
        snowflake_create_table_plan_info = SnowflakeCreateTablePlanInfo(
            create_table_node.table_name, resolved_child.attributes
        )

    return QueryGenerator(plan.session, snowflake_create_table_plan_info)
