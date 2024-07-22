#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator


def create_query_generator(plan: SnowflakePlan) -> QueryGenerator:
    table_create_child_attribute_map: Dict[str, List[Attribute]] = {}
    if plan.source_plan is not None and isinstance(
        plan.source_plan, SnowflakeCreateTable
    ):
        create_table_node = plan.source_plan
        # resolve the node child to get the child attribute.
        resolved_child = plan.session._analyzer.resolve(create_table_node.query)
        full_table_name = ".".join(create_table_node.table_name)
        table_create_child_attribute_map[full_table_name] = resolved_child.attributes

    return QueryGenerator(plan.session, table_create_child_attribute_map)
