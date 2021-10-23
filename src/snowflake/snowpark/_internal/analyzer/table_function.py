#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.plans.logical.logical_plan import LogicalPlan
from snowflake.snowpark._internal.sp_expressions import (
    TableFunctionExpression as SPTableFunctionExpression,
)


class TableFunctionRelation(LogicalPlan):
    def __init__(self, table_function: SPTableFunctionExpression):
        super().__init__()
        self.table_function = table_function


class TableFunctionJoin(LogicalPlan):
    def __init__(self, child: LogicalPlan, table_function: SPTableFunctionExpression):
        super().__init__()
        self.children = [child]
        self.table_function = table_function
