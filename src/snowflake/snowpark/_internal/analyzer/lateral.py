#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.plans.logical.logical_plan import LogicalPlan
from snowflake.snowpark._internal.sp_expressions import (
    TableFunctionExpression as SPTableFunctionExpression,
)


class Lateral(LogicalPlan):
    def __init__(self, child: LogicalPlan, table_function: SPTableFunctionExpression):
        super().__init__()
        self.children = [child]
        self.table_function = table_function
