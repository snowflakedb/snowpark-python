from typing import Any

from snowflake.snowpark._internal.plans.logical.logical_plan import LogicalPlan
from snowflake.snowpark._internal.sp_expressions import (
    TableFunctionExpression as SPTableFunctionExpression,
)

class TableFunctionRelation(LogicalPlan):
    table_function: Any
    def __init__(self, table_function: SPTableFunctionExpression) -> None: ...

class TableFunctionJoin(LogicalPlan):
    children: Any
    table_function: Any
    def __init__(
        self, child: LogicalPlan, table_function: SPTableFunctionExpression
    ) -> None: ...
