from typing import Any

from snowflake.snowpark._internal.plans.logical.logical_plan import LogicalPlan
from snowflake.snowpark._internal.sp_expressions import Expression as SPExpression

class Limit(LogicalPlan):
    limit_expr: Any
    child: Any
    children: Any
    def __init__(self, limit_expr: SPExpression, child: LogicalPlan) -> None: ...
