#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.snowflake_plan import LogicalPlan


class MergeExpression(Expression):
    def __init__(self, condition: Optional[Expression]) -> None:
        super().__init__()
        self.condition = condition


class UpdateMergeExpression(MergeExpression):
    def __init__(
        self, condition: Optional[Expression], assignments: Dict[Expression, Expression]
    ) -> None:
        super().__init__(condition)
        self.assignments = assignments


class DeleteMergeExpression(MergeExpression):
    pass


class InsertMergeExpression(MergeExpression):
    def __init__(
        self,
        condition: Optional[Expression],
        keys: List[Expression],
        values: List[Expression],
    ) -> None:
        super().__init__(condition)
        self.keys = keys
        self.values = values


class TableUpdate(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        assignments: Dict[Expression, Expression],
        condition: Optional[Expression],
        source_data: Optional[LogicalPlan],
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.assignments = assignments
        self.condition = condition
        self.source_data = source_data
        self.children = [source_data] if source_data else []


class TableDelete(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        condition: Optional[Expression],
        source_data: Optional[LogicalPlan],
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.condition = condition
        self.source_data = source_data
        self.children = [source_data] if source_data else []


class TableMerge(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        source: LogicalPlan,
        join_expr: Expression,
        clauses: List[Expression],
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.source = source
        self.join_expr = join_expr
        self.clauses = clauses
        self.children = [source] if source else []
