#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property
from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    LogicalPlan,
    SnowflakePlan,
)


class MergeExpression(Expression):
    def __init__(self, condition: Optional[Expression]) -> None:
        super().__init__()
        self.condition = condition

    @cached_property
    def expression_complexity(self) -> int:
        # WHEN MATCHED [AND condition] THEN DEL
        estimate = 4
        estimate += self.condition.expression_complexity if self.condition else 0
        return estimate


class UpdateMergeExpression(MergeExpression):
    def __init__(
        self, condition: Optional[Expression], assignments: Dict[Expression, Expression]
    ) -> None:
        super().__init__(condition)
        self.assignments = assignments

    @cached_property
    def expression_complexity(self) -> int:
        # WHEN MATCHED [AND condition] THEN UPDATE SET COMMA.join(k=v for k,v in assignments)
        estimate = 4
        estimate += 1 if self.condition else 0
        estimate += sum(
            key_expr.expression_complexity + val_expr.expression_complexity
            for key_expr, val_expr in self.assignments.items()
        )
        return estimate


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

    @cached_property
    def expression_complexity(self) -> int:
        # WHEN NOT MATCHED [AND cond] THEN INSERT [(COMMA.join(key))] VALUES (COMMA.join(values))
        estimate = 5
        estimate += sum(expr.expression_complexity for expr in self.keys)
        estimate += sum(expr.expression_complexity for expr in self.values)
        estimate += self.condition.expression_complexity if self.condition else 0
        return estimate


class TableUpdate(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        assignments: Dict[Expression, Expression],
        condition: Optional[Expression],
        source_data: Optional[SnowflakePlan],
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.assignments = assignments
        self.condition = condition
        self.source_data = source_data
        self.children = [source_data] if source_data else []

    @cached_property
    def individual_query_complexity(self) -> int:
        # UPDATE table_name SET COMMA.join(k, v in assignments) [source_data] [WHERE condition]
        estimate = 2
        estimate += sum(
            key_expr.expression_complexity + val_expr.expression_complexity
            for key_expr, val_expr in self.assignments.items()
        )
        estimate += self.condition.expression_complexity if self.condition else 0
        # note that source data will be handled by subtree aggregator since it is added as a child
        return estimate


class TableDelete(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        condition: Optional[Expression],
        source_data: Optional[SnowflakePlan],
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.condition = condition
        self.source_data = source_data
        self.children = [source_data] if source_data else []

    @cached_property
    def individual_query_complexity(self) -> int:
        # DELETE FROM table_name [USING source_data] [WHERE condition]
        estimate = 2
        estimate += self.condition.expression_complexity if self.condition else 0
        return estimate


class TableMerge(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        source: SnowflakePlan,
        join_expr: Expression,
        clauses: List[Expression],
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.source = source
        self.join_expr = join_expr
        self.clauses = clauses
        self.children = [source] if source else []

    @cached_property
    def individual_query_complexity(self) -> int:
        # MERGE INTO table_name USING (source) ON join_expr clauses
        return (
            4
            + self.join_expr.expression_complexity
            + sum(expr.expression_complexity for expr in self.clauses)
        )
