#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import Counter
from functools import cached_property
from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.complexity_stat import ComplexityStat
from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    LogicalPlan,
    SnowflakePlan,
)


class MergeExpression(Expression):
    def __init__(self, condition: Optional[Expression]) -> None:
        super().__init__()
        self.condition = condition

    @property
    def individual_complexity_stat(self) -> Dict[str, int]:
        return Counter({ComplexityStat.LOW_IMPACT.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Dict[str, int]:
        # WHEN MATCHED [AND condition] THEN DEL
        estimate = self.individual_complexity_stat
        estimate += (
            self.condition.cumulative_complexity_stat if self.condition else Counter()
        )
        return estimate


class UpdateMergeExpression(MergeExpression):
    def __init__(
        self, condition: Optional[Expression], assignments: Dict[Expression, Expression]
    ) -> None:
        super().__init__(condition)
        self.assignments = assignments

    @cached_property
    def cumulative_complexity_stat(self) -> Dict[str, int]:
        # WHEN MATCHED [AND condition] THEN UPDATE SET COMMA.join(k=v for k,v in assignments)
        estimate = self.individual_complexity_stat
        estimate += (
            self.condition.cumulative_complexity_stat if self.condition else Counter()
        )
        estimate += sum(
            (
                key_expr.cumulative_complexity_stat
                + val_expr.cumulative_complexity_stat
                for key_expr, val_expr in self.assignments.items()
            ),
            Counter(),
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
    def cumulative_complexity_stat(self) -> Dict[str, int]:
        # WHEN NOT MATCHED [AND cond] THEN INSERT [(COMMA.join(key))] VALUES (COMMA.join(values))
        estimate = self.individual_complexity_stat
        estimate += (
            self.condition.cumulative_complexity_stat if self.condition else Counter()
        )
        estimate += sum(
            (key.cumulative_complexity_stat for key in self.keys), Counter()
        )
        estimate += sum(
            (val.cumulative_complexity_stat for val in self.values), Counter()
        )
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

    @property
    def individual_complexity_stat(self) -> Dict[str, int]:
        # UPDATE table_name SET COMMA.join(k, v in assignments) [source_data] [WHERE condition]
        estimate = sum(
            (
                k.cumulative_complexity_stat + v.cumulative_complexity_stat
                for k, v in self.assignments.items()
            ),
            Counter(),
        )
        estimate += (
            self.condition.cumulative_complexity_stat if self.condition else Counter()
        )
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

    @property
    def individual_complexity_stat(self) -> Dict[str, int]:
        # DELETE FROM table_name [USING source_data] [WHERE condition]
        return (
            self.condition.cumulative_complexity_stat if self.condition else Counter()
        )


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

    @property
    def individual_complexity_stat(self) -> Dict[str, int]:
        # MERGE INTO table_name USING (source) ON join_expr clauses
        return self.join_expr.cumulative_complexity_stat + sum(
            (clause.cumulative_complexity_stat for clause in self.clauses), Counter()
        )
