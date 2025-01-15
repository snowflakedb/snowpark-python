#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, Iterable, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    LogicalPlan,
    SnowflakePlan,
)
from snowflake.snowpark._internal.type_utils import ColumnOrLiteral


class MergeExpression(Expression):
    def __init__(self, condition: Optional[Expression]) -> None:
        super().__init__()
        self.condition = condition

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.LOW_IMPACT

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # WHEN MATCHED [AND condition] THEN DEL
        complexity = {self.plan_node_category: 1}
        complexity = (
            sum_node_complexities(complexity, self.condition.cumulative_node_complexity)
            if self.condition
            else complexity
        )
        return complexity


class UpdateMergeExpression(MergeExpression):
    def __init__(
        self, condition: Optional[Expression], assignments: Dict[str, ColumnOrLiteral]
    ) -> None:
        super().__init__(condition)
        self._assignments = assignments

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # WHEN MATCHED [AND condition] THEN UPDATE SET COMMA.join(k=v for k,v in assignments)
        complexity = sum_node_complexities(
            {self.plan_node_category: 1},
            *(
                sum_node_complexities(
                    key_expr.cumulative_node_complexity,
                    val_expr.cumulative_node_complexity,
                )
                for key_expr, val_expr in self.assignments.items()
            ),
        )
        complexity = (
            sum_node_complexities(complexity, self.condition.cumulative_node_complexity)
            if self.condition
            else complexity
        )
        return complexity

    @property
    def assignments(self) -> Dict[Expression, Expression]:
        from snowflake.snowpark.column import Column

        return {
            Column(k)._expression: Column._to_expr(v)
            for k, v in self._assignments.items()
        }


class DeleteMergeExpression(MergeExpression):
    pass


class InsertMergeExpression(MergeExpression):
    def __init__(
        self,
        condition: Optional[Expression],
        keys: Iterable[str],
        values: Iterable[ColumnOrLiteral],
    ) -> None:
        super().__init__(condition)
        self._keys = keys
        self._values = values

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # WHEN NOT MATCHED [AND cond] THEN INSERT [(COMMA.join(key))] VALUES (COMMA.join(values))
        complexity = sum_node_complexities(
            {self.plan_node_category: 1},
            *(key.cumulative_node_complexity for key in self.keys),
            *(val.cumulative_node_complexity for val in self.values),
        )
        complexity = (
            sum_node_complexities(complexity, self.condition.cumulative_node_complexity)
            if self.condition
            else complexity
        )
        return complexity

    @property
    def keys(self) -> List[Expression]:
        from snowflake.snowpark.column import Column

        return [Column(k)._expression for k in self._keys]

    @property
    def values(self) -> List[Expression]:
        from snowflake.snowpark.column import Column

        return [Column._to_expr(v) for v in self._values]


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
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # UPDATE table_name SET COMMA.join(k, v in assignments) [source_data] [WHERE condition]
        complexity = sum_node_complexities(
            *(
                sum_node_complexities(
                    k.cumulative_node_complexity, v.cumulative_node_complexity
                )
                for k, v in self.assignments.items()
            ),
        )
        complexity = (
            sum_node_complexities(complexity, self.condition.cumulative_node_complexity)
            if self.condition
            else complexity
        )
        return complexity


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
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # DELETE FROM table_name [USING source_data] [WHERE condition]
        return self.condition.cumulative_node_complexity if self.condition else {}


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
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # MERGE INTO table_name USING (source) ON join_expr clauses
        return sum_node_complexities(
            self.join_expr.cumulative_node_complexity,
            *(clause.cumulative_node_complexity for clause in self.clauses),
        )
