#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.analyzer.sort_expression import SortOrder

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


class TableFunctionPartitionSpecDefinition(Expression):
    def __init__(
        self,
        over: bool = False,
        partition_spec: Optional[List[Expression]] = None,
        order_spec: Optional[List[SortOrder]] = None,
    ) -> None:
        super().__init__()
        self.over = over
        self.partition_spec = partition_spec
        self.order_spec = order_spec

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if not self.over:
            return {}
        complexity = {PlanNodeCategory.WINDOW: 1}
        complexity = (
            sum_node_complexities(
                complexity,
                *(expr.cumulative_node_complexity for expr in self.partition_spec),
                {PlanNodeCategory.PARTITION_BY: 1},
            )
            if self.partition_spec
            else complexity
        )
        complexity = (
            sum_node_complexities(
                complexity,
                *(expr.cumulative_node_complexity for expr in self.order_spec),
                {PlanNodeCategory.ORDER_BY: 1},
            )
            if self.order_spec
            else complexity
        )
        return complexity


class TableFunctionExpression(Expression):
    def __init__(
        self,
        func_name: str,
        partition_spec: Optional[TableFunctionPartitionSpecDefinition] = None,
        aliases: Optional[Iterable[str]] = None,
        api_call_source: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.func_name = func_name
        self.partition_spec = partition_spec
        self.aliases = aliases
        self.api_call_source = api_call_source

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.FUNCTION


class FlattenFunction(TableFunctionExpression):
    def __init__(
        self, input: Expression, path: str, outer: bool, recursive: bool, mode: str
    ) -> None:
        super().__init__("flatten")
        self.input = input
        self.path = path
        self.outer = outer
        self.recursive = recursive
        self.mode = mode

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            {self.plan_node_category: 1}, self.input.cumulative_node_complexity
        )


class PosArgumentsTableFunction(TableFunctionExpression):
    def __init__(
        self,
        func_name: str,
        args: List[Expression],
        partition_spec: Optional[TableFunctionPartitionSpecDefinition] = None,
    ) -> None:
        super().__init__(func_name, partition_spec)
        self.args = args

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        complexity = sum_node_complexities(
            {self.plan_node_category: 1},
            *(arg.cumulative_node_complexity for arg in self.args),
        )
        complexity = (
            sum_node_complexities(
                complexity, self.partition_spec.cumulative_node_complexity
            )
            if self.partition_spec
            else complexity
        )
        return complexity


class NamedArgumentsTableFunction(TableFunctionExpression):
    def __init__(
        self,
        func_name: str,
        args: Dict[str, Expression],
        partition_spec: Optional[TableFunctionPartitionSpecDefinition] = None,
    ) -> None:
        super().__init__(func_name, partition_spec)
        self.args = args

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        complexity = sum_node_complexities(
            {self.plan_node_category: 1},
            *(arg.cumulative_node_complexity for arg in self.args.values()),
        )
        complexity = (
            sum_node_complexities(
                complexity, self.partition_spec.cumulative_node_complexity
            )
            if self.partition_spec
            else complexity
        )
        return complexity


class GeneratorTableFunction(TableFunctionExpression):
    def __init__(self, args: Dict[str, Expression], operators: List[str]) -> None:
        super().__init__("generator")
        self.args = args
        self.operators = operators

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        complexity = sum_node_complexities(
            {self.plan_node_category: 1},
            *(arg.cumulative_node_complexity for arg in self.args.values()),
        )
        complexity = (
            sum_node_complexities(
                complexity, self.partition_spec.cumulative_node_complexity
            )
            if self.partition_spec
            else complexity
        )
        complexity = sum_node_complexities(
            complexity, {PlanNodeCategory.COLUMN: len(self.operators)}
        )
        return complexity


class TableFunctionRelation(LogicalPlan):
    def __init__(self, table_function: TableFunctionExpression) -> None:
        super().__init__()
        self.table_function = table_function

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT * FROM table_function
        return self.table_function.cumulative_node_complexity


class TableFunctionJoin(LogicalPlan):
    def __init__(
        self,
        child: LogicalPlan,
        table_function: TableFunctionExpression,
        left_cols: Optional[List[str]] = None,
        right_cols: Optional[List[str]] = None,
    ) -> None:
        super().__init__()
        self.children = [child]
        self.table_function = table_function
        self.left_cols = left_cols if left_cols is not None else ["*"]
        self.right_cols = right_cols if right_cols is not None else ["*"]

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT left_cols, right_cols FROM child as left_alias JOIN table(func(...)) as right_alias
        return sum_node_complexities(
            {
                PlanNodeCategory.COLUMN: len(self.left_cols) + len(self.right_cols),
                PlanNodeCategory.JOIN: 1,
            },
            self.table_function.cumulative_node_complexity,
        )


class Lateral(LogicalPlan):
    def __init__(
        self, child: LogicalPlan, table_function: TableFunctionExpression
    ) -> None:
        super().__init__()
        self.children = [child]
        self.table_function = table_function

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT * FROM (child), LATERAL table_func_expression
        return sum_node_complexities(
            {PlanNodeCategory.COLUMN: 1},
            self.table_function.cumulative_node_complexity,
        )
