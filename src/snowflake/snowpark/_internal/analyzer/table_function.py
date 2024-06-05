#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from functools import cached_property
from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    Counter,
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

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        if not self.over:
            return Counter()
        stat = Counter({PlanNodeCategory.WINDOW.value: 1})
        stat += (
            sum(
                (expr.cumulative_complexity_stat for expr in self.partition_spec),
                Counter({PlanNodeCategory.PARTITION_BY.value: 1}),
            )
            if self.partition_spec
            else Counter()
        )
        stat += (
            sum(
                (expr.cumulative_complexity_stat for expr in self.order_spec),
                Counter({PlanNodeCategory.ORDER_BY.value: 1}),
            )
            if self.order_spec
            else Counter()
        )
        return stat


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

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return self.individual_complexity_stat + self.input.cumulative_complexity_stat


class PosArgumentsTableFunction(TableFunctionExpression):
    def __init__(
        self,
        func_name: str,
        args: List[Expression],
        partition_spec: Optional[TableFunctionPartitionSpecDefinition] = None,
    ) -> None:
        super().__init__(func_name, partition_spec)
        self.args = args

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        stat = sum(
            (arg.cumulative_complexity_stat for arg in self.args),
            self.individual_complexity_stat,
        )
        stat += (
            self.partition_spec.cumulative_complexity_stat
            if self.partition_spec
            else Counter()
        )
        return stat


class NamedArgumentsTableFunction(TableFunctionExpression):
    def __init__(
        self,
        func_name: str,
        args: Dict[str, Expression],
        partition_spec: Optional[TableFunctionPartitionSpecDefinition] = None,
    ) -> None:
        super().__init__(func_name, partition_spec)
        self.args = args

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        stat = sum(
            (arg.cumulative_complexity_stat for arg in self.args.values()),
            self.individual_complexity_stat,
        )
        stat += (
            self.partition_spec.cumulative_complexity_stat
            if self.partition_spec
            else Counter()
        )
        return stat


class GeneratorTableFunction(TableFunctionExpression):
    def __init__(self, args: Dict[str, Expression], operators: List[str]) -> None:
        super().__init__("generator")
        self.args = args
        self.operators = operators

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        stat = sum(
            (arg.cumulative_complexity_stat for arg in self.args.values()),
            self.individual_complexity_stat,
        )
        stat += (
            self.partition_spec.cumulative_complexity_stat
            if self.partition_spec
            else Counter()
        )
        stat += Counter({PlanNodeCategory.COLUMN.value: len(self.operators)})
        return stat


class TableFunctionRelation(LogicalPlan):
    def __init__(self, table_function: TableFunctionExpression) -> None:
        super().__init__()
        self.table_function = table_function

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # SELECT * FROM table_function
        return self.table_function.cumulative_complexity_stat


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
    def individual_complexity_stat(self) -> Counter[str]:
        # SELECT left_cols, right_cols FROM child as left_alias JOIN table(func(...)) as right_alias
        return (
            Counter(
                {
                    PlanNodeCategory.COLUMN.value: len(self.left_cols)
                    + len(self.right_cols),
                    PlanNodeCategory.JOIN.value: 1,
                }
            )
            + self.table_function.cumulative_complexity_stat
        )


class Lateral(LogicalPlan):
    def __init__(
        self, child: LogicalPlan, table_function: TableFunctionExpression
    ) -> None:
        super().__init__()
        self.children = [child]
        self.table_function = table_function

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # SELECT * FROM (child), LATERAL table_func_expression
        return (
            Counter({PlanNodeCategory.COLUMN.value: 1})
            + self.table_function.cumulative_complexity_stat
        )
