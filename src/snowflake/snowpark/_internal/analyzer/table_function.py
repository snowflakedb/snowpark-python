#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Expression
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


class PosArgumentsTableFunction(TableFunctionExpression):
    def __init__(
        self,
        func_name: str,
        args: List[Expression],
        partition_spec: Optional[TableFunctionPartitionSpecDefinition] = None,
    ) -> None:
        super().__init__(func_name, partition_spec)
        self.args = args


class NamedArgumentsTableFunction(TableFunctionExpression):
    def __init__(
        self,
        func_name: str,
        args: Dict[str, Expression],
        partition_spec: Optional[TableFunctionPartitionSpecDefinition] = None,
    ) -> None:
        super().__init__(func_name, partition_spec)
        self.args = args


class GeneratorTableFunction(TableFunctionExpression):
    def __init__(self, args: Dict[str, Expression], operators: List[str]) -> None:
        super().__init__("generator")
        self.args = args
        self.operators = operators


class TableFunctionRelation(LogicalPlan):
    def __init__(self, table_function: TableFunctionExpression) -> None:
        super().__init__()
        self.table_function = table_function


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


class Lateral(LogicalPlan):
    def __init__(
        self, child: LogicalPlan, table_function: TableFunctionExpression
    ) -> None:
        super().__init__()
        self.children = [child]
        self.table_function = table_function
