#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List, Union

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.column import _to_col_if_str


class TableFunctionExpression(Expression):
    def __init__(self):
        super().__init__()


class FlattenFunction(TableFunctionExpression):
    def __init__(
        self, input: Expression, path: str, outer: bool, recursive: bool, mode: str
    ):
        super().__init__()
        self.input = input
        self.path = path
        self.outer = outer
        self.recursive = recursive
        self.mode = mode


class TableFunction(TableFunctionExpression):
    def __init__(self, func_name: str, args: List[Expression]):
        super().__init__()
        self.func_name = func_name
        self.args = args


class NamedArgumentsTableFunction(TableFunctionExpression):
    def __init__(self, func_name: str, args: Dict[str, Expression]):
        super().__init__()
        self.func_name = func_name
        self.args = args


class TableFunctionRelation(LogicalPlan):
    def __init__(self, table_function: TableFunctionExpression):
        super().__init__()
        self.table_function = table_function


class TableFunctionJoin(LogicalPlan):
    def __init__(self, child: LogicalPlan, table_function: TableFunctionExpression):
        super().__init__()
        self.children = [child]
        self.table_function = table_function


class Lateral(LogicalPlan):
    def __init__(self, child: LogicalPlan, table_function: TableFunctionExpression):
        super().__init__()
        self.children = [child]
        self.table_function = table_function


def create_table_function_expression(
    func_name: Union[str, List[str]],
    *args: ColumnOrName,
    **named_args: ColumnOrName,
) -> TableFunctionExpression:
    if args and named_args:
        raise ValueError("A table function shouldn't have both args and named args")
    if isinstance(func_name, str):
        fqdn = func_name
    elif isinstance(func_name, list):
        for n in func_name:
            Utils.validate_object_name(n)
        fqdn = ".".join(func_name)
    else:
        raise TypeError("The table function name should be a str or a list of strs.")
    func_arguments = args
    if func_arguments:
        return TableFunction(
            fqdn,
            [
                _to_col_if_str(arg, "table_function").expression
                for arg in func_arguments
            ],
        )
    return NamedArgumentsTableFunction(
        fqdn,
        {
            arg_name: _to_col_if_str(arg, "table_function").expression
            for arg_name, arg in named_args.items()
        },
    )
