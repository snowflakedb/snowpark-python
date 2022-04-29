#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List, Optional, Tuple, Union

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, SortOrder
from snowflake.snowpark._internal.analyzer.table_function import (
    NamedArgumentsTableFunction,
    PosArgumentsTableFunction,
    TableFunctionExpression,
    TableFunctionPartitionSpecDefinition,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import (
    parse_positional_args_to_list,
    validate_object_name,
)
from snowflake.snowpark.column import Column, _to_col_if_str


class TableFunction:
    def __init__(
        self,
        func_name: Union[str, List[str]],
        *func_arguments: ColumnOrName,
        **func_named_arguments: ColumnOrName,
    ):
        if func_arguments and func_named_arguments:
            raise ValueError("A table function shouldn't have both args and named args")
        self.name = func_name
        self.arguments = func_arguments
        self.named_arguments = func_named_arguments
        self._over = False
        self._partition_by = None
        self._order_by = None

    def over(self) -> "TableFunction":
        new_table_function = TableFunction(
            self.name, *self.arguments, **self.named_arguments
        )
        new_table_function._over = True
        return new_table_function

    def partition_by(
        self,
        *cols: Union[
            ColumnOrName,
            List[ColumnOrName],
            Tuple[ColumnOrName, ...],
        ],
    ):
        exprs = parse_positional_args_to_list(*cols)
        partition_spec = [
            e.expression if isinstance(e, Column) else Column(e).expression
            for e in exprs
        ]
        new_table_function = TableFunction(
            self.name, *self.arguments, **self.named_arguments
        )
        new_table_function._over = True
        new_table_function._partition_by = partition_spec
        new_table_function._order_by = self._order_by
        return new_table_function

    def order_by(
        self,
        *cols: Union[
            ColumnOrName,
            List[ColumnOrName],
            Tuple[ColumnOrName, ...],
        ],
    ):
        exprs = parse_positional_args_to_list(*cols)
        order_spec = []
        for e in exprs:
            if isinstance(e, str):
                order_spec.append(SortOrder(Column(e).expression, Ascending()))
            elif isinstance(e, Column):
                if isinstance(e.expression, SortOrder):
                    order_spec.append(e.expression)
                elif isinstance(e.expression, Expression):
                    order_spec.append(SortOrder(e.expression, Ascending()))

        new_table_function = TableFunction(
            self.name, *self.arguments, **self.named_arguments
        )
        new_table_function._over = True
        new_table_function._partition_by = self._partition_by
        new_table_function._order_by = order_spec
        return new_table_function


def _create_table_function_expression(
    func: Union[str, List[str], TableFunction],
    *args: ColumnOrName,
    **named_args: ColumnOrName,
) -> TableFunctionExpression:
    over = func
    partition_by = None
    order_by = None
    if args and named_args:
        raise ValueError("A table function shouldn't have both args and named args.")
    if isinstance(func, str):
        fqdn = func
    elif isinstance(func, list):
        for n in func:
            validate_object_name(n)
        fqdn = ".".join(func)
    elif isinstance(func, TableFunction):
        if args or named_args:
            raise ValueError(
                "'args' and 'named_args' shouldn't be used if a TableFunction instance is used."
            )
        fqdn = func.name
        args = func.arguments
        named_args = func.named_arguments
        over = func._over
        partition_by = func._partition_by
        order_by = func._order_by
    else:
        raise TypeError(
            "The table function name should be a str, a list of strs that have all or a part of the fully qualified name, or a TableFunction instance."
        )
    if args:
        table_function_expression = PosArgumentsTableFunction(
            fqdn,
            [_to_col_if_str(arg, "table_function").expression for arg in args],
        )
    else:
        table_function_expression = NamedArgumentsTableFunction(
            fqdn,
            {
                arg_name: _to_col_if_str(arg, "table_function").expression
                for arg_name, arg in named_args.items()
            },
        )
    table_function_expression.partition_spec = (
        TableFunctionPartitionSpecDefinition(over, partition_by, order_by)
        if over
        else None
    )
    return table_function_expression
