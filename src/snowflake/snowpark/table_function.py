#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import Iterable, List, Optional, Union

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, SortOrder
from snowflake.snowpark._internal.analyzer.table_function import (
    NamedArgumentsTableFunction,
    PosArgumentsTableFunction,
    TableFunctionExpression,
    TableFunctionPartitionSpecDefinition,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import validate_object_name
from snowflake.snowpark.column import Column, _to_col_if_str


class TableFunctionCall:
    def __init__(
        self,
        func_name: Union[str, Iterable[str]],
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

    def over(
        self,
        *,
        partition_by: Optional[Union[ColumnOrName, Iterable[ColumnOrName]]] = None,
        order_by: Optional[Union[ColumnOrName, Iterable[ColumnOrName]]] = None,
    ) -> "TableFunctionCall":
        new_table_function = TableFunctionCall(
            self.name, *self.arguments, **self.named_arguments
        )
        new_table_function._over = True

        if isinstance(partition_by, (str, Column)):
            partition_by_tuple = (partition_by,)
        elif partition_by is not None:
            partition_by_tuple = tuple(partition_by)
        else:
            partition_by_tuple = None
        partition_spec = (
            [
                e.expression if isinstance(e, Column) else Column(e).expression
                for e in partition_by_tuple
            ]
            if partition_by_tuple
            else None
        )
        new_table_function._partition_by = partition_spec

        if isinstance(order_by, (str, Column)):
            order_by_tuple = (order_by,)
        elif order_by is not None:
            order_by_tuple = tuple(order_by)
        else:
            order_by_tuple = None
        if order_by_tuple:
            order_spec = []
            if len(order_by_tuple) > 0:
                for e in order_by_tuple:
                    if isinstance(e, str):
                        order_spec.append(SortOrder(Column(e).expression, Ascending()))
                    elif isinstance(e, Column):
                        if isinstance(e.expression, SortOrder):
                            order_spec.append(e.expression)
                        elif isinstance(e.expression, Expression):
                            order_spec.append(SortOrder(e.expression, Ascending()))
            new_table_function._order_by = order_spec
        return new_table_function


def _create_table_function_expression(
    func: Union[str, List[str], TableFunctionCall],
    *args: ColumnOrName,
    **named_args: ColumnOrName,
) -> TableFunctionExpression:
    over = None
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
    elif isinstance(func, TableFunctionCall):
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
            "The table function name should be a str, a list of strs that have all or a part of the fully qualified name, or a TableFunctionCall instance."
        )
    if args:
        table_function_expression = PosArgumentsTableFunction(
            fqdn,
            [_to_col_if_str(arg, "table function").expression for arg in args],
        )
    else:
        table_function_expression = NamedArgumentsTableFunction(
            fqdn,
            {
                arg_name: _to_col_if_str(arg, "table function").expression
                for arg_name, arg in named_args.items()
            },
        )
    table_function_expression.partition_spec = (
        TableFunctionPartitionSpecDefinition(over, partition_by, order_by)
        if over
        else None
    )
    return table_function_expression
