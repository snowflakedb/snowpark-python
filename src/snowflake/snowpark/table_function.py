#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

"""Contains table function related classes."""
from typing import Dict, Iterable, List, Optional, Union

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
    """Represents a table function call.
    A table function call has the function names, positional arguments, named arguments and the partitioning information.

    The constructor of this class is not supposed to be called directly.
    Instead, use :func:`~snowflake.snowpark.function.call_table_function`, which will create an instance of this class.
    Or use :func:`~snowflake.snowpark.function.table_function` to create a ``Callable`` object and call it to create an
    instance of this class.
    """

    def __init__(
        self,
        func_name: Union[str, Iterable[str]],
        *func_arguments: ColumnOrName,
        **func_named_arguments: ColumnOrName,
    ):
        if func_arguments and func_named_arguments:
            raise ValueError("A table function shouldn't have both args and named args")
        self.name: str = func_name  #: The table function name
        self.arguments: Iterable[
            ColumnOrName
        ] = func_arguments  #: The positional arguments used to call this table function.
        self.named_arguments: Dict[
            str, ColumnOrName
        ] = func_named_arguments  #: The named arguments used to call this table function.
        self._over = False
        self._partition_by = None
        self._order_by = None

    def over(
        self,
        *,
        partition_by: Optional[Union[ColumnOrName, Iterable[ColumnOrName]]] = None,
        order_by: Optional[Union[ColumnOrName, Iterable[ColumnOrName]]] = None,
    ) -> "TableFunctionCall":
        """Specify the partitioning plan for this table function call when you lateral join this table function.

        When a query does a lateral join on a table function, the query feeds data to the table function row by row.
        Before rows are passed to table functions, the rows can be grouped into partitions. Partitioning has two main benefits:

          - Partitioning allows Snowflake to divide up the workload to improve parallelization and thus performance.
          - Partitioning allows Snowflake to process all rows with a common characteristic as a group. You can return results that are based on all rows in the group, not just on individual rows.

        Refer to `table functions and partitions <https://docs.snowflake.com/en/LIMITEDACCESS/udf-python-tabular-functions.html#processing-partitions>`__ for more information.

        Args:
            partition_by: Specify the partitioning column(s). It tells the table function to partition by these columns.
            order_by: Specify the ``order by`` column(s). It tells the table function to process input rows with this order within a partition.

        Note that if this function is called but both ``partition_by`` and ``order_by`` are ``None``, the table function call will put all input rows into a single partition.
        If this function isn't called at all, the Snowflake database will use implicit partitioning.
        """
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
                e._expression if isinstance(e, Column) else Column(e)._expression
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
                    order_spec.append(_create_order_by_expression(e))
            new_table_function._order_by = order_spec
        return new_table_function


def _create_order_by_expression(e: Union[str, Column]) -> SortOrder:
    if isinstance(e, str):
        return SortOrder(Column(e)._expression, Ascending())
    elif isinstance(e, Column):
        if isinstance(e._expression, SortOrder):
            return e._expression
        else:  # isinstance(e._expression, Expression):
            return SortOrder(e._expression, Ascending())
    else:
        raise TypeError(
            "Order By columns must be of column names in str, or a Column object."
        )


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
            "'func' should be a function name in str, a list of strs that have all or a part of the fully qualified name, or a TableFunctionCall instance."
        )
    if args:
        table_function_expression = PosArgumentsTableFunction(
            fqdn,
            [_to_col_if_str(arg, "table function")._expression for arg in args],
        )
    else:
        table_function_expression = NamedArgumentsTableFunction(
            fqdn,
            {
                arg_name: _to_col_if_str(arg, "table function")._expression
                for arg_name, arg in named_args.items()
            },
        )
    table_function_expression.partition_spec = (
        TableFunctionPartitionSpecDefinition(over, partition_by, order_by)
        if over
        else None
    )
    return table_function_expression
