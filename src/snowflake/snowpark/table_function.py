#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

"""Contains table function related classes."""
from typing import Dict, Iterable, List, Optional, Tuple, Union

from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
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

from ._internal.analyzer.snowflake_plan import SnowflakePlan


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
    ) -> None:
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
        self._aliases: Optional[Iterable[str]] = None
        self._api_call_source = None

    def _set_api_call_source(self, api_call_source):
        self._api_call_source = api_call_source

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

        Refer to `table functions and partitions <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions.html#processing-partitions>`__ for more information.

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

    def alias(self, *aliases: str) -> "TableFunctionCall":
        """Alias the output columns from the output of this table function call.

        Args:
            aliases: An iterable of unique column names that do not collide with column names after join with the main table.

        Raises:
            ValueError: Raises error when the aliases are not unique after being canonicalized.
        """
        canon_aliases = [quote_name(col) for col in aliases]
        if len(set(canon_aliases)) != len(aliases):
            raise ValueError("All output column names after aliasing must be unique.")

        self._aliases = canon_aliases
        return self

    as_ = alias


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
    aliases = None
    api_call_source = None
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
        aliases = func._aliases
        api_call_source = func._api_call_source
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
    table_function_expression.aliases = aliases
    table_function_expression.api_call_source = api_call_source
    return table_function_expression


def _get_cols_after_join_table(
    func_expr: TableFunctionExpression,
    current_plan: SnowflakePlan,
    join_plan: SnowflakePlan,
) -> Tuple[List, List]:
    def get_column_names_from_plan(plan: SnowflakePlan) -> List[str]:
        return [attr.name for attr in plan.output]

    # we ensure that all columns coming after the join should be unique
    cols_before_join = get_column_names_from_plan(current_plan)
    cols_after_join = get_column_names_from_plan(join_plan)
    aliases = func_expr.aliases

    new_cols = [col for col in cols_after_join if col not in cols_before_join]
    old_cols = [Column(col)._named() for col in cols_before_join]

    if aliases:
        if len(new_cols) != len(aliases):
            raise ValueError(
                f"The number of aliases should be same as the number of cols added by table function. "
                f"Columns added by table function are {new_cols} and aliases given are {aliases}"
            )
        new_cols = [
            Column(col).alias(alias_col)._named()
            for col, alias_col in zip(new_cols, aliases)
        ]
    else:
        new_cols = [Column(col)._named() for col in new_cols]

    return old_cols, new_cols
