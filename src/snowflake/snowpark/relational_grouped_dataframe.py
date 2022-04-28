#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import re
from typing import Callable, List, Tuple, Union

from snowflake.snowpark import functions
from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    Literal,
    NamedExpression,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.grouping_set import (
    Cube,
    GroupingSetsExpression,
    Rollup,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import Aggregate, Pivot
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import rgdf_api_usage
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import parse_positional_args_to_list
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame

INVALID_SF_IDENTIFIER_CHARS = re.compile("[^\\x20-\\x7E]")


def _strip_invalid_sf_identifier_chars(identifier: str) -> str:
    return INVALID_SF_IDENTIFIER_CHARS.sub("", identifier.replace('"', ""))


def _alias(expr: Expression) -> NamedExpression:
    if isinstance(expr, UnresolvedAttribute):
        return UnresolvedAlias(expr)
    elif isinstance(expr, NamedExpression):
        return expr
    else:
        return Alias(
            expr,
            _strip_invalid_sf_identifier_chars(expr.sql.upper()),
        )


def _expr_to_func(expr: str, input_expr: Expression) -> Expression:
    lowered = expr.lower()
    if lowered in ["avg", "average", "mean"]:
        return functions.avg(Column(input_expr))._expression
    elif lowered in ["stddev", "std"]:
        return functions.stddev(Column(input_expr))._expression
    elif lowered in ["count", "size"]:
        return functions.count(Column(input_expr))._expression
    else:
        return functions.builtin(lowered)(input_expr)._expression


def _str_to_expr(expr: str) -> Callable:
    return lambda input_expr: _expr_to_func(expr, input_expr)


class _GroupType:
    def to_string(self) -> str:
        return self.__class__.__name__[1:-4]


class _GroupByType(_GroupType):
    pass


class _CubeType(_GroupType):
    pass


class _RollupType(_GroupType):
    pass


class _PivotType(_GroupType):
    def __init__(self, pivot_col: Expression, values: List[Expression]) -> None:
        self.pivot_col = pivot_col
        self.values = values


class GroupingSets:
    """Creates a :class:`GroupingSets` object from a list of column/expression sets that you pass
    to :meth:`DataFrame.group_by_grouping_sets`. See :meth:`DataFrame.group_by_grouping_sets` for
    examples of how to use this class with a :class:`DataFrame`. See
    `GROUP BY GROUPING SETS <https://docs.snowflake.com/en/sql-reference/constructs/group-by-grouping-sets.html>`_
    for its counterpart in SQL (several examples are shown below).

    =============================================================  ==================================
    Python interface                                               SQL interface
    =============================================================  ==================================
    ``GroupingSets([col("a")], [col("b")])``                       ``GROUPING SETS ((a), (b))``
    ``GroupingSets([col("a") , col("b")], [col("c"), col("d")])``  ``GROUPING SETS ((a, b), (c, d))``
    ``GroupingSets([col("a"), col("b")])``                         ``GROUPING SETS ((a, b))``
    ``GroupingSets(col("a"), col("b"))``                           ``GROUPING SETS ((a, b))``
    =============================================================  ==================================
    """

    def __init__(self, *sets: Union[Column, List[Column]]) -> None:
        prepared_sets = parse_positional_args_to_list(*sets)
        prepared_sets = (
            prepared_sets if isinstance(prepared_sets[0], list) else [prepared_sets]
        )
        self._to_expression = GroupingSetsExpression(
            [[c._expression for c in s] for s in prepared_sets]
        )


class RelationalGroupedDataFrame:
    """Represents an underlying DataFrame with rows that are grouped by common values.
    Can be used to define aggregations on these grouped DataFrames.
    """

    def __init__(
        self, df: DataFrame, grouping_exprs: List[Expression], group_type: _GroupType
    ) -> None:
        self._df = df
        self._grouping_exprs = grouping_exprs
        self._group_type = group_type
        self._df_api_call = None

    def _to_df(self, agg_exprs: List[Expression]) -> DataFrame:
        aliased_agg = []
        for grouping_expr in self._grouping_exprs:
            if isinstance(grouping_expr, GroupingSetsExpression):
                # avoid doing list(set(grouping_expr.args)) because it will change the order
                gr_used = set()
                gr_uniq = [
                    a
                    for arg in grouping_expr.args
                    for a in arg
                    if a not in gr_used and (gr_used.add(a) or True)
                ]
                aliased_agg.extend(gr_uniq)
            else:
                aliased_agg.append(grouping_expr)

        aliased_agg.extend(agg_exprs)

        # Avoid doing aliased_agg = [self.alias(a) for a in list(set(aliased_agg))],
        # to keep order
        used = set()
        unique = [a for a in aliased_agg if a not in used and (used.add(a) or True)]
        aliased_agg = [_alias(a) for a in unique]

        if isinstance(self._group_type, _GroupByType):
            return DataFrame(
                self._df._session,
                Aggregate(self._grouping_exprs, aliased_agg, self._df._plan),
            )
        if isinstance(self._group_type, _RollupType):
            return DataFrame(
                self._df._session,
                Aggregate(
                    [Rollup(self._grouping_exprs)],
                    aliased_agg,
                    self._df._plan,
                ),
            )
        if isinstance(self._group_type, _CubeType):
            return DataFrame(
                self._df._session,
                Aggregate([Cube(self._grouping_exprs)], aliased_agg, self._df._plan),
            )
        if isinstance(self._group_type, _PivotType):
            if len(agg_exprs) != 1:
                raise SnowparkClientExceptionMessages.DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR()
            return DataFrame(
                self._df._session,
                Pivot(
                    self._group_type.pivot_col,
                    self._group_type.values,
                    agg_exprs,
                    self._df._plan,
                ),
            )

    @rgdf_api_usage
    def agg(self, exprs: List[Union[Column, Tuple[Column, str]]]) -> DataFrame:
        """Returns a :class:`DataFrame` with computed aggregates. The first element of
        the ``exprs`` pair is the column to aggregate and the second element is the
        aggregate function to compute. The name of the aggregate
        function to compute must be a valid Snowflake `aggregate function
        <https://docs.snowflake.com/en/sql-reference/functions-aggregation.html>`_.
        See examples in :meth:`DataFrame.group_by`.

        Valid input:

            - A Column object
            - A tuple where the first element is a column and the second element is a name (str) of the aggregate function
            - A list of the above
        """
        if not isinstance(exprs, (list, tuple)):
            exprs = [exprs]

        agg_exprs = []
        for e in exprs:
            if isinstance(e, Column):
                agg_exprs.append(e._expression)
            elif (
                isinstance(e, tuple)
                and isinstance(e[0], Column)
                and isinstance(e[1], str)
            ):
                agg_exprs.append(_str_to_expr(e[1])(e[0]._expression))
            else:
                raise TypeError("Invalid input types for agg()")

        return self._to_df(agg_exprs)

    @rgdf_api_usage
    def avg(self, *cols: ColumnOrName) -> DataFrame:
        """Return the average for the specified numeric columns."""
        return self._non_empty_argument_function("avg", *cols)

    mean = avg

    @rgdf_api_usage
    def sum(self, *cols: ColumnOrName) -> DataFrame:
        """Return the sum for the specified numeric columns."""
        return self._non_empty_argument_function("sum", *cols)

    @rgdf_api_usage
    def median(self, *cols: ColumnOrName) -> DataFrame:
        """Return the median for the specified numeric columns."""
        return self._non_empty_argument_function("median", *cols)

    @rgdf_api_usage
    def min(self, *cols: ColumnOrName) -> DataFrame:
        """Return the min for the specified numeric columns."""
        return self._non_empty_argument_function("min", *cols)

    @rgdf_api_usage
    def max(self, *cols: ColumnOrName) -> DataFrame:
        """Return the max for the specified numeric columns."""
        return self._non_empty_argument_function("max", *cols)

    @rgdf_api_usage
    def count(self) -> DataFrame:
        """Return the number of rows for each group."""
        return self._to_df(
            [
                Alias(
                    functions.builtin("count")(Literal(1))._expression,
                    "count",
                )
            ]
        )

    def builtin(self, agg_name: str) -> Callable:
        """Computes the builtin aggregate ``agg_name`` over the specified columns. Use
        this function to invoke any aggregates not explicitly listed in this class.
        See examples in :meth:`DataFrame.group_by`.
        """
        return lambda *cols: self._builtin_internal(agg_name, *cols)

    def _builtin_internal(self, agg_name: str, *cols: ColumnOrName) -> DataFrame:
        agg_exprs = []
        for c in cols:
            c_expr = Column(c)._expression if isinstance(c, str) else c._expression
            expr = functions.builtin(agg_name)(c_expr)._expression
            agg_exprs.append(expr)
        return self._to_df(agg_exprs)

    def _non_empty_argument_function(
        self, func_name: str, *cols: ColumnOrName
    ) -> DataFrame:
        if not cols:
            raise ValueError(
                f"You must pass a list of one or more Columns to function: {func_name}"
            )
        else:
            return self.builtin(func_name)(*cols)
