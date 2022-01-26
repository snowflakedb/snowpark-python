#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import re
from typing import Callable, List, Tuple, Union

import snowflake.snowpark  # type: ignore
from snowflake.snowpark import functions
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.plans.logical.basic_logical_operators import (
    Aggregate as SPAggregate,
    Pivot as SPPivot,
)
from snowflake.snowpark._internal.sp_expressions import (
    Alias as SPAlias,
    Cube as SPCube,
    Expression as SPExpression,
    GroupingSetsExpression as SPGroupingSetsExpression,
    Literal as SPLiteral,
    NamedExpression as SPNamedExpression,
    Rollup as SPRollup,
    UnresolvedAlias as SPUnresolvedAlias,
    UnresolvedAttribute as SPUnresolvedAttribute,
)
from snowflake.snowpark._internal.sp_types.types_package import ColumnOrName
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame


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
    def __init__(self, pivot_col: SPExpression, values: List[SPExpression]):
        self.pivot_col = pivot_col
        self.values = values


class GroupingSets:
    """Creates a GroupingSets object from a list of column/expression sets that you pass
    to :meth:`DataFrame.group_by_grouping_sets`. See :meth:`DataFrame.group_by_grouping_sets` for
    examples of how to use this class with a :class:`DataFrame`.

    Examples::

        GroupingSets([col("a")], [col("b")])  # Becomes "GROUPING SETS ((a), (b))"
        GroupingSets([col("a") , col("b")], [col("c"), col("d")])   # Becomes "GROUPING SETS ((a, b), (c, d))"
        GroupingSets([col("a"), col("b")])        # Becomes "GROUPING SETS ((a, b))"
        GroupingSets(col("a"), col("b"))  # Becomes "GROUPING SETS ((a, b))"
    """

    def __init__(self, *sets: Union[Column, List[Column]]):
        prepared_sets = Utils.parse_positional_args_to_list(*sets)
        prepared_sets = (
            prepared_sets if isinstance(prepared_sets[0], list) else [prepared_sets]
        )
        self.to_expression = SPGroupingSetsExpression(
            [[c.expression for c in s] for s in prepared_sets]
        )


class RelationalGroupedDataFrame:
    """Represents an underlying DataFrame with rows that are grouped by common values.
    Can be used to define aggregations on these grouped DataFrames.

    Example::

        grouped_df = df.group_by("dept")
        agg_df = grouped_df.agg([(grouped_df["salary"], "mean")])

    The method :py:func:`DataFrame.group_by()`
    returns a :class:`RelationalGroupedDataFrame` object."""

    def __init__(self, df, grouping_exprs: List[SPExpression], group_type: _GroupType):
        self.df = df
        self.grouping_exprs = grouping_exprs
        self.group_type = group_type

    # subscriptable returns new object

    def __toDF(self, agg_exprs: List[SPExpression]):
        aliased_agg = []
        for grouping_expr in self.grouping_exprs:
            if isinstance(grouping_expr, SPGroupingSetsExpression):
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
        aliased_agg = [self.__alias(a) for a in unique]

        if isinstance(self.group_type, _GroupByType):
            return DataFrame(
                self.df.session,
                SPAggregate(self.grouping_exprs, aliased_agg, self.df._plan),
            )
        if isinstance(self.group_type, _RollupType):
            return DataFrame(
                self.df.session,
                SPAggregate(
                    [SPRollup(self.grouping_exprs)],
                    aliased_agg,
                    self.df._plan,
                ),
            )
        if isinstance(self.group_type, _CubeType):
            return DataFrame(
                self.df.session,
                SPAggregate([SPCube(self.grouping_exprs)], aliased_agg, self.df._plan),
            )
        if isinstance(self.group_type, _PivotType):
            if len(agg_exprs) != 1:
                raise SnowparkClientExceptionMessages.DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR()
            return DataFrame(
                self.df.session,
                SPPivot(
                    self.group_type.pivot_col,
                    self.group_type.values,
                    agg_exprs,
                    self.df._plan,
                ),
            )

    def __alias(self, expr: SPExpression) -> SPNamedExpression:
        if isinstance(expr, SPUnresolvedAttribute):
            return SPUnresolvedAlias(expr, None)
        elif isinstance(expr, SPNamedExpression):
            return expr
        else:
            return SPAlias(
                expr,
                self.__strip_invalid_sf_identifier_chars(expr.sql().upper()),
            )

    @staticmethod
    def __strip_invalid_sf_identifier_chars(identifier: str):
        p = re.compile("[^\\x20-\\x7E]")
        return p.sub("", identifier.replace('"', ""))

    def __str_to_expr(self, expr: str) -> Callable:
        return lambda input_expr: self.__expr_to_func(expr, input_expr)

    @staticmethod
    def __expr_to_func(expr: str, input_expr: SPExpression) -> SPExpression:
        lowered = expr.lower()
        if lowered in ["avg", "average", "mean"]:
            return functions.avg(Column(input_expr)).expression
        elif lowered in ["stddev", "std"]:
            return functions.stddev(Column(input_expr)).expression
        elif lowered in ["count", "size"]:
            return functions.count(Column(input_expr)).expression
        else:
            return functions.builtin(lowered)(input_expr).expression

    def agg(self, exprs: List[Union[Column, Tuple[Column, str]]]) -> "DataFrame":
        """Returns a :class:`DataFrame` with computed aggregates. The first element of
        the ``exprs`` pair is the column to aggregate and the second element is the
        aggregate function to compute. The following example computes the mean of the
        price column and the sum of the sales column. The name of the aggregate
        function to compute must be a valid Snowflake `aggregate function
        <https://docs.snowflake.com/en/sql-reference/functions-aggregation.html>`_.
        :func:`avg` and :func:`mean` can be used to specify ``average``.

        Valid input:

            - A Column object
            - A tuple where the first element is a column and the second element is a name (str) of the aggregate function
            - A list of the above

        Example::

            from snowflake.snowpark.functions import col
            df.group_by("itemType").agg([(col("price"), "mean"), (col("sales"), "sum")])
        """
        if not isinstance(exprs, (list, tuple)):
            exprs = [exprs]

        agg_exprs = []
        for e in exprs:
            if isinstance(e, Column):
                agg_exprs.append(e.expression)
            elif (
                isinstance(e, tuple)
                and isinstance(e[0], Column)
                and isinstance(e[1], str)
            ):
                agg_exprs.append(self.__str_to_expr(e[1])(e[0].expression))
            else:
                raise TypeError("Invalid input types for agg()")

        return self.__toDF(agg_exprs)

    def avg(self, *cols: ColumnOrName) -> "DataFrame":
        """Return the average for the specified numeric columns."""
        return self.__non_empty_argument_function("avg", *cols)

    def mean(self, *cols: ColumnOrName) -> "DataFrame":
        """Return the average for the specified numeric columns. Alias of :obj:`avg`."""
        return self.avg(*cols)

    def sum(self, *cols: ColumnOrName) -> "DataFrame":
        """Return the sum for the specified numeric columns."""
        return self.__non_empty_argument_function("sum", *cols)

    def median(self, *cols: ColumnOrName) -> "DataFrame":
        """Return the median for the specified numeric columns."""
        return self.__non_empty_argument_function("median", *cols)

    def min(self, *cols: ColumnOrName) -> "DataFrame":
        """Return the min for the specified numeric columns."""
        return self.__non_empty_argument_function("min", *cols)

    def max(self, *cols: ColumnOrName) -> "DataFrame":
        """Return the max for the specified numeric columns."""
        return self.__non_empty_argument_function("max", *cols)

    def count(self) -> "DataFrame":
        """Return the number of rows for each group."""
        return self.__toDF(
            [
                SPAlias(
                    functions.builtin("count")(SPLiteral(1)).expression,
                    "count",
                )
            ]
        )

    def builtin(self, agg_name: str) -> Callable:
        """Computes the builtin aggregate ``agg_name`` over the specified columns. Use
        this function to invoke any aggregates not explicitly listed in this class.

        Example::

                df.group_by("a").builtin("max")(col("b"))
        """
        return lambda *cols: self.__builtin_internal(agg_name, *cols)

    def __builtin_internal(self, agg_name: str, *cols: ColumnOrName) -> "DataFrame":
        agg_exprs = []
        for c in cols:
            c_expr = Column(c).expression if isinstance(c, str) else c.expression
            expr = functions.builtin(agg_name)(c_expr).expression
            agg_exprs.append(expr)
        return self.__toDF(agg_exprs)

    def __non_empty_argument_function(
        self, func_name: str, *cols: ColumnOrName
    ) -> "DataFrame":
        if not cols:
            raise ValueError(
                f"You must pass a list of one or more Columns to function: {func_name}"
            )
        else:
            return self.builtin(func_name)(*cols)
