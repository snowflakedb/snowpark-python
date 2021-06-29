#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from typing import List, Optional, Union

from .column import Column
from .internal.sp_expressions import (
    AggregateFunction as SPAggregateFunction,
    Avg as SPAverage,
    Count as SPCount,
    Expression as SPExpression,
    Literal as SPLiteral,
    Max as SPMax,
    Min as SPMin,
    Star as SPStar,
    Sum as SPSum,
    UnresolvedFunction as SPUnresolvedFunction,
)
from .types.sp_data_types import IntegerType as SPIntegerType


def col(col_name: str) -> Column:
    """Returns the [[Column]] with the specified name."""
    return Column(col_name)


def column(col_name) -> Column:
    """Returns a [[Column]] with the specified name. Alias for col."""
    return Column(col_name)


def lit(literal) -> Column:
    """Creates a [[Column]] expression for a literal value."""
    return typedLit(literal)


def typedLit(literal) -> Column:
    """Creates a [[Column]] expression for a literal value."""
    if type(literal) == Column:
        return literal
    else:
        return Column(SPLiteral.create(literal))


def sql_expr(sql: str) -> Column:
    """Creates a [[Column]] expression from raw SQL text.
    Note that the function does not interpret or check the SQL text."""
    return Column.expr(sql)


def avg(e: Column) -> Column:
    """Returns the average of non-NULL records. If all records inside a group are NULL, the function
    returns NULL."""
    return __with_aggregate_function(SPAverage(e.expression))


def count(e: Column) -> Column:
    """Returns either the number of non-NULL records for the specified columns, or the total number
    of records."""
    exp = (
        SPCount(SPLiteral(1, SPIntegerType()))
        if isinstance(e, SPStar)
        else SPCount(e.expression)
    )
    return __with_aggregate_function(exp)


def count_distinct(columns: Union[Column, List[Column]]) -> Column:
    """Returns either the number of non-NULL distinct records for the specified columns,
    or the total number of the distinct records.

    Accepts a Column object or a list of Column objects.
    """
    cols = [columns] if type(columns) == Column else columns
    if not all(type(c) == Column for c in cols):
        raise TypeError("Invalid input to count_distinct().")
    return Column(
        SPUnresolvedFunction("count", [c.expression for c in cols], is_distinct=True)
    )


def max(e: Column) -> Column:
    """Returns the maximum value for the records in a group. NULL values are ignored unless all
    the records are NULL, in which case a NULL value is returned."""
    return __with_aggregate_function(SPMax(e.expression))


def mean(e: Column) -> Column:
    """Return the average for the specific numeric columns. Alias of avg"""
    return avg(e)


def median(e: Column) -> Column:
    """Returns the median value for the records in a group. NULL values are ignored unless all the
    records are NULL, in which case a NULL value is returned."""
    return builtin("median")(e)


def min(e: Column) -> Column:
    """Returns the minimum value for the records in a group. NULL values are ignored unless all
    the records are NULL, in which case a NULL value is returned."""
    return __with_aggregate_function(SPMin(e.expression))


def skew(e: Column) -> Column:
    """Returns the sample skewness of non-NULL records. If all records inside a group are NULL,
    the function returns NULL."""
    return builtin("skew")(e)


def stddev(e: Column) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of non-NULL values.
    If all records inside a group are NULL, returns NULL."""
    return builtin("stddev")(e)


def stddev_samp(e: Column) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of non-NULL values.
    If all records inside a group are NULL, returns NULL. Alias of stddev"""
    return builtin("stddev_samp")(e)


def stddev_pop(e: Column) -> Column:
    """Returns the population standard deviation (square root of variance) of non-NULL values.
    If all records inside a group are NULL, returns NULL."""
    return builtin("stddev_pop")(e)


def sum(e: Column) -> Column:
    """Returns the sum of non-NULL records in a group. You can use the DISTINCT keyword to compute
    the sum of unique non-null values. If all records inside a group are NULL, the function returns
    NULL."""
    return __with_aggregate_function(SPSum(e.expression))


def sum_distinct(e: Column) -> Column:
    """Returns the sum of non-NULL distinct records in a group. You can use the DISTINCT keyword to
    compute the sum of unique non-null values. If all records inside a group are NULL,
    the function returns NULL."""
    return __with_aggregate_function(SPSum(e.expression), is_distinct=True)


def parse_json(s: Column) -> Column:
    """Parse the value of the specified column as a JSON string and returns the resulting JSON
    document."""
    return builtin("parse_json")(s)


def to_decimal(expr: Column, precision: int, scale: int) -> Column:
    """Converts an input expression to a decimal."""
    return builtin("to_decimal")(expr, sql_expr(str(precision)), sql_expr(str(scale)))


def to_time(s: Column, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding time."""
    return builtin("to_time")(s, fmt) if fmt else builtin("to_time")(s)


def to_timestamp(s: Column, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding timestamp."""
    return builtin("to_timestamp")(s, fmt) if fmt else builtin("to_timestamp")(s)


def to_date(s: Column, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into a date."""
    return builtin("to_date")(s, fmt) if fmt else builtin("to_date")(s)


def to_array(s: Column) -> Column:
    """Converts any value to an ARRAY value or NULL (if input is NULL)."""
    return builtin("to_array")(s)


def to_variant(s: Column) -> Column:
    """Converts any value to a VARIANT value or NULL (if input is NULL)."""
    return builtin("to_variant")(s)


def to_object(s: Column) -> Column:
    """Converts any value to a OBJECT value or NULL (if input is NULL)."""
    return builtin("to_object")(s)


def __with_aggregate_function(
    func: SPAggregateFunction, is_distinct: bool = False
) -> Column:
    return Column(func.to_aggregate_expression(is_distinct))


def call_builtin(function_name, *args):
    """Invokes a built-in snowflake function with the specified name and arguments.
    Arguments can be of two types
        a. [[Column]], or
        b. Basic types such as Int, Long, Double, Decimal etc. which are converted to Snowpark
        literals.
    """

    sp_expressions = []
    for arg in args:
        if type(arg) == Column:
            sp_expressions.append(arg.expression)
        elif isinstance(arg, SPExpression):
            sp_expressions.append(arg)
        else:
            sp_expressions.append(SPLiteral.create(arg))

    return Column(
        SPUnresolvedFunction(function_name, sp_expressions, is_distinct=False)
    )


def builtin(function_name):
    """
    Function object to invoke a Snowflake builtin. Use this to invoke
    any builtins not explicitly listed in this object.

    Example
    {{{
       avg = functions.builtin('avg')
       df.select(avg(col("col_1")))
    }}}
    :param function_name: Name of built-in Snowflake function
    :return: Column
    """
    return lambda *args: call_builtin(function_name, *args)
