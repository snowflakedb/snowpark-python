#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import functools
from typing import Any, Callable, List, Optional, Tuple, Union

from snowflake.snowpark.column import CaseExpr, Column
from snowflake.snowpark.internal.sp_expressions import (
    AggregateFunction as SPAggregateFunction,
    Avg as SPAverage,
    CaseWhen as SPCaseWhen,
    Count as SPCount,
    Expression as SPExpression,
    Literal as SPLiteral,
    Max as SPMax,
    Min as SPMin,
    Star as SPStar,
    Sum as SPSum,
    UnresolvedFunction as SPUnresolvedFunction,
)
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import DataType, StringType
from snowflake.snowpark.types.sp_data_types import IntegerType as SPIntegerType


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


def count_distinct(col: Column, *columns: Column) -> Column:
    """Returns either the number of non-NULL distinct records for the specified columns,
    or the total number of the distinct records.
    """
    cols = [col]
    cols.extend(columns)
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
    """Return the average for the specific numeric columns. Alias of :obj:`avg`"""
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
    If all records inside a group are NULL, returns NULL. Alias of :obj:`stddev`"""
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


def to_binary(s: Column, fmt: Optional[str] = None) -> Column:
    """Converts the input expression to a binary value. For NULL input, the output is
    NULL"""
    return builtin("to_binary")(s, fmt) if fmt else builtin("to_binary")(s)


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


def when(condition: Column, value: Column) -> CaseExpr:
    """Works like a cascading if-then-else statement.
    A series of conditions are evaluated in sequence.
    When a condition evaluates to TRUE, the evaluation stops and the associated
    result (after THEN) is returned. If none of the conditions evaluate to TRUE,
    then the result after the optional OTHERWISE is returned, if present;
    otherwise NULL is returned.
    """
    return CaseExpr(SPCaseWhen([(condition.expression, value.expression)]))


def udf(
    func: Optional[Callable] = None,
    *,
    return_type: DataType = StringType(),
    input_types: Optional[List[DataType]] = None,
    name: Optional[str] = None,
) -> Callable:
    """Registers a Python function as a Snowflake Python UDF and returns the UDF.

    Args:
        func: A Python function used for creating the UDF.
        return_type: A :class:`sf_types.DataType` representing the return data
            type of the UDF.
        input_types: A list of :class:`sf_types.DataType` representing the input
            data types of the UDF.
        name: The name to use for the UDF in Snowflake. If not provided, the name of
            the UDF will be generated automatically.

    Returns:
        A UDF function that can be called with Column expressions (:class:`Column` or :class:`str`)

    Examples::

        from snowflake.snowpark.types.sf_types import IntegerType
        add_one = udf(lambda x: x+1, return_types=IntegerType(), input_types=[IntegerType()])

        @udf(return_types=IntegerType(), input_types=[IntegerType()], name="minus_one")
        def minus_one(x):
            return x-1

        df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
        df.select(add_one("a"), minus_one("b"))
        session.sql("select minus_one(1)")

    Note:
        ``return_type``, ``input_types`` and ``name`` must be passed with keyword arguments.
    """
    from snowflake.snowpark.session import Session

    session = Session._get_active_session()
    if not session:
        raise SnowparkClientException("No default SnowflakeSession found")

    if func is None:
        return functools.partial(
            session.udf.register,
            return_type=return_type,
            input_types=input_types,
            name=name,
        )
    else:
        return session.udf.register(func, return_type, input_types, name)


def __with_aggregate_function(
    func: SPAggregateFunction, is_distinct: bool = False
) -> Column:
    return Column(func.to_aggregate_expression(is_distinct))


def call_udf(
    udf_name: str,
    *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
) -> Column:
    """Calls a user-defined function (UDF) by name.

    Args:
        udf_name: The name of UDF in Snowflake.
        cols: Columns that the UDF will be applied to, as :class:`str`, :class:`Column`
            or a list of those.

    Returns:
        :class:`Column`.

    Example::

        df.select(call_udf("add", col("a"), col("b")))
    """

    Utils.validate_object_name(udf_name)
    exprs = Utils.parse_positional_args_to_list(*cols)
    return Column(
        SPUnresolvedFunction(
            udf_name,
            [
                e.expression if type(e) == Column else Column(e).expression
                for e in exprs
            ],
            is_distinct=False,
        )
    )


def call_builtin(function_name: str, *args: Any) -> Column:
    """Invokes a Snowflake `system-defined function <https://docs.snowflake.com/en/sql-reference-functions.html>`_ (built-in function) with the specified name
    and arguments.

    Args:
        function_name: The name of built-in function in Snowflake
        args: Arguments can be two types:
            a. :class:`Column`, or
            b. Basic Python types such as int, float, str, which are converted to Snowpark literals.

    Returns:
        :class:`Column`.

    Example::

        df.select(call_builtin("avg", col("a")))
    """

    sp_expressions = []
    for arg in Utils.parse_positional_args_to_list(*args):
        if type(arg) == Column:
            sp_expressions.append(arg.expression)
        elif isinstance(arg, SPExpression):
            sp_expressions.append(arg)
        else:
            sp_expressions.append(SPLiteral.create(arg))

    return Column(
        SPUnresolvedFunction(function_name, sp_expressions, is_distinct=False)
    )


def builtin(function_name: str) -> Callable:
    """
    Function object to invoke a Snowflake `system-defined function <https://docs.snowflake.com/en/sql-reference-functions.html>`_ (built-in function). Use this to invoke
    any built-in functions not explicitly listed in this object.

    Args:
        function_name: The name of built-in function in Snowflake.

    Returns:
        A :class:`Callable` object for calling a Snowflake system-defined function.

    Example::

        avg = functions.builtin('avg')
        df.select(avg(col("col_1")))
    """
    return lambda *args: call_builtin(function_name, *args)
