#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
"""
Provides utility functions that generate :class:`Column` expressions that you can pass
to :class:`DataFrame` transformation methods. These functions generate references to
columns, literals, and SQL expressions (e.g. "c + 1").

This object also provides functions that correspond to Snowflake
`system-defined functions <https://docs.snowflake.com/en/sql-reference-functions.html>`_
(built-in functions), including functions for aggregation and window functions.

The following examples demonstrate the use of some of these functions::

    # Use columns and literals in expressions.
    df.select(col("c") + lit(1))

    # Call system-defined (built-in) functions.
    # This example calls the function that corresponds to the TO_DATE() SQL function.
    df.select(to_date(col("d")))

    # Call system-defined functions that have no corresponding function in the functions
    # object. This example calls the RADIANS() SQL function, passing in values from the
    # column "e".
    df.select(callBuiltin("radians", col("e")))

    # Call a user-defined function (UDF) by name.
    df.select(callUDF("some_func", col("c")))

    # Evaluate an SQL expression
    df.select(sqlExpr("c + 1"))
"""
import functools
from typing import Any, Callable, List, Optional, Tuple, Union

from snowflake.snowpark.column import CaseExpr, Column
from snowflake.snowpark.internal.sp_expressions import (
    AggregateFunction as SPAggregateFunction,
    Avg as SPAverage,
    CaseWhen as SPCaseWhen,
    Count as SPCount,
    Expression as SPExpression,
    IsNaN as SPIsNan,
    IsNull as SPIsNull,
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
    """Returns the :class:`Column` with the specified name."""
    return Column(col_name)


def column(col_name: str) -> Column:
    """Returns a :class:`Column` with the specified name. Alias for col."""
    return Column(col_name)


def lit(literal) -> Column:
    """Creates a :class:`Column` expression for a literal value."""
    return typedLit(literal)


def typedLit(literal) -> Column:
    """Creates a :class:`Column` expression for a literal value."""
    if type(literal) == Column:
        return literal
    else:
        return Column(SPLiteral.create(literal))


def sql_expr(sql: str) -> Column:
    """Creates a :class:`Column` expression from raw SQL text.
    Note that the function does not interpret or check the SQL text."""
    return Column.expr(sql)


def avg(e: Union[Column, str]) -> Column:
    """Returns the average of non-NULL records. If all records inside a group are NULL,
    the function returns NULL."""
    c = __to_col_if_str(e, "avg")
    return __with_aggregate_function(SPAverage(c.expression))


def count(e: Union[Column, str]) -> Column:
    """Returns either the number of non-NULL records for the specified columns, or the
    total number of records."""
    c = __to_col_if_str(e, "count")
    exp = (
        SPCount(SPLiteral(1, SPIntegerType()))
        if isinstance(c, SPStar)
        else SPCount(c.expression)
    )
    return __with_aggregate_function(exp)


def count_distinct(col: Union[Column, str], *columns: Union[Column, str]) -> Column:
    """Returns either the number of non-NULL distinct records for the specified columns,
    or the total number of the distinct records.
    """
    cols = [__to_col_if_str(col, "count_distinct")]
    cols.extend([__to_col_if_str(c, "count_distinct") for c in columns])
    return Column(
        SPUnresolvedFunction("count", [c.expression for c in cols], is_distinct=True)
    )


def kurtosis(e: Union[Column, str]) -> Column:
    """Returns the population excess kurtosis of non-NULL records. If all records
    inside a group are NULL, the function returns NULL."""
    c = __to_col_if_str(e, "kurtosis")
    return builtin("kurtosis")(c)


def max(e: Union[Column, str]) -> Column:
    """Returns the maximum value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned."""
    c = __to_col_if_str(e, "max")
    return __with_aggregate_function(SPMax(c.expression))


def mean(e: Union[Column, str]) -> Column:
    """Return the average for the specific numeric columns. Alias of :func:`avg`."""
    c = __to_col_if_str(e, "mean")
    return avg(c)


def median(e: Union[Column, str]) -> Column:
    """Returns the median value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned."""
    c = __to_col_if_str(e, "median")
    return builtin("median")(c)


def min(e: Union[Column, str]) -> Column:
    """Returns the minimum value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned."""
    c = __to_col_if_str(e, "min")
    return __with_aggregate_function(SPMin(c.expression))


def skew(e: Union[Column, str]) -> Column:
    """Returns the sample skewness of non-NULL records. If all records inside a group
    are NULL, the function returns NULL."""
    c = __to_col_if_str(e, "skew")
    return builtin("skew")(c)


def stddev(e: Union[Column, str]) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of
    non-NULL values. If all records inside a group are NULL, returns NULL."""
    c = __to_col_if_str(e, "stddev")
    return builtin("stddev")(c)


def stddev_samp(e: Union[Column, str]) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of
    non-NULL values. If all records inside a group are NULL, returns NULL. Alias of
    :func:`stddev`."""
    c = __to_col_if_str(e, "stddev_samp")
    return builtin("stddev_samp")(c)


def stddev_pop(e: Union[Column, str]) -> Column:
    """Returns the population standard deviation (square root of variance) of non-NULL
    values. If all records inside a group are NULL, returns NULL."""
    c = __to_col_if_str(e, "stddev_pop")
    return builtin("stddev_pop")(c)


def sum(e: Union[Column, str]) -> Column:
    """Returns the sum of non-NULL records in a group. You can use the DISTINCT keyword
    to compute the sum of unique non-null values. If all records inside a group are
    NULL, the function returns NULL."""
    c = __to_col_if_str(e, "sum")
    return __with_aggregate_function(SPSum(c.expression))


def sum_distinct(e: Union[Column, str]) -> Column:
    """Returns the sum of non-NULL distinct records in a group. You can use the
    DISTINCT keyword to compute the sum of unique non-null values. If all records
    inside a group are NULL, the function returns NULL."""
    c = __to_col_if_str(e, "sum_distinct")
    return __with_aggregate_function(SPSum(c.expression), is_distinct=True)


def variance(e: Union[Column, str]) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned."""
    c = __to_col_if_str(e, "variance")
    return builtin("variance")(c)


def var_samp(e: Union[Column, str]) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. Alias of :func:`variance`"""
    c = __to_col_if_str(e, "var_samp")
    return variance(e)


def var_pop(e: Union[Column, str]) -> Column:
    """Returns the population variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned."""
    c = __to_col_if_str(e, "var_pop")
    return builtin("var_pop")(c)


def coalesce(*e: Union[Column, str]) -> Column:
    """Returns the first non-NULL expression among its arguments, or NULL if all its
    arguments are NULL."""
    c = [__to_col_if_str(ex, "coalesce") for ex in e]
    return builtin("coalesce")(*c)


def equal_nan(e: Union[Column, str]) -> Column:
    """Return true if the value in the column is not a number (NaN)."""
    c = __to_col_if_str(e, "equal_nan")
    return Column(SPIsNan(c.expression))


def is_null(e: Union[Column, str]) -> Column:
    """Return true if the value in the column is null."""
    c = __to_col_if_str(e, "is_null")
    return Column(SPIsNull(c.expression))


def parse_json(e: Union[Column, str]) -> Column:
    """Parse the value of the specified column as a JSON string and returns the
    resulting JSON document."""
    c = __to_col_if_str(e, "parse_json")
    return builtin("parse_json")(c)


def to_decimal(e: Union[Column, str], precision: int, scale: int) -> Column:
    """Converts an input expression to a decimal."""
    c = __to_col_if_str(e, "to_decimal")
    return builtin("to_decimal")(c, sql_expr(str(precision)), sql_expr(str(scale)))


def to_time(e: Union[Column, str], fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding time."""
    c = __to_col_if_str(e, "to_time")
    return builtin("to_time")(c, fmt) if fmt else builtin("to_time")(c)


def to_timestamp(e: Union[Column, str], fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding timestamp."""
    c = __to_col_if_str(e, "to_timestamp")
    return builtin("to_timestamp")(c, fmt) if fmt else builtin("to_timestamp")(c)


def to_binary(e: Union[Column, str], fmt: Optional[str] = None) -> Column:
    """Converts the input expression to a binary value. For NULL input, the output is
    NULL."""
    c = __to_col_if_str(e, "to_binary")
    return builtin("to_binary")(c, fmt) if fmt else builtin("to_binary")(c)


def to_date(e: Union[Column, str], fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into a date."""
    c = __to_col_if_str(e, "to_date")
    return builtin("to_date")(c, fmt) if fmt else builtin("to_date")(c)


def to_array(e: Union[Column, str]) -> Column:
    """Converts any value to an ARRAY value or NULL (if input is NULL)."""
    c = __to_col_if_str(e, "to_array")
    return builtin("to_array")(c)


def to_variant(e: Union[Column, str]) -> Column:
    """Converts any value to a VARIANT value or NULL (if input is NULL)."""
    c = __to_col_if_str(e, "to_variant")
    return builtin("to_variant")(c)


def to_object(e: Union[Column, str]) -> Column:
    """Converts any value to a OBJECT value or NULL (if input is NULL)."""
    c = __to_col_if_str(e, "to_object")
    return builtin("to_object")(c)


def array_agg(e: Union[Column, str]) -> Column:
    c = __to_col_if_str(e, "array_agg")
    return builtin("array_agg")(c)


def iff(condition: Column, e1: Union[Column, str], e2: Union[Column, str]) -> Column:
    c1 = __to_col_if_str(e1, "iff")
    c2 = __to_col_if_str(e2, "iff")
    return builtin("iff")(condition, c1, c2)


def mode(e: Union[Column, str]) -> Column:
    c = __to_col_if_str(e, "mode")
    return builtin("mode")(c)


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
    return_type: Optional[DataType] = None,
    input_types: Optional[List[DataType]] = None,
    name: Optional[str] = None,
) -> Callable:
    """Registers a Python function as a Snowflake Python UDF and returns the UDF.

    Args:
        func: A Python function used for creating the UDF.
        return_type: A :class:`sf_types.DataType` representing the return data
            type of the UDF. Optional if type hints are provided.
        input_types: A list of :class:`sf_types.DataType` representing the input
            data types of the UDF. Optional if type hints are provided.
        name: The name to use for the UDF in Snowflake, which allows to call this UDF
            in a SQL command or via :func:`call_udf()`. If it is not provided,
            a random name will be generated automatically for the UDF.

    Returns:
        A UDF function that can be called with Column expressions (:class:`Column` or :class:`str`)

    Examples::

        from snowflake.snowpark.types.sf_types import IntegerType
        add_one = udf(lambda x: x+1, return_types=IntegerType(), input_types=[IntegerType()])

        @udf(name="minus_one")
        def minus_one(x: int) -> int:
            return x-1

        df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
        df.select(add_one("a"), minus_one("b"))
        session.sql("select minus_one(1)")

    Note:
        When type hints are provided and are complete for a function, ``return_type`` and
        ``input_types`` are optional and will be ignored.
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
        cols: Columns that the UDF will be applied to, as :class:`str`,
            :class:`Column` or a list of those.

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


def __to_col_if_str(e: Union[Column, str], func_name: str):
    if isinstance(e, Column):
        return e
    elif isinstance(e, str):
        return col(e)
    else:
        raise TypeError(f"{func_name.upper()} expected Column or str, got: {type(e)}")
