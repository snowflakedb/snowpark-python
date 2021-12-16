#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
"""
Provides utility functions that generate :class:`~snowflake.snowpark.Column` expressions that you can pass
to :class:`~snowflake.snowpark.DataFrame` transformation methods. These functions generate references to
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
    df.select(call_builtin("radians", col("e")))

    # Call a user-defined function (UDF) by name.
    df.select(call_udf("some_func", col("c")))

    # Evaluate a SQL expression
    df.select(sql_expr("c + 1"))
"""
import functools
from random import randint
from typing import Callable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.sp_expressions import (
    AggregateFunction as SPAggregateFunction,
    ArrayIntersect as SPArrayIntersect,
    ArraysOverlap as SPArraysOverlap,
    Avg as SPAverage,
    CaseWhen as SPCaseWhen,
    Count as SPCount,
    FunctionExpression as SPFunctionExpression,
    IsNaN as SPIsNan,
    IsNull as SPIsNull,
    Literal as SPLiteral,
    Max as SPMax,
    Min as SPMin,
    MultipleExpression as SPMultipleExpression,
    NamedArgumentsTableFunction as SPNamedArgumentsTableFunction,
    Star as SPStar,
    Sum as SPSum,
    TableFunction as SPTableFunction,
    TableFunctionExpression as SPTableFunctionExpression,
)
from snowflake.snowpark._internal.sp_types.types_package import (
    ColumnOrLiteral,
    ColumnOrName,
    LiteralType,
)
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.column import CaseExpr, Column, _to_col_if_lit
from snowflake.snowpark.types import DataType
from snowflake.snowpark.udf import UserDefinedFunction


def col(col_name: str) -> Column:
    """Returns the :class:`~snowflake.snowpark.Column` with the specified name."""
    return Column(col_name)


def column(col_name: str) -> Column:
    """Returns a :class:`~snowflake.snowpark.Column` with the specified name. Alias for col."""
    return Column(col_name)


def lit(literal: LiteralType) -> Column:
    """
    Creates a :class:`~snowflake.snowpark.Column` expression for a literal value.
    It only supports basic Python data types, such as: ``int``, ``float``, ``str``,
    ``bool``, ``bytes``, ``bytearray``, ``datetime.time``, ``datetime.date``,
    ``datetime.datetime``, ``decimal.Decimal``. Structured data types,
    such as: ``list``, ``tuple``, ``dict`` are not supported.
    """
    return literal if isinstance(literal, Column) else Column(SPLiteral(literal))


def sql_expr(sql: str) -> Column:
    """Creates a :class:`~snowflake.snowpark.Column` expression from raw SQL text.
    Note that the function does not interpret or check the SQL text."""
    return Column._expr(sql)


def approx_count_distinct(e: ColumnOrName) -> Column:
    """Returns the average of non-NULL records. If all records inside a group are NULL,
    the function returns NULL."""
    c = _to_col_if_str(e, "approx_count_distinct")
    return builtin("approx_count_distinct")(c)


def avg(e: ColumnOrName) -> Column:
    """Returns the average of non-NULL records. If all records inside a group are NULL,
    the function returns NULL."""
    c = _to_col_if_str(e, "avg")
    return __with_aggregate_function(SPAverage(c.expression))


def corr(column1: ColumnOrName, column2: ColumnOrName) -> Column:
    """Returns the correlation coefficient for non-null pairs in a group."""
    c1 = _to_col_if_str(column1, "corr")
    c2 = _to_col_if_str(column2, "corr")
    return builtin("corr")(c1, c2)


def count(e: ColumnOrName) -> Column:
    """Returns either the number of non-NULL records for the specified columns, or the
    total number of records."""
    c = _to_col_if_str(e, "count")
    exp = (
        SPCount(SPLiteral(1))
        if isinstance(c.expression, SPStar)
        else SPCount(c.expression)
    )
    return __with_aggregate_function(exp)


def count_distinct(*cols: ColumnOrName) -> Column:
    """Returns either the number of non-NULL distinct records for the specified columns,
    or the total number of the distinct records.
    """
    cs = [_to_col_if_str(c, "count_distinct") for c in cols]
    return Column(
        SPFunctionExpression("count", [c.expression for c in cs], is_distinct=True)
    )


def covar_pop(column1: ColumnOrName, column2: ColumnOrName) -> Column:
    """Returns the population covariance for non-null pairs in a group."""
    col1 = _to_col_if_str(column1, "covar_pop")
    col2 = _to_col_if_str(column2, "covar_pop")
    return builtin("covar_pop")(col1, col2)


def covar_samp(column1: ColumnOrName, column2: ColumnOrName) -> Column:
    """Returns the sample covariance for non-null pairs in a group."""
    col1 = _to_col_if_str(column1, "covar_samp")
    col2 = _to_col_if_str(column2, "covar_samp")
    return builtin("covar_samp")(col1, col2)


def kurtosis(e: ColumnOrName) -> Column:
    """Returns the population excess kurtosis of non-NULL records. If all records
    inside a group are NULL, the function returns NULL."""
    c = _to_col_if_str(e, "kurtosis")
    return builtin("kurtosis")(c)


def max(e: ColumnOrName) -> Column:
    """Returns the maximum value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned."""
    c = _to_col_if_str(e, "max")
    return __with_aggregate_function(SPMax(c.expression))


def mean(e: ColumnOrName) -> Column:
    """Return the average for the specific numeric columns. Alias of :func:`avg`."""
    c = _to_col_if_str(e, "mean")
    return avg(c)


def median(e: ColumnOrName) -> Column:
    """Returns the median value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned."""
    c = _to_col_if_str(e, "median")
    return builtin("median")(c)


def min(e: ColumnOrName) -> Column:
    """Returns the minimum value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned."""
    c = _to_col_if_str(e, "min")
    return __with_aggregate_function(SPMin(c.expression))


def skew(e: ColumnOrName) -> Column:
    """Returns the sample skewness of non-NULL records. If all records inside a group
    are NULL, the function returns NULL."""
    c = _to_col_if_str(e, "skew")
    return builtin("skew")(c)


def stddev(e: ColumnOrName) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of
    non-NULL values. If all records inside a group are NULL, returns NULL."""
    c = _to_col_if_str(e, "stddev")
    return builtin("stddev")(c)


def stddev_samp(e: ColumnOrName) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of
    non-NULL values. If all records inside a group are NULL, returns NULL. Alias of
    :func:`stddev`."""
    c = _to_col_if_str(e, "stddev_samp")
    return builtin("stddev_samp")(c)


def stddev_pop(e: ColumnOrName) -> Column:
    """Returns the population standard deviation (square root of variance) of non-NULL
    values. If all records inside a group are NULL, returns NULL."""
    c = _to_col_if_str(e, "stddev_pop")
    return builtin("stddev_pop")(c)


def sum(e: ColumnOrName) -> Column:
    """Returns the sum of non-NULL records in a group. You can use the DISTINCT keyword
    to compute the sum of unique non-null values. If all records inside a group are
    NULL, the function returns NULL."""
    c = _to_col_if_str(e, "sum")
    return __with_aggregate_function(SPSum(c.expression))


def sum_distinct(e: ColumnOrName) -> Column:
    """Returns the sum of non-NULL distinct records in a group. You can use the
    DISTINCT keyword to compute the sum of unique non-null values. If all records
    inside a group are NULL, the function returns NULL."""
    c = _to_col_if_str(e, "sum_distinct")
    return __with_aggregate_function(SPSum(c.expression), is_distinct=True)


def variance(e: ColumnOrName) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned."""
    c = _to_col_if_str(e, "variance")
    return builtin("variance")(c)


def var_samp(e: ColumnOrName) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. Alias of :func:`variance`"""
    c = _to_col_if_str(e, "var_samp")
    return variance(e)


def var_pop(e: ColumnOrName) -> Column:
    """Returns the population variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned."""
    c = _to_col_if_str(e, "var_pop")
    return builtin("var_pop")(c)


def approx_percentile(col: ColumnOrName, percentile: float) -> Column:
    """Returns an approximated value for the desired percentile. This function uses the t-Digest algorithm."""
    c = _to_col_if_str(col, "approx_percentile")
    return builtin("approx_percentile")(c, sql_expr(str(percentile)))


def approx_percentile_accumulate(col: ColumnOrName) -> Column:
    """Returns the internal representation of the t-Digest state (as a JSON object) at the end of aggregation.
    This function uses the t-Digest algorithm.
    """
    c = _to_col_if_str(col, "approx_percentile_accumulate")
    return builtin("approx_percentile_accumulate")(c)


def approx_percentile_estimate(state: ColumnOrName, percentile: float) -> Column:
    """Returns the desired approximated percentile value for the specified t-Digest state.
    APPROX_PERCENTILE_ESTIMATE(APPROX_PERCENTILE_ACCUMULATE(.)) is equivalent to
    APPROX_PERCENTILE(.).
    """
    c = _to_col_if_str(state, "approx_percentile_estimate")
    return builtin("approx_percentile_estimate")(c, sql_expr(str(percentile)))


def approx_percentile_combine(state: ColumnOrName) -> Column:
    """Combines (merges) percentile input states into a single output state.
    This allows scenarios where APPROX_PERCENTILE_ACCUMULATE is run over horizontal partitions
    of the same table, producing an algorithm state for each table partition. These states can
    later be combined using APPROX_PERCENTILE_COMBINE, producing the same output state as a
    single run of APPROX_PERCENTILE_ACCUMULATE over the entire table.
    """
    c = _to_col_if_str(state, "approx_percentile_combine")
    return builtin("approx_percentile_combine")(c)


def coalesce(*e: ColumnOrName) -> Column:
    """Returns the first non-NULL expression among its arguments, or NULL if all its
    arguments are NULL."""
    c = [_to_col_if_str(ex, "coalesce") for ex in e]
    return builtin("coalesce")(*c)


def equal_nan(e: ColumnOrName) -> Column:
    """Return true if the value in the column is not a number (NaN)."""
    c = _to_col_if_str(e, "equal_nan")
    return Column(SPIsNan(c.expression))


def is_null(e: ColumnOrName) -> Column:
    """Return true if the value in the column is null."""
    c = _to_col_if_str(e, "is_null")
    return Column(SPIsNull(c.expression))


def negate(e: ColumnOrName) -> Column:
    """Returns the negation of the value in the column (equivalent to a unary minus)."""
    c = _to_col_if_str(e, "negate")
    return -c


def not_(e: ColumnOrName) -> Column:
    """Returns the inverse of a boolean expression."""
    c = _to_col_if_str(e, "not_")
    return ~c


def random(seed: Optional[int] = None) -> Column:
    """Each call returns a pseudo-random 64-bit integer."""
    s = seed if seed is not None else randint(-(2 ** 63), 2 ** 63 - 1)
    return builtin("random")(SPLiteral(s))


def to_decimal(e: ColumnOrName, precision: int, scale: int) -> Column:
    """Converts an input expression to a decimal."""
    c = _to_col_if_str(e, "to_decimal")
    return builtin("to_decimal")(c, sql_expr(str(precision)), sql_expr(str(scale)))


def sqrt(e: ColumnOrName) -> Column:
    """Returns the square-root of a non-negative numeric expression."""
    c = _to_col_if_str(e, "sqrt")
    return builtin("sqrt")(c)


def abs(e: ColumnOrName) -> Column:
    """Returns the absolute value of a numeric expression."""
    c = _to_col_if_str(e, "abs")
    return builtin("abs")(c)


def ceil(e: ColumnOrName) -> Column:
    """Returns values from the specified column rounded to the nearest equal or larger
    integer."""
    c = _to_col_if_str(e, "ceil")
    return builtin("ceil")(c)


def floor(e: ColumnOrName) -> Column:
    """Returns values from the specified column rounded to the nearest equal or
    smaller integer."""
    c = _to_col_if_str(e, "floor")
    return builtin("floor")(c)


def exp(e: ColumnOrName) -> Column:
    """Computes Euler's number e raised to a floating-point value."""
    c = _to_col_if_str(e, "exp")
    return builtin("exp")(c)


def log(
    base: Union[ColumnOrName, int, float], x: Union[ColumnOrName, int, float]
) -> Column:
    """Returns the logarithm of a numeric expression."""
    b = lit(base) if isinstance(base, (int, float)) else _to_col_if_str(base, "log")
    arg = lit(x) if isinstance(x, (int, float)) else _to_col_if_str(x, "log")
    return builtin("log")(b, arg)


def pow(
    l: Union[ColumnOrName, int, float], r: Union[ColumnOrName, int, float]
) -> Column:
    """Returns a number (l) raised to the specified power (r)."""
    number = lit(l) if isinstance(l, (int, float)) else _to_col_if_str(l, "pow")
    power = lit(r) if isinstance(r, (int, float)) else _to_col_if_str(r, "pow")
    return builtin("pow")(number, power)


def split(
    str: ColumnOrName,
    pattern: ColumnOrName,
) -> Column:
    """Splits a given string with a given separator and returns the result in an array
    of strings. To specify a string separator, use the :func:`lit()` function."""
    s = _to_col_if_str(str, "split")
    p = _to_col_if_str(pattern, "split")
    return builtin("split")(s, p)


def substring(
    str: ColumnOrName, pos: Union[Column, int], len: Union[Column, int]
) -> Column:
    """Returns the portion of the string or binary value str, starting from the
    character/byte specified by pos, with limited length. The length should be greater
    than or equal to zero. If the length is a negative number, the function returns an
    empty string."""
    s = _to_col_if_str(str, "substring")
    p = pos if isinstance(pos, Column) else lit(pos)
    l = len if isinstance(len, Column) else lit(len)
    return builtin("substring")(s, p, l)


def translate(
    src: ColumnOrName,
    matching_string: ColumnOrName,
    replace_string: ColumnOrName,
) -> Column:
    """Translates src from the characters in matchingString to the characters in
    replaceString."""
    source = _to_col_if_str(src, "translate")
    match = _to_col_if_str(matching_string, "translate")
    replace = _to_col_if_str(replace_string, "translate")
    return builtin("translate")(source, match, replace)


def trim(e: ColumnOrName, trim_string: ColumnOrName) -> Column:
    """Removes leading and trailing characters from a string."""
    c = _to_col_if_str(e, "trim")
    t = _to_col_if_str(trim_string, "trim")
    return builtin("trim")(c, t)


def upper(e: ColumnOrName) -> Column:
    """Returns the input string with all characters converted to uppercase."""
    c = _to_col_if_str(e, "upper")
    return builtin("upper")(c)


def contains(col: ColumnOrName, string: ColumnOrName) -> Column:
    """Returns true if col contains str."""
    c = _to_col_if_str(col, "contains")
    s = _to_col_if_str(string, "contains")
    return builtin("contains")(c, s)


def startswith(col: ColumnOrName, str: ColumnOrName) -> Column:
    """Returns true if col starts with str."""
    c = _to_col_if_str(col, "startswith")
    s = _to_col_if_str(str, "startswith")
    return builtin("startswith")(c, s)


def char(col: ColumnOrName) -> Column:
    """Converts a Unicode code point (including 7-bit ASCII) into the character that
    matches the input Unicode."""
    c = _to_col_if_str(col, "char")
    return builtin("char")(c)


def to_time(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding time."""
    c = _to_col_if_str(e, "to_time")
    return builtin("to_time")(c, fmt) if fmt else builtin("to_time")(c)


def to_timestamp(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding timestamp."""
    c = _to_col_if_str(e, "to_timestamp")
    return builtin("to_timestamp")(c, fmt) if fmt else builtin("to_timestamp")(c)


def to_date(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into a date."""
    c = _to_col_if_str(e, "to_date")
    return builtin("to_date")(c, fmt) if fmt else builtin("to_date")(c)


def arrays_overlap(array1: ColumnOrName, array2: ColumnOrName) -> Column:
    """Compares whether two ARRAYs have at least one element in common. Returns TRUE
    if there is at least one element in common; otherwise returns FALSE. The function
    is NULL-safe, meaning it treats NULLs as known values for comparing equality."""
    a1 = _to_col_if_str(array1, "arrays_overlap")
    a2 = _to_col_if_str(array2, "arrays_overlap")
    return Column(SPArraysOverlap(a1.expression, a2.expression))


def array_intersection(array1: ColumnOrName, array2: ColumnOrName) -> Column:
    """Returns an array that contains the matching elements in the two input arrays.

    The function is NULL-safe, meaning it treats NULLs as known values for comparing equality.

    Args:
        array1: An ARRAY that contains elements to be compared.
        array2: An ARRAY that contains elements to be compared."""
    a1 = _to_col_if_str(array1, "array_intersection")
    a2 = _to_col_if_str(array2, "array_intersection")
    return Column(SPArrayIntersect(a1.expression, a2.expression))


def datediff(part: str, col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Calculates the difference between two date, time, or timestamp columns based on the date or time part requested.

    `Supported date and time parts <https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts>`_

    Example::

        # year difference between two date columns
        date.select(datediff("year", col("date_col1"), col("date_col2")))

    Args:
        part: The time part to use for calculating the difference
        col1: The first timestamp column or minuend in the datediff
        col2: The second timestamp column or the subtrahend in the datediff
    """
    if not isinstance(part, str):
        raise ValueError("part must be a string")
    c1 = _to_col_if_str(col1, "datediff")
    c2 = _to_col_if_str(col2, "datediff")
    return builtin("datediff")(part, c1, c2)


def dateadd(part: str, col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Adds the specified value for the specified date or time part to date or time expr.

    `Supported date and time parts <https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts>`_

    Example::

        # add one year on dates
        date.select(dateadd("year", lit(1), col("date_col")))

    Args:
        part: The time part to use for the addition
        col1: The first timestamp column or addend in the dateadd
        col2: The second timestamp column or the addend in the dateadd
    """
    if not isinstance(part, str):
        raise ValueError("part must be a string")
    c1 = _to_col_if_str(col1, "dateadd")
    c2 = _to_col_if_str(col2, "dateadd")
    return builtin("dateadd")(part, c1, c2)


def is_array(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains an ARRAY value."""
    c = _to_col_if_str(col, "is_array")
    return builtin("is_array")(c)


def is_boolean(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a boolean value."""
    c = _to_col_if_str(col, "is_boolean")
    return builtin("is_boolean")(c)


def is_binary(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a binary value."""
    c = _to_col_if_str(col, "is_binary")
    return builtin("is_binary")(c)


def is_char(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a string."""
    c = _to_col_if_str(col, "is_char")
    return builtin("is_char")(c)


def is_varchar(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a string."""
    c = _to_col_if_str(col, "is_varchar")
    return builtin("is_varchar")(c)


def is_date(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a date value."""
    c = _to_col_if_str(col, "is_date")
    return builtin("is_date")(c)


def is_date_value(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a date value."""
    c = _to_col_if_str(col, "is_date_value")
    return builtin("is_date_value")(c)


def is_decimal(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a fixed-point decimal value or integer."""
    c = _to_col_if_str(col, "is_decimal")
    return builtin("is_decimal")(c)


def is_double(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a floating-point value, fixed-point decimal, or integer."""
    c = _to_col_if_str(col, "is_double")
    return builtin("is_double")(c)


def is_real(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a floating-point value, fixed-point decimal, or integer."""
    c = _to_col_if_str(col, "is_real")
    return builtin("is_real")(c)


def is_integer(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a integer value."""
    c = _to_col_if_str(col, "is_integer")
    return builtin("is_integer")(c)


def is_null_value(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a JSON null value."""
    c = _to_col_if_str(col, "is_null_value")
    return builtin("is_null_value")(c)


def is_object(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains an OBJECT value."""
    c = _to_col_if_str(col, "is_object")
    return builtin("is_object")(c)


def is_time(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a TIME value."""
    c = _to_col_if_str(col, "is_time")
    return builtin("is_time")(c)


def is_timestamp_ltz(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a TIMESTAMP value to be interpreted using the local time
    zone."""
    c = _to_col_if_str(col, "is_timestamp_ltz")
    return builtin("is_timestamp_ltz")(c)


def is_timestamp_ntz(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a TIMESTAMP value with no time zone."""
    c = _to_col_if_str(col, "is_timestamp_ntz")
    return builtin("is_timestamp_ntz")(c)


def is_timestamp_tz(col: ColumnOrName) -> Column:
    """Returns true if the specified VARIANT column contains a TIMESTAMP value with a time zone."""
    c = _to_col_if_str(col, "is_timestamp_tz")
    return builtin("is_timestamp_tz")(c)


def typeof(col: ColumnOrName) -> Column:
    """Reports the type of a value stored in a VARIANT column. The type is returned as a string."""
    c = _to_col_if_str(col, "typeof")
    return builtin("typeof")(c)


def check_json(col: ColumnOrName) -> Column:
    """Checks the validity of a JSON document.
    If the input string is a valid JSON document or a NULL (i.e. no error would occur when
    parsing the input string), the function returns NULL.
    In case of a JSON parsing error, the function returns a string that contains the error
    message."""
    c = _to_col_if_str(col, "check_json")
    return builtin("check_json")(c)


def check_xml(col: ColumnOrName) -> Column:
    """Checks the validity of an XML document.
    If the input string is a valid XML document or a NULL (i.e. no error would occur when parsing
    the input string), the function returns NULL.
    In case of an XML parsing error, the output string contains the error message."""
    c = _to_col_if_str(col, "check_xml")
    return builtin("check_xml")(c)


def json_extract_path_text(col: ColumnOrName, path: ColumnOrName) -> Column:
    """Parses a JSON string and returns the value of an element at a specified path in the resulting
    JSON document."""
    c = _to_col_if_str(col, "json_extract_path_text")
    p = _to_col_if_str(path, "json_extract_path_text")
    return builtin("json_extract_path_text")(c, p)


def parse_json(e: ColumnOrName) -> Column:
    """Parse the value of the specified column as a JSON string and returns the
    resulting JSON document."""
    c = _to_col_if_str(e, "parse_json")
    return builtin("parse_json")(c)


def parse_xml(e: ColumnOrName) -> Column:
    """Parse the value of the specified column as a JSON string and returns the
    resulting XML document."""
    c = _to_col_if_str(e, "parse_xml")
    return builtin("parse_xml")(c)


def strip_null_value(col: ColumnOrName) -> Column:
    """Converts a JSON "null" value in the specified column to a SQL NULL value.
    All other VARIANT values in the column are returned unchanged."""
    c = _to_col_if_str(col, "strip_null_value")
    return builtin("strip_null_value")(c)


def array_agg(col: ColumnOrName) -> Column:
    """Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
    ARRAY is returned."""
    c = _to_col_if_str(col, "array_agg")
    return builtin("array_agg")(c)


def array_append(array: ColumnOrName, element: ColumnOrName) -> Column:
    """Returns an ARRAY containing all elements from the source ARRAY as well as the new element.
    The new element is located at end of the ARRAY.

    Args:
        array: The column containing the source ARRAY.
        element: The column containing the element to be appended. The element may be of almost
            any data type. The data type does not need to match the data type(s) of the
            existing elements in the ARRAY."""
    a = _to_col_if_str(array, "array_append")
    e = _to_col_if_str(element, "array_append")
    return builtin("array_append")(a, e)


def array_cat(array1: ColumnOrName, array2: ColumnOrName) -> Column:
    """Returns the concatenation of two ARRAYs.

    Args:
        array1: Column containing the source ARRAY.
        array2: Column containing the ARRAY to be appended to array1."""
    a1 = _to_col_if_str(array1, "array_cat")
    a2 = _to_col_if_str(array2, "array_cat")
    return builtin("array_cat")(a1, a2)


def array_compact(array: ColumnOrName) -> Column:
    """Returns a compacted ARRAY with missing and null values removed,
    effectively converting sparse arrays into dense arrays.

    Args:
        array: Column containing the source ARRAY to be compacted
    """
    a = _to_col_if_str(array, "array_compact")
    return builtin("array_compact")(a)


def array_construct(*cols: ColumnOrName) -> Column:
    """Returns an ARRAY constructed from zero, one, or more inputs.

    Args:
        cols: Columns containing the values (or expressions that evaluate to values). The
            values do not all need to be of the same data type."""
    cs = [_to_col_if_str(c, "array_construct") for c in cols]
    return builtin("array_construct")(*cs)


def array_construct_compact(*cols: ColumnOrName) -> Column:
    """Returns an ARRAY constructed from zero, one, or more inputs.
    The constructed ARRAY omits any NULL input values.

    Args:
        cols: Columns containing the values (or expressions that evaluate to values). The
            values do not all need to be of the same data type.
    """
    cs = [_to_col_if_str(c, "array_construct_compact") for c in cols]
    return builtin("array_construct_compact")(*cs)


def array_contains(variant: ColumnOrName, array: ColumnOrName) -> Column:
    """Returns True if the specified VARIANT is found in the specified ARRAY.

    Args:
        variant: Column containing the VARIANT to find.
        array: Column containing the ARRAY to search."""
    v = _to_col_if_str(variant, "array_contains")
    a = _to_col_if_str(array, "array_contains")
    return builtin("array_contains")(v, a)


def array_insert(
    array: ColumnOrName, pos: ColumnOrName, element: ColumnOrName
) -> Column:
    """Returns an ARRAY containing all elements from the source ARRAY as well as the new element.

    Args:
        array: Column containing the source ARRAY.
        pos: Column containing a (zero-based) position in the source ARRAY.
            The new element is inserted at this position. The original element from this
            position (if any) and all subsequent elements (if any) are shifted by one position
            to the right in the resulting array (i.e. inserting at position 0 has the same
            effect as using array_prepend).
            A negative position is interpreted as an index from the back of the array (e.g.
            -1 results in insertion before the last element in the array).
        element: Column containing the element to be inserted. The new element is located at
            position pos. The relative order of the other elements from the source
            array is preserved."""
    a = _to_col_if_str(array, "array_insert")
    p = _to_col_if_str(pos, "array_insert")
    e = _to_col_if_str(element, "array_insert")
    return builtin("array_insert")(a, p, e)


def array_position(variant: ColumnOrName, array: ColumnOrName) -> Column:
    """Returns the index of the first occurrence of an element in an ARRAY.

    Args:
        variant: Column containing the VARIANT value that you want to find. The function
            searches for the first occurrence of this value in the array.
        array: Column containing the ARRAY to be searched."""
    v = _to_col_if_str(variant, "array_position")
    a = _to_col_if_str(array, "array_position")
    return builtin("array_position")(v, a)


def array_prepend(array: ColumnOrName, element: ColumnOrName) -> Column:
    """Returns an ARRAY containing the new element as well as all elements from the source ARRAY.
    The new element is positioned at the beginning of the ARRAY.

    Args:
        array Column containing the source ARRAY.
        element Column containing the element to be prepended."""
    a = _to_col_if_str(array, "array_prepend")
    e = _to_col_if_str(element, "array_prepend")
    return builtin("array_prepend")(a, e)


def array_size(array: ColumnOrName) -> Column:
    """Returns the size of the input ARRAY.

    If the specified column contains a VARIANT value that contains an ARRAY, the size of the ARRAY
    is returned; otherwise, NULL is returned if the value is not an ARRAY."""
    a = _to_col_if_str(array, "array_size")
    return builtin("array_size")(a)


def array_slice(array: ColumnOrName, from_: ColumnOrName, to: ColumnOrName) -> Column:
    """Returns an ARRAY constructed from a specified subset of elements of the input ARRAY.

    Args:
        array: Column containing the source ARRAY.
        from_: Column containing a position in the source ARRAY. The position of the first
            element is 0. Elements from positions less than this parameter are
            not included in the resulting ARRAY.
        to: Column containing a position in the source ARRAY. Elements from positions equal to
            or greater than this parameter are not included in the resulting array."""
    a = _to_col_if_str(array, "array_slice")
    f = _to_col_if_str(from_, "array_slice")
    t = _to_col_if_str(to, "array_slice")
    return builtin("array_slice")(a, f, t)


def array_to_string(array: ColumnOrName, separator: ColumnOrName) -> Column:
    """Returns an input ARRAY converted to a string by casting all values to strings (using
    TO_VARCHAR) and concatenating them (using the string from the second argument to separate
    the elements).

    Args:
        array: Column containing the ARRAY of elements to convert to a string.
        separator: Column containing the string to put between each element (e.g. a space,
            comma, or other human-readable separator)."""
    a = _to_col_if_str(array, "array_to_string")
    s = _to_col_if_str(separator, "array_to_string")
    return builtin("array_to_string")(a, s)


def object_agg(key: ColumnOrName, value: ColumnOrName) -> Column:
    """Returns one OBJECT per group. For each key-value input pair, where key must be a VARCHAR
    and value must be a VARIANT, the resulting OBJECT contains a key-value field."""
    k = _to_col_if_str(key, "object_agg")
    v = _to_col_if_str(value, "object_agg")
    return builtin("object_agg")(k, v)


def object_construct(*key_values: ColumnOrName) -> Column:
    """Returns an OBJECT constructed from the arguments."""
    kvs = [_to_col_if_str(kv, "object_construct") for kv in key_values]
    return builtin("object_construct")(*kvs)


def object_construct_keep_null(*key_values: ColumnOrName) -> Column:
    """Returns an object containing the contents of the input (i.e. source) object with one or more
    keys removed."""
    kvs = [_to_col_if_str(kv, "object_construct_keep_null") for kv in key_values]
    return builtin("object_construct_keep_null")(*kvs)


def object_delete(obj: ColumnOrName, key1: ColumnOrName, *keys: ColumnOrName) -> Column:
    """Returns an object consisting of the input object with a new key-value pair inserted.
    The input key must not exist in the object."""
    o = _to_col_if_str(obj, "object_delete")
    k1 = _to_col_if_str(key1, "object_delete")
    ks = [_to_col_if_str(k, "object_delete") for k in keys]
    return builtin("object_delete")(o, k1, *ks)


def object_insert(
    obj: ColumnOrName,
    key: ColumnOrName,
    value: ColumnOrName,
    update_flag: Optional[ColumnOrName] = None,
) -> Column:
    """Returns an object consisting of the input object with a new key-value pair inserted (or an
    existing key updated with a new value)."""
    o = _to_col_if_str(obj, "object_insert")
    k = _to_col_if_str(key, "object_insert")
    v = _to_col_if_str(value, "object_insert")
    uf = _to_col_if_str(update_flag, "update_flag") if update_flag else None
    if uf:
        return builtin("object_insert")(o, k, v, uf)
    else:
        return builtin("object_insert")(o, k, v)


def object_pick(obj: ColumnOrName, key1: ColumnOrName, *keys: ColumnOrName):
    """Returns a new OBJECT containing some of the key-value pairs from an existing object.

    To identify the key-value pairs to include in the new object, pass in the keys as arguments,
    or pass in an array containing the keys.

    If a specified key is not present in the input object, the key is ignored."""
    o = _to_col_if_str(obj, "object_pick")
    k1 = _to_col_if_str(key1, "object_pick")
    ks = [_to_col_if_str(k, "object_pick") for k in keys]
    return builtin("object_pick")(o, k1, *ks)


def as_array(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to an array."""
    c = _to_col_if_str(variant, "as_array")
    return builtin("as_array")(c)


def as_binary(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a binary string."""
    c = _to_col_if_str(variant, "as_binary")
    return builtin("as_binary")(c)


def as_char(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a string."""
    c = _to_col_if_str(variant, "as_char")
    return builtin("as_char")(c)


def as_varchar(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a string."""
    c = _to_col_if_str(variant, "as_varchar")
    return builtin("as_varchar")(c)


def as_date(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a date."""
    c = _to_col_if_str(variant, "as_date")
    return builtin("as_date")(c)


def __as_decimal_or_number(
    cast_type: str,
    variant: ColumnOrName,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    """Helper funtion that casts a VARIANT value to a decimal or number."""
    c = _to_col_if_str(variant, cast_type)
    if scale and not precision:
        raise ValueError("Cannot define scale without precision")
    if precision and scale:
        return builtin(cast_type)(c, sql_expr(str(precision)), sql_expr(str(scale)))
    elif precision:
        return builtin(cast_type)(c, sql_expr(str(precision)))
    else:
        return builtin(cast_type)(c)


def as_decimal(
    variant: ColumnOrName,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    """Casts a VARIANT value to a fixed-point decimal (does not match floating-point values)."""
    return __as_decimal_or_number("as_decimal", variant, precision, scale)


def as_number(
    variant: ColumnOrName,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    """Casts a VARIANT value to a fixed-point decimal (does not match floating-point values)."""
    return __as_decimal_or_number("as_number", variant, precision, scale)


def as_double(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a floating-point value."""
    c = _to_col_if_str(variant, "as_double")
    return builtin("as_double")(c)


def as_real(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a floating-point value."""
    c = _to_col_if_str(variant, "as_real")
    return builtin("as_real")(c)


def as_integer(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to an integer."""
    c = _to_col_if_str(variant, "as_integer")
    return builtin("as_integer")(c)


def as_object(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to an object."""
    c = _to_col_if_str(variant, "as_object")
    return builtin("as_object")(c)


def as_time(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a time value."""
    c = _to_col_if_str(variant, "as_time")
    return builtin("as_time")(c)


def as_timestamp_ltz(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with a local timezone."""
    c = _to_col_if_str(variant, "as_timestamp_ltz")
    return builtin("as_timestamp_ltz")(c)


def as_timestamp_ntz(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with no timezone."""
    c = _to_col_if_str(variant, "as_timestamp_ntz")
    return builtin("as_timestamp_ntz")(c)


def as_timestamp_tz(variant: ColumnOrName) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with a timezone."""
    c = _to_col_if_str(variant, "as_timestamp_tz")
    return builtin("as_timestamp_tz")(c)


def to_binary(e: ColumnOrName, fmt: Optional[str] = None) -> Column:
    """Converts the input expression to a binary value. For NULL input, the output is
    NULL."""
    c = _to_col_if_str(e, "to_binary")
    return builtin("to_binary")(c, fmt) if fmt else builtin("to_binary")(c)


def to_array(e: ColumnOrName) -> Column:
    """Converts any value to an ARRAY value or NULL (if input is NULL)."""
    c = _to_col_if_str(e, "to_array")
    return builtin("to_array")(c)


def to_json(e: ColumnOrName) -> Column:
    """Converts any VARIANT value to a string containing the JSON representation of the
    value. If the input is NULL, the result is also NULL."""
    c = _to_col_if_str(e, "to_json")
    return builtin("to_json")(c)


def to_object(e: ColumnOrName) -> Column:
    """Converts any value to a OBJECT value or NULL (if input is NULL)."""
    c = _to_col_if_str(e, "to_object")
    return builtin("to_object")(c)


def to_variant(e: ColumnOrName) -> Column:
    """Converts any value to a VARIANT value or NULL (if input is NULL)."""
    c = _to_col_if_str(e, "to_variant")
    return builtin("to_variant")(c)


def to_xml(e: ColumnOrName) -> Column:
    """Converts any VARIANT value to a string containing the XML representation of the
    value. If the input is NULL, the result is also NULL."""
    c = _to_col_if_str(e, "to_xml")
    return builtin("to_xml")(c)


def get_ignore_case(obj: ColumnOrName, field: ColumnOrName) -> Column:
    """Extracts a field value from an object. Returns NULL if either of the arguments is NULL.
    This function is similar to :meth:`get` but applies case-insensitive matching to field names.
    """
    c1 = _to_col_if_str(obj, "get_ignore_case")
    c2 = _to_col_if_str(field, "get_ignore_case")
    return builtin("get_ignore_case")(c1, c2)


def object_keys(obj: ColumnOrName) -> Column:
    """Returns an array containing the list of keys in the input object."""
    c = _to_col_if_str(obj, "object_keys")
    return builtin("object_keys")(c)


def xmlget(
    xml: ColumnOrName,
    tag: ColumnOrName,
    instance_num: Union[ColumnOrName, int] = 0,
) -> Column:
    """Extracts an XML element object (often referred to as simply a tag) from a content of outer
    XML element object by the name of the tag and its instance number (counting from 0).
    """
    c1 = _to_col_if_str(xml, "xmlget")
    c2 = _to_col_if_str(tag, "xmlget")
    c3 = (
        instance_num
        if isinstance(instance_num, int)
        else _to_col_if_str(instance_num, "xmlget")
    )
    return builtin("xmlget")(c1, c2, c3)


def get_path(col: ColumnOrName, path: ColumnOrName) -> Column:
    """Extracts a value from semi-structured data using a path name."""
    c1 = _to_col_if_str(col, "get_path")
    c2 = _to_col_if_str(path, "get_path")
    return builtin("get_path")(c1, c2)


def get(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Extracts a value from an object or array; returns NULL if either of the arguments is NULL."""
    c1 = _to_col_if_str(col1, "get")
    c2 = _to_col_if_str(col2, "get")
    return builtin("get")(c1, c2)


def when(condition: Column, value: Union[Column, LiteralType]) -> CaseExpr:
    """Works like a cascading if-then-else statement.
    A series of conditions are evaluated in sequence.
    When a condition evaluates to TRUE, the evaluation stops and the associated
    result (after THEN) is returned. If none of the conditions evaluate to TRUE,
    then the result after the optional OTHERWISE is returned, if present;
    otherwise NULL is returned.
    """
    return CaseExpr(SPCaseWhen([(condition.expression, Column._to_expr(value))]))


def iff(
    condition: Column,
    expr1: Union[Column, LiteralType],
    expr2: Union[Column, LiteralType],
) -> Column:
    """
    Returns one of two specified expressions, depending on a condition.
    This is equivalent to an ``if-then-else`` expression. If ``condition``
    evaluates to TRUE, the function returns ``expr1``. Otherwise, the
    function returns ``expr2``.
    """
    return builtin("iff")(condition, expr1, expr2)


def in_(
    cols: List[ColumnOrName],
    *vals: Union[
        "snowflake.snowpark.DataFrame", ColumnOrLiteral, List[ColumnOrLiteral]
    ],
) -> Column:
    if len(vals) == 1 and isinstance(vals[0], (list, set, tuple)):
        vals = vals[0]
    columns = [_to_col_if_str(c, "in_") for c in cols]
    return Column(SPMultipleExpression([c.expression for c in columns])).in_(vals)


def cume_dist() -> Column:
    """
    Finds the cumulative distribution of a value with regard to other values
    within the same window partition.
    """
    return builtin("cume_dist")()


def rank() -> Column:
    """
    Returns the rank of a value within an ordered group of values.
    The rank value starts at 1 and continues up.
    """
    return builtin("rank")()


def percent_rank() -> Column:
    """
    Returns the relative rank of a value within a group of values, specified as a percentage
    ranging from 0.0 to 1.0.
    """
    return builtin("percent_rank")()


def dense_rank() -> Column:
    """
    Returns the rank of a value within a group of values, without gaps in the ranks.
    The rank value starts at 1 and continues up sequentially.
    If two values are the same, they will have the same rank.
    """
    return builtin("dense_rank")()


def row_number() -> Column:
    """
    Returns a unique row number for each row within a window partition.
    The row number starts at 1 and continues up sequentially.
    """
    return builtin("row_number")()


def lag(
    e: ColumnOrName,
    offset: int = 1,
    default_value: Optional[Union[Column, LiteralType]] = None,
) -> Column:
    """
    Accesses data in a previous row in the same result set without having to
    join the table to itself.
    """
    c = _to_col_if_str(e, "lag")
    return builtin("lag")(
        c,
        SPLiteral(offset),
        SPLiteral(None) if default_value is None else default_value,
    )


def lead(
    e: ColumnOrName,
    offset: int = 1,
    default_value: Optional[Union[Column, LiteralType]] = None,
) -> Column:
    """
    Accesses data in a subsequent row in the same result set without having to
    join the table to itself.
    """
    c = _to_col_if_str(e, "lead")
    return builtin("lead")(
        c,
        SPLiteral(offset),
        SPLiteral(None) if default_value is None else default_value,
    )


def ntile(e: ColumnOrName) -> Column:
    """
    Divides an ordered data set equally into the number of buckets specified by n.
    Buckets are sequentially numbered 1 through n.
    """
    c = _to_col_if_str(e, "ntile")
    return builtin("ntile")(c)


def current_timestamp() -> Column:
    return builtin("current_timestamp")()


def udf(
    func: Optional[Callable] = None,
    *,
    return_type: Optional[DataType] = None,
    input_types: Optional[List[DataType]] = None,
    name: Optional[str] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    replace: bool = False,
    parallel: int = 4,
) -> Union[UserDefinedFunction, functools.partial]:
    """Registers a Python function as a Snowflake Python UDF and returns the UDF.

    Args:
        func: A Python function used for creating the UDF.
        return_type: A :class:`types.DataType` representing the return data
            type of the UDF. Optional if type hints are provided.
        input_types: A list of :class:`~snowflake.snowpark.types.DataType`
            representing the input data types of the UDF. Optional if
            type hints are provided.
        name: The name to use for the UDF in Snowflake, which allows you to call this UDF
            in a SQL command or via :func:`call_udf()`. If it is not provided,
            a name will be automatically generated for the UDF. A name must be
            specified when ``is_permanent`` is ``True``.
        is_permanent: Whether to create a permanent UDF. The default is ``False``.
            If it is ``True``, a valid ``stage_location`` must be provided.
        stage_location: The stage location where the Python file for the UDF
            and its dependencies should be uploaded. The stage location must be specified
            when ``is_permanent`` is ``True``, and it will be ignored when
            ``is_permanent`` is ``False``. It can be any stage other than temporary
            stages and external stages.
        replace: Whether to replace a UDF that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a UDF with a name that already exists
            results in a ``ProgrammingError`` exception being thrown. If it is ``True``,
            an existing UDF with the same name is overwritten.
        parallel: The number of threads to use for uploading UDF files with the
            `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
            command. The default value is 4 and supported values are from 1 to 99.
            Increasing the number of threads can improve performance when uploading
            large UDF files.

    Returns:
        A UDF function that can be called with :class:`~snowflake.snowpark.Column` expressions.

    Examples::

        from snowflake.snowpark.types import IntegerType
        # register a temporary udf
        add_one = udf(lambda x: x+1, return_type=IntegerType(), input_types=[IntegerType()])

        # register a permanent udf by setting is_permanent to True
        @udf(name="minus_one", is_permanent=True, stage_location="@mystage")
        def minus_one(x: int) -> int:
            return x-1

        df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
        df.select(add_one("a"), minus_one("b"))
        session.sql("select minus_one(1)")

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for UDFs in
        :class:`~snowflake.snowpark.udf.UDFRegistration`.

        2. This function registers a UDF using the last created session.
        If you want to register a UDF with a specific session, use
        :func:`session.udf.register() <snowflake.snowpark.udf.UDFRegistration.register>`.

        3. By default, UDF registration fails if a function with the same name is already
        registered. Invoking :func:`udf` with ``replace`` set to ``True`` will overwrite the
        previously registered function.
    """
    session = snowflake.snowpark.Session._get_active_session()
    if not session:
        raise SnowparkClientExceptionMessages.SERVER_NO_DEFAULT_SESSION()

    if func is None:
        return functools.partial(
            session.udf.register,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            replace=replace,
            parallel=parallel,
        )
    else:
        return session.udf.register(
            func,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            replace=replace,
            parallel=parallel,
        )


def __with_aggregate_function(
    func: SPAggregateFunction, is_distinct: bool = False
) -> Column:
    return Column(func.to_aggregate_expression(is_distinct))


def call_udf(
    udf_name: str,
    *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName]],
) -> Column:
    """Calls a user-defined function (UDF) by name.

    Args:
        udf_name: The name of UDF in Snowflake.
        cols: Columns that the UDF will be applied to, as :class:`str`,
            :class:`~snowflake.snowpark.Column` or a list of those.

    Example::

        df.select(call_udf("add", col("a"), col("b")))
    """

    Utils.validate_object_name(udf_name)
    exprs = Utils.parse_positional_args_to_list(*cols)
    return Column(
        SPFunctionExpression(
            udf_name,
            [_to_col_if_str(e, "call_udf").expression for e in exprs],
            is_distinct=False,
        )
    )


def call_builtin(function_name: str, *args: Union[Column, LiteralType]) -> Column:
    """Invokes a Snowflake `system-defined function <https://docs.snowflake.com/en/sql-reference-functions.html>`_ (built-in function) with the specified name
    and arguments.

    Args:
        function_name: The name of built-in function in Snowflake
        args: Arguments can be in two types:

            - :class:`~snowflake.snowpark.Column`, or
            - Basic Python types, which are converted to Snowpark literals.

    Example::

        df.select(call_builtin("avg", col("a")))
    """

    expressions = [
        Column._to_expr(arg) for arg in Utils.parse_positional_args_to_list(*args)
    ]

    return Column(SPFunctionExpression(function_name, expressions, is_distinct=False))


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


def _to_col_if_str(e: ColumnOrName, func_name: str) -> Column:
    if isinstance(e, Column):
        return e
    elif isinstance(e, str):
        return col(e)
    else:
        raise TypeError(f"'{func_name.upper()}' expected Column or str, got: {type(e)}")


def _create_table_function_expression(
    func_name: Union[str, List[str]],
    *args: ColumnOrName,
    **named_args: ColumnOrName,
) -> SPTableFunctionExpression:
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
        return SPTableFunction(
            fqdn,
            (
                _to_col_if_str(arg, "table_function").expression
                for arg in func_arguments
            ),
        )
    return SPNamedArgumentsTableFunction(
        fqdn,
        {
            arg_name: _to_col_if_str(arg, "table_function").expression
            for arg_name, arg in named_args.items()
        },
    )
