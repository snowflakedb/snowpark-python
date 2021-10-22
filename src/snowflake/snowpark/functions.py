#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
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
from random import randint
from typing import Any, Callable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.sp_expressions import (
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
from snowflake.snowpark._internal.sp_types.sp_data_types import (
    IntegerType as SPIntegerType,
    LongType as SPLongType,
)
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.column import CaseExpr, Column
from snowflake.snowpark.types import DataType
from snowflake.snowpark.udf import UserDefinedFunction


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
    return Column._expr(sql)


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


def negate(e: Union[Column, str]) -> Column:
    """Returns the negation of the value in the column (equivalent to a unary minus)."""
    c = __to_col_if_str(e, "negate")
    return -c


def not_(e: Union[Column, str]) -> Column:
    """Returns the inverse of a boolean expression."""
    c = __to_col_if_str(e, "not_")
    return ~c


def random(seed: Optional[int] = None) -> Column:
    """Each call returns a pseudo-random 64-bit integer."""
    s = seed if seed is not None else randint(-(2 ** 63), 2 ** 63 - 1)
    return builtin("random")(SPLiteral(s, SPLongType()))


def to_decimal(e: Union[Column, str], precision: int, scale: int) -> Column:
    """Converts an input expression to a decimal."""
    c = __to_col_if_str(e, "to_decimal")
    return builtin("to_decimal")(c, sql_expr(str(precision)), sql_expr(str(scale)))


def sqrt(e: Union[Column, str]) -> Column:
    """Returns the square-root of a non-negative numeric expression."""
    c = __to_col_if_str(e, "sqrt")
    return builtin("sqrt")(c)


def abs(e: Union[Column, str]) -> Column:
    """Returns the absolute value of a numeric expression."""
    c = __to_col_if_str(e, "abs")
    return builtin("abs")(c)


def ceil(e: Union[Column, str]) -> Column:
    """Returns values from the specified column rounded to the nearest equal or larger
    integer."""
    c = __to_col_if_str(e, "ceil")
    return builtin("ceil")(c)


def floor(e: Union[Column, str]) -> Column:
    """Returns values from the specified column rounded to the nearest equal or
    smaller integer."""
    c = __to_col_if_str(e, "floor")
    return builtin("floor")(c)


def exp(e: Union[Column, str]) -> Column:
    """Computes Euler's number e raised to a floating-point value."""
    c = __to_col_if_str(e, "exp")
    return builtin("exp")(c)


def log(
    base: Union[Column, str, int, float], x: Union[Column, str, int, float]
) -> Column:
    """Returns the logarithm of a numeric expression."""
    b = lit(base) if type(base) in [int, float] else __to_col_if_str(base, "log")
    arg = lit(x) if type(x) in [int, float] else __to_col_if_str(x, "log")
    return builtin("log")(b, arg)


def pow(l: Union[Column, str, int, float], r: Union[Column, str, int, float]) -> Column:
    """Returns a number (l) raised to the specified power (r)."""
    number = lit(l) if type(l) in [int, float] else __to_col_if_str(l, "pow")
    power = lit(r) if type(r) in [int, float] else __to_col_if_str(r, "pow")
    return builtin("pow")(number, power)


def split(
    str: Union[Column, str],
    pattern: Union[Column, str],
) -> Column:
    """Splits a given string with a given separator and returns the result in an array
    of strings. To specify a string separator, use the :func:`lit()` function."""
    s = __to_col_if_str(str, "split")
    p = __to_col_if_str(pattern, "split")
    return builtin("split")(s, p)


def substring(
    str: Union[Column, str], pos: Union[Column, int], len: Union[Column, int]
) -> Column:
    """Returns the portion of the string or binary value str, starting from the
    character/byte specified by pos, with limited length. The length should be greater
    than or equal to zero. If the length is a negative number, the function returns an
    empty string."""
    s = __to_col_if_str(str, "substring")
    p = pos if type(pos) == Column else lit(pos)
    l = len if type(len) == Column else lit(len)
    return builtin("substring")(s, p, l)


def translate(
    src: Union[Column, str],
    matching_string: Union[Column, str],
    replace_string: Union[Column, str],
) -> Column:
    """Translates src from the characters in matchingString to the characters in
    replaceString."""
    source = __to_col_if_str(src, "translate")
    match = __to_col_if_str(matching_string, "translate")
    replace = __to_col_if_str(replace_string, "translate")
    return builtin("translate")(source, match, replace)


def trim(e: Union[Column, str], trim_string: Union[Column, str]) -> Column:
    """Removes leading and trailing characters from a string."""
    c = __to_col_if_str(e, "trim")
    t = __to_col_if_str(trim_string, "trim")
    return builtin("trim")(c, t)


def upper(e: Union[Column, str]) -> Column:
    """Returns the input string with all characters converted to uppercase."""
    c = __to_col_if_str(e, "upper")
    return builtin("upper")(c)


def contains(col: Union[Column, str], str: Union[Column, str]) -> Column:
    """Returns true if col contains str."""
    c = __to_col_if_str(col, "contains")
    s = __to_col_if_str(str, "contains")
    return builtin("contains")(c, s)


def startswith(col: Union[Column, str], str: Union[Column, str]) -> Column:
    """Returns true if col starts with str."""
    c = __to_col_if_str(col, "startswith")
    s = __to_col_if_str(str, "startswith")
    return builtin("startswith")(c, s)


def char(col: Union[Column, str]) -> Column:
    """Converts a Unicode code point (including 7-bit ASCII) into the character that
    matches the input Unicode."""
    c = __to_col_if_str(col, "char")
    return builtin("char")(c)


def to_time(e: Union[Column, str], fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding time."""
    c = __to_col_if_str(e, "to_time")
    return builtin("to_time")(c, fmt) if fmt else builtin("to_time")(c)


def to_timestamp(e: Union[Column, str], fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding timestamp."""
    c = __to_col_if_str(e, "to_timestamp")
    return builtin("to_timestamp")(c, fmt) if fmt else builtin("to_timestamp")(c)


def to_date(e: Union[Column, str], fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into a date."""
    c = __to_col_if_str(e, "to_date")
    return builtin("to_date")(c, fmt) if fmt else builtin("to_date")(c)


def is_array(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains an ARRAY value."""
    c = __to_col_if_str(col, "is_array")
    return builtin("is_array")(c)


def is_boolean(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a boolean value."""
    c = __to_col_if_str(col, "is_boolean")
    return builtin("is_boolean")(c)


def is_binary(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a binary value."""
    c = __to_col_if_str(col, "is_binary")
    return builtin("is_binary")(c)


def is_char(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a string."""
    c = __to_col_if_str(col, "is_char")
    return builtin("is_char")(c)


def is_varchar(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a string."""
    c = __to_col_if_str(col, "is_varchar")
    return builtin("is_varchar")(c)


def is_date(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a date value."""
    c = __to_col_if_str(col, "is_date")
    return builtin("is_date")(c)


def is_date_value(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a date value."""
    c = __to_col_if_str(col, "is_date_value")
    return builtin("is_date_value")(c)


def is_decimal(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a fixed-point decimal value or integer."""
    c = __to_col_if_str(col, "is_decimal")
    return builtin("is_decimal")(c)


def is_double(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a floating-point value, fixed-point decimal, or integer."""
    c = __to_col_if_str(col, "is_double")
    return builtin("is_double")(c)


def is_real(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a floating-point value, fixed-point decimal, or integer."""
    c = __to_col_if_str(col, "is_real")
    return builtin("is_real")(c)


def is_integer(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a integer value."""
    c = __to_col_if_str(col, "is_integer")
    return builtin("is_integer")(c)


def is_null_value(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a JSON null value."""
    c = __to_col_if_str(col, "is_null_value")
    return builtin("is_null_value")(c)


def is_object(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains an OBJECT value."""
    c = __to_col_if_str(col, "is_object")
    return builtin("is_object")(c)


def is_time(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a TIME value."""
    c = __to_col_if_str(col, "is_time")
    return builtin("is_time")(c)


def is_timestamp_ltz(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a TIMESTAMP value to be interpreted using the local time
    zone."""
    c = __to_col_if_str(col, "is_timestamp_ltz")
    return builtin("is_timestamp_ltz")(c)


def is_timestamp_ntz(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a TIMESTAMP value with no time zone."""
    c = __to_col_if_str(col, "is_timestamp_ntz")
    return builtin("is_timestamp_ntz")(c)


def is_timestamp_tz(col: Union[Column, str]):
    """Returns true if the specified VARIANT column contains a TIMESTAMP value with a time zone."""
    c = __to_col_if_str(col, "is_timestamp_tz")
    return builtin("is_timestamp_tz")(c)


def typeof(col: Union[Column, str]) -> Column:
    """Reports the type of a value stored in a VARIANT column. The type is returned as a string."""
    c = __to_col_if_str(col, "typeof")
    return builtin("typeof")(c)


def parse_json(e: Union[Column, str]) -> Column:
    """Parse the value of the specified column as a JSON string and returns the
    resulting JSON document."""
    c = __to_col_if_str(e, "parse_json")
    return builtin("parse_json")(c)


def parse_xml(e: Union[Column, str]) -> Column:
    """Parse the value of the specified column as a JSON string and returns the
    resulting XML document."""
    c = __to_col_if_str(e, "parse_xml")
    return builtin("parse_xml")(c)


def array_agg(e: Union[Column, str]) -> Column:
    """Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
    ARRAY is returned."""
    c = __to_col_if_str(e, "array_agg")
    return builtin("array_agg")(c)


def as_array(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to an array."""
    c = __to_col_if_str(variant, "as_array")
    return builtin("as_array")(c)


def as_binary(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a binary string."""
    c = __to_col_if_str(variant, "as_binary")
    return builtin("as_binary")(c)


def as_char(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a string."""
    c = __to_col_if_str(variant, "as_char")
    return builtin("as_char")(c)


def as_varchar(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a string."""
    c = __to_col_if_str(variant, "as_varchar")
    return builtin("as_varchar")(c)


def as_date(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a date."""
    c = __to_col_if_str(variant, "as_date")
    return builtin("as_date")(c)


def __as_decimal_or_number(
    cast_type: str,
    variant: Union[Column, str],
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    """Helper funtion that casts a VARIANT value to a decimal or number."""
    c = __to_col_if_str(variant, cast_type)
    if scale and not precision:
        raise ValueError("Cannot define scale without precision")
    if precision and scale:
        return builtin(cast_type)(c, sql_expr(str(precision)), sql_expr(str(scale)))
    elif precision:
        return builtin(cast_type)(c, sql_expr(str(precision)))
    else:
        return builtin(cast_type)(c)


def as_decimal(
    variant: Union[Column, str],
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    """Casts a VARIANT value to a fixed-point decimal (does not match floating-point values)."""
    return __as_decimal_or_number("as_decimal", variant, precision, scale)


def as_number(
    variant: Union[Column, str],
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    """Casts a VARIANT value to a fixed-point decimal (does not match floating-point values)."""
    return __as_decimal_or_number("as_number", variant, precision, scale)


def as_double(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a floating-point value."""
    c = __to_col_if_str(variant, "as_double")
    return builtin("as_double")(c)


def as_real(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a floating-point value."""
    c = __to_col_if_str(variant, "as_real")
    return builtin("as_real")(c)


def as_integer(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to an integer."""
    c = __to_col_if_str(variant, "as_integer")
    return builtin("as_integer")(c)


def as_object(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to an object."""
    c = __to_col_if_str(variant, "as_object")
    return builtin("as_object")(c)


def as_time(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a time value."""
    c = __to_col_if_str(variant, "as_time")
    return builtin("as_time")(c)


def as_timestamp_ltz(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with a local timezone."""
    c = __to_col_if_str(variant, "as_timestamp_ltz")
    return builtin("as_timestamp_ltz")(c)


def as_timestamp_ntz(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with no timezone."""
    c = __to_col_if_str(variant, "as_timestamp_ntz")
    return builtin("as_timestamp_ntz")(c)


def as_timestamp_tz(variant: Union[Column, str]) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with a timezone."""
    c = __to_col_if_str(variant, "as_timestamp_tz")
    return builtin("as_timestamp_tz")(c)


def to_binary(e: Union[Column, str], fmt: Optional[str] = None) -> Column:
    """Converts the input expression to a binary value. For NULL input, the output is
    NULL."""
    c = __to_col_if_str(e, "to_binary")
    return builtin("to_binary")(c, fmt) if fmt else builtin("to_binary")(c)


def to_array(e: Union[Column, str]) -> Column:
    """Converts any value to an ARRAY value or NULL (if input is NULL)."""
    c = __to_col_if_str(e, "to_array")
    return builtin("to_array")(c)


def to_json(e: Union[Column, str]) -> Column:
    """Converts any VARIANT value to a string containing the JSON representation of the
    value. If the input is NULL, the result is also NULL."""
    c = __to_col_if_str(e, "to_json")
    return builtin("to_json")(c)


def to_object(e: Union[Column, str]) -> Column:
    """Converts any value to a OBJECT value or NULL (if input is NULL)."""
    c = __to_col_if_str(e, "to_object")
    return builtin("to_object")(c)


def to_variant(e: Union[Column, str]) -> Column:
    """Converts any value to a VARIANT value or NULL (if input is NULL)."""
    c = __to_col_if_str(e, "to_variant")
    return builtin("to_variant")(c)


def to_xml(e: Union[Column, str]) -> Column:
    """Converts any VARIANT value to a string containing the XML representation of the
    value. If the input is NULL, the result is also NULL."""
    c = __to_col_if_str(e, "to_xml")
    return builtin("to_xml")(c)


def get_ignore_case(obj: Union[Column, str], field: Union[Column, str]) -> Column:
    """Extracts a field value from an object. Returns NULL if either of the arguments is NULL.
    This function is similar to :meth:`get` but applies case-insensitive matching to field names.
    """
    c1 = __to_col_if_str(obj, "get_ignore_case")
    c2 = __to_col_if_str(field, "get_ignore_case")
    return builtin("get_ignore_case")(c1, c2)


def object_keys(obj: Union[Column, str]) -> Column:
    """Returns an array containing the list of keys in the input object."""
    c = __to_col_if_str(obj, "object_keys")
    return builtin("object_keys")(c)


def xmlget(
    xml: Union[Column, str],
    tag: Union[Column, str],
    instance_num: Union[Column, str] = lit(0),
) -> Column:
    """Extracts an XML element object (often referred to as simply a tag) from a content of outer
    XML element object by the name of the tag and its instance number (counting from 0).
    """
    c1 = __to_col_if_str(xml, "xmlget")
    c2 = __to_col_if_str(tag, "xmlget")
    c3 = __to_col_if_str(instance_num, "xmlget") if instance_num is not None else lit(0)
    return builtin("xmlget")(c1, c2, c3)


def get_path(col: Union[Column, str], path: Union[Column, str]) -> Column:
    """Extracts a value from semi-structured data using a path name."""
    c1 = __to_col_if_str(col, "get_path")
    c2 = __to_col_if_str(path, "get_path")
    return builtin("get_path")(c1, c2)


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
) -> Union[UserDefinedFunction, functools.partial]:
    """Registers a Python function as a Snowflake Python UDF and returns the UDF.

    Args:
        func: A Python function used for creating the UDF.
        return_type: A :class:`types.DataType` representing the return data
            type of the UDF. Optional if type hints are provided.
        input_types: A list of :class:`~snowflake.snowpark.types.DataType`
            representing the input data types of the UDF. Optional if
            type hints are provided.
        name: The name to use for the UDF in Snowflake, which allows to call this UDF
            in a SQL command or via :func:`call_udf()`. If it is not provided,
            a random name will be generated automatically for the UDF.

    Returns:
        A UDF function that can be called with Column expressions
        (:class:`~snowflake.snowpark.Column` or :class:`str`).

    Examples::

        from snowflake.snowpark.types import IntegerType
        add_one = udf(lambda x: x+1, return_types=IntegerType(), input_types=[IntegerType()])

        @udf(name="minus_one")
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
        args: Arguments can be in two types:
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
        raise TypeError(f"'{func_name.upper()}' expected Column or str, got: {type(e)}")
