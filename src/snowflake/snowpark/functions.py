#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""
Provides utility and SQL functions that generate :class:`~snowflake.snowpark.Column` expressions that you can pass to :class:`~snowflake.snowpark.DataFrame` transformation methods.

These utility functions generate references to columns, literals, and SQL expressions (e.g. "c + 1").

  - Use :func:`col()` to convert a column name to a :class:`Column` object. Refer to the API docs of :class:`Column` to know more ways of referencing a column.
  - Use :func:`lit()` to convert a Python value to a :class:`Column` object that represents a constant value in Snowflake SQL.
  - Use :func:`sql_expr()` to convert a Snowflake SQL expression to a :class:`Column`.

    >>> df = session.create_dataframe([[1, 'a', True, '2022-03-16'], [3, 'b', False, '2023-04-17']], schema=["a", "b", "c", "d"])
    >>> res1 = df.filter(col("a") == 1).collect()
    >>> res2 = df.filter(lit(1) == col("a")).collect()
    >>> res3 = df.filter(sql_expr("a = 1")).collect()
    >>> assert res1 == res2 == res3
    >>> res1
    [Row(A=1, B='a', C=True, D='2022-03-16')]

Some :class:`DataFrame` methods accept column names or SQL expressions text aside from a Column object for convenience.
For instance:

    >>> df.filter("a = 1").collect()  # use the SQL expression directly in filter
    [Row(A=1, B='a', C=True, D='2022-03-16')]
    >>> df.select("a").collect()
    [Row(A=1), Row(A=3)]

whereas :class:`Column` objects enable you to use chained column operators and transformations with
Python code fluently:

    >>> # Use columns and literals in expressions.
    >>> df.select(((col("a") + 1).cast("string")).alias("add_one")).show()
    -------------
    |"ADD_ONE"  |
    -------------
    |2          |
    |4          |
    -------------
    <BLANKLINE>

The Snowflake database has hundreds of `SQL functions <https://docs.snowflake.com/en/sql-reference-functions.html>`_
This module provides Python functions that correspond to the Snowflake SQL functions. They typically accept :class:`Column`
objects or column names as input parameters and return a new :class:`Column` objects.
The following examples demonstrate the use of some of these functions:

    >>> # This example calls the function that corresponds to the TO_DATE() SQL function.
    >>> df.select(dateadd('day', lit(1), to_date(col("d")))).show()
    ---------------------------------------
    |"DATEADD('DAY', 1, TO_DATE(""D""))"  |
    ---------------------------------------
    |2022-03-17                           |
    |2023-04-18                           |
    ---------------------------------------
    <BLANKLINE>

If you want to use a SQL function in Snowflake but can't find the corresponding Python function here,
you can create your own Python function with :func:`function`:

    >>> my_radians = function("radians")  # "radians" is the SQL function name.
    >>> df.select(my_radians(col("a")).alias("my_radians")).show()
    ------------------------
    |"MY_RADIANS"          |
    ------------------------
    |0.017453292519943295  |
    |0.05235987755982988   |
    ------------------------
    <BLANKLINE>

or call the SQL function directly:

    >>> df.select(call_function("radians", col("a")).as_("call_function_radians")).show()
    ---------------------------
    |"CALL_FUNCTION_RADIANS"  |
    ---------------------------
    |0.017453292519943295     |
    |0.05235987755982988      |
    ---------------------------
    <BLANKLINE>

A user-defined function (UDF) can be called by its name with :func:`call_udf`:

    >>> # Call a user-defined function (UDF) by name.
    >>> from snowflake.snowpark.types import IntegerType
    >>> add_one_udf = udf(lambda x: x + 1, name="add_one", input_types=[IntegerType()], return_type=IntegerType())
    >>> df.select(call_udf("add_one", col("a")).as_("call_udf_add_one")).sort("call_udf_add_one").show()
    ----------------------
    |"CALL_UDF_ADD_ONE"  |
    ----------------------
    |2                   |
    |4                   |
    ----------------------
    <BLANKLINE>

**How to find help on input parameters of the Python functions for SQL functions**
The Python functions have the same name as the corresponding `SQL functions <https://docs.snowflake.com/en/sql-reference-functions.html>`_.

By reading the API docs or the source code of a Python function defined in this module, you'll see the type hints of the input parameters and return type.
The return type is always ``Column``. The input types tell you the acceptable values:

  - ``ColumnOrName`` accepts a :class:`Column` object, or a column name in str. Most functions accept this type.
    If you still want to pass a literal to it, use `lit(value)`, which returns a ``Column`` object that represents a literal value.

    >>> df.select(avg("a")).show()
    ----------------
    |"AVG(""A"")"  |
    ----------------
    |2.000000      |
    ----------------
    <BLANKLINE>
    >>> df.select(avg(col("a"))).show()
    ----------------
    |"AVG(""A"")"  |
    ----------------
    |2.000000      |
    ----------------
    <BLANKLINE>

  - ``LiteralType`` accepts a value of type ``bool``, ``int``, ``float``, ``str``, ``bytearray``, ``decimal.Decimal``,
    ``datetime.date``, ``datetime.datetime``, ``datetime.time``, or ``bytes``. An example is the third parameter of :func:`lead`.

    >>> import datetime
    >>> from snowflake.snowpark.window import Window
    >>> df.select(col("d"), lead("d", 1, datetime.date(2024, 5, 18), False).over(Window.order_by("d")).alias("lead_day")).show()
    ---------------------------
    |"D"         |"LEAD_DAY"  |
    ---------------------------
    |2022-03-16  |2023-04-17  |
    |2023-04-17  |2024-05-18  |
    ---------------------------
    <BLANKLINE>

  - ``ColumnOrLiteral`` accepts a ``Column`` object, or a value of ``LiteralType`` mentioned above.
    The difference from ``ColumnOrLiteral`` is ``ColumnOrLiteral`` regards a str value as a SQL string value instead of
    a column name. When a function is much more likely to accept a SQL constant value than a column expression, ``ColumnOrLiteral``
    is used. Yet you can still pass in a ``Column`` object if you need to. An example is the second parameter of
    :func:``when``.

    >>> df.select(when(df["a"] > 2, "Greater than 2").else_("Less than 2").alias("compare_with_2")).show()
    --------------------
    |"COMPARE_WITH_2"  |
    --------------------
    |Less than 2       |
    |Greater than 2    |
    --------------------
    <BLANKLINE>

  - ``int``, ``bool``, ``str``, or another specific type accepts a value of that type. An example is :func:`to_decimal`.

    >>> df.with_column("e", lit("1.2")).select(to_decimal("e", 5, 2)).show()
    -----------------------------
    |"TO_DECIMAL(""E"", 5, 2)"  |
    -----------------------------
    |1.20                       |
    |1.20                       |
    -----------------------------
    <BLANKLINE>

  - ``ColumnOrSqlExpr`` accepts a ``Column`` object, or a SQL expression. For instance, the first parameter in :func:``when``.

    >>> df.select(when("a > 2", "Greater than 2").else_("Less than 2").alias("compare_with_2")).show()
    --------------------
    |"COMPARE_WITH_2"  |
    --------------------
    |Less than 2       |
    |Greater than 2    |
    --------------------
    <BLANKLINE>
"""
import functools
import typing
from random import randint
from types import ModuleType
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union, overload

import snowflake.snowpark
import snowflake.snowpark.table_function
from snowflake.snowpark._internal.analyzer.expression import (
    CaseWhen,
    FunctionExpression,
    ListAgg,
    Literal,
    MultipleExpression,
    Star,
)
from snowflake.snowpark._internal.analyzer.window_expression import FirstValue, Lag, LastValue, Lead
from snowflake.snowpark._internal.type_utils import (
    ColumnOrLiteral,
    ColumnOrLiteralStr,
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
)
from snowflake.snowpark._internal.utils import (
    parse_positional_args_to_list,
    validate_object_name,
)
from snowflake.snowpark.column import (
    CaseExpr,
    Column,
    _to_col_if_sql_expr,
    _to_col_if_str,
    _to_col_if_str_or_int,
)
from snowflake.snowpark.stored_procedure import StoredProcedure
from snowflake.snowpark.types import DataType, FloatType, StructType
from snowflake.snowpark.udf import UserDefinedFunction
from snowflake.snowpark.udtf import UserDefinedTableFunction


def col(col_name: str) -> Column:
    """Returns the :class:`~snowflake.snowpark.Column` with the specified name."""
    return Column(col_name)


def column(col_name: str) -> Column:
    """Returns a :class:`~snowflake.snowpark.Column` with the specified name. Alias for col."""
    return Column(col_name)


def lit(literal: LiteralType) -> Column:
    """
    Creates a :class:`~snowflake.snowpark.Column` expression for a literal value.
    It supports basic Python data types, including ``int``, ``float``, ``str``,
    ``bool``, ``bytes``, ``bytearray``, ``datetime.time``, ``datetime.date``,
    ``datetime.datetime`` and ``decimal.Decimal``. Also, it supports Python structured data types,
    including ``list``, ``tuple`` and ``dict``, but this container must
    be JSON serializable.
    """
    return literal if isinstance(literal, Column) else Column(Literal(literal))


def sql_expr(sql: str) -> Column:
    """Creates a :class:`~snowflake.snowpark.Column` expression from raw SQL text.
    Note that the function does not interpret or check the SQL text."""
    return Column._expr(sql)


def current_session() -> Column:
    """
    Returns a unique system identifier for the Snowflake session corresponding to the present connection.
    This will generally be a system-generated alphanumeric string. It is NOT derived from the user name or user account.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_session()).collect()
        >>> assert result is not None
    """
    return builtin("current_session")()


def current_statement() -> Column:
    """
    Returns the SQL text of the statement that is currently executing.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_statement()).collect()
        >>> assert result is not None
    """
    return builtin("current_statement")()


def current_user() -> Column:
    """
    Returns the name of the user currently logged into the system.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_user()).collect()
        >>> assert result is not None
    """
    return builtin("current_user")()


def current_version() -> Column:
    """
    Returns the current Snowflake version.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_version()).collect()
        >>> assert result is not None
    """
    return builtin("current_version")()


def current_warehouse() -> Column:
    """
    Returns the name of the warehouse in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_warehouse()).collect()
        >>> assert result is not None
    """
    return builtin("current_warehouse")()


def current_database() -> Column:
    """Returns the name of the database in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_database()).collect()
        >>> assert result is not None
    """
    return builtin("current_database")()


def current_role() -> Column:
    """Returns the name of the role in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_role()).collect()
        >>> assert result is not None
    """
    return builtin("current_role")()


def current_schema() -> Column:
    """Returns the name of the schema in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_schema()).collect()
        >>> assert result is not None
    """
    return builtin("current_schema")()


def current_schemas() -> Column:
    """Returns active search path schemas.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_schemas()).collect()
        >>> assert result is not None
    """
    return builtin("current_schemas")()


def current_region() -> Column:
    """Returns the name of the region for the account where the current user is logged in.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_region()).collect()
        >>> assert result is not None
    """
    return builtin("current_region")()


def current_available_roles() -> Column:
    """Returns a JSON string that lists all roles granted to the current user.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_available_roles()).collect()
        >>> assert result is not None
    """
    return builtin("current_available_roles")()


def add_months(
    date_or_timestamp: ColumnOrName, number_of_months: Union[Column, int]
) -> Column:
    """Adds or subtracts a specified number of months to a date or timestamp, preserving the end-of-month information.

    Example:
        >>> import datetime
        >>> df = session.create_dataframe([datetime.date(2022, 4, 6)], schema=["d"])
        >>> df.select(add_months("d", 4)).collect()[0][0]
        datetime.date(2022, 8, 6)
    """
    c = _to_col_if_str(date_or_timestamp, "add_months")
    return builtin("add_months")(c, number_of_months)


def any_value(e: ColumnOrName) -> Column:
    """Returns a non-deterministic any value for the specified column.
    This is an aggregate and window function.

    Example:
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        >>> result = df.select(any_value("a")).collect()
        >>> assert len(result) == 1  # non-deterministic value in result.
    """
    c = _to_col_if_str(e, "any_value")
    return call_builtin("any_value", c)


def bitnot(e: ColumnOrName) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(bitnot("a")).collect()[0][0]
        -2
    """
    c = _to_col_if_str(e, "bitnot")
    return call_builtin("bitnot", c)


def bitshiftleft(to_shift_column: ColumnOrName, n: Union[Column, int]) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.create_dataframe([2], schema=["a"])
        >>> df.select(bitshiftleft("a", 1)).collect()[0][0]
        4
    """
    c = _to_col_if_str(to_shift_column, "bitshiftleft")
    return call_builtin("bitshiftleft", c, n)


def bitshiftright(to_shift_column: ColumnOrName, n: Union[Column, int]) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.create_dataframe([2], schema=["a"])
        >>> df.select(bitshiftright("a", 1)).collect()[0][0]
        1
    """
    c = _to_col_if_str(to_shift_column, "bitshiftright")
    return call_builtin("bitshiftright", c, n)


def convert_timezone(
    target_timezone: ColumnOrName,
    source_time: ColumnOrName,
    source_timezone: Optional[ColumnOrName] = None,
) -> Column:
    """Converts the given source_time to the target timezone.

    For timezone information, refer to the `Snowflake SQL convert_timezone notes <https://docs.snowflake.com/en/sql-reference/functions/convert_timezone.html#usage-notes>`_

        Args:
            target_timezone: The time zone to which the input timestamp should be converted.=
            source_time: The timestamp to convert. When it's a TIMESTAMP_LTZ, use ``None`` for ``source_timezone``.
            source_timezone: The time zone for the ``source_time``. Required for timestamps with no time zone (i.e. TIMESTAMP_NTZ). Use ``None`` if the timestamps have a time zone (i.e. TIMESTAMP_LTZ). Default is ``None``.

        Note:
            The sequence of the 3 params is different from the SQL function, which two overloads:

              - ``CONVERT_TIMEZONE( <source_tz> , <target_tz> , <source_timestamp_ntz> )``
              - ``CONVERT_TIMEZONE( <target_tz> , <source_timestamp> )``

            The first parameter ``source_tz`` is optional. But in Python an optional argument shouldn't be placed at the first.
            So ``source_timezone`` is after ``source_time``.

        Example:
            >>> import datetime
            >>> from dateutil import tz
            >>> datetime_with_tz = datetime.datetime(2022, 4, 6, 9, 0, 0, tzinfo=tz.tzoffset("myzone", -3600*7))
            >>> datetime_with_no_tz = datetime.datetime(2022, 4, 6, 9, 0, 0)
            >>> df = session.create_dataframe([[datetime_with_tz, datetime_with_no_tz]], schema=["a", "b"])
            >>> result = df.select(convert_timezone(lit("UTC"), col("a")), convert_timezone(lit("UTC"), col("b"), lit("Asia/Shanghai"))).collect()
            >>> result[0][0]
            datetime.datetime(2022, 4, 6, 16, 0, tzinfo=<UTC>)
            >>> result[0][1]
            datetime.datetime(2022, 4, 6, 1, 0)
    """
    source_tz = (
        _to_col_if_str(source_timezone, "convert_timezone")
        if source_timezone is not None
        else None
    )
    target_tz = _to_col_if_str(target_timezone, "convert_timezone")
    source_time_to_convert = _to_col_if_str(source_time, "convert_timezone")

    if source_timezone is None:
        return call_builtin("convert_timezone", target_tz, source_time_to_convert)
    return call_builtin(
        "convert_timezone", source_tz, target_tz, source_time_to_convert
    )


def approx_count_distinct(e: ColumnOrName) -> Column:
    """Uses HyperLogLog to return an approximation of the distinct cardinality of the input (i.e. HLL(col1, col2, ... )
    returns an approximation of COUNT(DISTINCT col1, col2, ... ))."""
    c = _to_col_if_str(e, "approx_count_distinct")
    return builtin("approx_count_distinct")(c)


def avg(e: ColumnOrName) -> Column:
    """Returns the average of non-NULL records. If all records inside a group are NULL,
    the function returns NULL."""
    c = _to_col_if_str(e, "avg")
    return builtin("avg")(c)


def corr(column1: ColumnOrName, column2: ColumnOrName) -> Column:
    """Returns the correlation coefficient for non-null pairs in a group."""
    c1 = _to_col_if_str(column1, "corr")
    c2 = _to_col_if_str(column2, "corr")
    return builtin("corr")(c1, c2)


def count(e: ColumnOrName) -> Column:
    """Returns either the number of non-NULL records for the specified columns, or the
    total number of records."""
    c = _to_col_if_str(e, "count")
    return (
        builtin("count")(Literal(1))
        if isinstance(c._expression, Star)
        else builtin("count")(c._expression)
    )


def count_distinct(*cols: ColumnOrName) -> Column:
    """Returns either the number of non-NULL distinct records for the specified columns,
    or the total number of the distinct records.
    """
    cs = [_to_col_if_str(c, "count_distinct") for c in cols]
    return Column(
        FunctionExpression("count", [c._expression for c in cs], is_distinct=True)
    )


countDistinct = count_distinct


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
    return builtin("max")(c)


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
    return builtin("min")(c)


def mode(e: ColumnOrName) -> Column:
    """Returns the most frequent value for the records in a group. NULL values are ignored.
    If all the values are NULL, or there are 0 rows, then the function returns NULL."""
    c = _to_col_if_str(e, "mode")
    return builtin("mode")(c)


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
    return builtin("sum")(c)


def sum_distinct(e: ColumnOrName) -> Column:
    """Returns the sum of non-NULL distinct records in a group. You can use the
    DISTINCT keyword to compute the sum of unique non-null values. If all records
    inside a group are NULL, the function returns NULL."""
    c = _to_col_if_str(e, "sum_distinct")
    return _call_function("sum", True, c)


def variance(e: ColumnOrName) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned."""
    c = _to_col_if_str(e, "variance")
    return builtin("variance")(c)


def var_samp(e: ColumnOrName) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. Alias of :func:`variance`"""
    c = _to_col_if_str(e, "var_samp")
    return variance(c)


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


def grouping(*cols: ColumnOrName) -> Column:
    """
    Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.

    :func:`grouping_id` is an alias of :func:`grouping`.

    Example::
        >>> from snowflake.snowpark import GroupingSets
        >>> df = session.create_dataframe([[1, 2, 3], [4, 5, 6]],schema=["a", "b", "c"])
        >>> grouping_sets = GroupingSets([col("a")], [col("b")], [col("a"), col("b")])
        >>> df.group_by_grouping_sets(grouping_sets).agg([count("c"), grouping("a"), grouping("b"), grouping("a", "b")]).collect()
        [Row(A=1, B=2, COUNT(C)=1, GROUPING(A)=0, GROUPING(B)=0, GROUPING(A, B)=0), \
Row(A=4, B=5, COUNT(C)=1, GROUPING(A)=0, GROUPING(B)=0, GROUPING(A, B)=0), \
Row(A=1, B=None, COUNT(C)=1, GROUPING(A)=0, GROUPING(B)=1, GROUPING(A, B)=1), \
Row(A=4, B=None, COUNT(C)=1, GROUPING(A)=0, GROUPING(B)=1, GROUPING(A, B)=1), \
Row(A=None, B=2, COUNT(C)=1, GROUPING(A)=1, GROUPING(B)=0, GROUPING(A, B)=2), \
Row(A=None, B=5, COUNT(C)=1, GROUPING(A)=1, GROUPING(B)=0, GROUPING(A, B)=2)]
    """
    columns = [_to_col_if_str(c, "grouping") for c in cols]
    return builtin("grouping")(*columns)


grouping_id = grouping


def coalesce(*e: ColumnOrName) -> Column:
    """Returns the first non-NULL expression among its arguments, or NULL if all its
    arguments are NULL."""
    c = [_to_col_if_str(ex, "coalesce") for ex in e]
    return builtin("coalesce")(*c)


def equal_nan(e: ColumnOrName) -> Column:
    """Return true if the value in the column is not a number (NaN)."""
    c = _to_col_if_str(e, "equal_nan")
    return c.equal_nan()


def is_null(e: ColumnOrName) -> Column:
    """Return true if the value in the column is null."""
    c = _to_col_if_str(e, "is_null")
    return c.is_null()


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
    s = seed if seed is not None else randint(-(2**63), 2**63 - 1)
    return builtin("random")(Literal(s))


def uniform(
    min_: Union[ColumnOrName, int, float],
    max_: Union[ColumnOrName, int, float],
    gen: Union[ColumnOrName, int, float],
) -> Column:
    """
    Returns a uniformly random number.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[1]],
        ...     schema=["a"],
        ... )
        >>> df.select(uniform(1, 100, col("a")).alias("UNIFORM")).collect()
        [Row(UNIFORM=62)]
    """

    def convert_limit_to_col(limit):
        if isinstance(limit, int):
            return lit(limit)
        elif isinstance(limit, float):
            return lit(limit).cast(FloatType())
        return _to_col_if_str(limit, "uniform")

    min_col = convert_limit_to_col(min_)
    max_col = convert_limit_to_col(max_)
    gen_col = (
        lit(gen) if isinstance(gen, (int, float)) else _to_col_if_str(gen, "uniform")
    )
    return builtin("uniform")(min_col, max_col, gen_col)


def to_decimal(e: ColumnOrName, precision: int, scale: int) -> Column:
    """Converts an input expression to a decimal."""
    c = _to_col_if_str(e, "to_decimal")
    return builtin("to_decimal")(c, sql_expr(str(precision)), sql_expr(str(scale)))


def div0(
    dividend: Union[ColumnOrName, int, float], divisor: Union[ColumnOrName, int, float]
) -> Column:
    """Performs division like the division operator (/),
    but returns 0 when the divisor is 0 (rather than reporting an error)."""
    dividend_col = (
        lit(dividend)
        if isinstance(dividend, (int, float))
        else _to_col_if_str(dividend, "div0")
    )
    divisor_col = (
        lit(divisor)
        if isinstance(divisor, (int, float))
        else _to_col_if_str(divisor, "div0")
    )
    return builtin("div0")(dividend_col, divisor_col)


def sqrt(e: ColumnOrName) -> Column:
    """Returns the square-root of a non-negative numeric expression."""
    c = _to_col_if_str(e, "sqrt")
    return builtin("sqrt")(c)


def abs(e: ColumnOrName) -> Column:
    """Returns the absolute value of a numeric expression."""
    c = _to_col_if_str(e, "abs")
    return builtin("abs")(c)


def acos(e: ColumnOrName) -> Column:
    """Computes the inverse cosine (arc cosine) of its input;
    the result is a number in the interval [-pi, pi]."""
    c = _to_col_if_str(e, "acos")
    return builtin("acos")(c)


def asin(e: ColumnOrName) -> Column:
    """Computes the inverse sine (arc sine) of its input;
    the result is a number in the interval [-pi, pi]."""
    c = _to_col_if_str(e, "asin")
    return builtin("asin")(c)


def atan(e: ColumnOrName) -> Column:
    """Computes the inverse tangent (arc tangent) of its input;
    the result is a number in the interval [-pi, pi]."""
    c = _to_col_if_str(e, "atan")
    return builtin("atan")(c)


def atan2(y: ColumnOrName, x: ColumnOrName) -> Column:
    """Computes the inverse tangent (arc tangent) of its input;
    the result is a number in the interval [-pi, pi]."""
    y_col = _to_col_if_str(y, "atan2")
    x_col = _to_col_if_str(x, "atan2")
    return builtin("atan2")(y_col, x_col)


def ceil(e: ColumnOrName) -> Column:
    """Returns values from the specified column rounded to the nearest equal or larger
    integer."""
    c = _to_col_if_str(e, "ceil")
    return builtin("ceil")(c)


def cos(e: ColumnOrName) -> Column:
    """Computes the cosine of its argument; the argument should be expressed in radians."""
    c = _to_col_if_str(e, "cos")
    return builtin("cos")(c)


def cosh(e: ColumnOrName) -> Column:
    """Computes the hyperbolic cosine of its argument."""
    c = _to_col_if_str(e, "cosh")
    return builtin("cosh")(c)


def exp(e: ColumnOrName) -> Column:
    """Computes Euler's number e raised to a floating-point value."""
    c = _to_col_if_str(e, "exp")
    return builtin("exp")(c)


def factorial(e: ColumnOrName) -> Column:
    """Computes the factorial of its input. The input argument must be an integer
    expression in the range of 0 to 33."""
    c = _to_col_if_str(e, "factorial")
    return builtin("factorial")(c)


def floor(e: ColumnOrName) -> Column:
    """Returns values from the specified column rounded to the nearest equal or
    smaller integer."""
    c = _to_col_if_str(e, "floor")
    return builtin("floor")(c)


def sin(e: ColumnOrName) -> Column:
    """Computes the sine of its argument; the argument should be expressed in radians."""
    c = _to_col_if_str(e, "sin")
    return builtin("sin")(c)


def sinh(e: ColumnOrName) -> Column:
    """Computes the hyperbolic sine of its argument."""
    c = _to_col_if_str(e, "sinh")
    return builtin("sinh")(c)


def tan(e: ColumnOrName) -> Column:
    """Computes the tangent of its argument; the argument should be expressed in radians."""
    c = _to_col_if_str(e, "tan")
    return builtin("tan")(c)


def tanh(e: ColumnOrName) -> Column:
    """Computes the hyperbolic tangent of its argument."""
    c = _to_col_if_str(e, "tanh")
    return builtin("tanh")(c)


def degrees(e: ColumnOrName) -> Column:
    """Converts radians to degrees."""
    c = _to_col_if_str(e, "degrees")
    return builtin("degrees")(c)


def radians(e: ColumnOrName) -> Column:
    """Converts degrees to radians."""
    c = _to_col_if_str(e, "radians")
    return builtin("radians")(c)


def md5(e: ColumnOrName) -> Column:
    """Returns a 32-character hex-encoded string containing the 128-bit MD5 message digest."""
    c = _to_col_if_str(e, "md5")
    return builtin("md5")(c)


def sha1(e: ColumnOrName) -> Column:
    """Returns a 40-character hex-encoded string containing the 160-bit SHA-1 message digest."""
    c = _to_col_if_str(e, "sha1")
    return builtin("sha1")(c)


def sha2(e: ColumnOrName, num_bits: int) -> Column:
    """Returns a hex-encoded string containing the N-bit SHA-2 message digest,
    where N is the specified output digest size."""
    permitted_values = [0, 224, 256, 384, 512]
    if num_bits not in permitted_values:
        raise ValueError(
            f"num_bits {num_bits} is not in the permitted values {permitted_values}"
        )
    c = _to_col_if_str(e, "sha2")
    return builtin("sha2")(c, num_bits)


def hash(e: ColumnOrName) -> Column:
    """Returns a signed 64-bit hash value. Note that HASH never returns NULL, even for NULL inputs."""
    c = _to_col_if_str(e, "hash")
    return builtin("hash")(c)


def ascii(e: ColumnOrName) -> Column:
    """Returns the ASCII code for the first character of a string. If the string is empty,
    a value of 0 is returned."""
    c = _to_col_if_str(e, "ascii")
    return builtin("ascii")(c)


def initcap(e: ColumnOrName) -> Column:
    """Returns the input string with the first letter of each word in uppercase
    and the subsequent letters in lowercase."""
    c = _to_col_if_str(e, "initcap")
    return builtin("initcap")(c)


def length(e: ColumnOrName) -> Column:
    """Returns the length of an input string or binary value. For strings,
    the length is the number of characters, and UTF-8 characters are counted as a
    single character. For binary, the length is the number of bytes."""
    c = _to_col_if_str(e, "length")
    return builtin("length")(c)


def lower(e: ColumnOrName) -> Column:
    """Returns the input string with all characters converted to lowercase."""
    c = _to_col_if_str(e, "lower")
    return builtin("lower")(c)


def lpad(e: ColumnOrName, len: Union[Column, int], pad: ColumnOrName) -> Column:
    """Left-pads a string with characters from another string, or left-pads a
    binary value with bytes from another binary value."""
    c = _to_col_if_str(e, "lpad")
    p = _to_col_if_str(pad, "lpad")
    return builtin("lpad")(c, lit(len), p)


def ltrim(e: ColumnOrName, trim_string: Optional[ColumnOrName] = None) -> Column:
    """Removes leading characters, including whitespace, from a string."""
    c = _to_col_if_str(e, "ltrim")
    t = _to_col_if_str(trim_string, "ltrim") if trim_string is not None else None
    return builtin("ltrim")(c, t) if t is not None else builtin("ltrim")(c)


def rpad(e: ColumnOrName, len: Union[Column, int], pad: ColumnOrName) -> Column:
    """Right-pads a string with characters from another string, or right-pads a
    binary value with bytes from another binary value."""
    c = _to_col_if_str(e, "rpad")
    p = _to_col_if_str(pad, "rpad")
    return builtin("rpad")(c, lit(len), p)


def rtrim(e: ColumnOrName, trim_string: Optional[ColumnOrName] = None) -> Column:
    """Removes trailing characters, including whitespace, from a string."""
    c = _to_col_if_str(e, "rtrim")
    t = _to_col_if_str(trim_string, "rtrim") if trim_string is not None else None
    return builtin("rtrim")(c, t) if t is not None else builtin("rtrim")(c)


def repeat(s: ColumnOrName, n: Union[Column, int]) -> Column:
    """Builds a string by repeating the input for the specified number of times."""
    c = _to_col_if_str(s, "rtrim")
    return builtin("repeat")(c, lit(n))


def soundex(e: ColumnOrName) -> Column:
    """Returns a string that contains a phonetic representation of the input string."""
    c = _to_col_if_str(e, "soundex")
    return builtin("soundex")(c)


def trim(e: ColumnOrName, trim_string: Optional[ColumnOrName] = None) -> Column:
    """Removes leading and trailing characters from a string."""
    c = _to_col_if_str(e, "trim")
    t = _to_col_if_str(trim_string, "trim") if trim_string is not None else None
    return builtin("trim")(c, t) if t is not None else builtin("trim")(c)


def upper(e: ColumnOrName) -> Column:
    """Returns the input string with all characters converted to uppercase."""
    c = _to_col_if_str(e, "upper")
    return builtin("upper")(c)


def strtok_to_array(
    text: ColumnOrName, delimiter: Optional[ColumnOrName] = None
) -> Column:
    """
    Tokenizes the given string using the given set of delimiters and returns the tokens as an array.

    If either parameter is a NULL, a NULL is returned. An empty array is returned if tokenization produces no tokens.

    Example::
        >>> df = session.create_dataframe(
        ...     [["a.b.c", "."], ["1,2.3", ","]],
        ...     schema=["text", "delimiter"],
        ... )
        >>> df.select(strtok_to_array("text", "delimiter").alias("TIME_FROM_PARTS")).collect()
        [Row(TIME_FROM_PARTS='[\\n  "a",\\n  "b",\\n  "c"\\n]'), Row(TIME_FROM_PARTS='[\\n  "1",\\n  "2.3"\\n]')]
    """
    t = _to_col_if_str(text, "strtok_to_array")
    d = _to_col_if_str(delimiter, "strtok_to_array") if delimiter else None
    return (
        builtin("strtok_to_array")(t, d) if delimiter else builtin("strtok_to_array")(t)
    )


def log(
    base: Union[ColumnOrName, int, float], x: Union[ColumnOrName, int, float]
) -> Column:
    """Returns the logarithm of a numeric expression."""
    b = lit(base) if isinstance(base, (int, float)) else _to_col_if_str(base, "log")
    arg = lit(x) if isinstance(x, (int, float)) else _to_col_if_str(x, "log")
    return builtin("log")(b, arg)


def pow(
    left: Union[ColumnOrName, int, float], right: Union[ColumnOrName, int, float]
) -> Column:
    """Returns a number (left) raised to the specified power (right)."""
    number = (
        lit(left) if isinstance(left, (int, float)) else _to_col_if_str(left, "pow")
    )
    power = (
        lit(right) if isinstance(right, (int, float)) else _to_col_if_str(right, "pow")
    )
    return builtin("pow")(number, power)


def round(e: ColumnOrName, scale: Union[ColumnOrName, int, float] = 0) -> Column:
    """Returns values from the specified column rounded to the nearest equal or
    smaller integer."""
    c = _to_col_if_str(e, "round")
    scale_col = (
        lit(scale)
        if isinstance(scale, (int, float))
        else _to_col_if_str(scale, "round")
    )
    return builtin("round")(c, scale_col)


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
    empty string.

    Note:
        For ``pos``, 1 is the first character of the string in Snowflake database.

    :func:`substr` is an alias of :func:`substring`.
    """
    s = _to_col_if_str(str, "substring")
    p = pos if isinstance(pos, Column) else lit(pos)
    length = len if isinstance(len, Column) else lit(len)
    return builtin("substring")(s, p, length)


substr = substring


def regexp_count(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    position: Union[Column, int] = 1,
    *parameters: ColumnOrLiteral,
) -> Column:
    """Returns the number of times that a pattern occurs in the subject."""
    sql_func_name = "regexp_count"
    sub = _to_col_if_str(subject, sql_func_name)
    pat = lit(pattern)
    pos = lit(position)

    params = [lit(p) for p in parameters]
    return builtin(sql_func_name)(sub, pat, pos, *params)


def regexp_replace(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    replacement: ColumnOrLiteralStr = "",
    position: Union[Column, int] = 1,
    occurrences: Union[Column, int] = 0,
    *parameters: ColumnOrLiteral,
) -> Column:
    """Returns the subject with the specified pattern (or all occurrences of the pattern) either removed or replaced by a replacement string.
    If no matches are found, returns the original subject.
    """
    sql_func_name = "regexp_replace"
    sub = _to_col_if_str(subject, sql_func_name)
    pat = lit(pattern)
    rep = lit(replacement)
    pos = lit(position)
    occ = lit(occurrences)

    params = [lit(p) for p in parameters]
    return builtin(sql_func_name)(sub, pat, rep, pos, occ, *params)


def replace(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    replacement: ColumnOrLiteralStr = "",
) -> Column:
    """
    Removes all occurrences of a specified subject and optionally replaces them with replacement.
    """
    sql_func_name = "replace"
    sub = _to_col_if_str(subject, sql_func_name)
    pat = lit(pattern)
    rep = lit(replacement)
    return builtin(sql_func_name)(sub, pat, rep)


def charindex(
    target_expr: ColumnOrName,
    source_expr: ColumnOrName,
    position: Optional[Union[Column, int]] = None,
) -> Column:
    """Searches for ``target_expr`` in ``source_expr`` and, if successful,
    returns the position (1-based) of the ``target_expr`` in ``source_expr``."""
    t = _to_col_if_str(target_expr, "charindex")
    s = _to_col_if_str(source_expr, "charindex")
    return (
        builtin("charindex")(t, s, lit(position))
        if position is not None
        else builtin("charindex")(t, s)
    )


def collate(e: Column, collation_spec: str) -> Column:
    """Returns a copy of the original :class:`Column` with the specified ``collation_spec``
    property, rather than the original collation specification property.

    For details, see the Snowflake documentation on
    `collation specifications <https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification>`_.
    """
    c = _to_col_if_str(e, "collate")
    return builtin("collate")(c, collation_spec)


def collation(e: ColumnOrName) -> Column:
    """Returns the collation specification of expr."""
    c = _to_col_if_str(e, "collation")
    return builtin("collation")(c)


def concat(*cols: ColumnOrName) -> Column:
    """Concatenates one or more strings, or concatenates one or more binary values. If any of the values is null, the result is also null."""

    columns = [_to_col_if_str(c, "concat") for c in cols]
    return builtin("concat")(*columns)


def concat_ws(*cols: ColumnOrName) -> Column:
    """Concatenates two or more strings, or concatenates two or more binary values. If any of the values is null, the result is also null.
    The CONCAT_WS operator requires at least two arguments, and uses the first argument to separate all following arguments."""
    columns = [_to_col_if_str(c, "concat_ws") for c in cols]
    return builtin("concat_ws")(*columns)


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


def endswith(col: ColumnOrName, str: ColumnOrName) -> Column:
    """Returns true if col ends with str."""
    c = _to_col_if_str(col, "endswith")
    s = _to_col_if_str(str, "endswith")
    return builtin("endswith")(c, s)


def insert(
    base_expr: ColumnOrName,
    position: Union[Column, int],
    length: Union[Column, int],
    insert_expr: ColumnOrName,
) -> Column:
    """Replaces a substring of the specified length, starting at the specified position,
    with a new string or binary value."""
    b = _to_col_if_str(base_expr, "insert")
    i = _to_col_if_str(insert_expr, "insert")
    return builtin("insert")(b, lit(position), lit(length), i)


def left(str_expr: ColumnOrName, length: Union[Column, int]) -> Column:
    """Returns a left most substring of ``str_expr``."""
    s = _to_col_if_str(str_expr, "left")
    return builtin("left")(s, lit(length))


def right(str_expr: ColumnOrName, length: Union[Column, int]) -> Column:
    """Returns a right most substring of ``str_expr``."""
    s = _to_col_if_str(str_expr, "right")
    return builtin("right")(s, lit(length))


def char(col: ColumnOrName) -> Column:
    """Converts a Unicode code point (including 7-bit ASCII) into the character that
    matches the input Unicode."""
    c = _to_col_if_str(col, "char")
    return builtin("char")(c)


def to_char(c: ColumnOrName, format: Optional[ColumnOrLiteralStr] = None) -> Column:
    """Converts a Unicode code point (including 7-bit ASCII) into the character that
    matches the input Unicode."""
    c = _to_col_if_str(c, "to_char")
    return (
        builtin("to_char")(c, lit(format))
        if format is not None
        else builtin("to_char")(c)
    )


to_varchar = to_char


def to_time(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding time."""
    c = _to_col_if_str(e, "to_time")
    return builtin("to_time")(c, fmt) if fmt is not None else builtin("to_time")(c)


def to_timestamp(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding timestamp."""
    c = _to_col_if_str(e, "to_timestamp")
    return (
        builtin("to_timestamp")(c, fmt)
        if fmt is not None
        else builtin("to_timestamp")(c)
    )


def to_date(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into a date."""
    c = _to_col_if_str(e, "to_date")
    return builtin("to_date")(c, fmt) if fmt is not None else builtin("to_date")(c)


def current_timestamp() -> Column:
    """Returns the current timestamp for the system."""
    return builtin("current_timestamp")()


def current_date() -> Column:
    """Returns the current date for the system."""
    return builtin("current_date")()


def current_time() -> Column:
    """Returns the current time for the system."""
    return builtin("current_time")()


def hour(e: ColumnOrName) -> Column:
    """
    Extracts the hour from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(hour("a")).collect()
        [Row(HOUR("A")=13), Row(HOUR("A")=1)]
    """
    c = _to_col_if_str(e, "hour")
    return builtin("hour")(c)


def last_day(e: ColumnOrName) -> Column:
    """
    Returns the last day of the specified date part for a date or timestamp.
    Commonly used to return the last day of the month for a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(last_day("a")).collect()
        [Row(LAST_DAY("A")=datetime.date(2020, 5, 31)), Row(LAST_DAY("A")=datetime.date(2020, 8, 31))]
    """
    c = _to_col_if_str(e, "last_day")
    return builtin("last_day")(c)


def minute(e: ColumnOrName) -> Column:
    """
    Extracts the minute from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(minute("a")).collect()
        [Row(MINUTE("A")=11), Row(MINUTE("A")=30)]
    """
    c = _to_col_if_str(e, "minute")
    return builtin("minute")(c)


def next_day(date: ColumnOrName, day_of_week: ColumnOrLiteral) -> Column:
    """
    Returns the date of the first specified DOW (day of week) that occurs after the input date.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     (datetime.date.fromisoformat("2020-08-01"), "mo"),
        ...     (datetime.date.fromisoformat("2020-12-01"), "we"),
        ... ], schema=["a", "b"])
        >>> df.select(next_day("a", col("b"))).collect()
        [Row(NEXT_DAY("A", "B")=datetime.date(2020, 8, 3)), Row(NEXT_DAY("A", "B")=datetime.date(2020, 12, 2))]
        >>> df.select(next_day("a", "fr")).collect()
        [Row(NEXT_DAY("A", 'FR')=datetime.date(2020, 8, 7)), Row(NEXT_DAY("A", 'FR')=datetime.date(2020, 12, 4))]
    """
    c = _to_col_if_str(date, "next_day")
    return builtin("next_day")(c, Column._to_expr(day_of_week))


def previous_day(date: ColumnOrName, day_of_week: ColumnOrLiteral) -> Column:
    """
    Returns the date of the first specified DOW (day of week) that occurs before the input date.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     (datetime.date.fromisoformat("2020-08-01"), "mo"),
        ...     (datetime.date.fromisoformat("2020-12-01"), "we"),
        ... ], schema=["a", "b"])
        >>> df.select(previous_day("a", col("b"))).collect()
        [Row(PREVIOUS_DAY("A", "B")=datetime.date(2020, 7, 27)), Row(PREVIOUS_DAY("A", "B")=datetime.date(2020, 11, 25))]
        >>> df.select(previous_day("a", "fr")).collect()
        [Row(PREVIOUS_DAY("A", 'FR')=datetime.date(2020, 7, 31)), Row(PREVIOUS_DAY("A", 'FR')=datetime.date(2020, 11, 27))]
    """
    c = _to_col_if_str(date, "previous_day")
    return builtin("previous_day")(c, Column._to_expr(day_of_week))


def second(e: ColumnOrName) -> Column:
    """
    Extracts the second from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(second("a")).collect()
        [Row(SECOND("A")=20), Row(SECOND("A")=5)]
    """
    c = _to_col_if_str(e, "second")
    return builtin("second")(c)


def month(e: ColumnOrName) -> Column:
    """
    Extracts the month from a date or timestamp.


    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(month("a")).collect()
        [Row(MONTH("A")=5), Row(MONTH("A")=8)]
    """
    c = _to_col_if_str(e, "month")
    return builtin("month")(c)


def monthname(e: ColumnOrName) -> Column:
    """
    Extracts the three-letter month name from the specified date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(monthname("a")).collect()
        [Row(MONTHNAME("A")='May'), Row(MONTHNAME("A")='Aug')]
    """
    c = _to_col_if_str(e, "monthname")
    return builtin("monthname")(c)


def quarter(e: ColumnOrName) -> Column:
    """
    Extracts the quarter from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(quarter("a")).collect()
        [Row(QUARTER("A")=2), Row(QUARTER("A")=3)]
    """
    c = _to_col_if_str(e, "quarter")
    return builtin("quarter")(c)


def year(e: ColumnOrName) -> Column:
    """
    Extracts the year from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(year("a")).collect()
        [Row(YEAR("A")=2020), Row(YEAR("A")=2020)]
    """
    c = _to_col_if_str(e, "year")
    return builtin("year")(c)


def sysdate() -> Column:
    """
    Returns the current timestamp for the system, but in the UTC time zone.

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(sysdate()).collect() is not None
        True
    """
    return builtin("sysdate")()


def months_between(date1: ColumnOrName, date2: ColumnOrName) -> Column:
    """Returns the number of months between two DATE or TIMESTAMP values.
    For example, MONTHS_BETWEEN('2020-02-01'::DATE, '2020-01-01'::DATE) returns 1.0.
    """
    c1 = _to_col_if_str(date1, "months_between")
    c2 = _to_col_if_str(date2, "months_between")
    return builtin("months_between")(c1, c2)


def to_geography(e: ColumnOrName) -> Column:
    """Parses an input and returns a value of type GEOGRAPHY."""
    c = _to_col_if_str(e, "to_geography")
    return builtin("to_geography")(c)


def arrays_overlap(array1: ColumnOrName, array2: ColumnOrName) -> Column:
    """Compares whether two ARRAYs have at least one element in common. Returns TRUE
    if there is at least one element in common; otherwise returns FALSE. The function
    is NULL-safe, meaning it treats NULLs as known values for comparing equality."""
    a1 = _to_col_if_str(array1, "arrays_overlap")
    a2 = _to_col_if_str(array2, "arrays_overlap")
    return builtin("arrays_overlap")(a1, a2)


def array_intersection(array1: ColumnOrName, array2: ColumnOrName) -> Column:
    """Returns an array that contains the matching elements in the two input arrays.

    The function is NULL-safe, meaning it treats NULLs as known values for comparing equality.

    Args:
        array1: An ARRAY that contains elements to be compared.
        array2: An ARRAY that contains elements to be compared."""
    a1 = _to_col_if_str(array1, "array_intersection")
    a2 = _to_col_if_str(array2, "array_intersection")
    return builtin("array_intersection")(a1, a2)


def datediff(part: str, col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Calculates the difference between two date, time, or timestamp columns based on the date or time part requested.

    `Supported date and time parts <https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts>`_

    Example::

        >>> # year difference between two date columns
        >>> import datetime
        >>> date_df = session.create_dataframe([[datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)]], schema=["date_col1", "date_col2"])
        >>> date_df.select(datediff("year", col("date_col1"), col("date_col2")).alias("year_diff")).show()
        ---------------
        |"YEAR_DIFF"  |
        ---------------
        |1            |
        ---------------
        <BLANKLINE>

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


def trunc(e: ColumnOrName, scale: Union[ColumnOrName, int, float] = 0) -> Column:
    """Rounds the input expression down to the nearest (or equal) integer closer to zero,
    or to the nearest equal or smaller value with the specified number of
    places after the decimal point."""
    c = _to_col_if_str(e, "trunc")
    scale_col = (
        lit(scale)
        if isinstance(scale, (int, float))
        else _to_col_if_str(scale, "trunc")
    )
    return builtin("trunc")(c, scale_col)


def dateadd(part: str, col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Adds the specified value for the specified date or time part to date or time expr.

    `Supported date and time parts <https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts>`_

    Example::

        >>> # add one year on dates
        >>> import datetime
        >>> date_df = session.create_dataframe([[datetime.date(2020, 1, 1)]], schema=["date_col"])
        >>> date_df.select(dateadd("year", lit(1), col("date_col")).alias("year_added")).show()
        ----------------
        |"YEAR_ADDED"  |
        ----------------
        |2021-01-01    |
        ----------------
        <BLANKLINE>

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


def date_from_parts(
    y: Union[ColumnOrName, int],
    m: Union[ColumnOrName, int],
    d: Union[ColumnOrName, int],
) -> Column:
    """
    Creates a date from individual numeric components that represent the year, month, and day of the month.

    Example::
        >>> df = session.create_dataframe([[2022, 4, 1]], schema=["year", "month", "day"])
        >>> df.select(date_from_parts("year", "month", "day")).collect()
        [Row(DATE_FROM_PARTS("YEAR", "MONTH", "DAY")=datetime.date(2022, 4, 1))]
        >>> session.table("dual").select(date_from_parts(2022, 4, 1)).collect()
        [Row(DATE_FROM_PARTS(2022, 4, 1)=datetime.date(2022, 4, 1))]
    """
    y_col = _to_col_if_str_or_int(y, "date_from_parts")
    m_col = _to_col_if_str_or_int(m, "date_from_parts")
    d_col = _to_col_if_str_or_int(d, "date_from_parts")
    return builtin("date_from_parts")(y_col, m_col, d_col)


def date_trunc(part: ColumnOrName, expr: ColumnOrName) -> Column:
    """
    Truncates a DATE, TIME, or TIMESTAMP to the specified precision.

    Note that truncation is not the same as extraction. For example:
    - Truncating a timestamp down to the quarter returns the timestamp corresponding to midnight of the first day of the
    quarter for the input timestamp.
    - Extracting the quarter date part from a timestamp returns the quarter number of the year in the timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(date_trunc("YEAR", "a"), date_trunc("MONTH", "a"), date_trunc("DAY", "a")).collect()
        [Row(DATE_TRUNC("YEAR", "A")=datetime.datetime(2020, 1, 1, 0, 0), DATE_TRUNC("MONTH", "A")=datetime.datetime(2020, 5, 1, 0, 0), DATE_TRUNC("DAY", "A")=datetime.datetime(2020, 5, 1, 0, 0))]
        >>> df.select(date_trunc("HOUR", "a"), date_trunc("MINUTE", "a"), date_trunc("SECOND", "a")).collect()
        [Row(DATE_TRUNC("HOUR", "A")=datetime.datetime(2020, 5, 1, 13, 0), DATE_TRUNC("MINUTE", "A")=datetime.datetime(2020, 5, 1, 13, 11), DATE_TRUNC("SECOND", "A")=datetime.datetime(2020, 5, 1, 13, 11, 20))]
        >>> df.select(date_trunc("QUARTER", "a")).collect()
        [Row(DATE_TRUNC("QUARTER", "A")=datetime.datetime(2020, 4, 1, 0, 0))]
    """
    part_col = _to_col_if_str(part, "date_trunc")
    expr_col = _to_col_if_str(expr, "date_trunc")
    return builtin("date_trunc")(part_col, expr_col)


def dayname(e: ColumnOrName) -> Column:
    """
    Extracts the three-letter day-of-week name from the specified date or timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(dayname("a")).collect()
        [Row(DAYNAME("A")='Fri')]
    """
    c = _to_col_if_str(e, "dayname")
    return builtin("dayname")(c)


def dayofmonth(e: ColumnOrName) -> Column:
    """
    Extracts the corresponding day (number) of the month from a date or timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(dayofmonth("a")).collect()
        [Row(DAYOFMONTH("A")=1)]
    """
    c = _to_col_if_str(e, "dayofmonth")
    return builtin("dayofmonth")(c)


def dayofweek(e: ColumnOrName) -> Column:
    """
    Extracts the corresponding day (number) of the week from a date or timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(dayofweek("a")).collect()
        [Row(DAYOFWEEK("A")=5)]
    """
    c = _to_col_if_str(e, "dayofweek")
    return builtin("dayofweek")(c)


def dayofyear(e: ColumnOrName) -> Column:
    """
    Extracts the corresponding day (number) of the year from a date or timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(dayofyear("a")).collect()
        [Row(DAYOFYEAR("A")=122)]
    """
    c = _to_col_if_str(e, "dayofyear")
    return builtin("dayofyear")(c)


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


def _columns_from_timestamp_parts(
    func_name: str, *args: Union[ColumnOrName, int]
) -> Tuple[Column, ...]:
    if len(args) == 3:
        year_ = _to_col_if_str_or_int(args[0], func_name)
        month = _to_col_if_str_or_int(args[1], func_name)
        day = _to_col_if_str_or_int(args[2], func_name)
        return year_, month, day
    elif len(args) == 6:
        year_ = _to_col_if_str_or_int(args[0], func_name)
        month = _to_col_if_str_or_int(args[1], func_name)
        day = _to_col_if_str_or_int(args[2], func_name)
        hour = _to_col_if_str_or_int(args[3], func_name)
        minute = _to_col_if_str_or_int(args[4], func_name)
        second = _to_col_if_str_or_int(args[5], func_name)
        return year_, month, day, hour, minute, second
    else:
        # Should never happen since we only use this internally
        raise ValueError(f"Incorrect number of args passed to {func_name}")


def _timestamp_from_parts_internal(
    func_name: str, *args: Union[ColumnOrName, int], **kwargs: Union[ColumnOrName, int]
) -> Tuple[Column, ...]:
    num_args = len(args)
    if num_args == 2:
        # expression mode
        date_expr = _to_col_if_str(args[0], func_name)
        time_expr = _to_col_if_str(args[1], func_name)
        return date_expr, time_expr
    elif 6 <= num_args <= 8:
        # parts mode
        y, m, d, h, min_, s = _columns_from_timestamp_parts(func_name, *args[:6])
        ns_arg = args[6] if num_args == 7 else kwargs.get("nanoseconds")
        # Timezone is only accepted in timestamp_from_parts function
        tz_arg = args[7] if num_args == 8 else kwargs.get("timezone")
        if tz_arg is not None and func_name != "timestamp_from_parts":
            raise ValueError(f"{func_name} does not accept timezone as an argument")
        ns = None if ns_arg is None else _to_col_if_str_or_int(ns_arg, func_name)
        tz = None if tz_arg is None else _to_col_if_sql_expr(tz_arg, func_name)
        if ns is not None and tz is not None:
            return y, m, d, h, min_, s, ns, tz
        elif ns is not None:
            return y, m, d, h, min_, s, ns
        elif tz is not None:
            # We need to fill in nanoseconds as 0 to make the sql function work
            return y, m, d, h, min_, s, lit(0), tz
        else:
            return y, m, d, h, min_, s
    else:
        raise ValueError(
            f"{func_name} expected 2 or 6 required arguments, got {num_args}"
        )


def time_from_parts(
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
) -> Column:
    """
    Creates a time from individual numeric components.

    TIME_FROM_PARTS is typically used to handle values in "normal" ranges (e.g. hours 0-23, minutes 0-59),
    but it also handles values from outside these ranges. This allows, for example, choosing the N-th minute
    in a day, which can be used to simplify some computations.

    Example::

        >>> df = session.create_dataframe(
        ...     [[11, 11, 0, 987654321], [10, 10, 0, 987654321]],
        ...     schema=["hour", "minute", "second", "nanoseconds"],
        ... )
        >>> df.select(time_from_parts(
        ...     "hour", "minute", "second", nanoseconds="nanoseconds"
        ... ).alias("TIME_FROM_PARTS")).collect()
        [Row(TIME_FROM_PARTS=datetime.time(11, 11, 0, 987654)), Row(TIME_FROM_PARTS=datetime.time(10, 10, 0, 987654))]
    """
    h, m, s = _columns_from_timestamp_parts("time_from_parts", hour, minute, second)
    if nanoseconds:
        return builtin("time_from_parts")(
            h, m, s, _to_col_if_str_or_int(nanoseconds, "time_from_parts")
        )
    else:
        return builtin("time_from_parts")(h, m, s)


@overload
def timestamp_from_parts(date_expr: ColumnOrName, time_expr: ColumnOrName) -> Column:
    ...


@overload
def timestamp_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanosecond: Optional[Union[ColumnOrName, int]] = None,
    timezone: Optional[ColumnOrLiteralStr] = None,
) -> Column:
    ...


def timestamp_from_parts(*args, **kwargs) -> Column:
    """
    Creates a timestamp from individual numeric components. If no time zone is in effect,
    the function can be used to create a timestamp from a date expression and a time expression.

    Example 1::

        >>> df = session.create_dataframe(
        ...     [[2022, 4, 1, 11, 11, 0], [2022, 3, 31, 11, 11, 0]],
        ...     schema=["year", "month", "day", "hour", "minute", "second"],
        ... )
        >>> df.select(timestamp_from_parts(
        ...     "year", "month", "day", "hour", "minute", "second"
        ... ).alias("TIMESTAMP_FROM_PARTS")).collect()
        [Row(TIMESTAMP_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11)), Row(TIMESTAMP_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11))]

    Example 2::

        >>> df = session.create_dataframe(
        ...     [['2022-04-01', '11:11:00'], ['2022-03-31', '11:11:00']],
        ...     schema=["date", "time"]
        ... )
        >>> df.select(
        ...     timestamp_from_parts(to_date("date"), to_time("time")
        ... ).alias("TIMESTAMP_FROM_PARTS")).collect()
        [Row(TIMESTAMP_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11)), Row(TIMESTAMP_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11))]
    """
    return builtin("timestamp_from_parts")(
        *_timestamp_from_parts_internal("timestamp_from_parts", *args, **kwargs)
    )


def timestamp_ltz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
) -> Column:
    """
    Creates a timestamp from individual numeric components.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[2022, 4, 1, 11, 11, 0], [2022, 3, 31, 11, 11, 0]],
        ...     schema=["year", "month", "day", "hour", "minute", "second"],
        ... )
        >>> df.select(timestamp_ltz_from_parts(
        ...     "year", "month", "day", "hour", "minute", "second"
        ... ).alias("TIMESTAMP_LTZ_FROM_PARTS")).collect()
        [Row(TIMESTAMP_LTZ_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11, tzinfo=<DstTzInfo 'America/Los_Angeles' PDT-1 day, 17:00:00 DST>)), Row(TIMESTAMP_LTZ_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11, tzinfo=<DstTzInfo 'America/Los_Angeles' PDT-1 day, 17:00:00 DST>))]
    """
    func_name = "timestamp_ltz_from_parts"
    y, m, d, h, min_, s = _columns_from_timestamp_parts(
        func_name, year, month, day, hour, minute, second
    )
    ns = None if nanoseconds is None else _to_col_if_str_or_int(nanoseconds, func_name)
    return (
        builtin(func_name)(y, m, d, h, min_, s)
        if ns is None
        else builtin(func_name)(y, m, d, h, min_, s, ns)
    )


@overload
def timestamp_ntz_from_parts(
    date_expr: ColumnOrName, time_expr: ColumnOrName
) -> Column:
    ...


@overload
def timestamp_ntz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanosecond: Optional[Union[ColumnOrName, int]] = None,
) -> Column:
    ...


def timestamp_ntz_from_parts(*args, **kwargs) -> Column:
    """
    Creates a timestamp from individual numeric components. The function can be used to
    create a timestamp from a date expression and a time expression.

    Example 1::

        >>> df = session.create_dataframe(
        ...     [[2022, 4, 1, 11, 11, 0], [2022, 3, 31, 11, 11, 0]],
        ...     schema=["year", "month", "day", "hour", "minute", "second"],
        ... )
        >>> df.select(timestamp_ntz_from_parts(
        ...     "year", "month", "day", "hour", "minute", "second"
        ... ).alias("TIMESTAMP_NTZ_FROM_PARTS")).collect()
        [Row(TIMESTAMP_NTZ_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11)), Row(TIMESTAMP_NTZ_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11))]

    Example 2::

        >>> df = session.create_dataframe(
        ...     [['2022-04-01', '11:11:00'], ['2022-03-31', '11:11:00']],
        ...     schema=["date", "time"]
        ... )
        >>> df.select(
        ...     timestamp_ntz_from_parts(to_date("date"), to_time("time")
        ... ).alias("TIMESTAMP_NTZ_FROM_PARTS")).collect()
        [Row(TIMESTAMP_NTZ_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11)), Row(TIMESTAMP_NTZ_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11))]
    """
    return builtin("timestamp_ntz_from_parts")(
        *_timestamp_from_parts_internal("timestamp_ntz_from_parts", *args, **kwargs)
    )


def timestamp_tz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
    timezone: Optional[ColumnOrLiteralStr] = None,
) -> Column:
    """
    Creates a timestamp from individual numeric components and a string timezone.

    Example::

        >>> df = session.create_dataframe(
        ...     [[2022, 4, 1, 11, 11, 0, 'America/Los_Angeles'], [2022, 3, 31, 11, 11, 0, 'America/Los_Angeles']],
        ...     schema=["year", "month", "day", "hour", "minute", "second", "timezone"],
        ... )
        >>> df.select(timestamp_tz_from_parts(
        ...     "year", "month", "day", "hour", "minute", "second", timezone="timezone"
        ... ).alias("TIMESTAMP_TZ_FROM_PARTS")).collect()
        [Row(TIMESTAMP_TZ_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11, tzinfo=pytz.FixedOffset(-420))), Row(TIMESTAMP_TZ_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11, tzinfo=pytz.FixedOffset(-420)))]
    """
    func_name = "timestamp_tz_from_parts"
    y, m, d, h, min_, s = _columns_from_timestamp_parts(
        func_name, year, month, day, hour, minute, second
    )
    ns = None if nanoseconds is None else _to_col_if_str_or_int(nanoseconds, func_name)
    tz = None if timezone is None else _to_col_if_sql_expr(timezone, func_name)
    if nanoseconds is not None and timezone is not None:
        return builtin(func_name)(y, m, d, h, min_, s, ns, tz)
    elif nanoseconds is not None:
        return builtin(func_name)(y, m, d, h, min_, s, ns)
    elif timezone is not None:
        return builtin(func_name)(y, m, d, h, min_, s, lit(0), tz)
    else:
        return builtin(func_name)(y, m, d, h, min_, s)


def weekofyear(e: ColumnOrName) -> Column:
    """
    Extracts the corresponding week (number) of the year from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(weekofyear("a")).collect()
        [Row(WEEKOFYEAR("A")=18)]
    """
    c = _to_col_if_str(e, "weekofyear")
    return builtin("weekofyear")(c)


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


def array_agg(col: ColumnOrName, is_distinct: bool = False) -> Column:
    """Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
    ARRAY is returned."""
    c = _to_col_if_str(col, "array_agg")
    return _call_function("array_agg", is_distinct, c)


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
    uf = _to_col_if_str(update_flag, "update_flag") if update_flag is not None else None
    if uf is not None:
        return builtin("object_insert")(o, k, v, uf)
    else:
        return builtin("object_insert")(o, k, v)


def object_pick(obj: ColumnOrName, key1: ColumnOrName, *keys: ColumnOrName) -> Column:
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


def cast(column: ColumnOrName, to: Union[str, DataType]) -> Column:
    """Converts a value of one data type into another data type.
    The semantics of CAST are the same as the semantics of the corresponding to datatype conversion functions.
    If the cast is not possible, an error is raised."""
    c = _to_col_if_str(column, "cast")
    return c.cast(to)


def try_cast(column: ColumnOrName, to: Union[str, DataType]) -> Column:
    """A special version of CAST for a subset of data type conversions.
    It performs the same operation (i.e. converts a value of one data type into another data type), but returns a NULL value instead of raising an error when the conversion can not be performed.

    The ``column`` argument must be a string column in Snowflake.
    """
    c = _to_col_if_str(column, "try_cast")
    return c.try_cast(to)


def _as_decimal_or_number(
    cast_type: str,
    variant: ColumnOrName,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    """Helper function that casts a VARIANT value to a decimal or number."""
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
    return _as_decimal_or_number("as_decimal", variant, precision, scale)


def as_number(
    variant: ColumnOrName,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    """Casts a VARIANT value to a fixed-point decimal (does not match floating-point values)."""
    return _as_decimal_or_number("as_number", variant, precision, scale)


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


def when(condition: ColumnOrSqlExpr, value: Union[ColumnOrLiteral]) -> CaseExpr:
    """Works like a cascading if-then-else statement.
    A series of conditions are evaluated in sequence.
    When a condition evaluates to TRUE, the evaluation stops and the associated
    result (after THEN) is returned. If none of the conditions evaluate to TRUE,
    then the result after the optional OTHERWISE is returned, if present;
    otherwise NULL is returned.

    Args:
        condition: A :class:`Column` expression or SQL text representing the specified condition.
        value: A :class:`Column` expression or a literal value, which will be returned
            if ``condition`` is true.
    """
    return CaseExpr(
        CaseWhen(
            [
                (
                    _to_col_if_sql_expr(condition, "when")._expression,
                    Column._to_expr(value),
                )
            ]
        )
    )


def iff(
    condition: ColumnOrSqlExpr,
    expr1: Union[ColumnOrLiteral],
    expr2: Union[ColumnOrLiteral],
) -> Column:
    """
    Returns one of two specified expressions, depending on a condition.
    This is equivalent to an ``if-then-else`` expression.

    Args:
        condition: A :class:`Column` expression or SQL text representing the specified condition.
        expr1: A :class:`Column` expression or a literal value, which will be returned
            if ``condition`` is true.
        expr2: A :class:`Column` expression or a literal value, which will be returned
            if ``condition`` is false.
    """
    return builtin("iff")(_to_col_if_sql_expr(condition, "iff"), expr1, expr2)


def in_(
    cols: List[ColumnOrName],
    *vals: Union["snowflake.snowpark.DataFrame", LiteralType, Iterable[LiteralType]],
) -> Column:
    """Returns a conditional expression that you can pass to the filter or where methods to
    perform the equivalent of a WHERE ... IN query that matches rows containing a sequence of
    values.

    The expression evaluates to true if the values in a row matches the values in one of
    the specified sequences.

    The following code returns a DataFrame that contains the rows in which
    the columns `c1` and `c2` contain the values:
    - `1` and `"a"`, or
    - `2` and `"b"`
    This is equivalent to ``SELECT * FROM table WHERE (c1, c2) IN ((1, 'a'), (2, 'b'))``.

    Example::

        >>> df = session.create_dataframe([[1, "a"], [2, "b"], [3, "c"]], schema=["col1", "col2"])
        >>> df.filter(in_([col("col1"), col("col2")], [[1, "a"], [2, "b"]])).show()
        -------------------
        |"COL1"  |"COL2"  |
        -------------------
        |1       |a       |
        |2       |b       |
        -------------------
        <BLANKLINE>

    The following code returns a DataFrame that contains the rows where
    the values of the columns `c1` and `c2` in `df2` match the values of the columns
    `a` and `b` in `df1`. This is equivalent to
    ``SELECT * FROM table2 WHERE (c1, c2) IN (SELECT a, b FROM table1)``.

    Example::

        >>> df1 = session.sql("select 1, 'a'")
        >>> df.filter(in_([col("col1"), col("col2")], df1)).show()
        -------------------
        |"COL1"  |"COL2"  |
        -------------------
        |1       |a       |
        -------------------
        <BLANKLINE>

    Args::
        cols: A list of the columns to compare for the IN operation.
        vals: A list containing the values to compare for the IN operation.
    """
    vals = parse_positional_args_to_list(*vals)
    columns = [_to_col_if_str(c, "in_") for c in cols]
    return Column(MultipleExpression([c._expression for c in columns])).in_(vals)


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
    default_value: Optional[Union[ColumnOrLiteral]] = None,
    ignore_nulls: bool = False,
) -> Column:
    """
    Accesses data in a previous row in the same result set without having to
    join the table to itself.
    """
    c = _to_col_if_str(e, "lag")
    return Column(
        Lag(c._expression, offset, Column._to_expr(default_value), ignore_nulls)
    )


def lead(
    e: ColumnOrName,
    offset: int = 1,
    default_value: Optional[Union[Column, LiteralType]] = None,
    ignore_nulls: bool = False,
) -> Column:
    """
    Accesses data in a subsequent row in the same result set without having to
    join the table to itself.
    """
    c = _to_col_if_str(e, "lead")
    return Column(
        Lead(c._expression, offset, Column._to_expr(default_value), ignore_nulls)
    )


def last_value(
    e: ColumnOrName,
    ignore_nulls: bool = False,
) -> Column:
    """
    Returns the last value within an ordered group of values.
    """
    c = _to_col_if_str(e, "last_value")
    return Column(
        LastValue(c._expression, None, None, ignore_nulls)
    )


def first_value(
    e: ColumnOrName,
    ignore_nulls: bool = False,
) -> Column:
    """
    Returns the first value within an ordered group of values.
    """
    c = _to_col_if_str(e, "last_value")
    return Column(
        FirstValue(c._expression, None, None, ignore_nulls)
    )


def ntile(e: Union[int, ColumnOrName]) -> Column:
    """
    Divides an ordered data set equally into the number of buckets specified by n.
    Buckets are sequentially numbered 1 through n.

    Args:
        e: The desired number of buckets; must be a positive integer value.
    """
    c = _to_col_if_str_or_int(e, "ntile")
    return builtin("ntile")(c)


def percentile_cont(percentile: float) -> Column:
    """
    Return a percentile value based on a continuous distribution of the
    input column. If no input row lies exactly at the desired percentile,
    the result is calculated using linear interpolation of the two nearest
    input values. NULL values are ignored in the calculation.

    Args:
        percentile: the percentile of the value that you want to find.
            The percentile must be a constant between 0.0 and 1.0. For example,
            if you want to find the value at the 90th percentile, specify 0.9.

    Example:

        >>> df = session.create_dataframe([
        ...     (0, 0), (0, 10), (0, 20), (0, 30), (0, 40),
        ...     (1, 10), (1, 20), (2, 10), (2, 20), (2, 25),
        ...     (2, 30), (3, 60), (4, None)
        ... ], schema=["k", "v"])
        >>> df.group_by("k").agg(percentile_cont(0.25).within_group("v").as_("percentile")).sort("k").collect()
        [Row(K=0, PERCENTILE=Decimal('10.000')), \
Row(K=1, PERCENTILE=Decimal('12.500')), \
Row(K=2, PERCENTILE=Decimal('17.500')), \
Row(K=3, PERCENTILE=Decimal('60.000')), \
Row(K=4, PERCENTILE=None)]
    """
    return builtin("percentile_cont")(percentile)


def greatest(*columns: ColumnOrName) -> Column:
    """Returns the largest value from a list of expressions. If any of the argument values is NULL, the result is NULL. GREATEST supports all data types, including VARIANT."""
    c = [_to_col_if_str(ex, "greatest") for ex in columns]
    return builtin("greatest")(*c)


def least(*columns: ColumnOrName) -> Column:
    """Returns the smallest value from a list of expressions. LEAST supports all data types, including VARIANT."""
    c = [_to_col_if_str(ex, "least") for ex in columns]
    return builtin("least")(*c)


def listagg(e: ColumnOrName, delimiter: str = "", is_distinct: bool = False) -> Column:
    """
    Returns the concatenated input values, separated by `delimiter` string.
    See `LISTAGG <https://docs.snowflake.com/en/sql-reference/functions/listagg.html>`_ for details.

    Args:
        e: A :class:`Column` object or column name that determines the values
            to be put into the list.
        delimiter: A string delimiter.
        is_distinct: Whether the input expression is distinct.

    Examples::

        df.group_by(df.col1).agg(listagg(df.col2. ",")).within_group(df.col2.asc())
        df.select(listagg(df["col2"], ",", False)
    """
    c = _to_col_if_str(e, "listagg")
    return Column(ListAgg(c._expression, delimiter, is_distinct))


def when_matched(
    condition: Optional[Column] = None,
) -> "snowflake.snowpark.table.WhenMatchedClause":
    """
    Specifies a matched clause for the :meth:`Table.merge <snowflake.snowpark.Table.merge>` action.
    See :class:`~snowflake.snowpark.table.WhenMatchedClause` for details.
    """
    return snowflake.snowpark.table.WhenMatchedClause(condition)


def when_not_matched(
    condition: Optional[Column] = None,
) -> "snowflake.snowpark.table.WhenNotMatchedClause":
    """
    Specifies a not-matched clause for the :meth:`Table.merge <snowflake.snowpark.Table.merge>` action.
    See :class:`~snowflake.snowpark.table.WhenNotMatchedClause` for details.
    """
    return snowflake.snowpark.table.WhenNotMatchedClause(condition)


def udf(
    func: Optional[Callable] = None,
    *,
    return_type: Optional[DataType] = None,
    input_types: Optional[List[DataType]] = None,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    session: Optional["snowflake.snowpark.session.Session"] = None,
    parallel: int = 4,
    max_batch_size: Optional[int] = None,
    statement_params: Optional[Dict[str, str]] = None,
    source_code_display: bool = True,
) -> Union[UserDefinedFunction, functools.partial]:
    """Registers a Python function as a Snowflake Python UDF and returns the UDF.

    It can be used as either a function call or a decorator. In most cases you work with a single session.
    This function uses that session to register the UDF. If you have multiple sessions, you need to
    explicitly specify the ``session`` parameter of this function. If you have a function and would
    like to register it to multiple databases, use ``session.udf.register`` instead. See examples
    in :class:`~snowflake.snowpark.udf.UDFRegistration`.

    Args:
        func: A Python function used for creating the UDF.
        return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
            type of the UDF. Optional if type hints are provided.
        input_types: A list of :class:`~snowflake.snowpark.types.DataType`
            representing the input data types of the UDF. Optional if
            type hints are provided.
        name: A string or list of strings that specify the name or fully-qualified
            object identifier (database name, schema name, and function name) for
            the UDF in Snowflake, which allows you to call this UDF in a SQL
            command or via :func:`call_udf()`. If it is not provided, a name will
            be automatically generated for the UDF. A name must be specified when
            ``is_permanent`` is ``True``.
        is_permanent: Whether to create a permanent UDF. The default is ``False``.
            If it is ``True``, a valid ``stage_location`` must be provided.
        stage_location: The stage location where the Python file for the UDF
            and its dependencies should be uploaded. The stage location must be specified
            when ``is_permanent`` is ``True``, and it will be ignored when
            ``is_permanent`` is ``False``. It can be any stage other than temporary
            stages and external stages.
        imports: A list of imports that only apply to this UDF. You can use a string to
            represent a file path (similar to the ``path`` argument in
            :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
            strings to represent a file path and an import path (similar to the ``import_path``
            argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDF-level imports
            will override the session-level imports added by
            :meth:`~snowflake.snowpark.Session.add_import`. Note that an empty list means
            no import for this UDF, and ``None`` or not specifying this parameter means using
            session-level imports.
        packages: A list of packages that only apply to this UDF. These UDF-level packages
            will override the session-level packages added by
            :meth:`~snowflake.snowpark.Session.add_packages` and
            :meth:`~snowflake.snowpark.Session.add_requirements`. Note that an empty list means
            no package for this UDF, and ``None`` or not specifying this parameter means using
            session-level packages.
        replace: Whether to replace a UDF that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a UDF with a name that already exists
            results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
            an existing UDF with the same name is overwritten.
        session: Use this session to register the UDF. If it's not specified, the session that you created before calling this function will be used.
            You need to specify this parameter if you have created multiple sessions before calling this method.
        parallel: The number of threads to use for uploading UDF files with the
            `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
            command. The default value is 4 and supported values are from 1 to 99.
            Increasing the number of threads can improve performance when uploading
            large UDF files.
        max_batch_size: The maximum number of rows per input Pandas DataFrame or Pandas Series
            inside a vectorized UDF. Because a vectorized UDF will be executed within a time limit,
            which is `60` seconds, this optional argument can be used to reduce the running time of
            every batch by setting a smaller batch size. Note that setting a larger value does not
            guarantee that Snowflake will encode batches with the specified number of rows. It will
            be ignored when registering a non-vectorized UDF.
        statement_params: Dictionary of statement level parameters to be set while executing this action.
        source_code_display: Display the source code of the UDF `func` as comments in the generated script.
            The source code is dynamically generated therefore it may not be identical to how the
            `func` is originally defined. The default is ``True``.
            If it is ``False``, source code will not be generated or displayed.

    Returns:
        A UDF function that can be called with :class:`~snowflake.snowpark.Column` expressions.

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for UDFs in
        :class:`~snowflake.snowpark.udf.UDFRegistration`.

            - You can use use :attr:`~snowflake.snowpark.types.Variant` to
              annotate a variant, and use :attr:`~snowflake.snowpark.types.Geography`
              to annotate a geography when defining a UDF.

            - You can use use :attr:`~snowflake.snowpark.types.PandasSeries` to annotate
              a Pandas Series, and use :attr:`~snowflake.snowpark.types.PandasDataFrame`
              to annotate a Pandas DataFrame when defining a vectorized UDF.
              Note that they are generic types so you can specify the element type in a
              Pandas Series and DataFrame.

            - :class:`typing.Union` is not a valid type annotation for UDFs,
              but :class:`typing.Optional` can be used to indicate the optional type.

            - Type hints are not supported on functions decorated with decorators.

        2. A temporary UDF (when ``is_permanent`` is ``False``) is scoped to this ``session``
        and all UDF related files will be uploaded to a temporary session stage
        (:func:`session.get_session_stage() <snowflake.snowpark.Session.get_session_stage>`).
        For a permanent UDF, these files will be uploaded to the stage that you provide.

        3. By default, UDF registration fails if a function with the same name is already
        registered. Invoking :func:`udf` with ``replace`` set to ``True`` will overwrite the
        previously registered function.

        4. When registering a vectorized UDF, ``pandas`` library will be added as a package
        automatically, with the latest version on the Snowflake server. If you don't want to
        use this version, you can overwrite it by adding `pandas` with specific version
        requirement using ``package`` argument or :meth:`~snowflake.snowpark.Session.add_packages`.

    See Also:
        :class:`~snowflake.snowpark.udf.UDFRegistration`
    """
    session = session or snowflake.snowpark.session._get_active_session()
    if func is None:
        return functools.partial(
            session.udf.register,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
            max_batch_size=max_batch_size,
            statement_params=statement_params,
            source_code_display=source_code_display,
        )
    else:
        return session.udf.register(
            func,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
            max_batch_size=max_batch_size,
            statement_params=statement_params,
            source_code_display=source_code_display,
        )


def udtf(
    handler: Optional[Callable] = None,
    *,
    output_schema: Union[StructType, List[str]],
    input_types: Optional[List[DataType]] = None,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    session: Optional["snowflake.snowpark.session.Session"] = None,
    parallel: int = 4,
    statement_params: Optional[Dict[str, str]] = None,
) -> Union[UserDefinedTableFunction, functools.partial]:
    """Registers a Python class as a Snowflake Python UDTF and returns the UDTF.

    It can be used as either a function call or a decorator. In most cases you work with a single session.
    This function uses that session to register the UDTF. If you have multiple sessions, you need to
    explicitly specify the ``session`` parameter of this function. If you have a function and would
    like to register it to multiple databases, use ``session.udtf.register`` instead. See examples
    in :class:`~snowflake.snowpark.udtf.UDTFRegistration`.

    Args:
        handler: A Python class used for creating the UDTF.
        output_schema: A list of column names, or a :class:`~snowflake.snowpark.types.StructType` instance that represents the table function's columns.
         If a list of column names is provided, the ``process`` method of the handler class must have return type hints to indicate the output schema data types.
        input_types: A list of :class:`~snowflake.snowpark.types.DataType`
            representing the input data types of the UDTF. Optional if
            type hints are provided.
        name: A string or list of strings that specify the name or fully-qualified
            object identifier (database name, schema name, and function name) for
            the UDTF in Snowflake, which allows you to call this UDTF in a SQL
            command or via :func:`call_udtf()`. If it is not provided, a name will
            be automatically generated for the UDTF. A name must be specified when
            ``is_permanent`` is ``True``.
        is_permanent: Whether to create a permanent UDTF. The default is ``False``.
            If it is ``True``, a valid ``stage_location`` must be provided.
        stage_location: The stage location where the Python file for the UDTF
            and its dependencies should be uploaded. The stage location must be specified
            when ``is_permanent`` is ``True``, and it will be ignored when
            ``is_permanent`` is ``False``. It can be any stage other than temporary
            stages and external stages.
        imports: A list of imports that only apply to this UDTF. You can use a string to
            represent a file path (similar to the ``path`` argument in
            :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
            strings to represent a file path and an import path (similar to the ``import_path``
            argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDTF-level imports
            will override the session-level imports added by
            :meth:`~snowflake.snowpark.Session.add_import`.
        packages: A list of packages that only apply to this UDTF. These UDTF-level packages
            will override the session-level packages added by
            :meth:`~snowflake.snowpark.Session.add_packages` and
            :meth:`~snowflake.snowpark.Session.add_requirements`.
        replace: Whether to replace a UDTF that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a UDTF with a name that already exists
            results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
            an existing UDTF with the same name is overwritten.
        session: Use this session to register the UDTF. If it's not specified, the session that you created before calling this function will be used.
            You need to specify this parameter if you have created multiple sessions before calling this method.
        parallel: The number of threads to use for uploading UDTF files with the
            `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
            command. The default value is 4 and supported values are from 1 to 99.
            Increasing the number of threads can improve performance when uploading
            large UDTF files.
        statement_params: Dictionary of statement level parameters to be set while executing this action.

    Returns:
        A UDTF function that can be called with :class:`~snowflake.snowpark.Column` expressions.

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for UDTFs in
        :class:`~snowflake.snowpark.udtf.UDTFRegistration`.

            - You can use use :attr:`~snowflake.snowpark.types.Variant` to
              annotate a variant, and use :attr:`~snowflake.snowpark.types.Geography`
              to annotate a geography when defining a UDTF.

            - :class:`typing.Union` is not a valid type annotation for UDTFs,
              but :class:`typing.Optional` can be used to indicate the optional type.

            - Type hints are not supported on functions decorated with decorators.

        2. A temporary UDTF (when ``is_permanent`` is ``False``) is scoped to this ``session``
        and all UDTF related files will be uploaded to a temporary session stage
        (:func:`session.get_session_stage() <snowflake.snowpark.Session.get_session_stage>`).
        For a permanent UDTF, these files will be uploaded to the stage that you specify.

        3. By default, UDTF registration fails if a function with the same name is already
        registered. Invoking :func:`udtf` with ``replace`` set to ``True`` will overwrite the
        previously registered function.

    See Also:
        :class:`~snowflake.snowpark.udtf.UDTFRegistration`
    """
    session = session or snowflake.snowpark.session._get_active_session()
    if handler is None:
        return functools.partial(
            session.udtf.register,
            output_schema=output_schema,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
            statement_params=statement_params,
        )
    else:
        return session.udtf.register(
            handler,
            output_schema=output_schema,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
            statement_params=statement_params,
        )


def pandas_udf(
    func: Optional[Callable] = None,
    *,
    return_type: Optional[DataType] = None,
    input_types: Optional[List[DataType]] = None,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    session: Optional["snowflake.snowpark.session.Session"] = None,
    parallel: int = 4,
    max_batch_size: Optional[int] = None,
    statement_params: Optional[Dict[str, str]] = None,
    source_code_display: bool = True,
) -> Union[UserDefinedFunction, functools.partial]:
    """
    Registers a Python function as a vectorized UDF and returns the UDF.
    The arguments, return value and usage of this function are exactly the same as
    :func:`udf`, but this function can only be used for registering vectorized UDFs.
    See examples in :class:`~snowflake.snowpark.udf.UDFRegistration`.

    See Also:
        - :func:`udf`
        - :meth:`UDFRegistration.register() <snowflake.snowpark.udf.UDFRegistration.register>`
    """
    session = session or snowflake.snowpark.session._get_active_session()
    if func is None:
        return functools.partial(
            session.udf.register,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
            max_batch_size=max_batch_size,
            _from_pandas_udf_function=True,
            statement_params=statement_params,
            source_code_display=source_code_display,
        )
    else:
        return session.udf.register(
            func,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
            max_batch_size=max_batch_size,
            _from_pandas_udf_function=True,
            statement_params=statement_params,
            source_code_display=source_code_display,
        )


def call_udf(
    udf_name: str,
    *args: ColumnOrLiteral,
) -> Column:
    """Calls a user-defined function (UDF) by name.

    Args:
        udf_name: The name of UDF in Snowflake.
        args: Arguments can be in two types:

            - :class:`~snowflake.snowpark.Column`, or
            - Basic Python types, which are converted to Snowpark literals.

    Example::
        >>> from snowflake.snowpark.types import IntegerType
        >>> udf_def = session.udf.register(lambda x, y: x + y, name="add_columns", input_types=[IntegerType(), IntegerType()], return_type=IntegerType(), replace=True)
        >>> df = session.create_dataframe([[1, 2]], schema=["a", "b"])
        >>> df.select(call_udf("add_columns", col("a"), col("b"))).show()
        -------------------------------
        |"ADD_COLUMNS(""A"", ""B"")"  |
        -------------------------------
        |3                            |
        -------------------------------
        <BLANKLINE>
    """

    validate_object_name(udf_name)
    return _call_function(udf_name, False, *args, api_call_source="functions.call_udf")


def call_table_function(
    function_name: str, *args: ColumnOrLiteral, **kwargs: ColumnOrLiteral
) -> "snowflake.snowpark.table_function.TableFunctionCall":
    """Invokes a Snowflake table function, including system-defined table functions and user-defined table functions.

    It returns a :meth:`~snowflake.snowpark.table_function.TableFunctionCall` so you can specify the partition clause.

    Args:
        function_name: The name of the table function.
        args: The positional arguments of the table function.
        **kwargs: The named arguments of the table function. Some table functions (e.g., ``flatten``) have named arguments instead of positional ones.

    Example:
            >>> from snowflake.snowpark.functions import lit
            >>> session.table_function(call_table_function("split_to_table", lit("split words to table"), lit(" ")).over()).collect()
            [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]
    """
    return snowflake.snowpark.table_function.TableFunctionCall(
        function_name, *args, **kwargs
    )


def table_function(function_name: str) -> Callable:
    """Create a function object to invoke a Snowflake table function.

    Args:
        function_name: The name of the table function.

    Example:
            >>> from snowflake.snowpark.functions import lit
            >>> split_to_table = table_function("split_to_table")
            >>> session.table_function(split_to_table(lit("split words to table"), lit(" ")).over()).collect()
            [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]
    """
    return lambda *args, **kwargs: call_table_function(function_name, *args, **kwargs)


def call_function(function_name: str, *args: ColumnOrLiteral) -> Column:
    """Invokes a Snowflake `system-defined function <https://docs.snowflake.com/en/sql-reference-functions.html>`_ (built-in function) with the specified name
    and arguments.

    Args:
        function_name: The name of built-in function in Snowflake
        args: Arguments can be in two types:

            - :class:`~snowflake.snowpark.Column`, or
            - Basic Python types, which are converted to Snowpark literals.

    Example::
        >>> df = session.create_dataframe([1, 2, 3, 4], schema=["a"])  # a single column with 4 rows
        >>> df.select(call_function("avg", col("a"))).show()
        ----------------
        |"AVG(""A"")"  |
        ----------------
        |2.500000      |
        ----------------
        <BLANKLINE>

    """

    return _call_function(function_name, False, *args)


def function(function_name: str) -> Callable:
    """
    Function object to invoke a Snowflake `system-defined function <https://docs.snowflake.com/en/sql-reference-functions.html>`_ (built-in function). Use this to invoke
    any built-in functions not explicitly listed in this object.

    Args:
        function_name: The name of built-in function in Snowflake.

    Returns:
        A :class:`Callable` object for calling a Snowflake system-defined function.

    Example::
        >>> df = session.create_dataframe([1, 2, 3, 4], schema=["a"])  # a single column with 4 rows
        >>> df.select(call_function("avg", col("a"))).show()
        ----------------
        |"AVG(""A"")"  |
        ----------------
        |2.500000      |
        ----------------
        <BLANKLINE>
        >>> my_avg = function('avg')
        >>> df.select(my_avg(col("a"))).show()
        ----------------
        |"AVG(""A"")"  |
        ----------------
        |2.500000      |
        ----------------
        <BLANKLINE>
    """
    return lambda *args: call_function(function_name, *args)


call_builtin = call_function
builtin = function


def _call_function(
    name: str,
    is_distinct: bool = False,
    *args: ColumnOrLiteral,
    api_call_source: Optional[str] = None,
) -> Column:
    expressions = [Column._to_expr(arg) for arg in parse_positional_args_to_list(*args)]
    return Column(
        FunctionExpression(
            name, expressions, is_distinct=is_distinct, api_call_source=api_call_source
        )
    )


def sproc(
    func: Optional[Callable] = None,
    *,
    return_type: Optional[DataType] = None,
    input_types: Optional[List[DataType]] = None,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    session: Optional["snowflake.snowpark.Session"] = None,
    parallel: int = 4,
    statement_params: Optional[Dict[str, str]] = None,
    execute_as: typing.Literal["caller", "owner"] = "owner",
) -> Union[StoredProcedure, functools.partial]:
    """Registers a Python function as a Snowflake Python stored procedure and returns the stored procedure.

    It can be used as either a function call or a decorator. In most cases you work with a single session.
    This function uses that session to register the stored procedure. If you have multiple sessions, you need to
    explicitly specify the ``session`` parameter of this function. If you have a function and would
    like to register it to multiple databases, use ``session.sproc.register`` instead. See examples
    in :class:`~snowflake.snowpark.stored_procedure.StoredProcedureRegistration`.

    Note that the first parameter of your function should be a snowpark Session. Also, you need to add
    `snowflake-snowpark-python` package (version >= 0.4.0) to your session before trying to create a
    stored procedure.

    Args:
        func: A Python function used for creating the stored procedure.
        return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
            type of the stored procedure. Optional if type hints are provided.
        input_types: A list of :class:`~snowflake.snowpark.types.DataType`
            representing the input data types of the stored procedure. Optional if
            type hints are provided.
        name: A string or list of strings that specify the name or fully-qualified
            object identifier (database name, schema name, and function name) for
            the stored procedure in Snowflake, which allows you to call this stored procedure in a SQL
            command or via :func:`session.call()`. If it is not provided, a name will
            be automatically generated for the stored procedure. A name must be specified when
            ``is_permanent`` is ``True``.
        is_permanent: Whether to create a permanent stored procedure. The default is ``False``.
            If it is ``True``, a valid ``stage_location`` must be provided.
        stage_location: The stage location where the Python file for the stored procedure
            and its dependencies should be uploaded. The stage location must be specified
            when ``is_permanent`` is ``True``, and it will be ignored when
            ``is_permanent`` is ``False``. It can be any stage other than temporary
            stages and external stages.
        imports: A list of imports that only apply to this stored procedure. You can use a string to
            represent a file path (similar to the ``path`` argument in
            :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
            strings to represent a file path and an import path (similar to the ``import_path``
            argument in :meth:`~snowflake.snowpark.Session.add_import`). These stored-proc-level imports
            will override the session-level imports added by
            :meth:`~snowflake.snowpark.Session.add_import`.
        packages: A list of packages that only apply to this stored procedure. These stored-proc-level packages
            will override the session-level packages added by
            :meth:`~snowflake.snowpark.Session.add_packages` and
            :meth:`~snowflake.snowpark.Session.add_requirements`.
        replace: Whether to replace a stored procedure that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a stored procedure with a name that already exists
            results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
            an existing stored procedure with the same name is overwritten.
        session: Use this session to register the stored procedure. If it's not specified, the session that you created before calling this function will be used.
            You need to specify this parameter if you have created multiple sessions before calling this method.
        parallel: The number of threads to use for uploading stored procedure files with the
            `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
            command. The default value is 4 and supported values are from 1 to 99.
            Increasing the number of threads can improve performance when uploading
            large stored procedure files.
        execute_as: What permissions should the procedure have while executing. This
            supports caller, or owner for now. See `owner and caller rights <https://docs.snowflake.com/en/sql-reference/stored-procedures-rights.html>`_
            for more information.
        statement_params: Dictionary of statement level parameters to be set while executing this action.

    Returns:
        A stored procedure function that can be called with python value.

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for stored procedure in
        :class:`~snowflake.snowpark.stored_procedure.StoredProcedureRegistration`.

            - You can use :attr:`~snowflake.snowpark.types.Variant` to
              annotate a variant, and use :attr:`~snowflake.snowpark.types.Geography`
              to annotate a geography when defining a stored procedure.

            - :class:`typing.Union` is not a valid type annotation for stored procedures,
              but :class:`typing.Optional` can be used to indicate the optional type.

        2. A temporary stored procedure (when ``is_permanent`` is ``False``) is scoped to this ``session``
        and all stored procedure related files will be uploaded to a temporary session stage
        (:func:`session.get_session_stage() <snowflake.snowpark.Session.get_session_stage>`).
        For a permanent stored procedure, these files will be uploaded to the stage that you provide.

        3. By default, stored procedure registration fails if a function with the same name is already
        registered. Invoking :func:`sproc` with ``replace`` set to ``True`` will overwrite the
        previously registered function.

    See Also:
        :class:`~snowflake.snowpark.stored_procedure.StoredProcedureRegistration`
    """
    session = session or snowflake.snowpark.session._get_active_session()
    if func is None:
        return functools.partial(
            session.sproc.register,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
            statement_params=statement_params,
            execute_as=execute_as,
        )
    else:
        return session.sproc.register(
            func,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
            statement_params=statement_params,
            execute_as=execute_as,
        )
