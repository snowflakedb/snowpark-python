#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from .column import Column
from .internal.sp_expressions import Expression as SPExpression, Literal as SPLiteral, \
    UnresolvedFunction as SPUnresolvedFunction
from typing import Optional


def col(col_name) -> Column:
    """Returns the [[Column]] with the specified name. """
    return Column(col_name)


def column(col_name) -> Column:
    """Returns a [[Column]] with the specified name. Alias for col. """
    return Column(col_name)


def sql_expr(sql: str) -> Column:
    """Creates a [[Column]] expression from raw SQL text.
    Note that the function does not interpret or check the SQL text. """
    return Column.expr(sql)


def parse_json(s: Column) -> Column:
    """Parse the value of the specified column as a JSON string and returns the resulting JSON document. """
    return builtin("parse_json")(s)


def to_decimal(expr: Column, precision: int, scale: int) -> Column:
    """Converts an input expression to a decimal. """
    return builtin("to_decimal")(expr, sql_expr(str(precision)), sql_expr(str(scale)))


def to_time(s: Column, fmt: Optional['Column'] = None) -> Column:
    """Converts an input expression into the corresponding time. """
    return builtin("to_time")(s, fmt) if fmt else builtin("to_time")(s)


def to_timestamp(s: Column, fmt: Optional['Column'] = None) -> Column:
    """Converts an input expression into the corresponding timestamp. """
    return builtin("to_timestamp")(s, fmt) if fmt else builtin("to_timestamp")(s)


def to_date(s: Column, fmt: Optional['Column'] = None) -> Column:
    """Converts an input expression into a date. """
    return builtin("to_date")(s, fmt) if fmt else builtin("to_date")(s)


def to_array(s: Column) -> Column:
    """Converts any value to an ARRAY value or NULL (if input is NULL). """
    return builtin("to_array")(s)


def to_variant(s: Column) -> Column:
    """Converts any value to a VARIANT value or NULL (if input is NULL). """
    return builtin("to_variant")(s)


def to_object(s: Column) -> Column:
    """Converts any value to a OBJECT value or NULL (if input is NULL). """
    return builtin("to_object")(s)


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

    return Column(SPUnresolvedFunction(function_name, sp_expressions, is_distinct=False))


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
