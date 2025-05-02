#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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

Similarly, to call a table function, you can use :func:`~snowflake.snowpark.functions.table_function`, or :func:`~snowflake.snowpark.functions.call_table_function`.

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
import sys
import typing
from functools import reduce
from random import randint
from types import ModuleType
from typing import Callable, Dict, List, Optional, Tuple, Union, overload

import snowflake.snowpark
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
import snowflake.snowpark.table_function
from snowflake.snowpark._internal.analyzer.expression import (
    CaseWhen,
    FunctionExpression,
    Interval,
    ListAgg,
    Literal,
    MultipleExpression,
    Star,
)
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.analyzer.window_expression import (
    FirstValue,
    Lag,
    LastValue,
    Lead,
    NthValue,
)
from snowflake.snowpark._internal.ast.utils import (
    build_builtin_fn_apply,
    build_call_table_function_apply,
    build_expr_from_snowpark_column_or_python_val,
    build_expr_from_snowpark_column_or_sql_str,
    with_src_position,
    build_function_expr,
)
from snowflake.snowpark._internal.type_utils import (
    ColumnOrLiteral,
    ColumnOrLiteralStr,
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
    type_string_to_type_object,
)
from snowflake.snowpark._internal.udf_utils import check_decorator_args
from snowflake.snowpark._internal.utils import (
    parse_duration_string,
    parse_positional_args_to_list,
    publicapi,
    validate_object_name,
    check_create_map_parameter,
    deprecated,
    private_preview,
)
from snowflake.snowpark.column import (
    CaseExpr,
    Column,
    _to_col_if_lit,
    _to_col_if_sql_expr,
    _to_col_if_str,
    _to_col_if_str_or_int,
)
from snowflake.snowpark.stored_procedure import (
    StoredProcedure,
    StoredProcedureRegistration,
)
from snowflake.snowpark.types import (
    ArrayType,
    DataType,
    FloatType,
    PandasDataFrameType,
    StringType,
    StructType,
    TimestampTimeZone,
    TimestampType,
)
from snowflake.snowpark.udaf import UDAFRegistration, UserDefinedAggregateFunction
from snowflake.snowpark.udf import UDFRegistration, UserDefinedFunction
from snowflake.snowpark.udtf import UDTFRegistration, UserDefinedTableFunction

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


# check function to allow test_dataframe_alias_negative to pass in AST mode.
def _check_column_parameters(name1: str, name2: Optional[str]) -> None:
    if not isinstance(name1, str):
        raise ValueError(
            f"Expects first argument to be of type str, got {type(name1)}."
        )

    if name2 is not None and not isinstance(name2, str):
        raise ValueError(
            f"Expects second argument to be of type str or None, got {type(name1)}."
        )


@overload
@publicapi
def col(
    col_name: str, _emit_ast: bool = True, *, _is_qualified_name: bool = False
) -> Column:
    """Returns the :class:`~snowflake.snowpark.Column` with the specified name.

    Args:
        col_name: The name of the column.

    Example::
        >>> df = session.sql("select 1 as a")
        >>> df.select(col("a")).collect()
        [Row(A=1)]
    """
    ...  # pragma: no cover


@overload
@publicapi
def col(
    df_alias: str,
    col_name: str,
    _emit_ast: bool = True,
    *,
    _is_qualified_name: bool = False,
) -> Column:
    """Returns the :class:`~snowflake.snowpark.Column` with the specified dataframe alias and column name.

    Example::
        >>> df = session.sql("select 1 as a")
        >>> df.alias("df").select(col("df", "a")).collect()
        [Row(A=1)]
    """
    ...  # pragma: no cover


@publicapi
def col(
    name1: str,
    name2: Optional[str] = None,
    _emit_ast: bool = True,
    *,
    _is_qualified_name: bool = False,
) -> Column:

    _check_column_parameters(name1, name2)

    if name2 is None:
        return Column(
            name1,
            _is_qualified_name=_is_qualified_name,
            _emit_ast=_emit_ast,
            _caller_name="col",
        )
    else:
        return Column(
            name1,
            name2,
            _is_qualified_name=_is_qualified_name,
            _emit_ast=_emit_ast,
            _caller_name="col",
        )


@overload
@publicapi
def column(
    col_name: str, _emit_ast: bool = True, *, _is_qualified_name: bool = False
) -> Column:
    """Returns a :class:`~snowflake.snowpark.Column` with the specified name. Alias for col.

    Args:
         col_name: The name of the column.

    Example::
         >>> df = session.sql("select 1 as a")
         >>> df.select(column("a")).collect()
         [Row(A=1)]
    """
    ...  # pragma: no cover


@overload
@publicapi
def column(
    df_alias: str,
    col_name: str,
    _emit_ast: bool = True,
    *,
    _is_qualified_name: bool = False,
) -> Column:
    """Returns a :class:`~snowflake.snowpark.Column` with the specified name and dataframe alias name. Alias for col.

    Example::
        >>> df = session.sql("select 1 as a")
        >>> df.alias("df").select(column("df", "a")).collect()
        [Row(A=1)]
    """
    ...  # pragma: no cover


@publicapi
def column(
    name1: str,
    name2: Optional[str] = None,
    _emit_ast: bool = True,
    *,
    _is_qualified_name: bool = False,
) -> Column:
    _check_column_parameters(name1, name2)

    if name2 is None:
        return Column(
            name1,
            _is_qualified_name=_is_qualified_name,
            _emit_ast=_emit_ast,
            _caller_name="column",
        )
    else:
        return Column(
            name1,
            name2,
            _is_qualified_name=_is_qualified_name,
            _emit_ast=_emit_ast,
            _caller_name="column",
        )


@publicapi
def lit(
    literal: ColumnOrLiteral,
    datatype: Optional[DataType] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Creates a :class:`~snowflake.snowpark.Column` expression for a literal value.
    It supports basic Python data types, including ``int``, ``float``, ``str``,
    ``bool``, ``bytes``, ``bytearray``, ``datetime.time``, ``datetime.date``,
    ``datetime.datetime`` and ``decimal.Decimal``. Also, it supports Python structured data types,
    including ``list``, ``tuple`` and ``dict``, but this container must
    be JSON serializable. If a ``Column`` object is passed, it is returned as is.

    Example::

        >>> import datetime
        >>> columns = [lit(1), lit("1"), lit(1.0), lit(True), lit(b'snow'), lit(datetime.date(2023, 2, 2)), lit([1, 2]), lit({"snow": "flake"}), lit(lit(1))]
        >>> session.create_dataframe([[]]).select([c.as_(str(i)) for i, c in enumerate(columns)]).show()
        ---------------------------------------------------------------------------------------------
        |"0"  |"1"  |"2"  |"3"   |"4"                 |"5"         |"6"   |"7"                |"8"  |
        ---------------------------------------------------------------------------------------------
        |1    |1    |1.0  |True  |bytearray(b'snow')  |2023-02-02  |[     |{                  |1    |
        |     |     |     |      |                    |            |  1,  |  "snow": "flake"  |     |
        |     |     |     |      |                    |            |  2   |}                  |     |
        |     |     |     |      |                    |            |]     |                   |     |
        ---------------------------------------------------------------------------------------------
        <BLANKLINE>
    """

    if _emit_ast:
        ast = proto.Expr()
        if datatype is None:
            build_builtin_fn_apply(ast, "lit", literal)
        else:
            build_builtin_fn_apply(ast, "lit", literal, datatype)

        if isinstance(literal, Column):
            # Create new Column, and assign expression of current Column object.
            # This will encode AST correctly.
            c = Column("", _emit_ast=False)
            c._expression = literal._expression
            c._ast = ast
            return c
        return Column(Literal(literal, datatype=datatype), _ast=ast, _emit_ast=True)

    if isinstance(literal, Column):
        return literal

    return Column(Literal(literal, datatype=datatype), _ast=None, _emit_ast=False)


@publicapi
def sql_expr(sql: str, _emit_ast: bool = True) -> Column:
    """Creates a :class:`~snowflake.snowpark.Column` expression from raw SQL text.
    Note that the function does not interpret or check the SQL text.

    Example::
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
        >>> df.select(sql_expr("a + 1").as_("c"), sql_expr("a = 1").as_("d")).collect()  # use SQL expression
        [Row(C=2, D=True), Row(C=4, D=False)]
    """
    ast = None
    if _emit_ast:
        sql_expr_ast = proto.Expr()
        ast = with_src_position(sql_expr_ast.sql_expr)
        ast.sql = sql

        # Capture with ApplyFn in order to restore sql_expr(...) function.
        ast = proto.Expr()
        build_builtin_fn_apply(ast, "sql_expr", sql_expr_ast)

    return Column._expr(sql, ast=ast)


@publicapi
def system_reference(
    object_type: str,
    object_identifier: str,
    scope: str = "CALL",
    privileges: Optional[List[str]] = None,
    _emit_ast: bool = True,
):
    """
    Returns a reference to an object (a table, view, or function). When you execute SQL actions on a
    reference to an object, the actions are performed using the role of the user who created the
    reference.

    Example::
        >>> df = session.create_dataframe([(1,)], schema=["A"])
        >>> df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
        >>> df.select(substr(system_reference("table", "my_table"), 1, 14).alias("identifier")).collect()
        [Row(IDENTIFIER='ENT_REF_TABLE_')]
    """
    privileges = privileges or []
    ast = (
        build_function_expr(
            "system_reference",
            [object_type, object_identifier, scope] + privileges,
        )
        if _emit_ast
        else None
    )
    return _call_function(
        "system$reference",
        object_type,
        object_identifier,
        scope,
        *privileges,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def current_session(_emit_ast: bool = True) -> Column:
    """
    Returns a unique system identifier for the Snowflake session corresponding to the present connection.
    This will generally be a system-generated alphanumeric string. It is NOT derived from the user name or user account.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_session()).collect()
        >>> assert result[0]['CURRENT_SESSION()'] is not None
    """
    return _call_function("current_session", _emit_ast=_emit_ast)


@publicapi
def current_statement(_emit_ast: bool = True) -> Column:
    """
    Returns the SQL text of the statement that is currently executing.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> session.create_dataframe([1]).select(current_statement()).collect()
        [Row(CURRENT_STATEMENT()='SELECT current_statement() FROM ( SELECT "_1" FROM ( SELECT $1 AS "_1" FROM  VALUES (1 :: INT)))')]
    """
    return _call_function("current_statement", _emit_ast=_emit_ast)


@publicapi
def current_user(_emit_ast: bool = True) -> Column:
    """
    Returns the name of the user currently logged into the system.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_user()).collect()
        >>> assert result[0]['CURRENT_USER()'] is not None
    """
    return _call_function("current_user", _emit_ast=_emit_ast)


@publicapi
def current_version(_emit_ast: bool = True) -> Column:
    """
    Returns the current Snowflake version.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_version()).collect()
        >>> assert result[0]['CURRENT_VERSION()'] is not None
    """
    return _call_function("current_version", _emit_ast=_emit_ast)


@publicapi
def current_warehouse(_emit_ast: bool = True) -> Column:
    """
    Returns the name of the warehouse in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_warehouse()).collect()
        >>> assert result[0]['CURRENT_WAREHOUSE()'] is not None
    """
    return _call_function("current_warehouse", _emit_ast=_emit_ast)


@publicapi
def current_database(_emit_ast: bool = True) -> Column:
    """Returns the name of the database in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_database()).collect()
        >>> assert result[0]['CURRENT_DATABASE()'] is not None
    """
    return _call_function("current_database", _emit_ast=_emit_ast)


@publicapi
def current_role(_emit_ast: bool = True) -> Column:
    """Returns the name of the role in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_role()).collect()
        >>> assert result[0]['CURRENT_ROLE()'] is not None
    """
    return _call_function("current_role", _emit_ast=_emit_ast)


@publicapi
def current_schema(_emit_ast: bool = True) -> Column:
    """Returns the name of the schema in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_schema()).collect()
        >>> assert result[0]['CURRENT_SCHEMA()'] is not None
    """
    return _call_function("current_schema", _emit_ast=_emit_ast)


@publicapi
def current_schemas(_emit_ast: bool = True) -> Column:
    """Returns active search path schemas.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_schemas()).collect()
        >>> assert result[0]['CURRENT_SCHEMAS()'] is not None
    """
    return _call_function("current_schemas", _emit_ast=_emit_ast)


@publicapi
def current_region(_emit_ast: bool = True) -> Column:
    """Returns the name of the region for the account where the current user is logged in.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_region()).collect()
        >>> assert result[0]['CURRENT_REGION()'] is not None
    """
    return _call_function("current_region", _emit_ast=_emit_ast)


@publicapi
def current_account(_emit_ast: bool = True) -> Column:
    """Returns the name of the account used in the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_account()).collect()
        >>> assert result[0]['CURRENT_ACCOUNT()'] is not None
    """
    return _call_function("current_account", _emit_ast=_emit_ast)


@publicapi
def current_available_roles(_emit_ast: bool = True) -> Column:
    """Returns a JSON string that lists all roles granted to the current user.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_available_roles()).collect()
        >>> assert result[0]['CURRENT_AVAILABLE_ROLES()'] is not None
    """
    return _call_function("current_available_roles", _emit_ast=_emit_ast)


@publicapi
def add_months(
    date_or_timestamp: ColumnOrName,
    number_of_months: Union[Column, int],
    _emit_ast: bool = True,
) -> Column:
    """Adds or subtracts a specified number of months to a date or timestamp, preserving the end-of-month information.

    Example:
        >>> import datetime
        >>> df = session.create_dataframe([datetime.date(2022, 4, 6)], schema=["d"])
        >>> df.select(add_months("d", 4)).collect()[0][0]
        datetime.date(2022, 8, 6)
    """
    c = _to_col_if_str(date_or_timestamp, "add_months")
    return _call_function("add_months", c, number_of_months, _emit_ast=_emit_ast)


@publicapi
def any_value(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a non-deterministic any value for the specified column.
    This is an aggregate and window function.

    Example:
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        >>> result = df.select(any_value("a")).collect()
        >>> assert len(result) == 1  # non-deterministic value in result.
    """
    c = _to_col_if_str(e, "any_value")
    return _call_function("any_value", c, _emit_ast=_emit_ast)


@publicapi
def bitnot(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(bitnot("a")).collect()[0][0]
        -2
    """
    c = _to_col_if_str(e, "bitnot")
    return _call_function("bitnot", c, _emit_ast=_emit_ast)


@publicapi
def bitshiftleft(
    to_shift_column: ColumnOrName, n: Union[Column, int], _emit_ast: bool = True
) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.create_dataframe([2], schema=["a"])
        >>> df.select(bitshiftleft("a", 1)).collect()[0][0]
        4
    """
    c = _to_col_if_str(to_shift_column, "bitshiftleft")
    return _call_function("bitshiftleft", c, n, _emit_ast=_emit_ast)


@publicapi
def bitshiftright_unsigned(
    to_shift_column: ColumnOrName, n: Union[Column, int], _emit_ast: bool = True
) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.createDataFrame([(-1999)], ['a'])
        >>> df.select(bitshiftright_unsigned('a', 1)).collect()[0][0]
        9223372036854774808

        >>> df = session.createDataFrame([(42)], ['a'])
        >>> df.select(bitshiftright_unsigned('a', 1)).collect()[0][0]
        21

        >>> df = session.createDataFrame([(-21)], ['a'])
        >>> df.select(bitshiftright_unsigned('a', 1)).collect()[0][0]
        9223372036854775797
    """
    # AST.
    ast = (
        build_function_expr("bitshiftright_unsigned", [to_shift_column, n])
        if _emit_ast
        else None
    )

    c = _to_col_if_str(to_shift_column, "bitshiftright_unsigned")
    max_bit = bitshiftleft(lit(1, _emit_ast=False), 64, _emit_ast=False)
    unsigned_c = iff(
        c < 0,
        bitshiftright(c + max_bit, n, _emit_ast=False),
        bitshiftright(c, n, _emit_ast=False),
        _emit_ast=False,
    )
    return _call_function(
        "bitand", unsigned_c, max_bit - 1, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def bitshiftright(
    to_shift_column: ColumnOrName, n: Union[Column, int], _emit_ast: bool = True
) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.create_dataframe([2], schema=["a"])
        >>> df.select(bitshiftright("a", 1)).collect()[0][0]
        1
    """
    c = _to_col_if_str(to_shift_column, "bitshiftright")
    return _call_function("bitshiftright", c, n, _emit_ast=_emit_ast)


@publicapi
def bround(
    col: ColumnOrName, scale: Union[Column, int], _emit_ast: bool = True
) -> Column:
    """
    Rounds the number using `HALF_TO_EVEN` option. The `HALF_TO_EVEN` rounding mode rounds the given decimal value to the specified scale (number of decimal places) as follows:
    * If scale is greater than or equal to 0, round to the specified number of decimal places using half-even rounding. This rounds towards the nearest value with ties (equally close values) rounding to the nearest even digit.
    * If scale is less than 0, round to the integral part of the decimal. This rounds towards 0 and truncates the decimal places.

    Note:

        1. The data type of the expression must be one of the `data types for a fixed-point number
        <https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#label-data-types-for-fixed-point-numbers>`_.

        2. Data types for floating point numbers (e.g. FLOAT) are not supported with this argument.

        3. If the expression data type is not supported, the expression must be explicitly cast to decimal before calling.

    Example:
        >>> import decimal
        >>> data = [(decimal.Decimal(1.235)),(decimal.Decimal(3.5))]
        >>> df = session.createDataFrame(data, ["value"])
        >>> df.select(bround('VALUE',1).alias("VALUE")).show() # Rounds to 1 decimal place
        -----------
        |"VALUE"  |
        -----------
        |1.2      |
        |3.5      |
        -----------
        <BLANKLINE>
        >>> df.select(bround('VALUE',0).alias("VALUE")).show() # Rounds to 1 decimal place
        -----------
        |"VALUE"  |
        -----------
        |1        |
        |4        |
        -----------
        <BLANKLINE>
    """
    # AST.
    ast = build_function_expr("bround", [col, scale]) if _emit_ast else None

    col = _to_col_if_str(col, "bround")
    scale = _to_col_if_lit(scale, "bround")

    # Note: Original Snowpark python code capitalized here.
    return _call_function(
        "ROUND",
        col,
        scale,
        lit("HALF_TO_EVEN", _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def convert_timezone(
    target_timezone: ColumnOrName,
    source_time: ColumnOrName,
    source_timezone: Optional[ColumnOrName] = None,
    _emit_ast: bool = True,
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
    # Build AST here to prevent rearrangement of args in the encoded AST.
    ast = (
        build_function_expr(
            "convert_timezone",
            [target_timezone, source_time]
            + ([] if source_timezone is None else [source_timezone]),
        )
        if _emit_ast
        else None
    )
    if source_timezone is None:
        return _call_function(
            "convert_timezone",
            target_tz,
            source_time_to_convert,
            _ast=ast,
            _emit_ast=_emit_ast,
        )
    return _call_function(
        "convert_timezone",
        source_tz,
        target_tz,
        source_time_to_convert,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def approx_count_distinct(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Uses HyperLogLog to return an approximation of the distinct cardinality of the input (i.e. HLL(col1, col2, ... )
    returns an approximation of COUNT(DISTINCT col1, col2, ... )).

    Example::
        >>> df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
        >>> df.select(approx_count_distinct("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |3         |
        ------------
        <BLANKLINE>

    """
    c = _to_col_if_str(e, "approx_count_distinct")
    return _call_function("approx_count_distinct", c, _emit_ast=_emit_ast)


@publicapi
def avg(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the average of non-NULL records. If all records inside a group are NULL,
    the function returns NULL.

    Example::
        >>> df = session.create_dataframe([[1], [2], [2]], schema=["d"])
        >>> df.select(avg(df.d).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.666667  |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "avg")
    return _call_function("avg", c, _emit_ast=_emit_ast)


@publicapi
def corr(
    column1: ColumnOrName, column2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the correlation coefficient for non-null pairs in a group.

    Example:
        >>> df = session.create_dataframe([[1, 2], [1, 2], [4, 5], [2, 3], [3, None], [4, None], [6,4]], schema=["a", "b"])
        >>> df.select(corr(col("a"), col("b")).alias("result")).show()
        ---------------------
        |"RESULT"           |
        ---------------------
        |0.813681508328809  |
        ---------------------
        <BLANKLINE>
    """
    c1 = _to_col_if_str(column1, "corr")
    c2 = _to_col_if_str(column2, "corr")
    return _call_function("corr", c1, c2, _emit_ast=_emit_ast)


@publicapi
def count(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns either the number of non-NULL records for the specified columns, or the
    total number of records.

    Example:
        >>> df = session.create_dataframe([[1], [None], [3], [4], [None]], schema=["a"])
        >>> df.select(count(col("a")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |3         |
        ------------
        <BLANKLINE>
        >>> df.select(count("*").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |5         |
        ------------
        <BLANKLINE>
    """
    # Build AST here to avoid building from Snowflake Expression.
    ast = build_function_expr("count", [e]) if _emit_ast else None

    c = _to_col_if_str(e, "count")
    expr = Literal(1) if isinstance(c._expression, Star) else c._expression
    return _call_function("count", expr, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def count_distinct(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns either the number of non-NULL distinct records for the specified columns,
    or the total number of the distinct records.

    Example:

        >>> df = session.create_dataframe([[1, 2], [1, 2], [3, None], [2, 3], [3, None], [4, None]], schema=["a", "b"])
        >>> df.select(count_distinct(col("a"), col("b")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |2         |
        ------------
        <BLANKLINE>
        >>> #  The result should be 2 for {[1,2],[2,3]} since the rest are either duplicate or NULL records
    """
    cs = [_to_col_if_str(c, "count_distinct") for c in cols]
    ast = build_function_expr("count_distinct", cs) if _emit_ast else None
    return Column(
        FunctionExpression("count", [c._expression for c in cs], is_distinct=True),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def covar_pop(
    column1: ColumnOrName, column2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the population covariance for non-null pairs in a group.

    Example:
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([[1, 2], [1, 2], [4, 5], [2, 3], [3, None], [4, None], [6,4]], schema=["a", "b"])
        >>> df.select(covar_pop(col("a"), col("b")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.840     |
        ------------
        <BLANKLINE>
    """
    col1 = _to_col_if_str(column1, "covar_pop")
    col2 = _to_col_if_str(column2, "covar_pop")
    return _call_function("covar_pop", col1, col2, _emit_ast=_emit_ast)


@publicapi
def covar_samp(
    column1: ColumnOrName, column2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the sample covariance for non-null pairs in a group.

    Example:
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([[1, 2], [1, 2], [4, 5], [2, 3], [3, None], [4, None], [6,4]], schema=["a", "b"])
        >>> df.select(covar_samp(col("a"), col("b")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |2.300     |
        ------------
        <BLANKLINE>
    """
    col1 = _to_col_if_str(column1, "covar_samp")
    col2 = _to_col_if_str(column2, "covar_samp")
    return _call_function("covar_samp", col1, col2, _emit_ast=_emit_ast)


@publicapi
def create_map(
    *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName]],
    _emit_ast: bool = True,
) -> Column:
    """Transforms multiple column pairs into a single map :class:`~snowflake.snowpark.Column` where each pair of
    columns is treated as a key-value pair in the resulting map.

    Args:
        *cols: A variable number of column names or :class:`~snowflake.snowpark.Column` objects that can also be
               expressed as a list of columns.
               The function expects an even number of arguments, where each pair of arguments represents a key-value
               pair for the map.

    Returns:
        A :class:`~snowflake.snowpark.Column` where each row contains a map created from the provided column pairs.

    Example:
        >>> from snowflake.snowpark.functions import create_map
        >>> df = session.create_dataframe([("Paris", "France"), ("Tokyo", "Japan")], ("city", "country"))
        >>> df.select(create_map("city", "country").alias("map")).show()
        -----------------------
        |"MAP"                |
        -----------------------
        |{                    |
        |  "Paris": "France"  |
        |}                    |
        |{                    |
        |  "Tokyo": "Japan"   |
        |}                    |
        -----------------------
        <BLANKLINE>

        >>> df.select(create_map([df.city, df.country]).alias("map")).show()
        -----------------------
        |"MAP"                |
        -----------------------
        |{                    |
        |  "Paris": "France"  |
        |}                    |
        |{                    |
        |  "Tokyo": "Japan"   |
        |}                    |
        -----------------------
        <BLANKLINE>
    """
    variadic = check_create_map_parameter(*cols)
    ast = None
    if _emit_ast:
        ast = proto.Expr()
        build_builtin_fn_apply(
            ast, "create_map", *cols if variadic else cols
        ) if _emit_ast else None

    # TODO SNOW-1790918: Remove as part of refactoring with alias.
    if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
        cols = cols[0]

    col = object_construct_keep_null(*cols, _emit_ast=False)
    col._ast = ast
    return col


@publicapi
def kurtosis(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the population excess kurtosis of non-NULL records. If all records
    inside a group are NULL, the function returns NULL.

    Example::

        >>> from snowflake.snowpark.functions import kurtosis
        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(kurtosis("x").as_("x")).collect()
        [Row(X=Decimal('3.613736609956'))]
    """
    c = _to_col_if_str(e, "kurtosis")
    return _call_function("kurtosis", c, _emit_ast=_emit_ast)


@publicapi
def max(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the maximum value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(max("x").as_("x")).collect()
        [Row(X=10)]
    """
    c = _to_col_if_str(e, "max")
    return _call_function("max", c, _emit_ast=_emit_ast)


@publicapi
def mean(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Return the average for the specific numeric columns. Alias of :func:`avg`.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(mean("x").as_("x")).collect()
        [Row(X=Decimal('3.600000'))]
    """
    c = _to_col_if_str(e, "mean")
    ans = avg(c, _emit_ast=False)
    ans._ast = build_function_expr("mean", [c]) if _emit_ast else None
    return ans


@publicapi
def median(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the median value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(median("x").as_("x")).collect()
        [Row(X=Decimal('3.000'))]
    """
    c = _to_col_if_str(e, "median")
    return _call_function("median", c, _emit_ast=_emit_ast)


@publicapi
def min(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the minimum value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(min("x").as_("x")).collect()
        [Row(X=1)]
    """
    c = _to_col_if_str(e, "min")
    return _call_function("min", c, _emit_ast=_emit_ast)


@publicapi
def mode(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the most frequent value for the records in a group. NULL values are ignored.
    If all the values are NULL, or there are 0 rows, then the function returns NULL.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 4], schema=["x"])
        >>> df.select(mode("x").as_("x")).collect()
        [Row(X=1)]
    """
    c = _to_col_if_str(e, "mode")
    return _call_function("mode", c, _emit_ast=_emit_ast)


@publicapi
def skew(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the sample skewness of non-NULL records. If all records inside a group
    are NULL, the function returns NULL.

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe(
        ...     [10, 10, 20, 25, 30],
        ...     schema=["a"]
        ... ).select(skew("a").cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (SKEW("A") AS NUMBER(38, 4))=Decimal('0.0524'))]
    """
    c = _to_col_if_str(e, "skew")
    return _call_function("skew", c, _emit_ast=_emit_ast)


@publicapi
def stddev(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of
    non-NULL values. If all records inside a group are NULL, returns NULL.

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(stddev(col("N")).cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (STDDEV("N") AS NUMBER(38, 4))=Decimal('3.5355'))]
    """
    c = _to_col_if_str(e, "stddev")
    return _call_function("stddev", c, _emit_ast=_emit_ast)


@publicapi
def stddev_samp(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of
    non-NULL values. If all records inside a group are NULL, returns NULL. Alias of
    :func:`stddev`.

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(stddev_samp(col("N")).cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (STDDEV_SAMP("N") AS NUMBER(38, 4))=Decimal('3.5355'))]
    """
    c = _to_col_if_str(e, "stddev_samp")
    return _call_function("stddev_samp", c, _emit_ast=_emit_ast)


@publicapi
def stddev_pop(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the population standard deviation (square root of variance) of non-NULL
    values. If all records inside a group are NULL, returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(stddev_pop(col("N")))
        >>> df.collect()
        [Row(STDDEV_POP("N")=2.5)]
    """
    c = _to_col_if_str(e, "stddev_pop")
    return _call_function("stddev_pop", c, _emit_ast=_emit_ast)


@publicapi
def sum(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the sum of non-NULL records in a group. You can use the DISTINCT keyword
    to compute the sum of unique non-null values. If all records inside a group are
    NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(sum(col("N")))
        >>> df.collect()
        [Row(SUM("N")=13)]
    """
    c = _to_col_if_str(e, "sum")
    return _call_function("sum", c, _emit_ast=_emit_ast)


@publicapi
def sum_distinct(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the sum of non-NULL distinct records in a group. You can use the
    DISTINCT keyword to compute the sum of unique non-null values. If all records
    inside a group are NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [1, 2, None, 3],
        ...     schema=["N"],
        ... ).select(sum_distinct(col("N")))
        >>> df.collect()
        [Row(SUM( DISTINCT "N")=6)]
    """
    # AST.
    ast = build_function_expr("sum_distinct", [e]) if _emit_ast else None

    c = _to_col_if_str(e, "sum_distinct")
    return _call_function("sum", c, is_distinct=True, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def variance(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. For a single row, NULL is returned as sample variance.

    Example::

        >>> df = session.create_dataframe([1, -1, 1, -1, -1], schema=["a"])
        >>> df.select(variance(col("a"))).collect()
        [Row(VARIANCE("A")=Decimal('1.200000'))]

        >>> df = session.create_dataframe([1, None, 2, 3, None, 5, 6], schema=["a"])
        >>> df.select(variance(col("a"))).collect()
        [Row(VARIANCE("A")=Decimal('4.300000'))]

        >>> df = session.create_dataframe([None, None, None], schema=["a"])
        >>> df.select(variance(col("a"))).collect()
        [Row(VARIANCE("A")=None)]

        >>> df = session.create_dataframe([42], schema=["a"])
        >>> df.select(variance(col("a"))).collect()
        [Row(VARIANCE("A")=None)]

    """
    c = _to_col_if_str(e, "variance")
    return _call_function("variance", c, _emit_ast=_emit_ast)


@publicapi
def var_samp(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. For a single row, NULL is returned as sample variance.
    Alias of :func:`variance`

    Example::

        >>> df = session.create_dataframe([1, -1, 1, -1, -1], schema=["a"])
        >>> df.select(var_samp(col("a"))).collect()
        [Row(VARIANCE("A")=Decimal('1.200000'))]

        >>> df = session.create_dataframe([1, None, 2, 3, None, 5, 6], schema=["a"])
        >>> df.select(var_samp(col("a"))).collect()
        [Row(VARIANCE("A")=Decimal('4.300000'))]

        >>> df = session.create_dataframe([None, None, None], schema=["a"])
        >>> df.select(var_samp(col("a"))).collect()
        [Row(VARIANCE("A")=None)]

        >>> df = session.create_dataframe([42], schema=["a"])
        >>> df.select(var_samp(col("a"))).collect()
        [Row(VARIANCE("A")=None)]

    """
    # AST.
    ast = build_function_expr("var_samp", [e]) if _emit_ast else None

    c = _to_col_if_str(e, "var_samp")
    return _call_function("variance", c, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def var_pop(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the population variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. The population variance of a single row is 0.0.

    Example::

        >>> df = session.create_dataframe([1, -1, 1, -1, -1], schema=["a"])
        >>> df.select(var_pop(col("a"))).collect()
        [Row(VAR_POP("A")=Decimal('0.960000'))]

        >>> df = session.create_dataframe([1, None, 2, 3, None, 5, 6], schema=["a"])
        >>> df.select(var_pop(col("a"))).collect()
        [Row(VAR_POP("A")=Decimal('3.440000'))]

        >>> df = session.create_dataframe([None, None, None], schema=["a"])
        >>> df.select(var_pop(col("a"))).collect()
        [Row(VAR_POP("A")=None)]

        >>> df = session.create_dataframe([42], schema=["a"])
        >>> df.select(var_pop(col("a"))).collect()
        [Row(VAR_POP("A")=Decimal('0.000000'))]

    """
    c = _to_col_if_str(e, "var_pop")
    return _call_function("var_pop", c, _emit_ast=_emit_ast)


@publicapi
def approx_percentile(
    col: ColumnOrName, percentile: float, _emit_ast: bool = True
) -> Column:
    """Returns an approximated value for the desired percentile. This function uses the t-Digest algorithm.

    Example::
        >>> df = session.create_dataframe([0,1,2,3,4,5,6,7,8,9], schema=["a"])
        >>> df.select(approx_percentile("a", 0.5).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |4.5       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "approx_percentile")
    # Build AST here to prevent `percentile` from being recorded as a literal instead of float.
    ast = (
        build_function_expr("approx_percentile", [c, percentile]) if _emit_ast else None
    )
    return _call_function(
        "approx_percentile",
        c,
        lit(percentile, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


percentile_approx = approx_percentile


@publicapi
def approx_percentile_accumulate(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the internal representation of the t-Digest state (as a JSON object) at the end of aggregation.
    This function uses the t-Digest algorithm.

    Example::
        >>> df = session.create_dataframe([1,2,3,4,5], schema=["a"])
        >>> df.select(approx_percentile_accumulate("a").alias("result")).show()
        ------------------------------
        |"RESULT"                    |
        ------------------------------
        |{                           |
        |  "state": [                |
        |    1.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    2.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    3.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    4.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    5.000000000000000e+00,  |
        |    1.000000000000000e+00   |
        |  ],                        |
        |  "type": "tdigest",        |
        |  "version": 1              |
        |}                           |
        ------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "approx_percentile_accumulate")
    return _call_function("approx_percentile_accumulate", c, _emit_ast=_emit_ast)


@publicapi
def approx_percentile_estimate(
    state: ColumnOrName, percentile: float, _emit_ast: bool = True
) -> Column:
    """Returns the desired approximated percentile value for the specified t-Digest state.
    APPROX_PERCENTILE_ESTIMATE(APPROX_PERCENTILE_ACCUMULATE(.)) is equivalent to
    APPROX_PERCENTILE(.).

    Example::
        >>> df = session.create_dataframe([1,2,3,4,5], schema=["a"])
        >>> df_accu = df.select(approx_percentile_accumulate("a").alias("app_percentile_accu"))
        >>> df_accu.select(approx_percentile_estimate("app_percentile_accu", 0.5).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |3.0       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(state, "approx_percentile_estimate")
    # Build AST here to prevent `percentile` from being recorded as a literal instead of float.
    ast = (
        build_function_expr("approx_percentile_estimate", [c, percentile])
        if _emit_ast
        else None
    )
    return _call_function(
        "approx_percentile_estimate",
        c,
        lit(percentile, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def approx_percentile_combine(state: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Combines (merges) percentile input states into a single output state.
    This allows scenarios where APPROX_PERCENTILE_ACCUMULATE is run over horizontal partitions
    of the same table, producing an algorithm state for each table partition. These states can
    later be combined using APPROX_PERCENTILE_COMBINE, producing the same output state as a
    single run of APPROX_PERCENTILE_ACCUMULATE over the entire table.

    Example::
        >>> df1 = session.create_dataframe([1,2,3,4,5], schema=["a"])
        >>> df2 = session.create_dataframe([6,7,8,9,10], schema=["b"])
        >>> df_accu1 = df1.select(approx_percentile_accumulate("a").alias("app_percentile_accu"))
        >>> df_accu2 = df2.select(approx_percentile_accumulate("b").alias("app_percentile_accu"))
        >>> df_accu1.union(df_accu2).select(approx_percentile_combine("app_percentile_accu").alias("result")).show()
        ------------------------------
        |"RESULT"                    |
        ------------------------------
        |{                           |
        |  "state": [                |
        |    1.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    2.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    3.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    4.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    5.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    6.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    7.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    8.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    9.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    1.000000000000000e+01,  |
        |    1.000000000000000e+00   |
        |  ],                        |
        |  "type": "tdigest",        |
        |  "version": 1              |
        |}                           |
        ------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(state, "approx_percentile_combine")
    return _call_function("approx_percentile_combine", c, _emit_ast=_emit_ast)


@publicapi
def explode(
    col: ColumnOrName, _emit_ast: bool = True
) -> "snowflake.snowpark.table_function.TableFunctionCall":
    """Flattens a given array or map type column into individual rows. The default
    column name for the output column in case of array input column is ``VALUE``,
    and is ``KEY`` and ``VALUE`` in case of map input column.

    Examples::
        >>> df = session.create_dataframe([[1, [1, 2, 3], {"Ashi Garami": "Single Leg X"}, "Kimura"],
        ...                                [2, [11, 22], {"Sankaku": "Triangle"}, "Coffee"]],
        ...                                schema=["idx", "lists", "maps", "strs"])
        >>> df.select(df.idx, explode(df.lists)).sort(col("idx")).show()
        -------------------
        |"IDX"  |"VALUE"  |
        -------------------
        |1      |1        |
        |1      |2        |
        |1      |3        |
        |2      |11       |
        |2      |22       |
        -------------------
        <BLANKLINE>

        >>> df.select(df.strs, explode(df.maps)).sort(col("strs")).show()
        -----------------------------------------
        |"STRS"  |"KEY"        |"VALUE"         |
        -----------------------------------------
        |Coffee  |Sankaku      |"Triangle"      |
        |Kimura  |Ashi Garami  |"Single Leg X"  |
        -----------------------------------------
        <BLANKLINE>

        >>> df.select(explode(col("lists")).alias("uno")).sort(col("uno")).show()
        ---------
        |"UNO"  |
        ---------
        |1      |
        |2      |
        |3      |
        |11     |
        |22     |
        ---------
        <BLANKLINE>

        >>> df.select(explode('maps').as_("primo", "secundo")).sort(col("primo")).show()
        --------------------------------
        |"PRIMO"      |"SECUNDO"       |
        --------------------------------
        |Ashi Garami  |"Single Leg X"  |
        |Sankaku      |"Triangle"      |
        --------------------------------
        <BLANKLINE>
    """
    # AST.
    ast = build_function_expr("explode", [col]) if _emit_ast else None

    col = _to_col_if_str(col, "explode")
    func_call = snowflake.snowpark.table_function._ExplodeFunctionCall(
        col, lit(False, _emit_ast=False)
    )
    func_call._set_api_call_source("functions.explode")
    func_call._ast = ast

    return func_call


@publicapi
def explode_outer(
    col: ColumnOrName, _emit_ast: bool = True
) -> "snowflake.snowpark.table_function.TableFunctionCall":
    """Flattens a given array or map type column into individual rows. Unlike :func:`explode`,
    if array or map is empty, null or empty, then null values are produced. The default column
    name for the output column in case of array input column is ``VALUE``, and is ``KEY`` and
    ``VALUE`` in case of map input column.

    Args:
        col: Column object or string name of the desired column

    Examples::
        >>> df = session.create_dataframe([[1, [1, 2, 3], {"Ashi Garami": "Single Leg X"}],
        ...                                [2, [11, 22], {"Sankaku": "Triangle"}],
        ...                                [3, [], {}]],
        ...                                schema=["idx", "lists", "maps"])
        >>> df.select(df.idx, explode_outer(df.lists)).sort(col("idx")).show()
        -------------------
        |"IDX"  |"VALUE"  |
        -------------------
        |1      |1        |
        |1      |2        |
        |1      |3        |
        |2      |11       |
        |2      |22       |
        |3      |NULL     |
        -------------------
        <BLANKLINE>

        >>> df.select(df.idx, explode_outer(df.maps)).sort(col("idx")).show()
        ----------------------------------------
        |"IDX"  |"KEY"        |"VALUE"         |
        ----------------------------------------
        |1      |Ashi Garami  |"Single Leg X"  |
        |2      |Sankaku      |"Triangle"      |
        |3      |NULL         |NULL            |
        ----------------------------------------
        <BLANKLINE>

    See Also:
        :func:`explode`
    """
    # AST.
    ast = build_function_expr("explode_outer", [col]) if _emit_ast else None

    col = _to_col_if_str(col, "explode_outer")
    func_call = snowflake.snowpark.table_function._ExplodeFunctionCall(
        col, lit(True, _emit_ast=False)
    )
    func_call._set_api_call_source("functions.explode_outer")
    func_call._ast = ast

    return func_call


@publicapi
def flatten(
    col: ColumnOrName,
    path: str = "",
    outer: bool = False,
    recursive: bool = False,
    mode: typing.Literal["object", "array", "both"] = "both",
    _emit_ast: bool = True,
) -> "snowflake.snowpark.table_function.TableFunctionCall":
    """FLATTEN explodes compound values into multiple rows. This table function takes a
    VARIANT, OBJECT, or ARRAY column and produces a lateral view.

    Args:
        col: Column object or string name of the desired column.
        path: The path to the element within VARIANT data structure which needs to be
            flattened. Defaults to "".
        outer: When ``False``, any input rows that cannot be expanded are completely
            omitted from the output. When ``True``, exactly one row s generated for
            zero-row expansions. Defaults to ``False``.
        recursive: When ``False``, only the reference by ``path`` is expanded. When
            ``True``, the expansion is performed for all sub-elements recursively.
            Defaults to ``False``.
        mode: Specifies whether only objects, arrays, or both should be flattened.
            Defaults to "both".

    Examples::
        >>> df = session.create_dataframe([[1, [1, 2, 3], {"Ashi Garami": ["X", "Leg Entanglement"]}, "Kimura"],
        ...                                [2, [11, 22], {"Sankaku": ["Triangle"]}, "Coffee"],
        ...                                [3, [], {}, "empty"]],
        ...                                schema=["idx", "lists", "maps", "strs"])
        >>> df.select(df.idx, flatten(df.lists, outer=True)).select("idx", "value").sort("idx").show()
        -------------------
        |"IDX"  |"VALUE"  |
        -------------------
        |1      |1        |
        |1      |2        |
        |1      |3        |
        |2      |11       |
        |2      |22       |
        |3      |NULL     |
        -------------------
        <BLANKLINE>

        >>> df.select(df.strs, flatten(df.maps, recursive=True)).select("strs", "key", "value").where("key is not NULL").sort("strs").show()
        -----------------------------------------------
        |"STRS"  |"KEY"        |"VALUE"               |
        -----------------------------------------------
        |Coffee  |Sankaku      |[                     |
        |        |             |  "Triangle"          |
        |        |             |]                     |
        |Kimura  |Ashi Garami  |[                     |
        |        |             |  "X",                |
        |        |             |  "Leg Entanglement"  |
        |        |             |]                     |
        -----------------------------------------------
        <BLANKLINE>

        >>> df.select(df.strs, flatten(df.maps, recursive=True)).select("strs", "key", "value").where("key is NULL").sort("strs", "value").show()
        ---------------------------------------
        |"STRS"  |"KEY"  |"VALUE"             |
        ---------------------------------------
        |Coffee  |NULL   |"Triangle"          |
        |Kimura  |NULL   |"Leg Entanglement"  |
        |Kimura  |NULL   |"X"                 |
        ---------------------------------------
        <BLANKLINE>

    See Also:
        - :func:`explode`
        - `Flatten <https://docs.snowflake.com/en/sql-reference/functions/flatten>`_
    """
    # AST.
    ast = (
        build_function_expr("flatten", [col, path, outer, recursive, mode])
        if _emit_ast
        else None
    )

    col = _to_col_if_str(col, "flatten")
    func_call = snowflake.snowpark.table_function.TableFunctionCall(
        "flatten",
        input=col,
        path=lit(path),
        outer=lit(outer),
        recursive=lit(recursive),
        mode=lit(mode),
        _emit_ast=False,
    )
    func_call._set_api_call_source("functions.flatten")
    func_call._ast = ast

    return func_call


@publicapi
def grouping(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.

    :func:`grouping_id` is an alias of :func:`grouping`.

    Example::
        >>> from snowflake.snowpark import GroupingSets
        >>> df = session.create_dataframe([[1, 2, 3], [4, 5, 6]],schema=["a", "b", "c"])
        >>> grouping_sets = GroupingSets([col("a")], [col("b")], [col("a"), col("b")])
        >>> df.group_by_grouping_sets(grouping_sets).agg([count("c").alias("count_c"), grouping("a").alias("ga"), grouping("b").alias("gb"), grouping("a", "b").alias("gab")]).sort("a", "b").collect()
        [Row(A=None, B=2, COUNT_C=1, GA=1, GB=0, GAB=2), \
Row(A=None, B=5, COUNT_C=1, GA=1, GB=0, GAB=2), \
Row(A=1, B=None, COUNT_C=1, GA=0, GB=1, GAB=1), \
Row(A=1, B=2, COUNT_C=1, GA=0, GB=0, GAB=0), \
Row(A=4, B=None, COUNT_C=1, GA=0, GB=1, GAB=1), \
Row(A=4, B=5, COUNT_C=1, GA=0, GB=0, GAB=0)]
    """
    columns = [_to_col_if_str(c, "grouping") for c in cols]
    return _call_function("grouping", *columns, _emit_ast=_emit_ast)


grouping_id = grouping


@publicapi
def coalesce(*e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the first non-NULL expression among its arguments, or NULL if all its
    arguments are NULL.

    Example::

        >>> df = session.create_dataframe([[1, 2, 3], [None, 2, 3], [None, None, 3], [None, None, None]], schema=['a', 'b', 'c'])
        >>> df.select(df.a, df.b, df.c, coalesce(df.a, df.b, df.c).as_("COALESCE")).show()
        -----------------------------------
        |"A"   |"B"   |"C"   |"COALESCE"  |
        -----------------------------------
        |1     |2     |3     |1           |
        |NULL  |2     |3     |2           |
        |NULL  |NULL  |3     |3           |
        |NULL  |NULL  |NULL  |NULL        |
        -----------------------------------
        <BLANKLINE>
    """
    c = [_to_col_if_str(ex, "coalesce") for ex in e]
    return _call_function("coalesce", *c, _emit_ast=_emit_ast)


@publicapi
def equal_nan(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Return true if the value in the column is not a number (NaN).

    Example::

        >>> import math
        >>> df = session.create_dataframe([1.1, math.nan, 2.3], schema=["a"])
        >>> df.select(equal_nan(df["a"]).alias("equal_nan")).collect()
        [Row(EQUAL_NAN=False), Row(EQUAL_NAN=True), Row(EQUAL_NAN=False)]
    """
    # AST.
    ast = build_function_expr("equal_nan", [e]) if _emit_ast else None

    c = _to_col_if_str(e, "equal_nan")
    ans = c.equal_nan(_emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def is_null(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Return true if the value in the column is null.

    Example::

        >>> from snowflake.snowpark.functions import is_null
        >>> df = session.create_dataframe([1.2, float("nan"), None, 1.0], schema=["a"])
        >>> df.select(is_null("a").as_("a")).collect()
        [Row(A=False), Row(A=False), Row(A=True), Row(A=False)]
    """
    # AST
    ast = build_function_expr("is_null", [e]) if _emit_ast else None

    c = _to_col_if_str(e, "is_null")
    ans = c.is_null(_emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def negate(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the negation of the value in the column (equivalent to a unary minus).

    Example::

        >>> df = session.create_dataframe([[1]], schema=["a"])
        >>> df.select(negate(col("a").alias("result"))).show()
        ------------
        |"RESULT"  |
        ------------
        |-1        |
        ------------
        <BLANKLINE>
    """
    # AST
    ast = build_function_expr("negate", [e]) if _emit_ast else None

    c = _to_col_if_str(e, "negate")
    ans = -c
    ans._ast = ast
    return ans


@publicapi
def not_(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the inverse of a boolean expression.

    Example::

        >>> df = session.create_dataframe([[True]], schema=["a"])
        >>> df.select(not_(col("a").alias("result"))).show()
        ------------
        |"RESULT"  |
        ------------
        |False     |
        ------------
        <BLANKLINE>
    """
    # AST
    ast = build_function_expr("not_", [e]) if _emit_ast else None

    c = _to_col_if_str(e, "not_")
    ans = ~c
    ans._ast = ast
    return ans


@publicapi
def random(seed: Optional[int] = None, _emit_ast: bool = True) -> Column:
    """Each call returns a pseudo-random 64-bit integer.

    Example::
        >>> df = session.sql("select 1")
        >>> df = df.select(random(123).alias("result"))
    """

    # Create AST here to encode whether a seed was supplied by the user or not.
    ast = (
        build_function_expr("random", [] if seed is None else [seed])
        if _emit_ast
        else None
    )

    s = seed if seed is not None else randint(-(2**63), 2**63 - 1)
    return _call_function("random", Literal(s), _ast=ast, _emit_ast=_emit_ast)


@publicapi
def uniform(
    min_: Union[ColumnOrName, int, float],
    max_: Union[ColumnOrName, int, float],
    gen: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
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
    ast = build_function_expr("uniform", [min_, max_, gen]) if _emit_ast else None

    def convert_limit_to_col(limit):
        if isinstance(limit, int):
            return lit(limit, _emit_ast=False)
        elif isinstance(limit, float):
            return lit(limit, _emit_ast=False).cast(FloatType(), _emit_ast=False)
        return _to_col_if_str(limit, "uniform")

    min_col = convert_limit_to_col(min_)
    max_col = convert_limit_to_col(max_)
    gen_col = (
        lit(gen, _emit_ast=False)
        if isinstance(gen, (int, float))
        else _to_col_if_str(gen, "uniform")
    )
    return _call_function(
        "uniform",
        min_col,
        max_col,
        gen_col,
        is_data_generator=True,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def seq1(sign: int = 0, _emit_ast: bool = True) -> Column:
    """Returns a sequence of monotonically increasing integers, with wrap-around
    which happens after largest representable integer of integer width 1 byte.

    Args:
        sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
            continues at smallest representable 1 byte integer. Defaults to 0.

    See Also:
        - :meth:`Session.generator`, which can be used to generate in tandem with `seq1` to
            generate sequences.

    Example::
        >>> df = session.generator(seq1(0), rowcount=3)
        >>> df.collect()
        [Row(SEQ1(0)=0), Row(SEQ1(0)=1), Row(SEQ1(0)=2)]
    """
    ast = build_function_expr("seq1", [sign]) if _emit_ast else None
    return _call_function(
        "seq1", Literal(sign), is_data_generator=True, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def seq2(sign: int = 0, _emit_ast: bool = True) -> Column:
    """Returns a sequence of monotonically increasing integers, with wrap-around
    which happens after largest representable integer of integer width 2 byte.

    Args:
        sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
            continues at smallest representable 2 byte integer. Defaults to 0.

    See Also:
        - :meth:`Session.generator`, which can be used to generate in tandem with `seq2` to
            generate sequences.

    Example::
        >>> df = session.generator(seq2(0), rowcount=3)
        >>> df.collect()
        [Row(SEQ2(0)=0), Row(SEQ2(0)=1), Row(SEQ2(0)=2)]
    """
    ast = build_function_expr("seq2", [sign]) if _emit_ast else None
    return _call_function(
        "seq2", Literal(sign), is_data_generator=True, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def seq4(sign: int = 0, _emit_ast: bool = True) -> Column:
    """Returns a sequence of monotonically increasing integers, with wrap-around
    which happens after largest representable integer of integer width 4 byte.

    Args:
        sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
            continues at smallest representable 4 byte integer. Defaults to 0.

    See Also:
        - :meth:`Session.generator`, which can be used to generate in tandem with `seq4` to
            generate sequences.

    Example::
        >>> df = session.generator(seq4(0), rowcount=3)
        >>> df.collect()
        [Row(SEQ4(0)=0), Row(SEQ4(0)=1), Row(SEQ4(0)=2)]
    """
    ast = build_function_expr("seq4", [sign]) if _emit_ast else None
    return _call_function(
        "seq4", Literal(sign), is_data_generator=True, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def seq8(sign: int = 0, _emit_ast: bool = True) -> Column:
    """Returns a sequence of monotonically increasing integers, with wrap-around
    which happens after largest representable integer of integer width 8 byte.

    Args:
        sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
            continues at smallest representable 8 byte integer. Defaults to 0.

    See Also:
        - :meth:`Session.generator`, which can be used to generate in tandem with `seq8` to
            generate sequences.

    Example::
        >>> df = session.generator(seq8(0), rowcount=3)
        >>> df.collect()
        [Row(SEQ8(0)=0), Row(SEQ8(0)=1), Row(SEQ8(0)=2)]
    """
    ast = build_function_expr("seq8", [sign]) if _emit_ast else None
    return _call_function(
        "seq8", Literal(sign), is_data_generator=True, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def to_boolean(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts an input expression to a boolean.

    Example::
        >>> df = session.create_dataframe(['yes', 'no'], schema=['a'])
        >>> df.select(to_boolean(col('a')).as_('ans')).collect()
        [Row(ANS=True), Row(ANS=False)]
    """
    c = _to_col_if_str(e, "to_boolean")
    return _call_function("to_boolean", c, _emit_ast=_emit_ast)


@publicapi
def to_decimal(
    e: ColumnOrName, precision: int, scale: int, _emit_ast: bool = True
) -> Column:
    """Converts an input expression to a decimal.

    Example::
        >>> df = session.create_dataframe(['12', '11.3', '-90.12345'], schema=['a'])
        >>> df.select(to_decimal(col('a'), 38, 0).as_('ans')).collect()
        [Row(ANS=12), Row(ANS=11), Row(ANS=-90)]

        >>> df.select(to_decimal(col('a'), 38, 2).as_('ans')).collect()
        [Row(ANS=Decimal('12.00')), Row(ANS=Decimal('11.30')), Row(ANS=Decimal('-90.12'))]
    """
    # Build AST here to prevent `precision` and `scale` from being recorded as a literal instead of int.
    ast = (
        build_function_expr("to_decimal", [e, precision, scale]) if _emit_ast else None
    )
    c = _to_col_if_str(e, "to_decimal")
    return _call_function(
        "to_decimal",
        c,
        lit(precision, _emit_ast=False),
        lit(scale, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def to_double(
    e: ColumnOrName, fmt: Optional[ColumnOrLiteralStr] = None, _emit_ast: bool = True
) -> Column:
    """Converts an input expression to a decimal.

    Example::
        >>> df = session.create_dataframe(['12', '11.3', '-90.12345'], schema=['a'])
        >>> df.select(to_double(col('a')).as_('ans')).collect()
        [Row(ANS=12.0), Row(ANS=11.3), Row(ANS=-90.12345)]

    Example::
        >>> df = session.create_dataframe(['12+', '11.3+', '90.12-'], schema=['a'])
        >>> df.select(to_double(col('a'), "999.99MI").as_('ans')).collect()
        [Row(ANS=12.0), Row(ANS=11.3), Row(ANS=-90.12)]
    """
    ast = (
        build_function_expr("to_double", [e] if fmt is None else [e, fmt])
        if _emit_ast
        else None
    )
    c = _to_col_if_str(e, "to_double")
    fmt_col = _to_col_if_lit(fmt, "to_double") if fmt is not None else None
    return (
        _call_function("to_double", c, _ast=ast, _emit_ast=_emit_ast)
        if fmt_col is None
        else _call_function("to_double", c, fmt_col, _ast=ast, _emit_ast=_emit_ast)
    )


@publicapi
def div0(
    dividend: Union[ColumnOrName, int, float],
    divisor: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
) -> Column:
    """
    Performs division like the division operator (/),
    but returns 0 when the divisor is 0 (rather than reporting an error).

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(div0(df["a"], 1).alias("divided_by_one"), div0(df["a"], 0).alias("divided_by_zero")).collect()
        [Row(DIVIDED_BY_ONE=Decimal('1.000000'), DIVIDED_BY_ZERO=Decimal('0.000000'))]
    """
    # Build AST here to prevent `dividend` and `divisor` from being recorded as a literal instead of int/float.
    ast = build_function_expr("div0", [dividend, divisor]) if _emit_ast else None
    dividend_col = (
        lit(dividend, _emit_ast=False)
        if isinstance(dividend, (int, float))
        else _to_col_if_str(dividend, "div0")
    )
    divisor_col = (
        lit(divisor, _emit_ast=False)
        if isinstance(divisor, (int, float))
        else _to_col_if_str(divisor, "div0")
    )
    return _call_function(
        "div0", dividend_col, divisor_col, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def divnull(
    dividend: Union[ColumnOrName, int, float],
    divisor: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
) -> Column:
    """Performs division like the division operator (/),
    but returns NULL when the divisor is 0 (rather then reporting error).

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(divnull(df["a"], 1).alias("divided_by_one"), divnull(df["a"], 0).alias("divided_by_zero")).collect()
        [Row(DIVIDED_BY_ONE=Decimal('1.000000'), DIVIDED_BY_ZERO=None)]
    """
    # TODO SNOW-1864578: find a way to encode divnull.
    dividend_col = (
        lit(dividend, _emit_ast=False)
        if isinstance(dividend, (int, float))
        else _to_col_if_str(dividend, "divnull")
    )
    divisor_col = (
        lit(divisor, _emit_ast=False)
        if isinstance(divisor, (int, float))
        else _to_col_if_str(divisor, "divnull")
    )
    ans = dividend_col / nullifzero(divisor_col, _emit_ast=False)
    ans._ast = (
        build_function_expr("divnull", [dividend, divisor]) if _emit_ast else None
    )
    return ans


@publicapi
def nullifzero(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns NULL if the argument evaluates to 0; otherwise, returns the argument.

    Example::
        >>> df = session.create_dataframe([0, 1], schema=["a"])
        >>> df.select(nullifzero(df["a"]).alias("result")).collect()
        [Row(RESULT=None), Row(RESULT=1)]
    """
    c = _to_col_if_str(e, "nullifzero")
    return _call_function("nullifzero", c, _emit_ast=_emit_ast)


@publicapi
def sqrt(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the square-root of a non-negative numeric expression.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(sqrt(col("N")))
        >>> df.collect()
        [Row(SQRT("N")=2.0), Row(SQRT("N")=3.0)]
    """
    c = _to_col_if_str(e, "sqrt")
    return _call_function("sqrt", c, _emit_ast=_emit_ast)


@publicapi
def abs(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the absolute value of a numeric expression.

    Example::
        >>> df = session.create_dataframe([[-1]], schema=["a"])
        >>> df.select(abs(col("a")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "abs")
    return _call_function("abs", c, _emit_ast=_emit_ast)


@publicapi
def acos(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the inverse cosine (arc cosine) of its input;
    the result is a number in the interval [-pi, pi].

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([[0.5]], schema=["deg"])
        >>> df.select(acos(col("deg")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.047     |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "acos")
    return _call_function("acos", c, _emit_ast=_emit_ast)


@publicapi
def asin(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the inverse sine (arc sine) of its input;
    the result is a number in the interval [-pi, pi].

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([[1]], schema=["deg"])
        >>> df.select(asin(col("deg")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.571     |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "asin")
    return _call_function("asin", c, _emit_ast=_emit_ast)


@publicapi
def atan(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the inverse tangent (arc tangent) of its input;
    the result is a number in the interval [-pi, pi].

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([[1]], schema=["deg"])
        >>> df.select(atan(col("deg")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.785     |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "atan")
    return _call_function("atan", c, _emit_ast=_emit_ast)


@publicapi
def atan2(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the inverse tangent (arc tangent) of its input;
    the result is a number in the interval [-pi, pi].

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([[1, 2]], schema=["x", "y"])
        >>> df.select(atan2(df.x, df.y).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.464     |
        ------------
        <BLANKLINE>
    """
    y_col = _to_col_if_str(y, "atan2")
    x_col = _to_col_if_str(x, "atan2")
    return _call_function("atan2", y_col, x_col, _emit_ast=_emit_ast)


@publicapi
def ceil(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns values from the specified column rounded to the nearest equal or larger
    integer.

    Example::

        >>> df = session.create_dataframe([135.135, -975.975], schema=["a"])
        >>> df.select(ceil(df["a"]).alias("ceil")).collect()
        [Row(CEIL=136.0), Row(CEIL=-975.0)]
    """
    c = _to_col_if_str(e, "ceil")
    return _call_function("ceil", c, _emit_ast=_emit_ast)


@publicapi
def cos(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the cosine of its argument; the argument should be expressed in radians.

    Example:
        >>> from math import pi
        >>> df = session.create_dataframe([[pi]], schema=["deg"])
        >>> df.select(cos(col("deg")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |-1.0      |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "cos")
    return _call_function("cos", c, _emit_ast=_emit_ast)


@publicapi
def cosh(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the hyperbolic cosine of its argument.

    Example:
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([[0]], schema=["deg"])
        >>> df.select(cosh(col("deg")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.0       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "cosh")
    return _call_function("cosh", c, _emit_ast=_emit_ast)


@publicapi
def exp(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Computes Euler's number e raised to a floating-point value.

    Example::

        >>> import math
        >>> from snowflake.snowpark.types import IntegerType
        >>> df = session.create_dataframe([0.0, math.log(10)], schema=["a"])
        >>> df.select(exp(df["a"]).cast(IntegerType()).alias("exp")).collect()
        [Row(EXP=1), Row(EXP=10)]
    """
    c = _to_col_if_str(e, "exp")
    return _call_function("exp", c, _emit_ast=_emit_ast)


@publicapi
def factorial(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Computes the factorial of its input. The input argument must be an integer
    expression in the range of 0 to 33.

    Example::

        >>> df = session.create_dataframe([0, 1, 5, 10], schema=["a"])
        >>> df.select(factorial(df["a"]).alias("factorial")).collect()
        [Row(FACTORIAL=1), Row(FACTORIAL=1), Row(FACTORIAL=120), Row(FACTORIAL=3628800)]
    """
    c = _to_col_if_str(e, "factorial")
    return _call_function("factorial", c, _emit_ast=_emit_ast)


@publicapi
def floor(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns values from the specified column rounded to the nearest equal or
    smaller integer.

    Examples::

        >>> df = session.create_dataframe([135.135, -975.975], schema=["a"])
        >>> df.select(floor(df["a"]).alias("floor")).collect()
        [Row(FLOOR=135.0), Row(FLOOR=-976.0)]
    """
    c = _to_col_if_str(e, "floor")
    return _call_function("floor", c, _emit_ast=_emit_ast)


@publicapi
def format_number(col: ColumnOrName, d: Union[Column, int], _emit_ast: bool = True):
    """Format numbers to a specific number of decimal places with HALF_TO_EVEN rounding.

    Note:
        1. The data type of the expression must be one of the `data types for a fixed-point number
        <https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#label-data-types-for-fixed-point-numbers>`_.

        2. Data types for floating point numbers (e.g. FLOAT) are not supported with this argument.

        3. If the expression data type is not supported, the expression must be explicitly cast to decimal before calling.

    Example::
            >>> import decimal
            >>> from snowflake.snowpark.functions import format_number
            >>> data = [(1, decimal.Decimal(3.14159)), (2, decimal.Decimal(2.71828)), (3, decimal.Decimal(1.41421))]
            >>> df = session.createDataFrame(data, ["id", "value"])
            >>> df.select("id",format_number("value",2).alias("value")).show()
            ------------------
            |"ID"  |"VALUE"  |
            ------------------
            |1     |3.14     |
            |2     |2.72     |
            |3     |1.41     |
            ------------------
            <BLANKLINE>
    """
    col = _to_col_if_str(col, "format_number")
    c = bround(col, d, _emit_ast=False).cast(StringType(), _emit_ast=False)
    c._ast = build_function_expr("format_number", [col, d]) if _emit_ast else None
    return c


@publicapi
def sin(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the sine of its argument; the argument should be expressed in radians.

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.generator(seq1(0), rowcount=3).select(sin(seq1(0)).cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (SIN(SEQ1(0)) AS NUMBER(38, 4))=Decimal('0.0000')), Row(CAST (SIN(SEQ1(0)) AS NUMBER(38, 4))=Decimal('0.8415')), Row(CAST (SIN(SEQ1(0)) AS NUMBER(38, 4))=Decimal('0.9093'))]
    """
    c = _to_col_if_str(e, "sin")
    return _call_function("sin", c, _emit_ast=_emit_ast)


@publicapi
def sinh(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the hyperbolic sine of its argument.

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.generator(seq1(0), rowcount=3).select(sinh(seq1(0)).cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (SINH(SEQ1(0)) AS NUMBER(38, 4))=Decimal('0.0000')), Row(CAST (SINH(SEQ1(0)) AS NUMBER(38, 4))=Decimal('1.1752')), Row(CAST (SINH(SEQ1(0)) AS NUMBER(38, 4))=Decimal('3.6269'))]
    """
    c = _to_col_if_str(e, "sinh")
    return _call_function("sinh", c, _emit_ast=_emit_ast)


@publicapi
def tan(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the tangent of its argument; the argument should be expressed in radians.

    Example::
       >>> from snowflake.snowpark.types import DecimalType
       >>> df = session.create_dataframe([0, 1], schema=["N"]).select(
       ...     tan(col("N")).cast(DecimalType(scale=4))
       ... )
       >>> df.collect()
       [Row(CAST (TAN("N") AS NUMBER(38, 4))=Decimal('0.0000')), Row(CAST (TAN("N") AS NUMBER(38, 4))=Decimal('1.5574'))]
    """
    c = _to_col_if_str(e, "tan")
    return _call_function("tan", c, _emit_ast=_emit_ast)


@publicapi
def tanh(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Computes the hyperbolic tangent of its argument.

    Example::
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([0, 1], schema=["N"]).select(
        ...     tanh(col("N").cast(DecimalType(scale=4)))
        ... )
        >>> df.collect()
        [Row(TANH( CAST ("N" AS NUMBER(38, 4)))=0.0), Row(TANH( CAST ("N" AS NUMBER(38, 4)))=0.7615941559557649)]
    """
    c = _to_col_if_str(e, "tanh")
    return _call_function("tanh", c, _emit_ast=_emit_ast)


@publicapi
def degrees(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Converts radians to degrees.

    Example::

        >>> import math
        >>> from snowflake.snowpark.types import StructType, StructField, DoubleType, IntegerType
        >>> df = session.create_dataframe(
        ...     [math.pi / 3, math.pi, 3 * math.pi],
        ...     schema=StructType([StructField("a", DoubleType())]),
        ... )
        >>> df.select(degrees(col("a")).cast(IntegerType()).alias("DEGREES")).collect()
        [Row(DEGREES=60), Row(DEGREES=180), Row(DEGREES=540)]
    """
    c = _to_col_if_str(e, "degrees")
    return _call_function("degrees", c, _emit_ast=_emit_ast)


@publicapi
def radians(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts degrees to radians.

    Examples::

        >>> df = session.create_dataframe([[1.111], [2.222], [3.333]], schema=["a"])
        >>> df.select(radians(col("a")).cast("number(38, 5)").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.01939   |
        |0.03878   |
        |0.05817   |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "radians")
    return _call_function("radians", c, _emit_ast=_emit_ast)


@publicapi
def md5(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns a 32-character hex-encoded string containing the 128-bit MD5 message digest.

     Example::

        >>> df = session.create_dataframe(["a", "b"], schema=["col"]).select(md5("col"))
        >>> df.collect()
        [Row(MD5("COL")='0cc175b9c0f1b6a831c399e269772661'), Row(MD5("COL")='92eb5ffee6ae2fec3ad71c777531578f')]
    """
    c = _to_col_if_str(e, "md5")
    return _call_function("md5", c, _emit_ast=_emit_ast)


@publicapi
def sha1(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a 40-character hex-encoded string containing the 160-bit SHA-1 message digest.

    Example::
        >>> df = session.create_dataframe(["a", "b"], schema=["col"]).select(sha1("col"))
        >>> df.collect()
        [Row(SHA1("COL")='86f7e437faa5a7fce15d1ddcb9eaeaea377667b8'), Row(SHA1("COL")='e9d71f5ee7c92d6dc9e92ffdad17b8bd49418f98')]
    """
    c = _to_col_if_str(e, "sha1")
    return _call_function("sha1", c, _emit_ast=_emit_ast)


@publicapi
def sha2(e: ColumnOrName, num_bits: int, _emit_ast: bool = True) -> Column:
    """Returns a hex-encoded string containing the N-bit SHA-2 message digest,
    where N is the specified output digest size.

    Example::
        >>> df = session.create_dataframe(["a", "b"], schema=["col"]).select(sha2("col", 256))
        >>> df.collect()
        [Row(SHA2("COL", 256)='ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb'), Row(SHA2("COL", 256)='3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d')]
    """
    permitted_values = [0, 224, 256, 384, 512]
    if num_bits not in permitted_values:
        raise ValueError(
            f"num_bits {num_bits} is not in the permitted values {permitted_values}"
        )
    c = _to_col_if_str(e, "sha2")
    return _call_function("sha2", c, num_bits, _emit_ast=_emit_ast)


@publicapi
def hash(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns a signed 64-bit hash value. Note that HASH never returns NULL, even for NULL inputs.

    Examples::

        >>> import decimal
        >>> df = session.create_dataframe([[10, "10", decimal.Decimal(10), 10.0]], schema=["a", "b", "c", "d"])
        >>> df.select(hash("a").alias("hash_a"), hash("b").alias("hash_b"), hash("c").alias("hash_c"), hash("d").alias("hash_d")).collect()
        [Row(HASH_A=1599627706822963068, HASH_B=3622494980440108984, HASH_C=1599627706822963068, HASH_D=1599627706822963068)]
        >>> df.select(hash(lit(None)).alias("one"), hash(lit(None), lit(None)).alias("two"), hash(lit(None), lit(None), lit(None)).alias("three")).collect()
        [Row(ONE=8817975702393619368, TWO=953963258351104160, THREE=2941948363845684412)]
    """
    columns = [_to_col_if_str(c, "hash") for c in cols]
    return _call_function("hash", *columns, _emit_ast=_emit_ast)


@publicapi
def ascii(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the ASCII code for the first character of a string. If the string is empty,
    a value of 0 is returned.

    Example::

        >>> df = session.create_dataframe(['!', 'A', 'a', '', 'bcd', None], schema=['a'])
        >>> df.select(df.a, ascii(df.a).as_('ascii')).collect()
        [Row(A='!', ASCII=33), Row(A='A', ASCII=65), Row(A='a', ASCII=97), Row(A='', ASCII=0), Row(A='bcd', ASCII=98), Row(A=None, ASCII=None)]
    """
    c = _to_col_if_str(e, "ascii")
    return _call_function("ascii", c, _emit_ast=_emit_ast)


@publicapi
def initcap(
    e: ColumnOrName, delimiters: ColumnOrName = None, _emit_ast: bool = True
) -> Column:
    """
    Returns the input string with the first letter of each word in uppercase
    and the subsequent letters in lowercase.

    ``delimiters`` is an optional argument specifying a string of one or more
    characters that ``initcap`` uses as separators for words in the input expression.

    If ``delimiters`` is not specified, any of the following characters in the
    input expressions are treated as word separators:

        ``<whitespace> ! ? @ " ^ # $ & ~ _ , . : ; + - * % / | \\ [ ] ( ) { } < >``

    Examples::

        >>> df = session.create_dataframe(["the sky is blue", "WE CAN HANDLE THIS", "ÄäÖößÜü", None], schema=["a"])
        >>> df.select(initcap(df["a"]).alias("initcap")).collect()
        [Row(INITCAP='The Sky Is Blue'), Row(INITCAP='We Can Handle This'), Row(INITCAP='Ääöößüü'), Row(INITCAP=None)]
        >>> df.select(initcap(df["a"], lit('')).alias("initcap")).collect()
        [Row(INITCAP='The sky is blue'), Row(INITCAP='We can handle this'), Row(INITCAP='Ääöößüü'), Row(INITCAP=None)]
    """
    c = _to_col_if_str(e, "initcap")
    if delimiters is None:
        return _call_function("initcap", c, _emit_ast=_emit_ast)

    delimiter_col = _to_col_if_str(delimiters, "initcap")
    return _call_function("initcap", c, delimiter_col, _emit_ast=_emit_ast)


@publicapi
def length(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the length of an input string or binary value. For strings,
    the length is the number of characters, and UTF-8 characters are counted as a
    single character. For binary, the length is the number of bytes.

    Example::

        >>> df = session.create_dataframe(["the sky is blue", "WE CAN HANDLE THIS", "ÄäÖößÜü", None], schema=["a"])
        >>> df.select(length(df["a"]).alias("length")).collect()
        [Row(LENGTH=15), Row(LENGTH=18), Row(LENGTH=7), Row(LENGTH=None)]
    """
    c = _to_col_if_str(e, "length")
    return _call_function("length", c, _emit_ast=_emit_ast)


@publicapi
def lower(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the input string with all characters converted to lowercase.

    Example::

        >>> df = session.create_dataframe(['abc', 'Abc', 'aBC', 'Anführungszeichen', '14.95 €'], schema=["a"])
        >>> df.select(lower(col("a"))).collect()
        [Row(LOWER("A")='abc'), Row(LOWER("A")='abc'), Row(LOWER("A")='abc'), Row(LOWER("A")='anführungszeichen'), Row(LOWER("A")='14.95 €')]
    """
    c = _to_col_if_str(e, "lower")
    return _call_function("lower", c, _emit_ast=_emit_ast)


@publicapi
def lpad(
    e: ColumnOrName, len: Union[Column, int], pad: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Left-pads a string with characters from another string, or left-pads a
    binary value with bytes from another binary value.

    Example::

        >>> from snowflake.snowpark.functions import lit
        >>> df = session.create_dataframe([["a"], ["b"], ["c"]], schema=["a"])
        >>> df.select(lpad(col("a"), 3, lit("k")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |kka       |
        |kkb       |
        |kkc       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "lpad")
    p = _to_col_if_str(pad, "lpad")
    # Build AST here to prevent `len` from being recorded as a literal instead of int.
    ast = build_function_expr("lpad", [c, len, p]) if _emit_ast else None
    return _call_function(
        "lpad",
        c,
        len if isinstance(len, Column) else lit(len, _emit_ast=False),
        p,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def ltrim(
    e: ColumnOrName, trim_string: Optional[ColumnOrName] = None, _emit_ast: bool = True
) -> Column:
    """
    Removes leading characters, including whitespace, from a string.

    Example::

        >>> from snowflake.snowpark.functions import lit
        >>> df = session.create_dataframe([["asss"], ["bsss"], ["csss"]], schema=["a"])
        >>> df.select(rtrim(col("a"), trim_string=lit("sss")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |a         |
        |b         |
        |c         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "ltrim")
    t = _to_col_if_str(trim_string, "ltrim") if trim_string is not None else None
    return (
        _call_function("ltrim", c, t, _emit_ast=_emit_ast)
        if t is not None
        else _call_function("ltrim", c, _emit_ast=_emit_ast)
    )


@publicapi
def rpad(
    e: ColumnOrName, len: Union[Column, int], pad: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Right-pads a string with characters from another string, or right-pads a
    binary value with bytes from another binary value. When called, `e` is padded to length `len`
    with characters/bytes from `pad`.

    Example::

        >>> from snowflake.snowpark.functions import lit
        >>> df = session.create_dataframe([["a"], ["b"], ["c"]], schema=["a"])
        >>> df.select(rpad(col("a"), 3, lit("k")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |akk       |
        |bkk       |
        |ckk       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "rpad")
    p = _to_col_if_str(pad, "rpad")
    # Build AST here to prevent `len` from being recorded as a literal instead of int.
    ast = build_function_expr("rpad", [c, len, p]) if _emit_ast else None
    return _call_function(
        "rpad",
        c,
        len if isinstance(len, Column) else lit(len, _emit_ast=False),
        p,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def rtrim(
    e: ColumnOrName, trim_string: Optional[ColumnOrName] = None, _emit_ast: bool = True
) -> Column:
    """Removes trailing characters, including whitespace, from a string.

    Example::

        >>> from snowflake.snowpark.functions import lit
        >>> df = session.create_dataframe([["asss"], ["bsss"], ["csss"]], schema=["a"])
        >>> df.select(rtrim(col("a"), trim_string=lit("sss")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |a         |
        |b         |
        |c         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "rtrim")
    t = _to_col_if_str(trim_string, "rtrim") if trim_string is not None else None
    return (
        _call_function("rtrim", c, t, _emit_ast=_emit_ast)
        if t is not None
        else _call_function("rtrim", c, _emit_ast=_emit_ast)
    )


@publicapi
def repeat(s: ColumnOrName, n: Union[Column, int], _emit_ast: bool = True) -> Column:
    """Builds a string by repeating the input for the specified number of times.

    Example::

        >>> df = session.create_dataframe([["a"], ["b"], ["c"]], schema=["a"])
        >>> df.select(repeat(col("a"), 3).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |aaa       |
        |bbb       |
        |ccc       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(s, "repeat")
    # Build AST here to prevent `n` from being recorded as a literal instead of int.
    ast = build_function_expr("repeat", [c, n]) if _emit_ast else None
    return _call_function(
        "repeat",
        c,
        n if isinstance(n, Column) else lit(n, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def reverse(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Reverses the order of characters in a string, or of bytes in a binary value.

    Example::

        >>> df = session.create_dataframe([["Hello"], ["abc"]], schema=["col1"])
        >>> df.select(reverse(col("col1"))).show()
        -----------------------
        |"REVERSE(""COL1"")"  |
        -----------------------
        |olleH                |
        |cba                  |
        -----------------------
        <BLANKLINE>
    """
    col = _to_col_if_str(col, "reverse")
    return _call_function("reverse", col, _emit_ast=_emit_ast)


@publicapi
def soundex(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a string that contains a phonetic representation of the input string.

    Example::
        >>> df = session.create_dataframe(["Marsha", "Marcia"], schema=["V"]).select(soundex(col("V")))
        >>> df.collect()
        [Row(SOUNDEX("V")='M620'), Row(SOUNDEX("V")='M620')]
    """
    c = _to_col_if_str(e, "soundex")
    return _call_function("soundex", c, _emit_ast=_emit_ast)


@publicapi
def trim(
    e: ColumnOrName, trim_string: Optional[ColumnOrName] = None, _emit_ast: bool = True
) -> Column:
    """Removes leading and trailing characters from a string. Per default only whitespace ' ' characters are removed.

    Example::

        >>> df = session.create_dataframe(['hello', ' world', '   !   '], schema=["a"])
        >>> df.collect()
        [Row(A='hello'), Row(A=' world'), Row(A='   !   ')]
        >>> df.select(trim(col("a"))).collect()
        [Row(TRIM("A")='hello'), Row(TRIM("A")='world'), Row(TRIM("A")='!')]

    Example::

        >>> df = session.create_dataframe(['EUR 12.96', '7.89USD', '5.99E'], schema=["a"])
        >>> df.select(trim(col("a"), lit("EURUSD ")).as_("ans")).collect()
        [Row(ANS='12.96'), Row(ANS='7.89'), Row(ANS='5.99')]

    Example::

        >>> df = session.create_dataframe(['abc12 45a 79bc!'], schema=["a"])
        >>> df.select(trim(col("a"), lit("abc!")).as_("ans")).collect()
        [Row(ANS='12 45a 79')]

    """
    c = _to_col_if_str(e, "trim")
    t = _to_col_if_str(trim_string, "trim") if trim_string is not None else None
    return (
        _call_function("trim", c, t, _emit_ast=_emit_ast)
        if t is not None
        else _call_function("trim", c, _emit_ast=_emit_ast)
    )


@publicapi
def upper(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the input string with all characters converted to uppercase.
       Unicode characters are supported.

    Example::

        >>> df = session.create_dataframe(['abc', 'Abc', 'aBC', 'Anführungszeichen', '14.95 €'], schema=["a"])
        >>> df.select(upper(col("a"))).collect()
        [Row(UPPER("A")='ABC'), Row(UPPER("A")='ABC'), Row(UPPER("A")='ABC'), Row(UPPER("A")='ANFÜHRUNGSZEICHEN'), Row(UPPER("A")='14.95 €')]
    """
    c = _to_col_if_str(e, "upper")
    return _call_function("upper", c, _emit_ast=_emit_ast)


@publicapi
def strtok_to_array(
    text: ColumnOrName, delimiter: Optional[ColumnOrName] = None, _emit_ast: bool = True
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
    d = (
        _to_col_if_str(delimiter, "strtok_to_array")
        if (delimiter is not None)
        else None
    )
    return (
        _call_function("strtok_to_array", t, d, _emit_ast=_emit_ast)
        if (delimiter is not None)
        else _call_function("strtok_to_array", t, _emit_ast=_emit_ast)
    )


@publicapi
def struct(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns an OBJECT constructed with the given columns.

    Example::
        >>> from snowflake.snowpark.functions import struct
        >>> df = session.createDataFrame([("Bob", 80), ("Alice", None)], ["name", "age"])
        >>> res = df.select(struct("age", "name").alias("struct")).show()
        ---------------------
        |"STRUCT"           |
        ---------------------
        |{                  |
        |  "age": 80,       |
        |  "name": "Bob"    |
        |}                  |
        |{                  |
        |  "age": null,     |
        |  "name": "Alice"  |
        |}                  |
        ---------------------
        <BLANKLINE>
    """
    # AST.
    ast = build_function_expr("struct", cols) if _emit_ast else None

    def flatten_col_list(obj):
        if isinstance(obj, str) or isinstance(obj, Column):
            return [obj]
        elif hasattr(obj, "__iter__"):
            acc = []
            for innerObj in obj:
                acc = acc + flatten_col_list(innerObj)
            return acc

    new_cols = []
    for c in flatten_col_list(cols):
        # first insert field_name
        if isinstance(c, str):
            new_cols.append(lit(c, _emit_ast=False))
        else:
            name = c._expression.name
            name = name[1:] if name.startswith('"') else name
            name = name[:-1] if name.endswith('"') else name
            new_cols.append(lit(name, _emit_ast=False))
        # next insert field value
        c = _to_col_if_str(c, "struct")
        if isinstance(c, Column) and isinstance(c._expression, Alias):
            new_cols.append(Column(c._expression.children[0], _emit_ast=False))
        else:
            new_cols.append(c)
    ans = object_construct_keep_null(*new_cols, _emit_ast=False)
    ans._ast = ast

    return ans


@publicapi
def log(
    base: Union[ColumnOrName, int, float],
    x: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the logarithm of a numeric expression.

    Example::

        >>> from snowflake.snowpark.types import IntegerType
        >>> df = session.create_dataframe([1, 10], schema=["a"])
        >>> df.select(log(10, df["a"]).cast(IntegerType()).alias("log")).collect()
        [Row(LOG=0), Row(LOG=1)]
    """
    # Build AST here to prevent `base` and `x` from being recorded as a literal instead of int/float.
    ast = build_function_expr("log", [base, x]) if _emit_ast else None
    b = (
        lit(base, _emit_ast=False)
        if isinstance(base, (int, float))
        else _to_col_if_str(base, "log")
    )
    arg = (
        lit(x, _emit_ast=False)
        if isinstance(x, (int, float))
        else _to_col_if_str(x, "log")
    )
    return _call_function("log", b, arg, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def log1p(
    x: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the natural logarithm of (1 + x).

    Example::

        >>> df = session.create_dataframe([0, 1], schema=["a"])
        >>> df.select(log1p(df["a"]).alias("log1p")).collect()
        [Row(LOG1P=0.0), Row(LOG1P=0.6931471805599453)]
    """
    ast = build_function_expr("log1p", [x]) if _emit_ast else None
    x = (
        lit(x, _emit_ast=False)
        if isinstance(x, (int, float))
        else _to_col_if_str(x, "log")
    )
    one_plus_x = _to_col_if_str(x, "log1p") + lit(1, _emit_ast=False)
    ans = ln(one_plus_x, _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def log10(
    x: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the base-10 logarithm of x.

    Example::

        >>> df = session.create_dataframe([1, 10], schema=["a"])
        >>> df.select(log10(df["a"]).alias("log10")).collect()
        [Row(LOG10=0.0), Row(LOG10=1.0)]
    """
    ans = _log10(x)
    ans._ast = build_function_expr("log10", [x]) if _emit_ast else None
    return ans


@publicapi
def log2(
    x: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the base-2 logarithm of x.

    Example::

        >>> df = session.create_dataframe([1, 2, 8], schema=["a"])
        >>> df.select(log2(df["a"]).alias("log2")).collect()
        [Row(LOG2=0.0), Row(LOG2=1.0), Row(LOG2=3.0)]
    """
    ans = _log2(x)
    ans._ast = build_function_expr("log2", [x]) if _emit_ast else None
    return ans


# Create base 2 and base 10 wrappers for use with the Modin log2 and log10 functions
def _log2(
    x: Union[ColumnOrName, int, float],
) -> Column:
    return log(2, x, _emit_ast=False)


def _log10(
    x: Union[ColumnOrName, int, float],
) -> Column:
    return log(10, x, _emit_ast=False)


@publicapi
def pow(
    left: Union[ColumnOrName, int, float],
    right: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
) -> Column:
    """Returns a number (left) raised to the specified power (right).

    Example::
        >>> df = session.create_dataframe([[2, 3], [3, 4]], schema=["x", "y"])
        >>> df.select(pow(col("x"), col("y")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |8.0       |
        |81.0      |
        ------------
        <BLANKLINE>
    """
    # Build AST here to prevent `left` and `right` from being recorded as a literal instead of int/float.
    ast = build_function_expr("pow", [left, right]) if _emit_ast else None
    number = (
        lit(left, _emit_ast=False)
        if isinstance(left, (int, float))
        else _to_col_if_str(left, "pow")
    )
    power = (
        lit(right, _emit_ast=False)
        if isinstance(right, (int, float))
        else _to_col_if_str(right, "pow")
    )
    return _call_function("pow", number, power, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def round(
    e: ColumnOrName, scale: Union[ColumnOrName, int, float] = 0, _emit_ast: bool = True
) -> Column:
    """Returns rounded values from the specified column.

    Example::

        >>> df = session.create_dataframe([[1.11], [2.22], [3.33]], schema=["a"])
        >>> df.select(round(col("a")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.0       |
        |2.0       |
        |3.0       |
        ------------
        <BLANKLINE>
    """
    # Build AST here to prevent `scale` from being recorded as a literal instead of int/float.
    ast = build_function_expr("round", [e, scale]) if _emit_ast else None

    c = _to_col_if_str(e, "round")
    scale_col = (
        lit(scale, _emit_ast=False)
        if isinstance(scale, (int, float))
        else _to_col_if_str(scale, "round")
    )

    return _call_function("round", c, scale_col, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def sign(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the sign of its argument:

        - -1 if the argument is negative.
        - 1 if it is positive.
        - 0 if it is 0.

    Args:
        col: The column to evaluate its sign

    Example::
        >>> df = session.create_dataframe([(-2, 2, 0)], ["a", "b", "c"])
        >>> df.select(sign("a").alias("a_sign"), sign("b").alias("b_sign"), sign("c").alias("c_sign")).show()
        ----------------------------------
        |"A_SIGN"  |"B_SIGN"  |"C_SIGN"  |
        ----------------------------------
        |-1        |1         |0         |
        ----------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "sign")
    return _call_function("sign", c, _emit_ast=_emit_ast)


@publicapi
def split(str: ColumnOrName, pattern: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Splits a given string with a given separator and returns the result in an array
    of strings. To specify a string separator, use the :func:`lit()` function.

    Example 1::

        >>> df = session.create_dataframe(
        ...     [["many-many-words", "-"], ["hello--hello", "--"]],
        ...     schema=["V", "D"],
        ... ).select(split(col("V"), col("D")))
        >>> df.show()
        -------------------------
        |"SPLIT(""V"", ""D"")"  |
        -------------------------
        |[                      |
        |  "many",              |
        |  "many",              |
        |  "words"              |
        |]                      |
        |[                      |
        |  "hello",             |
        |  "hello"              |
        |]                      |
        -------------------------
        <BLANKLINE>

    Example 2::

        >>> df = session.create_dataframe([["many-many-words"],["hello-hi-hello"]],schema=["V"],)
        >>> df.select(split(col("V"), lit("-"))).show()
        -----------------------
        |"SPLIT(""V"", '-')"  |
        -----------------------
        |[                    |
        |  "many",            |
        |  "many",            |
        |  "words"            |
        |]                    |
        |[                    |
        |  "hello",           |
        |  "hi",              |
        |  "hello"            |
        |]                    |
        -----------------------
        <BLANKLINE>
    """
    s = _to_col_if_str(str, "split")
    p = _to_col_if_str(pattern, "split")
    return _call_function("split", s, p, _emit_ast=_emit_ast)


@publicapi
def substring(
    str: ColumnOrName,
    pos: Union[Column, int],
    len: Optional[Union[Column, int]] = None,
    _emit_ast: bool = True,
) -> Column:
    """Returns the portion of the string or binary value str, starting from the
    character/byte specified by pos, with limited length. The length should be greater
    than or equal to zero. If the length is a negative number, the function returns an
    empty string.

    Note:
        For ``pos``, 1 is the first character of the string in Snowflake database.

    :func:`substr` is an alias of :func:`substring`.

    Example 1::
        >>> df = session.create_dataframe(
        ...     ["abc", "def"],
        ...     schema=["S"],
        ... )
        >>> df.select(substring(col("S"), 1, 1)).collect()
        [Row(SUBSTRING("S", 1, 1)='a'), Row(SUBSTRING("S", 1, 1)='d')]

    Example 2::
        >>> df = session.create_dataframe(
        ...     ["abc", "def"],
        ...     schema=["S"],
        ... )
        >>> df.select(substring(col("S"), 2)).collect()
        [Row(SUBSTRING("S", 2)='bc'), Row(SUBSTRING("S", 2)='ef')]
    """
    s = _to_col_if_str(str, "substring")
    # Build AST here to prevent `pos` and `len` from being recorded as a literal instead of int/None.
    ast = (
        build_function_expr("substring", [s, pos] if len is None else [s, pos, len])
        if _emit_ast
        else None
    )
    p = pos if isinstance(pos, Column) else lit(pos, _emit_ast=False)
    if len is None:
        return _call_function("substring", s, p, _ast=ast, _emit_ast=_emit_ast)
    length = len if isinstance(len, Column) else lit(len, _emit_ast=False)
    return _call_function("substring", s, p, length, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def substring_index(
    text: ColumnOrName, delim: ColumnOrLiteralStr, count: int, _emit_ast: bool = True
) -> Column:
    """
    Returns the substring from string ``text`` before ``count`` occurrences of the delimiter ``delim``.
    If ``count`` is positive, everything to the left of the final delimiter (counting from left) is
    returned. If ``count`` is negative, everything to the right of the final delimiter (counting from the
    right) is returned. If ``count`` is zero, returns empty string.

    Example 1::
        >>> df = session.create_dataframe(
        ...     ["a.b.c.d"],
        ...     schema=["S"],
        ... ).select(substring_index(col("S"), ".", 2).alias("result"))
        >>> df.show()
        ------------
        |"RESULT"  |
        ------------
        |a.b       |
        ------------
        <BLANKLINE>

    Example 2::
        >>> df = session.create_dataframe(
        ...     [["a.b.c.d", "."]],
        ...     schema=["S", "delimiter"],
        ... ).select(substring_index(col("S"), col("delimiter"), 2).alias("result"))
        >>> df.show()
        ------------
        |"RESULT"  |
        ------------
        |a.b       |
        ------------
        <BLANKLINE>
    """

    # AST.
    ast = (
        build_function_expr("substring_index", [text, delim, count])
        if _emit_ast
        else None
    )

    s = _to_col_if_str(text, "substring_index")
    strtok_array = _call_function("strtok_to_array", s, delim, _emit_ast=False)
    return _call_function(
        "array_to_string",
        _call_function(
            "array_slice",
            strtok_array,
            0
            if count >= 0
            else _call_function("array_size", strtok_array, _emit_ast=False) + count,
            count
            if count >= 0
            else _call_function("array_size", strtok_array, _emit_ast=False),
            _emit_ast=False,
        ),
        delim,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def regexp_count(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    position: Union[Column, int] = 1,
    *parameters: ColumnOrLiteral,
    _emit_ast: bool = True,
) -> Column:
    """Returns the number of times that a pattern occurs in the subject.

    Example::

        >>> df = session.sql("select * from values('apple'),('banana'),('peach') as T(a)")
        >>> df.select(regexp_count(col("a"), "a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        |3         |
        |1         |
        ------------
        <BLANKLINE>
    """
    sql_func_name = "regexp_count"
    sub = _to_col_if_str(subject, sql_func_name)

    # Build AST here to prevent `pattern` and `position` from being recorded as a literal instead of int/str.
    ast = (
        build_function_expr(sql_func_name, [sub, pattern, position, *parameters])
        if _emit_ast
        else None
    )

    pat = pattern if isinstance(pattern, Column) else lit(pattern, _emit_ast=False)
    pos = position if isinstance(position, Column) else lit(position, _emit_ast=False)

    params = [lit(p, _emit_ast=False) for p in parameters]
    return _call_function(
        sql_func_name, sub, pat, pos, *params, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def regexp_extract(
    value: ColumnOrLiteralStr,
    regexp: ColumnOrLiteralStr,
    idx: int,
    _emit_ast: bool = True,
) -> Column:
    r"""
    Extract a specific group matched by a regex, from the specified string column.
    If the regex did not match, or the specified group did not match,
    an empty string is returned.

    Example::

        >>> from snowflake.snowpark.functions import regexp_extract
        >>> df = session.createDataFrame([["id_20_30", 10], ["id_40_50", 30]], ["id", "age"])
        >>> df.select(regexp_extract("id", r"(\d+)", 1).alias("RES")).show()
        ---------
        |"RES"  |
        ---------
        |20     |
        |40     |
        ---------
        <BLANKLINE>
    """

    # AST.
    ast = (
        build_function_expr("regexp_extract", [value, regexp, idx])
        if _emit_ast
        else None
    )

    value = _to_col_if_str(value, "regexp_extract")
    regexp = _to_col_if_lit(regexp, "regexp_extract")
    idx = _to_col_if_lit(idx, "regexp_extract")
    c = coalesce(
        _call_function(
            "regexp_substr",
            value,
            regexp,
            lit(1),
            lit(1),
            lit("e"),
            idx,
            _emit_ast=False,
        ),
        lit("", _emit_ast=False),
        _emit_ast=False,
    )

    c._ast = ast

    return c


@publicapi
def regexp_replace(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    replacement: ColumnOrLiteralStr = "",
    position: Union[Column, int] = 1,
    occurrences: Union[Column, int] = 0,
    *parameters: ColumnOrLiteral,
    _emit_ast: bool = True,
) -> Column:
    """Returns the subject with the specified pattern (or all occurrences of the pattern) either removed or replaced by a replacement string.
    If no matches are found, returns the original subject.

    Example::
        >>> df = session.create_dataframe(
        ...     [["It was the best of times, it was the worst of times"]], schema=["a"]
        ... )
        >>> df.select(regexp_replace(col("a"), lit("( ){1,}"), lit("")).alias("result")).show()
        --------------------------------------------
        |"RESULT"                                  |
        --------------------------------------------
        |Itwasthebestoftimes,itwastheworstoftimes  |
        --------------------------------------------
        <BLANKLINE>
    """
    # Build AST here to prevent `pattern`, `replacement`, `position`, `occurrences` from being recorded as a literal
    # instead of int/str.
    ast = (
        build_function_expr(
            "regexp_replace",
            [subject, pattern, replacement, position, occurrences, *parameters],
        )
        if _emit_ast
        else None
    )

    sql_func_name = "regexp_replace"
    sub = _to_col_if_str(subject, sql_func_name)

    pat = pattern if isinstance(pattern, Column) else lit(pattern, _emit_ast=False)
    rep = (
        replacement
        if isinstance(replacement, Column)
        else lit(replacement, _emit_ast=False)
    )
    pos = position if isinstance(position, Column) else lit(position, _emit_ast=False)
    occ = (
        occurrences
        if isinstance(occurrences, Column)
        else lit(occurrences, _emit_ast=False)
    )

    params = [
        p if isinstance(p, Column) else lit(p, _emit_ast=False) for p in parameters
    ]
    return _call_function(
        sql_func_name, sub, pat, rep, pos, occ, *params, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def replace(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    replacement: ColumnOrLiteralStr = "",
    _emit_ast: bool = True,
) -> Column:
    """
    Removes all occurrences of a specified subject and optionally replaces them with replacement.

    Example::

        >>> df = session.create_dataframe([["apple"], ["apple pie"], ["apple juice"]], schema=["a"])
        >>> df.select(replace(col("a"), "apple", "orange").alias("result")).show()
        ----------------
        |"RESULT"      |
        ----------------
        |orange        |
        |orange pie    |
        |orange juice  |
        ----------------
        <BLANKLINE>
    """
    # Build AST here to prevent `pattern` and `replacement` from being recorded as a literal instead of str.
    ast = (
        build_function_expr("replace", [subject, pattern, replacement])
        if _emit_ast
        else None
    )
    sql_func_name = "replace"
    sub = _to_col_if_str(subject, sql_func_name)
    pat = pattern if isinstance(pattern, Column) else lit(pattern, _emit_ast=False)
    rep = (
        replacement
        if isinstance(replacement, Column)
        else lit(replacement, _emit_ast=False)
    )
    return _call_function(sql_func_name, sub, pat, rep, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def charindex(
    target_expr: ColumnOrName,
    source_expr: ColumnOrName,
    position: Optional[Union[Column, int]] = None,
    _emit_ast: bool = True,
) -> Column:
    """Searches for ``target_expr`` in ``source_expr`` and, if successful,
    returns the position (1-based) of the ``target_expr`` in ``source_expr``.

    Args:
        target_expr: A string or binary expression representing the value to look for.
        source_expr: A string or binary expression representing the value to search.
        position: A number indication the position (1-based) from where to start the search. Defaults to None.

    Examples::
        >>> df = session.create_dataframe(["banana"], schema=['a'])
        >>> df.select(charindex(lit("an"), df.a, 1).as_("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |2         |
        ------------
        <BLANKLINE>

        >>> df.select(charindex(lit("an"), df.a, 3).as_("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |4         |
        ------------
        <BLANKLINE>
    """
    # Build AST here to prevent `position` from being recorded as a literal instead of int/None.
    ast = (
        build_function_expr(
            "charindex",
            [target_expr, source_expr]
            if position is None
            else [target_expr, source_expr, position],
        )
        if _emit_ast
        else None
    )
    t = _to_col_if_str(target_expr, "charindex")
    s = _to_col_if_str(source_expr, "charindex")
    return (
        _call_function(
            "charindex",
            t,
            s,
            position
            if isinstance(position, Column)
            else lit(position, _emit_ast=False),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
        if position is not None
        else _call_function("charindex", t, s, _ast=ast, _emit_ast=_emit_ast)
    )


@publicapi
def collate(e: Column, collation_spec: str, _emit_ast: bool = True) -> Column:
    """Returns a copy of the original :class:`Column` with the specified ``collation_spec``
    property, rather than the original collation specification property.

    For details, see the Snowflake documentation on
    `collation specifications <https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification>`_.

    Example::
        >>> df = session.create_dataframe(['ñ'], schema=['v'])
        >>> df.select(df.v == lit('Ñ'), collate(df.v, 'sp-upper') == lit('Ñ')).show()
        ----------------------------------------------------------
        |"(""V"" = 'Ñ')"  |"(COLLATE(""V"", 'SP-UPPER') = 'Ñ')"  |
        ----------------------------------------------------------
        |False            |True                                  |
        ----------------------------------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "collate")
    return _call_function("collate", c, collation_spec, _emit_ast=_emit_ast)


@publicapi
def collation(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the collation specification of expr.

    Example::
        >>> df = session.create_dataframe(['ñ'], schema=['v'])
        >>> df.select(collation(collate(df.v, 'sp-upper'))).show()
        -------------------------------------------
        |"COLLATION(COLLATE(""V"", 'SP-UPPER'))"  |
        -------------------------------------------
        |sp-upper                                 |
        -------------------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "collation")
    return _call_function("collation", c, _emit_ast=_emit_ast)


@publicapi
def concat(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Concatenates one or more strings, or concatenates one or more binary values. If any of the values is null, the result is also null.

    Example::
        >>> df = session.create_dataframe([['Hello', 'World']], schema=['a', 'b'])
        >>> df.select(concat(df.a, df.b)).show()
        --------------------------
        |"CONCAT(""A"", ""B"")"  |
        --------------------------
        |HelloWorld              |
        --------------------------
        <BLANKLINE>
    """
    columns = [_to_col_if_str(c, "concat") for c in cols]
    return _call_function("concat", *columns, _emit_ast=_emit_ast)


@publicapi
def concat_ws(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Concatenates two or more strings, or concatenates two or more binary values. If any of the values is null, the result is also null.
    The CONCAT_WS operator requires at least two arguments, and uses the first argument to separate all following arguments.

    Examples::
        >>> from snowflake.snowpark.functions import lit
        >>> df = session.create_dataframe([['Hello', 'World']], schema=['a', 'b'])
        >>> df.select(concat_ws(lit(','), df.a, df.b)).show()
        ----------------------------------
        |"CONCAT_WS(',', ""A"", ""B"")"  |
        ----------------------------------
        |Hello,World                     |
        ----------------------------------
        <BLANKLINE>

        >>> df = session.create_dataframe([['Hello', 'World', ',']], schema=['a', 'b', 'sep'])
        >>> df.select(concat_ws('sep', df.a, df.b)).show()
        --------------------------------------
        |"CONCAT_WS(""SEP"", ""A"", ""B"")"  |
        --------------------------------------
        |Hello,World                         |
        --------------------------------------
        <BLANKLINE>
    """
    columns = [_to_col_if_str(c, "concat_ws") for c in cols]
    return _call_function("concat_ws", *columns, _emit_ast=_emit_ast)


@publicapi
def _concat_ws_ignore_nulls(
    sep: str, *cols: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Concatenates two or more strings, or concatenates two or more binary values. Null values are ignored.

    Args:
        sep: The separator to use between the strings.

    Examples::
        >>> df = session.create_dataframe([
        ...     ['Hello', 'World', None],
        ...     [None, None, None],
        ...     ['Hello', None, None],
        ... ], schema=['a', 'b', 'c'])
        >>> df.select(_concat_ws_ignore_nulls(',', df.a, df.b, df.c)).show()
        ----------------------------------------------------
        |"CONCAT_WS_IGNORE_NULLS(',', ""A"",""B"",""C"")"  |
        ----------------------------------------------------
        |Hello,World                                       |
        |                                                  |
        |Hello                                             |
        ----------------------------------------------------
        <BLANKLINE>

        >>> df.select(_concat_ws_ignore_nulls('--', df.a, df.b, df.c)).show()
        -----------------------------------------------------
        |"CONCAT_WS_IGNORE_NULLS('--', ""A"",""B"",""C"")"  |
        -----------------------------------------------------
        |Hello--World                                       |
        |                                                   |
        |Hello                                              |
        -----------------------------------------------------
        <BLANKLINE>

        >>> df = session.create_dataframe([
        ...     (['Hello', 'World', None], None, '!'),
        ...     (['Hi', 'World', "."], "I'm Dad", '.'),
        ... ], schema=['a', 'b', 'c'])
        >>> df.select(_concat_ws_ignore_nulls(", ", "a", "b", "c")).show()
        -----------------------------------------------------
        |"CONCAT_WS_IGNORE_NULLS(', ', ""A"",""B"",""C"")"  |
        -----------------------------------------------------
        |Hello, World, !                                    |
        |Hi, World, ., I'm Dad, .                           |
        -----------------------------------------------------
        <BLANKLINE>
    """
    ast = (
        build_function_expr("_concat_ws_ignore_nulls", [sep, *cols])
        if _emit_ast
        else None
    )
    columns = [_to_col_if_str(c, "_concat_ws_ignore_nulls") for c in cols]
    names = ",".join([c.get_name() or f"COL{i}" for i, c in enumerate(columns)])

    # The implementation of this function is as follows with example input of
    # sep = "," and row = [a, NULL], b, NULL, c:
    # 1. Cast all columns to array.
    #   [a, NULL], [b], NULL, [c]
    # 2. Combine all arrays into a array of arrays after removing nulls (array_construct_compact).
    #   [[a, NULL], [b], [c]]
    # 3. Flatten the array of arrays into a single array (array_flatten).
    #   [a, NULL, b, c]
    # 4. Filter out nulls (array_remove_nulls).
    #   [a, b, c]
    # 5. Concatenate the non-null values into a single string (array_to_string).
    #   "a,b,c"

    def array_remove_nulls(col: Column) -> Column:
        """Expects an array and returns an array with nulls removed."""
        return _call_function(
            "filter",
            col,
            sql_expr("x -> NOT IS_NULL_VALUE(x)", _emit_ast=False),
            _emit_ast=False,
        )

    res = array_to_string(
        array=array_remove_nulls(
            array_flatten(
                array_construct_compact(
                    *[c.cast(ArrayType(), _emit_ast=False) for c in columns],
                    _emit_ast=False,
                ),
                _emit_ast=False,
            )
        ),
        separator=lit(sep, _emit_ast=False),
        _emit_ast=False,
    )._alias(f"CONCAT_WS_IGNORE_NULLS('{sep}', {names})")
    res._ast = ast
    return res


@publicapi
def translate(
    src: ColumnOrName,
    source_alphabet: ColumnOrName,
    target_alphabet: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """Translates src from the characters in source_alphabet to the characters in
    target_alphabet. Each character matching a character at position i in the source_alphabet is replaced
    with the character at position i in the target_alphabet. If target_alphabet is shorter, and there is no corresponding
    character the character is omitted. target_alphabet can not be longer than source_alphabet.

    Example::

        >>> df = session.create_dataframe(["abcdef", "abba"], schema=["a"])
        >>> df.select(translate(col("a"), lit("abc"), lit("ABC")).as_("ans")).collect()
        [Row(ANS='ABCdef'), Row(ANS='ABBA')]

        >>> df = session.create_dataframe(["file with spaces.txt", "\\ttest"], schema=["a"])
        >>> df.select(translate(col("a"), lit(" \\t"), lit("_")).as_("ans")).collect()
        [Row(ANS='file_with_spaces.txt'), Row(ANS='test')]

    """
    source = _to_col_if_str(src, "translate")
    source_alphabet = _to_col_if_str(source_alphabet, "translate")
    target_alphabet = _to_col_if_str(target_alphabet, "translate")
    return _call_function(
        "translate", source, source_alphabet, target_alphabet, _emit_ast=_emit_ast
    )


@publicapi
def contains(col: ColumnOrName, string: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns if `col` contains `string` for each row. See `CONTAINS <https://docs.snowflake.com/en/sql-reference/functions/contains>`

    Example:
        >>> df = session.create_dataframe([[1,2], [3,4], [5,5] ], schema=["a","b"])
        >>> df.select(contains(col("a"), col("b")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |False     |
        |False     |
        |True      |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "contains")
    s = _to_col_if_str(string, "contains")
    return _call_function("contains", c, s, _emit_ast=_emit_ast)


@publicapi
def startswith(col: ColumnOrName, str: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns true if col starts with str.

    Example::
        >>> df = session.create_dataframe(
        ...     [["abc", "a"], ["abc", "s"]],
        ...     schema=["S", "P"],
        ... ).select(startswith(col("S"), col("P")))
        >>> df.collect()
        [Row(STARTSWITH("S", "P")=True), Row(STARTSWITH("S", "P")=False)]
    """
    c = _to_col_if_str(col, "startswith")
    s = _to_col_if_str(str, "startswith")
    return _call_function("startswith", c, s, _emit_ast=_emit_ast)


@publicapi
def endswith(col: ColumnOrName, str: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if col ends with str.

    Example::

        >>> df = session.create_dataframe(["apple", "banana", "peach"], schema=["a"])
        >>> df.select(endswith(df["a"], lit("ana")).alias("endswith")).collect()
        [Row(ENDSWITH=False), Row(ENDSWITH=True), Row(ENDSWITH=False)]
    """
    c = _to_col_if_str(col, "endswith")
    s = _to_col_if_str(str, "endswith")
    return _call_function("endswith", c, s, _emit_ast=_emit_ast)


@publicapi
def insert(
    base_expr: ColumnOrName,
    position: Union[Column, int],
    length: Union[Column, int],
    insert_expr: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Replaces a substring of the specified length, starting at the specified position,
    with a new string or binary value.

    Examples::

        >>> df = session.create_dataframe(["abc"], schema=["a"])
        >>> df.select(insert(df["a"], 1, 2, lit("Z")).alias("insert")).collect()
        [Row(INSERT='Zc')]
    """
    # Build AST here to prevent `position` and `length` from being recorded as a literal instead of int.
    ast = (
        build_function_expr("insert", [base_expr, position, length, insert_expr])
        if _emit_ast
        else None
    )
    b = _to_col_if_str(base_expr, "insert")
    i = _to_col_if_str(insert_expr, "insert")
    return _call_function(
        "insert",
        b,
        position if isinstance(position, Column) else lit(position, _emit_ast=False),
        length if isinstance(length, Column) else lit(length, _emit_ast=False),
        i,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def left(
    str_expr: ColumnOrName, length: Union[Column, int], _emit_ast: bool = True
) -> Column:
    """Returns a left most substring of ``str_expr``.

    Example::

        >>> df = session.create_dataframe([["abc"], ["def"]], schema=["a"])
        >>> df.select(left(col("a"), 2).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |ab        |
        |de        |
        ------------
        <BLANKLINE>
    """
    # Build AST here to prevent `length` from being recorded as a literal instead of int.
    ast = build_function_expr("left", [str_expr, length]) if _emit_ast else None
    s = _to_col_if_str(str_expr, "left")
    return _call_function(
        "left",
        s,
        length if isinstance(length, Column) else lit(length, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def right(
    str_expr: ColumnOrName, length: Union[Column, int], _emit_ast: bool = True
) -> Column:
    """Returns a right most substring of ``str_expr``.

    Example::

        >>> df = session.create_dataframe([["abc"], ["def"]], schema=["a"])
        >>> df.select(right(col("a"), 2).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |bc        |
        |ef        |
        ------------
        <BLANKLINE>
    """
    # Build AST here to prevent `length` from being recorded as a literal instead of int.
    ast = build_function_expr("right", [str_expr, length]) if _emit_ast else None
    s = _to_col_if_str(str_expr, "right")
    return _call_function(
        "right",
        s,
        length if isinstance(length, Column) else lit(length, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def char(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts a Unicode code point (including 7-bit ASCII) into the character that
    matches the input Unicode.

    Example::

        >>> df = session.create_dataframe([83, 33, 169, 8364, None], schema=['a'])
        >>> df.select(df.a, char(df.a).as_('char')).sort(df.a).show()
        -----------------
        |"A"   |"CHAR"  |
        -----------------
        |NULL  |NULL    |
        |33    |!       |
        |83    |S       |
        |169   |©       |
        |8364  |€       |
        -----------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "char")
    return _call_function("char", c, _emit_ast=_emit_ast)


@publicapi
def to_char(
    c: ColumnOrName, format: Optional[str] = None, _emit_ast: bool = True
) -> Column:
    """Converts a Unicode code point (including 7-bit ASCII) into the character that
    matches the input Unicode.

    Example::
        >>> df = session.create_dataframe([1, 2, 3, 4], schema=['a'])
        >>> df.select(to_char(col('a')).as_('ans')).collect()
        [Row(ANS='1'), Row(ANS='2'), Row(ANS='3'), Row(ANS='4')]

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([datetime.datetime(2023, 4, 16), datetime.datetime(2017, 4, 3, 2, 59, 37, 153)], schema=['a'])
        >>> df.select(to_char(col('a')).as_('ans')).collect()
        [Row(ANS='2023-04-16 00:00:00.000'), Row(ANS='2017-04-03 02:59:37.000')]

    """
    # Build AST here to prevent `format` from being recorded as a literal instead of str/None.
    ast = (
        build_function_expr("to_char", [c] if format is None else [c, format])
        if _emit_ast
        else None
    )
    c = _to_col_if_str(c, "to_char")
    return (
        _call_function(
            "to_char",
            c,
            format if isinstance(format, Column) else lit(format, _emit_ast=False),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
        if format is not None
        else _call_function("to_char", c, _ast=ast, _emit_ast=_emit_ast)
    )


@publicapi
def date_format(
    c: ColumnOrName, fmt: ColumnOrLiteralStr, _emit_ast: bool = True
) -> Column:
    """Converts an input expression into the corresponding date in the specified date format.

    Example::
        >>> df = session.create_dataframe([("2023-10-10",), ("2022-05-15",), ("invalid",)], schema=['date'])
        >>> df.select(date_format('date', 'YYYY/MM/DD').as_('formatted_date')).show()
        --------------------
        |"FORMATTED_DATE"  |
        --------------------
        |2023/10/10        |
        |2022/05/15        |
        |NULL              |
        --------------------
        <BLANKLINE>

    Example::
        >>> df = session.create_dataframe([("2023-10-10 15:30:00",), ("2022-05-15 10:45:00",)], schema=['timestamp'])
        >>> df.select(date_format('timestamp', 'YYYY/MM/DD HH:mi:ss').as_('formatted_ts')).show()
        -----------------------
        |"FORMATTED_TS"       |
        -----------------------
        |2023/10/10 15:30:00  |
        |2022/05/15 10:45:00  |
        -----------------------
        <BLANKLINE>

    Example::
        >>> df = session.sql("select '2023-10-10'::DATE as date_col, '2023-10-10 15:30:00'::TIMESTAMP as timestamp_col")
        >>> df.select(
        ...     date_format('date_col', 'YYYY/MM/DD').as_('formatted_dt'),
        ...     date_format('timestamp_col', 'YYYY/MM/DD HH:mi:ss').as_('formatted_ts')
        ... ).show()
        ----------------------------------------
        |"FORMATTED_DT"  |"FORMATTED_TS"       |
        ----------------------------------------
        |2023/10/10      |2023/10/10 15:30:00  |
        ----------------------------------------
        <BLANKLINE>
    """

    # AST.
    ast = build_function_expr("date_format", [c, fmt]) if _emit_ast else None

    ans = to_char(
        try_cast(to_char(c, _emit_ast=False), TimestampType(), _emit_ast=False),
        fmt,
        _emit_ast=False,
    )
    ans._ast = ast
    return ans


@publicapi
def to_time(
    e: ColumnOrName, fmt: Optional[ColumnOrLiteralStr] = None, _emit_ast: bool = True
) -> Column:
    """Converts an input expression into the corresponding time.

    Example::

        >>> df = session.create_dataframe(['04:15:29.999'], schema=['a'])
        >>> df.select(to_time(col("a"))).collect()
        [Row(TO_TIME("A")=datetime.time(4, 15, 29, 999000))]
    """
    c = _to_col_if_str(e, "to_time")
    return (
        _call_function("to_time", c, fmt, _emit_ast=_emit_ast)
        if fmt is not None
        else _call_function("to_time", c, _emit_ast=_emit_ast)
    )


@publicapi
def to_timestamp(
    e: ColumnOrName, fmt: Optional["Column"] = None, _emit_ast: bool = True
) -> Column:
    """Converts an input expression into the corresponding timestamp.

    Per default fmt is set to auto, which makes Snowflake detect the format automatically. With `to_timestamp` strings
    can be converted to timestamps. The format has to be specified according to the rules set forth in
    <https://docs.snowflake.com/en/sql-reference/functions-conversion#date-and-time-formats-in-conversion-functions>

    Example::
        >>> df = session.create_dataframe(['2019-01-31 01:02:03.004'], schema=['a'])
        >>> df.select(to_timestamp(col("a")).as_("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 31, 1, 2, 3, 4000))]
        >>> df = session.create_dataframe(["2020-05-01 13:11:20.000"], schema=['a'])
        >>> df.select(to_timestamp(col("a"), lit("YYYY-MM-DD HH24:MI:SS.FF3")).as_("ans")).collect()
        [Row(ANS=datetime.datetime(2020, 5, 1, 13, 11, 20))]

    Another option is to convert dates into timestamps

    Example::
        >>> import datetime
        >>> df = session.createDataFrame([datetime.datetime(2022, 12, 25, 13, 59, 38, 467)], schema=["a"])
        >>> df.select(to_timestamp(col("a"))).collect()
        [Row(TO_TIMESTAMP("A")=datetime.datetime(2022, 12, 25, 13, 59, 38, 467))]
        >>> df = session.createDataFrame([datetime.date(2023, 3, 1)], schema=["a"])
        >>> df.select(to_timestamp(col("a"))).collect()
        [Row(TO_TIMESTAMP("A")=datetime.datetime(2023, 3, 1, 0, 0))]

    Integers can be converted into a timestamp as well, by providing optionally a scale as an integer as lined out in
    <https://docs.snowflake.com/en/sql-reference/functions/to_timestamp#usage-notes>. Currently Snowpark does support
    integers in the range of an 8-byte signed integer only.

    Example::
        >>> df = session.createDataFrame([20, 31536000000], schema=['a'])
        >>> df.select(to_timestamp(col("a"))).collect()
        [Row(TO_TIMESTAMP("A")=datetime.datetime(1970, 1, 1, 0, 0, 20)), Row(TO_TIMESTAMP("A")=datetime.datetime(2969, 5, 3, 0, 0))]
        >>> df.select(to_timestamp(col("a"), lit(9))).collect()
        [Row(TO_TIMESTAMP("A", 9)=datetime.datetime(1970, 1, 1, 0, 0)), Row(TO_TIMESTAMP("A", 9)=datetime.datetime(1970, 1, 1, 0, 0, 31, 536000))]

    Larger numbers stored in a string can be also converted via this approach

    Example::
        >>> df = session.createDataFrame(['20', '31536000000', '31536000000000', '31536000000000000'], schema=['a'])
        >>> df.select(to_timestamp(col("a")).as_("ans")).collect()
        [Row(ANS=datetime.datetime(1970, 1, 1, 0, 0, 20)), Row(ANS=datetime.datetime(1971, 1, 1, 0, 0)), Row(ANS=datetime.datetime(1971, 1, 1, 0, 0)), Row(ANS=datetime.datetime(1971, 1, 1, 0, 0))]
    """
    c = _to_col_if_str(e, "to_timestamp")
    return (
        _call_function("to_timestamp", c, fmt, _emit_ast=_emit_ast)
        if fmt is not None
        else _call_function("to_timestamp", c, _emit_ast=_emit_ast)
    )


@publicapi
def to_timestamp_ntz(
    e: ColumnOrName, fmt: Optional[ColumnOrLiteralStr] = None, _emit_ast: bool = True
) -> Column:
    """Converts an input expression into the corresponding timestamp without a timezone.

    Per default fmt is set to auto, which makes Snowflake detect the format automatically. With `to_timestamp` strings
    can be converted to timestamps. The format has to be specified according to the rules set forth in
    <https://docs.snowflake.com/en/sql-reference/functions-conversion#date-and-time-formats-in-conversion-functions>

    Example::
        >>> import datetime
        >>> df = session.createDataFrame([datetime.datetime(2022, 12, 25, 13, 59, 38, 467)], schema=["a"])
        >>> df.select(to_timestamp_ntz(col("a"))).collect()
        [Row(TO_TIMESTAMP_NTZ("A")=datetime.datetime(2022, 12, 25, 13, 59, 38, 467))]
        >>> df = session.createDataFrame([datetime.date(2023, 3, 1)], schema=["a"])
        >>> df.select(to_timestamp_ntz(col("a"))).collect()
        [Row(TO_TIMESTAMP_NTZ("A")=datetime.datetime(2023, 3, 1, 0, 0))]
    """

    # AST.
    ast = (
        build_function_expr("to_timestamp_ntz", [e] if fmt is None else [e, fmt])
        if _emit_ast
        else None
    )

    c = _to_col_if_str(e, "to_timestamp_ntz")
    return (
        _call_function(
            "to_timestamp_ntz",
            c,
            _to_col_if_lit(fmt, "to_timestamp_ntz"),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
        if fmt is not None
        else _call_function("to_timestamp_ntz", c, _ast=ast, _emit_ast=_emit_ast)
    )


@publicapi
def to_timestamp_ltz(
    e: ColumnOrName, fmt: Optional[ColumnOrLiteralStr] = None, _emit_ast: bool = True
) -> Column:
    """Converts an input expression into the corresponding timestamp using the local timezone.

    Per default fmt is set to auto, which makes Snowflake detect the format automatically. With `to_timestamp` strings
    can be converted to timestamps. The format has to be specified according to the rules set forth in
    <https://docs.snowflake.com/en/sql-reference/functions-conversion#date-and-time-formats-in-conversion-functions>
    """

    # AST.
    ast = (
        build_function_expr("to_timestamp_ltz", [e] if fmt is None else [e, fmt])
        if _emit_ast
        else None
    )

    c = _to_col_if_str(e, "to_timestamp_ltz")
    return (
        _call_function(
            "to_timestamp_ltz",
            c,
            _to_col_if_lit(fmt, "to_timestamp_ltz"),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
        if fmt is not None
        else _call_function("to_timestamp_ltz", c, _ast=ast, _emit_ast=_emit_ast)
    )


@publicapi
def to_timestamp_tz(
    e: ColumnOrName, fmt: Optional[ColumnOrLiteralStr] = None, _emit_ast: bool = True
) -> Column:
    """Converts an input expression into the corresponding timestamp with the timezone represented in each row.

    Per default fmt is set to auto, which makes Snowflake detect the format automatically. With `to_timestamp` strings
    can be converted to timestamps. The format has to be specified according to the rules set forth in
    <https://docs.snowflake.com/en/sql-reference/functions-conversion#date-and-time-formats-in-conversion-functions>
    """

    # AST.
    ast = (
        build_function_expr("to_timestamp_tz", [e] if fmt is None else [e, fmt])
        if _emit_ast
        else None
    )

    c = _to_col_if_str(e, "to_timestamp_tz")
    return (
        _call_function(
            "to_timestamp_tz",
            c,
            _to_col_if_lit(fmt, "to_timestamp_tz"),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
        if fmt is not None
        else _call_function("to_timestamp_tz", c, _ast=ast, _emit_ast=_emit_ast)
    )


@publicapi
def from_utc_timestamp(
    e: ColumnOrName, tz: ColumnOrLiteral, _emit_ast: bool = True
) -> Column:
    """Interprets an input expression as a UTC timestamp and converts it to the given time zone.

    Note:
        Time zone names are case-sensitive.
        Snowflake does not support the majority of timezone abbreviations (e.g. PDT, EST, etc.). Instead you can
        specify a time zone name or a link name from release 2021a of the IANA Time Zone Database (e.g.
        America/Los_Angeles, Europe/London, UTC, Etc/GMT, etc.).
        See the following for more information:
        <https://data.iana.org/time-zones/tzdb-2021a/zone1970.tab>
        <https://data.iana.org/time-zones/tzdb-2021a/backward>

    Example::
        >>> df = session.create_dataframe(['2019-01-31 01:02:03.004'], schema=['t'])
        >>> df.select(from_utc_timestamp(col("t"), "America/Los_Angeles").alias("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 30, 17, 2, 3, 4000))]

    Example::
        >>> df = session.create_dataframe([('2019-01-31 01:02:03.004', "America/Los_Angeles")], schema=['t', 'tz'])
        >>> df.select(from_utc_timestamp(col("t"), col("tz")).alias("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 30, 17, 2, 3, 4000))]
    """
    # AST.
    ast = build_function_expr("from_utc_timestamp", [e, tz]) if _emit_ast else None

    c = _to_col_if_str(e, "from_utc_timestamp")
    tz_c = _to_col_if_lit(tz, "from_utc_timestamp")
    return _call_function(
        "convert_timezone", "UTC", tz_c, c, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def to_utc_timestamp(
    e: ColumnOrName, tz: ColumnOrLiteral, _emit_ast: bool = True
) -> Column:
    """Interprets an input expression as a timestamp and converts from given time zone to UTC.

    Note:
        Time zone names are case-sensitive.
        Snowflake does not support the majority of timezone abbreviations (e.g. PDT, EST, etc.). Instead you can
        specify a time zone name or a link name from release 2021a of the IANA Time Zone Database (e.g.
        America/Los_Angeles, Europe/London, UTC, Etc/GMT, etc.).
        See the following for more information:
        <https://data.iana.org/time-zones/tzdb-2021a/zone1970.tab>
        <https://data.iana.org/time-zones/tzdb-2021a/backward>

    Example::
        >>> df = session.create_dataframe(['2019-01-31 01:02:03.004'], schema=['t'])
        >>> df.select(to_utc_timestamp(col("t"), "America/Los_Angeles").alias("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 31, 9, 2, 3, 4000))]

    Example::
        >>> df = session.create_dataframe([('2019-01-31 01:02:03.004', "America/Los_Angeles")], schema=['t', 'tz'])
        >>> df.select(to_utc_timestamp(col("t"), col("tz")).alias("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 31, 9, 2, 3, 4000))]
    """
    # AST.
    ast = build_function_expr("to_utc_timestamp", [e, tz]) if _emit_ast else None

    c = _to_col_if_str(e, "to_utc_timestamp")
    tz_c = _to_col_if_lit(tz, "to_utc_timestamp")
    return _call_function(
        "convert_timezone", tz_c, "UTC", c, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def to_date(
    e: ColumnOrName, fmt: Optional[ColumnOrLiteral] = None, _emit_ast: bool = True
) -> Column:
    """Converts an input expression into a date.

    Example::

        >>> df = session.create_dataframe(['2013-05-17', '2013-05-17'], schema=['a'])
        >>> df.select(to_date(col('a')).as_('ans')).collect()
        [Row(ANS=datetime.date(2013, 5, 17)), Row(ANS=datetime.date(2013, 5, 17))]

        >>> df = session.create_dataframe(['2013-05-17', '2013-05-17'], schema=['a'])
        >>> df.select(to_date(col('a'), 'YYYY-MM-DD').as_('ans')).collect()
        [Row(ANS=datetime.date(2013, 5, 17)), Row(ANS=datetime.date(2013, 5, 17))]

        >>> df = session.create_dataframe(['2013-05-17', '2013-05-17'], schema=['a'])
        >>> df.select(to_date(col('a'), 'YYYY-MM-DD').as_('ans')).collect()
        [Row(ANS=datetime.date(2013, 5, 17)), Row(ANS=datetime.date(2013, 5, 17))]

        >>> df = session.create_dataframe(['31536000000000', '71536004000000'], schema=['a'])
        >>> df.select(to_date(col('a')).as_('ans')).collect()
        [Row(ANS=datetime.date(1971, 1, 1)), Row(ANS=datetime.date(1972, 4, 7))]

    """
    # AST.
    ast = (
        build_function_expr("to_date", [e] if fmt is None else [e, fmt])
        if _emit_ast
        else None
    )

    c = _to_col_if_str(e, "to_date")

    return (
        _call_function("to_date", c, _ast=ast, _emit_ast=_emit_ast)
        if fmt is None
        else _call_function(
            "to_date", c, Column._to_expr(fmt), _ast=ast, _emit_ast=_emit_ast
        )
    )


@publicapi
def current_timestamp(_emit_ast: bool = True) -> Column:
    """Returns the current timestamp for the system.

    Example:
        >>> import datetime
        >>> result = session.create_dataframe([1]).select(current_timestamp()).collect()
        >>> assert isinstance(result[0]["CURRENT_TIMESTAMP()"], datetime.datetime)
    """
    return _call_function("current_timestamp", _emit_ast=_emit_ast)


@publicapi
def current_date(_emit_ast: bool = True) -> Column:
    """Returns the current date for the system.

    Example:
        >>> import datetime
        >>> result = session.create_dataframe([1]).select(current_date()).collect()
        >>> assert isinstance(result[0]["CURRENT_DATE()"], datetime.date)
    """
    return _call_function("current_date", _emit_ast=_emit_ast)


@publicapi
def current_time(_emit_ast: bool = True) -> Column:
    """Returns the current time for the system.

    Example:
        >>> import datetime
        >>> result = session.create_dataframe([1]).select(current_time()).collect()
        >>> assert isinstance(result[0]["CURRENT_TIME()"], datetime.time)
    """
    return _call_function("current_time", _emit_ast=_emit_ast)


@publicapi
def hour(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("hour", c, _emit_ast=_emit_ast)


@publicapi
def last_day(
    expr: ColumnOrName, part: Optional[ColumnOrName] = None, _emit_ast: bool = True
) -> Column:
    """
    Returns the last day of the specified date part for a date or timestamp.
    Commonly used to return the last day of the month for a date or timestamp.

    Args:
        expr: The array column
        part: The date part used to compute the last day of the given array column, default is "MONTH".
            Valid values are "YEAR", "MONTH", "QUARTER", "WEEK" or any of their supported variations.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(last_day("a")).collect()
        [Row(LAST_DAY("A")=datetime.date(2020, 5, 31)), Row(LAST_DAY("A")=datetime.date(2020, 8, 31))]
        >>> df.select(last_day("a", "YEAR")).collect()
        [Row(LAST_DAY("A", "YEAR")=datetime.date(2020, 12, 31)), Row(LAST_DAY("A", "YEAR")=datetime.date(2020, 12, 31))]
    """
    ast = (
        build_function_expr("last_day", [expr] if part is None else [expr, part])
        if _emit_ast
        else None
    )

    expr_col = _to_col_if_str(expr, "last_day")
    if part is None:
        # Ensure we do not change the column name
        return _call_function("last_day", expr_col, _ast=ast, _emit_ast=_emit_ast)

    part_col = _to_col_if_str(part, "last_day")
    return _call_function("last_day", expr_col, part_col, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def minute(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("minute", c, _emit_ast=_emit_ast)


@publicapi
def next_day(
    date: ColumnOrName, day_of_week: ColumnOrLiteral, _emit_ast: bool = True
) -> Column:
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
    # Build AST here to prevent `date` from being recorded as a Column instead of a literal and
    # `day_of_week` from being recorded as a literal instead of Column.
    ast = build_function_expr("next_day", [date, day_of_week]) if _emit_ast else None
    c = _to_col_if_str(date, "next_day")
    return _call_function(
        "next_day", c, Column._to_expr(day_of_week), _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def previous_day(
    date: ColumnOrName, day_of_week: ColumnOrLiteral, _emit_ast: bool = True
) -> Column:
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
    # Build AST here to prevent `date` from being recorded as a Column instead of a literal and
    # `day_of_week` from being recorded as a literal instead of Column.
    ast = (
        build_function_expr("previous_day", [date, day_of_week]) if _emit_ast else None
    )
    c = _to_col_if_str(date, "previous_day")
    return _call_function(
        "previous_day", c, Column._to_expr(day_of_week), _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def second(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("second", c, _emit_ast=_emit_ast)


@publicapi
def month(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("month", c, _emit_ast=_emit_ast)


@publicapi
def monthname(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("monthname", c, _emit_ast=_emit_ast)


@publicapi
def quarter(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("quarter", c, _emit_ast=_emit_ast)


@publicapi
def year(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("year", c, _emit_ast=_emit_ast)


@publicapi
def sysdate(_emit_ast: bool = True) -> Column:
    """
    Returns the current timestamp for the system, but in the UTC time zone.

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(sysdate()).collect() is not None
        True
    """
    return _call_function("sysdate", _emit_ast=_emit_ast)


@publicapi
def months_between(
    date1: ColumnOrName, date2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns the number of months between two DATE or TIMESTAMP values.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([[
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ]], schema=["a", "b"])
        >>> df.select(months_between("a", "b")).collect()
        [Row(MONTHS_BETWEEN("A", "B")=Decimal('-3.629452'))]
    """
    c1 = _to_col_if_str(date1, "months_between")
    c2 = _to_col_if_str(date2, "months_between")
    return _call_function("months_between", c1, c2, _emit_ast=_emit_ast)


@publicapi
def to_geography(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Parses an input and returns a value of type GEOGRAPHY. Supported inputs are strings in

        - WKT (well-known text).
        - WKB (well-known binary) in hexadecimal format (without a leading 0x).
        - EWKT (extended well-known text).
        - EWKB (extended well-known binary) in hexadecimal format (without a leading 0x).
        - GeoJSON.

    format.

    Example::
        >>> df = session.create_dataframe(['POINT(-122.35 37.55)', 'POINT(20.92 43.33)'], schema=['a'])
        >>> df.select(to_geography(col("a"))).collect()
        [Row(TO_GEOGRAPHY("A")='{\\n  "coordinates": [\\n    -122.35,\\n    37.55\\n  ],\\n  "type": "Point"\\n}'), Row(TO_GEOGRAPHY("A")='{\\n  "coordinates": [\\n    20.92,\\n    43.33\\n  ],\\n  "type": "Point"\\n}')]

    Besides strings, binary representation in WKB and EWKB format can be parsed, or objects adhering to GeoJSON format.
    For all supported formats confer https://docs.snowflake.com/en/sql-reference/data-types-geospatial#supported-geospatial-object-types.
    """
    c = _to_col_if_str(e, "to_geography")
    return _call_function("to_geography", c, _emit_ast=_emit_ast)


@publicapi
def to_geometry(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Parses an input and returns a value of type GEOMETRY. Supported inputs are strings in

        - WKT (well-known text).
        - WKB (well-known binary) in hexadecimal format (without a leading 0x).
        - EWKT (extended well-known text).
        - EWKB (extended well-known binary) in hexadecimal format (without a leading 0x).
        - GeoJSON.

    format.

    Example::
        >>> df = session.create_dataframe(['POINT(-122.35 37.55)', 'POINT(20.92 43.33)'], schema=['a'])
        >>> df.select(to_geometry(col("a"))).collect(statement_params={"GEOMETRY_OUTPUT_FORMAT": "WKT"})
        [Row(TO_GEOMETRY("A")='POINT(-122.35 37.55)'), Row(TO_GEOMETRY("A")='POINT(20.92 43.33)')]

    Besides strings, binary representation in WKB and EWKB format can be parsed, or objects adhering to GeoJSON format.
    For all supported formats confer https://docs.snowflake.com/en/sql-reference/data-types-geospatial#supported-geospatial-object-types.
    """
    c = _to_col_if_str(e, "to_geometry")
    return _call_function("to_geometry", c, _emit_ast=_emit_ast)


@publicapi
def arrays_overlap(
    array1: ColumnOrName, array2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Compares whether two ARRAYs have at least one element in common. Returns TRUE
    if there is at least one element in common; otherwise returns FALSE. The function
    is NULL-safe, meaning it treats NULLs as known values for comparing equality.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row([1, 2], [1, 3]), Row([1, 2], [3, 4])], schema=["a", "b"])
        >>> df.select(arrays_overlap("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |True      |
        |False     |
        ------------
        <BLANKLINE>
    """
    a1 = _to_col_if_str(array1, "arrays_overlap")
    a2 = _to_col_if_str(array2, "arrays_overlap")
    return _call_function("arrays_overlap", a1, a2, _emit_ast=_emit_ast)


@publicapi
def array_distinct(col: ColumnOrName, _emit_ast: bool = True):
    """The function excludes any duplicate elements that are present in the input ARRAY.
    The function is not guaranteed to return the elements in the ARRAY in a specific order.
    The function is NULL safe, which means that it treats NULLs as known values when identifying duplicate elements.

    Args:
        col: The array column

    Returns:
        Returns a new ARRAY that contains only the distinct elements from the input ARRAY.

    Example::

        >>> from snowflake.snowpark.functions import array_construct,array_distinct,lit
        >>> df = session.createDataFrame([["1"]], ["A"])
        >>> df = df.withColumn("array", array_construct(lit(1), lit(1), lit(1), lit(2), lit(3), lit(2), lit(2)))
        >>> df.withColumn("array_d", array_distinct("ARRAY")).show()
        -----------------------------
        |"A"  |"ARRAY"  |"ARRAY_D"  |
        -----------------------------
        |1    |[        |[          |
        |     |  1,     |  1,       |
        |     |  1,     |  2,       |
        |     |  1,     |  3        |
        |     |  2,     |]          |
        |     |  3,     |           |
        |     |  2,     |           |
        |     |  2      |           |
        |     |]        |           |
        -----------------------------
        <BLANKLINE>
    """
    col = _to_col_if_str(col, "array_distinct")
    return _call_function("array_distinct", col, _emit_ast=_emit_ast)


@publicapi
def array_intersection(
    array1: ColumnOrName, array2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns an array that contains the matching elements in the two input arrays.

    The function is NULL-safe, meaning it treats NULLs as known values for comparing equality.

    Args:
        array1: An ARRAY that contains elements to be compared.
        array2: An ARRAY that contains elements to be compared.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row([1, 2], [1, 3])], schema=["a", "b"])
        >>> df.select(array_intersection("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a1 = _to_col_if_str(array1, "array_intersection")
    a2 = _to_col_if_str(array2, "array_intersection")
    return _call_function("array_intersection", a1, a2, _emit_ast=_emit_ast)


@publicapi
def array_except(
    source_array: ColumnOrName,
    array_of_elements_to_exclude: ColumnOrName,
    allow_duplicates: bool = True,
    _emit_ast: bool = True,
) -> Column:
    """Returns a new ARRAY that contains the elements from one input ARRAY that are not in another input ARRAY.

    The function is NULL-safe, meaning it treats NULLs as known values for comparing equality.

    When allow_duplicates is set to True (default), this function is the same as the Snowflake ARRAY_EXCEPT semantic:

    This function compares arrays by using multi-set semantics (sometimes called "bag semantics"). If source_array
    includes multiple copies of a value, the function only removes the number of copies of that value that are specified
    in array_of_elements_to_exclude.

    For example, if source_array contains 5 elements with the value 'A' and array_of_elements_to_exclude contains 2
    elements with the value 'A', the returned array contains 3 elements with the value 'A'.

    When allow_duplicates is set to False:

    This function compares arrays by using set semantics. Specifically, it will first do an element deduplication
    for both arrays, and then compute the array_except result.

    For example, if source_array contains 5 elements with the value 'A' and array_of_elements_to_exclude contains 2
    elements with the value 'A', the returned array is empty.

    Args:
        source_array: An array that contains elements to be included in the new ARRAY.
        array_of_elements_to_exclude: An array that contains elements to be excluded from the new ARRAY.
        allow_duplicates: If True, we use multi-set semantic. Otherwise use set semantic.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(["A", "B"], ["B", "C"])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A"     |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", "B", "B", "B", "C"], ["B"])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A",    |
        |  "B",    |
        |  "B",    |
        |  "C"     |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", None, None], ["B", None])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A",    |
        |  null    |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row([{'a': 1, 'b': 2}, 1], [{'a': 1, 'b': 2}, 3])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1       |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", "B"], None)], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |NULL      |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", "B"], ["B", "C"])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude", False).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A"     |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", "B", "B", "B", "C"], ["B"])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude", False).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A",    |
        |  "C"     |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", None, None], ["B", None])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude", False).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A"     |
        |]         |
        ------------
        <BLANKLINE>
    """
    # AST.
    ast = (
        build_function_expr(
            "array_except",
            [source_array, array_of_elements_to_exclude, allow_duplicates],
        )
        if _emit_ast
        else None
    )

    array1 = _to_col_if_str(source_array, "array_except")
    array2 = _to_col_if_str(array_of_elements_to_exclude, "array_except")
    return (
        _call_function("array_except", array1, array2, _ast=ast, _emit_ast=_emit_ast)
        if allow_duplicates
        else _call_function(
            "array_except",
            _call_function("array_distinct", array1, _emit_ast=False),
            _call_function("array_distinct", array2, _emit_ast=False),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
    )


@publicapi
def array_min(array: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns smallest defined non-NULL element in the input array. If the input
    array is empty, or there is no defined element in the input array, then the
    function returns NULL.

    Args:
        array: the input array

    Returns:
        a VARIANT containing the smallest defined element in the array, or NULL

    Examples::
            Behavior with SQL nulls:
                >>> df = session.sql("select array_construct(20, 0, null, 10) as A")
                >>> df.select(array_min(df.a).as_("min_a")).collect()
                [Row(MIN_A='0')]
                >>> df = session.sql("select array_construct() as A")
                >>> df.select(array_min(df.a).as_("min_a")).collect()
                [Row(MIN_A=None)]
                >>> df = session.sql("select array_construct(null, null, null) as A")
                >>> df.select(array_min(df.a).as_("min_a")).collect()
                [Row(MIN_A=None)]

            Behavior with JSON nulls:
                >>> df = session.create_dataframe([[[None, None, None]]], schema=["A"])
                >>> df.select(array_min(df.a).as_("min_a")).collect()
                [Row(MIN_A='null')]
    """
    array = _to_col_if_str(array, "array_min")
    return _call_function("array_min", array, _emit_ast=_emit_ast)


@publicapi
def array_max(array: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns largest defined non-NULL element in the input array. If the input
    array is empty, or there is no defined element in the input array, then the
    function returns NULL.

    Args:
        array: the input array

    Returns:
        a VARIANT containing the largest defined element in the array, or NULL

    Examples::
        Behavior with SQL nulls:
            >>> df = session.sql("select array_construct(20, 0, null, 10) as A")
            >>> df.select(array_max(df.a).as_("max_a")).collect()
            [Row(MAX_A='20')]
            >>> df = session.sql("select array_construct() as A")
            >>> df.select(array_max(df.a).as_("max_a")).collect()
            [Row(MAX_A=None)]
            >>> df = session.sql("select array_construct(null, null, null) as A")
            >>> df.select(array_max(df.a).as_("max_a")).collect()
            [Row(MAX_A=None)]

        Behavior with JSON nulls:
            >>> df = session.create_dataframe([[[None, None, None]]], schema=["A"])
            >>> df.select(array_max(df.a).as_("max_a")).collect()
            [Row(MAX_A='null')]
    """
    array = _to_col_if_str(array, "array_max")
    return _call_function("array_max", array, _emit_ast=_emit_ast)


@publicapi
def array_flatten(array: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a single array from an array or arrays. If the array is nested more than
    two levels deep, then only a single level of nesting is removed.

    Must enable parameter `ENABLE_ARRAY_FLATTEN_FUNCTION` in your session.

    Args:
        array: the input array
    """
    array = _to_col_if_str(array, "array_flatten")
    return _call_function("array_flatten", array, _emit_ast=_emit_ast)


@publicapi
def array_reverse(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns an array with the elements of the input array in reverse order.

    Args:
        col: The source array.

    Example::
        >>> df = session.sql("select [1, 2, 3, 4] :: ARRAY(INT) as A")
        >>> df.select(array_reverse("A")).show()
        --------------------------
        |"ARRAY_REVERSE(""A"")"  |
        --------------------------
        |[                       |
        |  4,                    |
        |  3,                    |
        |  2,                    |
        |  1                     |
        |]                       |
        --------------------------
        <BLANKLINE>
    """
    array = _to_col_if_str(col, "array_reverse")
    return _call_function("array_reverse", array, _emit_ast=_emit_ast)


@publicapi
def array_sort(
    array: ColumnOrName,
    sort_ascending: Optional[bool] = True,
    nulls_first: Optional[bool] = False,
    _emit_ast: bool = True,
) -> Column:
    """Returns rows of array column in sorted order. Users can choose the sort order and decide where to keep null elements.

    Args:
        array: name of the column or column element which describes the column
        sort_ascending: Boolean that decides if array elements are sorted in ascending order.
            Defaults to True.
        nulls_first: Boolean that decides if SQL null elements will be placed in the beginning
            of the array. Note that this does not affect JSON null. Defaults to False.

    Examples::
        Behavior with SQL nulls:
            >>> df = session.sql("select array_construct(20, 0, null, 10) as A")
            >>> df.select(array_sort(df.a).as_("sorted_a")).show()
            ---------------
            |"SORTED_A"   |
            ---------------
            |[            |
            |  0,         |
            |  10,        |
            |  20,        |
            |  undefined  |
            |]            |
            ---------------
            <BLANKLINE>
            >>> df.select(array_sort(df.a, False).as_("sorted_a")).show()
            ---------------
            |"SORTED_A"   |
            ---------------
            |[            |
            |  20,        |
            |  10,        |
            |  0,         |
            |  undefined  |
            |]            |
            ---------------
            <BLANKLINE>
            >>> df.select(array_sort(df.a, False, True).as_("sorted_a")).show()
            ----------------
            |"SORTED_A"    |
            ----------------
            |[             |
            |  undefined,  |
            |  20,         |
            |  10,         |
            |  0           |
            |]             |
            ----------------
            <BLANKLINE>

        Behavior with JSON nulls:
            >>> df = session.create_dataframe([[[20, 0, None, 10]]], schema=["a"])
            >>> df.select(array_sort(df.a, False, False).as_("sorted_a")).show()
            --------------
            |"SORTED_A"  |
            --------------
            |[           |
            |  null,     |
            |  20,       |
            |  10,       |
            |  0         |
            |]           |
            --------------
            <BLANKLINE>
            >>> df.select(array_sort(df.a, False, True).as_("sorted_a")).show()
            --------------
            |"SORTED_A"  |
            --------------
            |[           |
            |  null,     |
            |  20,       |
            |  10,       |
            |  0         |
            |]           |
            --------------
            <BLANKLINE>

    See Also:
        - https://docs.snowflake.com/en/user-guide/semistructured-considerations#null-values
        - :func:`~snowflake.snowpark.functions.sort_array` which is an alias of :meth:`~snowflake.snowpark.functions.array_sort`.
    """
    # Build AST here to prevent `sort_ascending` and `nulls_first` from being recorded as a literal instead of bool.
    ast = (
        build_function_expr("array_sort", [array, sort_ascending, nulls_first])
        if _emit_ast
        else None
    )
    array = _to_col_if_str(array, "array_sort")
    return _call_function(
        "array_sort",
        array,
        lit(sort_ascending, _emit_ast=False),
        lit(nulls_first, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def arrays_to_object(
    keys: ColumnOrName, values: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns an object constructed from 2 arrays.

    Args:
        keys: The column containing keys of the object.
        values: The column containing values of the object.
    Examples::
        >>> df = session.sql("select array_construct('10', '20', '30') as A, array_construct(10, 20, 30) as B")
        >>> df.select(arrays_to_object(df.a, df.b).as_("object")).show()
        ---------------
        |"OBJECT"     |
        ---------------
        |{            |
        |  "10": 10,  |
        |  "20": 20,  |
        |  "30": 30   |
        |}            |
        ---------------
        <BLANKLINE>
        >>> df = session.create_dataframe([[["a"], [1]], [["b", "c"],[2, 3]]], schema=["k", "v"])
        >>> df.select(arrays_to_object(df.k, df.v).as_("objects")).show()
        -------------
        |"OBJECTS"  |
        -------------
        |{          |
        |  "a": 1   |
        |}          |
        |{          |
        |  "b": 2,  |
        |  "c": 3   |
        |}          |
        -------------
        <BLANKLINE>

    See Also:
        - https://docs.snowflake.com/en/sql-reference/data-types-semistructured#label-data-type-object for information on Objects
    """
    keys_c = _to_col_if_str(keys, "arrays_to_object")
    values_c = _to_col_if_str(values, "arrays_to_object")
    return _call_function("arrays_to_object", keys_c, values_c, _emit_ast=_emit_ast)


@publicapi
def arrays_zip(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns an array of structured objects, where the N-th object contains the N-th elements of the input arrays.

    Args:
        cols: The columns to zip together.

    Returns:
        A new array of structured objects.

    Examples::
        >>> df = session.sql("select array_construct('10', '20', '30') as A, array_construct(10, 20, 30) as B")
        >>> df.select(arrays_zip(df.a, df.b).as_("zipped")).show(statement_params={"enable_arrays_zip_function": "TRUE"})
        -------------------
        |"ZIPPED"         |
        -------------------
        |[                |
        |  {              |
        |    "$1": "10",  |
        |    "$2": 10     |
        |  },             |
        |  {              |
        |    "$1": "20",  |
        |    "$2": 20     |
        |  },             |
        |  {              |
        |    "$1": "30",  |
        |    "$2": 30     |
        |  }              |
        |]                |
        -------------------
        <BLANKLINE>
        >>> df = session.sql("select array_construct('10', '20', '30') as A, array_construct(1, 2) as B, array_construct(1.1) as C")
        >>> df.select(arrays_zip(df.a, df.b, df.c).as_("zipped")).show(statement_params={"enable_arrays_zip_function": "TRUE"})
        -------------------
        |"ZIPPED"         |
        -------------------
        |[                |
        |  {              |
        |    "$1": "10",  |
        |    "$2": 1,     |
        |    "$3": 1.1    |
        |  },             |
        |  {              |
        |    "$1": "20",  |
        |    "$2": 2,     |
        |    "$3": null   |
        |  },             |
        |  {              |
        |    "$1": "30",  |
        |    "$2": null,  |
        |    "$3": null   |
        |  }              |
        |]                |
        -------------------
        <BLANKLINE>
    """
    cols = [_to_col_if_str(c, "arrays_zip") for c in cols]
    return _call_function("arrays_zip", *cols, _emit_ast=_emit_ast)


@publicapi
def array_generate_range(
    start: ColumnOrName,
    stop: ColumnOrName,
    step: Optional[ColumnOrName] = None,
    _emit_ast: bool = True,
) -> Column:
    """Generate a range of integers from `start` to `stop`, incrementing by `step`.
    If `step` is not set, incrementing by 1.

    Args:
        start: the column that contains the integer to start with (inclusive).
        stop: the column that contains the integer to stop (exclusive).
        step: the column that contains the integer to increment.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df1 = session.create_dataframe([(-2, 2)], ["a", "b"])
        >>> df1.select(array_generate_range("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  -2,     |
        |  -1,     |
        |  0,      |
        |  1       |
        |]         |
        ------------
        <BLANKLINE>
        >>> df2 = session.create_dataframe([(4, -4, -2)], ["a", "b", "c"])
        >>> df2.select(array_generate_range("a", "b", "c").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  4,      |
        |  2,      |
        |  0,      |
        |  -2      |
        |]         |
        ------------
        <BLANKLINE>
    """
    start_col = _to_col_if_str(start, "array_generate_range")
    stop_col = _to_col_if_str(stop, "array_generate_range")
    if step is None:
        return _call_function(
            "array_generate_range", start_col, stop_col, _emit_ast=_emit_ast
        )
    step_col = _to_col_if_str(step, "array_generate_range")
    return _call_function(
        "array_generate_range", start_col, stop_col, step_col, _emit_ast=_emit_ast
    )


@publicapi
def sequence(
    start: ColumnOrName,
    stop: ColumnOrName,
    step: Optional[ColumnOrName] = None,
    _emit_ast: bool = True,
) -> Column:
    """Generate a sequence of integers from `start` to `stop`, incrementing by `step`.
    If `step` is not set, incrementing by 1 if start is less than or equal to stop, otherwise -1.

    Args:
        start: the column that contains the integer to start with (inclusive).
        stop: the column that contains the integer to stop (inclusive).
        step: the column that contains the integer to increment.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df1 = session.create_dataframe([(-2, 2)], ["a", "b"])
        >>> df1.select(sequence("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  -2,     |
        |  -1,     |
        |  0,      |
        |  1,      |
        |  2       |
        |]         |
        ------------
        <BLANKLINE>
        >>> df2 = session.create_dataframe([(4, -4, -2)], ["a", "b", "c"])
        >>> df2.select(sequence("a", "b", "c").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  4,      |
        |  2,      |
        |  0,      |
        |  -2,     |
        |  -4      |
        |]         |
        ------------
        <BLANKLINE>
    """

    # AST.
    ast = (
        build_function_expr(
            "sequence", [start, stop] if step is None else [start, stop, step]
        )
        if _emit_ast
        else None
    )

    start_col = _to_col_if_str(start, "sequence")
    stop_col = _to_col_if_str(stop, "sequence")
    if step is None:
        step = iff(
            _call_function("sign", stop_col - start_col, _emit_ast=False) > 0,
            1,
            -1,
            _emit_ast=False,
        )
        return _call_function(
            "array_generate_range",
            start_col,
            stop_col + step,
            step,
            _ast=ast,
            _emit_ast=_emit_ast,
        )

    step_col = _to_col_if_str(step, "sequence")
    step_sign = iff(
        _call_function("sign", step_col, _emit_ast=False) > 0, 1, -1, _emit_ast=False
    )
    return _call_function(
        "array_generate_range",
        start_col,
        stop_col + step_sign,
        step_col,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def date_add(
    col: ColumnOrName, num_of_days: Union[ColumnOrName, int], _emit_ast: bool = True
):
    """
    Adds a number of days to a date column.

    Args:
        col: The column to add to.
        num_of_days: The number of days to add.

    Example::

        >>> from snowflake.snowpark.functions import date_add, to_date
        >>> df = session.createDataFrame([("1976-01-06")], ["date"])
        >>> df = df.withColumn("date", to_date("date"))
        >>> res = df.withColumn("date", date_add("date", 4)).show()
        --------------
        |"DATE"      |
        --------------
        |1976-01-10  |
        --------------
        <BLANKLINE>
    """
    # AST.
    ast = build_function_expr("date_add", [col, num_of_days]) if _emit_ast else None

    # Convert the input to a column if it is a string
    col = _to_col_if_str(col, "date_add")
    num_of_days = (
        lit(num_of_days, _emit_ast=False)
        if isinstance(num_of_days, int)
        else _to_col_if_str(num_of_days, "date_add")
    )
    # Return the dateadd function with the column and number of days
    ans = dateadd("day", num_of_days, col, _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def date_sub(
    col: ColumnOrName, num_of_days: Union[ColumnOrName, int], _emit_ast: bool = True
):
    """
    Subtracts a number of days from a date column.

    Args:
        col: The column to subtract from.
        num_of_days: The number of days to subtract.

    Example::

        >>> from snowflake.snowpark.functions import date_sub, to_date
        >>> df = session.createDataFrame([("1976-01-06")], ["date"])
        >>> df = df.withColumn("date", to_date("date"))
        >>> df.withColumn("date", date_sub("date", 2)).show()
        --------------
        |"DATE"      |
        --------------
        |1976-01-04  |
        --------------
        <BLANKLINE>
    """
    # AST.
    ast = build_function_expr("date_sub", [col, num_of_days]) if _emit_ast else None

    # Convert the input parameters to the appropriate type
    col = _to_col_if_str(col, "date_sub")
    num_of_days = (
        lit(num_of_days, _emit_ast=False)
        if isinstance(num_of_days, int)
        else _to_col_if_str(num_of_days, "date_sub")
    )
    # Return the date column with the number of days subtracted
    ans = dateadd("day", -1 * num_of_days, col, _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def datediff(
    part: str, col1: ColumnOrName, col2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Calculates the difference between two date, time, or timestamp columns based on the date or time part requested, and
    returns result of ``col2 - col1`` based on the requested date or time part.

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
        col1: The first timestamp column or subtrahend in the datediff
        col2: The second timestamp column or the minuend in the datediff

    See Also:
        - `Snowflake Datediff <https://docs.snowflake.com/en/sql-reference/functions/datediff>`_
    """
    if not isinstance(part, str):
        raise ValueError("part must be a string")
    c1 = _to_col_if_str(col1, "datediff")
    c2 = _to_col_if_str(col2, "datediff")
    return _call_function("datediff", part, c1, c2, _emit_ast=_emit_ast)


@publicapi
def daydiff(col1: ColumnOrName, col2: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Calculates the difference between two dates, or timestamp columns based in days.
    The result will reflect the difference between ``col1 - col2``

    Example::
        >>> from snowflake.snowpark.functions import daydiff, to_date
        >>> df = session.createDataFrame([("2015-04-08", "2015-05-10")], ["d1", "d2"])
        >>> res = df.select(daydiff(to_date(df.d2), to_date(df.d1)).alias("diff")).show()
        ----------
        |"DIFF"  |
        ----------
        |32      |
        ----------
        <BLANKLINE>
    """
    # AST.
    ast = build_function_expr("daydiff", [col1, col2]) if _emit_ast else None

    col1 = _to_col_if_str(col1, "daydiff")
    col2 = _to_col_if_str(col2, "daydiff")
    return _call_function(
        "datediff",
        lit("day", _emit_ast=False),
        col2,
        col1,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def trunc(
    e: ColumnOrName, scale: Union[ColumnOrName, int, float] = 0, _emit_ast: bool = True
) -> Column:
    """Rounds the input expression down to the nearest (or equal) integer closer to zero,
    or to the nearest equal or smaller value with the specified number of
    places after the decimal point.

    Example::

        >>> df = session.createDataFrame([-1.0, -0.9, -.5, -.2, 0.0, 0.2, 0.5, 0.9, 1.1, 3.14159], schema=["a"])
        >>> df.select(trunc(col("a"))).collect()
        [Row(TRUNC("A", 0)=-1.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=1.0), Row(TRUNC("A", 0)=3.0)]

        >>> df = session.createDataFrame([-1.323, 4.567, 0.0123], schema=["a"])
        >>> df.select(trunc(col("a"), lit(0))).collect()
        [Row(TRUNC("A", 0)=-1.0), Row(TRUNC("A", 0)=4.0), Row(TRUNC("A", 0)=0.0)]
        >>> df.select(trunc(col("a"), lit(1))).collect()
        [Row(TRUNC("A", 1)=-1.3), Row(TRUNC("A", 1)=4.5), Row(TRUNC("A", 1)=0.0)]
        >>> df.select(trunc(col("a"), lit(2))).collect()
        [Row(TRUNC("A", 2)=-1.32), Row(TRUNC("A", 2)=4.56), Row(TRUNC("A", 2)=0.01)]

    Note that the function ``trunc`` is overloaded with ``date_trunc`` for datetime types. It allows to round datetime
    objects.

    Example::

        >>> import datetime
        >>> df = session.createDataFrame([datetime.date(2022, 12, 25), datetime.date(2022, 1, 10), datetime.date(2022, 7, 7)], schema=["a"])
        >>> df.select(trunc(col("a"), lit("QUARTER"))).collect()
        [Row(TRUNC("A", 'QUARTER')=datetime.date(2022, 10, 1)), Row(TRUNC("A", 'QUARTER')=datetime.date(2022, 1, 1)), Row(TRUNC("A", 'QUARTER')=datetime.date(2022, 7, 1))]

        >>> df = session.createDataFrame([datetime.datetime(2022, 12, 25, 13, 59, 38, 467)], schema=["a"])
        >>> df.collect()
        [Row(A=datetime.datetime(2022, 12, 25, 13, 59, 38, 467))]
        >>> df.select(trunc(col("a"), lit("MINUTE"))).collect()
        [Row(TRUNC("A", 'MINUTE')=datetime.datetime(2022, 12, 25, 13, 59))]

    """
    # AST.
    ast = build_function_expr("trunc", [e, scale]) if _emit_ast else None

    c = _to_col_if_str(e, "trunc")
    scale_col = (
        lit(scale, _emit_ast=False)
        if isinstance(scale, (int, float))
        else _to_col_if_str(scale, "trunc")
    )
    return _call_function("trunc", c, scale_col, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def dateadd(
    part: str, col1: ColumnOrName, col2: ColumnOrName, _emit_ast: bool = True
) -> Column:
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
        >>> date_df.select(dateadd("month", lit(1), col("date_col")).alias("month_added")).show()
        -----------------
        |"MONTH_ADDED"  |
        -----------------
        |2020-02-01     |
        -----------------
        <BLANKLINE>
        >>> date_df.select(dateadd("day", lit(1), col("date_col")).alias("day_added")).show()
        ---------------
        |"DAY_ADDED"  |
        ---------------
        |2020-01-02   |
        ---------------
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
    return _call_function("dateadd", part, c1, c2, _emit_ast=_emit_ast)


@publicapi
def date_part(part: str, e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Extracts the specified date or time part from a date, time, or timestamp. See
    `DATE_PART <https://docs.snowflake.com/en/sql-reference/functions/date_part.html>`_ for details.

    Args:
        part: The time part to use for the addition.
        e: The column expression of a date, time, or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([[datetime.datetime(2023, 1, 1, 1, 1, 1)]], schema=["ts_col"])
        >>> df.select(date_part("year", col("ts_col")).alias("year"), date_part("epoch_second", col("ts_col")).alias("epoch_second")).show()
        ---------------------------
        |"YEAR"  |"EPOCH_SECOND"  |
        ---------------------------
        |2023    |1672534861      |
        ---------------------------
        <BLANKLINE>
    """
    if not isinstance(part, str):
        raise ValueError("part must be a string")
    c = _to_col_if_str(e, "date_part")
    return _call_function("date_part", part, c, _emit_ast=_emit_ast)


@publicapi
def date_from_parts(
    y: Union[ColumnOrName, int],
    m: Union[ColumnOrName, int],
    d: Union[ColumnOrName, int],
    _emit_ast: bool = True,
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
    # AST.
    ast = build_function_expr("date_from_parts", [y, m, d]) if _emit_ast else None

    y_col = _to_col_if_str_or_int(y, "date_from_parts")
    m_col = _to_col_if_str_or_int(m, "date_from_parts")
    d_col = _to_col_if_str_or_int(d, "date_from_parts")
    return _call_function(
        "date_from_parts", y_col, m_col, d_col, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def date_trunc(part: str, expr: ColumnOrName, _emit_ast: bool = True) -> Column:
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
        [Row(DATE_TRUNC('YEAR', "A")=datetime.datetime(2020, 1, 1, 0, 0), DATE_TRUNC('MONTH', "A")=datetime.datetime(2020, 5, 1, 0, 0), DATE_TRUNC('DAY', "A")=datetime.datetime(2020, 5, 1, 0, 0))]
        >>> df.select(date_trunc("HOUR", "a"), date_trunc("MINUTE", "a"), date_trunc("SECOND", "a")).collect()
        [Row(DATE_TRUNC('HOUR', "A")=datetime.datetime(2020, 5, 1, 13, 0), DATE_TRUNC('MINUTE', "A")=datetime.datetime(2020, 5, 1, 13, 11), DATE_TRUNC('SECOND', "A")=datetime.datetime(2020, 5, 1, 13, 11, 20))]
        >>> df.select(date_trunc("QUARTER", "a")).collect()
        [Row(DATE_TRUNC('QUARTER', "A")=datetime.datetime(2020, 4, 1, 0, 0))]
    """
    if not isinstance(part, str):
        raise ValueError("part must be a string")
    expr_col = _to_col_if_str(expr, "date_trunc")
    return _call_function("date_trunc", part, expr_col, _emit_ast=_emit_ast)


@publicapi
def dayname(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("dayname", c, _emit_ast=_emit_ast)


@publicapi
def dayofmonth(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("dayofmonth", c, _emit_ast=_emit_ast)


@publicapi
def dayofweek(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("dayofweek", c, _emit_ast=_emit_ast)


@publicapi
def dayofyear(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("dayofyear", c, _emit_ast=_emit_ast)


@publicapi
def window(
    time_column: ColumnOrName,
    window_duration: str,
    slide_duration: Optional[str] = None,
    start_time: Optional[str] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Converts a time column into a window object with start and end times. Window start times are
    inclusive while end times are exclusive. For example 9:30 is in the window [9:30, 10:00), but not [9:00, 9:30).

    Args:
        time_column: The column to apply the window transformation to.
        window_duration: An interval string that determines the length of each window.
        slide_duration: An interval string representing the amount of time in-between the start of
            each window. Note that this parameter is not supported yet. Specifying it will raise a
            NotImplementedError exception.
        start_time: An interval string representing the amount of time the start of each window is
            offset. eg. a five minute window with start_time of '2 minutes' will be from [9:02, 9:07)
            instead of [9:00, 9:05)

    Note:
        Interval strings are of the form 'quantity unit' where quantity is an integer and unitis
        is a supported time unit. This function supports the same time units as dateadd. see
        `supported time units <https://docs.snowflake.com/en/sql-reference/functions-date-time#label-supported-date-time-parts>`_
        for more information.

    Example::

        >>> import datetime
        >>> from snowflake.snowpark.functions import window
        >>> df = session.createDataFrame(
        ...      [(datetime.datetime.strptime("2024-10-31 09:05:00.000", "%Y-%m-%d %H:%M:%S.%f"),)],
        ...      schema=["time"]
        ... )
        >>> df.select(window(df.time, "5 minutes")).show()
        ----------------------------------------
        |"WINDOW"                              |
        ----------------------------------------
        |{                                     |
        |  "end": "2024-10-31 09:10:00.000",   |
        |  "start": "2024-10-31 09:05:00.000"  |
        |}                                     |
        ----------------------------------------
        <BLANKLINE>

        >>> df.select(window(df.time, "5 minutes", start_time="2 minutes")).show()
        ----------------------------------------
        |"WINDOW"                              |
        ----------------------------------------
        |{                                     |
        |  "end": "2024-10-31 09:07:00.000",   |
        |  "start": "2024-10-31 09:02:00.000"  |
        |}                                     |
        ----------------------------------------
        <BLANKLINE>

    Example::

        >>> import datetime
        >>> from snowflake.snowpark.functions import sum, window
        >>> df = session.createDataFrame([
        ...         (datetime.datetime(2024, 10, 31, 1, 0, 0), 1),
        ...         (datetime.datetime(2024, 10, 31, 2, 0, 0), 1),
        ...         (datetime.datetime(2024, 10, 31, 3, 0, 0), 1),
        ...         (datetime.datetime(2024, 10, 31, 4, 0, 0), 1),
        ...         (datetime.datetime(2024, 10, 31, 5, 0, 0), 1),
        ...     ], schema=["time", "value"]
        ... )
        >>> df.group_by(window(df.time, "2 hours")).agg(sum(df.value)).sort("window").show()
        -------------------------------------------------------
        |"WINDOW"                              |"SUM(VALUE)"  |
        -------------------------------------------------------
        |{                                     |1             |
        |  "end": "2024-10-31 02:00:00.000",   |              |
        |  "start": "2024-10-31 00:00:00.000"  |              |
        |}                                     |              |
        |{                                     |2             |
        |  "end": "2024-10-31 04:00:00.000",   |              |
        |  "start": "2024-10-31 02:00:00.000"  |              |
        |}                                     |              |
        |{                                     |2             |
        |  "end": "2024-10-31 06:00:00.000",   |              |
        |  "start": "2024-10-31 04:00:00.000"  |              |
        |}                                     |              |
        -------------------------------------------------------
        <BLANKLINE>
    """
    if slide_duration:
        # SNOW-1063685: slide_duration changes this function from a 1:1 mapping to a 1:N mapping. That
        # currently would require a udtf which may have significantly different performance.
        raise NotImplementedError(
            "snowflake.snowpark.functions.window does not support slide_duration parameter yet."
        )

    ast = (
        build_function_expr(
            "window",
            [time_column, window_duration]
            + ([] if slide_duration is None else [slide_duration])
            + ([] if start_time is None else [start_time]),
        )
        if _emit_ast
        else None
    )

    epoch = lit("1970-01-01 00:00:00", _emit_ast=False).cast(
        TimestampType(timezone=TimestampTimeZone.NTZ), _emit_ast=False
    )
    time = _to_col_if_str(time_column, "window")

    window_duration, window_unit = parse_duration_string(window_duration)
    window_duration = lit(window_duration, _emit_ast=False)
    window_unit = f"{window_unit}s"

    base = epoch
    if start_time:
        start_duration, start_unit = parse_duration_string(start_time)
        base += make_interval(**{f"{start_unit}s": start_duration}, _emit_ast=False)

    window = floor(
        datediff(window_unit, base, time, _emit_ast=False) / window_duration,
        _emit_ast=False,
    )
    window_start = dateadd(window_unit, window * window_duration, base, _emit_ast=False)
    ans = object_construct_keep_null(
        lit("start", _emit_ast=False),
        window_start,
        lit("end", _emit_ast=False),
        dateadd(window_unit, window_duration, window_start, _emit_ast=False),
        _emit_ast=False,
    )._alias("window")
    ans._ast = ast
    return ans


@publicapi
def is_array(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains an ARRAY value.

    Examples::

        >>> df = session.create_dataframe([[["element"], True]], schema=["a", "b"])
        >>> df.select(is_array(df["a"]).alias("is_array_a"), is_array(df["b"]).alias("is_array_b")).collect()
        [Row(IS_ARRAY_A=True, IS_ARRAY_B=False)]
    """
    c = _to_col_if_str(col, "is_array")
    return _call_function("is_array", c, _emit_ast=_emit_ast)


@publicapi
def is_boolean(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a boolean value.

    Examples::

        >>> from snowflake.snowpark.types import StructField, VariantType
        >>> df = session.create_dataframe([[True, 'X']], schema=["a", "b"])
        >>> df.select(is_boolean(to_variant(df["a"])).alias("boolean"), is_boolean(to_variant(df["b"])).alias("varchar")).collect()
        [Row(BOOLEAN=True, VARCHAR=False)]
    """
    c = _to_col_if_str(col, "is_boolean")
    return _call_function("is_boolean", c, _emit_ast=_emit_ast)


@publicapi
def is_binary(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a binary value.

    Examples::

        >>> from snowflake.snowpark.types import StructField, VariantType
        >>> df = session.create_dataframe([[b"snow", "snow"]], schema=["a", "b"])
        >>> df.select(is_binary(to_variant(df["a"])).alias("binary"), is_binary(to_variant(df["b"])).alias("varchar")).collect()
        [Row(BINARY=True, VARCHAR=False)]
    """
    c = _to_col_if_str(col, "is_binary")
    return _call_function("is_binary", c, _emit_ast=_emit_ast)


@publicapi
def is_char(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a string.

    Examples::

        >>> from snowflake.snowpark.types import StructField, VariantType
        >>> df = session.create_dataframe([["abc", 123]], schema=["a", "b"])
        >>> df.select(is_char(to_variant(df["a"])).alias("varchar"), is_char(to_variant(df["b"])).alias("int")).collect()
        [Row(VARCHAR=True, INT=False)]
    """
    c = _to_col_if_str(col, "is_char")
    return _call_function("is_char", c, _emit_ast=_emit_ast)


is_varchar = is_char


@publicapi
def is_date(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a date value.

    Examples::

        >>> import datetime
        >>> from snowflake.snowpark.types import StructField, VariantType
        >>> df = session.create_dataframe([[datetime.date(2023, 3, 2), 123]], schema=["a", "b"])
        >>> df.select(is_date(to_variant(df["a"])).alias("date"), is_date(to_variant(df["b"])).alias("int")).collect()
        [Row(DATE=True, INT=False)]
    """
    c = _to_col_if_str(col, "is_date")
    return _call_function("is_date", c, _emit_ast=_emit_ast)


is_date_value = is_date


@publicapi
def is_decimal(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a fixed-point decimal value or integer.

    Examples::

        >>> import decimal
        >>> from snowflake.snowpark.types import StructField, VariantType
        >>> df = session.create_dataframe([[decimal.Decimal(1), "X"]], schema=["a", "b"])
        >>> df.select(is_decimal(to_variant(df["a"])).alias("decimal"), is_decimal(to_variant(df["b"])).alias("varchar")).collect()
        [Row(DECIMAL=True, VARCHAR=False)]
    """
    c = _to_col_if_str(col, "is_decimal")
    return _call_function("is_decimal", c, _emit_ast=_emit_ast)


@publicapi
def is_double(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a floating-point value, fixed-point decimal, or integer.

    Examples::

        >>> from snowflake.snowpark.types import StructField, VariantType
        >>> df = session.create_dataframe([[1.2, "X"]], schema=["a", "b"])
        >>> df.select(is_double(to_variant(df["a"])).alias("double"), is_double(to_variant(df["b"])).alias("varchar")).collect()
        [Row(DOUBLE=True, VARCHAR=False)]
    """
    c = _to_col_if_str(col, "is_double")
    return _call_function("is_double", c, _emit_ast=_emit_ast)


@publicapi
def is_real(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a floating-point value, fixed-point decimal, or integer.

    Example::

        >>> from snowflake.snowpark.functions import to_variant, is_real
        >>> df = session.create_dataframe([[1.2, "X"]], schema=["a", "b"])
        >>> df.select(is_real(to_variant("a")).as_("a"), is_real(to_variant("b")).as_("b")).collect()
        [Row(A=True, B=False)]
    """
    c = _to_col_if_str(col, "is_real")
    return _call_function("is_real", c, _emit_ast=_emit_ast)


@publicapi
def is_integer(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a integer value.

    Examples::

        >>> from snowflake.snowpark.types import StructField, VariantType
        >>> df = session.create_dataframe([[1, "X"]], schema=["a", "b"])
        >>> df.select(is_integer(to_variant(df["a"])).alias("int"), is_integer(to_variant(df["b"])).alias("varchar")).collect()
        [Row(INT=True, VARCHAR=False)]
    """
    c = _to_col_if_str(col, "is_integer")
    return _call_function("is_integer", c, _emit_ast=_emit_ast)


@publicapi
def is_null_value(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a JSON null value.

    Example::

        >>> from snowflake.snowpark.functions import to_variant, is_null_value
        >>> df = session.create_dataframe([[{"a": "foo"}], [{"a": None}], [None]], schema=["a"])
        >>> df.select(is_null_value(to_variant("a")["a"]).as_("a")).collect()
        [Row(A=False), Row(A=True), Row(A=None)]
    """
    c = _to_col_if_str(col, "is_null_value")
    return _call_function("is_null_value", c, _emit_ast=_emit_ast)


@publicapi
def is_object(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains an OBJECT value.

    Example::

        >>> from snowflake.snowpark.functions import to_variant, is_object
        >>> df = session.create_dataframe([[[1, 2], {"a": "snow"}]], schema=["a", "b"])
        >>> df.select(is_object(to_variant("a")).as_("a"), is_object(to_variant("b")).as_("b")).collect()
        [Row(A=False, B=True)]
    """
    c = _to_col_if_str(col, "is_object")
    return _call_function("is_object", c, _emit_ast=_emit_ast)


@publicapi
def is_time(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a TIME value.

    Example::

        >>> import datetime
        >>> from snowflake.snowpark.functions import to_variant, is_time
        >>> df = session.create_dataframe([[datetime.time(10, 10), "X"]], schema=["a", "b"])
        >>> df.select(is_time(to_variant("a")).as_("a"), is_time(to_variant("b")).as_("b")).collect()
        [Row(A=True, B=False)]
    """
    c = _to_col_if_str(col, "is_time")
    return _call_function("is_time", c, _emit_ast=_emit_ast)


@publicapi
def is_timestamp_ltz(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a TIMESTAMP_LTZ value to be
    interpreted using the local time zone.

    Example::

        >>> from snowflake.snowpark.functions import to_variant, is_timestamp_ltz
        >>> df = session.sql("select to_timestamp_ntz('2017-02-24 12:00:00.456') as timestamp_ntz1, "
        ...                  "to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00') as timestamp_ltz1, "
        ...                  "to_timestamp_tz('2017-02-24 13:00:00.123 +01:00') as timestamp_tz1")
        >>> df.select(is_timestamp_ltz(to_variant("timestamp_ntz1")).as_("a"),
        ...           is_timestamp_ltz(to_variant("timestamp_ltz1")).as_("b"),
        ...           is_timestamp_ltz(to_variant("timestamp_tz1")).as_("c")).collect()
        [Row(A=False, B=True, C=False)]
    """
    c = _to_col_if_str(col, "is_timestamp_ltz")
    return _call_function("is_timestamp_ltz", c, _emit_ast=_emit_ast)


@publicapi
def is_timestamp_ntz(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a TIMESTAMP_NTZ value with no time zone.

    Example::

        >>> from snowflake.snowpark.functions import to_variant, is_timestamp_ntz
        >>> df = session.sql("select to_timestamp_ntz('2017-02-24 12:00:00.456') as timestamp_ntz1, "
        ...                  "to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00') as timestamp_ltz1, "
        ...                  "to_timestamp_tz('2017-02-24 13:00:00.123 +01:00') as timestamp_tz1")
        >>> df.select(is_timestamp_ntz(to_variant("timestamp_ntz1")).as_("a"),
        ...           is_timestamp_ntz(to_variant("timestamp_ltz1")).as_("b"),
        ...           is_timestamp_ntz(to_variant("timestamp_tz1")).as_("c")).collect()
        [Row(A=True, B=False, C=False)]
    """
    c = _to_col_if_str(col, "is_timestamp_ntz")
    return _call_function("is_timestamp_ntz", c, _emit_ast=_emit_ast)


@publicapi
def is_timestamp_tz(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the specified VARIANT column contains a TIMESTAMP_TZ value with a time zone.

    Example::

        >>> from snowflake.snowpark.functions import to_variant, is_timestamp_tz
        >>> df = session.sql("select to_timestamp_ntz('2017-02-24 12:00:00.456') as timestamp_ntz1, "
        ...                  "to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00') as timestamp_ltz1, "
        ...                  "to_timestamp_tz('2017-02-24 13:00:00.123 +01:00') as timestamp_tz1")
        >>> df.select(is_timestamp_tz(to_variant("timestamp_ntz1")).as_("a"),
        ...           is_timestamp_tz(to_variant("timestamp_ltz1")).as_("b"),
        ...           is_timestamp_tz(to_variant("timestamp_tz1")).as_("c")).collect()
        [Row(A=False, B=False, C=True)]
    """
    c = _to_col_if_str(col, "is_timestamp_tz")
    return _call_function("is_timestamp_tz", c, _emit_ast=_emit_ast)


def _columns_from_timestamp_parts(
    func_name: str, *args: Union[ColumnOrName, int]
) -> Tuple[Column, ...]:

    if len(args) not in [3, 6]:
        # Should never happen since we only use this internally
        raise ValueError(f"Incorrect number of args passed to {func_name}")
    args = tuple(_to_col_if_str_or_int(arg, func_name) for arg in args)
    return args


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
        ns_arg = args[6] if num_args >= 7 else kwargs.get("nanoseconds")
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
            return y, m, d, h, min_, s, lit(0, _emit_ast=False), tz
        else:
            return y, m, d, h, min_, s
    else:
        raise ValueError(
            f"{func_name} expected 2, 6, 7 or 8  required arguments, got {num_args}"
        )


@publicapi
def time_from_parts(
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
    _emit_ast: bool = True,
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

    # AST.
    ast = (
        build_function_expr(
            "time_from_parts",
            [hour, minute, second] + ([] if nanoseconds is None else [nanoseconds]),
        )
        if _emit_ast
        else None
    )

    h, m, s = _columns_from_timestamp_parts("time_from_parts", hour, minute, second)
    return (
        _call_function(
            "time_from_parts",
            h,
            m,
            s,
            _to_col_if_str_or_int(nanoseconds, "time_from_parts"),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
        if nanoseconds
        else _call_function("time_from_parts", h, m, s, _ast=ast, _emit_ast=_emit_ast)
    )


@overload
@publicapi
def timestamp_from_parts(
    date_expr: ColumnOrName, time_expr: ColumnOrName, _emit_ast: bool = True
) -> Column:
    ...  # pragma: no cover


@overload
@publicapi
def timestamp_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanosecond: Optional[Union[ColumnOrName, int]] = None,
    timezone: Optional[ColumnOrLiteralStr] = None,
    _emit_ast: bool = True,
) -> Column:
    ...  # pragma: no cover


@publicapi
def timestamp_from_parts(*args, _emit_ast: bool = True, **kwargs) -> Column:
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
    # AST.
    ast = build_function_expr("timestamp_from_parts", args) if _emit_ast else None
    return _call_function(
        "timestamp_from_parts",
        *_timestamp_from_parts_internal("timestamp_from_parts", *args, **kwargs),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def timestamp_ltz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
    _emit_ast: bool = True,
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
    # AST.
    ast = (
        build_function_expr(
            "timestamp_ltz_from_parts",
            [year, month, day, hour, minute, second]
            + ([] if nanoseconds is None else [nanoseconds]),
        )
        if _emit_ast
        else None
    )

    func_name = "timestamp_ltz_from_parts"

    y, m, d, h, min_, s = _columns_from_timestamp_parts(
        func_name, year, month, day, hour, minute, second
    )
    ns = None if nanoseconds is None else _to_col_if_str_or_int(nanoseconds, func_name)
    return (
        _call_function(func_name, y, m, d, h, min_, s, _ast=ast, _emit_ast=_emit_ast)
        if ns is None
        else _call_function(
            func_name, y, m, d, h, min_, s, ns, _ast=ast, _emit_ast=_emit_ast
        )
    )


@overload
@publicapi
def timestamp_ntz_from_parts(
    date_expr: ColumnOrName, time_expr: ColumnOrName, _emit_ast: bool = True
) -> Column:
    ...  # pragma: no cover


@overload
@publicapi
def timestamp_ntz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanosecond: Optional[Union[ColumnOrName, int]] = None,
    _emit_ast: bool = True,
) -> Column:
    ...  # pragma: no cover


@publicapi
def timestamp_ntz_from_parts(*args, _emit_ast: bool = True, **kwargs) -> Column:
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
    ast = build_function_expr("timestamp_ntz_from_parts", args) if _emit_ast else None
    return _call_function(
        "timestamp_ntz_from_parts",
        *_timestamp_from_parts_internal(
            "timestamp_ntz_from_parts",
            *args,
            **kwargs,
        ),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def timestamp_tz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
    timezone: Optional[ColumnOrLiteralStr] = None,
    _emit_ast: bool = True,
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
    # AST.
    ast = (
        build_function_expr(
            "timestamp_tz_from_parts",
            [year, month, day, hour, minute, second]
            + ([] if nanoseconds is None else [nanoseconds])
            + ([] if timezone is None else [timezone]),
        )
        if _emit_ast
        else None
    )

    func_name = "timestamp_tz_from_parts"
    y, m, d, h, min_, s = _columns_from_timestamp_parts(
        func_name, year, month, day, hour, minute, second
    )
    ns = None if nanoseconds is None else _to_col_if_str_or_int(nanoseconds, func_name)
    tz = None if timezone is None else _to_col_if_sql_expr(timezone, func_name)

    if nanoseconds is not None and timezone is not None:
        return _call_function(
            func_name, y, m, d, h, min_, s, ns, tz, _ast=ast, _emit_ast=_emit_ast
        )
    elif nanoseconds is not None:
        return _call_function(
            func_name, y, m, d, h, min_, s, ns, _ast=ast, _emit_ast=_emit_ast
        )
    elif timezone is not None:
        return _call_function(
            func_name,
            y,
            m,
            d,
            h,
            min_,
            s,
            lit(0, _emit_ast=False),
            tz,
            _ast=ast,
            _emit_ast=_emit_ast,
        )

    return _call_function(func_name, y, m, d, h, min_, s, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def weekofyear(e: ColumnOrName, _emit_ast: bool = True) -> Column:
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
    return _call_function("weekofyear", c, _emit_ast=_emit_ast)


@publicapi
def typeof(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Reports the type of a value stored in a VARIANT column. The type is returned as a string.

    For columns where all rows share the same type, the result of `typeof` is the underlying Snowflake column type.

    Example::

        >>> df = session.create_dataframe([1, 2, 3], schema=["A"])
        >>> df.select(typeof(col("A")).as_("ans")).collect()
        [Row(ANS='INTEGER'), Row(ANS='INTEGER'), Row(ANS='INTEGER')]

    For columns of VARIANT type, the underlying stored type is returned.

    Example::

        >>> from snowflake.snowpark.types import VariantType, StructType, StructField
        >>> schema = StructType([StructField("A", VariantType())])
        >>> df = session.create_dataframe([1, 3.1, 'test'], schema=schema)
        >>> df.select(typeof(col("A")).as_("ans")).collect()
        [Row(ANS='INTEGER'), Row(ANS='DECIMAL'), Row(ANS='VARCHAR')]

    """
    c = _to_col_if_str(col, "typeof")
    return _call_function("typeof", c, _emit_ast=_emit_ast)


@publicapi
def check_json(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Checks the validity of a JSON document.
    If the input string is a valid JSON document or a NULL (i.e. no error would occur when
    parsing the input string), the function returns NULL.
    In case of a JSON parsing error, the function returns a string that contains the error
    message.

    Example::

        >>> df = session.create_dataframe(["{'ValidKey1': 'ValidValue1'}", "{'Malformed -- missing val':}", None], schema=['a'])
        >>> df.select(check_json(df.a)).show()
        -----------------------
        |"CHECK_JSON(""A"")"  |
        -----------------------
        |NULL                 |
        |misplaced }, pos 29  |
        |NULL                 |
        -----------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "check_json")
    return _call_function("check_json", c, _emit_ast=_emit_ast)


@publicapi
def check_xml(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Checks the validity of an XML document.
    If the input string is a valid XML document or a NULL (i.e. no error would occur when parsing
    the input string), the function returns NULL.
    In case of an XML parsing error, the output string contains the error message.

    Example::

        >>> df = session.create_dataframe(["<name> Valid </name>", "<name> Invalid </WRONG_CLOSING_TAG>", None], schema=['a'])
        >>> df.select(check_xml(df.a)).show()
        ---------------------------------------------------
        |"CHECK_XML(""A"")"                               |
        ---------------------------------------------------
        |NULL                                             |
        |no opening tag for </WRONG_CLOSING_TAG>, pos 35  |
        |NULL                                             |
        ---------------------------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "check_xml")
    return _call_function("check_xml", c, _emit_ast=_emit_ast)


@publicapi
def json_extract_path_text(
    col: ColumnOrName, path: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Parses a JSON string and returns the value of an element at a specified path in the resulting
    JSON document.

    Example::

        >>> from snowflake.snowpark.functions import json_extract_path_text, to_variant
        >>> df = session.create_dataframe([[{"a": "foo"}, "a"], [{"a": None}, "a"], [{"a": "foo"}, "b"], [None, "a"]], schema=["k", "v"])
        >>> df.select(json_extract_path_text(to_variant("k"), "v").as_("res")).collect()
        [Row(RES='foo'), Row(RES=None), Row(RES=None), Row(RES=None)]
    """
    c = _to_col_if_str(col, "json_extract_path_text")
    p = _to_col_if_str(path, "json_extract_path_text")
    return _call_function("json_extract_path_text", c, p, _emit_ast=_emit_ast)


@publicapi
def parse_json(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Parse the value of the specified column as a JSON string and returns the
    resulting JSON document.

    Example::

        >>> df = session.create_dataframe([['{"key": "1"}']], schema=["a"])
        >>> df.select(parse_json(df["a"]).alias("result")).show()
        ----------------
        |"RESULT"      |
        ----------------
        |{             |
        |  "key": "1"  |
        |}             |
        ----------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "parse_json")
    return _call_function("parse_json", c, _emit_ast=_emit_ast)


@publicapi
def from_json(
    e: ColumnOrName, schema: Union[str, DataType], _emit_ast: bool = True
) -> Column:
    """Parses a column contains a JSON string value into a column of the type specified by schema.
    Schema can be defined as a DataType object or as a compatible type string.

    Example::

        >>> from snowflake.snowpark.types import MapType, StringType
        >>> df = session.create_dataframe([('{"key": "value"}',),], schema=["a"])
        >>> df.select(from_json(df.a, MapType(StringType(), StringType()))).show()
        ----------------------
        |"from_json(""A"")"  |
        ----------------------
        |{                   |
        |  "key": "value"    |
        |}                   |
        ----------------------
        <BLANKLINE>

    Example::

        >>> df = session.create_dataframe([('[1, 2, 3]',),], schema=["b"])
        >>> df.select(from_json(df.b, "array<integer>")).show()
        ----------------------
        |"from_json(""B"")"  |
        ----------------------
        |[                   |
        |  1,                |
        |  2,                |
        |  3                 |
        |]                   |
        ----------------------
        <BLANKLINE>
    """
    ast = build_function_expr("from_json", [e, schema]) if _emit_ast else None
    c = _to_col_if_str(e, "from_json")
    if isinstance(schema, str):
        schema = type_string_to_type_object(schema)
    ans = (
        parse_json(e, _emit_ast=False)
        .cast(schema, _emit_ast=False)
        ._alias(f"from_json({c.get_name()})")
    )
    ans._ast = ast
    return ans


@publicapi
def parse_xml(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Parse the value of the specified column as a JSON string and returns the
    resulting XML document.

    Example::

        >>> df = session.sql(
        ...     "select (column1) as v from values ('<t1>foo<t2>bar</t2><t3></t3></t1>'), "
        ...     "('<t1></t1>')"
        ... )
        >>> df.select(parse_xml("v").alias("result")).show()
        ------------------
        |"RESULT"        |
        ------------------
        |<t1>            |
        |  foo           |
        |  <t2>bar</t2>  |
        |  <t3></t3>     |
        |</t1>           |
        |<t1></t1>       |
        ------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "parse_xml")
    return _call_function("parse_xml", c, _emit_ast=_emit_ast)


@publicapi
def strip_null_value(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts a JSON "null" value in the specified column to a SQL NULL value.
    All other VARIANT values in the column are returned unchanged.

    Example::
        >>> df = session.create_dataframe(
        ...     ["null", "1"],
        ...     schema=["S"],
        ... ).select(strip_null_value(parse_json(col("S"))).as_("B")).where(
        ...     sql_expr("B is null")
        ... )
        >>> df.collect()
        [Row(B=None)]
    """
    c = _to_col_if_str(col, "strip_null_value")
    return _call_function("strip_null_value", c, _emit_ast=_emit_ast)


@publicapi
def array_agg(
    col: ColumnOrName, is_distinct: bool = False, _emit_ast: bool = True
) -> Column:
    """Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
    ARRAY is returned.

    Example::
        >>> df = session.create_dataframe([[1], [2], [3], [1]], schema=["a"])
        >>> df.select(array_agg("a", True).within_group("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2,      |
        |  3       |
        |]         |
        ------------
        <BLANKLINE>
    """
    ast = build_function_expr("array_agg", [col, is_distinct]) if _emit_ast else None
    c = _to_col_if_str(col, "array_agg")
    return _call_function(
        "array_agg", c, is_distinct=is_distinct, _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def array_append(
    array: ColumnOrName, element: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns an ARRAY containing all elements from the source ARRAY as well as the new element.
    The new element is located at end of the ARRAY.

    Args:
        array: The column containing the source ARRAY.
        element: The column containing the element to be appended. The element may be of almost
            any data type. The data type does not need to match the data type(s) of the
            existing elements in the ARRAY.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3])])
        >>> df.select(array_append("a", lit(4)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2,      |
        |  3,      |
        |  4       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_append")
    e = _to_col_if_str(element, "array_append")
    return _call_function("array_append", a, e, _emit_ast=_emit_ast)


@publicapi
def array_remove(
    array: ColumnOrName, element: ColumnOrLiteral, _emit_ast: bool = True
) -> Column:
    """Given a source ARRAY, returns an ARRAY with elements of the specified value removed.

    Args:
        array: name of column containing array.
        element: element to be removed from the array. If the element is a VARCHAR, it needs
            to be casted into VARIANT data type.

    Examples::
        >>> from snowflake.snowpark.types import VariantType
        >>> df = session.create_dataframe([([1, '2', 3.1, 1, 1],)], ['data'])
        >>> df.select(array_remove(df.data, 1).alias("objects")).show()
        -------------
        |"OBJECTS"  |
        -------------
        |[          |
        |  "2",     |
        |  3.1      |
        |]          |
        -------------
        <BLANKLINE>

        >>> df.select(array_remove(df.data, lit('2').cast(VariantType())).alias("objects")).show()
        -------------
        |"OBJECTS"  |
        -------------
        |[          |
        |  1,       |
        |  3.1,     |
        |  1,       |
        |  1        |
        |]          |
        -------------
        <BLANKLINE>

        >>> df.select(array_remove(df.data, None).alias("objects")).show()
        -------------
        |"OBJECTS"  |
        -------------
        |NULL       |
        -------------
        <BLANKLINE>

        >>> df.select(array_remove(array_remove(df.data, 1), "2").alias("objects")).show()
        -------------
        |"OBJECTS"  |
        -------------
        |[          |
        |  3.1      |
        |]          |
        -------------
        <BLANKLINE>

    See Also:
        - `ARRAY <https://docs.snowflake.com/en/sql-reference/data-types-semistructured#label-data-type-array>`_ for more details on semi-structured arrays.
    """

    # AST.
    ast = build_function_expr("array_remove", [array, element]) if _emit_ast else None

    a = _to_col_if_str(array, "array_remove")
    e = (
        lit(element, _emit_ast=False).cast("VARIANT", _emit_ast=False)
        if isinstance(element, str)
        else element
    )
    return _call_function("array_remove", a, e, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def array_cat(
    array1: ColumnOrName, array2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the concatenation of two ARRAYs.

    Args:
        array1: Column containing the source ARRAY.
        array2: Column containing the ARRAY to be appended to array1.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3], b=[4, 5])])
        >>> df.select(array_cat("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2,      |
        |  3,      |
        |  4,      |
        |  5       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a1 = _to_col_if_str(array1, "array_cat")
    a2 = _to_col_if_str(array2, "array_cat")
    return _call_function("array_cat", a1, a2, _emit_ast=_emit_ast)


@publicapi
def array_compact(array: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a compacted ARRAY with missing and null values removed,
    effectively converting sparse arrays into dense arrays.

    Args:
        array: Column containing the source ARRAY to be compacted

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(a=[1, None, 3])])
        >>> df.select("a", array_compact("a").alias("compacted")).show()
        -------------------------
        |"A"      |"COMPACTED"  |
        -------------------------
        |[        |[            |
        |  1,     |  1,         |
        |  null,  |  3          |
        |  3      |]            |
        |]        |             |
        -------------------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_compact")
    return _call_function("array_compact", a, _emit_ast=_emit_ast)


@publicapi
def array_construct(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns an ARRAY constructed from zero, one, or more inputs.

    Args:
        cols: Columns containing the values (or expressions that evaluate to values). The
            values do not all need to be of the same data type.

    Example::
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        >>> df.select(array_construct("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2       |
        |]         |
        |[         |
        |  3,      |
        |  4       |
        |]         |
        ------------
        <BLANKLINE>
    """
    cs = [_to_col_if_str(c, "array_construct") for c in cols]
    return _call_function("array_construct", *cs, _emit_ast=_emit_ast)


@publicapi
def array_construct_compact(*cols: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns an ARRAY constructed from zero, one, or more inputs.
    The constructed ARRAY omits any NULL input values.

    Args:
        cols: Columns containing the values (or expressions that evaluate to values). The
            values do not all need to be of the same data type.

    Example::
        >>> df = session.create_dataframe([[1, None, 2], [3, None, 4]], schema=["a", "b", "c"])
        >>> df.select(array_construct_compact("a", "b", "c").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2       |
        |]         |
        |[         |
        |  3,      |
        |  4       |
        |]         |
        ------------
        <BLANKLINE>
    """
    cs = [_to_col_if_str(c, "array_construct_compact") for c in cols]
    return _call_function("array_construct_compact", *cs, _emit_ast=_emit_ast)


@publicapi
def array_contains(
    variant: ColumnOrName, array: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns True if the specified VARIANT is found in the specified ARRAY.

    Args:
        variant: Column containing the VARIANT to find.
        array: Column containing the ARRAY to search.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row([1, 2]), Row([1, 3])], schema=["a"])
        >>> df.select(array_contains(lit(2), "a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |True      |
        |False     |
        ------------
        <BLANKLINE>
    """
    v = _to_col_if_str(variant, "array_contains")
    a = _to_col_if_str(array, "array_contains")
    return _call_function("array_contains", v, a, _emit_ast=_emit_ast)


@publicapi
def array_insert(
    array: ColumnOrName,
    pos: ColumnOrName,
    element: ColumnOrName,
    _emit_ast: bool = True,
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
            array is preserved.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row([1, 2]), Row([1, 3])], schema=["a"])
        >>> df.select(array_insert("a", lit(0), lit(10)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  10,     |
        |  1,      |
        |  2       |
        |]         |
        |[         |
        |  10,     |
        |  1,      |
        |  3       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_insert")
    p = _to_col_if_str(pos, "array_insert")
    e = _to_col_if_str(element, "array_insert")
    return _call_function("array_insert", a, p, e, _emit_ast=_emit_ast)


@publicapi
def array_position(
    variant: ColumnOrName, array: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the index of the first occurrence of an element in an ARRAY.

    Args:
        variant: Column containing the VARIANT value that you want to find. The function
            searches for the first occurrence of this value in the array.
        array: Column containing the ARRAY to be searched.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row([2, 1]), Row([1, 3])], schema=["a"])
        >>> df.select(array_position(lit(1), "a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        |0         |
        ------------
        <BLANKLINE>
    """
    v = _to_col_if_str(variant, "array_position")
    a = _to_col_if_str(array, "array_position")
    return _call_function("array_position", v, a, _emit_ast=_emit_ast)


@publicapi
def array_prepend(
    array: ColumnOrName, element: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns an ARRAY containing the new element as well as all elements from the source ARRAY.
    The new element is positioned at the beginning of the ARRAY.

    Args:
        array Column containing the source ARRAY.
        element Column containing the element to be prepended.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3])])
        >>> df.select(array_prepend("a", lit(4)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  4,      |
        |  1,      |
        |  2,      |
        |  3       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_prepend")
    e = _to_col_if_str(element, "array_prepend")
    return _call_function("array_prepend", a, e, _emit_ast=_emit_ast)


@publicapi
def array_size(array: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the size of the input ARRAY.

    If the specified column contains a VARIANT value that contains an ARRAY, the size of the ARRAY
    is returned; otherwise, NULL is returned if the value is not an ARRAY.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3])])
        >>> df.select(array_size("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |3         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_size")
    return _call_function("array_size", a, _emit_ast=_emit_ast)


@publicapi
def array_slice(
    array: ColumnOrName, from_: ColumnOrName, to: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns an ARRAY constructed from a specified subset of elements of the input ARRAY.

    Args:
        array: Column containing the source ARRAY.
        from_: Column containing a position in the source ARRAY. The position of the first
            element is 0. Elements from positions less than this parameter are
            not included in the resulting ARRAY.
        to: Column containing a position in the source ARRAY. Elements from positions equal to
            or greater than this parameter are not included in the resulting array.

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3, 4, 5])])
        >>> df.select(array_slice("a", lit(1), lit(3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  2,      |
        |  3       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_slice")
    f = _to_col_if_str(from_, "array_slice")
    t = _to_col_if_str(to, "array_slice")
    return _call_function("array_slice", a, f, t, _emit_ast=_emit_ast)


@publicapi
def array_to_string(
    array: ColumnOrName, separator: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns an input ARRAY converted to a string by casting all values to strings (using
    TO_VARCHAR) and concatenating them (using the string from the second argument to separate
    the elements).

    Args:
        array: Column containing the ARRAY of elements to convert to a string.
        separator: Column containing the string to put between each element (e.g. a space,
            comma, or other human-readable separator).

    Example::
        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(a=[1, True, "s"])])
        >>> df.select(array_to_string("a", lit(",")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1,true,s  |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_to_string")
    s = _to_col_if_str(separator, "array_to_string")
    return _call_function("array_to_string", a, s, _emit_ast=_emit_ast)


@publicapi
def array_unique_agg(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a Column containing the distinct values in the specified column col.
    The values in the Column are in no particular order, and the order is not deterministic.
    The function ignores NULL values in col.
    If col contains only NULL values or col is empty, the function returns an empty Column.

    Args:
        col: A :class:`Column` object or column name that determines the values.

    Example::
        >>> df = session.create_dataframe([[5], [2], [1], [2], [1]], schema=["a"])
        >>> df.select(array_unique_agg("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  5,      |
        |  2,      |
        |  1       |
        |]         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "array_unique_agg")
    return _call_function("array_unique_agg", c, _emit_ast=_emit_ast)


@publicapi
def map_cat(
    col1: ColumnOrName, col2: ColumnOrName, *cols: ColumnOrName, _emit_ast: bool = True
):
    """Returns the concatenatation of two or more MAPs.

    Args:
        col1: The source map
        col2: The map to be appended to col1
        cols: More maps to be appended

    Example::
        >>> df = session.sql("select {'k1': 'v1'} :: MAP(STRING,STRING) as A, {'k2': 'v2'} :: MAP(STRING,STRING) as B")
        >>> df.select(map_cat("A", "B")).show()
        ---------------------------
        |"MAP_CAT(""A"", ""B"")"  |
        ---------------------------
        |{                        |
        |  "k1": "v1",            |
        |  "k2": "v2"             |
        |}                        |
        ---------------------------
        <BLANKLINE>
        >>> df = session.sql("select {'k1': 'v1'} :: MAP(STRING,STRING) as A, {'k2': 'v2'} :: MAP(STRING,STRING) as B, {'k3': 'v3'} :: MAP(STRING,STRING) as C")
        >>> df.select(map_cat("A", "B", "C")).show()
        -------------------------------------------
        |"MAP_CAT(MAP_CAT(""A"", ""B""), ""C"")"  |
        -------------------------------------------
        |{                                        |
        |  "k1": "v1",                            |
        |  "k2": "v2",                            |
        |  "k3": "v3"                             |
        |}                                        |
        -------------------------------------------
        <BLANKLINE>
    """
    m1 = _to_col_if_str(col1, "map_cat")
    m2 = _to_col_if_str(col2, "map_cat")
    ast = build_function_expr("map_cat", [col1, col2, *cols]) if _emit_ast else None

    def map_cat_two_maps(first, second):
        return _call_function("map_cat", first, second, _emit_ast=False)

    cols_to_concat = [m1, m2]
    for c in cols:
        cols_to_concat.append(_to_col_if_str(c, "map_cat"))

    ans = reduce(map_cat_two_maps, cols_to_concat)
    ans._ast = ast
    return ans


@publicapi
def map_contains_key(value: ColumnOrLiteral, col: ColumnOrName, _emit_ast: bool = True):
    """Determines whether the specified MAP contains the specified key.

    Args:
        value: The key to find.
        col: The map to be searched.

    Example 1::
        >>> df = session.sql("select {'k1': 'v1'} :: MAP(STRING,STRING) as M, 'k1' as V")
        >>> df.select(map_contains_key(col("V"), "M")).show()
        ------------------------------------
        |"MAP_CONTAINS_KEY(""V"", ""M"")"  |
        ------------------------------------
        |True                              |
        ------------------------------------
        <BLANKLINE>

    Example 2::
        >>> df = session.sql("select {'k1': 'v1'} :: MAP(STRING,STRING) as M")
        >>> df.select(map_contains_key("k1", "M")).show()
        -----------------------------------
        |"MAP_CONTAINS_KEY('K1', ""M"")"  |
        -----------------------------------
        |True                             |
        -----------------------------------
        <BLANKLINE>
    """
    m = _to_col_if_str(col, "map_contains")
    return _call_function("map_contains_key", value, m, _emit_ast=_emit_ast)


@publicapi
def map_keys(col: ColumnOrName, _emit_ast: bool = True):
    """Returns the keys in a MAP.

    Args:
        col: The input map.

    Example 1::
        >>> df = session.sql("select {'k1': 'v1', 'k2': 'v2'} :: MAP(STRING,STRING) as M")
        >>> df.select(map_keys("M")).show()
        ---------------------
        |"MAP_KEYS(""M"")"  |
        ---------------------
        |[                  |
        |  "k1",            |
        |  "k2"             |
        |]                  |
        ---------------------
        <BLANKLINE>
    """
    m = _to_col_if_str(col, "map_keys")
    return _call_function("map_keys", m, _emit_ast=_emit_ast)


@publicapi
def size(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the size of the input ARRAY, OBJECT or MAP. Returns NULL if the
    input column does not match any of these types.

    Args:
        col: A :class:`Column` object or column name that determines the values.

    Example::
        >>> df = session.create_dataframe([([1,2,3], {'a': 1, 'b': 2}, 3)], ['col1', 'col2', 'col3'])
        >>> df.select(size(df.col1), size(df.col2), size(df.col3)).show()
        ----------------------------------------------------------
        |"SIZE(""COL1"")"  |"SIZE(""COL2"")"  |"SIZE(""COL3"")"  |
        ----------------------------------------------------------
        |3                 |2                 |NULL              |
        ----------------------------------------------------------
        <BLANKLINE>
    """

    # AST.
    ast = build_function_expr("size", [col]) if _emit_ast else None

    c = _to_col_if_str(col, "size")
    v = to_variant(c, _emit_ast=False)

    result = (
        when(
            is_array(v, _emit_ast=False),
            array_size(v, _emit_ast=False),
            _emit_ast=False,
        )
        .when(
            is_object(v, _emit_ast=False),
            array_size(object_keys(v, _emit_ast=False), _emit_ast=False),
            _emit_ast=False,
        )
        .otherwise(lit(None), _emit_ast=False)
        ._alias(f"SIZE({c.get_name()})")
    )
    result._ast = ast
    return result


@publicapi
def object_agg(
    key: ColumnOrName, value: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns one OBJECT per group. For each key-value input pair, where key must be a VARCHAR
    and value must be a VARIANT, the resulting OBJECT contains a key-value field.

    Example::

        >>> from snowflake.snowpark.types import StructType, StructField, VariantType, StringType
        >>> df = session.create_dataframe(
        ...     [["name", "Joe"], ["zip", "98004"]],
        ...     schema=StructType([StructField("k", StringType()), StructField("v", VariantType())])
        ... )
        >>> df.select(object_agg(col("k"), col("v")).alias("result")).show()
        --------------------
        |"RESULT"          |
        --------------------
        |{                 |
        |  "name": "Joe",  |
        |  "zip": "98004"  |
        |}                 |
        --------------------
        <BLANKLINE>
    """
    k = _to_col_if_str(key, "object_agg")
    v = _to_col_if_str(value, "object_agg")
    return _call_function("object_agg", k, v, _emit_ast=_emit_ast)


@publicapi
def object_construct(*key_values: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns an OBJECT constructed from the arguments.

    Example::

        >>> from snowflake.snowpark.types import StructType, StructField, VariantType, StringType
        >>> df = session.create_dataframe(
        ...     [["name", "Joe"], ["zip", "98004"],["age", None], [None, "value"]],
        ...     schema=StructType([StructField("k", StringType()), StructField("v", VariantType())])
        ... )
        >>> df.select(object_construct(col("k"), col("v")).alias("result")).show()
        --------------------
        |"RESULT"          |
        --------------------
        |{                 |
        |  "name": "Joe"   |
        |}                 |
        |{                 |
        |  "zip": "98004"  |
        |}                 |
        |{}                |
        |{}                |
        --------------------
        <BLANKLINE>
    """
    kvs = [_to_col_if_str(kv, "object_construct") for kv in key_values]
    return _call_function("object_construct", *kvs, _emit_ast=_emit_ast)


@publicapi
def object_construct_keep_null(
    *key_values: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns an object containing the contents of the input (i.e. source) object with one or more
    keys removed.

    Example::

        >>> from snowflake.snowpark.types import StructType, StructField, VariantType, StringType
        >>> df = session.create_dataframe(
        ...     [["key_1", "one"], ["key_2", None]],
        ...     schema=StructType([StructField("k", StringType()), StructField("v", VariantType())])
        ... )
        >>> df.select(object_construct_keep_null(col("k"), col("v")).alias("result")).show()
        --------------------
        |"RESULT"          |
        --------------------
        |{                 |
        |  "key_1": "one"  |
        |}                 |
        |{                 |
        |  "key_2": null   |
        |}                 |
        --------------------
        <BLANKLINE>
    """
    kvs = [_to_col_if_str(kv, "object_construct_keep_null") for kv in key_values]
    return _call_function("object_construct_keep_null", *kvs, _emit_ast=_emit_ast)


@publicapi
def object_delete(
    obj: ColumnOrName, key1: ColumnOrName, *keys: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns an object consisting of the input object with one or more keys removed.
    The input key must not exist in the object.

    Example::

        >>> from snowflake.snowpark.functions import lit
        >>> df = session.sql(
        ...     "select object_construct(a,b,c,d,e,f) as obj from "
        ...     "values('age', 21, 'zip', 21021, 'name', 'Joe'),"
        ...     "('age', 26, 'zip', 94021, 'name', 'Jay') as T(a,b,c,d,e,f)"
        ... )
        >>> df.select(object_delete(col("obj"), lit("age")).alias("result")).show()
        --------------------
        |"RESULT"          |
        --------------------
        |{                 |
        |  "name": "Joe",  |
        |  "zip": 21021    |
        |}                 |
        |{                 |
        |  "name": "Jay",  |
        |  "zip": 94021    |
        |}                 |
        --------------------
        <BLANKLINE>
    """
    o = _to_col_if_str(obj, "object_delete")
    k1 = _to_col_if_str(key1, "object_delete")
    ks = [_to_col_if_str(k, "object_delete") for k in keys]
    return _call_function("object_delete", o, k1, *ks, _emit_ast=_emit_ast)


@publicapi
def object_insert(
    obj: ColumnOrName,
    key: ColumnOrName,
    value: ColumnOrName,
    update_flag: Optional[ColumnOrName] = None,
    _emit_ast: bool = True,
) -> Column:
    """Returns an object consisting of the input object with a new key-value pair inserted (or an
    existing key updated with a new value).

    Example::
        >>> from snowflake.snowpark.functions import lit
        >>> df = session.sql(
        ...     "select object_construct(a,b,c,d,e,f) as obj, k, v from "
        ...     "values('age', 21, 'zip', 21021, 'name', 'Joe', 'age', 0),"
        ...     "('age', 26, 'zip', 94021, 'name', 'Jay', 'age', 0) as T(a,b,c,d,e,f,k,v)"
        ... )
        >>> df.select(object_insert(col("obj"), lit("key"), lit("v")).alias("result")).show()
        --------------------
        |"RESULT"          |
        --------------------
        |{                 |
        |  "age": 21,      |
        |  "key": "v",     |
        |  "name": "Joe",  |
        |  "zip": 21021    |
        |}                 |
        |{                 |
        |  "age": 26,      |
        |  "key": "v",     |
        |  "name": "Jay",  |
        |  "zip": 94021    |
        |}                 |
        --------------------
        <BLANKLINE>
    """
    o = _to_col_if_str(obj, "object_insert")
    k = _to_col_if_str(key, "object_insert")
    v = _to_col_if_str(value, "object_insert")
    uf = _to_col_if_str(update_flag, "update_flag") if update_flag is not None else None
    if uf is not None:
        return _call_function("object_insert", o, k, v, uf, _emit_ast=_emit_ast)
    else:
        return _call_function("object_insert", o, k, v, _emit_ast=_emit_ast)


@publicapi
def object_pick(
    obj: ColumnOrName, key1: ColumnOrName, *keys: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns a new OBJECT containing some of the key-value pairs from an existing object.

    To identify the key-value pairs to include in the new object, pass in the keys as arguments,
    or pass in an array containing the keys.

    If a specified key is not present in the input object, the key is ignored.

    Example::
        >>> from snowflake.snowpark.functions import lit
        >>> df = session.sql(
        ...     "select object_construct(a,b,c,d,e,f) as obj, k, v from "
        ...     "values('age', 21, 'zip', 21021, 'name', 'Joe', 'age', 0),"
        ...     "('age', 26, 'zip', 94021, 'name', 'Jay', 'age', 0) as T(a,b,c,d,e,f,k,v)"
        ... )
        >>> df.select(object_pick(col("obj"), col("k"), lit("name")).alias("result")).show()
        -------------------
        |"RESULT"         |
        -------------------
        |{                |
        |  "age": 21,     |
        |  "name": "Joe"  |
        |}                |
        |{                |
        |  "age": 26,     |
        |  "name": "Jay"  |
        |}                |
        -------------------
        <BLANKLINE>
    """
    o = _to_col_if_str(obj, "object_pick")
    k1 = _to_col_if_str(key1, "object_pick")
    ks = [_to_col_if_str(k, "object_pick") for k in keys]
    return _call_function("object_pick", o, k1, *ks, _emit_ast=_emit_ast)


# The following three vector functions have doctests that are disabled (">>" instead of ">>>")
# since vectors are not yet rolled out.


@publicapi
def vector_cosine_distance(
    v1: ColumnOrName, v2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the cosine distance between two vectors of equal dimension and element type.

    Example::
        >>> from snowflake.snowpark.functions import vector_cosine_distance
        >>> df = session.sql("select [1,2,3]::vector(int,3) as a, [2,3,4]::vector(int,3) as b")
        >>> df.select(vector_cosine_distance(df.a, df.b).as_("dist")).show()
        ----------------------
        |"DIST"              |
        ----------------------
        |0.9925833339709303  |
        ----------------------
        <BLANKLINE>
    """
    v1 = _to_col_if_str(v1, "vector_cosine_distance")
    v2 = _to_col_if_str(v2, "vector_cosine_distance")
    return _call_function("vector_cosine_distance", v1, v2, _emit_ast=_emit_ast)


@publicapi
def vector_l2_distance(
    v1: ColumnOrName, v2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the l2 distance between two vectors of equal dimension and element type.

    Example::
        >>> from snowflake.snowpark.functions import vector_l2_distance
        >>> df = session.sql("select [1,2,3]::vector(int,3) as a, [2,3,4]::vector(int,3) as b")
        >>> df.select(vector_l2_distance(df.a, df.b).as_("dist")).show()
        ----------------------
        |"DIST"              |
        ----------------------
        |1.7320508075688772  |
        ----------------------
        <BLANKLINE>
    """
    v1 = _to_col_if_str(v1, "vector_l2_distance")
    v2 = _to_col_if_str(v2, "vector_l2_distance")
    return _call_function("vector_l2_distance", v1, v2, _emit_ast=_emit_ast)


@publicapi
def vector_inner_product(
    v1: ColumnOrName, v2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the inner product between two vectors of equal dimension and element type.

    Example::
        >>> from snowflake.snowpark.functions import vector_inner_product
        >>> df = session.sql("select [1,2,3]::vector(int,3) as a, [2,3,4]::vector(int,3) as b")
        >>> df.select(vector_inner_product(df.a, df.b).as_("dist")).show()
        ----------
        |"DIST"  |
        ----------
        |20.0    |
        ----------
        <BLANKLINE>
    """
    v1 = _to_col_if_str(v1, "vector_inner_product")
    v2 = _to_col_if_str(v2, "vector_inner_product")
    return _call_function("vector_inner_product", v1, v2, _emit_ast=_emit_ast)


@publicapi
def ln(c: ColumnOrLiteral, _emit_ast: bool = True) -> Column:
    """Returns the natrual logarithm of given column expression.

    Example::
        >>> from snowflake.snowpark.functions import ln
        >>> from math import e
        >>> df = session.create_dataframe([[e]], schema=["ln_value"])
        >>> df.select(ln(col("ln_value")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.0       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(c, "ln")
    return _call_function("ln", c, _emit_ast=_emit_ast)


@publicapi
def asc(c: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a Column expression with values sorted in ascending order.

    Example::

        >>> df = session.create_dataframe([None, 3, 2, 1, None], schema=["a"])
        >>> df.sort(asc(df["a"])).collect()
        [Row(A=None), Row(A=None), Row(A=1), Row(A=2), Row(A=3)]
    """
    # AST.
    ast = build_function_expr("asc", [c]) if _emit_ast else None

    c = _to_col_if_str(c, "asc")
    ans = c.asc(_emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def asc_nulls_first(c: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a Column expression with values sorted in ascending order
    (null values sorted before non-null values).

    Example::

        >>> df = session.create_dataframe([None, 3, 2, 1, None], schema=["a"])
        >>> df.sort(asc_nulls_first(df["a"])).collect()
        [Row(A=None), Row(A=None), Row(A=1), Row(A=2), Row(A=3)]
    """
    # AST.
    ast = build_function_expr("asc_nulls_first", [c]) if _emit_ast else None

    c = _to_col_if_str(c, "asc_nulls_first")
    ans = c.asc_nulls_first(_emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def asc_nulls_last(c: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns a Column expression with values sorted in ascending order
    (null values sorted after non-null values).

    Example::

        >>> df = session.create_dataframe([None, 3, 2, 1, None], schema=["a"])
        >>> df.sort(asc_nulls_last(df["a"])).collect()
        [Row(A=1), Row(A=2), Row(A=3), Row(A=None), Row(A=None)]
    """
    # AST.
    ast = build_function_expr("asc_nulls_last", [c]) if _emit_ast else None

    c = _to_col_if_str(c, "asc_nulls_last")
    ans = c.asc_nulls_last(_emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def desc(c: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns a Column expression with values sorted in descending order.

    Example::

        >>> df = session.create_dataframe([1, 2, 3, None, None], schema=["a"])
        >>> df.sort(desc(df["a"])).collect()
        [Row(A=3), Row(A=2), Row(A=1), Row(A=None), Row(A=None)]
    """
    # AST.
    ast = build_function_expr("desc", [c]) if _emit_ast else None

    c = _to_col_if_str(c, "desc")
    ans = c.desc(_emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def desc_nulls_first(c: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns a Column expression with values sorted in descending order
    (null values sorted before non-null values).

    Example::

        >>> df = session.create_dataframe([1, 2, 3, None, None], schema=["a"])
        >>> df.sort(desc_nulls_first(df["a"])).collect()
        [Row(A=None), Row(A=None), Row(A=3), Row(A=2), Row(A=1)]
    """
    # AST.
    ast = build_function_expr("desc_nulls_first", [c]) if _emit_ast else None

    c = _to_col_if_str(c, "desc_nulls_first")
    ans = c.desc_nulls_first(_emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def desc_nulls_last(c: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns a Column expression with values sorted in descending order
    (null values sorted after non-null values).

    Example::
        >>> df = session.create_dataframe([1, 2, 3, None, None], schema=["a"])
        >>> df.sort(desc_nulls_last(df["a"])).collect()
        [Row(A=3), Row(A=2), Row(A=1), Row(A=None), Row(A=None)]
    """
    # AST.
    ast = build_function_expr("desc_nulls_last", [c]) if _emit_ast else None

    c = _to_col_if_str(c, "desc_nulls_last")
    ans = c.desc_nulls_last(_emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def as_array(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to an array.

    Example::
        >>> df = session.sql("select array_construct(1, 2)::variant as a")
        >>> df.select(as_array("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2       |
        |]         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(variant, "as_array")
    return _call_function("as_array", c, _emit_ast=_emit_ast)


@publicapi
def as_binary(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a binary string.

    Example::
        >>> df = session.sql("select to_binary('F0A5')::variant as a")
        >>> df.select(as_binary("a").alias("result")).show()
        --------------------------
        |"RESULT"                |
        --------------------------
        |bytearray(b'\xf0\xa5')  |
        --------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(variant, "as_binary")
    return _call_function("as_binary", c, _emit_ast=_emit_ast)


@publicapi
def as_char(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a string.

    Example::

        >>> from snowflake.snowpark.functions import as_char, to_variant
        >>> df = session.sql("select 'some string' as char")
        >>> df.char_v = to_variant(df.char)
        >>> df.select(df.char_v.as_("char")).collect() == df.select(df.char).collect()
        False
        >>> df.select(as_char(df.char_v).as_("char")).collect() == df.select(df.char).collect()
        True
    """
    c = _to_col_if_str(variant, "as_char")
    return _call_function("as_char", c, _emit_ast=_emit_ast)


@publicapi
def as_varchar(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a string.

    Example::

        >>> from snowflake.snowpark.functions import as_varchar, to_variant
        >>> df = session.sql("select 'some string' as char")
        >>> df.char_v = to_variant(df.char)
        >>> df.select(df.char_v.as_("char")).collect() == df.select(df.char).collect()
        False
        >>> df.select(as_varchar(df.char_v).as_("char")).collect() == df.select(df.char).collect()
        True
    """
    c = _to_col_if_str(variant, "as_varchar")
    return _call_function("as_varchar", c, _emit_ast=_emit_ast)


@publicapi
def as_date(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a date.

    Example::
        >>> df = session.sql("select date'2020-1-1'::variant as a")
        >>> df.select(as_date("a").alias("result")).show()
        --------------
        |"RESULT"    |
        --------------
        |2020-01-01  |
        --------------
        <BLANKLINE>
    """
    c = _to_col_if_str(variant, "as_date")
    return _call_function("as_date", c, _emit_ast=_emit_ast)


@publicapi
def cast(
    column: ColumnOrName, to: Union[str, DataType], _emit_ast: bool = True
) -> Column:
    """Converts a value of one data type into another data type.
    The semantics of CAST are the same as the semantics of the corresponding to datatype conversion functions.
    If the cast is not possible, a ``SnowparkSQLException`` exception is thrown.

    Example::

        >>> from snowflake.snowpark.types import DecimalType, IntegerType
        >>> df = session.create_dataframe([[1.5432]], schema=["d"])
        >>> df.select(cast(df.d, DecimalType(15, 2)).as_("DECIMAL"), cast(df.d, IntegerType()).as_("INT")).show()
        ---------------------
        |"DECIMAL"  |"INT"  |
        ---------------------
        |1.54       |2      |
        ---------------------
        <BLANKLINE>
    """

    ast = build_function_expr("cast", [column, to]) if _emit_ast else None

    c = _to_col_if_str(column, "cast")
    ans = c.cast(to, _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def try_cast(
    column: ColumnOrName, to: Union[str, DataType], _emit_ast: bool = True
) -> Column:
    """A special version of CAST for a subset of data type conversions.
    It performs the same operation (i.e. converts a value of one data type into another data type),
    but returns a NULL value instead of raising an error when the conversion can not be performed.

    The ``column`` argument must be a string column in Snowflake.

    Example::

        >>> from snowflake.snowpark.types import IntegerType, FloatType
        >>> df = session.create_dataframe(['0', '-12', '22', '1001'], schema=["a"])
        >>> df.select(try_cast(col("a"), IntegerType()).as_('ans')).collect()
        [Row(ANS=0), Row(ANS=-12), Row(ANS=22), Row(ANS=1001)]

    Example::

        >>> df = session.create_dataframe(['0.12', 'USD 27.90', '13.97 USD', '€97.0', '17,-'], schema=["a"])
        >>> df.select(try_cast(col("a"), FloatType()).as_('ans')).collect()
        [Row(ANS=0.12), Row(ANS=None), Row(ANS=None), Row(ANS=None), Row(ANS=None)]

    """
    # AST.
    ast = build_function_expr("try_cast", [column, to]) if _emit_ast else None

    c = _to_col_if_str(column, "try_cast")
    ans = c.try_cast(to, _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def as_decimal(
    variant: ColumnOrName,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    _emit_ast: bool = True,
) -> Column:
    """Casts a VARIANT value to a fixed-point decimal (does not match floating-point values).

    Example::
        >>> df = session.sql("select 1.2345::variant as a")
        >>> df.select(as_decimal("a", 4, 1).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.2       |
        ------------
        <BLANKLINE>
    """
    # AST.
    ast = (
        build_function_expr(
            "as_decimal",
            [variant]
            + ([] if precision is None else [precision])
            + ([] if scale is None else [scale]),
        )
        if _emit_ast
        else None
    )

    cast_type = "as_decimal"
    c = _to_col_if_str(variant, cast_type)
    if scale and not precision:
        raise ValueError("Cannot define scale without precision")
    if precision and scale:
        return _call_function(
            cast_type,
            c,
            lit(precision, _emit_ast=False),
            lit(scale, _emit_ast=False),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
    elif precision:
        return _call_function(
            cast_type, c, lit(precision, _emit_ast=False), _ast=ast, _emit_ast=_emit_ast
        )
    else:
        return _call_function(cast_type, c, _ast=ast, _emit_ast=_emit_ast)


as_number = as_decimal


@publicapi
def as_double(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a floating-point value.

    Example::
        >>> df = session.sql("select 1.2345::variant as a")
        >>> df.select(as_double("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.2345    |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(variant, "as_double")
    return _call_function("as_double", c, _emit_ast=_emit_ast)


@publicapi
def as_real(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a floating-point value.

    Example::

        >>> from snowflake.snowpark.types import VariantType, StructType, StructField, DoubleType
        >>> schema=StructType([StructField("radius", DoubleType()),  StructField("radius_v", VariantType())])
        >>> df = session.create_dataframe(data=[[2.0, None]], schema=schema)
        >>> df.radius_v = to_variant(df.radius)
        >>> df.select(df.radius_v.as_("radius_v"), df.radius).collect()
        [Row(RADIUS_V='2.000000000000000e+00', RADIUS=2.0)]
        >>> df.select(as_real(df.radius_v).as_("real_radius_v"), df.radius).collect()
        [Row(REAL_RADIUS_V=2.0, RADIUS=2.0)]
    """
    c = _to_col_if_str(variant, "as_real")
    return _call_function("as_real", c, _emit_ast=_emit_ast)


@publicapi
def as_integer(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to an integer.

    Example::
        >>> df = session.sql("select 1.2345::variant as a")
        >>> df.select(as_integer("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(variant, "as_integer")
    return _call_function("as_integer", c, _emit_ast=_emit_ast)


@publicapi
def as_object(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to an object.

    Example::
        >>> df = session.sql("select object_construct('A',1,'B','BBBB')::variant as a")
        >>> df.select(as_object("a").alias("result")).show()
        -----------------
        |"RESULT"       |
        -----------------
        |{              |
        |  "A": 1,      |
        |  "B": "BBBB"  |
        |}              |
        -----------------
        <BLANKLINE>
    """
    c = _to_col_if_str(variant, "as_object")
    return _call_function("as_object", c, _emit_ast=_emit_ast)


@publicapi
def as_time(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a time value.

    Example::

        >>> from snowflake.snowpark.functions import as_time, to_variant
        >>> df = session.sql("select TO_TIME('12:34:56') as alarm")
        >>> df.alarm_v = to_variant(df.alarm)
        >>> df.select(df.alarm_v.as_("alarm")).collect() == df.select(df.alarm).collect()
        False
        >>> df.select(as_time(df.alarm_v).as_("alarm")).collect() == df.select(df.alarm).collect()
        True
    """
    c = _to_col_if_str(variant, "as_time")
    return _call_function("as_time", c, _emit_ast=_emit_ast)


@publicapi
def as_timestamp_ltz(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with a local timezone.

    Example::

        >>> from snowflake.snowpark.functions import as_timestamp_ltz, to_variant
        >>> df = session.sql("select TO_TIMESTAMP_LTZ('2018-10-10 12:34:56') as alarm")
        >>> df.alarm_v = to_variant(df.alarm)
        >>> df.select(df.alarm_v.as_("alarm")).collect() == df.select(df.alarm).collect()
        False
        >>> df.select(as_timestamp_ltz(df.alarm_v).as_("alarm")).collect() == df.select(df.alarm).collect()
        True
    """
    c = _to_col_if_str(variant, "as_timestamp_ltz")
    return _call_function("as_timestamp_ltz", c, _emit_ast=_emit_ast)


@publicapi
def as_timestamp_ntz(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with no timezone.

    Example::

        >>> from snowflake.snowpark.functions import as_timestamp_ntz, to_variant
        >>> df = session.sql("select TO_TIMESTAMP_NTZ('2018-10-10 12:34:56') as alarm")
        >>> df.alarm_v = to_variant(df.alarm)
        >>> df.select(df.alarm_v.as_("alarm")).collect() == df.select(df.alarm).collect()
        False
        >>> df.select(as_timestamp_ntz(df.alarm_v).as_("alarm")).collect() == df.select(df.alarm).collect()
        True
    """
    c = _to_col_if_str(variant, "as_timestamp_ntz")
    return _call_function("as_timestamp_ntz", c, _emit_ast=_emit_ast)


@publicapi
def as_timestamp_tz(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Casts a VARIANT value to a TIMESTAMP with a timezone.

    Example::

        >>> from snowflake.snowpark.functions import as_timestamp_tz, to_variant
        >>> df = session.sql("select TO_TIMESTAMP_TZ('2018-10-10 12:34:56 +0000') as alarm")
        >>> df.alarm_v = to_variant(df.alarm)
        >>> df.select(df.alarm_v.as_("alarm")).collect() == df.select(df.alarm).collect()
        False
        >>> df.select(as_timestamp_tz(df.alarm_v).as_("alarm")).collect() == df.select(df.alarm).collect()
        True
    """
    c = _to_col_if_str(variant, "as_timestamp_tz")
    return _call_function("as_timestamp_tz", c, _emit_ast=_emit_ast)


@publicapi
def to_binary(
    e: ColumnOrName, fmt: Optional[str] = None, _emit_ast: bool = True
) -> Column:
    """Converts the input expression to a binary value. For NULL input, the output is NULL.

    Example::

        >>> df = session.create_dataframe(['00', '67', '0312'], schema=['a'])
        >>> df.select(to_binary(col('a')).as_('ans')).collect()
        [Row(ANS=bytearray(b'\\x00')), Row(ANS=bytearray(b'g')), Row(ANS=bytearray(b'\\x03\\x12'))]

        >>> df = session.create_dataframe(['aGVsbG8=', 'd29ybGQ=', 'IQ=='], schema=['a'])
        >>> df.select(to_binary(col('a'), 'BASE64').as_('ans')).collect()
        [Row(ANS=bytearray(b'hello')), Row(ANS=bytearray(b'world')), Row(ANS=bytearray(b'!'))]

        >>> df.select(to_binary(col('a'), 'UTF-8').as_('ans')).collect()
        [Row(ANS=bytearray(b'aGVsbG8=')), Row(ANS=bytearray(b'd29ybGQ=')), Row(ANS=bytearray(b'IQ=='))]
    """
    c = _to_col_if_str(e, "to_binary")
    return (
        _call_function("to_binary", c, fmt, _emit_ast=_emit_ast)
        if fmt
        else _call_function("to_binary", c, _emit_ast=_emit_ast)
    )


@publicapi
def to_array(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts any value to an ARRAY value or NULL (if input is NULL).

    Example::

        >>> df = session.create_dataframe([1, 2, 3, 4], schema=['a'])
        >>> df.select(to_array(col('a')).as_('ans')).collect()
        [Row(ANS='[\\n  1\\n]'), Row(ANS='[\\n  2\\n]'), Row(ANS='[\\n  3\\n]'), Row(ANS='[\\n  4\\n]')]


        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3]), Row(a=None)])
        >>> df.select(to_array(col('a')).as_('ans')).collect()
        [Row(ANS='[\\n  1,\\n  2,\\n  3\\n]'), Row(ANS=None)]
    """
    c = _to_col_if_str(e, "to_array")
    return _call_function("to_array", c, _emit_ast=_emit_ast)


@publicapi
def to_json(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts any VARIANT value to a string containing the JSON representation of the
    value. If the input is NULL, the result is also NULL.

    Example::
        >>> from snowflake.snowpark.types import VariantType, StructField, StructType
        >>> from snowflake.snowpark import Row
        >>> schema = StructType([StructField("a", VariantType())])
        >>> df = session.create_dataframe([Row(a=None),Row(a=12),Row(a=3.141),Row(a={'a':10,'b':20}),Row(a=[1,23,456])], schema=schema)
        >>> df.select(to_json(col("a")).as_('ans')).collect()
        [Row(ANS=None), Row(ANS='12'), Row(ANS='3.141'), Row(ANS='{"a":10,"b":20}'), Row(ANS='[1,23,456]')]
    """
    c = _to_col_if_str(e, "to_json")
    return _call_function("to_json", c, _emit_ast=_emit_ast)


@publicapi
def to_object(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts any value to a OBJECT value or NULL (if input is NULL).

    Example::
        >>> from snowflake.snowpark.types import VariantType, StructField, StructType
        >>> from snowflake.snowpark import Row
        >>> schema = StructType([StructField("a", VariantType())])
        >>> df = session.create_dataframe(["{'a':10,'b':20}", None], schema=schema)
        >>> df.select(to_object(col("a")).as_('ans')).collect()
        [Row(ANS='{\\n  "a": 10,\\n  "b": 20\\n}'), Row(ANS=None)]
    """
    c = _to_col_if_str(e, "to_object")
    return _call_function("to_object", c, _emit_ast=_emit_ast)


@publicapi
def to_variant(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts any value to a VARIANT value or NULL (if input is NULL).

    Example::

        >>> df = session.create_dataframe([1, 2, 3, 4], schema=['a'])
        >>> df_conv = df.select(to_variant(col("a")).as_("ans")).sort("ans")
        >>> df_conv.collect()
        [Row(ANS='1'), Row(ANS='2'), Row(ANS='3'), Row(ANS='4')]

        After conversion via to_variant, another variant dataframe can be merged.

        >>> from snowflake.snowpark.types import VariantType, StructField, StructType
        >>> from snowflake.snowpark import Row
        >>> schema = StructType([StructField("a", VariantType())])
        >>> df_other = session.create_dataframe([Row(a=10), Row(a='test'), Row(a={'a': 10, 'b': 20}), Row(a=[1, 2, 3])], schema=schema)
        >>> df_conv.union(df_other).select(typeof(col("ans")).as_("ans")).sort("ans").collect()
        [Row(ANS='ARRAY'), Row(ANS='INTEGER'), Row(ANS='INTEGER'), Row(ANS='INTEGER'), Row(ANS='INTEGER'), Row(ANS='INTEGER'), Row(ANS='OBJECT'), Row(ANS='VARCHAR')]
    """
    c = _to_col_if_str(e, "to_variant")
    return _call_function("to_variant", c, _emit_ast=_emit_ast)


@publicapi
def to_xml(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Converts any VARIANT value to a string containing the XML representation of the
    value. If the input is NULL, the result is also NULL.

    Example::
        >>> from snowflake.snowpark.types import VariantType, StructField, StructType
        >>> from snowflake.snowpark import Row
        >>> schema = StructType([StructField("a", VariantType())])
        >>> df = session.create_dataframe([Row(a=10), Row(a='test'), Row(a={'a': 10, 'b': 20}), Row(a=[1, 2, 3])], schema=schema)
        >>> df.select(to_xml(col("A")).as_("ans")).collect()
        [Row(ANS='<SnowflakeData type="INTEGER">10</SnowflakeData>'), Row(ANS='<SnowflakeData type="VARCHAR">test</SnowflakeData>'), Row(ANS='<SnowflakeData type="OBJECT"><a type="INTEGER">10</a><b type="INTEGER">20</b></SnowflakeData>'), Row(ANS='<SnowflakeData type="ARRAY"><e type="INTEGER">1</e><e type="INTEGER">2</e><e type="INTEGER">3</e></SnowflakeData>')]
    """
    c = _to_col_if_str(e, "to_xml")
    return _call_function("to_xml", c, _emit_ast=_emit_ast)


@publicapi
def get_ignore_case(
    obj: ColumnOrName, field: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Extracts a field value from an object. Returns NULL if either of the arguments is NULL.
    This function is similar to :meth:`get` but applies case-insensitive matching to field names.

    Examples::

        >>> df = session.create_dataframe([{"a": {"aa": 1, "bb": 2, "cc": 3}}])
        >>> df.select(get_ignore_case(df["a"], lit("AA")).alias("get_ignore_case")).collect()
        [Row(GET_IGNORE_CASE='1')]
    """
    c1 = _to_col_if_str(obj, "get_ignore_case")
    c2 = _to_col_if_str(field, "get_ignore_case")
    return _call_function("get_ignore_case", c1, c2, _emit_ast=_emit_ast)


@publicapi
def object_keys(obj: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns an array containing the list of keys in the input object.


    Example::
        >>> from snowflake.snowpark.functions import lit
        >>> df = session.sql(
        ...     "select object_construct(a,b,c,d,e,f) as obj, k, v from "
        ...     "values('age', 21, 'zip', 21021, 'name', 'Joe', 'age', 0),"
        ...     "('age', 26, 'zip', 94021, 'name', 'Jay', 'age', 0) as T(a,b,c,d,e,f,k,v)"
        ... )
        >>> df.select(object_keys(col("obj")).alias("result")).show()
        -------------
        |"RESULT"   |
        -------------
        |[          |
        |  "age",   |
        |  "name",  |
        |  "zip"    |
        |]          |
        |[          |
        |  "age",   |
        |  "name",  |
        |  "zip"    |
        |]          |
        -------------
        <BLANKLINE>
    """
    c = _to_col_if_str(obj, "object_keys")
    return _call_function("object_keys", c, _emit_ast=_emit_ast)


@publicapi
def xmlget(
    xml: ColumnOrName,
    tag: ColumnOrName,
    instance_num: Union[ColumnOrName, int] = 0,
    _emit_ast: bool = True,
) -> Column:
    """Extracts an XML element object (often referred to as simply a tag) from a content of outer
    XML element object by the name of the tag and its instance number (counting from 0).

    The following example returns the first inner level (level2) from the XML object created via `parse_xml`.

    Example::

        >>> df = session.create_dataframe(['<level1 attr1="a">1<level2 attr2="b">2<level3>3a</level3><level3>3b</level3></level2></level1>'], schema=["str"]).select(parse_xml("str").as_("obj"))
        >>> df.collect()
        [Row(OBJ='<level1 attr1="a">\\n  1\\n  <level2 attr2="b">\\n    2\\n    <level3>3a</level3>\\n    <level3>3b</level3>\\n  </level2>\\n</level1>')]
        >>> df.select(xmlget("obj", lit("level2")).as_("ans")).collect()
        [Row(ANS='<level2 attr2="b">\\n  2\\n  <level3>3a</level3>\\n  <level3>3b</level3>\\n</level2>')]

    When multiple tags exist at a level, instance_num can be used to distinguish which element to return.

    Example::

        >>> df.select(xmlget(xmlget("obj", lit("level2")), lit("level3"), lit(0)).as_("ans")).collect()
        [Row(ANS='<level3>3a</level3>')]
        >>> df.select(xmlget(xmlget("obj", lit("level2")), lit("level3"), lit(1)).as_("ans")).collect()
        [Row(ANS='<level3>3b</level3>')]
        >>> df.select(xmlget("obj", lit("level2"), lit(5)).as_("ans")).collect()
        [Row(ANS=None)]

    In order to get the tagname, the value of an attribute or the content within a tag the `get` function can be used.

    Example::

        >>> df.select(get(xmlget("obj", lit("level2")), lit("@")).as_("ans")).collect()
        [Row(ANS='"level2"')]
        >>> df.select(get(xmlget("obj", lit("level2")), lit("$")).as_("ans")).collect()
        [Row(ANS='[\\n  2,\\n  {\\n    "$": "3a",\\n    "@": "level3"\\n  },\\n  {\\n    "$": "3b",\\n    "@": "level3"\\n  }\\n]')]
        >>> df.select(get(xmlget(xmlget("obj", lit("level2")), lit("level3")), lit("$")).as_("ans")).collect()
        [Row(ANS='"3a"')]
        >>> df.select(get(xmlget("obj", lit("level2")), lit("@attr2")).as_("ans")).collect()
        [Row(ANS='"b"')]
    """
    c1 = _to_col_if_str(xml, "xmlget")
    c2 = _to_col_if_str(tag, "xmlget")
    c3 = (
        instance_num
        if isinstance(instance_num, int)
        else _to_col_if_str(instance_num, "xmlget")
    )
    return _call_function("xmlget", c1, c2, c3, _emit_ast=_emit_ast)


@publicapi
def get_path(col: ColumnOrName, path: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Extracts a value from semi-structured data using a path name.

    Examples::

        >>> df = session.create_dataframe([{"a": {"aa": {"dd": 4}, "bb": 2, "cc": 3}}])
        >>> df.select(get_path(df["a"], lit("aa.dd")).alias("get_path")).collect()
        [Row(GET_PATH='4')]
    """
    c1 = _to_col_if_str(col, "get_path")
    c2 = _to_col_if_str(path, "get_path")
    return _call_function("get_path", c1, c2, _emit_ast=_emit_ast)


@publicapi
def get(
    col1: Union[ColumnOrName, int],
    col2: Union[ColumnOrName, int],
    _emit_ast: bool = True,
) -> Column:
    """Extracts a value from an object or array; returns NULL if either of the arguments is NULL.

    Example::

        >>> from snowflake.snowpark.functions import lit
        >>> df = session.createDataFrame([({"a": 1.0, "b": 2.0}, [1, 2, 3],), ({}, [],)], ["map", "list"])
        >>> df.select(get(df.list, 1).as_("idx1")).sort(col("idx1")).show()
        ----------
        |"IDX1"  |
        ----------
        |NULL    |
        |2       |
        ----------
        <BLANKLINE>

        >>> df.select(get(df.map, lit("a")).as_("get_a")).sort(col("get_a")).show()
        -----------
        |"GET_A"  |
        -----------
        |NULL     |
        |1        |
        -----------
        <BLANKLINE>"""
    c1 = _to_col_if_str_or_int(col1, "get")
    c2 = _to_col_if_str_or_int(col2, "get")
    return _call_function("get", c1, c2, _emit_ast=_emit_ast)


element_at = get


@publicapi
def when(
    condition: ColumnOrSqlExpr, value: ColumnOrLiteral, _emit_ast: bool = True
) -> CaseExpr:
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

    Example::

        >>> df = session.create_dataframe([1, None, 2, 3, None, 5, 6], schema=["a"])
        >>> df.collect()
        [Row(A=1), Row(A=None), Row(A=2), Row(A=3), Row(A=None), Row(A=5), Row(A=6)]
        >>> df.select(when(col("a") % 2 == 0, lit("even")).as_("ans")).collect()
        [Row(ANS=None), Row(ANS=None), Row(ANS='even'), Row(ANS=None), Row(ANS=None), Row(ANS=None), Row(ANS='even')]

    Multiple when statements can be changed and `otherwise`/`else_` used to create expressions similar to ``CASE WHEN ... ELSE ... END`` in SQL.

    Example::

        >>> df.select(when(col("a") % 2 == 0, lit("even")).when(col("a") % 2 == 1, lit("odd")).as_("ans")).collect()
        [Row(ANS='odd'), Row(ANS=None), Row(ANS='even'), Row(ANS='odd'), Row(ANS=None), Row(ANS='odd'), Row(ANS='even')]
        >>> df.select(when(col("a") % 2 == 0, lit("even")).when(col("a") % 2 == 1, lit("odd")).otherwise(lit("unknown")).as_("ans")).collect()
        [Row(ANS='odd'), Row(ANS='unknown'), Row(ANS='even'), Row(ANS='odd'), Row(ANS='unknown'), Row(ANS='odd'), Row(ANS='even')]
    """

    # AST.
    ast = None
    if _emit_ast:
        ast = proto.Expr()
        expr = with_src_position(ast.column_case_expr)
        case_expr = with_src_position(expr.cases.add())
        build_expr_from_snowpark_column_or_sql_str(case_expr.condition, condition)
        build_expr_from_snowpark_column_or_python_val(case_expr.value, value)

    return CaseExpr(
        CaseWhen(
            [
                (
                    _to_col_if_sql_expr(condition, "when")._expression,
                    Column._to_expr(value),
                )
            ]
        ),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def iff(
    condition: ColumnOrSqlExpr,
    expr1: ColumnOrLiteral,
    expr2: ColumnOrLiteral,
    _emit_ast: bool = True,
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

    Examples::

        >>> df = session.create_dataframe([True, False, None], schema=["a"])
        >>> df.select(iff(df["a"], lit("true"), lit("false")).alias("iff")).collect()
        [Row(IFF='true'), Row(IFF='false'), Row(IFF='false')]
    """
    ast = build_function_expr("iff", [condition, expr1, expr2]) if _emit_ast else None
    return _call_function(
        "iff",
        _to_col_if_sql_expr(condition, "iff"),
        expr1,
        expr2,
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def in_(
    cols: List[ColumnOrName],
    *vals: Union[
        "snowflake.snowpark.DataFrame",
        LiteralType,
        Column,
        Iterable[LiteralType],
        Iterable[Column],
    ],
    _emit_ast: bool = True,
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
        vals: A list containing the values or columns, or a Snowpark DataFrame to compare for the IN operation.
    """

    # AST.
    ast = None
    if _emit_ast:
        # MultipleExpression uses _expression field from columns, which will drop the column info/its ast.
        # Fix here by constructing ast based on current column expressions.
        list_arg = proto.Expr()
        list_ast = with_src_position(list_arg.list_val)
        for col in cols:
            col_ast = list_ast.vs.add()
            build_expr_from_snowpark_column_or_python_val(col_ast, col)

        values_args = []
        for val in vals:
            val_ast = proto.Expr()
            if isinstance(val, snowflake.snowpark.dataframe.DataFrame):
                val._set_ast_ref(val_ast)
            else:
                build_expr_from_snowpark_column_or_python_val(val_ast, val)
            values_args.append(val_ast)

        ast = proto.Expr()
        build_builtin_fn_apply(ast, "in_", list_arg, *values_args)

    vals = parse_positional_args_to_list(*vals)
    columns = [_to_col_if_str(c, "in_") for c in cols]
    ans = Column(
        MultipleExpression([c._expression for c in columns]), _emit_ast=False
    ).in_(vals, _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def cume_dist(_emit_ast: bool = True) -> Column:
    """
    Finds the cumulative distribution of a value with regard to other values
    within the same window partition.

    Example:

        >>> from snowflake.snowpark.window import Window
        >>> from snowflake.snowpark.types import DecimalType
        >>> df = session.create_dataframe([[1, 2], [1, 2], [1,3], [4, 5], [2, 3], [3, 4], [4, 7], [3,7], [4,5]], schema=["a", "b"])
        >>> df.select(cume_dist().over(Window.order_by("a")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.333     |
        |0.333     |
        |0.333     |
        |0.444     |
        |0.667     |
        |0.667     |
        |1.000     |
        |1.000     |
        |1.000     |
        ------------
        <BLANKLINE>
    """
    return _call_function("cume_dist", _emit_ast=_emit_ast)


@publicapi
def rank(_emit_ast: bool = True) -> Column:
    """
    Returns the rank of a value within an ordered group of values.
    The rank value starts at 1 and continues up.

    Example::
        >>> from snowflake.snowpark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(rank().over(Window.partition_by(col("X")).order_by(col("Y"))).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        |2         |
        |2         |
        |1         |
        |1         |
        ------------
        <BLANKLINE>
    """
    return _call_function("rank", _emit_ast=_emit_ast)


@publicapi
def percent_rank(_emit_ast: bool = True) -> Column:
    """
    Returns the relative rank of a value within a group of values, specified as a percentage
    ranging from 0.0 to 1.0.

    Example::
        >>> from snowflake.snowpark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(percent_rank().over(Window.partition_by("x").order_by(col("y"))).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.0       |
        |0.5       |
        |0.5       |
        |0.0       |
        |0.0       |
        ------------
        <BLANKLINE>
    """
    return _call_function("percent_rank", _emit_ast=_emit_ast)


@publicapi
def dense_rank(_emit_ast: bool = True) -> Column:
    """
    Returns the rank of a value within a group of values, without gaps in the ranks.
    The rank value starts at 1 and continues up sequentially.
    If two values are the same, they will have the same rank.

    Example::

        >>> from snowflake.snowpark.window import Window
        >>> window = Window.order_by("key")
        >>> df = session.create_dataframe([(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"])
        >>> df.select(dense_rank().over(window).as_("dense_rank")).collect()
        [Row(DENSE_RANK=1), Row(DENSE_RANK=1), Row(DENSE_RANK=2), Row(DENSE_RANK=2)]
    """
    return _call_function("dense_rank", _emit_ast=_emit_ast)


@publicapi
def row_number(_emit_ast: bool = True) -> Column:
    """
    Returns a unique row number for each row within a window partition.
    The row number starts at 1 and continues up sequentially.

    Example::

        >>> from snowflake.snowpark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(row_number().over(Window.partition_by(col("X")).order_by(col("Y"))).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        |2         |
        |3         |
        |1         |
        |2         |
        ------------
        <BLANKLINE>
    """
    return _call_function("row_number", _emit_ast=_emit_ast)


@publicapi
def lag(
    e: ColumnOrName,
    offset: int = 1,
    default_value: Optional[ColumnOrLiteral] = None,
    ignore_nulls: bool = False,
    _emit_ast: bool = True,
) -> Column:
    """
    Accesses data in a previous row in the same result set without having to
    join the table to itself.

    Example::

        >>> from snowflake.snowpark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(lag("Z").over(Window.partition_by(col("X")).order_by(col("Y"))).alias("result")).collect()
        [Row(RESULT=None), Row(RESULT=10), Row(RESULT=1), Row(RESULT=None), Row(RESULT=1)]
    """
    # AST.
    ast = (
        build_function_expr("lag", [e, offset, default_value, ignore_nulls])
        if _emit_ast
        else None
    )

    c = _to_col_if_str(e, "lag")

    ans = Column(
        Lag(c._expression, offset, Column._to_expr(default_value), ignore_nulls),
        _emit_ast=False,
    )
    ans._ast = ast
    return ans


@publicapi
def lead(
    e: ColumnOrName,
    offset: int = 1,
    default_value: Optional[Union[Column, LiteralType]] = None,
    ignore_nulls: bool = False,
    _emit_ast: bool = True,
) -> Column:
    """
    Accesses data in a subsequent row in the same result set without having to
    join the table to itself.

    Example::

        >>> from snowflake.snowpark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(lead("Z").over(Window.partition_by(col("X")).order_by(col("Y"))).alias("result")).collect()
        [Row(RESULT=1), Row(RESULT=3), Row(RESULT=None), Row(RESULT=3), Row(RESULT=None)]
    """
    # AST.
    ast = (
        build_function_expr("lead", [e, offset, default_value, ignore_nulls])
        if _emit_ast
        else None
    )

    c = _to_col_if_str(e, "lead")

    ans = Column(
        Lead(c._expression, offset, Column._to_expr(default_value), ignore_nulls),
        _emit_ast=False,
    )
    ans._ast = ast
    return ans


@publicapi
def last_value(
    e: ColumnOrName, ignore_nulls: bool = False, _emit_ast: bool = True
) -> Column:
    """
    Returns the last value within an ordered group of values.

    Example::

        >>> from snowflake.snowpark.window import Window
        >>> window = Window.partition_by("column1").order_by("column2")
        >>> df = session.create_dataframe([[1, 10], [1, 11], [2, 20], [2, 21]], schema=["column1", "column2"])
        >>> df.select(df["column1"], df["column2"], last_value(df["column2"]).over(window).as_("column2_last")).collect()
        [Row(COLUMN1=1, COLUMN2=10, COLUMN2_LAST=11), Row(COLUMN1=1, COLUMN2=11, COLUMN2_LAST=11), Row(COLUMN1=2, COLUMN2=20, COLUMN2_LAST=21), Row(COLUMN1=2, COLUMN2=21, COLUMN2_LAST=21)]
    """
    # AST.
    ast = build_function_expr("last_value", [e, ignore_nulls]) if _emit_ast else None

    c = _to_col_if_str(e, "last_value")

    ans = Column(LastValue(c._expression, None, None, ignore_nulls), _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def first_value(
    e: ColumnOrName, ignore_nulls: bool = False, _emit_ast: bool = True
) -> Column:
    """
    Returns the first value within an ordered group of values.

    Example::

        >>> from snowflake.snowpark.window import Window
        >>> window = Window.partition_by("column1").order_by("column2")
        >>> df = session.create_dataframe([[1, 10], [1, 11], [2, 20], [2, 21]], schema=["column1", "column2"])
        >>> df.select(df["column1"], df["column2"], first_value(df["column2"]).over(window).as_("column2_first")).collect()
        [Row(COLUMN1=1, COLUMN2=10, COLUMN2_FIRST=10), Row(COLUMN1=1, COLUMN2=11, COLUMN2_FIRST=10), Row(COLUMN1=2, COLUMN2=20, COLUMN2_FIRST=20), Row(COLUMN1=2, COLUMN2=21, COLUMN2_FIRST=20)]
    """
    # AST.
    ast = build_function_expr("first_value", [e, ignore_nulls]) if _emit_ast else None

    c = _to_col_if_str(e, "first_value")

    ans = Column(FirstValue(c._expression, None, None, ignore_nulls), _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def nth_value(
    e: ColumnOrName, n: int, ignore_nulls: bool = False, _emit_ast: bool = True
) -> Column:
    """
    Returns the nth value within an ordered group of values.

    Example::

        >>> from snowflake.snowpark.window import Window
        >>> window = Window.partition_by("column1").order_by("column2")
        >>> df = session.create_dataframe([[1, 10], [1, 11], [2, 20], [2, 21]], schema=["column1", "column2"])
        >>> df.select(df["column1"], df["column2"], nth_value(df["column2"], 2).over(window).as_("column2_2nd")).collect()
        [Row(COLUMN1=1, COLUMN2=10, COLUMN2_2ND=11), Row(COLUMN1=1, COLUMN2=11, COLUMN2_2ND=11), Row(COLUMN1=2, COLUMN2=20, COLUMN2_2ND=21), Row(COLUMN1=2, COLUMN2=21, COLUMN2_2ND=21)]
    """
    # AST.
    ast = build_function_expr("nth_value", [e, n, ignore_nulls]) if _emit_ast else None

    c = _to_col_if_str(e, "nth_value")

    ans = Column(NthValue(c._expression, n, None, ignore_nulls), _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def ntile(e: Union[int, ColumnOrName], _emit_ast: bool = True) -> Column:
    """
    Divides an ordered data set equally into the number of buckets specified by n.
    Buckets are sequentially numbered 1 through n.

    Args:
        e: The desired number of buckets; must be a positive integer value.

    Example::

        >>> from snowflake.snowpark.window import Window
        >>> df = session.create_dataframe(
        ...     [["C", "SPY", 3], ["C", "AAPL", 10], ["N", "SPY", 5], ["N", "AAPL", 7], ["Q", "MSFT", 3]],
        ...     schema=["exchange", "symbol", "shares"]
        ... )
        >>> df.select(col("exchange"), col("symbol"), ntile(3).over(Window.partition_by("exchange").order_by("shares")).alias("ntile_3")).order_by(["exchange","symbol"]).show()
        -------------------------------------
        |"EXCHANGE"  |"SYMBOL"  |"NTILE_3"  |
        -------------------------------------
        |C           |AAPL      |2          |
        |C           |SPY       |1          |
        |N           |AAPL      |2          |
        |N           |SPY       |1          |
        |Q           |MSFT      |1          |
        -------------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str_or_int(e, "ntile")
    return _call_function("ntile", c, _emit_ast=_emit_ast)


@publicapi
def percentile_cont(percentile: float, _emit_ast: bool = True) -> Column:
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
    return _call_function("percentile_cont", percentile, _emit_ast=_emit_ast)


@publicapi
def greatest(*columns: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the largest value from a list of expressions.
    If any of the argument values is NULL, the result is NULL.
    GREATEST supports all data types, including VARIANT.

    Examples::

        >>> df = session.create_dataframe([[1, 2, 3], [2, 4, -1], [3, 6, None]], schema=["a", "b", "c"])
        >>> df.select(greatest(df["a"], df["b"], df["c"]).alias("greatest")).collect()
        [Row(GREATEST=3), Row(GREATEST=4), Row(GREATEST=None)]
    """
    c = [_to_col_if_str(ex, "greatest") for ex in columns]
    return _call_function("greatest", *c, _emit_ast=_emit_ast)


@publicapi
def least(*columns: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the smallest value from a list of expressions.
    If any of the argument values is NULL, the result is NULL.
    LEAST supports all data types, including VARIANT.

    Example::

        >>> df = session.create_dataframe([[1, 2, 3], [2, 4, -1], [3, 6, None]], schema=["a", "b", "c"])
        >>> df.select(least(df["a"], df["b"], df["c"]).alias("least")).collect()
        [Row(LEAST=1), Row(LEAST=-1), Row(LEAST=None)]
    """
    c = [_to_col_if_str(ex, "least") for ex in columns]
    return _call_function("least", *c, _emit_ast=_emit_ast)


@publicapi
def listagg(
    e: ColumnOrName,
    delimiter: str = "",
    is_distinct: bool = False,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the concatenated input values, separated by `delimiter` string.
    See `LISTAGG <https://docs.snowflake.com/en/sql-reference/functions/listagg.html>`_ for details.

    Args:
        e: A :class:`Column` object or column name that determines the values
            to be put into the list.
        delimiter: A string delimiter.
        is_distinct: Whether the input expression is distinct.

    Examples::

        >>> df = session.create_dataframe([1, 2, 3, 2, 4, 5], schema=["col"])
        >>> df.select(listagg("col", ",").within_group(df["col"].asc()).as_("result")).collect()
        [Row(RESULT='1,2,2,3,4,5')]
    """
    # AST.
    ast = (
        build_function_expr("listagg", [e, delimiter, is_distinct])
        if _emit_ast
        else None
    )

    c = _to_col_if_str(e, "listagg")

    ans = Column(ListAgg(c._expression, delimiter, is_distinct), _emit_ast=False)
    ans._ast = ast
    return ans


@publicapi
def when_matched(
    condition: Optional[Column] = None, _emit_ast: bool = True
) -> "snowflake.snowpark.table.WhenMatchedClause":
    """
    Specifies a matched clause for the :meth:`Table.merge <snowflake.snowpark.Table.merge>` action.
    See :class:`~snowflake.snowpark.table.WhenMatchedClause` for details.

    Convenience function to create a new WhenMatchedClause instance which is required together with an action when merging
    a Snowpark table with a Snowpark DataFrame (see snowflake.snowpark.Table.merge for more details).

    Example::

        >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
        >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
        >>> target = session.table("my_table")
        >>> source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=["key", "value"])
        >>> target.merge(source, (target["key"] == source["key"]) & (target["value"] == "too_old"),
        ...              [when_matched().update({"value": source["value"]})])
        MergeResult(rows_inserted=0, rows_updated=1, rows_deleted=0)
        >>> target.collect()
        [Row(KEY=10, VALUE='old'), Row(KEY=10, VALUE='new'), Row(KEY=11, VALUE='old')]

    """
    return snowflake.snowpark.table.WhenMatchedClause(condition, _emit_ast=_emit_ast)


@publicapi
def when_not_matched(
    condition: Optional[Column] = None, _emit_ast: bool = True
) -> "snowflake.snowpark.table.WhenNotMatchedClause":
    """
    Specifies a not-matched clause for the :meth:`Table.merge <snowflake.snowpark.Table.merge>` action.
    See :class:`~snowflake.snowpark.table.WhenNotMatchedClause` for details.

    Convenience function to create a new WhenNotMatchedClause instance which is required together with an action when merging
    a Snowpark table with a Snowpark DataFrame (see snowflake.snowpark.Table.merge for more details).

    Example::

        >>> from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
        >>> schema = StructType([StructField("key", IntegerType()), StructField("value", StringType())])
        >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=schema)
        >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
        >>> target = session.table("my_table")
        >>> source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=schema)
        >>> target.merge(source, (target["key"] == source["key"]) & (target["value"] == "too_old"),
        ...              [when_not_matched().insert({"key": source["key"]})])
        MergeResult(rows_inserted=2, rows_updated=0, rows_deleted=0)
        >>> target.sort(col("key"), col("value")).collect()
        [Row(KEY=10, VALUE='old'), Row(KEY=10, VALUE='too_old'), Row(KEY=11, VALUE='old'), Row(KEY=12, VALUE=None), Row(KEY=13, VALUE=None)]
    """
    return snowflake.snowpark.table.WhenNotMatchedClause(condition, _emit_ast=_emit_ast)


@publicapi
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
    if_not_exists: bool = False,
    session: Optional["snowflake.snowpark.session.Session"] = None,
    parallel: int = 4,
    max_batch_size: Optional[int] = None,
    statement_params: Optional[Dict[str, str]] = None,
    source_code_display: bool = True,
    strict: bool = False,
    secure: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    comment: Optional[str] = None,
    artifact_repository: Optional[str] = None,
    artifact_repository_packages: Optional[List[str]] = None,
    resource_constraint: Optional[Dict[str, str]] = None,
    _emit_ast: bool = True,
    **kwargs,
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
            session-level packages. To use Python packages that are not available in Snowflake,
            refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.
        replace: Whether to replace a UDF that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a UDF with a name that already exists
            results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
            an existing UDF with the same name is overwritten.
        if_not_exists: Whether to skip creation of a UDF when one with the same signature already exists.
            The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
            and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDF with
            the same signature exists, the UDF creation is skipped.
        session: Use this session to register the UDF. If it's not specified, the session that you created before calling this function will be used.
            You need to specify this parameter if you have created multiple sessions before calling this method.
        parallel: The number of threads to use for uploading UDF files with the
            `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
            command. The default value is 4 and supported values are from 1 to 99.
            Increasing the number of threads can improve performance when uploading
            large UDF files.
        max_batch_size: The maximum number of rows per input pandas DataFrame or pandas Series
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
        strict: Whether the created UDF is strict. A strict UDF will not invoke the UDF if any input is
            null. Instead, a null value will always be returned for that row. Note that the UDF might
            still return null for non-null inputs.
        secure: Whether the created UDF is secure. For more information about secure functions,
            see `Secure UDFs <https://docs.snowflake.com/en/sql-reference/udf-secure.html>`_.
        external_access_integrations: The names of one or more external access integrations. Each
            integration you specify allows access to the external network locations and secrets
            the integration specifies.
        secrets: The key-value pairs of string types of secrets used to authenticate the external network location.
            The secrets can be accessed from handler code. The secrets specified as values must
            also be specified in the external access integration and the keys are strings used to
            retrieve the secrets using secret API.
        immutable: Whether the UDF result is deterministic or not for the same input.
        comment: Adds a comment for the created object. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_
        artifact_repository: The name of an artifact_repository that the ``artifact_repository_packages``
            parameter will search for packages in.
        artifact_repository_packages: A list of packages to search for within the pypi repository
            set in the above parameter.
        resource_constraint: A dictionary containing a resource properties of a warehouse and then
            constraints needed to run this function. Eg ``{"architecture": "x86"}`` requires an x86
            warehouse be used for execution.

    Returns:
        A UDF function that can be called with :class:`~snowflake.snowpark.Column` expressions.

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for UDFs in
        :class:`~snowflake.snowpark.udf.UDFRegistration`.

            - You can use use :attr:`~snowflake.snowpark.types.Variant` to
              annotate a variant, and use :attr:`~snowflake.snowpark.types.Geography`
              or :attr:`~snowflake.snowpark.types.Geometry` to annotate geospatial
              types when defining a UDF.

            - You can use use :attr:`~snowflake.snowpark.types.PandasSeries` to annotate
              a pandas Series, and use :attr:`~snowflake.snowpark.types.PandasDataFrame`
              to annotate a pandas DataFrame when defining a vectorized UDF.
              Note that they are generic types so you can specify the element type in a
              pandas Series and DataFrame.

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

    UDFs can be created as anonymous UDFs

    Example::

        >>> from snowflake.snowpark.types import IntegerType
        >>> add_one = udf(lambda x: x+1, return_type=IntegerType(), input_types=[IntegerType()])
        >>> df = session.create_dataframe([1, 2, 3], schema=["a"])
        >>> df.select(add_one(col("a")).as_("ans")).collect()
        [Row(ANS=2), Row(ANS=3), Row(ANS=4)]

    or as named UDFs that are accessible in the same session. Instead of calling `udf` as function, it can be also used
    as a decorator:

    Example::

        >>> @udf(name="minus_one", replace=True)
        ... def minus_one(x: int) -> int:
        ...     return x - 1
        >>> df.select(minus_one(col("a")).as_("ans")).collect()
        [Row(ANS=0), Row(ANS=1), Row(ANS=2)]
        >>> session.sql("SELECT minus_one(10)").collect()
        [Row(MINUS_ONE(10)=9)]

    """

    # Initial check to make sure no unexpected args are passed in
    check_decorator_args(**kwargs)

    session = snowflake.snowpark.session._get_sandbox_conditional_active_session(
        session
    )

    if session is None:
        udf_registration_method = UDFRegistration(session=session).register
    else:
        udf_registration_method = session.udf.register

    if func is None and kwargs.get("_registered_object_name") is None:
        return functools.partial(
            udf_registration_method,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            if_not_exists=if_not_exists,
            parallel=parallel,
            max_batch_size=max_batch_size,
            statement_params=statement_params,
            source_code_display=source_code_display,
            strict=strict,
            secure=secure,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            immutable=immutable,
            comment=comment,
            artifact_repository=artifact_repository,
            artifact_repository_packages=artifact_repository_packages,
            resource_constraint=resource_constraint,
            _emit_ast=_emit_ast,
            **kwargs,
        )
    else:
        return udf_registration_method(
            func,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            if_not_exists=if_not_exists,
            parallel=parallel,
            max_batch_size=max_batch_size,
            statement_params=statement_params,
            source_code_display=source_code_display,
            strict=strict,
            secure=secure,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            immutable=immutable,
            comment=comment,
            artifact_repository=artifact_repository,
            artifact_repository_packages=artifact_repository_packages,
            resource_constraint=resource_constraint,
            _emit_ast=_emit_ast,
            **kwargs,
        )


@publicapi
def udtf(
    handler: Optional[Callable] = None,
    *,
    output_schema: Union[StructType, List[str], "PandasDataFrameType"],
    input_types: Optional[List[DataType]] = None,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    if_not_exists: bool = False,
    session: Optional["snowflake.snowpark.session.Session"] = None,
    parallel: int = 4,
    statement_params: Optional[Dict[str, str]] = None,
    strict: bool = False,
    secure: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    comment: Optional[str] = None,
    artifact_repository: Optional[str] = None,
    artifact_repository_packages: Optional[List[str]] = None,
    resource_constraint: Optional[Dict[str, str]] = None,
    _emit_ast: bool = True,
    **kwargs,
) -> Union[UserDefinedTableFunction, functools.partial]:
    """Registers a Python class as a Snowflake Python UDTF and returns the UDTF.

    It can be used as either a function call or a decorator. In most cases you work with a single session.
    This function uses that session to register the UDTF. If you have multiple sessions, you need to
    explicitly specify the ``session`` parameter of this function. If you have a function and would
    like to register it to multiple databases, use ``session.udtf.register`` instead. See examples
    in :class:`~snowflake.snowpark.udtf.UDTFRegistration`.

    Args:
        handler: A Python class used for creating the UDTF.
        output_schema: A list of column names, or a :class:`~snowflake.snowpark.types.StructType` instance that represents the table function's columns, or a ``PandasDataFrameType`` instance for vectorized UDTF.
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
            :meth:`~snowflake.snowpark.Session.add_requirements`. To use Python packages that are not available
            in Snowflake, refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.
        replace: Whether to replace a UDTF that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a UDTF with a name that already exists
            results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
            an existing UDTF with the same name is overwritten.
        if_not_exists: Whether to skip creation of a UDTF when one with the same signature already exists.
            The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
            and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDTF with
            the same signature exists, the UDTF creation is skipped.
        session: Use this session to register the UDTF. If it's not specified, the session that you created before calling this function will be used.
            You need to specify this parameter if you have created multiple sessions before calling this method.
        parallel: The number of threads to use for uploading UDTF files with the
            `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
            command. The default value is 4 and supported values are from 1 to 99.
            Increasing the number of threads can improve performance when uploading
            large UDTF files.
        statement_params: Dictionary of statement level parameters to be set while executing this action.
        strict: Whether the created UDTF is strict. A strict UDTF will not invoke the UDTF if any input is
            null. Instead, a null value will always be returned for that row. Note that the UDTF might
            still return null for non-null inputs.
        secure: Whether the created UDTF is secure. For more information about secure functions,
            see `Secure UDFs <https://docs.snowflake.com/en/sql-reference/udf-secure.html>`_.
        external_access_integrations: The names of one or more external access integrations. Each
            integration you specify allows access to the external network locations and secrets
            the integration specifies.
        secrets: The key-value pairs of string types of secrets used to authenticate the external network location.
            The secrets can be accessed from handler code. The secrets specified as values must
            also be specified in the external access integration and the keys are strings used to
            retrieve the secrets using secret API.
        immutable: Whether the UDTF result is deterministic or not for the same input.
        comment: Adds a comment for the created object. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_
        artifact_repository: The name of an artifact_repository that the ``artifact_repository_packages``
            parameter will search for packages in.
        artifact_repository_packages: A list of packages to search for within the pypi repository
            set in the above parameter.
        resource_constraint: A dictionary containing a resource properties of a warehouse and then
            constraints needed to run this function. Eg ``{"architecture": "x86"}`` requires an x86
            warehouse be used for execution.

    Returns:
        A UDTF function that can be called with :class:`~snowflake.snowpark.Column` expressions.

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for UDTFs in
        :class:`~snowflake.snowpark.udtf.UDTFRegistration`.

            - You can use use :attr:`~snowflake.snowpark.types.Variant` to
              annotate a variant, and use :attr:`~snowflake.snowpark.types.Geography`
              or :attr:`~snowflake.snowpark.types.Geometry` to annotate geospatial
              types when defining a UDTF.

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


    Example::

        >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
        >>> class PrimeSieve:
        ...     def process(self, n):
        ...         is_prime = [True] * (n + 1)
        ...         is_prime[0] = False
        ...         is_prime[1] = False
        ...         p = 2
        ...         while p * p <= n:
        ...             if is_prime[p]:
        ...                 # set all multiples of p to False
        ...                 for i in range(p * p, n + 1, p):
        ...                     is_prime[i] = False
        ...             p += 1
        ...         # yield all prime numbers
        ...         for p in range(2, n + 1):
        ...             if is_prime[p]:
        ...                 yield (p,)
        >>> prime_udtf = udtf(PrimeSieve, output_schema=StructType([StructField("number", IntegerType())]), input_types=[IntegerType()])
        >>> session.table_function(prime_udtf(lit(20))).collect()
        [Row(NUMBER=2), Row(NUMBER=3), Row(NUMBER=5), Row(NUMBER=7), Row(NUMBER=11), Row(NUMBER=13), Row(NUMBER=17), Row(NUMBER=19)]

        Instead of calling `udtf` it is also possible to use udtf as a decorator.

    Example::

        >>> @udtf(name="alt_int",replace=True, output_schema=StructType([StructField("number", IntegerType())]), input_types=[IntegerType()])
        ... class Alternator:
        ...     def __init__(self):
        ...         self._positive = True
        ...
        ...     def process(self, n):
        ...         for i in range(n):
        ...             if self._positive:
        ...                 yield (1,)
        ...             else:
        ...                 yield (-1,)
        ...             self._positive = not self._positive
        >>> session.table_function("alt_int", lit(3)).collect()
        [Row(NUMBER=1), Row(NUMBER=-1), Row(NUMBER=1)]
        >>> session.table_function("alt_int", lit(2)).collect()
        [Row(NUMBER=1), Row(NUMBER=-1)]
        >>> session.table_function("alt_int", lit(1)).collect()
        [Row(NUMBER=1)]
    """

    # Initial check to make sure no unexpected args are passed in
    check_decorator_args(**kwargs)

    session = snowflake.snowpark.session._get_sandbox_conditional_active_session(
        session
    )
    if session is None:
        udtf_registration_method = UDTFRegistration(session=session).register
    else:
        udtf_registration_method = session.udtf.register

    if handler is None and kwargs.get("_registered_object_name") is None:
        return functools.partial(
            udtf_registration_method,
            output_schema=output_schema,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            if_not_exists=if_not_exists,
            parallel=parallel,
            statement_params=statement_params,
            strict=strict,
            secure=secure,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            immutable=immutable,
            comment=comment,
            artifact_repository=artifact_repository,
            artifact_repository_packages=artifact_repository_packages,
            resource_constraint=resource_constraint,
            _emit_ast=_emit_ast,
            **kwargs,
        )
    else:
        return udtf_registration_method(
            handler,
            output_schema=output_schema,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            if_not_exists=if_not_exists,
            parallel=parallel,
            statement_params=statement_params,
            strict=strict,
            secure=secure,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            immutable=immutable,
            comment=comment,
            artifact_repository=artifact_repository,
            artifact_repository_packages=artifact_repository_packages,
            resource_constraint=resource_constraint,
            _emit_ast=_emit_ast,
            **kwargs,
        )


@publicapi
def udaf(
    handler: Optional[typing.Type] = None,
    *,
    return_type: Optional[DataType] = None,
    input_types: Optional[List[DataType]] = None,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    if_not_exists: bool = False,
    session: Optional["snowflake.snowpark.session.Session"] = None,
    parallel: int = 4,
    statement_params: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    comment: Optional[str] = None,
    artifact_repository: Optional[str] = None,
    artifact_repository_packages: Optional[List[str]] = None,
    resource_constraint: Optional[Dict[str, str]] = None,
    _emit_ast: bool = True,
    **kwargs,
) -> Union[UserDefinedAggregateFunction, functools.partial]:
    """Registers a Python class as a Snowflake Python UDAF and returns the UDAF.

    It can be used as either a function call or a decorator. In most cases you work with a single session.
    This function uses that session to register the UDAF. If you have multiple sessions, you need to
    explicitly specify the ``session`` parameter of this function. If you have a function and would
    like to register it to multiple databases, use ``session.udaf.register`` instead. See examples
    in :class:`~snowflake.snowpark.udaf.UDAFRegistration`.

    Args:
        handler: A Python class used for creating the UDAF.
        return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
            type of the UDAF. Optional if type hints are provided.
        input_types: A list of :class:`~snowflake.snowpark.types.DataType`
            representing the input data types of the UDAF. Optional if
            type hints are provided.
        name: A string or list of strings that specify the name or fully-qualified
            object identifier (database name, schema name, and function name) for
            the UDAF in Snowflake, which allows you to call this UDAF in a SQL
            command or via :func:`DataFrame.agg`. If it is not provided, a name will
            be automatically generated for the UDAF. A name must be specified when
            ``is_permanent`` is ``True``.
        is_permanent: Whether to create a permanent UDAF. The default is ``False``.
            If it is ``True``, a valid ``stage_location`` must be provided.
        stage_location: The stage location where the Python file for the UDAF
            and its dependencies should be uploaded. The stage location must be specified
            when ``is_permanent`` is ``True``, and it will be ignored when
            ``is_permanent`` is ``False``. It can be any stage other than temporary
            stages and external stages.
        imports: A list of imports that only apply to this UDAF. You can use a string to
            represent a file path (similar to the ``path`` argument in
            :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
            strings to represent a file path and an import path (similar to the ``import_path``
            argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDAF-level imports
            will override the session-level imports added by
            :meth:`~snowflake.snowpark.Session.add_import`. Note that an empty list means
            no import for this UDAF, and ``None`` or not specifying this parameter means using
            session-level imports.
        packages: A list of packages that only apply to this UDAF. These UDAF-level packages
            will override the session-level packages added by
            :meth:`~snowflake.snowpark.Session.add_packages` and
            :meth:`~snowflake.snowpark.Session.add_requirements`. Note that an empty list means
            no package for this UDAF, and ``None`` or not specifying this parameter means using
            session-level packages. To use Python packages that are not available in Snowflake,
            refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.
        replace: Whether to replace a UDAF that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a UDAF with a name that already exists
            results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
            an existing UDAF with the same name is overwritten.
        if_not_exists: Whether to skip creation of a UDAF when one with the same signature already exists.
            The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
            and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDAF with
            the same signature exists, the UDAF creation is skipped.
        session: Use this session to register the UDAF. If it's not specified, the session that you created before
            calling this function will be used. You need to specify this parameter if you have created multiple
            sessions before calling this method.
        parallel: The number of threads to use for uploading UDAF files with the
            `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
            command. The default value is 4 and supported values are from 1 to 99.
            Increasing the number of threads can improve performance when uploading
            large UDAF files.
        statement_params: Dictionary of statement level parameters to be set while executing this action.
        immutable: Whether the UDAF result is deterministic or not for the same input.
        external_access_integrations: The names of one or more external access integrations. Each
            integration you specify allows access to the external network locations and secrets
            the integration specifies.
        secrets: The key-value pairs of string types of secrets used to authenticate the external network location.
            The secrets can be accessed from handler code. The secrets specified as values must
            also be specified in the external access integration and the keys are strings used to
            retrieve the secrets using secret API.
        comment: Adds a comment for the created object. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_
        artifact_repository: The name of an artifact_repository that the ``artifact_repository_packages``
            parameter will search for packages in.
        artifact_repository_packages: A list of packages to search for within the pypi repository
            set in the above parameter.
        resource_constraint: A dictionary containing a resource properties of a warehouse and then
            constraints needed to run this function. Eg ``{"architecture": "x86"}`` requires an x86
            warehouse be used for execution.

    Returns:
        A UDAF function that can be called with :class:`~snowflake.snowpark.Column` expressions.

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for UDAFs in
        :class:`~snowflake.snowpark.udaf.UDAFRegistration`.

            - You can use use :attr:`~snowflake.snowpark.types.Variant` to
              annotate a variant, and use :attr:`~snowflake.snowpark.types.Geography`
              to annotate a geography when defining a UDAF.

            - :class:`typing.Union` is not a valid type annotation for UDAFs,
              but :class:`typing.Optional` can be used to indicate the optional type.

            - Type hints are not supported on functions decorated with decorators.

        2. A temporary UDAF (when ``is_permanent`` is ``False``) is scoped to this ``session``
        and all UDAF related files will be uploaded to a temporary session stage
        (:func:`session.get_session_stage() <snowflake.snowpark.Session.get_session_stage>`).
        For a permanent UDAF, these files will be uploaded to the stage that you provide.

        3. By default, UDAF registration fails if a function with the same name is already
        registered. Invoking :func:`udaf` with ``replace`` set to ``True`` will overwrite the
        previously registered function.

    See Also:
        :class:`~snowflake.snowpark.udaf.UDAFRegistration`


    Example::
        >>> from snowflake.snowpark.types import IntegerType
        >>> class PythonSumUDAF:
        ...     def __init__(self) -> None:
        ...         self._sum = 0
        ...
        ...     @property
        ...     def aggregate_state(self):
        ...         return self._sum
        ...
        ...     def accumulate(self, input_value):
        ...         self._sum += input_value
        ...
        ...     def merge(self, other_sum):
        ...         self._sum += other_sum
        ...
        ...     def finish(self):
        ...         return self._sum
        >>> sum_udaf = udaf(
        ...     PythonSumUDAF,
        ...     name="sum_int",
        ...     replace=True,
        ...     return_type=IntegerType(),
        ...     input_types=[IntegerType()],
        ... )
        >>> df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
        >>> df.agg(sum_udaf("a")).collect()
        [Row(SUM_INT("A")=6)]

        Instead of calling `udaf` it is also possible to use udaf as a decorator.

    Example::

        >>> @udaf(name="sum_int", replace=True, return_type=IntegerType(), input_types=[IntegerType()])
        ... class PythonSumUDAF:
        ...     def __init__(self) -> None:
        ...         self._sum = 0
        ...
        ...     @property
        ...     def aggregate_state(self):
        ...         return self._sum
        ...
        ...     def accumulate(self, input_value):
        ...         self._sum += input_value
        ...
        ...     def merge(self, other_sum):
        ...         self._sum += other_sum
        ...
        ...     def finish(self):
        ...         return self._sum

        >>> df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
        >>> df.agg(PythonSumUDAF("a")).collect()
        [Row(SUM_INT("A")=6)]
    """

    # Initial check to make sure no unexpected args are passed in
    check_decorator_args(**kwargs)

    session = snowflake.snowpark.session._get_sandbox_conditional_active_session(
        session
    )
    if session is None:
        udaf_registration_method = UDAFRegistration(session=session).register
    else:
        udaf_registration_method = session.udaf.register

    if handler is None and kwargs.get("_registered_object_name") is None:
        return functools.partial(
            udaf_registration_method,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            if_not_exists=if_not_exists,
            parallel=parallel,
            statement_params=statement_params,
            immutable=immutable,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            comment=comment,
            artifact_repository=artifact_repository,
            artifact_repository_packages=artifact_repository_packages,
            resource_constraint=resource_constraint,
            _emit_ast=_emit_ast,
            **kwargs,
        )
    else:
        return udaf_registration_method(
            handler,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            if_not_exists=if_not_exists,
            parallel=parallel,
            statement_params=statement_params,
            immutable=immutable,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            comment=comment,
            artifact_repository=artifact_repository,
            artifact_repository_packages=artifact_repository_packages,
            resource_constraint=resource_constraint,
            _emit_ast=_emit_ast,
            **kwargs,
        )


@publicapi
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
    if_not_exists: bool = False,
    session: Optional["snowflake.snowpark.session.Session"] = None,
    parallel: int = 4,
    max_batch_size: Optional[int] = None,
    statement_params: Optional[Dict[str, str]] = None,
    strict: bool = False,
    secure: bool = False,
    source_code_display: bool = True,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    comment: Optional[str] = None,
    _emit_ast: bool = True,
    **kwargs,
) -> Union[UserDefinedFunction, functools.partial]:
    """
    Registers a Python function as a vectorized UDF and returns the UDF.
    The arguments, return value and usage of this function are exactly the same as
    :func:`udf`, but this function can only be used for registering vectorized UDFs.
    See examples in :class:`~snowflake.snowpark.udf.UDFRegistration`.

    See Also:
        - :func:`udf`
        - :meth:`UDFRegistration.register() <snowflake.snowpark.udf.UDFRegistration.register>`

    Example::

        >>> from snowflake.snowpark.types import PandasSeriesType, PandasDataFrameType, IntegerType
        >>> add_one_df_pandas_udf = pandas_udf(
        ...     lambda df: df[0] + df[1] + 1,
        ...     return_type=PandasSeriesType(IntegerType()),
        ...     input_types=[PandasDataFrameType([IntegerType(), IntegerType()])]
        ... )
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        >>> df.select(add_one_df_pandas_udf("a", "b").alias("result")).order_by("result").show()
        ------------
        |"RESULT"  |
        ------------
        |4         |
        |8         |
        ------------
        <BLANKLINE>

    or as named pandas UDFs that are accesible in the same session. Instead of calling `pandas_udf` as function,
    it can be also used as a decorator:

    Example::

        >>> from snowflake.snowpark.types import PandasSeriesType, PandasDataFrameType, IntegerType
        >>> @pandas_udf(
        ...     return_type=PandasSeriesType(IntegerType()),
        ...     input_types=[PandasDataFrameType([IntegerType(), IntegerType()])],
        ... )
        ... def add_one_df_pandas_udf(df):
        ...     return df[0] + df[1] + 1
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        >>> df.select(add_one_df_pandas_udf("a", "b").alias("result")).order_by("result").show()
        ------------
        |"RESULT"  |
        ------------
        |4         |
        |8         |
        ------------
        <BLANKLINE>
    """

    # Same as udf, except in addition _from_pandas_udf_function=True is passed.
    return udf(
        func,
        return_type=return_type,
        input_types=input_types,
        name=name,
        is_permanent=is_permanent,
        stage_location=stage_location,
        imports=imports,
        packages=packages,
        replace=replace,
        if_not_exists=if_not_exists,
        session=session,
        parallel=parallel,
        max_batch_size=max_batch_size,
        statement_params=statement_params,
        source_code_display=source_code_display,
        strict=strict,
        secure=secure,
        external_access_integrations=external_access_integrations,
        secrets=secrets,
        immutable=immutable,
        comment=comment,
        _from_pandas_udf_function=True,
        _emit_ast=_emit_ast,
        **kwargs,
    )


@publicapi
def pandas_udtf(
    handler: Optional[Callable] = None,
    *,
    output_schema: Union[StructType, List[str], "PandasDataFrameType"],
    input_types: Optional[List[DataType]] = None,
    input_names: Optional[List[str]] = None,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    if_not_exists: bool = False,
    session: Optional["snowflake.snowpark.session.Session"] = None,
    parallel: int = 4,
    statement_params: Optional[Dict[str, str]] = None,
    strict: bool = False,
    secure: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    max_batch_size: Optional[int] = None,
    comment: Optional[str] = None,
    _emit_ast: bool = True,
    **kwargs,
) -> Union[UserDefinedTableFunction, functools.partial]:
    """Registers a Python class as a vectorized Python UDTF and returns the UDTF.

    The arguments, return value and usage of this function are exactly the same as
    :func:`udtf`, but this function can only be used for registering vectorized UDTFs.
    See examples in :class:`~snowflake.snowpark.udtf.UDTFRegistration`.

    See Also:
        - :func:`udtf`
        - :meth:`UDTFRegistration.register() <snowflake.snowpark.udf.UDTFRegistration.register>`

    Compared to the default row-by-row processing pattern of a normal UDTF, which sometimes is
    inefficient, vectorized Python UDTFs (user-defined table functions) enable seamless partition-by-partition processing
    by operating on partitions as
    `pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_
    and returning results as
    `pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_
    or lists of `pandas arrays <https://pandas.pydata.org/docs/reference/api/pandas.array.html>`_
    or `pandas Series <https://pandas.pydata.org/docs/reference/series.html>`_.

    In addition, vectorized Python UDTFs allow for easy integration with libraries that operate on pandas DataFrames or pandas arrays.

    A vectorized UDTF handler class:
    - defines an :code:`end_partition` method that takes in a DataFrame argument and returns a :code:`pandas.DataFrame` or a tuple of :code:`pandas.Series` or :code:`pandas.arrays` where each array is a column.
    - does NOT define a :code:`process` method.
    - optionally defines a handler class with an :code:`__init__` method which will be invoked before processing each partition.

    You can use :func:`~snowflake.snowpark.functions.udtf`, :meth:`register` or
    :func:`~snowflake.snowpark.functions.pandas_udtf` to create a vectorized UDTF by providing
    appropriate return and input types. If you would like to use :meth:`register_from_file` to
    create a vectorized UDTF, you need to explicitly mark the handler method as vectorized using
    either the decorator `@vectorized(input=pandas.DataFrame)` or setting `<class>.end_partition._sf_vectorized_input = pandas.DataFrame`

    Note: A vectorized UDTF must be called with `~snowflake.snowpark.Window.partition_by` to build the partitions.

    Example::
        >>> from snowflake.snowpark.types import PandasSeriesType, PandasDataFrameType, IntegerType
        >>> class multiply:
        ...     def __init__(self):
        ...         self.multiplier = 10
        ...     def end_partition(self, df):
        ...         df.col1 = df.col1*self.multiplier
        ...         df.col2 = df.col2*self.multiplier
        ...         yield df
        >>> multiply_udtf = pandas_udtf(
        ...     multiply,
        ...     output_schema=PandasDataFrameType([StringType(), IntegerType(), FloatType()], ["id_", "col1_", "col2_"]),
        ...     input_types=[PandasDataFrameType([StringType(), IntegerType(), FloatType()])],
        ...     input_names=['"id"', '"col1"', '"col2"']
        ... )
        >>> df = session.create_dataframe([['x', 3, 35.9],['x', 9, 20.5]], schema=["id", "col1", "col2"])
        >>> df.select(multiply_udtf("id", "col1", "col2").over(partition_by=["id"])).sort("col1_").show()
        -----------------------------
        |"ID_"  |"COL1_"  |"COL2_"  |
        -----------------------------
        |x      |30       |359.0    |
        |x      |90       |205.0    |
        -----------------------------
        <BLANKLINE>

    Example::

        >>> @pandas_udtf(
        ... output_schema=PandasDataFrameType([StringType(), IntegerType(), FloatType()], ["id_", "col1_", "col2_"]),
        ... input_types=[PandasDataFrameType([StringType(), IntegerType(), FloatType()])],
        ... input_names=['"id"', '"col1"', '"col2"']
        ... )
        ... class _multiply:
        ...     def __init__(self):
        ...         self.multiplier = 10
        ...     def end_partition(self, df):
        ...         df.col1 = df.col1*self.multiplier
        ...         df.col2 = df.col2*self.multiplier
        ...         yield df
        >>> df.select(multiply_udtf("id", "col1", "col2").over(partition_by=["id"])).sort("col1_").show()
        -----------------------------
        |"ID_"  |"COL1_"  |"COL2_"  |
        -----------------------------
        |x      |30       |359.0    |
        |x      |90       |205.0    |
        -----------------------------
        <BLANKLINE>
    """

    # Same as udtf, but pandas_udtf has kwargs input_names, max_batch_size in addition to plain udtf.
    return udtf(
        handler,
        output_schema=output_schema,
        input_types=input_types,
        input_names=input_names,
        name=name,
        is_permanent=is_permanent,
        stage_location=stage_location,
        imports=imports,
        packages=packages,
        replace=replace,
        if_not_exists=if_not_exists,
        session=session,
        parallel=parallel,
        statement_params=statement_params,
        strict=strict,
        secure=secure,
        external_access_integrations=external_access_integrations,
        secrets=secrets,
        immutable=immutable,
        max_batch_size=max_batch_size,
        comment=comment,
        _emit_ast=_emit_ast,
        **kwargs,
    )


@publicapi
def call_udf(udf_name: str, *args: ColumnOrLiteral, _emit_ast: bool = True) -> Column:
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

    ast = build_function_expr("call_udf", [udf_name, *args]) if _emit_ast else None

    return _call_function(
        udf_name,
        *args,
        api_call_source="functions.call_udf",
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def call_table_function(
    function_name: Union[str, Iterable[str]],
    *args: ColumnOrLiteral,
    _emit_ast: bool = True,
    **kwargs: ColumnOrLiteral,
) -> "snowflake.snowpark.table_function.TableFunctionCall":
    """Invokes a Snowflake table function, including system-defined table functions and user-defined table functions.

    It returns a :meth:`~snowflake.snowpark.table_function.TableFunctionCall` so you can specify the partition clause.

    Args:
        function_name: The name of the table function.
        args: The positional arguments of the table function.
        **kwargs: The named arguments of the table function. Some table functions (e.g., ``flatten``) have named arguments instead of positional ones.

    Example::
        >>> from snowflake.snowpark.functions import lit
        >>> session.table_function(call_table_function("split_to_table", lit("split words to table"), lit(" ")).over()).collect()
        [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]
    """
    # AST.
    ast = None
    if _emit_ast:
        ast = proto.Expr()
        build_call_table_function_apply(ast, function_name, *args, **kwargs)

    func_call = snowflake.snowpark.table_function.TableFunctionCall(
        function_name, *args, _ast=ast, _emit_ast=_emit_ast, **kwargs
    )

    return func_call


@publicapi
def table_function(function_name: str, _emit_ast: bool = True) -> Callable:
    """Create a function object to invoke a Snowflake table function.

    Args:
        function_name: The name of the table function.

    Example::
        >>> from snowflake.snowpark.functions import lit
        >>> split_to_table = table_function("split_to_table")
        >>> session.table_function(split_to_table(lit("split words to table"), lit(" ")).over()).collect()
        [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]
    """
    fn = lambda *args, **kwargs: call_table_function(  # noqa: E731
        function_name, *args, **kwargs
    )

    # AST.
    ast = build_function_expr("table_function", [function_name]) if _emit_ast else None
    fn._ast = ast

    return fn


@publicapi
def call_function(
    function_name: str,
    *args: ColumnOrLiteral,
    _emit_ast: bool = True,
) -> Column:
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
    ast = (
        build_function_expr("call_function", [function_name, *args])
        if _emit_ast
        else None
    )
    return _call_function(function_name, *args, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def function(function_name: str, _emit_ast: bool = True) -> Callable:
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
    return lambda *args: call_function(function_name, *args, _emit_ast=_emit_ast)


def _call_function(
    name: str,
    *args: ColumnOrLiteral,
    is_distinct: bool = False,
    api_call_source: Optional[str] = None,
    is_data_generator: bool = False,
    _ast: proto.Expr = None,
    _emit_ast: bool = True,
) -> Column:

    if _emit_ast and _ast is None:
        _ast = build_function_expr(name, args)

    args_list = parse_positional_args_to_list(*args)
    expressions = [Column._to_expr(arg) for arg in args_list]
    return Column(
        FunctionExpression(
            name,
            expressions,
            is_distinct=is_distinct,
            api_call_source=api_call_source,
            is_data_generator=is_data_generator,
        ),
        _ast=_ast,
        _emit_ast=_emit_ast,
    )


@publicapi
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
    if_not_exists: bool = False,
    session: Optional["snowflake.snowpark.Session"] = None,
    parallel: int = 4,
    statement_params: Optional[Dict[str, str]] = None,
    execute_as: typing.Literal["caller", "owner", "restricted caller"] = "owner",
    strict: bool = False,
    source_code_display: bool = True,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    comment: Optional[str] = None,
    _emit_ast: bool = True,
    artifact_repository: Optional[str] = None,
    artifact_repository_packages: Optional[List[str]] = None,
    resource_constraint: Optional[Dict[str, str]] = None,
    **kwargs,
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
            :meth:`~snowflake.snowpark.Session.add_requirements`. To use Python packages that are not available in
            Snowflake, refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.
        replace: Whether to replace a stored procedure that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a stored procedure with a name that already exists
            results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
            an existing stored procedure with the same name is overwritten.
        if_not_exists: Whether to skip creation of a stored procedure the same procedure is already registered.
            The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive and a ``ValueError``
            is raised when both are set. If it is ``True`` and a stored procedure is already registered, the registration is skipped.
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
        strict: Whether the created stored procedure is strict. A strict stored procedure will not invoke
            the stored procedure if any input is null. Instead, a null value will always be returned. Note
            that the stored procedure might still return null for non-null inputs.
        source_code_display: Display the source code of the stored procedure `func` as comments in the generated script.
            The source code is dynamically generated therefore it may not be identical to how the
            `func` is originally defined. The default is ``True``.
            If it is ``False``, source code will not be generated or displayed.
        external_access_integrations: The names of one or more external access integrations. Each
            integration you specify allows access to the external network locations and secrets
            the integration specifies.
        secrets: The key-value pairs of string types of secrets used to authenticate the external network location.
            The secrets can be accessed from handler code. The secrets specified as values must
            also be specified in the external access integration and the keys are strings used to
            retrieve the secrets using secret API.
        comment: Adds a comment for the created object. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_
        artifact_repository: The name of an artifact_repository that the ``artifact_repository_packages``
            parameter will search for packages in.
        artifact_repository_packages: A list of packages to search for within the pypi repository
            set in the above parameter.
        resource_constraint: A dictionary containing a resource properties of a warehouse and then
            constraints needed to run this function. Eg ``{"architecture": "x86"}`` requires an x86
            warehouse be used for execution.

    Returns:
        A stored procedure function that can be called with python value.

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for stored procedure in
        :class:`~snowflake.snowpark.stored_procedure.StoredProcedureRegistration`.

            - You can use :attr:`~snowflake.snowpark.types.Variant` to
              annotate a variant, and use :attr:`~snowflake.snowpark.types.Geography`
              or :attr:`~snowflake.snowpark.types.Geometry` to annotate geospatial
              types when defining a stored procedure.

            - :class:`typing.Union` is not a valid type annotation for stored procedures,
              but :class:`typing.Optional` can be used to indicate the optional type.

        2. A temporary stored procedure (when ``is_permanent`` is ``False``) is scoped to this ``session``
        and all stored procedure related files will be uploaded to a temporary session stage
        (:func:`session.get_session_stage() <snowflake.snowpark.Session.get_session_stage>`).
        For a permanent stored procedure, these files will be uploaded to the stage that you provide.

        3. By default, stored procedure registration fails if a function with the same name is already
        registered. Invoking :func:`sproc` with ``replace`` set to ``True`` will overwrite the
        previously registered function.

        4. To describe the return type for a stored procedure that `returns tabular data
        <https://docs.snowflake.com/en/sql-reference/stored-procedures-python#returning-tabular-data>`_,
        use one of the following ways:

            - (Recommended) Describe the return type using :attr:`~snowflake.snowpark.types.StructType`
              and :attr:`~snowflake.snowpark.types.StructField`. Set ``return_type =
              StructType([StructField("a", DataTypeA()), ...])`` to describe the case
              ``RETURNS TABLE(A DataTypeA, ...)``.

            - Set ``return_type = StructType()`` to describe the case ``RETURNS TABLE()``.

            - When using type hints, the return type of function can be set as
              :class:`~snowflake.snowpark.dataframe.DataFrame`. This registers a
              table stored procedure with return type defined using ``RETURNS TABLE()``.
              Check **See also** below for more examples.

    See Also:
        :class:`~snowflake.snowpark.stored_procedure.StoredProcedureRegistration`

    Example::
        >>> from snowflake.snowpark.types import IntegerType
        >>> @sproc(return_type=IntegerType(), input_types=[IntegerType(), IntegerType()], packages=["snowflake-snowpark-python"])
        ... def add_sp(session_, x, y):
        ...     return session_.sql(f"SELECT {x} + {y}").collect()[0][0]
        ...
        >>> add_sp(1, 1)
        2
    """

    # Initial check to make sure no unexpected args are passed in
    check_decorator_args(**kwargs)

    session = snowflake.snowpark.session._get_sandbox_conditional_active_session(
        session
    )
    if session is None:
        sproc_registration_method = StoredProcedureRegistration(
            session=session
        ).register
    else:
        sproc_registration_method = session.sproc.register

    if func is None and kwargs.get("_registered_object_name") is None:
        return functools.partial(
            sproc_registration_method,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            if_not_exists=if_not_exists,
            parallel=parallel,
            statement_params=statement_params,
            execute_as=execute_as,
            strict=strict,
            source_code_display=source_code_display,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            comment=comment,
            artifact_repository=artifact_repository,
            artifact_repository_packages=artifact_repository_packages,
            resource_constraint=resource_constraint,
            _emit_ast=_emit_ast,
            **kwargs,
        )
    else:
        return sproc_registration_method(
            func,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            if_not_exists=if_not_exists,
            parallel=parallel,
            statement_params=statement_params,
            execute_as=execute_as,
            strict=strict,
            source_code_display=source_code_display,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            comment=comment,
            artifact_repository=artifact_repository,
            artifact_repository_packages=artifact_repository_packages,
            resource_constraint=resource_constraint,
            _emit_ast=_emit_ast,
            **kwargs,
        )


# Add these alias for user code migration
call_builtin = call_function
collect_set = array_unique_agg
collect_list = array_agg
builtin = function
countDistinct = count_distinct
substr = substring
to_varchar = to_char
expr = sql_expr
monotonically_increasing_id = seq8
from_unixtime = to_timestamp
sort_array = array_sort
map_from_arrays = arrays_to_object
signum = sign
array_join = array_to_string
array_union = array_cat
map_concat = map_cat


@publicapi
def unix_timestamp(
    e: ColumnOrName, fmt: Optional["Column"] = None, _emit_ast: bool = True
) -> Column:
    """
    Converts a timestamp or a timestamp string to Unix time stamp (in seconds).

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([["2013-05-08T23:39:20.123-07:00"]], schema=["ts_col"])
        >>> df.select(unix_timestamp(col("ts_col")).alias("unix_time")).show()
        ---------------
        |"UNIX_TIME"  |
        ---------------
        |1368056360   |
        ---------------
        <BLANKLINE>
    """
    # AST.
    ast = (
        build_function_expr(
            "unix_timestamp",
            [e, fmt] if fmt is not None else [e],
        )
        if _emit_ast
        else None
    )

    ans = date_part(
        "epoch_second", to_timestamp(e, fmt, _emit_ast=False), _emit_ast=False
    )
    ans._ast = ast
    return ans


@publicapi
def locate(
    expr1: str, expr2: ColumnOrName, start_pos: int = 1, _emit_ast: bool = True
) -> Column:
    """
    Searches for the first occurrence of the first argument in the second argument.
    If successful, returns the position (1-based) of the first argument in the second argument.
    Otherwise, return 0.

    Note::

        If the first argument is empty, this function always returns 1.

    Example::

        >>> df = session.create_dataframe([["find a needle in a haystack"],["nothing but hay in a haystack"]], schema=["expr"])
        >>> df.select(locate("needle", col("expr")).alias("1-pos")).show()
        -----------
        |"1-pos"  |
        -----------
        |8        |
        |0        |
        -----------
        <BLANKLINE>
    """
    ast = (
        build_function_expr("locate", [expr1, expr2, start_pos]) if _emit_ast else None
    )
    _substr = lit(expr1)
    _str = _to_col_if_str(expr2, "locate")
    return _call_function(
        "charindex", _substr, _str, lit(start_pos), _ast=ast, _emit_ast=_emit_ast
    )


@publicapi
def make_interval(
    years: Optional[int] = None,
    quarters: Optional[int] = None,
    months: Optional[int] = None,
    weeks: Optional[int] = None,
    days: Optional[int] = None,
    hours: Optional[int] = None,
    minutes: Optional[int] = None,
    seconds: Optional[int] = None,
    milliseconds: Optional[int] = None,
    microseconds: Optional[int] = None,
    nanoseconds: Optional[int] = None,
    mins: Optional[int] = None,
    secs: Optional[int] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Creates an interval column with the specified years, quarters, months, weeks, days, hours,
    minutes, seconds, milliseconds, microseconds, and nanoseconds. You can find more details in
    `Interval constants <https://docs.snowflake.com/en/sql-reference/data-types-datetime#interval-constants>`_.

    INTERVAL is not a data type (that is, you can’t define a table column to be of data type INTERVAL).
    Intervals can only be used in date, time, and timestamp arithmetic. For example,
    ``df.select(make_interval(days=0))`` is not valid.

    Example::

        >>> import datetime
        >>> from snowflake.snowpark.functions import to_date
        >>>
        >>> df = session.create_dataframe([datetime.datetime(2023, 8, 8, 1, 2, 3)], schema=["ts"])
        >>> df.select(to_date(col("ts") + make_interval(days=10)).alias("next_day")).show()
        --------------
        |"NEXT_DAY"  |
        --------------
        |2023-08-18  |
        --------------
        <BLANKLINE>

    You can also find some examples to use interval constants with :meth:`~snowflake.snowpark.Window.range_between`
    method.
    """
    ast = None
    if _emit_ast:
        ast = proto.Expr()
        # Encode the parameters as kwargs to make them more readable.
        # If any of the parameters are None, ignore them.
        kwargs = {
            k: v
            for k, v in {
                "years": years,
                "quarters": quarters,
                "months": months,
                "weeks": weeks,
                "days": days,
                "hours": hours,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": milliseconds,
                "microseconds": microseconds,
                "nanoseconds": nanoseconds,
                "mins": mins,
                "secs": secs,
            }.items()
            if v is not None
        }
        build_builtin_fn_apply(ast, "make_interval", **kwargs)

    # for migration purpose
    minutes = minutes or mins
    seconds = seconds or secs

    # create column
    res = Column(
        Interval(
            years,
            quarters,
            months,
            weeks,
            days,
            hours,
            minutes,
            seconds,
            milliseconds,
            microseconds,
            nanoseconds,
        ),
        _emit_ast=False,
    )

    res._ast = ast
    return res


@publicapi
@deprecated(
    version="1.28.0",
    extra_warning_text="Please consider installing snowflake-ml-python and using `snowflake.cortex.summarize` instead.",
    extra_doc_string="Use :meth:`snowflake.cortex.summarize` instead.",
)
def snowflake_cortex_summarize(
    text: ColumnOrLiteralStr, _emit_ast: bool = True
) -> Column:
    """
    Summarizes the given English-language input text.
    Args:
        text: A string containing the English text from which a summary should be generated.
    Returns:
        A string containing a summary of the original text.
    """
    ast = (
        build_function_expr("snowflake_cortex_summarize", [text]) if _emit_ast else None
    )

    sql_func_name = "snowflake.cortex.summarize"
    text_col = _to_col_if_lit(text, sql_func_name)
    return _call_function(sql_func_name, text_col, _ast=ast, _emit_ast=_emit_ast)


@publicapi
@deprecated(
    version="1.28.0",
    extra_warning_text="Please consider installing snowflake-ml-python and using `snowflake.cortex.sentiment` instead.",
    extra_doc_string="Use :meth:`snowflake.cortex.sentiment` instead.",
)
def snowflake_cortex_sentiment(
    text: ColumnOrLiteralStr, _emit_ast: bool = True
) -> Column:
    """
    A string containing the text for which a sentiment score should be calculated.
    Args:
        text: A string containing the English text from which a summary should be generated.
    Returns:
        A floating-point number from -1 to 1 (inclusive) indicating the level of negative or positive sentiment in the
        text. Values around 0 indicate neutral sentiment.
    """
    ast = (
        build_function_expr("snowflake_cortex_sentiment", [text]) if _emit_ast else None
    )

    sql_func_name = "snowflake.cortex.sentiment"
    text_col = _to_col_if_lit(text, sql_func_name)
    return _call_function(sql_func_name, text_col, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def acosh(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the inverse(arc) hyperbolic cosine of the input value.

    Example::

        >>> df = session.create_dataframe([2.352409615], schema=["a"])
        >>> df.select(acosh("a").as_("acosh")).collect()
        [Row(ACOSH=1.4999999998857607)]
    """
    c = _to_col_if_str(e, "acosh")
    return _call_function("acosh", c, _emit_ast=_emit_ast)


@publicapi
def asinh(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the inverse(arc) hyperbolic sine of the input value.

    Example::

        >>> df = session.create_dataframe([2.129279455], schema=["a"])
        >>> df.select(asinh(df["a"]).alias("asinh")).collect()
        [Row(ASINH=1.4999999999596934)]
    """
    c = _to_col_if_str(e, "asinh")
    return _call_function("asinh", c, _emit_ast=_emit_ast)


@publicapi
def atanh(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the inverse(arc) hyperbolic tangent of the input value.

    Example::

        >>> df = session.create_dataframe([0.9051482536], schema=["a"])
        >>> df.select(atanh(df["a"]).alias("result")).collect()
        [Row(RESULT=1.4999999997517164)]
    """
    c = _to_col_if_str(e, "atanh")
    return _call_function("atanh", c, _emit_ast=_emit_ast)


@publicapi
def bit_length(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the length of a string or binary value in bits.

    Example::

        >>> df = session.create_dataframe([['abc'], ['\u0394']], schema=["v"])
        >>> df = df.withColumn("b", lit("A1B2").cast("binary"))
        >>> df.select(bit_length(col("v")).alias("BIT_LENGTH_V"), bit_length(col("b")).alias("BIT_LENGTH_B")).collect()
        [Row(BIT_LENGTH_V=24, BIT_LENGTH_B=16), Row(BIT_LENGTH_V=16, BIT_LENGTH_B=16)]
    """
    c = _to_col_if_str(e, "bit_length")
    return _call_function("bit_length", c, _emit_ast=_emit_ast)


@publicapi
def bitmap_bit_position(numeric_expr: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the position of the first set bit (i.e., the first '1' bit) in the binary representation of the input number.

    Example::

        >>> df = session.create_dataframe([1, 2, 3, 32768, 32769], schema=["a"])
        >>> df.select(bitmap_bit_position("a").alias("bit_position")).collect()
        [Row(BIT_POSITION=0), Row(BIT_POSITION=1), Row(BIT_POSITION=2), Row(BIT_POSITION=32767), Row(BIT_POSITION=0)]
    """
    c = _to_col_if_str(numeric_expr, "bitmap_bit_position")
    return _call_function("bitmap_bit_position", c, _emit_ast=_emit_ast)


@publicapi
def bitmap_bucket_number(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the bucket number of the input value in a bitmap index.

    The bucket number is a value between 1 and the number of buckets in the bitmap index.

    Example::

        >>> df = session.create_dataframe([1, 2, 3, 32768, 32769], schema=["a"])
        >>> df.select(bitmap_bucket_number(col("a")).alias("bucket_number")).collect()
        [Row(BUCKET_NUMBER=1), Row(BUCKET_NUMBER=1), Row(BUCKET_NUMBER=1), Row(BUCKET_NUMBER=1), Row(BUCKET_NUMBER=2)]
    """
    c = _to_col_if_str(e, "bitmap_bucket_number")
    return _call_function("bitmap_bucket_number", c, _emit_ast=_emit_ast)


@publicapi
def bitmap_construct_agg(
    relative_position: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns a bitmap constructed from the relative positions of the input values.

    Example::

        >>> df = session.create_dataframe([1, 32769], schema=["a"])
        >>> df.select(bitmap_bucket_number(df["a"]).alias("bitmap_id"),bitmap_bit_position(df["a"]).alias("bit_position")).group_by("bitmap_id").agg(bitmap_construct_agg(col("bit_position")).alias("bitmap")).collect()
        [Row(BITMAP_ID=1, BITMAP=bytearray(b'\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00')), Row(BITMAP_ID=2, BITMAP=bytearray(b'\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'))]
    """
    c = _to_col_if_str(relative_position, "bitmap_construct_agg")
    return _call_function("bitmap_construct_agg", c, _emit_ast=_emit_ast)


@publicapi
def cbrt(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the cube root of value in a column.

    Example::

        >>> df = session.create_dataframe([0, 2, -10, None], schema=["x"])
        >>> df.select(cbrt("x").alias("cbrt_x")).collect()
        [Row(CBRT_X=0.0), Row(CBRT_X=1.2599210498948734), Row(CBRT_X=-2.1544346900318834), Row(CBRT_X=None)]
    """
    c = _to_col_if_str(e, "cbrt")
    return _call_function("cbrt", c, _emit_ast=_emit_ast)


@publicapi
def equal_null(e1: ColumnOrName, e2: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Compares whether two expressions are equal. The function is NULL-safe, meaning it treats NULLs as
    known values for comparing equality. Note that this is different from the EQUAL comparison operator (=),
    which treats NULLs as unknown values.

    Example::

        >>> df = session.create_dataframe([[1, 1], [1, None], [None, 2], [None, None]], schema=["a", "b"])
        >>> df.select(equal_null(df["a"], df["b"]).alias("equal_null")).collect()
        [Row(EQUAL_NULL=True), Row(EQUAL_NULL=False), Row(EQUAL_NULL=False), Row(EQUAL_NULL=True)]
    """
    c1 = _to_col_if_str(e1, "equal_null")
    c2 = _to_col_if_str(e2, "equal_null")
    return _call_function("equal_null", c1, c2, _emit_ast=_emit_ast)


@publicapi
def ifnull(e1: ColumnOrName, e2: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    If expr1 is NULL, returns expr2, otherwise returns expr1.

    Example::

        >>> df = session.create_dataframe([("a", "b"), ("c", None), (None, "d"), (None, None)], schema=["e1", "e2"])
        >>> df.select(ifnull(df["e1"], df["e2"]).alias("result")).collect()
        [Row(RESULT='a'), Row(RESULT='c'), Row(RESULT='d'), Row(RESULT=None)]
    """
    c1 = _to_col_if_str(e1, "ifnull")
    c2 = _to_col_if_str(e2, "ifnull")
    return _call_function("ifnull", c1, c2, _emit_ast=_emit_ast)


nvl = ifnull


@publicapi
def localtimestamp(fract_sec_precision: int = 9, _emit_ast: bool = True) -> Column:
    """
    Returns the current timestamp at the start of the query with the specified fractional second precision.

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(localtimestamp(3)).collect()  # doctest: +SKIP
    """
    ast = (
        build_function_expr("localtimestamp", [fract_sec_precision])
        if _emit_ast
        else None
    )
    return _call_function(
        "localtimestamp",
        lit(fract_sec_precision, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def max_by(
    col_to_return: ColumnOrName,
    col_containing_maximum: ColumnOrName,
    maximum_number_of_values_to_return: Optional[int] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Finds the row(s) containing the maximum value for a column and returns the value of another column in that row.

    Example::

        >>> df = session.create_dataframe([
        ...     [1001, 10, 10000],
        ...     [1020, 10, 9000],
        ...     [1030, 10, 8000],
        ...     [900, 20, 15000],
        ...     [2000, 20, None],
        ...     [2010, 20, 15000],
        ...     [2020, 20, 8000]
        ... ], schema=["employee_id", "department_id", "salary"])
        >>> df.select(max_by("employee_id", "salary", 3)).collect()
        [Row(MAX_BY("EMPLOYEE_ID", "SALARY", 3)='[\\n  900,\\n  2010,\\n  1001\\n]')]
    """
    c1 = _to_col_if_str(col_to_return, "max_by")
    c2 = _to_col_if_str(col_containing_maximum, "max_by")
    if maximum_number_of_values_to_return is not None:
        ast = (
            build_function_expr("max_by", [c1, c2, maximum_number_of_values_to_return])
            if _emit_ast
            else None
        )
        return _call_function(
            "max_by",
            c1,
            c2,
            lit(maximum_number_of_values_to_return, _emit_ast=False),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
    else:
        return _call_function("max_by", c1, c2, _emit_ast=_emit_ast)


@publicapi
def min_by(
    col_to_return: ColumnOrName,
    col_containing_minimum: ColumnOrName,
    maximum_number_of_values_to_return: Optional[int] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Finds the row(s) containing the minimum value for a column and returns the value of another column in that row.

    Example::

        >>> df = session.create_dataframe([
        ...     [1001, 10, 10000],
        ...     [1020, 10, 9000],
        ...     [1030, 10, 8000],
        ...     [900, 20, 15000],
        ...     [2000, 20, None],
        ...     [2010, 20, 15000],
        ...     [2020, 20, 8000]
        ... ], schema=["employee_id", "department_id", "salary"])
        >>> df.select(min_by("employee_id", "salary", 3).alias("min_by")).collect()
        [Row(MIN_BY='[\\n  1030,\\n  2020,\\n  1020\\n]')]

    """
    c1 = _to_col_if_str(col_to_return, "min_by")
    c2 = _to_col_if_str(col_containing_minimum, "min_by")
    if maximum_number_of_values_to_return is not None:
        ast = (
            build_function_expr("min_by", [c1, c2, maximum_number_of_values_to_return])
            if _emit_ast
            else None
        )
        return _call_function(
            "min_by",
            c1,
            c2,
            lit(maximum_number_of_values_to_return, _emit_ast=False),
            _ast=ast,
            _emit_ast=_emit_ast,
        )
    else:
        return _call_function("min_by", c1, c2, _emit_ast=_emit_ast)


@publicapi
def octet_length(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the length of a string or binary value in bytes.
    This will be the same as LENGTH for ASCII strings and greater than LENGTH for strings using Unicode code points.
    For binary, this is always the same as LENGTH.

    Example::

        >>> df = session.create_dataframe(['abc', '\u0392', "X'A1B2'"], schema=["a"])
        >>> df.select(octet_length(col("a")).alias("octet_length")).collect()
        [Row(OCTET_LENGTH=3), Row(OCTET_LENGTH=2), Row(OCTET_LENGTH=7)]
    """
    c = _to_col_if_str(e, "octet_length")
    return _call_function("octet_length", c, _emit_ast=_emit_ast)


@publicapi
def position(
    expr1: ColumnOrName,
    expr2: ColumnOrName,
    start_pos: int = 1,
    _emit_ast: bool = True,
) -> Column:
    """
    Searches for the first occurrence of the first argument in the second argument and, if successful, returns
    the position (1-based) of the first argument in the second argument.

    Example::

        >>> df = session.create_dataframe([['an', 'banana'], ['nan', 'banana']], schema=["expr1", "expr2"])
        >>> df.select(position(df["expr1"], df["expr2"], 3).alias("position")).collect()
        [Row(POSITION=4), Row(POSITION=3)]
    """
    ast = (
        build_function_expr("position", [expr1, expr2, start_pos])
        if _emit_ast
        else None
    )
    c1 = _to_col_if_str(expr1, "position")
    c2 = _to_col_if_str(expr2, "position")
    return _call_function(
        "position",
        c1,
        c2,
        lit(start_pos, _emit_ast=False),
        _ast=ast,
        _emit_ast=_emit_ast,
    )


@publicapi
def regr_avgx(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the average of the independent variable for non-null pairs in a group, where x is the
    independent variable and y is the dependent variable.

    Example::

        >>> df = session.create_dataframe([[10, 11], [20, 22], [25, None], [30, 35]], schema=["v", "v2"])
        >>> df.groupBy("v").agg(regr_avgx(df["v"], df["v2"]).alias("regr_avgx")).collect()
        [Row(V=10, REGR_AVGX=11.0), Row(V=20, REGR_AVGX=22.0), Row(V=25, REGR_AVGX=None), Row(V=30, REGR_AVGX=35.0)]
    """
    c1 = _to_col_if_str(y, "regr_avgx")
    c2 = _to_col_if_str(x, "regr_avgx")
    return _call_function("regr_avgx", c1, c2, _emit_ast=_emit_ast)


@publicapi
def regr_avgy(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the average of the dependent variable for non-null pairs in a group, where x is the
    independent variable and y is the dependent variable.

    Example::

        >>> df = session.create_dataframe([[10, 11], [20, 22], [25, None], [30, 35]], schema=["v", "v2"])
        >>> df = df.group_by("v").agg(regr_avgy(df["v"], df["v2"]).alias("regr_avgy"))
        >>> df.collect()
        [Row(V=10, REGR_AVGY=10.0), Row(V=20, REGR_AVGY=20.0), Row(V=25, REGR_AVGY=None), Row(V=30, REGR_AVGY=30.0)]
    """
    y = _to_col_if_str(y, "regr_avgy")
    x = _to_col_if_str(x, "regr_avgy")
    return _call_function("regr_avgy", y, x, _emit_ast=_emit_ast)


@publicapi
def regr_count(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the number of non-null number pairs in a group.

    Example::

        >>> df = session.create_dataframe([[1, 10, 11], [1, 20, 22], [1, 25, None], [2, 30, 35]], schema=["k", "v", "v2"])
        >>> df.group_by("k").agg(regr_count(col("v"), col("v2")).alias("regr_count")).collect()
        [Row(K=1, REGR_COUNT=2), Row(K=2, REGR_COUNT=1)]
    """
    c1 = _to_col_if_str(y, "regr_count")
    c2 = _to_col_if_str(x, "regr_count")
    return _call_function("regr_count", c1, c2, _emit_ast=_emit_ast)


@publicapi
def regr_intercept(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the intercept of the univariate linear regression line for non-null pairs in a group.
    It is computed for non-null pairs using the following formula: AVG(y)-REGR_SLOPE(y,x)*AVG(x), where x is
    the independent variable and y is the dependent variable.

    Example::

        >>> df = session.create_dataframe([[10, 11], [20, 22], [30, 35]], schema=["v", "v2"])
        >>> df.groupBy().agg(regr_intercept(df["v"], df["v2"]).alias("regr_intercept")).collect()
        [Row(REGR_INTERCEPT=1.1547344110854496)]
    """
    c1 = _to_col_if_str(y, "regr_intercept")
    c2 = _to_col_if_str(x, "regr_intercept")
    return _call_function("regr_intercept", c1, c2, _emit_ast=_emit_ast)


@publicapi
def regr_r2(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the coefficient of determination for non-null pairs in a group.
    It is computed for non-null pairs using the following formula: NULL if VAR_POP(x) = 0, else
    1 if VAR_POP(y) = 0 and VAR_POP(x) <> 0, else POWER(CORR(y,x), 2).
    Where x is the independent variable and y is the dependent variable.

    Example::

        >>> df = session.create_dataframe([[10, 11], [20, 22], [25, None], [30, 35]], schema=["v", "v2"])
        >>> df.groupBy("v").agg(regr_r2(col("v"), col("v2")).alias("regr_r2")).collect()
        [Row(V=10, REGR_R2=None), Row(V=20, REGR_R2=None), Row(V=25, REGR_R2=None), Row(V=30, REGR_R2=None)]
    """
    y = _to_col_if_str(y, "regr_r2")
    x = _to_col_if_str(x, "regr_r2")
    return _call_function("regr_r2", y, x, _emit_ast=_emit_ast)


@publicapi
def regr_slope(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the slope of the linear regression line for non-null pairs in a group.
    It is computed for non-null pairs using the following formula: COVAR_POP(x,y) / VAR_POP(x), where x is the
    independent variable and y is the dependent variable.

    Example::

        >>> df = session.create_dataframe([[10, 11], [20, 22], [25, None], [30, 35]], schema=["v", "v2"])
        >>> df = df.group_by("v").agg(regr_slope(df["v2"], df["v"]).alias("regr_slope"))
        >>> df.collect()
        [Row(V=10, REGR_SLOPE=None), Row(V=20, REGR_SLOPE=None), Row(V=25, REGR_SLOPE=None), Row(V=30, REGR_SLOPE=None)]
    """
    c1 = _to_col_if_str(y, "regr_slope")
    c2 = _to_col_if_str(x, "regr_slope")
    return _call_function("regr_slope", c1, c2, _emit_ast=_emit_ast)


@publicapi
def regr_sxx(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs.

    Example::

        >>> df = session.create_dataframe([[10, 11], [20, 22], [25, None], [30, 35]], schema=["v", "v2"])
        >>> df.group_by("v").agg(regr_sxx(col("v"), col("v2")).alias("regr_sxx")).collect()
        [Row(V=10, REGR_SXX=0.0), Row(V=20, REGR_SXX=0.0), Row(V=25, REGR_SXX=None), Row(V=30, REGR_SXX=0.0)]
    """
    y_col = _to_col_if_str(y, "regr_sxx")
    x_col = _to_col_if_str(x, "regr_sxx")
    return _call_function("regr_sxx", y_col, x_col, _emit_ast=_emit_ast)


@publicapi
def regr_sxy(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns REGR_COUNT(expr1, expr2) * COVAR_POP(expr1, expr2) for non-null pairs.

    Example::

        >>> df = session.create_dataframe([[10, 11], [20, 22], [25, None], [30, 35]], schema=["v", "v2"])
        >>> df = df.filter(df["v2"].is_not_null())
        >>> df.group_by("v").agg(regr_sxy(df["v"], df["v2"]).alias("regr_sxy")).collect()
        [Row(V=10, REGR_SXY=0.0), Row(V=20, REGR_SXY=0.0), Row(V=30, REGR_SXY=0.0)]
    """
    y_col = _to_col_if_str(y, "regr_sxy")
    x_col = _to_col_if_str(x, "regr_sxy")
    return _call_function("regr_sxy", y_col, x_col, _emit_ast=_emit_ast)


@publicapi
def regr_syy(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs.

    Example::

        >>> df = session.create_dataframe([[10, 11], [20, 22], [25, None], [30, 35]], schema=["v", "v2"])
        >>> df.groupBy("v").agg(regr_syy(df["v"], df["v2"]).alias("regr_syy")).collect()
        [Row(V=10, REGR_SYY=0.0), Row(V=20, REGR_SYY=0.0), Row(V=25, REGR_SYY=None), Row(V=30, REGR_SYY=0.0)]
    """
    c1 = _to_col_if_str(y, "regr_syy")
    c2 = _to_col_if_str(x, "regr_syy")
    return _call_function("regr_syy", c1, c2, _emit_ast=_emit_ast)


@publicapi
def try_to_binary(
    e: ColumnOrName, fmt: Optional[str] = None, _emit_ast: bool = True
) -> Column:
    """
    A special version of TO_BINARY that performs the same operation (i.e. converts an input expression to
    a binary value), but with error handling support (i.e. if the conversion cannot be performed,
    it returns a NULL value instead of raising an error).

    Example::

        >>> df = session.create_dataframe(["01", "A B", "Hello", None], schema=["hex_encoded_string"])
        >>> df.select(try_to_binary(df["hex_encoded_string"], 'HEX').alias("b")).collect()
        [Row(B=bytearray(b'\\x01')), Row(B=None), Row(B=None), Row(B=None)]
    """
    c = _to_col_if_str(e, "try_to_binary")
    return (
        _call_function("try_to_binary", c, fmt, _emit_ast=_emit_ast)
        if fmt
        else _call_function("try_to_binary", c, _emit_ast=_emit_ast)
    )


@publicapi
def base64_encode(
    e: ColumnOrName,
    max_line_length: Optional[int] = 0,
    alphabet: Optional[str] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Encodes the input (string or binary) using Base64 encoding.

    Example:
        >>> df = session.create_dataframe(["Snowflake", "Data"], schema=["input"])
        >>> df.select(base64_encode(col("input")).alias("encoded")).collect()
        [Row(ENCODED='U25vd2ZsYWtl'), Row(ENCODED='RGF0YQ==')]
    """
    # Convert input to a column if it is not already one.
    ast = (
        build_function_expr(
            "base64_encode",
            [e, max_line_length] + ([] if alphabet is None else [alphabet]),
        )
        if _emit_ast
        else None
    )
    col_input = _to_col_if_str(e, "base64_encode")

    # Prepare arguments for the function call.
    args = [col_input]

    if max_line_length:
        args.append(lit(max_line_length))

    if alphabet:
        args.append(lit(alphabet))

    # Call the built-in Base64 encode function.
    return _call_function("base64_encode", *args, _ast=ast, _emit_ast=_emit_ast)


base64 = base64_encode


@publicapi
def base64_decode_string(
    e: ColumnOrName, alphabet: Optional[str] = None, _emit_ast: bool = True
) -> Column:
    """
    Decodes a Base64-encoded string to a string.

    Example:
        >>> df = session.create_dataframe(["U25vd2ZsYWtl", "SEVMTE8="], schema=["input"])
        >>> df.select(base64_decode_string(col("input")).alias("decoded")).collect()
        [Row(DECODED='Snowflake'), Row(DECODED='HELLO')]
    """
    # Convert input to a column if it is not already one.
    ast = (
        build_function_expr(
            "base64_decode_string", [e] if alphabet is None else [e, alphabet]
        )
        if _emit_ast
        else None
    )
    col_input = _to_col_if_str(e, "base64_decode_string")

    # Prepare arguments for the function call.
    args = [col_input]

    if alphabet:
        args.append(lit(alphabet))

    # Call the built-in Base64 encode function.
    return _call_function("base64_decode_string", *args, _ast=ast, _emit_ast=_emit_ast)


unbase64 = base64_decode_string


@publicapi
def hex_encode(e: ColumnOrName, case: int = 1, _emit_ast: bool = True):
    """
    Encodes the input using hexadecimal (also ‘hex’ or ‘base16’) encoding.

    Example:
        >>> df = session.create_dataframe(["Snowflake", "Hello"], schema=["input"])
        >>> df.select(hex_encode(col("input")).alias("hex_encoded")).collect()
        [Row(HEX_ENCODED='536E6F77666C616B65'), Row(HEX_ENCODED='48656C6C6F')]
    """
    ast = build_function_expr("hex_encode", [e, case]) if _emit_ast else None
    col_input = _to_col_if_str(e, "hex_encode")
    return _call_function(
        "hex_encode", col_input, lit(case), _ast=ast, _emit_ast=_emit_ast
    )


hex = hex_encode


@publicapi
def editdistance(
    e1: ColumnOrName,
    e2: ColumnOrName,
    max_distance: Optional[Union[int, ColumnOrName]] = None,
    _emit_ast: bool = True,
) -> Column:
    """Computes the Levenshtein distance between two input strings.

    Optionally, a maximum distance can be specified. If the distance exceeds this value,
    the computation halts and returns the maximum distance.

    Example::

        >>> df = session.create_dataframe(
        ...     [["abc", "def"], ["abcdef", "abc"], ["snow", "flake"]],
        ...     schema=["s1", "s2"]
        ... )
        >>> df.select(
        ...     editdistance(col("s1"), col("s2")).alias("distance"),
        ...     editdistance(col("s1"), col("s2"), 2).alias("max_2_distance")
        ... ).collect()
        [Row(DISTANCE=3, MAX_2_DISTANCE=2), Row(DISTANCE=3, MAX_2_DISTANCE=2), Row(DISTANCE=5, MAX_2_DISTANCE=2)]
    """
    ast = (
        build_function_expr(
            "editdistance",
            [e1, e2] if max_distance is None else [e1, e2, max_distance],
        )
        if _emit_ast
        else None
    )
    s1 = _to_col_if_str(e1, "editdistance")
    s2 = _to_col_if_str(e2, "editdistance")

    args = [s1, s2]
    if max_distance is not None:
        max_dist = (
            lit(max_distance, _emit_ast=False)
            if isinstance(max_distance, int)
            else _to_col_if_str(max_distance, "editdistance")
        )
        args.append(max_dist)

    return _call_function("editdistance", *args, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def instr(str: ColumnOrName, substr: str, _emit_ast: bool = True):
    """
    Locate the position of the first occurrence of substr column in the given string. Returns null if either of the arguments are null.

    Example::
        >>> df = session.create_dataframe([["hello world"], ["world hello"]], schema=["text"])
        >>> df.select(instr(col("text"), "world").alias("position")).collect()
        [Row(POSITION=7), Row(POSITION=1)]
    """
    s1 = _to_col_if_str(str, "instr")
    ans = position(lit(substr, _emit_ast=False), s1, _emit_ast=False)
    ans._ast = build_function_expr("instr", [str, substr]) if _emit_ast else None
    return ans


@publicapi
def normal(
    mean: Union[int, float],
    stddev: Union[int, float],
    gen: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
):
    """
    Generates a normally-distributed pseudo-random floating point number with specified mean and stddev (standard deviation).

    Example::
        >>> df = session.create_dataframe([1,2,3], schema=["a"])
        >>> df.select(normal(0, 1, "a").alias("normal")).collect()
        [Row(NORMAL=-1.143416214223267), Row(NORMAL=-0.78469958830255), Row(NORMAL=-0.365971322006404)]
    """
    # SNOW-1906511: normal function does not support passing mean and stddev as column name in the following way:
    # the following fails: SELECT normal("A", "A", 2) FROM ( SELECT $1 AS "A" FROM  VALUES (0 :: BIGINT))
    # but it supports reading from a table, we don't do type validation on mean and stddev here so users can still
    # use the functions on normal table
    ast = build_function_expr("normal", [mean, stddev, gen]) if _emit_ast else None
    mean = lit(mean, _emit_ast=False) if isinstance(mean, (int, float)) else mean
    stddev = (
        lit(stddev, _emit_ast=False) if isinstance(stddev, (int, float)) else stddev
    )
    gen = (
        lit(gen, _emit_ast=False)
        if isinstance(gen, (int, float))
        else _to_col_if_str(gen, "normal")
    )
    return _call_function("normal", mean, stddev, gen, _ast=ast, _emit_ast=_emit_ast)


@publicapi
def randn(
    seed: Optional[Union[ColumnOrName, int, float]] = None, _emit_ast: bool = True
) -> Column:
    """
    Generates a column with independent and identically distributed (i.i.d.) samples from the standard normal distribution.

    Example::
        >>> df = session.create_dataframe([1,2,3], schema=["seed"])
        >>> df.select(randn("seed").alias("randn")).collect()
        [Row(RANDN=-1.143416214223267), Row(RANDN=-0.78469958830255), Row(RANDN=-0.365971322006404)]
        >>> df.select(randn().alias("randn")).collect()  # doctest: +SKIP
    """
    ast = (
        build_function_expr("randn", [] if seed is None else [seed])
        if _emit_ast
        else None
    )
    if seed is None:
        seed = random(_emit_ast=False)  # pragma: no cover
    ans = normal(
        lit(0, _emit_ast=False),
        lit(1, _emit_ast=False),
        seed,
        _emit_ast=False,
    )
    ans._ast = ast
    return ans


@publicapi
def build_stage_file_url(
    stage_name: str, relative_file_path: str, _emit_ast: bool = True
) -> Column:
    """
    Generates a Snowflake file URL to a staged file using the stage name and relative file path as inputs.
    A file URL permits prolonged access to a specified file. That is, the file URL does not expire.
    The file URL is in the following format:

    ``https://<account_identifier>/api/files/<db_name>/<schema_name>/<stage_name>/<relative_path>``

    See more details `here <https://docs.snowflake.com/en/sql-reference/functions/build_stage_file_url#returns>`_.

    Args:
        stage_name: Name of the internal or external stage where the file is stored.
            If the stage name includes spaces or special characters, it must be enclosed in single quotes
            (e.g. '@"my stage"' for a stage named "my stage"). It has to be a constant instead of a column expression.
        relative_file_path: Path and filename of the file relative to its location in the stage.
            It has to be a constant instead of a column expression.

    Example::

        >>> df.select(build_stage_file_url("@images_stage", "/us/yosemite/half_dome.jpg").alias("url")).collect()  # doctest: +SKIP
    """
    return _call_function(
        "build_stage_file_url", stage_name, relative_file_path, _emit_ast=_emit_ast
    )


@private_preview(version="1.29.0")
@publicapi
def to_file(stage_file_uri: ColumnOrLiteral, _emit_ast: bool = True) -> Column:
    """
    Converts a stage file URI to a FILE value or NULL (if input is NULL), with the
    `metadata <https://docs.snowflake.com/LIMITEDACCESS/sql-reference/data-types-unstructured#file-data-type>`_
    related to the file.

    Args:
        stage_file_uri: The stage file URI to convert to a FILE value, e.g., ``@mystage/myfile.txt``.
            It has to be a constant instead of a column expression.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(to_file("@mystage/testCSV.csv").alias("file"))
        >>> result = json.loads(df.collect()[0][0])
        >>> result["RELATIVE_PATH"]
        'testCSV.csv'
        >>> result["CONTENT_TYPE"]
        'text/csv'
    """
    ast = build_function_expr("to_file", [stage_file_uri]) if _emit_ast else None
    c = _to_col_if_lit(stage_file_uri, "to_file")
    return _call_function("to_file", c, _ast=ast, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_content_type(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the content type (also known as mime type) of a FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_content_type(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        'text/csv'
    """
    function_name = "fl_get_content_type"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_etag(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the hash content (ETAG) of a FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_etag(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> len(df.collect()[0][0])  # doctest: +SKIP
    """
    function_name = "fl_get_etag"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_file_type(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the file type (modality) of a FILE. One of following values are returned:

        - document

        - video

        - audio

        - image

        - compressed

        - unknown

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_file_type(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        'document'
    """
    function_name = "fl_get_file_type"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_last_modified(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the last modified date of a FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_last_modified(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> type(df.collect()[0][0])
        <class 'datetime.datetime'>
    """
    function_name = "fl_get_last_modified"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_relative_path(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the relative path of a FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_relative_path(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        'testCSV.csv'
    """
    function_name = "fl_get_relative_path"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_scoped_file_url(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the scoped URL of a FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_scoped_file_url(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
    """
    function_name = "fl_get_scoped_file_url"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_size(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the size, in bytes, of a FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_size(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        32
    """
    function_name = "fl_get_size"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_stage(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the stage name of a FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_stage(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0].split(".")[-1]
        'MYSTAGE'
    """
    function_name = "fl_get_stage"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_get_stage_file_url(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the stage URL of a FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_get_stage_file_url(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]  # doctest: +SKIP
        'https://'
    """
    function_name = "fl_get_stage_file_url"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_is_audio(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Checks if the input is an audio FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_is_audio(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        False
    """
    function_name = "fl_is_audio"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_is_video(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Checks if the input is a video FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_is_video(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        False
    """
    function_name = "fl_is_video"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_is_document(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Checks if the input is a document FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_is_document(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        True
    """
    function_name = "fl_is_document"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_is_compressed(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Checks if the input is a compressed FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_is_compressed(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        False
    """
    function_name = "fl_is_compressed"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def fl_is_image(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Checks if the input is an image FILE.

    Example::

        >>> import json
        >>> # Create a temp stage.
        >>> _ = session.sql("create or replace temp stage mystage").collect()
        >>> # Upload a file to a stage.
        >>> r = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False, overwrite=True)
        >>> df = session.range(1).select(fl_is_image(to_file("@mystage/testCSV.csv")).alias("file"))
        >>> df.collect()[0][0]
        False
    """
    function_name = "fl_is_image"
    col_input = _to_col_if_str(e, function_name)
    return _call_function(function_name, col_input, _emit_ast=_emit_ast)


@private_preview(version="1.31.0")
@publicapi
def prompt(
    template_string: str,
    *exprs: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Constructs a structured OBJECT containing a template string and a list of arguments.
    This object is useful for dynamically formatting messages, constructing structured prompts,
    or storing formatted data for further processing, such as by Cortex AI functions.

    Args:
        template_string: A string containing numbered placeholders like {0}
            where the number is at least 0 and less than the number of expressions specified.
            The first expression is substituted for {0}, the second for {1}, and so on.
        *exprs: Expressions whose values will eventually be substituted into the template string
            in place of the numbered placeholders. These can be column names or other expressions.
            Values can be of any type coercible to a string (for example, VARCHAR, NUMBER, etc.), or FILE.

    Returns:
        A SQL OBJECT with the following structure:
        ``{ 'template': '<template_string>', 'args': ARRAY(<value_1>, <value_2>, ...) }``

        The args array contains the value of the expressions specified in the PROMPT function call.

    Note:

        - This function does not perform any string formatting itself.
          It is intended to construct an object to be consumed by Cortex AI functions.

        - It is an error to use a placeholder in the template string that does not have a corresponding expression,
          but it is not an error to have expressions that are not used in the template string.

    Examples::

        >>> df = session.create_dataframe([["Alice", "Monday"], ["Bob", "Tuesday"]], schema=["name", "day"])
        >>> df.select(prompt("Hello, {0}. Today is {1}", col("name"), col("day")).alias("greeting")).show()
        --------------------------------------------
        |"GREETING"                                |
        --------------------------------------------
        |{                                         |
        |  "args": [                               |
        |    "Alice",                              |
        |    "Monday"                              |
        |  ],                                      |
        |  "template": "Hello, {0}. Today is {1}"  |
        |}                                         |
        |{                                         |
        |  "args": [                               |
        |    "Bob",                                |
        |    "Tuesday"                             |
        |  ],                                      |
        |  "template": "Hello, {0}. Today is {1}"  |
        |}                                         |
        --------------------------------------------
        <BLANKLINE>
    """
    function_name = "prompt"
    ast = (
        build_function_expr(function_name, [template_string] + [expr for expr in exprs])
        if _emit_ast
        else None
    )
    exprs = [_to_col_if_str(expr, function_name) for expr in exprs]
    return _call_function(
        function_name, lit(template_string), *exprs, _ast=ast, _emit_ast=_emit_ast
    )


@private_preview(version="1.29.0")
@publicapi
def ai_filter(
    predicate: ColumnOrLiteralStr,
    file: Optional[ColumnOrLiteralStr] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Classifies free-form prompt inputs into a boolean. Currently supports both text and image filtering.

    Args:
        predicate: If you’re specifying an input string, it is string containing the text to be classified;
            If you’re filtering on one file, it is a string containing the instructions to
            classify the file input as either TRUE or FALSE.
        file:The column that the file is classified by based on the instructions specified in ``predicate``.
            You can use IMAGE FILE as an input to the AI_FILTER function.

    Note:
        For more complicated prompts, especially with multiple file columns, you can use the :func:`prompt()`
        function to help with creating an input, which supports formatting across both strings and FILE datatypes.

    Examples::

        >>> # for text
        >>> session.range(1).select(ai_filter('Is Canada in North America?').alias("answer")).show()
        ------------
        |"ANSWER"  |
        ------------
        |True      |
        ------------
        <BLANKLINE>
        >>> # use prompt function
        >>> df = session.create_dataframe(["Switzerland", "Korea"], schema=["country"])
        >>> df.select(
        ...     ai_filter(prompt("Is {0} in Asia?", col("country"))).as_("asia"),
        ...     ai_filter(prompt("Is {0} in Europe?", col("country"))).as_("europe"),
        ...     ai_filter(prompt("Is {0} in North America?", col("country"))).as_("north_america"),
        ...     ai_filter(prompt("Is {0} in Central America?", col("country"))).as_("central_america"),
        ... ).show()
        -----------------------------------------------------------
        |"ASIA"  |"EUROPE"  |"NORTH_AMERICA"  |"CENTRAL_AMERICA"  |
        -----------------------------------------------------------
        |False   |True      |False            |False              |
        |True    |False     |False            |False              |
        -----------------------------------------------------------
        <BLANKLINE>
        >>> # for image
        >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
        >>> _ = session.file.put("tests/resources/dog.jpg", "@mystage", auto_compress=False)
        >>> df = session.range(1).select(ai_filter("is it a dog picture?", to_file("@mystage/dog.jpg")).alias("is_dog"))
        >>> df.show()
        ------------
        |"IS_DOG"  |
        ------------
        |True      |
        ------------
        <BLANKLINE>
    """
    sql_func_name = "ai_filter"
    predicate_col = _to_col_if_lit(predicate, sql_func_name)
    if file is None:
        ast = build_function_expr(sql_func_name, [predicate]) if _emit_ast else None
        return _call_function(
            sql_func_name, predicate_col, _ast=ast, _emit_ast=_emit_ast
        )
    else:
        ast = (
            build_function_expr(sql_func_name, [predicate, file]) if _emit_ast else None
        )
        expr_col = _to_col_if_lit(file, sql_func_name)
        return _call_function(
            sql_func_name, predicate_col, expr_col, _ast=ast, _emit_ast=_emit_ast
        )


@private_preview(version="1.31.0")
@publicapi
def ai_classify(
    expr: ColumnOrLiteralStr,
    list_of_categories: Union[Column, List[str]],
    _emit_ast: bool = True,
) -> Column:
    """
    Classifies text or images into categories that you specify.

    Args:
        expr: The string, image, or a SQL object from :func:`prompt()` that you’re classifying.
            If you’re classifying text, the input string is case sensitive.
            You might get different results if you use different capitalization.
        list_of_categories: An array of strings that represents the different categories.
            Categories are case-sensitive. The array must contain at least 2 and no more than 100 categories.
            If the requirements aren’t met, the function returns an error.

    Returns:
        A serialized object. The object's ``label`` field is a string that specifies the category to
        which the input belongs. If you specify invalid values for the arguments, an error is returned.

    Examples::

        >>> # for text
        >>> session.range(1).select(ai_classify('One day I will see the world', ['travel', 'cooking']).alias("answer")).show()
        -----------------------
        |"ANSWER"             |
        -----------------------
        |{                    |
        |  "label": "travel"  |
        |}                    |
        -----------------------
        <BLANKLINE>
        >>> df = session.create_dataframe([
        ...     ['France', ['North America', 'Europe', 'Asia']],
        ...     ['Singapore', ['North America', 'Europe', 'Asia']],
        ...     ['one day I will see the world', ['travel', 'cooking', 'dancing']],
        ...     ['my lobster bisque is second to none', ['travel', 'cooking', 'dancing']]
        ... ], schema=["data", "category"])
        >>> df.select("data", ai_classify(col("data"), col("category"))["label"].alias("class")).sort("data").show()
        ---------------------------------------------------
        |"DATA"                               |"CLASS"    |
        ---------------------------------------------------
        |France                               |"Europe"   |
        |Singapore                            |"Asia"     |
        |my lobster bisque is second to none  |"cooking"  |
        |one day I will see the world         |"travel"   |
        ---------------------------------------------------
        <BLANKLINE>
        >>> # for image
        >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
        >>> _ = session.file.put("tests/resources/dog.jpg", "@mystage", auto_compress=False)
        >>> df = session.range(1).select(
        ...     ai_classify(
        ...         prompt("Please help me classify the dog within this image {0}", to_file("@mystage/dog.jpg")),
        ...         ["French Bulldog", "Golden Retriever", "Bichon", "Cavapoo", "Beagle"]
        ...     ).alias("classes")
        ... )
        >>> df.show()
        ------------------------
        |"CLASSES"             |
        ------------------------
        |{                     |
        |  "label": "Cavapoo"  |
        |}                     |
        ------------------------
        <BLANKLINE>

    """
    sql_func_name = "ai_classify"
    ast = (
        build_function_expr(sql_func_name, [expr, list_of_categories])
        if _emit_ast
        else None
    )

    expr_col = _to_col_if_lit(expr, sql_func_name)
    if isinstance(list_of_categories, list) and all(
        isinstance(x, str) for x in list_of_categories
    ):
        cat_col = lit(
            list_of_categories, datatype=ArrayType(StringType()), _emit_ast=False
        )
    elif isinstance(list_of_categories, Column):
        cat_col = list_of_categories
    else:
        raise TypeError(
            f"list_of_categories must be a list of str or a Column, got {list_of_categories}"
        )

    return _call_function(
        sql_func_name, expr_col, cat_col, _ast=ast, _emit_ast=_emit_ast
    )


@private_preview(version="1.29.0")
@publicapi
def summarize_agg(expr: ColumnOrLiteralStr, _emit_ast: bool = True) -> Column:
    """
    Summarizes a column of text data.

    Args:
        expr: This is an expression that contains text for summarization, such as restaurant reviews or phone transcripts.

    Example::

        >>> df = session.create_dataframe([
        ...     [1, "Excellent"],
        ...     [1, "Excellent"],
        ...     [1, "Great"],
        ...     [1, "Mediocre"],
        ...     [2, "Terrible"],
        ...     [2, "Bad"],
        ... ], schema=["product_id", "review"])
        >>> summary_df = df.select(summarize_agg(col("review")))
        >>> summary_df.count()
        1
        >>> summary_df = df.group_by("product_id").agg(summarize_agg(col("review")))
        >>> summary_df.count()
        2
    """
    sql_func_name = "summarize_agg"
    ast = build_function_expr(sql_func_name, [expr]) if _emit_ast else None
    expr_col = _to_col_if_lit(expr, sql_func_name)
    return _call_function(sql_func_name, expr_col, _ast=ast, _emit_ast=_emit_ast)


@private_preview(version="1.29.0")
@publicapi
def ai_agg(
    expr: ColumnOrLiteralStr,
    task_description: ColumnOrLiteralStr,
    _emit_ast: bool = True,
) -> Column:
    """
    Aggregates a column of text data using a natural language task description.

    This function reduces a column of text by performing a natural language aggregation
    as described in the task description. For instance, it can summarize large datasets or
    extract specific insights.

    Args:
        expr: A column or literal string containing the text data on which the aggregation operation
            is to be performed.
        task_description: A plain English string that describes the aggregation task, such as
            "Summarize the product reviews for a blog post targeting consumers" or
            "Identify the most positive review and translate it into French and Polish, one word only".

    Example::

        >>> df = session.create_dataframe([
        ...     [1, "Excellent"],
        ...     [1, "Excellent"],
        ...     [1, "Great"],
        ...     [1, "Mediocre"],
        ...     [2, "Terrible"],
        ...     [2, "Bad"],
        ... ], schema=["product_id", "review"])
        >>> summary_df = df.select(ai_agg(col("review"), "Summarize the product reviews for a blog post targeting consumers"))
        >>> summary_df.count()
        1
        >>> summary_df = df.group_by("product_id").agg(ai_agg(col("review"), "Summarize the product reviews for a blog post targeting consumers"))
        >>> summary_df.count()
        2

    Note:
        For optimal performance, follow these guidelines:

            - Use plain English text for the task description.

            - Describe the text provided in the task description. For example, instead of a task description like "summarize", use "Summarize the phone call transcripts".

            - Describe the intended use case. For example, instead of "find the best review", use "Find the most positive and well-written restaurant review to highlight on the restaurant website".

            - Consider breaking the task description into multiple steps. For example, instead of "Summarize the new articles", use "You will be provided with news articles from various publishers presenting events from different points of view. Please create a concise and elaborative summary of source texts without missing any crucial information.".
    """
    sql_func_name = "ai_agg"
    ast = (
        build_function_expr(sql_func_name, [expr, task_description])
        if _emit_ast
        else None
    )
    expr_col = _to_col_if_lit(expr, sql_func_name)
    task_description_col = _to_col_if_lit(task_description, sql_func_name)
    return _call_function(
        sql_func_name, expr_col, task_description_col, _ast=ast, _emit_ast=_emit_ast
    )
