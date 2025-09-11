#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import Optional, Union

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import publicapi
from snowflake.snowpark.column import (
    Column,
    _to_col_if_str,
    _to_col_if_str_or_int,
)
from snowflake.snowpark._functions.general_functions import (
    builtin,
)


@publicapi
def all_user_names(_emit_ast: bool = True) -> Column:
    """
    Returns a list of all user names in the current account.

    Example::

        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(all_user_names()).collect()
        >>> assert result[0]['ALL_USER_NAMES()'] is not None
        >>> assert isinstance(result[0]['ALL_USER_NAMES()'], str)
    """
    return builtin("all_user_names", _emit_ast=_emit_ast)()


@publicapi
def current_account_name(_emit_ast: bool = True) -> Column:
    """Returns the name of the account used in the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_account_name()).collect()
        >>> assert result[0]['CURRENT_ACCOUNT_NAME()'] is not None
    """
    return builtin("current_account_name", _emit_ast=_emit_ast)()


@publicapi
def current_ip_address(_emit_ast: bool = True) -> Column:
    """
    Returns the IP address of the client that submitted the current SQL statement.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_ip_address()).collect()
        >>> assert result[0]['CURRENT_IP_ADDRESS()'] is not None
    """
    return builtin("current_ip_address", _emit_ast=_emit_ast)()


@publicapi
def current_role_type(_emit_ast: bool = True) -> Column:
    """Returns the type of the role in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_role_type()).collect()
        >>> assert result[0]['CURRENT_ROLE_TYPE()'] is not None
    """
    return builtin("current_role_type", _emit_ast=_emit_ast)()


@publicapi
def current_secondary_roles(_emit_ast: bool = True) -> Column:
    """Returns a JSON string that lists all secondary roles granted to the current user.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_secondary_roles()).collect()
        >>> assert result[0]['CURRENT_SECONDARY_ROLES()'] is not None
    """
    return builtin("current_secondary_roles", _emit_ast=_emit_ast)()


@publicapi
def current_client(_emit_ast: bool = True) -> Column:
    """
    Returns the name of the client application used to connect to Snowflake.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_client()).collect()
        >>> assert result[0]['CURRENT_CLIENT()'] is not None
    """
    return builtin("current_client", _emit_ast=_emit_ast)()


@publicapi
def current_organization_name(_emit_ast: bool = True) -> Column:
    """Returns the name of the organization for the account where the current user is logged in.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_organization_name()).collect()
        >>> assert result[0]['CURRENT_ORGANIZATION_NAME()'] is not None
    """
    return builtin("current_organization_name", _emit_ast=_emit_ast)()


@publicapi
def current_organization_user(_emit_ast: bool = True) -> Column:
    """
    Returns the name of the organization user currently logged into the system.

    Example:
        >>> result = session.create_dataframe([1]).select(current_organization_user().alias('RESULT')).collect()
        >>> assert result[0]['RESULT'] == None
    """
    return builtin("current_organization_user", _emit_ast=_emit_ast)()


@publicapi
def current_transaction(_emit_ast: bool = True) -> Column:
    """
    Returns the current transaction ID for the session, or NULL if no transaction is active.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_transaction()).collect()
        >>> assert result[0]['CURRENT_TRANSACTION()'] is None or isinstance(result[0]['CURRENT_TRANSACTION()'], str)
    """
    return builtin("current_transaction", _emit_ast=_emit_ast)()


@publicapi
def bitand_agg(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the bitwise AND of all non-NULL records in a group. If all records inside a group are NULL, returns NULL.

    Example::

        >>> df = session.create_dataframe([[15], [26], [12], [14], [8]], schema=["a"])
        >>> df.select(bitand_agg("a")).collect()
        [Row(BITAND_AGG("A")=8)]
    """
    c = _to_col_if_str(e, "bitand_agg")
    return builtin("bitand_agg", _emit_ast=_emit_ast)(c)


@publicapi
def bitor_agg(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the bitwise OR of all non-NULL records in a group. If all records inside a group are NULL, a NULL is returned.

    Example::

        >>> df = session.create_dataframe([[15], [26], [12], [14], [8]], schema=["a"])
        >>> df.select(bitor_agg("a").alias("result")).collect()
        [Row(RESULT=31)]
    """
    c = _to_col_if_str(e, "bitor_agg")
    return builtin("bitor_agg", _emit_ast=_emit_ast)(c)


@publicapi
def bitxor_agg(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the bitwise XOR of all non-NULL records in a group. If all records inside a group are NULL, the function returns NULL.

    Example::

        >>> df = session.create_dataframe([[15], [26], [12]], schema=["a"])
        >>> df.select(bitxor_agg("a")).collect()
        [Row(BITXOR_AGG("A")=25)]
    """
    c = _to_col_if_str(e, "bitxor_agg")
    return builtin("bitxor_agg", _emit_ast=_emit_ast)(c)


@publicapi
def bitand(
    expr1: ColumnOrName,
    expr2: ColumnOrName,
    padside: Optional[str] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the bitwise AND of two numeric expressions.

    Args:
        expr1: The first numeric expression.
        expr2: The second numeric expression.
        padside: Optional padding side parameter.

    Example::

        >>> df = session.create_dataframe([[1, 1], [2, 4], [16, 24]], schema=["a", "b"])
        >>> df.select(bitand("a", "b").alias("result")).collect()
        [Row(RESULT=1), Row(RESULT=0), Row(RESULT=16)]

    Additional Example with padside parameter::
        >>> from snowflake.snowpark.functions import to_binary
        >>> df = session.create_dataframe([['1010', '1011']], schema=["a", "b"])
        >>> result = df.select(bitand(to_binary("a"), to_binary("b"), padside="LEFT").alias("RESULT")).collect()
        >>> expected = b'\x10\x10'
        >>> actual = result[0]["RESULT"]
        >>> assert  isinstance(actual, bytearray)
        >>> assert actual == expected
    """
    c1 = _to_col_if_str(expr1, "bitand")
    c2 = _to_col_if_str(expr2, "bitand")

    if padside is not None:
        return builtin("bitand", _emit_ast=_emit_ast)(c1, c2, padside)
    else:
        return builtin("bitand", _emit_ast=_emit_ast)(c1, c2)


@publicapi
def bitor(
    expr1: ColumnOrName,
    expr2: ColumnOrName,
    padside: Optional[str] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the bitwise OR of two numeric expressions.

    Args:
        expr1: The first numeric expression.
        expr2: The second numeric expression.
        padside: Optional padding side parameter.

    Example::

        >>> df = session.create_dataframe([[1, 1], [2, 4], [16, 24]], schema=["a", "b"])
        >>> df.select(bitor("a", "b")).collect()
        [Row(BITOR("A", "B")=1), Row(BITOR("A", "B")=6), Row(BITOR("A", "B")=24)]

    Additional Example with padside parameter::
        >>> from snowflake.snowpark.functions import to_binary
        >>> df = session.create_dataframe([['1010', '1011']], schema=["a", "b"])
        >>> result = df.select(bitor(to_binary("a"), to_binary("b"), padside="LEFT").alias("RESULT")).collect()
        >>> expected = b'\x10\x11'
        >>> actual = result[0]["RESULT"]
        >>> assert  isinstance(actual, bytearray)
        >>> assert actual == expected
    """
    c1 = _to_col_if_str(expr1, "bitor")
    c2 = _to_col_if_str(expr2, "bitor")
    if padside is not None:
        return builtin("bitor", _emit_ast=_emit_ast)(c1, c2, padside)
    else:
        return builtin("bitor", _emit_ast=_emit_ast)(c1, c2)


@publicapi
def bitxor(
    expr1: ColumnOrName,
    expr2: ColumnOrName,
    padside: Optional[str] = None,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the bitwise XOR of two numeric expressions.

    Args:
        expr1: The first numeric expression.
        expr2: The second numeric expression.
        padside: Optional padding side specification.

    Example::

        >>> df = session.create_dataframe([[1, 1], [2, 4], [4, 2], [16, 24]], schema=["bit1", "bit2"])
        >>> df.select(bitxor("bit1", "bit2")).collect()
        [Row(BITXOR("BIT1", "BIT2")=0), Row(BITXOR("BIT1", "BIT2")=6), Row(BITXOR("BIT1", "BIT2")=6), Row(BITXOR("BIT1", "BIT2")=8)]

    Additional Example with padside parameter::
        >>> from snowflake.snowpark.functions import to_binary
        >>> df = session.create_dataframe([['1110', '1011']], schema=["a", "b"])
        >>> result = df.select(bitxor(to_binary("a"), to_binary("b"), padside="LEFT").alias("RESULT")).collect()
        >>> expected = b'\x01\x01'
        >>> actual = result[0]["RESULT"]
        >>> assert  isinstance(actual, bytearray)
        >>> assert actual == expected
    """
    c1 = _to_col_if_str(expr1, "bitxor")
    c2 = _to_col_if_str(expr2, "bitxor")

    if padside is not None:
        return builtin("bitxor", _emit_ast=_emit_ast)(c1, c2, padside)
    else:
        return builtin("bitxor", _emit_ast=_emit_ast)(c1, c2)


@publicapi
def getbit(
    integer_expr: ColumnOrName,
    bit_position: Union[ColumnOrName, int],
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the bit value at the specified position in an integer expression.
    The bit position is 0-indexed from the right (least significant bit).

    Example::

        >>> df = session.create_dataframe([11], schema=["a"])
        >>> df.select(getbit("a", 3)).collect()
        [Row(GETBIT("A", 3)=1)]
    """
    c = _to_col_if_str(integer_expr, "getbit")
    pos = (
        _to_col_if_str_or_int(bit_position, "getbit")
        if bit_position is not None
        else None
    )
    return builtin("getbit", _emit_ast=_emit_ast)(c, pos)


@publicapi
def getdate(_emit_ast: bool = True) -> Column:
    """
    Returns the current timestamp for the system in the local time zone.

    Args:
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        A :class:`~snowflake.snowpark.Column` with the current date and time.

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> result = df.select(getdate()).collect()
        >>> import datetime
        >>> assert isinstance(result[0]["GETDATE()"], datetime.datetime)
    """
    return builtin("getdate", _emit_ast=_emit_ast)()


@publicapi
def localtime(_emit_ast: bool = True) -> Column:
    """
    Returns the current time for the system.

    Args:
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        A :class:`~snowflake.snowpark.Column` with the current local time.

    Example::

        >>> import datetime
        >>> result = session.create_dataframe([1]).select(localtime()).collect()
        >>> assert isinstance(result[0]["LOCALTIME()"], datetime.time)
    """
    return builtin("localtime", _emit_ast=_emit_ast)()


@publicapi
def systimestamp(_emit_ast: bool = True) -> Column:
    """
    Returns the current timestamp for the system.

    Args:
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        A :class:`~snowflake.snowpark.Column` with the current system timestamp.

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> result = df.select(systimestamp()).collect()
        >>> import datetime
        >>> assert isinstance(result[0]["SYSTIMESTAMP()"], datetime.datetime)
    """
    return builtin("systimestamp", _emit_ast=_emit_ast)()


@publicapi
def invoker_role(_emit_ast: bool = True) -> Column:
    """
    Returns the name of the role that was active when the current stored procedure or user-defined function was called.

    Args:
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: A Snowflake `Column` object representing the name of the active role.

    Example::

        >>> df = session.create_dataframe([1])
        >>> result = df.select(invoker_role()).collect()
        >>> assert len(result) == 1
        >>> assert isinstance(result[0]["INVOKER_ROLE()"], str)
        >>> assert len(result[0]["INVOKER_ROLE()"]) > 0
    """
    return builtin("invoker_role", _emit_ast=_emit_ast)()


@publicapi
def invoker_share(_emit_ast: bool = True) -> Column:
    """
    Returns the name of the share that directly accessed the table or view where the INVOKER_SHARE
    function is invoked, otherwise the function returns None.

    Args:
        _emit_ast (bool, optional): A flag indicating whether to emit the abstract
                                    syntax tree (AST). Defaults to True.

    Returns:
        Column: A Snowflake `Column` object representing the name of the active share.

    Example::
        >>> df = session.create_dataframe([1])
        >>> result = df.select(invoker_share().alias("INVOKER_SHARE")).collect()
        >>> assert result[0]["INVOKER_SHARE"] is None
    """
    return builtin("invoker_share", _emit_ast=_emit_ast)()


@publicapi
def is_application_role_in_session(role_name: str, _emit_ast: bool = True) -> Column:
    """
    Verifies whether the application role is activated in the consumer’s current session.

    Args:
        role_name (str): The name of the application role to check.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        A :class:`~snowflake.snowpark.Column` indicating whether the specified application role is active in the current session.

    Example::

        >>> df = session.create_dataframe([1])
        >>> result = df.select(is_application_role_in_session('ANALYST')).collect()
        >>> assert len(result) == 1
        >>> assert isinstance(result[0]["IS_APPLICATION_ROLE_IN_SESSION('ANALYST')"], bool)
    """
    return builtin("is_application_role_in_session", _emit_ast=_emit_ast)(role_name)


@publicapi
def is_database_role_in_session(
    role_name: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns True if the specified database role is granted to the current user and is currently in use; otherwise, returns False.

    Args:
        role_name (ColumnOrName): The name of the database role to check. Can be a string or a Column.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: A Snowflake `Column` object representing the result of the check.

    Example::
        >>> from snowflake.snowpark.functions import lit
        >>> df = session.create_dataframe([1])
        >>> result = df.select(is_database_role_in_session(lit("PUBLIC")).alias("is_db_role_active")).collect()
        >>> assert len(result) == 1
        >>> assert isinstance(result[0]["IS_DB_ROLE_ACTIVE"], bool)
    """
    c = _to_col_if_str(role_name, "is_database_role_in_session")
    return builtin("is_database_role_in_session", _emit_ast=_emit_ast)(c)


@publicapi
def is_granted_to_invoker_role(role_name: str, _emit_ast: bool = True) -> Column:
    """
    Returns True if the role returned by the INVOKER_ROLE function inherits the privileges of the specified
    role in the argument based on the context in which the function is called.

    Args:
        role_name (str): The name of the role to check.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: A Snowflake `Column` object representing the result of the check.

    Example::
        >>> from snowflake.snowpark.functions import lit
        >>> df = session.create_dataframe([1])
        >>> result = df.select(is_granted_to_invoker_role('ANALYST').alias('RESULT')).collect()
        >>> assert len(result) == 1
        >>> assert isinstance(result[0]["RESULT"], bool)
    """
    return builtin("is_granted_to_invoker_role", _emit_ast=_emit_ast)(role_name)


@publicapi
def is_role_in_session(role: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns True if the specified role is granted to the current user and is currently in use; otherwise, returns False.

    Args:
        role (ColumnOrName): A Column or column name containing the role name to check.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: A Snowflake `Column` object representing the result of the check.

    Example::

        >>> df = session.create_dataframe([["ANALYST"], ["PUBLIC"], ["ACCOUNTADMIN"]], schema=["role_name"])
        >>> df.select(is_role_in_session(df["role_name"]).alias("is_role_active")).collect()
        [Row(IS_ROLE_ACTIVE=False), Row(IS_ROLE_ACTIVE=True), Row(IS_ROLE_ACTIVE=False)]
    """
    c = _to_col_if_str(role, "is_role_in_session")
    return builtin("is_role_in_session", _emit_ast=_emit_ast)(c)


@publicapi
def getvariable(name: str, _emit_ast: bool = True) -> Column:
    """
    Retrieves the value of a session variable by its name.

    Args:
        name (str): The name of the session variable to retrieve.
        _emit_ast (bool, optional): A flag indicating whether to emit the abstract syntax tree (AST).
                                    Defaults to True.

    Returns:
        Column: A Snowflake `Column` object representing the value of the specified session variable.

    Example::
        >>> result = session.create_dataframe([1]).select(getvariable("MY_VARIABLE").alias("RESULT")).collect()
        >>> assert result[0]["RESULT"] is None
    """
    return builtin("getvariable", _emit_ast=_emit_ast)(name)


@publicapi
def booland(expr1: ColumnOrName, expr2: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Computes the Boolean AND of two numeric expressions. In accordance with Boolean semantics:
        - Non-zero values (including negative numbers) are regarded as True.
        - Zero values are regarded as False.

    As a result, the function returns:
        - True if both expressions are non-zero.
        - False if both expressions are zero or one expression is zero and the other expression is non-zero or NULL.
        - NULL if both expressions are NULL or one expression is NULL and the other expression is non-zero.

    Example::

        >>> from snowflake.snowpark.functions import col
        >>> df = session.create_dataframe([[1, -2], [0, 2], [0, 0], [5, 3]], schema=["a", "b"])
        >>> df.select(booland(col("a"), col("b")).alias("result")).collect()
        [Row(RESULT=True), Row(RESULT=False), Row(RESULT=False), Row(RESULT=True)]
    """
    c1 = _to_col_if_str(expr1, "booland")
    c2 = _to_col_if_str(expr2, "booland")
    return builtin("booland", _emit_ast=_emit_ast)(c1, c2)


@publicapi
def boolnot(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Computes the Boolean NOT of a single numeric expression. In accordance with Boolean semantics:
        - Non-zero values (including negative numbers) are regarded as True.
        - Zero values are regarded as False.

    As a result, the function returns:
        - True if the expression is zero.
        - False if the expression is non-zero.
        - NULL if the expression is NULL.

    Args:
        e: A numeric expression (Column or column name) to be evaluated.
        _emit_ast: Whether to emit the AST node for this function. This is for internal use only.

    Returns:
        A `~snowflake.snowpark.column` representing the Boolean NOT of the input expression.

    Example::

        >>> df = session.create_dataframe([0, 10, -5], schema=["a"])
        >>> df.select(boolnot("a")).collect()
        [Row(BOOLNOT("A")=True), Row(BOOLNOT("A")=False), Row(BOOLNOT("A")=False)]
    """
    c = _to_col_if_str(e, "boolnot")
    return builtin("boolnot", _emit_ast=_emit_ast)(c)


@publicapi
def boolor(expr1: ColumnOrName, expr2: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Computes the Boolean OR of two numeric expressions. In accordance with Boolean semantics:
        - Non-zero values (including negative numbers) are regarded as True.
        - Zero values are regarded as False.

    As a result, the function returns:
        - True if both expressions are non-zero or one expression is non-zero and the other expression is zero or None.
        - False if both expressions are zero.
        - None if both expressions are None or one expression is None and the other expression is zero.

    Args:
        expr1: A :class:`Column` or column name representing the first boolean expression.
        expr2: A :class:`Column` or column name representing the second boolean expression.

    Returns:
        A :class:`Column` object representing the logical OR result.

    Example::

        >>> from snowflake.snowpark.functions import col
        >>> df = session.create_dataframe([
        ...     [1, 2],
        ...     [-1, 0],
        ...     [3, None],
        ...     [0, 0],
        ...     [None, 0],
        ...     [None, None]
        ... ], schema=["expr1", "expr2"])
        >>> df.select(boolor(col("expr1"), col("expr2")).alias("result")).collect()
        [Row(RESULT=True), Row(RESULT=True), Row(RESULT=True), Row(RESULT=False), Row(RESULT=None), Row(RESULT=None)]
    """
    c1 = _to_col_if_str(expr1, "boolor")
    c2 = _to_col_if_str(expr2, "boolor")
    return builtin("boolor", _emit_ast=_emit_ast)(c1, c2)


@publicapi
def boolxor(expr1: ColumnOrName, expr2: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Computes the Boolean XOR of two numeric expressions (i.e. one of the expressions, but not both expressions, is TRUE).
    In accordance with Boolean semantics:
        - Non-zero values (including negative numbers) are regarded as True.
        - Zero values are regarded as False.
    As a result, the function returns:
        - True if one expression is non-zero and the other expression is zero.
        - False if both expressions are non-zero or both expressions are zero.
        - NULL if one or both expressions are NULL.
    Args:
        expr1: First numeric expression or a string name of the column.
        expr2: Second numeric expression or a string name of the column.
        _emit_ast: Whether to emit the AST for this function. This is for internal use only.
    Example::
        >>> from snowflake.snowpark.functions import col
        >>> df = session.create_dataframe([[2, 0], [1, -1], [0, 0], [None, 3]], schema=["a", "b"])
        >>> df.select(boolxor(col("a"), col("b")).alias("result")).collect()
        [Row(RESULT=True), Row(RESULT=False), Row(RESULT=False), Row(RESULT=None)]
    """
    c1 = _to_col_if_str(expr1, "boolxor")
    c2 = _to_col_if_str(expr2, "boolxor")
    return builtin("boolxor", _emit_ast=_emit_ast)(c1, c2)


@publicapi
def decode(expr: ColumnOrName, *args: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Decodes an expression by comparing it with search values and returning corresponding result values.

    Similar to a Case statement, this function compares an expression to one or more search values
    and returns the corresponding result when a match is found.

    Args:
        expr (ColumnOrName): The expression to decode. Can be a Column or column name string.
        *args (ColumnOrName): Variable length argument list containing pairs of search values and
            result values, with an optional default value at the end.
        _emit_ast (bool, optional): Internal parameter for AST generation. Defaults to True.

    Returns:
        Column: A Column object representing the decoded result.

    Example:

        >>> from snowflake.snowpark.functions import col, lit
        >>> df = session.create_dataframe([[1, 1], [2, 4], [16, 24]], schema=["a", "b"])
        >>> df.select(decode(col("a"), lit(1), lit("one"), lit(2), lit("two"), lit("default")).alias("RESULT")).collect()
        [Row(RESULT='one'), Row(RESULT='two'), Row(RESULT='default')]
    """
    expr_col = _to_col_if_str(expr, "decode")
    arg_cols = [_to_col_if_str(arg, "decode") for arg in args]
    return builtin("decode", _emit_ast=_emit_ast)(expr_col, *arg_cols)


@publicapi
def greatest_ignore_nulls(*columns: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the largest value from a list of expressions, ignoring NULL values.
    If all argument values are NULL, the result is NULL.

    Args:
        columns (ColumnOrName): A variable number of Column or column name strings to compare.
        _emit_ast (bool, optional): Internal parameter for AST generation. Defaults to True.

    Returns:
        Column: A `~snowflake.snowpark.Column` object representing the greatest value, ignoring NULLs.

    Examples::

        >>> df = session.create_dataframe([[1, 2, 3, 4.25], [2, 4, -1, None], [3, 6, None, -2.75]], schema=["a", "b", "c", "d"])
        >>> df.select(greatest_ignore_nulls(df["a"], df["b"], df["c"], df["d"]).alias("greatest_ignore_nulls")).collect()
        [Row(GREATEST_IGNORE_NULLS=4.25), Row(GREATEST_IGNORE_NULLS=4.0), Row(GREATEST_IGNORE_NULLS=6.0)]
    """
    c = [_to_col_if_str(ex, "greatest_ignore_nulls") for ex in columns]
    return builtin("greatest_ignore_nulls", _emit_ast=_emit_ast)(*c)


@publicapi
def least_ignore_nulls(*columns: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the smallest value from a list of expressions, ignoring NULL values.
    If all argument values are NULL, the result is NULL.

    Args:
        columns: list of :class:`Column` or column names to compare.
        _emit_ast: Whether to emit the AST node for this function. This is for internal use only.

    Returns:
        :class:`Column`: A column containing the smallest value from the list of expressions, ignoring NULL values.

    Example::

        >>> df = session.create_dataframe([[1, 2, 3], [2, 4, -1], [3, 6, None]], schema=["a", "b", "c"])
        >>> df.select(least_ignore_nulls(df["a"], df["b"], df["c"]).alias("least_ignore_nulls")).collect()
        [Row(LEAST_IGNORE_NULLS=1), Row(LEAST_IGNORE_NULLS=-1), Row(LEAST_IGNORE_NULLS=3)]
    """
    c = [_to_col_if_str(ex, "least_ignore_nulls") for ex in columns]
    return builtin("least_ignore_nulls", _emit_ast=_emit_ast)(*c)


@publicapi
def nullif(expr1: ColumnOrName, expr2: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns NULL if expr1 is equal to expr2, otherwise returns expr1.

    Args:
        expr1: The first expression to compare. Can be a :class:`Column` or a column name.
        expr2: The second expression to compare. Can be a :class:`Column` or a column name.
        _emit_ast: Whether to emit the AST node for this function. This is for internal use only.

    Returns:
        :class:`Column`: A column containing NULL if expr1 is equal to expr2, otherwise expr1.

    Example::

        >>> df = session.create_dataframe([[0, 0], [0, 1], [1, 0], [1, 1], [None, 0]], schema=["a", "b"])
        >>> df.select(nullif(df["a"], df["b"]).alias("result")).collect()
        [Row(RESULT=None), Row(RESULT=0), Row(RESULT=1), Row(RESULT=None), Row(RESULT=None)]
    """
    c1 = _to_col_if_str(expr1, "nullif")
    c2 = _to_col_if_str(expr2, "nullif")
    return builtin("nullif", _emit_ast=_emit_ast)(c1, c2)


@publicapi
def nvl2(
    expr1: ColumnOrName,
    expr2: ColumnOrName,
    expr3: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns expr2 if expr1 is not NULL, otherwise returns expr3.

    Args:
        expr1: The expression to test for NULL.
        expr2: The value to return if expr1 is not NULL.
        expr3: The value to return if expr1 is NULL.
        _emit_ast: Whether to emit the AST node for this function. This is for internal use only.

    Returns:
        A :class:`Column` representing the result of the nvl2 function.

    Example::

        >>> from snowflake.snowpark.functions import col
        >>> df = session.create_dataframe([
        ...     [0, 5, 3],
        ...     [0, 5, None],
        ...     [0, None, 3],
        ...     [None, 5, 3],
        ...     [None, None, 3]
        ... ], schema=["a", "b", "c"])
        >>> df.select(nvl2(col("a"), col("b"), col("c")).alias("nvl2_result")).collect()
        [Row(NVL2_RESULT=5), Row(NVL2_RESULT=5), Row(NVL2_RESULT=None), Row(NVL2_RESULT=3), Row(NVL2_RESULT=3)]
    """
    c1 = _to_col_if_str(expr1, "nvl2")
    c2 = _to_col_if_str(expr2, "nvl2")
    c3 = _to_col_if_str(expr3, "nvl2")
    return builtin("nvl2", _emit_ast=_emit_ast)(c1, c2, c3)


@publicapi
def regr_valx(y: ColumnOrName, x: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns NULL if either argument is NULL; otherwise, returns the second argument.
    Note that REGR_VALX is a NULL-preserving function, while the more commonly-used NVL is a NULL-replacing function.

    Args:
        y: The dependent variable column.
        x: The independent variable column.
        _emit_ast: Whether to emit the AST node for this function. This is for internal use only.

    Returns:
        A :class:`Column` representing the result of the regr_valx function.

    Example::

        >>> from snowflake.snowpark import Row
        >>> df = session.create_dataframe([[2.0, 1.0], [None, 3.0], [6.0, None]], schema=["col_y", "col_x"])
        >>> result = df.select(regr_valx(df["col_y"], df["col_x"]).alias("result")).collect()
        >>> assert result == [Row(RESULT=1.0), Row(RESULT=None), Row(RESULT=None)]

        Important: Note the order of the arguments; y precedes x
    """
    y_col = _to_col_if_str(y, "regr_valx")
    x_col = _to_col_if_str(x, "regr_valx")
    return builtin("regr_valx", _emit_ast=_emit_ast)(y_col, x_col)
