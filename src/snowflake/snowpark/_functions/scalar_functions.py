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
    lit,
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
def array_remove_at(
    array: ColumnOrName, position: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns an ARRAY with the element at the specified position removed.

    Args:
        array (ColumnOrName): Column containing the source ARRAY.
        position (ColumnOrName): Column containing a (zero-based) position in the source ARRAY.
            The element at this position is removed from the resulting ARRAY.
            A negative position is interpreted as an index from the back of the array (e.g. -1 removes the last element in the array).

    Returns:
        Column: The resulting ARRAY with the specified element removed.

    Example::

        >>> df = session.create_dataframe([([2, 5, 7], 0), ([2, 5, 7], -1), ([2, 5, 7], 10)], schema=["array_col", "position_col"])
        >>> df.select(array_remove_at("array_col", "position_col").alias("result")).collect()
        [Row(RESULT='[\\n  5,\\n  7\\n]'), Row(RESULT='[\\n  2,\\n  5\\n]'), Row(RESULT='[\\n  2,\\n  5,\\n  7\\n]')]
    """
    a = _to_col_if_str(array, "array_remove_at")
    p = _to_col_if_str(position, "array_remove_at")
    return builtin("array_remove_at", _emit_ast=_emit_ast)(a, p)


@publicapi
def as_boolean(variant: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Casts a VARIANT value to a boolean.

    Args:
        variant (ColumnOrName): A Column or column name containing VARIANT values to be cast to boolean.

    Returns:
        ColumnL The boolean values cast from the VARIANT input.

    Example::
        >>> from snowflake.snowpark.functions import to_variant, to_boolean
        >>> df = session.create_dataframe([
        ...     [True],
        ...     [False]
        ... ], schema=["a"])
        >>> df.select(as_boolean(to_variant(to_boolean(df["a"]))).alias("result")).collect()
        [Row(RESULT=True), Row(RESULT=False)]
    """
    c = _to_col_if_str(variant, "as_boolean")
    return builtin("as_boolean", _emit_ast=_emit_ast)(c)


@publicapi
def boolor_agg(e: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the logical OR of all non-NULL records in a group. If all records are NULL, returns NULL.

    Args:
        e (ColumnOrName): Boolean values to aggregate.

    Returns:
        Column: The logical OR aggregation result.

    Example::

        >>> df = session.create_dataframe([
        ...     [True, False, True],
        ...     [False, False, False],
        ...     [True, True, False],
        ...     [False, True, True]
        ... ], schema=["a", "b", "c"])
        >>> df.select(
        ...     boolor_agg(df["a"]).alias("boolor_a"),
        ...     boolor_agg(df["b"]).alias("boolor_b"),
        ...     boolor_agg(df["c"]).alias("boolor_c")
        ... ).collect()
        [Row(BOOLOR_A=True, BOOLOR_B=True, BOOLOR_C=True)]
    """
    c = _to_col_if_str(e, "boolor_agg")
    return builtin("boolor_agg", _emit_ast=_emit_ast)(c)


@publicapi
def chr(col: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Converts a Unicode code point (including 7-bit ASCII) into the character that matches the input Unicode.

    Args:
        col (ColumnOrName): Integer Unicode code points.

    Returns:
        Column: The corresponding character for each code point.

    Example::

        >>> df = session.create_dataframe([83, 33, 169, 8364, None], schema=['a'])
        >>> df.select(df.a, chr(df.a).as_('char')).sort(df.a).show()
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
    c = _to_col_if_str(col, "chr")
    return builtin("chr", _emit_ast=_emit_ast)(c)


@publicapi
def div0null(
    dividend: Union[ColumnOrName, int, float],
    divisor: Union[ColumnOrName, int, float],
    _emit_ast: bool = True,
) -> Column:
    """
    Performs division like the division operator (/), but returns 0 when the divisor is 0 or NULL (rather than reporting an error).

    Args:
        dividend (ColumnOrName, int, float): The dividend.
        divisor (ColumnOrName, int, float): The divisor.

    Returns:
        Column: The result of the division, with 0 returned for cases where the divisor is 0 or NULL.

    Example::

        >>> df = session.create_dataframe([[10, 2], [10, 0], [10, None]], schema=["dividend", "divisor"])
        >>> df.select(div0null(df["dividend"], df["divisor"]).alias("result")).collect()
        [Row(RESULT=Decimal('5.000000')), Row(RESULT=Decimal('0.000000')), Row(RESULT=Decimal('0.000000'))]
    """
    dividend_col = (
        lit(dividend)
        if isinstance(dividend, (int, float))
        else _to_col_if_str(dividend, "div0null")
    )
    divisor_col = (
        lit(divisor)
        if isinstance(divisor, (int, float))
        else _to_col_if_str(divisor, "div0null")
    )
    return builtin("div0null", _emit_ast=_emit_ast)(dividend_col, divisor_col)


@publicapi
def dp_interval_high(aggregated_column: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the high end of the confidence interval for a differentially private aggregate.
    This function is used with differential privacy aggregation functions to provide
    the upper bound of the confidence interval for the aggregated result.

    Args:
        aggregated_column (ColumnOrName): The result of a differential privacy aggregation function.

    Returns:
        Column: The high end of the confidence interval for the differentially private aggregate.

    Example::

        >>> from snowflake.snowpark.functions import sum as sum_
        >>> df = session.create_dataframe([[10], [20], [30]], schema=["num_claims"])
        >>> df.select(sum_(df["num_claims"]).alias("sum_claims")).select(dp_interval_high("sum_claims")).collect()
        [Row(DP_INTERVAL_HIGH("SUM_CLAIMS")=None)]
    """
    c = _to_col_if_str(aggregated_column, "dp_interval_high")
    return builtin("dp_interval_high", _emit_ast=_emit_ast)(c)


@publicapi
def dp_interval_low(aggregated_column: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the lower bound of the confidence interval for a differentially private aggregate. This function is used with differential privacy aggregation functions to provide statistical bounds on the results.

    Args:
        aggregated_column (ColumnOrName): The differentially private aggregate result.

    Returns:
        Column: The lower bound of the confidence interval.

    Example::

        >>> from snowflake.snowpark.functions import sum as sum_
        >>> df = session.create_dataframe([[10], [20], [30]], schema=["num_claims"])
        >>> result = df.select(sum_("num_claims").alias("sum_claims")).select(dp_interval_low("sum_claims").alias("interval_low"))
        >>> result.collect()
        [Row(INTERVAL_LOW=None)]
    """
    c = _to_col_if_str(aggregated_column, "dp_interval_low")
    return builtin("dp_interval_low", _emit_ast=_emit_ast)(c)


@publicapi
def hex_decode_binary(input_expr: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Decodes a hex-encoded string to binary data.

    Args:
        input_expr (:class:`ColumnOrName`): the hex-encoded string to decode.
    Returns:
        :class:`Column`: the decoded binary data.

    Example::

        >>> df = session.create_dataframe(['48454C4C4F', '576F726C64'], schema=['hex_string'])
        >>> df.select(hex_decode_binary(df['hex_string']).alias('decoded_binary')).collect()
        [Row(DECODED_BINARY=bytearray(b'HELLO')), Row(DECODED_BINARY=bytearray(b'World'))]
    """
    c = _to_col_if_str(input_expr, "hex_decode_binary")
    return builtin("hex_decode_binary", _emit_ast=_emit_ast)(c)


@publicapi
def last_query_id(num: ColumnOrName = None, _emit_ast: bool = True) -> Column:
    """
    Returns the query ID of the last statement executed in the current session.
    If num is specified, returns the query ID of the nth statement executed in the current session.

    Args:
        num (ColumnOrName, optional): The number of statements back to retrieve the query ID for. If None, returns the query ID of the last statement.

    Returns:
        Column: The query ID as a string.

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> result1 = df.select(last_query_id().alias("QUERY_ID")).collect()
        >>> assert len(result1) == 1
        >>> assert isinstance(result1[0]["QUERY_ID"], str)
        >>> assert len(result1[0]["QUERY_ID"]) > 0
        >>> result2 = df.select(last_query_id(1).alias("QUERY_ID")).collect()
        >>> assert len(result2) == 1
        >>> assert isinstance(result2[0]["QUERY_ID"], str)
        >>> assert len(result2[0]["QUERY_ID"]) > 0
    """
    if num is None:
        return builtin("last_query_id", _emit_ast=_emit_ast)()
    else:
        return builtin("last_query_id", _emit_ast=_emit_ast)(num)


@publicapi
def last_transaction(_emit_ast: bool = True) -> Column:
    """
    Returns the query ID of the last transaction committed or rolled back in the current session. If no transaction has been committed or rolled back in the current session, returns NULL.

    Returns:
        Column: The last transaction.

    Example::

        >>> df = session.create_dataframe([1])
        >>> result = df.select(last_transaction()).collect()
        >>> # Result will be None if no transaction has occurred
        >>> assert result[0]['LAST_TRANSACTION()'] is None or isinstance(result[0]['LAST_TRANSACTION()'], str)
    """
    return builtin("last_transaction", _emit_ast=_emit_ast)()
