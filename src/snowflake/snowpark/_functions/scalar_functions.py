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
