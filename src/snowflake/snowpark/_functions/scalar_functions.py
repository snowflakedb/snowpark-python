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
def h3_polygon_to_cells(
    geography_polygon: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """Returns the H3 cell IDs contained by the input polygon at the specified resolution.

    Args:
        geography_polygon (ColumnOrName): A GEOGRAPHY object representing a polygon.
        target_resolution (ColumnOrName): The H3 resolution (0-15).
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: An array of H3 cell IDs as integers.

    Example::
        >>> from snowflake.snowpark.functions import to_geography, lit
        >>> df = session.create_dataframe([
        ...     ['POLYGON((-122.481889 37.826683,-122.479487 37.808548,-122.474150 37.808904,-122.476510 37.826935,-122.481889 37.826683))']
        ... ], schema=["polygon_wkt"])
        >>> df.select(h3_polygon_to_cells(to_geography(df["polygon_wkt"]), lit(9))).collect()
        [Row(H3_POLYGON_TO_CELLS(TO_GEOGRAPHY("POLYGON_WKT"), 9)='[\\n  617700171177525247,\\n  617700171225497599,\\n  617700171167563775,\\n  617700171167825919,\\n  617700171188011007,\\n  617700171168350207,\\n  617700171168612351,\\n  617700171176476671,\\n  617700171168874495\\n]')]
    """
    geography_polygon_c = _to_col_if_str(geography_polygon, "h3_polygon_to_cells")
    target_resolution_c = _to_col_if_str(target_resolution, "h3_polygon_to_cells")
    return builtin("h3_polygon_to_cells", _emit_ast=_emit_ast)(
        geography_polygon_c, target_resolution_c
    )


@publicapi
def h3_polygon_to_cells_strings(
    geography_polygon: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns an array of H3 cell IDs (as strings) that cover the given geography polygon at the specified resolution.

    Args:
        geography_polygon (ColumnOrName): The GEOGRAPHY polygon to convert to H3 cells.
        target_resolution (ColumnOrName): The H3 resolution level (0-15) for the output cells.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: An array of H3 cell IDs as strings.

    Example::
        >>> from snowflake.snowpark.functions import to_geography, lit
        >>> df = session.create_dataframe([
        ...     ['POLYGON((-122.481889 37.826683,-122.479487 37.808548,-122.474150 37.808904,-122.476510 37.826935,-122.481889 37.826683))']
        ... ], schema=['polygon_wkt'])
        >>> df.select(h3_polygon_to_cells_strings(to_geography(df['polygon_wkt']), lit(9))).collect()
        [Row(H3_POLYGON_TO_CELLS_STRINGS(TO_GEOGRAPHY("POLYGON_WKT"), 9)='[\\n  "892830870bbffff",\\n  "89283087397ffff",\\n  "89283087023ffff",\\n  "89283087027ffff",\\n  "8928308715bffff",\\n  "8928308702fffff",\\n  "89283087033ffff",\\n  "892830870abffff",\\n  "89283087037ffff"\\n]')]
    """
    geography_polygon_c = _to_col_if_str(
        geography_polygon, "h3_polygon_to_cells_strings"
    )
    target_resolution_c = _to_col_if_str(
        target_resolution, "h3_polygon_to_cells_strings"
    )
    return builtin("h3_polygon_to_cells_strings", _emit_ast=_emit_ast)(
        geography_polygon_c, target_resolution_c
    )


@publicapi
def h3_string_to_int(cell_id: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Converts an H3 cell ID from hexadecimal string representation to its integer representation.

    Args:
        cell_id (ColumnOrName): The H3 cell ID as a hexadecimal string.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: The H3 cell ID as an integer.

    Example::

        >>> df = session.create_dataframe([['89283087033FFFF']], schema=["cell_id"])
        >>> df.select(h3_string_to_int(df["cell_id"]).alias("result")).collect()
        [Row(RESULT=617700171168612351)]
    """
    c = _to_col_if_str(cell_id, "h3_string_to_int")
    return builtin("h3_string_to_int", _emit_ast=_emit_ast)(c)


@publicapi
def h3_try_polygon_to_cells_strings(
    geography_polygon: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns an array of H3 cell IDs as strings that cover the given geography polygon
    at the specified resolution. Returns NULL if the input is invalid.

    Args:
        geography_polygon (ColumnOrName): The GEOGRAPHY polygon.
        target_resolution (ColumnOrName): The target H3 resolution (0-15).

    Returns:
        Column: An array of H3 cell IDs as strings, or NULL if the input is invalid.

    Example::
        >>> from snowflake.snowpark.functions import to_geography, lit

        >>> df = session.create_dataframe([
        ...     ['POLYGON((-122.4194 37.7749, -122.4094 37.7749, -122.4094 37.7849, -122.4194 37.7849, -122.4194 37.7749))']
        ... ], schema=["polygon_wkt"])

        >>> df.select(h3_try_polygon_to_cells_strings(to_geography(df["polygon_wkt"]), lit(9))).collect()
        [Row(H3_TRY_POLYGON_TO_CELLS_STRINGS(TO_GEOGRAPHY("POLYGON_WKT"), 9)='[\\n  "8928308287bffff",\\n  "8928308280fffff",\\n  "89283082873ffff",\\n  "8928308286bffff",\\n  "89283082847ffff",\\n  "89283082863ffff",\\n  "89283082877ffff",\\n  "8928308280bffff",\\n  "89283082867ffff"\\n]')]
    """
    geography_col = _to_col_if_str(geography_polygon, "h3_try_polygon_to_cells_strings")
    resolution_col = _to_col_if_str(
        target_resolution, "h3_try_polygon_to_cells_strings"
    )
    return builtin("h3_try_polygon_to_cells_strings", _emit_ast=_emit_ast)(
        geography_col, resolution_col
    )


@publicapi
def h3_cell_to_children(
    cell_id: ColumnOrName, target_resolution: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns the children of an H3 cell at a specified target resolution.

    Args:
        cell_id (ColumnOrName): The H3 cell ID to get children for.
        target_resolution (ColumnOrName): The target resolution for the children cells.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: A JSON array string containing the children H3 cell IDs.

    Example::
        >>> from snowflake.snowpark.functions import col
        >>> df = session.create_dataframe([[613036919424548863, 9]], schema=["cell_id", "target_resolution"])
        >>> df.select(h3_cell_to_children(col("cell_id"), col("target_resolution")).alias("children")).collect()
        [Row(CHILDREN='[\\n  617540519050084351,\\n  617540519050346495,\\n  617540519050608639,\\n  617540519050870783,\\n  617540519051132927,\\n  617540519051395071,\\n  617540519051657215\\n]')]
    """
    cell_id_c = _to_col_if_str(cell_id, "h3_cell_to_children")
    target_resolution_c = _to_col_if_str(target_resolution, "h3_cell_to_children")
    return builtin("h3_cell_to_children", _emit_ast=_emit_ast)(
        cell_id_c, target_resolution_c
    )


@publicapi
def h3_cell_to_children_string(
    cell_id: ColumnOrName, target_resolution: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns the children of the given H3 cell at the specified target resolution as a JSON array string.

    Args:
        cell_id (ColumnOrName): Column containing the H3 cell ID.
        target_resolution (ColumnOrName): Column containing the target resolution for the children cells.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: A JSON array string containing the children H3 cell IDs.

    Example::
        >>> from snowflake.snowpark.functions import col
        >>> df = session.create_dataframe([["881F1D4887FFFFF", 9]], schema=["cell_id", "target_resolution"])
        >>> df.select(h3_cell_to_children_string(col("cell_id"), col("target_resolution"))).collect()
        [Row(H3_CELL_TO_CHILDREN_STRING("CELL_ID", "TARGET_RESOLUTION")='[\\n  "891f1d48863ffff",\\n  "891f1d48867ffff",\\n  "891f1d4886bffff",\\n  "891f1d4886fffff",\\n  "891f1d48873ffff",\\n  "891f1d48877ffff",\\n  "891f1d4887bffff"\\n]')]
    """
    cell_id_col = _to_col_if_str(cell_id, "h3_cell_to_children_string")
    target_resolution_col = _to_col_if_str(
        target_resolution, "h3_cell_to_children_string"
    )
    return builtin("h3_cell_to_children_string", _emit_ast=_emit_ast)(
        cell_id_col, target_resolution_col
    )


@publicapi
def h3_int_to_string(cell_id: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Converts an H3 cell ID from integer format to string format.

    Args:
        cell_id (ColumnOrName): The H3 cell ID as an integer.
        _emit_ast (bool, optional): Whether to emit the abstract syntax tree (AST). Defaults to True.

    Returns:
        Column: The H3 cell ID as a hexadecimal string.

    Example::
        >>> df = session.create_dataframe([617700171168612351], schema=["cell_id"])
        >>> df.select(h3_int_to_string(df["cell_id"]).alias("result")).collect()
        [Row(RESULT='89283087033ffff')]
    """
    c = _to_col_if_str(cell_id, "h3_int_to_string")
    return builtin("h3_int_to_string", _emit_ast=_emit_ast)(c)
