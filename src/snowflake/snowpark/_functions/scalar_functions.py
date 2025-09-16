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
def h3_cell_to_boundary(cell_id: ColumnOrName, _emit_ast: bool = True) -> Column:
    """Returns the boundary of an H3 cell as a GeoJSON polygon.

    Args:
        cell_id (ColumnOrName): The H3 cell IDs.

    Returns:
        Column: The boundary of the H3 cell as a GeoJSON polygon string.

    Example::
        >>> df = session.create_dataframe([613036919424548863, 577023702256844799], schema=["cell_id"])
        >>> result = df.select(h3_cell_to_boundary(df["cell_id"]).alias("boundary")).collect()
        >>> len(result) == 2
        True
    """
    c = _to_col_if_str(cell_id, "h3_cell_to_boundary")
    return builtin("h3_cell_to_boundary", _emit_ast=_emit_ast)(c)


@publicapi
def h3_cell_to_parent(
    cell_id: ColumnOrName, target_resolution: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """Returns the parent H3 cell at the specified target resolution.

    Args:
        cell_id (ColumnOrName): The H3 cell IDs.
        target_resolution (ColumnOrName) : The target resolution levels.

    Returns:
        Column: The parent H3 cell at the target resolution.

    Example::
        >>> from snowflake.snowpark.functions import col
        >>> df = session.create_dataframe([[613036919424548863, 7], [608533319805566975, 6]], schema=["cell_id", "target_resolution"])
        >>> df.select(h3_cell_to_parent(col("cell_id"), col("target_resolution")).alias("parent_cell")).collect()
        [Row(PARENT_CELL=608533319805566975), Row(PARENT_CELL=604029720295636991)]
    """
    cell_id_c = _to_col_if_str(cell_id, "h3_cell_to_parent")
    target_resolution_c = _to_col_if_str(target_resolution, "h3_cell_to_parent")
    return builtin("h3_cell_to_parent", _emit_ast=_emit_ast)(
        cell_id_c, target_resolution_c
    )


@publicapi
def h3_cell_to_point(cell_id: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns the center point of an H3 cell as a GeoJSON Point object.

    Args:
        cell_id (ColumnOrName): The H3 cell IDs.

    Returns:
        Column: GeoJSON Point objects representing the center points of the H3 cells.

    Example::
        >>> import json
        >>> df = session.create_dataframe([613036919424548863], schema=["cell_id"])
        >>> result = df.select(h3_cell_to_point(df["cell_id"]).alias("POINT")).collect()
        >>> assert len(result) == 1
        >>> point_json = json.loads(result[0]["POINT"])
        >>> assert point_json["type"] == "Point"
        >>> assert "coordinates" in point_json
        >>> assert len(point_json["coordinates"]) == 2
    """
    c = _to_col_if_str(cell_id, "h3_cell_to_point")
    return builtin("h3_cell_to_point", _emit_ast=_emit_ast)(c)


@publicapi
def h3_compact_cells(array_of_cell_ids: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns a compacted array of H3 cell IDs by merging cells at the same resolution into their parent cells when possible.

    Args:
        array_of_cell_ids (ColumnOrName): An array of H3 cell IDs to be compacted.

    Returns:
        Column: An array of compacted H3 cell IDs.

    Example::
        >>> df = session.create_dataframe([
        ...     [[622236750562230271, 622236750562263039, 622236750562295807, 622236750562328575, 622236750562361343, 622236750562394111, 622236750562426879, 622236750558396415]]
        ... ], schema=["cell_ids"])
        >>> df.select(h3_compact_cells(df["cell_ids"]).alias("compacted")).collect()
        [Row(COMPACTED='[\\n  622236750558396415,\\n  617733150935089151\\n]')]
    """
    c = _to_col_if_str(array_of_cell_ids, "h3_compact_cells")
    return builtin("h3_compact_cells", _emit_ast=_emit_ast)(c)


@publicapi
def h3_compact_cells_strings(
    array_of_cell_ids: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns a compacted array of H3 cell IDs by removing redundant cells that are covered by their parent cells at coarser resolutions.

    Args:
        array_of_cell_ids (ColumnOrName): An array of H3 cell ID strings to be compacted.

    Returns:
        Column: The compacted array of H3 cell ID strings.

    Example::

        >>> df = session.create_dataframe([[
        ...     ['8a2a10705507fff', '8a2a1070550ffff', '8a2a10705517fff', '8a2a1070551ffff',
        ...      '8a2a10705527fff', '8a2a1070552ffff', '8a2a10705537fff', '8a2a10705cdffff']
        ... ]], schema=["cell_ids"])
        >>> df.select(h3_compact_cells_strings("cell_ids").alias("compacted")).collect()
        [Row(COMPACTED='[\\n  "8a2a10705cdffff",\\n  "892a1070553ffff"\\n]')]
    """
    c = _to_col_if_str(array_of_cell_ids, "h3_compact_cells_strings")
    return builtin("h3_compact_cells_strings", _emit_ast=_emit_ast)(c)


@publicapi
def h3_coverage(
    geography_expression: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns an array of H3 cell IDs that cover the given geography at the specified resolution.

    Args:
        geography_expression (ColumnOrName) : A GEOGRAPHY object.
        target_resolution (ColumnOrName) : The target H3 resolution (0-15).

    Returns:
        Column: An array of H3 cell IDs as strings.

    Example::
        >>> from snowflake.snowpark.functions import to_geography, lit
        >>> df = session.create_dataframe([
        ...     ["POLYGON((-122.481889 37.826683,-122.479487 37.808548,-122.474150 37.808904,-122.476510 37.826935,-122.481889 37.826683))"]
        ... ], schema=["polygon_wkt"])
        >>> result = df.select(h3_coverage(to_geography(df["polygon_wkt"]), lit(8)).alias("h3_cells")).collect()
        >>> result
        [Row(H3_CELLS='[\\n  613196571539931135,\\n  613196571542028287,\\n  613196571548319743,\\n  613196571550416895,\\n  613196571560902655,\\n  613196571598651391\\n]')]
    """
    geography_col = _to_col_if_str(geography_expression, "h3_coverage")
    resolution_col = _to_col_if_str(target_resolution, "h3_coverage")
    return builtin("h3_coverage", _emit_ast=_emit_ast)(geography_col, resolution_col)


@publicapi
def h3_coverage_strings(
    geography_expression: ColumnOrName,
    target_resolution: Union[ColumnOrName, int],
    _emit_ast: bool = True,
) -> Column:
    """
    Returns an array of H3 cell identifiers as strings that cover the given geography at the specified resolution.

    Args:
        geography_expression (ColumnOrName): The GEOGRAPHY to cover.
        target_resolution (ColumnOrName, int): The H3 resolution level (0-15).

    Returns:
        Column: An array of H3 cell identifiers as strings.

    Example::

        >>> from snowflake.snowpark.functions import to_geography
        >>> df = session.create_dataframe([
        ...     "POLYGON((-122.481889 37.826683,-122.479487 37.808548,-122.474150 37.808904,-122.476510 37.826935,-122.481889 37.826683))"
        ... ], schema=["geo_wkt"])
        >>> df.select(h3_coverage_strings(to_geography(df["geo_wkt"]), 8).alias("h3_cells")).collect()
        [Row(H3_CELLS='[\\n  "8828308701fffff",\\n  "8828308703fffff",\\n  "8828308709fffff",\\n  "882830870bfffff",\\n  "8828308715fffff",\\n  "8828308739fffff"\\n]')]
    """
    geo_col = _to_col_if_str(geography_expression, "h3_coverage_strings")
    res_col = (
        target_resolution
        if isinstance(target_resolution, Column)
        else lit(target_resolution)
    )
    return builtin("h3_coverage_strings", _emit_ast=_emit_ast)(geo_col, res_col)


@publicapi
def h3_get_resolution(cell_id: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
        Returns the resolution of an H3 cell ID.

    Args:
        cell_id (ColumnOrName): The H3 cell ID.

    Returns:
        Column: The resolution of the H3 cell ID.

    Example::

        >>> df = session.create_dataframe([617540519050084351, 617540519050084352], schema=["cell_id"])
        >>> df.select(h3_get_resolution(df["cell_id"]).alias("resolution")).collect()
        [Row(RESOLUTION=9), Row(RESOLUTION=9)]
    """
    c = _to_col_if_str(cell_id, "h3_get_resolution")
    return builtin("h3_get_resolution", _emit_ast=_emit_ast)(c)


@publicapi
def h3_grid_disk(
    cell_id: ColumnOrName, k_value: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns an array of H3 cell IDs within k distance of the origin cell.

    Args:
        cell_id (ColumnOrName): The H3 cell ID as the center of the disk.
        k_value (ColumnOrName): The distance (number of rings) from the center cell.

    Returns:
        Column: An array of H3 cell IDs within the specified distance.

    Example::

        >>> df = session.create_dataframe([[617540519050084351, 1], [617540519050084351, 2]], schema=["cell_id", "k_value"])
        >>> df.select(h3_grid_disk("cell_id", "k_value").alias("grid_disk")).collect()
        [Row(GRID_DISK='[\\n  617540519050084351,\\n  617540519051657215,\\n  617540519050608639,\\n  617540519050870783,\\n  617540519050346495,\\n  617540519051395071,\\n  617540519051132927\\n]'), Row(GRID_DISK='[\\n  617540519050084351,\\n  617540519051657215,\\n  617540519050608639,\\n  617540519050870783,\\n  617540519050346495,\\n  617540519051395071,\\n  617540519051132927,\\n  617540519048249343,\\n  617540519048773631,\\n  617540519089143807,\\n  617540519088095231,\\n  617540519107756031,\\n  617540519108018175,\\n  617540519104086015,\\n  617540519103561727,\\n  617540519046414335,\\n  617540519047462911,\\n  617540519044579327,\\n  617540519044317183\\n]')]
    """
    cell_id_col = _to_col_if_str(cell_id, "h3_grid_disk")
    k_value_col = _to_col_if_str(k_value, "h3_grid_disk")
    return builtin("h3_grid_disk", _emit_ast=_emit_ast)(cell_id_col, k_value_col)


@publicapi
def h3_grid_distance(
    cell_id_1: ColumnOrName, cell_id_2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
        Returns the grid distance between two H3 cell IDs.

    Args:
        cell_id_1 (ColumnOrName): The first H3 cell ID column or value.
        cell_id_2 (ColumnOrName): The second H3 cell ID column or value.

    Returns:
        Column: The grid distance between the two H3 cells.

    Example::
        >>> df = session.create_dataframe([[617540519103561727, 617540519052967935]], schema=["cell_id_1", "cell_id_2"])
        >>> df.select(h3_grid_distance(df["cell_id_1"], df["cell_id_2"]).alias("distance")).collect()
        [Row(DISTANCE=5)]
    """
    cell_id_1 = _to_col_if_str(cell_id_1, "h3_grid_distance")
    cell_id_2 = _to_col_if_str(cell_id_2, "h3_grid_distance")
    return builtin("h3_grid_distance", _emit_ast=_emit_ast)(cell_id_1, cell_id_2)
