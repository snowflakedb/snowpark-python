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


@publicapi
def booland(expr1: ColumnOrName, expr2: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Computes the Boolean AND of two numeric expressions. In accordance with Boolean semantics:
        - Non-zero values (including negative numbers) are regarded as True.
        - Zero values are regarded as False.

    Args:
        expr1 (ColumnOrName): The first boolean expression.
        expr2 (ColumnOrName): The second boolean expression.

    Returns:
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

    Args:
        e (ColumnOrName): A numeric expression to be evaluated.

    Returns:
        - True if the expression is zero.
        - False if the expression is non-zero.
        - NULL if the expression is NULL.

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

    Args:
        expr1 (ColumnOrName): The first boolean expression.
        expr2 (ColumnOrName): The second boolean expression.

    Returns:
        - True if both expressions are non-zero or the first expression is non-zero and the second expression is zero or None.
        - False if both expressions are zero.
        - None if both expressions are None or the first expression is None and the second expression is zero.

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
    Computes the Boolean XOR of two numeric expressions (i.e. one of the expressions, but not both expressions, is True). In accordance with Boolean semantics:
        - Non-zero values (including negative numbers) are regarded as True.
        - Zero values are regarded as False.

    Args:
        expr1 (ColumnOrName): First numeric expression or a string name of the column.
        expr2 (ColumnOrName): Second numeric expression or a string name of the column.

    Returns:
        - True if exactly one of the expressions is non-zero.
        - False if both expressions are zero or both expressions are non-zero.
        - None if both expressions are None, or one expression is None and the other expression is zero.

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
        expr (ColumnOrName): The expression to decode.
        *args (ColumnOrName): Variable length argument list containing pairs of search values and
            result values, with an optional default value at the end.


    Returns:
        Column: The decoded result.

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
    Returns the largest value from a list of expressions, ignoring None values.
    If all argument values are None, the result is None.

    Args:
        columns (ColumnOrName): The name strings to compare.

    Returns:
        Column: The greatest value, ignoring None values.

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
    Returns the smallest value from a list of expressions, ignoring None values.
    If all argument values are None, the result is None.

    Args:
        columns (ColumnOrName): list of column or column names to compare.

    Returns:
        Column: The smallest value from the list of expressions, ignoring None values.

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
    Returns None if expr1 is equal to expr2, otherwise returns expr1.

    Args:
        expr1 (ColumnOrName): The first expression to compare.
        expr2 (ColumnOrName): The second expression to compare.

    Returns:
        Column: None if expr1 is equal to expr2, otherwise expr1.

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
    Returns expr2 if expr1 is not None, otherwise returns expr3.

    Args:
        expr1 (ColumnOrName): The expression to test for None.
        expr2 (ColumnOrName): The value to return if expr1 is not None.
        expr3 (ColumnOrName): The value to return if expr1 is None.

    Returns:
        Column: The result of the nvl2 function.

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
    Returns None if either argument is None; otherwise, returns the second argument.
    Note that REGR_VALX is a None-preserving function, while the more commonly-used NVL is a None-replacing function.

    Args:
        y (ColumnOrName): The dependent variable column.
        x (ColumnOrName): The independent variable column.

    Returns:
        Column: The result of the regr_valx function.

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

    Returns:
        Column: The H3 cell ID as a hexadecimal string.

    Example::
        >>> df = session.create_dataframe([617700171168612351], schema=["cell_id"])
        >>> df.select(h3_int_to_string(df["cell_id"]).alias("result")).collect()
        [Row(RESULT='89283087033ffff')]
    """
    c = _to_col_if_str(cell_id, "h3_int_to_string")
    return builtin("h3_int_to_string", _emit_ast=_emit_ast)(c)


def h3_grid_path(
    cell_id_1: ColumnOrName, cell_id_2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns the grid path between two H3 cell IDs as a JSON array of H3 cell IDs.

    Args:
        cell_id_1 (ColumnOrName): The starting H3 cell ID.
        cell_id_2 (ColumnOrName): The ending H3 cell ID.

    Returns:
        Column: A JSON array of H3 cell IDs representing the grid path.

    Examples::

        >>> df = session.create_dataframe([
        ...     [617540519103561727, 617540519052967935],
        ...     [617540519046414335, 617540519047462911]
        ... ], schema=["cell_id_1", "cell_id_2"])
        >>> df.select(h3_grid_path("cell_id_1", "cell_id_2").alias("grid_path")).collect()
        [Row(GRID_PATH='[\\n  617540519103561727,\\n  617540519046414335,\\n  617540519047462911,\\n  617540519044055039,\\n  617540519045103615,\\n  617540519052967935\\n]'), Row(GRID_PATH='[\\n  617540519046414335,\\n  617540519047462911\\n]')]
    """
    c1 = _to_col_if_str(cell_id_1, "h3_grid_path")
    c2 = _to_col_if_str(cell_id_2, "h3_grid_path")
    return builtin("h3_grid_path", _emit_ast=_emit_ast)(c1, c2)


@publicapi
def h3_is_pentagon(cell_id: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Returns true if the given H3 cell ID is a pentagon.

    Args:
        cell_id (ColumnOrName): The H3 cell IDs.

    Returns:
        Column: Whether each H3 cell ID is a pentagon.

    Example::
        >>> df = session.create_dataframe([613036919424548863], schema=["cell_id"])
        >>> df.select(h3_is_pentagon(df["cell_id"]).alias("is_pentagon")).collect()
        [Row(IS_PENTAGON=False)]
        >>> df = session.create_dataframe(['804dfffffffffff'], schema=["cell_id"])
        >>> df.select(h3_is_pentagon(df["cell_id"]).alias("is_pentagon")).collect()
        [Row(IS_PENTAGON=True)]
    """
    c = _to_col_if_str(cell_id, "h3_is_pentagon")
    return builtin("h3_is_pentagon", _emit_ast=_emit_ast)(c)


@publicapi
def h3_is_valid_cell(cell_id: ColumnOrName, _emit_ast: bool = True) -> Column:
    """
    Checks if the given H3 cell ID is valid.

    Args:
        cell_id (ColumnOrName): The H3 cell IDs to validate.

    Returns:
        Column: Whether each H3 cell ID is valid, returns True for valid H3 cell IDs, False for invalid ones, and None for None inputs.

    Example::

        >>> df = session.create_dataframe([613036919424548863, 123456789, None], schema=["cell_id"])
        >>> df.select(h3_is_valid_cell(df["cell_id"]).alias("is_valid")).collect()
        [Row(IS_VALID=True), Row(IS_VALID=False), Row(IS_VALID=None)]
    """
    c = _to_col_if_str(cell_id, "h3_is_valid_cell")
    return builtin("h3_is_valid_cell", _emit_ast=_emit_ast)(c)


@publicapi
def h3_latlng_to_cell(
    latitude: ColumnOrName,
    longitude: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the H3 cell ID for the given latitude, longitude, and resolution.

    Args:
        latitude (ColumnOrName): The latitude values.
        longitude (ColumnOrName): The longitude values.
        target_resolution (ColumnOrName): The target H3 resolution.

    Returns:
        Column: H3 cell ID for the given latitude, longitude, and resolution.

    Example::

        >>> df = session.create_dataframe([[52.516262, 13.377704, 8]], schema=["lat", "lng", "res"])
        >>> df.select(h3_latlng_to_cell(df["lat"], df["lng"], df["res"])).collect()
        [Row(H3_LATLNG_TO_CELL("LAT", "LNG", "RES")=613036919424548863)]
    """
    lat_col = _to_col_if_str(latitude, "h3_latlng_to_cell")
    lng_col = _to_col_if_str(longitude, "h3_latlng_to_cell")
    res_col = _to_col_if_str(target_resolution, "h3_latlng_to_cell")
    return builtin("h3_latlng_to_cell", _emit_ast=_emit_ast)(lat_col, lng_col, res_col)


@publicapi
def h3_latlng_to_cell_string(
    latitude: ColumnOrName,
    longitude: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the H3 cell ID string for the given latitude, longitude, and resolution.

    Args:
        latitude (ColumnOrName): The latitude values.
        longitude (ColumnOrName): The longitude values.
        target_resolution (ColumnOrName): The H3 resolution values (0-15).

    Returns:
        Column: H3 cell ID string for the given latitude, longitude, and resolution.

    Example::

        >>> df = session.create_dataframe([[52.516262, 13.377704, 8]], schema=["lat", "lng", "res"])
        >>> df.select(h3_latlng_to_cell_string("lat", "lng", "res")).collect()
        [Row(H3_LATLNG_TO_CELL_STRING("LAT", "LNG", "RES")='881f1d4887fffff')]
    """
    lat = _to_col_if_str(latitude, "h3_latlng_to_cell_string")
    lng = _to_col_if_str(longitude, "h3_latlng_to_cell_string")
    res = _to_col_if_str(target_resolution, "h3_latlng_to_cell_string")
    return builtin("h3_latlng_to_cell_string", _emit_ast=_emit_ast)(lat, lng, res)


@publicapi
def h3_point_to_cell(
    geography_point: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns the H3 cell ID for a given geography point at the specified resolution.

    Args:
        geography_point (ColumnOrName): The geography points.
        target_resolution (ColumnOrName): The target H3 resolution (0-15).

    Returns:
        Column: H3 cell ID for the given geography point at the specified resolution.

    Example::
        >>> from snowflake.snowpark.functions import col
        >>> df = session.sql("SELECT ST_POINT(13.377704, 52.516262) as geography_point, 8 as resolution")
        >>> df.select(h3_point_to_cell(col("geography_point"), col("resolution"))).collect()
        [Row(H3_POINT_TO_CELL("GEOGRAPHY_POINT", "RESOLUTION")=613036919424548863)]
    """
    geography_point_c = _to_col_if_str(geography_point, "h3_point_to_cell")
    target_resolution_c = _to_col_if_str(target_resolution, "h3_point_to_cell")
    return builtin("h3_point_to_cell", _emit_ast=_emit_ast)(
        geography_point_c, target_resolution_c
    )


@publicapi
def h3_point_to_cell_string(
    geography_point: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """Returns the H3 cell ID string for a given geography point at the specified resolution.

    Args:
        geography_point (ColumnOrName): The geography point to convert to H3 cell.
        target_resolution (ColumnOrName): The target H3 resolution (0-15).

    Returns:
        Column: H3 cell ID string for the given geography point at the specified resolution.

    Example::
        >>> from snowflake.snowpark.functions import col
        >>> df = session.sql("SELECT ST_POINT(13.377704, 52.516262) as point, 8 as resolution")
        >>> df.select(h3_point_to_cell_string(col("point"), col("resolution"))).collect()
        [Row(H3_POINT_TO_CELL_STRING("POINT", "RESOLUTION")='881f1d4887fffff')]
    """
    geography_point_col = _to_col_if_str(geography_point, "h3_point_to_cell_string")
    target_resolution_col = _to_col_if_str(target_resolution, "h3_point_to_cell_string")
    return builtin("h3_point_to_cell_string", _emit_ast=_emit_ast)(
        geography_point_col, target_resolution_col
    )


@publicapi
def h3_try_coverage(
    geography_expression: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns an array of H3 cell IDs that cover the given geography at the specified resolution.
    This function attempts to provide coverage of the geography using H3 cells, returning None
    if the operation cannot be performed.

    Args:
        geography_expression (ColumnOrName): The geography object to cover with H3 cells.
        target_resolution (ColumnOrName): The H3 resolution level (0-15) for the coverage.

    Returns:
        Column: An array of H3 cell IDs covering the geography or None if the operation cannot be performed.

    Example::
        >>> from snowflake.snowpark.functions import to_geography, lit
        >>> df = session.create_dataframe([
        ...     ("POLYGON((-122.4194 37.7749, -122.4094 37.7749, -122.4094 37.7849, -122.4194 37.7849, -122.4194 37.7749))",)
        ... ], schema=["geog"])
        >>> df.select(h3_try_coverage(to_geography(df["geog"]), lit(9)).alias("coverage")).collect()
        [Row(COVERAGE='[\\n  617700169957507071,\\n  617700169958031359,\\n  617700169958293503,\\n  617700169961701375,\\n  617700169961963519,\\n  617700169962487807,\\n  617700169963012095,\\n  617700169963798527,\\n  617700169964060671,\\n  617700169964322815,\\n  617700169964584959,\\n  617700169964847103,\\n  617700169965109247,\\n  617700169965371391,\\n  617700170001809407,\\n  617700170002857983,\\n  617700170017800191\\n]')]
    """
    geography_col = _to_col_if_str(geography_expression, "h3_try_coverage")
    resolution_col = _to_col_if_str(target_resolution, "h3_try_coverage")
    return builtin("h3_try_coverage", _emit_ast=_emit_ast)(
        geography_col, resolution_col
    )


@publicapi
def h3_try_coverage_strings(
    geography_expression: ColumnOrName,
    target_resolution: ColumnOrName,
    _emit_ast: bool = True,
) -> Column:
    """
    Returns an array of H3 cell IDs as strings that cover the given geography at the specified resolution.
    This function attempts to provide coverage of the geography using H3 cells, returning the cell IDs as strings.

    Args:
        geography_expression (ColumnOrName): A geography expression to be covered by H3 cells
        target_resolution (ColumnOrName): The H3 resolution level for the coverage, ranging from 0 to 15

    Returns:
        Column: An array of H3 cell ID strings that cover the input geography

    Examples::

        >>> from snowflake.snowpark.functions import to_geography
        >>> df = session.create_dataframe([
        ...     "POLYGON((-122.4194 37.7749, -122.4094 37.7749, -122.4094 37.7849, -122.4194 37.7849, -122.4194 37.7749))"
        ... ], schema=["geog"])
        >>> df.select(h3_try_coverage_strings(to_geography(df["geog"]), lit(9)).alias("coverage")).collect()
        [Row(COVERAGE='[\\n  "89283082803ffff",\\n  "8928308280bffff",\\n  "8928308280fffff",\\n  "89283082843ffff",\\n  "89283082847ffff",\\n  "8928308284fffff",\\n  "89283082857ffff",\\n  "89283082863ffff",\\n  "89283082867ffff",\\n  "8928308286bffff",\\n  "8928308286fffff",\\n  "89283082873ffff",\\n  "89283082877ffff",\\n  "8928308287bffff",\\n  "89283082aa7ffff",\\n  "89283082ab7ffff",\\n  "89283082b9bffff"\\n]')]"""
    from snowflake.snowpark.functions import builtin, lit

    g = _to_col_if_str(geography_expression, "h3_try_coverage_strings")
    r = (
        target_resolution
        if isinstance(target_resolution, Column)
        else lit(target_resolution)
    )
    return builtin("h3_try_coverage_strings", _emit_ast=_emit_ast)(g, r)


@publicapi
def h3_try_grid_distance(
    cell_id_1: ColumnOrName, cell_id_2: ColumnOrName, _emit_ast: bool = True
) -> Column:
    """
    Returns the grid distance between two H3 cell IDs. Returns None if the input is invalid.

    Args:
        cell_id_1 (ColumnOrName): First H3 cell ID.
        cell_id_2 (ColumnOrName): Second H3 cell ID.

    Returns:
        Column: The grid distance between the two H3 cell IDs or None if the input is invalid.

    Example::

        >>> df = session.create_dataframe([[582046271372525567, 581883543651614719]], schema=["cell_id_1", "cell_id_2"])
        >>> df.select(h3_try_grid_distance(df["cell_id_1"], df["cell_id_2"]).alias("result")).collect()
        [Row(RESULT=None)]
    """
    cell_id_1 = _to_col_if_str(cell_id_1, "h3_try_grid_distance")
    cell_id_2 = _to_col_if_str(cell_id_2, "h3_try_grid_distance")
    return builtin("h3_try_grid_distance", _emit_ast=_emit_ast)(cell_id_1, cell_id_2)
