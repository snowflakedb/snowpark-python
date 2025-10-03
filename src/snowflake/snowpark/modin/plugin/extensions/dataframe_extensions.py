#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing DataFrame APIs defined in Snowpark pandas but not the Modin API layer, such
as `DataFrame.to_snowflake`.
"""

from collections.abc import Iterable
import functools
from typing import Any, List, Literal, Optional, Union

import modin.pandas as pd
from modin.pandas.api.extensions import (
    register_dataframe_accessor as _register_dataframe_accessor,
)
import pandas
from pandas._typing import IndexLabel

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.async_job import AsyncJob
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.modin.plugin.extensions.utils import (
    add_cache_result_docstring,
    pandas_to_snowflake,
    register_non_snowflake_accessors,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    materialization_warning,
)
from snowflake.snowpark.row import Row

register_dataframe_accessor = functools.partial(
    _register_dataframe_accessor, backend="Snowflake"
)

register_non_snowflake_accessors(_register_dataframe_accessor, "DataFrame")


# Implementation note: Arguments names and types are kept consistent with pandas.DataFrame.to_sql
@register_dataframe_accessor("to_snowflake", backend="Snowflake")
def to_snowflake(
    self,
    name: Union[str, Iterable[str]],
    if_exists: Optional[Literal["fail", "replace", "append"]] = "fail",
    index: bool = True,
    index_label: Optional[IndexLabel] = None,
    table_type: Literal["", "temp", "temporary", "transient"] = "",
) -> None:
    """
    Save the Snowpark pandas DataFrame as a Snowflake table.

    Args:
        name:
            Name of the SQL table or fully-qualified object identifier
        if_exists:
            How to behave if table already exists. default 'fail'
                - fail: Raise ValueError.
                - replace: Drop the table before inserting new values.
                - append: Insert new values to the existing table. The order of insertion is not guaranteed.
        index: default True
            If true, save DataFrame index columns as table columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
        table_type:
            The table type of table to be created. The supported values are: ``temp``, ``temporary``,
            and ``transient``. An empty string means to create a permanent table. Learn more about table
            types `here <https://docs.snowflake.com/en/user-guide/tables-temp-transient.html>`_.

    See also:
        - :func:`to_snowflake <modin.pandas.to_snowflake>`
        - :func:`Series.to_snowflake <modin.pandas.Series.to_snowflake>`
        - :func:`read_snowflake <modin.pandas.read_snowflake>`

    """
    self._query_compiler.to_snowflake(name, if_exists, index, index_label, table_type)


register_dataframe_accessor("to_snowflake", backend="Pandas")(pandas_to_snowflake)


@register_dataframe_accessor("to_snowpark")
def to_snowpark(
    self, index: bool = True, index_label: Optional[IndexLabel] = None
) -> SnowparkDataFrame:
    """
    Convert the Snowpark pandas DataFrame to a Snowpark DataFrame.
    Note that once converted to a Snowpark DataFrame, no ordering information will be preserved. You can call
    reset_index to generate a default index column that is the same as the row position before the call to_snowpark.

    Args:
        index: bool, default True.
            Whether to keep the index columns in the result Snowpark DataFrame. If True, the index columns
            will be the first set of columns. Otherwise, no index column will be included in the final Snowpark
            DataFrame.
        index_label: IndexLabel, default None.
            Column label(s) to use for the index column(s). If None is given (default) and index is True,
            then the original index column labels are used. A sequence should be given if the DataFrame uses
            MultiIndex, and the length of the given sequence should be the same as the number of index columns.

    Returns:
        Snowpark :class:`~snowflake.snowpark.dataframe.DataFrame`
            A Snowpark DataFrame contains the index columns if index=True and all data columns of the Snowpark pandas
            DataFrame. The identifier for the Snowpark DataFrame will be the normalized quoted identifier with
            the same name as the pandas label.

    Raises:
         ValueError if duplicated labels occur among the index and data columns.
         ValueError if the label used for a index or data column is None.

    See also:
        - :func:`to_snowpark <modin.pandas.to_snowpark>`
        - :func:`Series.to_snowpark <modin.pandas.Series.to_snowpark>`

    Note:
        The labels of the Snowpark pandas DataFrame or index_label provided will be used as Normalized Snowflake
        Identifiers of the Snowpark DataFrame.
        For details about Normalized Snowflake Identifiers, please refer to the Note in :func:`~modin.pandas.read_snowflake`

    Examples::

        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]})
        >>> df
           Animal  Max Speed
        0  Falcon      380.0
        1  Falcon      370.0
        2  Parrot       24.0
        3  Parrot       26.0
        >>> snowpark_df = df.to_snowpark(index_label='Order')
        >>> snowpark_df.order_by('"Max Speed"').show()
        ------------------------------------
        |"Order"  |"Animal"  |"Max Speed"  |
        ------------------------------------
        |2        |Parrot    |24.0         |
        |3        |Parrot    |26.0         |
        |1        |Falcon    |370.0        |
        |0        |Falcon    |380.0        |
        ------------------------------------
        <BLANKLINE>
        >>> snowpark_df = df.to_snowpark(index=False)
        >>> snowpark_df.order_by('"Max Speed"').show()
        --------------------------
        |"Animal"  |"Max Speed"  |
        --------------------------
        |Parrot    |24.0         |
        |Parrot    |26.0         |
        |Falcon    |370.0        |
        |Falcon    |380.0        |
        --------------------------
        <BLANKLINE>
        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]}, index=pd.Index([3, 5, 6, 7], name="id"))
        >>> df      # doctest: +NORMALIZE_WHITESPACE
            Animal  Max Speed
        id
        3  Falcon      380.0
        5  Falcon      370.0
        6  Parrot       24.0
        7  Parrot       26.0
        >>> snowpark_df = df.to_snowpark()
        >>> snowpark_df.order_by('"id"').show()
        ---------------------------------
        |"id"  |"Animal"  |"Max Speed"  |
        ---------------------------------
        |3     |Falcon    |380.0        |
        |5     |Falcon    |370.0        |
        |6     |Parrot    |24.0         |
        |7     |Parrot    |26.0         |
        ---------------------------------
        <BLANKLINE>

        MultiIndex usage

        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]},
        ...                    index=pd.MultiIndex.from_tuples([('bar', 'one'), ('foo', 'one'), ('bar', 'two'), ('foo', 'three')], names=['first', 'second']))
        >>> df      # doctest: +NORMALIZE_WHITESPACE
                        Animal  Max Speed
        first second
        bar   one     Falcon      380.0
        foo   one     Falcon      370.0
        bar   two     Parrot       24.0
        foo   three   Parrot       26.0
        >>> snowpark_df = df.to_snowpark(index=True, index_label=['A', 'B'])
        >>> snowpark_df.order_by('"A"', '"B"').show()
        ----------------------------------------
        |"A"  |"B"    |"Animal"  |"Max Speed"  |
        ----------------------------------------
        |bar  |one    |Falcon    |380.0        |
        |bar  |two    |Parrot    |24.0         |
        |foo  |one    |Falcon    |370.0        |
        |foo  |three  |Parrot    |26.0         |
        ----------------------------------------
        <BLANKLINE>
        >>> snowpark_df = df.to_snowpark(index=False)
        >>> snowpark_df.order_by('"Max Speed"').show()
        --------------------------
        |"Animal"  |"Max Speed"  |
        --------------------------
        |Parrot    |24.0         |
        |Parrot    |26.0         |
        |Falcon    |370.0        |
        |Falcon    |380.0        |
        --------------------------
        <BLANKLINE>
    """
    return self._query_compiler.to_snowpark(index, index_label)


@register_dataframe_accessor("to_pandas")
@materialization_warning
def to_pandas(
    self,
    *,
    statement_params: Optional[dict[str, str]] = None,
    **kwargs: Any,
) -> pandas.DataFrame:
    """
    Convert Snowpark pandas DataFrame to `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_

    Args:
        statement_params: Dictionary of statement level parameters to be set while executing this action.

    Returns:
        pandas DataFrame

    See also:
        - :func:`to_pandas <modin.pandas.io.to_pandas>`
        - :func:`Series.to_pandas <modin.pandas.Series.to_pandas>`

    Examples:

        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]})
        >>> df.to_pandas()
           Animal  Max Speed
        0  Falcon      380.0
        1  Falcon      370.0
        2  Parrot       24.0
        3  Parrot       26.0

        >>> df['Animal'].to_pandas()
        0    Falcon
        1    Falcon
        2    Parrot
        3    Parrot
        Name: Animal, dtype: object
    """
    return self._to_pandas(statement_params=statement_params, **kwargs)


@register_dataframe_accessor("cache_result")
@add_cache_result_docstring
@materialization_warning
def cache_result(self, inplace: bool = False) -> Optional[pd.DataFrame]:
    """
    Persists the current Snowpark pandas DataFrame to a temporary table that lasts the duration of the session.
    """
    new_qc = self._query_compiler.cache_result()
    if inplace:
        self._update_inplace(new_qc)
    else:
        return pd.DataFrame(query_compiler=new_qc)


@register_dataframe_accessor("create_or_replace_view")
def create_or_replace_view(
    self,
    name: Union[str, Iterable[str]],
    *,
    comment: Optional[str] = None,
    index: bool = False,
    index_label: Optional[IndexLabel] = None,
) -> List[Row]:
    """
    Creates a view that captures the computation expressed by this DataFrame.

    For ``name``, you can include the database and schema name (i.e. specify a
    fully-qualified name). If no database name or schema name are specified, the
    view will be created in the current database or schema.

    ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

    Args:
        name: The name of the view to create or replace. Can be a list of strings
            that specifies the database name, schema name, and view name.
        comment: Adds a comment for the created view. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
        index: default False
            If true, save DataFrame index columns in view columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
    """
    return self._query_compiler.create_or_replace_view(
        name=name,
        comment=comment,
        index=index,
        index_label=index_label,
    )


@register_dataframe_accessor("create_or_replace_dynamic_table")
def create_or_replace_dynamic_table(
    self,
    name: Union[str, Iterable[str]],
    *,
    warehouse: str,
    lag: str,
    comment: Optional[str] = None,
    mode: str = "overwrite",
    refresh_mode: Optional[str] = None,
    initialize: Optional[str] = None,
    clustering_keys: Optional[Iterable[ColumnOrName]] = None,
    is_transient: bool = False,
    data_retention_time: Optional[int] = None,
    max_data_extension_time: Optional[int] = None,
    iceberg_config: Optional[dict] = None,
    index: bool = False,
    index_label: Optional[IndexLabel] = None,
) -> List[Row]:
    """
    Creates a dynamic table that captures the computation expressed by this DataFrame.

    For ``name``, you can include the database and schema name (i.e. specify a
    fully-qualified name). If no database name or schema name are specified, the
    dynamic table will be created in the current database or schema.

    ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

    Args:
        name: The name of the dynamic table to create or replace. Can be a list of strings
            that specifies the database name, schema name, and view name.
        warehouse: The name of the warehouse used to refresh the dynamic table.
        lag: specifies the target data freshness
        comment: Adds a comment for the created table. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
        mode: Specifies the behavior of create dynamic table. Allowed values are:
            - "overwrite" (default): Overwrite the table by dropping the old table.
            - "errorifexists": Throw and exception if the table already exists.
            - "ignore": Ignore the operation if table already exists.
        refresh_mode: Specifies the refresh mode of the dynamic table. The value can be "AUTO",
            "FULL", or "INCREMENTAL".
        initialize: Specifies the behavior of initial refresh. The value can be "ON_CREATE" or
            "ON_SCHEDULE".
        clustering_keys: Specifies one or more columns or column expressions in the table as the clustering key.
            See `Clustering Keys & Clustered Tables <https://docs.snowflake.com/en/user-guide/tables-clustering-keys>`_
            for more details.
        is_transient: A boolean value that specifies whether the dynamic table is transient.
        data_retention_time: Specifies the retention period for the dynamic table in days so that
            Time Travel actions can be performed on historical data in the dynamic table.
        max_data_extension_time: Specifies the maximum number of days for which Snowflake can extend
            the data retention period of the dynamic table to prevent streams on the dynamic table
            from becoming stale.
        iceberg_config: A dictionary that can contain the following iceberg configuration values:

            - external_volume: specifies the identifier for the external volume where
                the Iceberg table stores its metadata files and data in Parquet format.
            - catalog: specifies either Snowflake or a catalog integration to use for this table.
            - base_location: the base directory that snowflake can write iceberg metadata and files to.
            - catalog_sync: optionally sets the catalog integration configured for Polaris Catalog.
            - storage_serialization_policy: specifies the storage serialization policy for the table.
        index: default False
            If true, save DataFrame index columns as table columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.


    Note:
        See `understanding dynamic table refresh <https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh>`_.
        for more details on refresh mode.
    """
    return self._query_compiler.create_or_replace_dynamic_table(
        name=name,
        warehouse=warehouse,
        lag=lag,
        comment=comment,
        mode=mode,
        refresh_mode=refresh_mode,
        initialize=initialize,
        clustering_keys=clustering_keys,
        is_transient=is_transient,
        data_retention_time=data_retention_time,
        max_data_extension_time=max_data_extension_time,
        iceberg_config=iceberg_config,
        index=index,
        index_label=index_label,
    )


@register_dataframe_accessor("to_view")
def to_view(
    self,
    name: Union[str, Iterable[str]],
    *,
    comment: Optional[str] = None,
    index: bool = False,
    index_label: Optional[IndexLabel] = None,
) -> List[Row]:
    """
    Creates a view that captures the computation expressed by this DataFrame.

    For ``name``, you can include the database and schema name (i.e. specify a
    fully-qualified name). If no database name or schema name are specified, the
    view will be created in the current database or schema.

    ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

    Args:
        name: The name of the view to create or replace. Can be a list of strings
            that specifies the database name, schema name, and view name.
        comment: Adds a comment for the created view. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
        index: default False
            If true, save DataFrame index columns in view columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
    """
    return self.create_or_replace_view(
        name=name,
        comment=comment,
        index=index,
        index_label=index_label,
    )


@register_dataframe_accessor("to_dynamic_table")
def to_dynamic_table(
    self,
    name: Union[str, Iterable[str]],
    *,
    warehouse: str,
    lag: str,
    comment: Optional[str] = None,
    mode: str = "overwrite",
    refresh_mode: Optional[str] = None,
    initialize: Optional[str] = None,
    clustering_keys: Optional[Iterable[ColumnOrName]] = None,
    is_transient: bool = False,
    data_retention_time: Optional[int] = None,
    max_data_extension_time: Optional[int] = None,
    iceberg_config: Optional[dict] = None,
    index: bool = False,
    index_label: Optional[IndexLabel] = None,
) -> List[Row]:
    """
    Creates a dynamic table that captures the computation expressed by this DataFrame.

    For ``name``, you can include the database and schema name (i.e. specify a
    fully-qualified name). If no database name or schema name are specified, the
    dynamic table will be created in the current database or schema.

    ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

    Args:
        name: The name of the dynamic table to create or replace. Can be a list of strings
            that specifies the database name, schema name, and view name.
        warehouse: The name of the warehouse used to refresh the dynamic table.
        lag: specifies the target data freshness
        comment: Adds a comment for the created table. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
        mode: Specifies the behavior of create dynamic table. Allowed values are:
            - "overwrite" (default): Overwrite the table by dropping the old table.
            - "errorifexists": Throw and exception if the table already exists.
            - "ignore": Ignore the operation if table already exists.
        refresh_mode: Specifies the refresh mode of the dynamic table. The value can be "AUTO",
            "FULL", or "INCREMENTAL".
        initialize: Specifies the behavior of initial refresh. The value can be "ON_CREATE" or
            "ON_SCHEDULE".
        clustering_keys: Specifies one or more columns or column expressions in the table as the clustering key.
            See `Clustering Keys & Clustered Tables <https://docs.snowflake.com/en/user-guide/tables-clustering-keys>`_
            for more details.
        is_transient: A boolean value that specifies whether the dynamic table is transient.
        data_retention_time: Specifies the retention period for the dynamic table in days so that
            Time Travel actions can be performed on historical data in the dynamic table.
        max_data_extension_time: Specifies the maximum number of days for which Snowflake can extend
            the data retention period of the dynamic table to prevent streams on the dynamic table
            from becoming stale.
        iceberg_config: A dictionary that can contain the following iceberg configuration values:

            - external_volume: specifies the identifier for the external volume where
                the Iceberg table stores its metadata files and data in Parquet format.
            - catalog: specifies either Snowflake or a catalog integration to use for this table.
            - base_location: the base directory that snowflake can write iceberg metadata and files to.
            - catalog_sync: optionally sets the catalog integration configured for Polaris Catalog.
            - storage_serialization_policy: specifies the storage serialization policy for the table.
        index: default False
            If true, save DataFrame index columns as table columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.


    Note:
        See `understanding dynamic table refresh <https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh>`_.
        for more details on refresh mode.
    """
    return self.create_or_replace_dynamic_table(
        name=name,
        warehouse=warehouse,
        lag=lag,
        comment=comment,
        mode=mode,
        refresh_mode=refresh_mode,
        initialize=initialize,
        clustering_keys=clustering_keys,
        is_transient=is_transient,
        data_retention_time=data_retention_time,
        max_data_extension_time=max_data_extension_time,
        iceberg_config=iceberg_config,
        index=index,
        index_label=index_label,
    )


@register_dataframe_accessor("to_iceberg")
def to_iceberg(
    self,
    table_name: Union[str, Iterable[str]],
    *,
    iceberg_config: dict,
    mode: Optional[str] = None,
    column_order: str = "index",
    clustering_keys: Optional[Iterable[ColumnOrName]] = None,
    block: bool = True,
    comment: Optional[str] = None,
    enable_schema_evolution: Optional[bool] = None,
    data_retention_time: Optional[int] = None,
    max_data_extension_time: Optional[int] = None,
    change_tracking: Optional[bool] = None,
    copy_grants: bool = False,
    index: bool = True,
    index_label: Optional[IndexLabel] = None,
) -> Optional[AsyncJob]:
    """
    Writes the DataFrame data to the specified iceberg table in a Snowflake database.

    Args:
        table_name: A string or list of strings representing table name.
            If input is a string, it represents the table name; if input is of type iterable of strings,
            it represents the fully-qualified object identifier (database name, schema name, and table name).
        iceberg_config: A dictionary that can contain the following iceberg configuration values:

            * external_volume: specifies the identifier for the external volume where
                the Iceberg table stores its metadata files and data in Parquet format

            * catalog: specifies either Snowflake or a catalog integration to use for this table

            * base_location: the base directory that snowflake can write iceberg metadata and files to

            * catalog_sync: optionally sets the catalog integration configured for Polaris Catalog

            * storage_serialization_policy: specifies the storage serialization policy for the table
        mode: One of the following values. When it's ``None`` or not provided,
            the save mode set by :meth:`mode` is used.

            "append": Append data of this DataFrame to the existing table. Creates a table if it does not exist.

            "overwrite": Overwrite the existing table by dropping old table.

            "truncate": Overwrite the existing table by truncating old table.

            "errorifexists": Throw an exception if the table already exists.

            "ignore": Ignore this operation if the table already exists.

        column_order: When ``mode`` is "append", data will be inserted into the target table by matching column sequence or column name. Default is "index". When ``mode`` is not "append", the ``column_order`` makes no difference.

            "index": Data will be inserted into the target table by column sequence.
            "name": Data will be inserted into the target table by matching column names. If the target table has more columns than the source DataFrame, use this one.

        clustering_keys: Specifies one or more columns or column expressions in the table as the clustering key.
            See `Clustering Keys & Clustered Tables <https://docs.snowflake.com/en/user-guide/tables-clustering-keys#defining-a-clustering-key-for-a-table>`_
            for more details.
        block: A bool value indicating whether this function will wait until the result is available.
            When it is ``False``, this function executes the underlying queries of the dataframe
            asynchronously and returns an :class:`AsyncJob`.
        comment: Adds a comment for the created table. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_. This argument is ignored if a
            table already exists and save mode is ``append`` or ``truncate``.
        enable_schema_evolution: Enables or disables automatic changes to the table schema from data loaded into the table from source files. Setting
            to ``True`` enables automatic schema evolution and setting to ``False`` disables it. If not set, the default behavior is used.
        data_retention_time: Specifies the retention period for the table in days so that Time Travel actions (SELECT, CLONE, UNDROP) can be performed
            on historical data in the table.
        max_data_extension_time: Specifies the maximum number of days for which Snowflake can extend the data retention period for the table to prevent
            streams on the table from becoming stale.
        change_tracking: Specifies whether to enable change tracking for the table. If not set, the default behavior is used.
        copy_grants: When true, retain the access privileges from the original table when a new table is created with "overwrite" mode.
        index: default True
            If true, save DataFrame index columns as table columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.


    Example::

        Saving DataFrame to an Iceberg table. Note that the external_volume, catalog, and base_location should have been setup externally.
        See `Create your first Iceberg table <https://docs.snowflake.com/en/user-guide/tutorials/create-your-first-iceberg-table>`_ for more information on creating iceberg resources.

        >>> df = session.create_dataframe([[1,2],[3,4]], schema=["a", "b"])
        >>> iceberg_config = {
        ...     "external_volume": "example_volume",
        ...     "catalog": "example_catalog",
        ...     "base_location": "/iceberg_root",
        ...     "storage_serialization_policy": "OPTIMIZED",
        ... }
        >>> df.to_snowpark_pandas().to_iceberg("my_table", iceberg_config=iceberg_config, mode="overwrite") # doctest: +SKIP
    """
    return self._query_compiler.to_iceberg(
        table_name=table_name,
        iceberg_config=iceberg_config,
        mode=mode,
        column_order=column_order,
        clustering_keys=clustering_keys,
        block=block,
        comment=comment,
        enable_schema_evolution=enable_schema_evolution,
        data_retention_time=data_retention_time,
        max_data_extension_time=max_data_extension_time,
        change_tracking=change_tracking,
        copy_grants=copy_grants,
        index=index,
        index_label=index_label,
    )
