#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from typing import Any, Dict, List, Literal, Optional, Union, overload

import snowflake.snowpark  # for forward references of type hints
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoLocationNode,
    SaveMode,
    SnowflakeCreateTable,
    TableCreationSource,
)
from snowflake.snowpark._internal.open_telemetry import open_telemetry_context_manager
from snowflake.snowpark._internal.telemetry import (
    add_api_call,
    dfw_collect_api_telemetry,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName, ColumnOrSqlExpr
from snowflake.snowpark._internal.utils import (
    SUPPORTED_TABLE_TYPES,
    get_aliased_option_name,
    get_copy_into_location_options,
    normalize_remote_file_or_dir,
    parse_table_name,
    str_to_enum,
    validate_object_name,
    warning,
)
from snowflake.snowpark.async_job import AsyncJob, _AsyncResultType
from snowflake.snowpark.column import Column, _to_col_if_str
from snowflake.snowpark.functions import sql_expr
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.row import Row

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

WRITER_OPTIONS_ALIAS_MAP = {
    "SEP": "FIELD_DELIMITER",
    "LINESEP": "RECORD_DELIMITER",
    "QUOTE": "FIELD_OPTIONALLY_ENCLOSED_BY",
    "NULLVALUE": "NULL_IF",
    "DATEFORMAT": "DATE_FORMAT",
    "TIMESTAMPFORMAT": "TIMESTAMP_FORMAT",
}


class DataFrameWriter:
    """Provides methods for writing data from a :class:`DataFrame` to supported output destinations.

    To use this object:

    1. Create an instance of a :class:`DataFrameWriter` by accessing the :attr:`DataFrame.write` property.
    2. (Optional) Specify the save mode by calling :meth:`mode`, which returns the same
       :class:`DataFrameWriter` that is configured to save data using the specified mode.
       The default mode is "errorifexists".
    3. Call :meth:`save_as_table` or :meth:`copy_into_location` to save the data to the
       specified destination.
    """

    def __init__(self, dataframe: "snowflake.snowpark.dataframe.DataFrame") -> None:
        self._dataframe = dataframe
        self._save_mode = SaveMode.ERROR_IF_EXISTS
        self._partition_by: Optional[ColumnOrSqlExpr] = None
        self._cur_options: Dict[str, Any] = {}

    def mode(self, save_mode: str) -> "DataFrameWriter":
        """Set the save mode of this :class:`DataFrameWriter`.

        Args:
            save_mode: One of the following strings.

                "append": Append data of this DataFrame to the existing table. Creates a table if it does not exist.

                "overwrite": Overwrite the existing table by dropping old table.

                "truncate": Overwrite the existing table by truncating old table.

                "errorifexists": Throw an exception if the table already exists.

                "ignore": Ignore this operation if the table already exists.

                Default value is "errorifexists".

        Returns:
            The :class:`DataFrameWriter` itself.
        """
        self._save_mode = str_to_enum(save_mode.lower(), SaveMode, "`save_mode`")
        return self

    def partition_by(self, expr: ColumnOrSqlExpr) -> "DataFrameWriter":
        """Specifies an expression used to partition the unloaded table rows into separate files. It can be a
        :class:`Column`, a column name, or a SQL expression.
        """
        self._partition_by = expr
        return self

    def option(self, key: str, value: Any) -> "DataFrameWriter":
        """Depending on the ``file_format_type`` specified, you can include more format specific options.
        Use the options documented in the `Format Type Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions>`__.
        """
        aliased_key = get_aliased_option_name(key, WRITER_OPTIONS_ALIAS_MAP)
        self._cur_options[aliased_key] = value
        return self

    def options(self, configs: Dict) -> "DataFrameWriter":
        for k, v in configs.items():
            self.option(k, v)
        return self

    @overload
    def save_as_table(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        mode: Optional[str] = None,
        column_order: str = "index",
        create_temp_table: bool = False,
        table_type: Literal["", "temp", "temporary", "transient"] = "",
        clustering_keys: Optional[Iterable[ColumnOrName]] = None,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
    ) -> None:
        ...  # pragma: no cover

    @overload
    def save_as_table(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        mode: Optional[str] = None,
        column_order: str = "index",
        create_temp_table: bool = False,
        table_type: Literal["", "temp", "temporary", "transient"] = "",
        clustering_keys: Optional[Iterable[ColumnOrName]] = None,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
    ) -> AsyncJob:
        ...  # pragma: no cover

    @dfw_collect_api_telemetry
    def save_as_table(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        mode: Optional[str] = None,
        column_order: str = "index",
        create_temp_table: bool = False,
        table_type: Literal["", "temp", "temporary", "transient"] = "",
        clustering_keys: Optional[Iterable[ColumnOrName]] = None,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        comment: Optional[str] = None,
        enable_schema_evolution: Optional[bool] = None,
        data_retention_time: Optional[int] = None,
        max_data_extension_time: Optional[int] = None,
        change_tracking: Optional[bool] = None,
        copy_grants: bool = False,
        iceberg_config: Optional[dict] = None,
    ) -> Optional[AsyncJob]:
        """Writes the data to the specified table in a Snowflake database.

        Args:
            table_name: A string or list of strings representing table name.
                If input is a string, it represents the table name; if input is of type iterable of strings,
                it represents the fully-qualified object identifier (database name, schema name, and table name).
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

            create_temp_table: (Deprecated) The to-be-created table will be temporary if this is set to ``True``.
            table_type: The table type of table to be created. The supported values are: ``temp``, ``temporary``,
                        and ``transient``. An empty string means to create a permanent table. Not applicable
                        for iceberg tables. Learn more about table types
                        `here <https://docs.snowflake.com/en/user-guide/tables-temp-transient.html>`_.
            clustering_keys: Specifies one or more columns or column expressions in the table as the clustering key.
                See `Clustering Keys & Clustered Tables <https://docs.snowflake.com/en/user-guide/tables-clustering-keys#defining-a-clustering-key-for-a-table>`_
                for more details.
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
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
            iceberg_config: A dictionary that can contain the following iceberg configuration values:

                * external_volume: specifies the identifier for the external volume where
                    the Iceberg table stores its metadata files and data in Parquet format

                * catalog: specifies either Snowflake or a catalog integration to use for this table

                * base_location: the base directory that snowflake can write iceberg metadata and files to

                * catalog_sync: optionally sets the catalog integration configured for Polaris Catalog

                * storage_serialization_policy: specifies the storage serialization policy for the table

        Examples::

            >>> df = session.create_dataframe([[1,2],[3,4]], schema=["a", "b"])
            >>> df.write.mode("overwrite").save_as_table("my_table", table_type="temporary")
            >>> session.table("my_table").collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
            >>> df.write.save_as_table("my_table", mode="append", table_type="temporary")
            >>> session.table("my_table").collect()
            [Row(A=1, B=2), Row(A=3, B=4), Row(A=1, B=2), Row(A=3, B=4)]
            >>> df.write.mode("overwrite").save_as_table("my_transient_table", table_type="transient")
            >>> session.table("my_transient_table").collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
        """
        with open_telemetry_context_manager(self.save_as_table, self._dataframe):
            save_mode = (
                str_to_enum(mode.lower(), SaveMode, "'mode'")
                if mode
                else self._save_mode
            )
            full_table_name = (
                table_name if isinstance(table_name, str) else ".".join(table_name)
            )
            validate_object_name(full_table_name)
            table_name = (
                parse_table_name(table_name)
                if isinstance(table_name, str)
                else table_name
            )
            if column_order is None or column_order.lower() not in ("name", "index"):
                raise ValueError("'column_order' must be either 'name' or 'index'")
            column_names = (
                self._dataframe.columns if column_order.lower() == "name" else None
            )
            clustering_exprs = (
                [
                    _to_col_if_str(col, "DataFrameWriter.save_as_table")._expression
                    for col in clustering_keys
                ]
                if clustering_keys
                else []
            )

            if create_temp_table:
                warning(
                    "save_as_table.create_temp_table",
                    "create_temp_table is deprecated. We still respect this parameter when it is True but "
                    'please consider using `table_type="temporary"` instead.',
                )
                table_type = "temporary"

            if table_type and table_type.lower() not in SUPPORTED_TABLE_TYPES:
                raise ValueError(
                    f"Unsupported table type. Expected table types: {SUPPORTED_TABLE_TYPES}"
                )

            session = self._dataframe._session
            if not isinstance(session._conn, MockServerConnection) and save_mode in [
                SaveMode.APPEND,
                SaveMode.TRUNCATE,
            ]:
                # whether the table already exists in the database
                # determines the compiled SQL for APPEND and TRUNCATE mode
                # if the table does not exist, we need to create it first;
                # if the table exists, we can skip the creation step and insert data directly
                table_exists = session._table_exists(table_name)
            else:
                table_exists = None

            create_table_logic_plan = SnowflakeCreateTable(
                table_name,
                column_names,
                save_mode,
                self._dataframe._plan,
                TableCreationSource.OTHERS,
                table_type,
                clustering_exprs,
                comment,
                enable_schema_evolution,
                data_retention_time,
                max_data_extension_time,
                change_tracking,
                copy_grants,
                iceberg_config,
                table_exists,
            )
            snowflake_plan = session._analyzer.resolve(create_table_logic_plan)
            result = session._conn.execute(
                snowflake_plan,
                _statement_params=statement_params or self._dataframe._statement_params,
                block=block,
                data_type=_AsyncResultType.NO_RESULT,
            )
            return result if not block else None

    @overload
    def copy_into_location(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrSqlExpr] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        block: Literal[True] = True,
        **copy_options: Optional[Dict[str, Any]],
    ) -> List[Row]:
        ...  # pragma: no cover

    @overload
    def copy_into_location(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrSqlExpr] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        block: Literal[False] = False,
        **copy_options: Optional[Dict[str, Any]],
    ) -> AsyncJob:
        ...  # pragma: no cover

    def copy_into_location(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrSqlExpr] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        **copy_options: Optional[Dict[str, Any]],
    ) -> Union[List[Row], AsyncJob]:
        """Executes a `COPY INTO <location> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html>`__ to unload data from a ``DataFrame`` into one or more files in a stage or external stage.

        Args:
            location: The destination stage location.
            partition_by: Specifies an expression used to partition the unloaded table rows into separate files. It can be a :class:`Column`, a column name, or a SQL expression.
            file_format_name: Specifies an existing named file format to use for unloading data from the table. The named file format determines the format type (CSV, JSON, PARQUET), as well as any other format options, for the data files.
            file_format_type: Specifies the type of files unloaded from the table. If a format type is specified, additional format-specific options can be specified in ``format_type_options``.
            format_type_options: Depending on the ``file_format_type`` specified, you can include more format specific options. Use the options documented in the `Format Type Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions>`__.
            header: Specifies whether to include the table column headings in the output files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            copy_options: The kwargs that are used to specify the copy options. Use the options documented in the `Copy Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions>`__.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Returns:
            A list of :class:`Row` objects containing unloading results.

        Example::

            >>> # save this dataframe to a parquet file on the session stage
            >>> df = session.create_dataframe([["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]], schema = ["FIRST_NAME", "LAST_NAME"])
            >>> remote_file_path = f"{session.get_session_stage()}/names.parquet"
            >>> copy_result = df.write.copy_into_location(remote_file_path, file_format_type="parquet", header=True, overwrite=True, single=True)
            >>> copy_result[0].rows_unloaded
            3
            >>> # the following code snippet just verifies the file content and is actually irrelevant to Snowpark
            >>> # download this file and read it using pyarrow
            >>> import os
            >>> import tempfile
            >>> import pyarrow.parquet as pq
            >>> with tempfile.TemporaryDirectory() as tmpdirname:
            ...     _ = session.file.get(remote_file_path, tmpdirname)
            ...     pq.read_table(os.path.join(tmpdirname, "names.parquet"))
            pyarrow.Table
            FIRST_NAME: string not null
            LAST_NAME: string not null
            ----
            FIRST_NAME: [["John","Rick","Anthony"]]
            LAST_NAME: [["Berry","Berry","Davis"]]
        """
        stage_location = normalize_remote_file_or_dir(location)
        partition_by = partition_by if partition_by is not None else self._partition_by
        if isinstance(partition_by, str):
            partition_by = sql_expr(partition_by)._expression
        elif isinstance(partition_by, Column):
            partition_by = partition_by._expression
        elif partition_by is not None:
            raise TypeError(  # pragma: no cover
                f"'partition_by' is expected to be a column name, a Column object, or a sql expression. Got type {type(partition_by)}"
            )

        # read current options and update them with the new options
        cur_format_type_options, cur_copy_options = get_copy_into_location_options(
            self._cur_options
        )
        if copy_options:
            cur_copy_options.update(copy_options)

        if format_type_options:
            # apply writer option alias mapping
            format_type_aliased_options = {}
            for key, value in format_type_options.items():
                aliased_key = get_aliased_option_name(key, WRITER_OPTIONS_ALIAS_MAP)
                format_type_aliased_options[aliased_key] = value

            cur_format_type_options.update(format_type_aliased_options)

        df = self._dataframe._with_plan(
            CopyIntoLocationNode(
                self._dataframe._plan,
                stage_location,
                partition_by=partition_by,
                file_format_name=file_format_name,
                file_format_type=file_format_type,
                format_type_options=cur_format_type_options,
                copy_options=cur_copy_options,
                header=header,
            )
        )
        add_api_call(df, "DataFrameWriter.copy_into_location")
        return df._internal_collect_with_tag(
            statement_params=statement_params or self._dataframe._statement_params,
            block=block,
        )

    def csv(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrSqlExpr] = None,
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        **copy_options: Optional[str],
    ) -> Union[List[Row], AsyncJob]:
        """Executes internally a `COPY INTO <location> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html>`__ to unload data from a ``DataFrame`` into one or more CSV files in a stage or external stage.

        Args:
            location: The destination stage location.
            partition_by: Specifies an expression used to partition the unloaded table rows into separate files. It can be a :class:`Column`, a column name, or a SQL expression.
            format_type_options: Depending on the ``file_format_type`` specified, you can include more format specific options. Use the options documented in the `Format Type Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions>`__.
            header: Specifies whether to include the table column headings in the output files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            copy_options: The kwargs that are used to specify the copy options. Use the options documented in the `Copy Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions>`__.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
        Returns:
            A list of :class:`Row` objects containing unloading results.

        Example::

            >>> # save this dataframe to a csv file on the session stage
            >>> df = session.create_dataframe([["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]], schema = ["FIRST_NAME", "LAST_NAME"])
            >>> remote_file_path = f"{session.get_session_stage()}/names.csv"
            >>> copy_result = df.write.csv(remote_file_path, overwrite=True, single=True)
            >>> copy_result[0].rows_unloaded
            3
        """
        return self.copy_into_location(
            location,
            file_format_type="CSV",
            partition_by=partition_by,
            format_type_options=format_type_options,
            header=header,
            statement_params=statement_params,
            block=block,
            **copy_options,
        )

    def json(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrSqlExpr] = None,
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        **copy_options: Optional[str],
    ) -> Union[List[Row], AsyncJob]:
        """Executes internally a `COPY INTO <location> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html>`__ to unload data from a ``DataFrame`` into a JSON file in a stage or external stage.

        Args:
            location: The destination stage location.
            partition_by: Specifies an expression used to partition the unloaded table rows into separate files. It can be a :class:`Column`, a column name, or a SQL expression.
            format_type_options: Depending on the ``file_format_type`` specified, you can include more format specific options. Use the options documented in the `Format Type Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions>`__.
            header: Specifies whether to include the table column headings in the output files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            copy_options: The kwargs that are used to specify the copy options. Use the options documented in the `Copy Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions>`__.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Returns:
            A list of :class:`Row` objects containing unloading results.

        Example::

            >>> # save this dataframe to a json file on the session stage
            >>> df = session.sql("select parse_json('[{a: 1, b: 2}, {a: 3, b: 0}]')")
            >>> remote_file_path = f"{session.get_session_stage()}/names.json"
            >>> copy_result = df.write.json(remote_file_path, overwrite=True, single=True)
            >>> copy_result[0].rows_unloaded
            1
        """
        return self.copy_into_location(
            location,
            file_format_type="JSON",
            partition_by=partition_by,
            format_type_options=format_type_options,
            header=header,
            statement_params=statement_params,
            block=block,
            **copy_options,
        )

    def parquet(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrSqlExpr] = None,
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        **copy_options: Optional[str],
    ) -> Union[List[Row], AsyncJob]:
        """Executes internally a `COPY INTO <location> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html>`__ to unload data from a ``DataFrame`` into a PARQUET file in a stage or external stage.

        Args:
            location: The destination stage location.
            partition_by: Specifies an expression used to partition the unloaded table rows into separate files. It can be a :class:`Column`, a column name, or a SQL expression.
            format_type_options: Depending on the ``file_format_type`` specified, you can include more format specific options. Use the options documented in the `Format Type Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions>`__.
            header: Specifies whether to include the table column headings in the output files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            copy_options: The kwargs that are used to specify the copy options. Use the options documented in the `Copy Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions>`__.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Returns:
            A list of :class:`Row` objects containing unloading results.

        Example::

            >>> # save this dataframe to a parquet file on the session stage
            >>> df = session.create_dataframe([["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]], schema = ["FIRST_NAME", "LAST_NAME"])
            >>> remote_file_path = f"{session.get_session_stage()}/names.parquet"
            >>> copy_result = df.write.parquet(remote_file_path, overwrite=True, single=True)
            >>> copy_result[0].rows_unloaded
            3
        """
        return self.copy_into_location(
            location,
            file_format_type="PARQUET",
            partition_by=partition_by,
            format_type_options=format_type_options,
            header=header,
            statement_params=statement_params,
            block=block,
            **copy_options,
        )

    saveAsTable = save_as_table
