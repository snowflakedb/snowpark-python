#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, Iterable, List, Optional, Union

import snowflake.snowpark  # for forward references of type hints
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoLocationNode,
    SaveMode,
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.telemetry import dfw_action_telemetry
from snowflake.snowpark._internal.type_utils import ColumnOrSqlExpr
from snowflake.snowpark._internal.utils import (
    normalize_remote_file_or_dir,
    str_to_enum,
    validate_object_name,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import sql_expr
from snowflake.snowpark.row import Row


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

    def mode(self, save_mode: str) -> "DataFrameWriter":
        """Set the save mode of this :class:`DataFrameWriter`.

        Args:
            save_mode: One of the following strings.

                "append": Append data of this DataFrame to existing data.

                "overwrite": Overwrite existing data.

                "errorifexists": Throw an exception if data already exists.

                "ignore": Ignore this operation if data already exists.

                Default value is "errorifexists".

        Returns:
            The :class:`DataFrameWriter` itself.
        """
        self._save_mode = str_to_enum(save_mode.lower(), SaveMode, "`save_mode`")
        return self

    @dfw_action_telemetry
    def save_as_table(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        mode: Optional[str] = None,
        create_temp_table: bool = False,
    ) -> None:
        """Writes the data to the specified table in a Snowflake database.

        Args:
            table_name: A string or list of strings that specify the table name or fully-qualified object identifier
                (database name, schema name, and table name).
            mode: One of the following values. When it's ``None`` or not provided,
                the save mode set by :meth:`mode` is used.

                "append": Append data of this DataFrame to existing data.

                "overwrite": Overwrite existing data.

                "errorifexists": Throw an exception if data already exists.

                "ignore": Ignore this operation if data already exists.

            create_temp_table: The to-be-created table will be temporary if this is set to ``True``.

        Examples::

            >>> df = session.create_dataframe([[1,2],[3,4]], schema=["a", "b"])
            >>> df.write.mode("overwrite").save_as_table("my_table", create_temp_table=True)
            >>> session.table("my_table").collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
            >>> df.write.save_as_table("my_table", mode="append", create_temp_table=True)
            >>> session.table("my_table").collect()
            [Row(A=1, B=2), Row(A=3, B=4), Row(A=1, B=2), Row(A=3, B=4)]
        """
        save_mode = (
            str_to_enum(mode.lower(), SaveMode, "'mode'") if mode else self._save_mode
        )
        full_table_name = (
            table_name if isinstance(table_name, str) else ".".join(table_name)
        )
        validate_object_name(full_table_name)
        create_table_logic_plan = SnowflakeCreateTable(
            full_table_name,
            save_mode,
            self._dataframe._plan,
            create_temp_table,
        )
        session = self._dataframe._session
        snowflake_plan = session._analyzer.resolve(create_table_logic_plan)
        session._conn.execute(snowflake_plan)

    def copy_into_location(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrSqlExpr] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        **copy_options: Optional[str],
    ) -> List[Row]:
        """Executes a `COPY INTO <location> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html>`__ to unload data from a ``DataFrame`` into one or more files in a stage or external stage.

        Args:
            location: The destination stage location.
            partition_by: Specifies an expression used to partition the unloaded table rows into separate files. It can be a :class:`Column`, a column name, or a SQL expression.
            file_format_name: Specifies an existing named file format to use for unloading data from the table. The named file format determines the format type (CSV, JSON, PARQUET), as well as any other format options, for the data files.
            file_format_type: Specifies the type of files unloaded from the table. If a format type is specified, additional format-specific options can be specified in ``format_type_options``.
            format_type_options: Depending on the ``file_format_type`` specified, you can include more format specific options. Use the options documented in the `Format Type Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions>`__.
            header: Specifies whether to include the table column headings in the output files.
            copy_options: The kwargs that are used to specify the copy options. Use the options documented in the `Copy Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions>`__.

        Returns:
            A list of :class:`Row` objects containing unloading results.

        Example::

            >>> # save this dataframe to a parquet file on the session stage
            >>> df = session.create_dataframe([["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]], schema = ["FIRST_NAME", "LAST_NAME"])
            >>> remote_file_path = f"{session.get_session_stage()}/names.parquet"
            >>> df.write.copy_into_location(remote_file_path, file_format_type="parquet", header=True, overwrite=True, single=True)
            [Row(rows_unloaded=3, input_bytes=597, output_bytes=597)]
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
        if isinstance(partition_by, str):
            partition_by = sql_expr(partition_by)._expression
        elif isinstance(partition_by, Column):
            partition_by = partition_by._expression
        elif partition_by is not None:
            raise TypeError(
                f"'partition_by' is expected to be a column name, a Column object, or a sql expression. Got type {type(partition_by)}"
            )
        return self._dataframe._with_plan(
            CopyIntoLocationNode(
                self._dataframe._plan,
                stage_location,
                partition_by=partition_by,
                file_format_name=file_format_name,
                file_format_type=file_format_type,
                format_type_options=format_type_options,
                copy_options=copy_options,
                header=header,
            )
        )._internal_collect_with_tag()

    saveAsTable = save_as_table
