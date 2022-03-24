#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, Iterable, Optional, Union

import snowflake.snowpark  # for forward references of type hints
from snowflake.snowpark import Column
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    CopyIntoLocationNode,
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.telemetry import action_telemetry
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import Utils, _SaveMode
from snowflake.snowpark.functions import sql_expr


class DataFrameWriter:
    """Provides methods for writing data from a :class:`DataFrame` to supported output destinations.

    To use this object:

    1. Create an instance of a :class:`DataFrameWriter` by accessing the :attr:`DataFrame.write` property.
    2. Specify the save mode by calling :meth:`mode`, which returns the same
       :class:`DataFrameWriter` that is configured to save data using the specified mode.
       The default mode is "errorifexists".
    3. Call the :meth:`save_as_table` method to save the data to the specified destination.

    Example::

        df.write.mode("overwrite").save_as_table("table1")


    """

    def __init__(self, dataframe: "snowflake.snowpark.DataFrame"):
        self._dataframe = dataframe
        self.__save_mode = _SaveMode.APPEND  # spark default value is error.

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
        self.__save_mode = Utils.str_to_enum(
            save_mode.lower(), _SaveMode, "`save_mode`"
        )
        return self

    @action_telemetry
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
            mode: One of the following values. When it's ``None``, the save mode set by calling ``df.write.mode(save_mode)`` is used.

                "append": Append data of this DataFrame to existing data.

                "overwrite": Overwrite existing data.

                "errorifexists": Throw an exception if data already exists.

                "ignore": Ignore this operation if data already exists.

            create_temp_table: The to-be-created table will be temporary if this is set to ``True``. Default is ``False``.

        Example::

            df.write.mode("overwrite").save_as_table("table1")
            df.write.save_as_table("table2", mode="overwrite", create_temp_table=True)

        """
        # Snowpark scala doesn't have mode as a param but pyspark has it.
        # They both have mode()
        save_mode = (
            Utils.str_to_enum(mode.lower(), _SaveMode, "'mode'")
            if mode
            else self.__save_mode
        )
        full_table_name = (
            table_name if isinstance(table_name, str) else ".".join(table_name)
        )
        Utils.validate_object_name(full_table_name)
        create_table_logic_plan = SnowflakeCreateTable(
            full_table_name,
            save_mode,
            self._dataframe._plan,
            create_temp_table,
        )
        session = self._dataframe.session
        snowflake_plan = session._analyzer.resolve(create_table_logic_plan)
        session._conn.execute(snowflake_plan)

    def copy_into_location(
        self,
        location: str,
        *,
        partition_by: Optional[ColumnOrName] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        **copy_options: Optional[str],
    ) -> None:
        """Executes a `COPY INTO <location> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html>`__ to unload data from a ``DataFrame`` into one or more files in a stage or external stage.

        Example::

            df = session.create_dataframe([["John", "Berry"], ["Rick", "Berry"], ["Anthony", "Davis"]], schema = ["FIRST_NAME", "LAST_NAME"])
            df.write.copy_into_location("@my_stage_location", partition_by=col("LAST_NAME"), file_format_type="csv")

        Args:
            location: The destination stage location.
            partition_by: Specifies an expression used to partition the unloaded table rows into separate files. It can be a :class:`Column`, a column name, or a SQL expression.
            file_format_name: Specifies an existing named file format to use for unloading data from the table. The named file format determines the format type (CSV, JSON, PARQUET), as well as any other format options, for the data files.
            file_format_type: Specifies the type of files unloaded from the table. If a format type is specified, additional format-specific options can be specified in ``format_type_options``.
            format_type_options: Depending on the ``file_format_type`` specified, you can include more format specific options. Use the options documented in the `Format Type Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions>`__.
            header: Specifies whether to include the table column headings in the output files.
            copy_options: The kwargs that are used to specify the copy options. Use the options documented in the `Copy Options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions>`__.
        """
        stage_location = Utils.normalize_remote_file_or_dir(location)
        if isinstance(partition_by, str):
            partition_by = sql_expr(partition_by).expression
        elif isinstance(partition_by, Column):
            partition_by = partition_by.expression
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
