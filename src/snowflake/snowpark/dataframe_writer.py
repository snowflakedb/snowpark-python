#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Any, Dict, List, Literal, Optional, Union, overload

import snowflake.snowpark  # for forward references of type hints
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoLocationNode,
    SaveMode,
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import (
    add_api_call,
    dfw_collect_api_telemetry,
)
from snowflake.snowpark._internal.type_utils import ColumnOrSqlExpr
from snowflake.snowpark._internal.utils import (
    SUPPORTED_TABLE_TYPES,
    normalize_remote_file_or_dir,
    parse_table_name,
    str_to_enum,
    validate_object_name,
    warning,
)
from snowflake.snowpark.async_job import AsyncJob, _AsyncResultType
from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import sql_expr
from snowflake.snowpark.row import Row

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
try:
    from typing import Iterable
except ImportError:
    from collections.abc import Iterable

option_aliases = {
    "DELIMITER": ("FIELD_DELIMITER", lambda val: val),
    "SEP": ("FIELD_DELIMITER", lambda val: val),
    "LINESEP": ("RECORD_DELIMITER", lambda val: val),
    "PATHGLOBFILTER": ("PATTERN", lambda val: val),
    "QUOTE": ("FIELD_OPTIONALLY_ENCLOSED_BY", lambda val: val),
    "NULLVALUE": ("NULL_IF", lambda val: val),
    "DATEFORMAT": ("DATE_FORMAT", lambda val: val),
    "TIMESTAMPFORMAT": ("TIMESTAMP_FORMAT", lambda val: val),
}

copy_options = [
    "OVERWRITE",
    "SINGLE",
    "MAX_FILE_SIZE",
    "INCLUDE_QUERY_ID",
    "DETAILED_OUTPUT",
]


format_type_options = {
    "CSV": [
        "COMPRESSION",
        "RECORD_DELIMITER",
        "FIELD_DELIMITER",
        "FILE_EXTENSION",
        "DATE_FORMAT",
        "TIME_FORMAT",
        "TIMESTAMP_FORMAT",
        "BINARY_FORMAT",
        "ESCAPE",
        "ESCAPE_UNENCLOSED_FIELD",
        "FIELD_OPTIONALLY_ENCLOSED_BY",
        "NULL_IF",
        "EMPTY_FIELD_AS_NULL",
    ],
    "JSON": ["COMPRESSION", "FILE_EXTENSION"],
    "PARQUET": ["COMPRESSION", "SNAPPY_COMPRESSION"],
}

copy_options = [
    "OVERWRITE",
    "SINGLE",
    "MAX_FILE_SIZE",
    "INCLUDE_QUERY_ID",
    "DETAILED_OUTPUT",
]


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
        self._cur_options: dict[str, str] = {}
        self._copy_options: dict[str, str] = {}
        self._save_mode = SaveMode.ERROR_IF_EXISTS
        self._partition_by = None
        self._file_format_name = None
        self._file_format_type = None
        self._header = False
        self._path = None

    def save(
        self,
        path: str = None,
        format: str = None,
        mode: str = None,
        partitionBy: Optional[ColumnOrSqlExpr] = None,
        block=True,
        **kwargs,
    ):
        """
        Unloads data into the given path.

        This method is typically the last method in a daisy chain of DataFrame writer methods.
        It allows specifying the file type using the `format` parameter, providing a schema using the `schema` parameter,
        and passing any copy or format-specific options using the `kwargs` parameter.

        Args:
            path: The path to the file(s) to load.
            format: The file format to use for ingestion. If not specified, it is assumed that it was already passed with an `option`.
            mode: a string specifying the mode for the writer
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
            **kwargs: Additional options to be passed for format-specific or copy-specific configurations.

        Notes:
            - If the `format` parameter is specified, it will take precedence over the format set through the `format` method or previous `option` calls.

        Alternatively, you can use the `format` method and specify the format, and then use the `save` method to specify the path:
            df = spark.read.format("csv").option("header", "true").save("path/to/your/file.csv")

        The following example demonstrate how to use a DataFrameWriter.
            >>> # Create a temp stage to run the example code.
            >>> _ = session.sql("CREATE or REPLACE temp STAGE mystage").collect()

        Example 1:
            Loading the first two columns of a CSV file and skipping the first header line:
            >>> from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
            >>> _ = session.sql("create file format if not exists csv_format type=csv skip_header=1 null_if='none';").collect()
            >>> _ = session.file.put("tests/resources/testCSVspecialFormat.csv", "@mystage", auto_compress=False)
            >>> # Define the schema for the data in the CSV files.
            >>> schema = StructType([StructField("ID", IntegerType()),StructField("USERNAME", StringType()),StructField("FIRSTNAME", StringType()),StructField("LASTNAME", StringType())])
            >>> # Create a DataFrame that is configured to load data from the CSV files in the stage with a given schema and format.
            >>> df = session.read.load("@mystage/testCSVspecialFormat.csv",format="csv",schema=schema,format_name="csv_format")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(ID=0, USERNAME='admin', FIRSTNAME=None, LASTNAME=None), Row(ID=1, USERNAME='test_user', FIRSTNAME='test', LASTNAME='user')]
        """
        if not path and self._path:
            path = self._path
        if format:
            self.format(format)
        if partitionBy:
            self.partitionBy(partitionBy)
        for key, value in kwargs.items():
            self.option(key, value)
        if mode:
            self.mode(mode)
        if not self._file_format_type:
            raise SnowparkClientExceptionMessages.DF_MUST_PROVIDE_FILETYPE_FOR_WRITING_FILE()
        return self._write_to_location(
            path, self._file_format_type, self._save_mode.value, block
        )

    def partitionBy(self, partition_by: Optional[ColumnOrSqlExpr]) -> "DataFrameWriter":
        """Specifies an expression used to partition the unloaded table rows into separate files. It can be a :class:`Column`, a column name, or a SQL expression."""
        self._partition_by = partition_by
        return self

    def option(self, key: str, value: Any) -> "DataFrameWriter":
        """Sets the specified option in the DataFrameWriter.

        Use this method to configure any
        `format-specific options <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions>`_
        and
        `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_.
        (Note that although specifying copy options can make error handling more robust during the
        reading process, it may have an effect on performance.)

        Args:
            key: Name of the option (e.g. ``compression``, ``skip_header``, etc.).
            value: Value of the option.
        """
        if key.upper() == "PATH":
            self._path = value
            return self
        elif key.upper() == "MODE":
            return self.mode(value)
        elif key.upper() in ["FORMAT", "TYPE"]:
            return self.format(value)
        elif key.upper() in ["PARTITIONBY", "PARTITION_BY"]:
            return self.partitionBy(value)
        elif key.upper() == "HEADER":
            self._header = value
            return self
        elif key.upper() == "OVERWRITE":
            if isinstance(value, bool):
                if value:
                    self.mode("overwrite")
                else:
                    self.mode("errorifexists")
            elif isinstance(value, str):
                if value.lower() == "true":
                    self.mode("overwrite")
                else:
                    self.mode("errorifexists")
            return self
        elif key.upper() in copy_options:
            self._copy_options[key.lower()] = value
            return self
        elif key.upper() in option_aliases:
            supported_key, convert_value_function = option_aliases[key.upper()]
            key = supported_key.upper()
            value = convert_value_function(value)
        self._cur_options[key.upper()] = value
        return self

    def options(self, configs: Dict) -> "DataFrameWriter":
        """Sets multiple specified options in the DataFrameReader.

        This method is the same as the :meth:`option` except that you can set multiple options in one call.

        Args:
            configs: Dictionary of the names of options (e.g. ``compression``,
                ``skip_header``, etc.) and their corresponding values.
        """
        for k, v in configs.items():
            self.option(k, v)
        return self

    def format(self, format: str) -> "DataFrameWriter":
        """
        Sets the file type that will be use for unloading.

        This method allows specifying the file type that will used for data unloading. The accepted file formats are:
        - csv
        - json
        - parquet

        Alternatives to using the `format` method are:
        1. Using the `option` method with either "format" or "type" key-value pairs.
        Example: option("format", "csv") or option("type", "json")

        2. Passing the file type directly into the `load` method.
        Example: load("path/to/your/file", format="json")

        Note: Ensure that you use a valid file format and use the appropriate method or option to specify it.

        Args:
            format (str): The file format to use for ingestion.

        Returns:
            DataFrameReader: The DataFrameReader object with the specified format set.
        """
        self._file_format_type = format
        return self

    def _write_to_location(
        self, path: str, file_format_type: str, mode: str, block: bool, **kwargs
    ) -> Union[List[Row], AsyncJob]:
        if mode:
            self.mode(mode)
        for key, value in kwargs.items():
            self.option(key, value)
        return self.copy_into_location(
            path,
            header=self._header,
            partition_by=self._partition_by,
            file_format_name=self._file_format_name,
            file_format_type=file_format_type,
            format_type_options=self._cur_options,
            block=block,
            **self._copy_options,
        )

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
        block: bool = True,
        **copy_options: Optional[str],
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
        block: bool = True,
        **copy_options: Optional[str],
    ) -> List[Row]:
        ...  # pragma: no cover

    def csv(
        self,
        path: str,
        mode: Optional[str] = SaveMode.ERROR_IF_EXISTS.value,
        block=True,
        **kwargs,
    ) -> Union[List[Row], AsyncJob]:
        return self._write_to_location(path, "CSV", mode, block, **kwargs)

    def parquet(
        self,
        path: str,
        mode: Optional[str] = SaveMode.ERROR_IF_EXISTS.value,
        block=True,
        **kwargs,
    ) -> Union[List[Row], AsyncJob]:
        return self._write_to_location(path, "PARQUET", mode, block, **kwargs)

    def json(
        self,
        path: str,
        mode: Optional[str] = SaveMode.ERROR_IF_EXISTS.value,
        block=True,
        **kwargs,
    ) -> Union[List[Row], AsyncJob]:
        return self._write_to_location(path, "JSON", mode, block, **kwargs)

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

    @overload
    def save_as_table(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        mode: Optional[str] = None,
        column_order: str = "index",
        create_temp_table: bool = False,
        table_type: Literal["", "temp", "temporary", "transient"] = "",
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
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
    ) -> Optional[AsyncJob]:
        """Writes the data to the specified table in a Snowflake database.

        Args:
            table_name: A string or list of strings representing table name.
                If input is a string, it represents the table name; if input is of type iterable of strings,
                it represents the fully-qualified object identifier (database name, schema name, and table name).
            mode: One of the following values. When it's ``None`` or not provided,
                the save mode set by :meth:`mode` is used.

                "append": Append data of this DataFrame to existing data.

                "overwrite": Overwrite existing data.

                "errorifexists": Throw an exception if data already exists.

                "ignore": Ignore this operation if data already exists.

            column_order: When ``mode`` is "append", data will be inserted into the target table by matching column sequence or column name. Default is "index". When ``mode`` is not "append", the ``column_order`` makes no difference.

                "index": Data will be inserted into the target table by column sequence.
                "name": Data will be inserted into the target table by matching column names. If the target table has more columns than the source DataFrame, use this one.

            create_temp_table: (Deprecated) The to-be-created table will be temporary if this is set to ``True``.
            table_type: The table type of table to be created. The supported values are: ``temp``, ``temporary``,
                        and ``transient``. An empty string means to create a permanent table. Learn more about table
                        types `here <https://docs.snowflake.com/en/user-guide/tables-temp-transient.html>`_.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

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
        save_mode = (
            str_to_enum(mode.lower(), SaveMode, "'mode'") if mode else self._save_mode
        )
        full_table_name = (
            table_name if isinstance(table_name, str) else ".".join(table_name)
        )
        validate_object_name(full_table_name)
        table_name = (
            parse_table_name(table_name) if isinstance(table_name, str) else table_name
        )
        if column_order is None or column_order.lower() not in ("name", "index"):
            raise ValueError("'column_order' must be either 'name' or 'index'")
        column_names = (
            self._dataframe.columns if column_order.lower() == "name" else None
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

        create_table_logic_plan = SnowflakeCreateTable(
            table_name,
            column_names,
            save_mode,
            self._dataframe._plan,
            table_type,
        )
        session = self._dataframe._session
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
        block: bool = True,
        **copy_options: Optional[str],
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
        block: bool = False,
        **copy_options: Optional[str],
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
        **copy_options: Optional[str],
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
        if isinstance(partition_by, str):
            partition_by = sql_expr(partition_by)._expression
        elif isinstance(partition_by, Column):
            partition_by = partition_by._expression
        elif partition_by is not None:
            raise TypeError(  # pragma: no cover
                f"'partition_by' is expected to be a column name, a Column object, or a sql expression. Got type {type(partition_by)}"
            )
        df = self._dataframe._with_plan(
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
        )
        add_api_call(df, "DataFrameWriter.copy_into_location")
        return df._internal_collect_with_tag(
            statement_params=statement_params or self._dataframe._statement_params,
            block=block,
        )

    saveAsTable = save_as_table
