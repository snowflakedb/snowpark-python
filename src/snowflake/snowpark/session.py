#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import json
import logging
import os
from array import array
from functools import reduce
from logging import getLogger
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import cloudpickle

import snowflake.snowpark  # type: ignore
from snowflake.connector import ProgrammingError, SnowflakeConnection
from snowflake.connector.options import pandas
from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark._internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    SnowflakePlanBuilder,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.table_function import (
    TableFunctionRelation as SPTableFunctionRelation,
)
from snowflake.snowpark._internal.analyzer_obj import Analyzer
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.plans.logical.basic_logical_operators import Range
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark._internal.sp_expressions import (
    AttributeReference as SPAttributeReference,
    FlattenFunction as SPFlattenFunction,
)
from snowflake.snowpark._internal.sp_types.types_package import (
    ColumnOrName,
    _infer_schema,
    _merge_type,
)
from snowflake.snowpark._internal.utils import (
    PythonObjJSONEncoder,
    TempObjectType,
    Utils,
    deprecate,
)
from snowflake.snowpark._internal.write_pandas import write_pandas
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.file_operation import FileOperation
from snowflake.snowpark.functions import (
    _create_table_function_expression,
    col,
    column,
    parse_json,
    to_array,
    to_date,
    to_decimal,
    to_object,
    to_time,
    to_timestamp,
    to_variant,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.table import Table
from snowflake.snowpark.types import (
    ArrayType,
    DateType,
    DecimalType,
    MapType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
    _AtomicType,
)
from snowflake.snowpark.udf import UDFRegistration

logger = getLogger(__name__)
_active_session = None


class Session:
    """
    Establishes a connection with a Snowflake database and provides methods for creating DataFrames
    and accessing objects for working with files in stages.

    When you create a Session object, you provide connection parameters to establish a
    connection with a Snowflake database (e.g. an account, a user name, etc.). You can
    specify these settings in a dict that associates connection parameters names with values.
    The Snowpark library uses `the Snowflake Connector for Python <https://docs.snowflake.com/en/user-guide/python-connector.html>`_
    to connect to Snowflake. Refer to
    `Connecting to Snowflake using the Python Connector <https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-to-snowflake>`_
    for the details of `Connection Parameters <https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect>`_.

    To create a Session object from a dict of connection parameters::

        connection_parameters = {
            "user": "<user_name>",
            "password": "<password>",
            "account": "<account_name>",
            "role": "<role_name>",
            "warehouse": "<warehouse_name>",
            "database": <database_name>,
            "schema": <schema1_name>,
        }
        session = Session.builder.configs(connection_parameters).create()

    :class:`Session` contains functions to construct a :class:`DataFrame` like :func:`table`,
    :func:`sql` and :func:`read`.
    """

    class SessionBuilder:
        """
        Provides methods to set connection parameters and create a :class:`Session`.
        """

        def __init__(self):
            self.__options = {}

        def _remove_config(self, key: str) -> "Session.SessionBuilder":
            """Only used in test."""
            self.__options.pop(key, None)
            return self

        def config(self, key: str, value: Union[int, str]) -> "Session.SessionBuilder":
            """
            Adds the specified connection parameter to the SessionBuilder configuration.
            """
            self.__options[key] = value
            return self

        def configs(
            self, options: Dict[str, Union[int, str]]
        ) -> "Session.SessionBuilder":
            """
            Adds the specified :class:`dict` of connection parameters to
            the SessionBuilder configuration.

            Note:
                Calling this method overwrites any existing connection parameters
                that you have already set in the SessionBuilder.
            """
            self.__options = {**self.__options, **options}
            return self

        def create(self) -> "Session":
            """Creates a new Session."""
            return self.__create_internal(conn=None)

        def __create_internal(
            self, conn: Optional[SnowflakeConnection] = None
        ) -> "Session":
            # set the log level of the conncector logger to ERROR to avoid massive logging
            logging.getLogger("snowflake.connector").setLevel(logging.ERROR)
            return Session._set_active_session(
                Session(
                    ServerConnection({}, conn)
                    if conn
                    else ServerConnection(self.__options)
                )
            )

        def __get__(self, obj, objtype=None):
            return Session.SessionBuilder()

    __STAGE_PREFIX = "@"

    #: Returns a builder you can use to set configuration properties
    #: and create a :class:`Session` object.
    builder: SessionBuilder = SessionBuilder()

    def __init__(self, conn: ServerConnection):
        self._conn = conn
        self.__query_tag = None
        self.__import_paths: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        self.__cloudpickle_path = {
            os.path.dirname(cloudpickle.__file__): ("cloudpickle", None)
        }
        self.__session_id = self._conn.get_session_id()
        self._session_info = f"""
"version" : {Utils.get_version()},
"python.version" : {Utils.get_python_version()},
"python.connector.version" : {Utils.get_connector_version()},
"python.connector.session.id" : {self.__session_id},
"os.name" : {Utils.get_os_name()}
"""
        self.__session_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
        self.__stage_created = False
        self.__udf_registration = None
        self.__plan_builder = SnowflakePlanBuilder(self)

        self.__last_action_id = 0
        self.__last_canceled_id = 0

        self.__file = None

        self._analyzer = Analyzer(self)

    def _generate_new_action_id(self) -> int:
        self.__last_action_id += 1
        return self.__last_action_id

    def close(self) -> None:
        """Close this session."""
        global _active_session
        if _active_session == self:
            _active_session = None
        try:
            if self._conn.is_closed():
                logger.warning("This session has been closed.")
            else:
                logger.info(f"Closing session: {self._session_info}")
                self.cancel_all()
        except Exception as ex:
            raise SnowparkClientExceptionMessages.SERVER_FAILED_CLOSE_SESSION(str(ex))
        finally:
            self._conn.close()

    def _get_last_canceled_id(self) -> int:
        return self.__last_canceled_id

    def cancel_all(self) -> None:
        """
        Cancel all action methods that are running currently.
        This does not affect any action methods called in the future.
        """
        logger.info("Canceling all running queries")
        self.__last_canceled_id = self.__last_action_id
        self._conn.run_query(f"select system$cancel_all_queries({self.__session_id})")

    @deprecate(
        deprecate_version="0.4.0",
        remove_version="0.5.0",
        extra_warning_text="Use get_imports.",
        extra_doc_string="Use :meth:`get_imports`.",
    )
    def getImports(self) -> List[str]:
        return self.get_imports()

    def get_imports(self) -> List[str]:
        """
        Returns a list of imports added for user defined functions (UDFs).
        This list includes any Python or zip files that were added automatically by the library.
        """
        return list(self.__import_paths.keys())

    def _get_local_imports(self) -> List[str]:
        return [
            dep for dep in self.get_imports() if not dep.startswith(self.__STAGE_PREFIX)
        ]

    @deprecate(
        deprecate_version="0.4.0",
        remove_version="0.5.0",
        extra_warning_text="Use add_import.",
        extra_doc_string="Use :meth:`add_import`.",
    )
    def addImport(self, path: str, import_path: Optional[str] = None) -> None:
        return self.add_import(path, import_path)

    def add_import(self, path: str, import_path: Optional[str] = None) -> None:
        """
        Registers a remote file in stage or a local file as an import of a user-defined function
        (UDF). The local file can be a compressed file (e.g., zip), a Python file (.py),
        a directory, or any other file resource.

        Args:
            path: The path of a local file or a remote file in the stage. In each case:

                * if the path points to a local file, this file will be uploaded to the
                  stage where the UDF is registered and Snowflake will import the file when
                  executing that UDF.

                * if the path points to a local directory, the directory will be compressed
                  as a zip file and will be uploaded to the stage where the UDF is registered
                  and Snowflake will import the file when executing that UDF.

                * if the path points to a file in a stage, the file will be included in the
                  imports when executing a UDF.

            import_path: The relative Python import path for a UDF.
                If it is not provided or it is None, the UDF will import the package
                directly without any leading package/module. This argument will become
                a no-op if the path  points to a stage file or a non-Python local file.

        Examples::

            # import a local file
            session.add_import(“/tmp/my_dir/my_module.py”)
            @udf
            def f():
                from my_module import g
                return g()

            # import a local file with "import_path"
            session.add_import(“/tmp/my_dir/my_module.py”, import_path="my_dir.my_module")
            @udf
            def f():
                from my_dir.my_module import g
                return g()

            # import a stage file
            session.add_import(“@stage/test.py”)

        Note:
            1. In favor of the lazy execution, the file will not be uploaded to the stage
            immediately, and it will be uploaded when a UDF is created.

            2. The Snowpark library calculates an MD5 checksum for every file/directory.
            Each file is uploaded to a subdirectory named after the MD5 checksum for the
            file in the stage. If there is an existing file or directory, the Snowpark
            library will compare their checksums to determine whether it should be re-uploaded.
            Therefore, after uploading a local file to the stage, if the user makes
            some changes to this file and intends to upload it again, just call this
            function with the file path again, the existing file in the stage will be
            overwritten by the re-uploaded file.

            3. Adding two files with the same file name is not allowed, because UDFs
            can't be created with two imports with the same name.

            4. This method will register the file for all UDFs created later in the current
            session. If you only want to import a file for a specific UDF, you can use
            ``imports`` argument in :func:`functions.udf` or
            :meth:`session.udf.register() <snowflake.snowpark.udf.UDFRegistration.register>`.
        """
        path, checksum, leading_path = self._resolve_import_path(path, import_path)
        self.__import_paths[path] = (checksum, leading_path)

    @deprecate(
        deprecate_version="0.4.0",
        remove_version="0.5.0",
        extra_warning_text="Use remove_import.",
        extra_doc_string="Use :meth:`remove_import`.",
    )
    def removeImport(self, path: str) -> None:
        return self.remove_import(path)

    def remove_import(self, path: str) -> None:
        """
        Removes a file in stage or local file from the imports of a user-defined function (UDF).

        Args:
            path: a path pointing to a local file or a remote file in the stage

        Examples::

            session.remove_import(“/tmp/dir1/test.py”)
            session.remove_import(“/tmp/dir1”)
            session.remove_import(“@stage/test.py”)
        """
        trimmed_path = path.strip()
        abs_path = (
            os.path.abspath(trimmed_path)
            if not trimmed_path.startswith(self.__STAGE_PREFIX)
            else trimmed_path
        )
        if abs_path not in self.__import_paths:
            raise KeyError(f"{abs_path} is not found in the existing imports")
        else:
            self.__import_paths.pop(abs_path)

    @deprecate(
        deprecate_version="0.4.0",
        remove_version="0.5.0",
        extra_warning_text="Use clear_imports.",
        extra_doc_string="Use :meth:`clear_imports`.",
    )
    def clearImports(self) -> None:
        return self.clear_imports()

    def clear_imports(self) -> None:
        """
        Clears all files in a stage or local files from the imports of a user-defined function (UDF).

        Example::

            session.clear_imports()
        """
        self.__import_paths.clear()

    def _resolve_import_path(
        self, path: str, import_path: Optional[str] = None
    ) -> Tuple[str, Optional[str], Optional[str]]:
        trimmed_path = path.strip()
        trimmed_import_path = import_path.strip() if import_path else None

        if not trimmed_path.startswith(self.__STAGE_PREFIX):
            if not os.path.exists(trimmed_path):
                raise FileNotFoundError(f"{trimmed_path} is not found")
            if not os.path.isfile(trimmed_path) and not os.path.isdir(trimmed_path):
                raise ValueError(
                    f"add_import() only accepts a local file or directory, "
                    f"or a file in a stage, but got {trimmed_path}"
                )
            abs_path = os.path.abspath(trimmed_path)

            # convert the Python import path to the file path
            # and extract the leading path, where
            # absolute path = [leading path]/[parsed file path of Python import path]
            if trimmed_import_path is not None:
                # the import path only works for the directory and the Python file
                if os.path.isdir(abs_path):
                    import_file_path = trimmed_import_path.replace(".", os.path.sep)
                elif os.path.isfile(abs_path) and abs_path.endswith(".py"):
                    import_file_path = (
                        f"{trimmed_import_path.replace('.', os.path.sep)}.py"
                    )
                else:
                    import_file_path = None
                if import_file_path:
                    if abs_path.endswith(import_file_path):
                        leading_path = abs_path[: -len(import_file_path)]
                    else:
                        raise ValueError(
                            f"import_path {trimmed_import_path} is invalid "
                            f"because it's not a part of path {abs_path}"
                        )
                else:
                    leading_path = None
            else:
                leading_path = None

            # Include the information about import path to the checksum
            # calculation, so if the import path changes, the checksum
            # will change and the file in the stage will be overwritten.
            return (
                abs_path,
                Utils.calculate_md5(abs_path, additional_info=leading_path),
                leading_path,
            )
        else:
            return trimmed_path, None, None

    def _resolve_imports(
        self,
        stage_location: str,
        udf_level_import_paths: Optional[
            Dict[str, Tuple[Optional[str], Optional[str]]]
        ] = None,
    ) -> List[str]:
        """Resolve the imports and upload local files (if any) to the stage."""
        resolved_stage_files = []
        stage_file_list = self._list_files_in_stage(stage_location)
        normalized_stage_location = Utils.normalize_stage_location(stage_location)

        # always import cloudpickle for non-stored-proc mode
        # TODO(SNOW-500845): Remove importing cloudpickle after it is installed on the server side by default
        import_paths = (
            udf_level_import_paths.copy()
            if udf_level_import_paths
            else self.__import_paths.copy()
        )
        if not self._conn._is_stored_proc:
            import_paths.update(self.__cloudpickle_path)

        for path, (prefix, leading_path) in import_paths.items():
            # stage file
            if path.startswith(self.__STAGE_PREFIX):
                resolved_stage_files.append(path)
            else:
                filename = (
                    f"{os.path.basename(path)}.zip"
                    if os.path.isdir(path) or path.endswith(".py")
                    else os.path.basename(path)
                )
                filename_with_prefix = f"{prefix}/{filename}"
                if filename_with_prefix in stage_file_list:
                    logger.info(
                        f"{filename} exists on {normalized_stage_location}, skipped"
                    )
                else:
                    # local directory or .py file
                    if os.path.isdir(path) or path.endswith(".py"):
                        with Utils.zip_file_or_directory_to_stream(
                            path, leading_path, add_init_py=True
                        ) as input_stream:
                            self._conn.upload_stream(
                                input_stream=input_stream,
                                stage_location=normalized_stage_location,
                                dest_filename=filename,
                                dest_prefix=prefix,
                                source_compression="DEFLATE",
                                compress_data=False,
                                overwrite=True,
                            )
                    # local file
                    else:
                        self._conn.upload_file(
                            path=path,
                            stage_location=normalized_stage_location,
                            dest_prefix=prefix,
                            compress_data=False,
                            overwrite=True,
                        )
                resolved_stage_files.append(
                    f"{normalized_stage_location}/{filename_with_prefix}"
                )

        return resolved_stage_files

    def _list_files_in_stage(self, stage_location: Optional[str] = None) -> Set[str]:
        normalized = Utils.normalize_stage_location(
            stage_location if stage_location else self.__session_stage
        )
        file_list = self.sql(f"ls {normalized}").select('"name"').collect()
        prefix_length = Utils.get_stage_file_prefix_length(normalized)
        return {str(row[0])[prefix_length:] for row in file_list}

    @property
    def query_tag(self) -> Optional[str]:
        """
        The query tag for this session.

        :getter: Returns the query tag. You can use the query tag to find all queries
            run for this session in the History page of the Snowflake web interface.

        :setter: Sets the query tag. If the input is ``None`` or an empty :class:`str`,
            the session's query_tag will be unset. If the query tag is not set, the default
            will be the call stack when a :class:`DataFrame` method that pushes down the SQL
            query to the Snowflake Database is called. For example, :meth:`DataFrame.collect`,
            :meth:`DataFrame.show`, :meth:`DataFrame.create_or_replace_view` and
            :meth:`DataFrame.create_or_replace_temp_view` will push down the SQL query.
        """
        return self.__query_tag

    @query_tag.setter
    def query_tag(self, tag: str) -> None:
        if tag:
            self._conn.run_query(f"alter session set query_tag = '{tag}'")
        else:
            self._conn.run_query("alter session unset query_tag")
        self.__query_tag = tag

    def table(self, name: Union[str, Iterable[str]]) -> Table:
        """
        Returns a DataFrame that points the specified table.

        Args:
            name: A string or list of strings that specify the table name or
                fully-qualified object identifier (database name, schema name, and table name).

        Examples::

            df1 = session.table("mytable")
            df2 = session.table(["mydb", "myschema", "mytable"])
        """

        if not isinstance(name, str) and isinstance(name, Iterable):
            name = ".".join(name)
        Utils.validate_object_name(name)
        return Table(name, self)

    def table_function(
        self,
        func_name: Union[str, List[str]],
        *func_arguments: ColumnOrName,
        **func_named_arguments: ColumnOrName,
    ) -> DataFrame:
        """Creates a new DataFrame from the given snowflake SQL table function.

        References: `Snowflake SQL functions <https://docs.snowflake.com/en/sql-reference/functions-table.html>`_.

        Example::

            word_list = session.table_function("split_to_table", lit("split words to table"), " ").collect()

        Args:

            func_name: The SQL function name.
            func_arguments: The positional arguments for the SQL function.
            func_named_arguments: The named arguments for the SQL function, if it accepts named arguments.

        Returns:
            A new :class:`DataFrame` with data from calling the table function.

        See Also:
            - :meth:`DataFrame.join_table_function`, which lateral joins an existing :class:`DataFrame` and a SQL function.
        """
        func_expr = _create_table_function_expression(
            func_name, *func_arguments, **func_named_arguments
        )
        return DataFrame(
            self,
            SPTableFunctionRelation(func_expr),
        )

    def sql(self, query: str) -> DataFrame:
        """
        Returns a new DataFrame representing the results of a SQL query.
        You can use this method to execute a SQL statement. Note that you still
        need to call :func:`DataFrame.collect` to execute this query in Snowflake.

        Args:
            query: The SQL statement to execute.

        Example::

            # create a dataframe from a SQL query
            df = session.sql("select 1")
            # execute the query
            df.collect()
        """
        return DataFrame(session=self, plan=self.__plan_builder.query(query, None))

    @property
    def read(self) -> "DataFrameReader":
        """Returns a :class:`DataFrameReader` that you can use to read data from various
        supported sources (e.g. a file in a stage) as a DataFrame."""
        return DataFrameReader(self)

    def _run_query(self, query: str) -> List[Any]:
        return self._conn.run_query(query)["data"]

    def _get_result_attributes(self, query: str) -> List[Attribute]:
        return self._conn.get_result_attributes(query)

    @deprecate(
        deprecate_version="0.4.0",
        remove_version="0.5.0",
        extra_warning_text="Use get_session_stage.",
        extra_doc_string="Use :meth:`get_session_stage`.",
    )
    def getSessionStage(self) -> str:
        return self.get_session_stage()

    def get_session_stage(self) -> str:
        """
        Returns the name of the temporary stage created by the Snowpark library
        for uploading and storing temporary artifacts for this session.
        These artifacts include libraries and packages for UDFs that you define
        in this session via :func:`add_import`.
        """
        qualified_stage_name = (
            f"{self.get_fully_qualified_current_schema()}.{self.__session_stage}"
        )
        if not self.__stage_created:
            self._run_query(
                f"create temporary stage if not exists {qualified_stage_name}"
            )
            self.__stage_created = True
        return f"@{qualified_stage_name}"

    # TODO make the table input consistent with session.table
    def write_pandas(
        self,
        pd: "pandas.DataFrame",
        table_name: str,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        chunk_size: Optional[int] = None,
        compression: str = "gzip",
        on_error: str = "abort_statement",
        parallel: int = 4,
        quote_identifiers: bool = True,
        auto_create_table: bool = False,
        create_temp_table: bool = False,
    ) -> DataFrame:
        """Writes a pandas DataFrame to a table in Snowflake and returns a
        Snowpark :class:DataFrame object referring to the table where the
        pandas DataFrame was written to.

        Note: Unless auto_create_table is true, you must first create a table in
        Snowflake that the passed in pandas DataFrame can be written to. If
        your pandas DataFrame cannot be written to the specified table, an
        exception will be raised.

        Args:
            pd: The pandas DataFrame we'd like to write back.
            table_name: Name of the table we want to insert into.
            database: Database that the table is in. If not provided, the default one will be used (Default value = None).
            schema: Schema that the table is in. If not provided, the default one will be used (Default value = None).
            chunk_size: Number of elements to be inserted once. If not provided, all elements will be dumped once
                (Default value = None).
            compression: The compression used on the Parquet files: gzip or snappy. Gzip gives supposedly a
                better compression, while snappy is faster. Use whichever is more appropriate (Default value = 'gzip').
            on_error: Action to take when COPY INTO statements fail, default follows documentation at:
                https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
                (Default value = 'abort_statement').
            parallel: Number of threads to be used when uploading chunks, default follows documentation at:
                https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters (Default value = 4).
            quote_identifiers: By default, identifiers, specifically database, schema, table and column names
                (from df.columns) will be quoted. If set to False, identifiers are passed on to Snowflake without quoting.
                I.e. identifiers will be coerced to uppercase by Snowflake.  (Default value = True)
            auto_create_table: When true, automatically creates a table to store the passed in pandas DataFrame using the
                passed in database, schema, and table_name. Note: there are usually multiple table configurations that
                would allow you to upload a particular pandas DataFrame successfully. If you don't like the auto created
                table, you can always create your own table before calling this function. For example, Auto-created
                tables will store :class:`list`, :class:`tuple`, :class:`dict` as strings in a VARCHAR column.

        Example::

            import pandas as pd

            pandas_df = pd.DataFrame([(1, "Steve"), (2, "Bob")], columns=["id", "name"])
            # "write_pandas_table" is a table that was pre-created with two columns,
            # id and name which are an integer and varchar respectively
            snowpark_df = session.write_pandas(pandas_df, "write_pandas_table")
            snowpark_pandas_df = snowpark_df.to_pandas()
            # These two pandas DataFrames have the same data
            snowpark_pandas_df.eq(snowpark_df)
        """
        success = None  # forward declaration
        try:
            if quote_identifiers:
                location = (
                    (('"' + database + '".') if database else "")
                    + (('"' + schema + '".') if schema else "")
                    + ('"' + table_name + '"')
                )
            else:
                location = (
                    (database + "." if database else "")
                    + (schema + "." if schema else "")
                    + (table_name)
                )
            success, nchunks, nrows, ci_output = write_pandas(
                self._conn._conn,
                pd,
                table_name,
                database=database,
                schema=schema,
                chunk_size=chunk_size,
                compression=compression,
                on_error=on_error,
                parallel=parallel,
                quote_identifiers=quote_identifiers,
                auto_create_table=auto_create_table,
                create_temp_table=create_temp_table,
            )
        except ProgrammingError as pe:
            if pe.msg.endswith("does not exist"):
                raise SnowparkClientExceptionMessages.DF_PANDAS_TABLE_DOES_NOT_EXIST_EXCEPTION(
                    location
                ) from pe
            else:
                raise pe

        if success:
            return self.table(location)
        else:
            raise SnowparkClientExceptionMessages.DF_PANDAS_GENERAL_EXCEPTION(
                str(ci_output)
            )

    def create_dataframe(
        self,
        data: Union[List, Tuple, "pandas.DataFrame"],
        schema: Optional[Union[StructType, List[str]]] = None,
    ) -> DataFrame:
        """Creates a new DataFrame containing the specified values from the local data.

        If creating a new DataFrame from a pandas Dataframe, we will store the pandas
        DataFrame in a temporary table and return a DataFrame pointing to that temporary
        table for you to then do further transformations on. This temporary table will be
        dropped at the end of your session. If you would like to save the pandas DataFrame,
        use the :meth:`write_pandas` method instead.

        Args:
            data: The local data for building a :class:`DataFrame`. ``data`` can only
                be a :class:`list`, :class:`tuple` or pandas DataFrame. Every element in
                ``data`` will constitute a row in the DataFrame.
            schema: A :class:`~snowflake.snowpark.types.StructType` containing names and
                data types of columns, or a list of column names, or ``None``.
                When ``schema`` is a list of column names or ``None``, the schema of the
                DataFrame will be inferred from the data across all rows. To improve
                performance, provide a schema. This avoids the need to infer data types
                with large data sets.

        Examples::

            import pandas as pd

            # infer schema
            session.create_dataframe([1, 2, 3, 4]).to_df("a")  # one single column
            session.create_dataframe([[1, 2, 3, 4]]).to_df("a", "b", "c", "d")
            session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
            session.create_dataframe([Row(a=1, b=2, c=3, d=4)])
            session.create_dataframe([{"a": "snow", "b": "flake"}])
            session.create_dataframe(pd.DataFrame([(1, 2, 3, 4)], columns=["a", "b", "c", "d"]))

            # given a schema
            from snowflake.snowpark.types import IntegerType, StringType
            schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
            session.create_dataframe([[1, "snow"], [3, "flake"]], schema)
        """
        if data is None:
            raise ValueError("data cannot be None.")

        # check the type of data
        if isinstance(data, Row):
            raise TypeError("create_dataframe() function does not accept a Row object.")

        if not isinstance(data, (list, tuple, pandas.DataFrame)):
            raise TypeError(
                "create_dataframe() function only accepts data as a list, tuple or a pandas DataFrame."
            )

        # check to see if it is a Pandas DataFrame and if so, write that to a temp
        # table and return as a DataFrame
        if isinstance(data, pandas.DataFrame):
            table_name = AnalyzerPackage._escape_quotes(
                Utils.random_name_for_temp_object(TempObjectType.TABLE)
            )
            database = self.get_current_database(unquoted=True)
            schema = self.get_current_schema(unquoted=True)

            return self.write_pandas(
                data,
                table_name,
                database=database,
                schema=schema,
                quote_identifiers=True,
                auto_create_table=True,
                create_temp_table=True,
            )

        if not data:
            raise ValueError("data cannot be empty.")

        # infer the schema based on the data
        names = None
        if isinstance(schema, StructType):
            new_schema = schema
        else:
            if isinstance(schema, list):
                names = schema
            new_schema = reduce(
                _merge_type,
                (_infer_schema(row, names) for row in data),
            )
        if len(new_schema.fields) == 0:
            raise ValueError(
                "The provided schema or inferred schema cannot be None or empty"
            )

        def convert_row_to_list(
            row: Union[Dict, List, Tuple], names: List[str]
        ) -> List:
            row_dict = None
            if not row:
                row = [None]
            elif isinstance(row, (tuple, list)):
                if getattr(row, "_fields", None):  # Row or namedtuple
                    row_dict = row.asDict() if isinstance(row, Row) else row._asdict()
            elif isinstance(row, dict):
                row_dict = row.copy()
            else:
                row = [row]

            if row_dict:
                # fill None if the key doesn't exist
                return [row_dict.get(name) for name in names]
            else:
                # check the length of every row, which should be same across data
                if len(row) != len(names):
                    raise ValueError(
                        f"{len(names)} fields are required by schema "
                        f"but {len(row)} values are provided. This might be because "
                        f"data consists of rows with different lengths, or mixed rows "
                        f"with column names or without column names"
                    )
                return list(row)

        # always overwrite the column names if they are provided via schema
        if names:
            for i, name in enumerate(names):
                new_schema.fields[i].name = name
        else:
            names = [f.name for f in new_schema.fields]
        rows = [convert_row_to_list(row, names) for row in data]

        # get spark attributes and data types
        attrs, data_types = [], []
        for field in new_schema.fields:
            sf_type = (
                StringType()
                if isinstance(
                    field.datatype,
                    (
                        VariantType,
                        ArrayType,
                        MapType,
                        TimeType,
                        DateType,
                        TimestampType,
                    ),
                )
                else field.datatype
            )
            attrs.append(
                SPAttributeReference(
                    AnalyzerPackage.quote_name(field.name), sf_type, field.nullable
                )
            )
            data_types.append(field.datatype)

        # convert all variant/time/geography/array/map data to string
        converted = []
        for row in rows:
            converted_row = []
            for value, data_type in zip(row, data_types):
                if value is None:
                    converted_row.append(None)
                elif isinstance(value, decimal.Decimal) and isinstance(
                    data_type, DecimalType
                ):
                    converted_row.append(value)
                elif isinstance(value, datetime.datetime) and isinstance(
                    data_type, TimestampType
                ):
                    converted_row.append(str(value))
                elif isinstance(value, datetime.time) and isinstance(
                    data_type, TimeType
                ):
                    converted_row.append(str(value))
                elif isinstance(value, datetime.date) and isinstance(
                    data_type, DateType
                ):
                    converted_row.append(str(value))
                elif isinstance(data_type, _AtomicType):  # consider inheritance
                    converted_row.append(value)
                elif isinstance(value, (list, tuple, array)) and isinstance(
                    data_type, ArrayType
                ):
                    converted_row.append(json.dumps(value, cls=PythonObjJSONEncoder))
                elif isinstance(value, dict) and isinstance(data_type, MapType):
                    converted_row.append(json.dumps(value, cls=PythonObjJSONEncoder))
                elif isinstance(data_type, VariantType):
                    converted_row.append(json.dumps(value, cls=PythonObjJSONEncoder))
                else:
                    raise TypeError(
                        f"Cannot cast {type(value)}({value}) to {str(data_type)}."
                    )
            converted.append(Row(*converted_row))

        # construct a project statement to convert string value back to variant
        project_columns = []
        for field in new_schema.fields:
            if isinstance(field.datatype, DecimalType):
                project_columns.append(
                    to_decimal(
                        column(field.name),
                        field.datatype.precision,
                        field.datatype.scale,
                    ).as_(field.name)
                )
            elif isinstance(field.datatype, TimestampType):
                project_columns.append(to_timestamp(column(field.name)).as_(field.name))
            elif isinstance(field.datatype, TimeType):
                project_columns.append(to_time(column(field.name)).as_(field.name))
            elif isinstance(field.datatype, DateType):
                project_columns.append(to_date(column(field.name)).as_(field.name))
            elif isinstance(field.datatype, VariantType):
                project_columns.append(
                    to_variant(parse_json(column(field.name))).as_(field.name)
                )
            elif isinstance(field.datatype, ArrayType):
                project_columns.append(
                    to_array(parse_json(column(field.name))).as_(field.name)
                )
            elif isinstance(field.datatype, MapType):
                project_columns.append(
                    to_object(parse_json(column(field.name))).as_(field.name)
                )
            # TODO: support geo type
            # elif isinstance(field.data_type, Geography):
            else:
                project_columns.append(column(field.name))

        return DataFrame(self, SnowflakeValues(attrs, converted)).select(
            project_columns
        )

    def range(self, start: int, end: Optional[int] = None, step: int = 1) -> DataFrame:
        """
        Creates a new DataFrame from a range of numbers. The resulting DataFrame has
        single column named ``ID``, containing elements in a range from ``start`` to
        ``end`` (exclusive) with the step value ``step``.

        Args:
            start: The start of the range. If ``end`` is not specified,
                ``start`` will be used as the value of ``end``.
            end: The end of the range.
            step: The step of the range.

        Examples::

            # create a dataframe with one column containing values from 0 to 9
            df1 = session.range(10)
            # create a dataframe with one column containing values from 1 to 9
            df2 = session.range(1, 10)
            # create a dataframe with one column containing values 1, 3, 5, 7, 9
            df3 = session.range(1, 10, 2)
        """
        range_plan = Range(0, start, step) if end is None else Range(start, end, step)
        return DataFrame(session=self, plan=range_plan)

    @deprecate(deprecate_version="0.4.0", remove_version="0.5.0")
    def getDefaultDatabase(self) -> Optional[str]:
        """
        Returns the name of the default database configured for this session in :attr:`builder`.
        """
        return self._conn.get_default_database()

    @deprecate(deprecate_version="0.4.0", remove_version="0.5.0")
    def getDefaultSchema(self) -> Optional[str]:
        """
        Returns the name of the default schema configured for this session in :attr:`builder`.
        """
        return self._conn.get_default_schema()

    @deprecate(
        deprecate_version="0.4.0",
        remove_version="0.5.0",
        extra_warning_text="Use get_current_database.",
        extra_doc_string="Use :meth:`get_current_database`.",
    )
    def getCurrentDatabase(self, unquoted: bool = False) -> Optional[str]:
        return self.get_current_schema(unquoted)

    @deprecate(
        deprecate_version="0.4.0",
        remove_version="0.5.0",
        extra_warning_text="Use get_current_schema.",
        extra_doc_string="Use :meth:`get_current_schema`.",
    )
    def getCurrentSchema(self, unquoted: bool = False) -> Optional[str]:
        return self.get_current_schema(unquoted)

    @deprecate(
        deprecate_version="0.4.0",
        remove_version="0.5.0",
        extra_warning_text="Use get_fully_qualified_current_schema.",
        extra_doc_string="Use :meth:`get_fully_qualified_current_schema`.",
    )
    def getFullyQualifiedCurrentSchema(self) -> str:
        return self.get_fully_qualified_current_schema()

    def get_current_database(self, unquoted: bool = False) -> Optional[str]:
        """
        Returns the name of the current database for the Python connector session attached
        to this session.

        Example::

            session.sql("use database newDB").collect()
            # return "newDB"
            session.get_current_database()
        """
        return self._conn.get_current_database(unquoted=unquoted)

    def get_current_schema(self, unquoted: bool = False) -> Optional[str]:
        """
        Returns the name of the current schema for the Python connector session attached
        to this session.

        Example::

            session.sql("use schema newSchema").collect()
            # return "newSchema"
            session.get_current_schema()
        """
        return self._conn.get_current_schema(unquoted=unquoted)

    def get_fully_qualified_current_schema(self) -> str:
        """Returns the fully qualified name of the current schema for the session."""
        database = self.get_current_database()
        schema = self.get_current_schema()
        if database is None or schema is None:
            missing_item = "DATABASE" if not database else "SCHEMA"
            # TODO: SNOW-372569 Use ErrorMessage
            raise SnowparkClientExceptionMessages.SERVER_CANNOT_FIND_CURRENT_DB_OR_SCHEMA(
                missing_item, missing_item, missing_item
            )
        return database + "." + schema

    @property
    def file(self) -> FileOperation:
        """Returns a :class:`FileOperation` object that you can use to perform file operations on stages.

        Examples:
            session.file.put("file:///tmp/file1.csv", "@myStage/prefix1")
            session.file.get("@myStage/prefix1", "file:///tmp")
        """
        if not self.__file:
            self.__file = FileOperation(self)
        return self.__file

    @property
    def udf(self) -> UDFRegistration:
        """
        Returns a :class:`udf.UDFRegistration` object that you can use to register UDFs.
        See details of how to use this object in :class:`udf.UDFRegistration`.
        """
        if not self.__udf_registration:
            self.__udf_registration = UDFRegistration(self)
        return self.__udf_registration

    def flatten(
        self,
        input: ColumnOrName,
        path: Optional[str] = None,
        outer: bool = False,
        recursive: bool = False,
        mode: str = "BOTH",
    ) -> DataFrame:
        """Creates a new :class:`DataFrame` by flattening compound values into multiple rows.

        The new :class:`DataFrame` will consist of the following columns:

            - SEQ
            - KEY
            - PATH
            - INDEX
            - VALUE
            - THIS

        References: `Snowflake SQL function FLATTEN <https://docs.snowflake.com/en/sql-reference/functions/flatten.html>`_.

        Example::

            df = session.flatten(parse_json(lit('{"a":[1,2]}')), "a", False, False, "BOTH")

        Args:
            input: The name of a column or a :class:`Column` instance that will be unseated into rows.
                The column data must be of Snowflake data type VARIANT, OBJECT, or ARRAY.
            path: The path to the element within a VARIANT data structure which needs to be flattened.
                The outermost element is to be flattened if path is empty or None.
            outer: If ``False``, any input rows that cannot be expanded, either because they cannot be accessed in the ``path``
                or because they have zero fields or entries, are completely omitted from the output.
                Otherwise, exactly one row is generated for zero-row expansions
                (with NULL in the KEY, INDEX, and VALUE columns).
            recursive: If ``False``, only the element referenced by ``path`` is expanded.
                Otherwise, the expansion is performed for all sub-elements recursively.
            mode: Specifies which types should be flattened "OBJECT", "ARRAY", or "BOTH".

        Returns:
            A new :class:`DataFrame` that has the flattened new columns and new rows from the compound data.

        See Also:
            - :meth:`DataFrame.flatten`, which creates a new :class:`DataFrame` by exploding a VARIANT column of an existing :class:`DataFrame`.
            - :meth:`Session.table_function`, which can be used for any Snowflake table functions, including ``flatten``.
        """

        mode = mode.upper()
        if mode not in ("OBJECT", "ARRAY", "BOTH"):
            raise ValueError("mode must be one of ('OBJECT', 'ARRAY', 'BOTH')")
        if isinstance(input, str):
            input = col(input)
        return DataFrame(
            self,
            SPTableFunctionRelation(
                SPFlattenFunction(input.expression, path, outer, recursive, mode)
            ),
        )

    def _table_exists(self, table_name: str):
        tables = self._run_query(f"show tables like '{table_name}'")
        return tables is not None and len(tables) > 0

    def _explain_query(self, query: str) -> Optional[str]:
        try:
            return self._run_query(f"explain using text {query}")[0][0]
        # return None for queries which can't be explained
        except ProgrammingError:
            logger.warning("query '%s' cannot be explained")
            return None

    @staticmethod
    def _get_active_session() -> Optional["Session"]:
        return _active_session

    @staticmethod
    def _set_active_session(session: "Session") -> "Session":
        logger.info(f"Python Snowpark Session information: {session._session_info}")
        global _active_session
        if _active_session:
            logger.info("Overwriting an already active session")
        _active_session = session
        return session

    createDataFrame = create_dataframe
