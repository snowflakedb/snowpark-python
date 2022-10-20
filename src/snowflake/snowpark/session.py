#!/usr/bin/env python3
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
from threading import RLock
from types import ModuleType
from typing import Any, Dict, Iterable, List, Literal, Optional, Set, Tuple, Union

import cloudpickle
import pkg_resources

from snowflake.connector import ProgrammingError, SnowflakeConnection
from snowflake.connector.options import installed_pandas, pandas
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    escape_quotes,
    quote_name,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import str_to_sql, to_sql
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.select_statement import (
    SelectSnowflakePlan,
    SelectSQL,
    SelectStatement,
    SelectTableFunction,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    Range,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.table_function import (
    FlattenFunction,
    TableFunctionRelation,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark._internal.telemetry import set_api_call_source
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    infer_schema,
    infer_type,
    merge_type,
)
from snowflake.snowpark._internal.utils import (
    MODULE_NAME_TO_PACKAGE_NAME_MAP,
    STAGE_PREFIX,
    SUPPORTED_TABLE_TYPES,
    PythonObjJSONEncoder,
    TempObjectType,
    calculate_checksum,
    deprecated,
    experimental,
    get_connector_version,
    get_os_name,
    get_python_version,
    get_stage_file_prefix_length,
    get_temp_type_for_object,
    get_version,
    is_in_stored_procedure,
    normalize_remote_file_or_dir,
    parse_positional_args_to_list,
    random_name_for_temp_object,
    unwrap_single_quote,
    unwrap_stage_location_single_quote,
    validate_object_name,
    warning,
    zip_file_or_directory_to_stream,
)
from snowflake.snowpark.async_job import AsyncJob
from snowflake.snowpark.column import Column
from snowflake.snowpark.context import _use_scoped_temp_objects
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.file_operation import FileOperation
from snowflake.snowpark.functions import (
    array_agg,
    col,
    column,
    parse_json,
    to_array,
    to_date,
    to_decimal,
    to_geography,
    to_object,
    to_time,
    to_timestamp,
    to_variant,
)
from snowflake.snowpark.query_history import QueryHistory
from snowflake.snowpark.row import Row
from snowflake.snowpark.stored_procedure import StoredProcedureRegistration
from snowflake.snowpark.table import Table
from snowflake.snowpark.table_function import (
    TableFunctionCall,
    _create_table_function_expression,
)
from snowflake.snowpark.types import (
    ArrayType,
    DateType,
    DecimalType,
    GeographyType,
    MapType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
    _AtomicType,
)
from snowflake.snowpark.udf import UDFRegistration
from snowflake.snowpark.udtf import UDTFRegistration

_logger = getLogger(__name__)

_session_management_lock = RLock()
_active_sessions: Set["Session"] = set()
_PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING = (
    "PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS"
)


def _get_active_session() -> Optional["Session"]:
    with _session_management_lock:
        if len(_active_sessions) == 1:
            return next(iter(_active_sessions))
        elif len(_active_sessions) > 1:
            raise SnowparkClientExceptionMessages.MORE_THAN_ONE_ACTIVE_SESSIONS()
        else:
            raise SnowparkClientExceptionMessages.SERVER_NO_DEFAULT_SESSION()


def _add_session(session: "Session") -> None:
    with _session_management_lock:
        _active_sessions.add(session)


def _remove_session(session: "Session") -> None:
    with _session_management_lock:
        _active_sessions.remove(session)


class Session:
    """
    Establishes a connection with a Snowflake database and provides methods for creating DataFrames
    and accessing objects for working with files in stages.

    When you create a :class:`Session` object, you provide connection parameters to establish a
    connection with a Snowflake database (e.g. an account, a user name, etc.). You can
    specify these settings in a dict that associates connection parameters names with values.
    The Snowpark library uses `the Snowflake Connector for Python <https://docs.snowflake.com/en/user-guide/python-connector.html>`_
    to connect to Snowflake. Refer to
    `Connecting to Snowflake using the Python Connector <https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-to-snowflake>`_
    for the details of `Connection Parameters <https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect>`_.

    To create a :class:`Session` object from a ``dict`` of connection parameters::

        >>> connection_parameters = {
        ...     "user": "<user_name>",
        ...     "password": "<password>",
        ...     "account": "<account_name>",
        ...     "role": "<role_name>",
        ...     "warehouse": "<warehouse_name>",
        ...     "database": "<database_name>",
        ...     "schema": "<schema1_name>",
        ... }
        >>> session = Session.builder.configs(connection_parameters).create() # doctest: +SKIP

    :class:`Session` contains functions to construct a :class:`DataFrame` like :meth:`table`,
    :meth:`sql` and :attr:`read`.

    A :class:`Session` object is not thread-safe.
    """

    class SessionBuilder:
        """
        Provides methods to set connection parameters and create a :class:`Session`.
        """

        def __init__(self) -> None:
            self._options = {}

        def _remove_config(self, key: str) -> "Session.SessionBuilder":
            """Only used in test."""
            self._options.pop(key, None)
            return self

        def config(self, key: str, value: Union[int, str]) -> "Session.SessionBuilder":
            """
            Adds the specified connection parameter to the :class:`SessionBuilder` configuration.
            """
            self._options[key] = value
            return self

        def configs(
            self, options: Dict[str, Union[int, str]]
        ) -> "Session.SessionBuilder":
            """
            Adds the specified :class:`dict` of connection parameters to
            the :class:`SessionBuilder` configuration.

            Note:
                Calling this method overwrites any existing connection parameters
                that you have already set in the SessionBuilder.
            """
            self._options = {**self._options, **options}
            return self

        def create(self) -> "Session":
            """Creates a new Session."""
            if "connection" in self._options:
                return self._create_internal(self._options["connection"])
            return self._create_internal(conn=None)

        def _create_internal(
            self, conn: Optional[SnowflakeConnection] = None
        ) -> "Session":
            new_session = Session(
                ServerConnection({}, conn) if conn else ServerConnection(self._options)
            )
            if "password" in self._options:
                self._options["password"] = None
            _add_session(new_session)
            return new_session

        def __get__(self, obj, objtype=None):
            return Session.SessionBuilder()

    #: Returns a builder you can use to set configuration properties
    #: and create a :class:`Session` object.
    builder: SessionBuilder = SessionBuilder()

    def __init__(self, conn: ServerConnection) -> None:
        if len(_active_sessions) >= 1 and is_in_stored_procedure():
            raise SnowparkClientExceptionMessages.DONT_CREATE_SESSION_IN_SP()
        self._conn = conn
        self._query_tag = None
        self._import_paths: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        self._packages: Dict[str, str] = {}
        self._session_id = self._conn.get_session_id()
        self._session_info = f"""
"version" : {get_version()},
"python.version" : {get_python_version()},
"python.connector.version" : {get_connector_version()},
"python.connector.session.id" : {self._session_id},
"os.name" : {get_os_name()}
"""
        self._session_stage = random_name_for_temp_object(TempObjectType.STAGE)
        self._stage_created = False
        self._udf_registration = UDFRegistration(self)
        self._udtf_registration = UDTFRegistration(self)
        self._sp_registration = StoredProcedureRegistration(self)
        self._plan_builder = SnowflakePlanBuilder(self)
        self._last_action_id = 0
        self._last_canceled_id = 0
        # consider making _session_parameters check a helpful function
        self._use_scoped_temp_objects = bool(
            _use_scoped_temp_objects
            and (
                conn._conn._session_parameters.get(
                    _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING, True
                )
                if conn._conn._session_parameters
                else True
            )
        )

        self._file = FileOperation(self)

        self._analyzer = Analyzer(self)
        self._sql_simplifier_enabled: bool = False
        _logger.info("Snowpark Session information: %s", self._session_info)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __str__(self):
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}: account={self.get_current_account()}, "
            f"role={self.get_current_role()}, database={self.get_current_database()}, "
            f"schema={self.get_current_schema()}, warehouse={self.get_current_warehouse()}>"
        )

    def _generate_new_action_id(self) -> int:
        self._last_action_id += 1
        return self._last_action_id

    def close(self) -> None:
        """Close this session."""
        if is_in_stored_procedure():
            raise SnowparkClientExceptionMessages.DONT_CLOSE_SESSION_IN_SP()
        try:
            if self._conn.is_closed():
                _logger.debug(
                    "No-op because session %s had been previously closed.",
                    self._session_id,
                )
            else:
                _logger.info("Closing session: %s", self._session_id)
                self.cancel_all()
        except Exception as ex:
            raise SnowparkClientExceptionMessages.SERVER_FAILED_CLOSE_SESSION(str(ex))
        finally:
            try:
                self._conn.close()
                _logger.info("Closed session: %s", self._session_id)
            finally:
                _remove_session(self)

    @property
    def sql_simplifier_enabled(self) -> bool:
        return self._sql_simplifier_enabled

    @sql_simplifier_enabled.setter
    def sql_simplifier_enabled(self, value: bool) -> None:
        """Set to ``True`` to use the SQL simplifier.
        The generated SQLs from ``DataFrame`` transformations would have fewer layers of nested queries if the SQL simplifier is enabled."""
        self._conn._telemetry_client.send_sql_simplifier_telemetry(
            self._session_id, value
        )
        self._sql_simplifier_enabled = value

    def cancel_all(self) -> None:
        """
        Cancel all action methods that are running currently.
        This does not affect any action methods called in the future.
        """
        _logger.info("Canceling all running queries")
        self._last_canceled_id = self._last_action_id
        self._conn.run_query(f"select system$cancel_all_queries({self._session_id})")

    def get_imports(self) -> List[str]:
        """
        Returns a list of imports added for user defined functions (UDFs).
        This list includes any Python or zip files that were added automatically by the library.
        """
        return list(self._import_paths.keys())

    def add_import(self, path: str, import_path: Optional[str] = None) -> None:
        """
        Registers a remote file in stage or a local file as an import of a user-defined function
        (UDF). The local file can be a compressed file (e.g., zip), a Python file (.py),
        a directory, or any other file resource. You can also find examples in
        :class:`~snowflake.snowpark.udf.UDFRegistration`.

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

        Example::

            >>> from snowflake.snowpark.types import IntegerType
            >>> from resources.test_udf_dir.test_udf_file import mod5
            >>> session.add_import("tests/resources/test_udf_dir/test_udf_file.py", import_path="resources.test_udf_dir.test_udf_file")
            >>> mod5_and_plus1_udf = session.udf.register(
            ...     lambda x: mod5(x) + 1,
            ...     return_type=IntegerType(),
            ...     input_types=[IntegerType()]
            ... )
            >>> session.range(1, 8, 2).select(mod5_and_plus1_udf("id")).to_df("col1").collect()
            [Row(COL1=2), Row(COL1=4), Row(COL1=1), Row(COL1=3)]
            >>> session.clear_imports()

        Note:
            1. In favor of the lazy execution, the file will not be uploaded to the stage
            immediately, and it will be uploaded when a UDF is created.

            2. The Snowpark library calculates a sha256 checksum for every file/directory.
            Each file is uploaded to a subdirectory named after the checksum for the
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
        self._import_paths[path] = (checksum, leading_path)

    def remove_import(self, path: str) -> None:
        """
        Removes a file in stage or local file from the imports of a user-defined function (UDF).

        Args:
            path: a path pointing to a local file or a remote file in the stage

        Examples::

            >>> session.clear_imports()
            >>> len(session.get_imports())
            0
            >>> session.add_import("tests/resources/test_udf_dir/test_udf_file.py")
            >>> len(session.get_imports())
            1
            >>> session.remove_import("tests/resources/test_udf_dir/test_udf_file.py")
            >>> len(session.get_imports())
            0
        """
        trimmed_path = path.strip()
        abs_path = (
            os.path.abspath(trimmed_path)
            if not trimmed_path.startswith(STAGE_PREFIX)
            else trimmed_path
        )
        if abs_path not in self._import_paths:
            raise KeyError(f"{abs_path} is not found in the existing imports")
        else:
            self._import_paths.pop(abs_path)

    def clear_imports(self) -> None:
        """
        Clears all files in a stage or local files from the imports of a user-defined function (UDF).
        """
        self._import_paths.clear()

    def _resolve_import_path(
        self, path: str, import_path: Optional[str] = None
    ) -> Tuple[str, Optional[str], Optional[str]]:
        trimmed_path = path.strip()
        trimmed_import_path = import_path.strip() if import_path else None

        if not trimmed_path.startswith(STAGE_PREFIX):
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
                calculate_checksum(abs_path, additional_info=leading_path),
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
        *,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """Resolve the imports and upload local files (if any) to the stage."""
        resolved_stage_files = []
        stage_file_list = self._list_files_in_stage(
            stage_location, statement_params=statement_params
        )
        normalized_stage_location = unwrap_stage_location_single_quote(stage_location)

        import_paths = udf_level_import_paths or self._import_paths
        for path, (prefix, leading_path) in import_paths.items():
            # stage file
            if path.startswith(STAGE_PREFIX):
                resolved_stage_files.append(path)
            else:
                filename = (
                    f"{os.path.basename(path)}.zip"
                    if os.path.isdir(path) or path.endswith(".py")
                    else os.path.basename(path)
                )
                filename_with_prefix = f"{prefix}/{filename}"
                if filename_with_prefix in stage_file_list:
                    _logger.debug(
                        f"{filename} exists on {normalized_stage_location}, skipped"
                    )
                else:
                    # local directory or .py file
                    if os.path.isdir(path) or path.endswith(".py"):
                        with zip_file_or_directory_to_stream(
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
                                is_in_udf=True,
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
                    normalize_remote_file_or_dir(
                        f"{normalized_stage_location}/{filename_with_prefix}"
                    )
                )

        return resolved_stage_files

    def _list_files_in_stage(
        self,
        stage_location: Optional[str] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> Set[str]:
        normalized = normalize_remote_file_or_dir(
            unwrap_single_quote(stage_location)
            if stage_location
            else self._session_stage
        )
        file_list = (
            self.sql(f"ls {normalized}")
            .select('"name"')
            ._internal_collect_with_tag(statement_params=statement_params)
        )
        prefix_length = get_stage_file_prefix_length(stage_location)
        return {str(row[0])[prefix_length:] for row in file_list}

    def get_packages(self) -> Dict[str, str]:
        """
        Returns a ``dict`` of packages added for user-defined functions (UDFs).
        The key of this ``dict`` is the package name and the value of this ``dict``
        is the corresponding requirement specifier.
        """
        return self._packages.copy()

    def add_packages(
        self, *packages: Union[str, ModuleType, Iterable[Union[str, ModuleType]]]
    ) -> None:
        """
        Adds third-party packages as dependencies of a user-defined function (UDF).
        Use this method to add packages for UDFs as installing packages using
        `conda <https://docs.conda.io/en/latest/>`_. You can also find examples in
        :class:`~snowflake.snowpark.udf.UDFRegistration`. See details of
        `third-party Python packages in Snowflake <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html>`_.

        Args:
            packages: A `requirement specifier <https://packaging.python.org/en/latest/glossary/#term-Requirement-Specifier>`_,
                a ``module`` object or a list of them for installing the packages. An exception
                will be raised if two conflicting requirement specifiers are provided.
                The syntax of a requirement specifier is defined in full in
                `PEP 508 <https://www.python.org/dev/peps/pep-0508/>`_, but currently only the
                `version matching clause <https://www.python.org/dev/peps/pep-0440/#version-matching>`_ (``==``)
                is supported as a `version specifier <https://packaging.python.org/en/latest/glossary/#term-Version-Specifier>`_
                for this argument. If a ``module`` object is provided, the package will be
                installed with the version in the local environment.

        Example::

            >>> import numpy as np
            >>> from snowflake.snowpark.functions import udf
            >>> import numpy
            >>> import pandas
            >>> import dateutil
            >>> # add numpy with the latest version on Snowflake Anaconda
            >>> # and pandas with the version "1.3.*"
            >>> # and dateutil with the local version in your environment
            >>> session.add_packages("numpy", "pandas==1.3.*", dateutil)
            >>> @udf
            ... def get_package_name_udf() -> list:
            ...     return [numpy.__name__, pandas.__name__, dateutil.__name__]
            >>> session.sql(f"select {get_package_name_udf.name}()").to_df("col1").show()
            ----------------
            |"COL1"        |
            ----------------
            |[             |
            |  "numpy",    |
            |  "pandas",   |
            |  "dateutil"  |
            |]             |
            ----------------
            <BLANKLINE>
            >>> session.clear_packages()

        Note:
            1. This method will add packages for all UDFs created later in the current
            session. If you only want to add packages for a specific UDF, you can use
            ``packages`` argument in :func:`functions.udf` or
            :meth:`session.udf.register() <snowflake.snowpark.udf.UDFRegistration.register>`.

            2. We recommend you to `setup the local environment with Anaconda <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#local-development-and-testing>`_,
            to ensure the consistent experience of a UDF between your local environment
            and the Snowflake server.
        """
        self._resolve_packages(parse_positional_args_to_list(*packages), self._packages)

    def remove_package(self, package: str) -> None:
        """
        Removes a third-party package from the dependency list of a user-defined function (UDF).

        Args:
            package: The package name.

        Examples::

            >>> session.clear_packages()
            >>> len(session.get_packages())
            0
            >>> session.add_packages("numpy", "pandas==1.3.5")
            >>> len(session.get_packages())
            2
            >>> session.remove_package("numpy")
            >>> len(session.get_packages())
            1
            >>> session.remove_package("pandas")
            >>> len(session.get_packages())
            0
        """
        package_name = pkg_resources.Requirement.parse(package).key
        if package_name in self._packages:
            self._packages.pop(package_name)
        else:
            raise ValueError(f"{package_name} is not in the package list")

    def clear_packages(self) -> None:
        """
        Clears all third-party packages of a user-defined function (UDF).
        """
        self._packages.clear()

    def add_requirements(self, file_path: str) -> None:
        """
        Adds a `requirement file <https://pip.pypa.io/en/stable/user_guide/#requirements-files>`_
        that contains a list of packages as dependencies of a user-defined function (UDF).

        Args:
            file_path: The path of a local requirement file.

        Example::

            >>> from snowflake.snowpark.functions import udf
            >>> import numpy
            >>> import pandas
            >>> # test_requirements.txt contains "numpy" and "pandas"
            >>> session.add_requirements("tests/resources/test_requirements.txt")
            >>> @udf
            ... def get_package_name_udf() -> list:
            ...     return [numpy.__name__, pandas.__name__]
            >>> session.sql(f"select {get_package_name_udf.name}()").to_df("col1").show()
            --------------
            |"COL1"      |
            --------------
            |[           |
            |  "numpy",  |
            |  "pandas"  |
            |]           |
            --------------
            <BLANKLINE>
            >>> session.clear_packages()

        Note:
            1. This method will add packages for all UDFs created later in the current
            session. If you only want to add packages for a specific UDF, you can use
            ``packages`` argument in :func:`functions.udf` or
            :meth:`session.udf.register() <snowflake.snowpark.udf.UDFRegistration.register>`.

            2. We recommend you to `setup the local environment with Anaconda <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#local-development-and-testing>`_,
            to ensure the consistent experience of a UDF between your local environment
            and the Snowflake server.
        """
        packages = []
        with open(file_path) as f:
            for line in f:
                package = line.rstrip()
                if package:
                    packages.append(package)
        self.add_packages(packages)

    def _resolve_packages(
        self,
        packages: List[str],
        existing_packages_dict: Optional[Dict[str, str]] = None,
        validate_package: bool = True,
        include_pandas: bool = False,
    ) -> List[str]:
        package_dict = dict()
        for package in packages:
            if isinstance(package, ModuleType):
                package_name = MODULE_NAME_TO_PACKAGE_NAME_MAP.get(
                    package.__name__, package.__name__
                )
                package = f"{package_name}=={pkg_resources.get_distribution(package_name).version}"
                use_local_version = True
            else:
                package = package.strip().lower()
                use_local_version = False
            package_req = pkg_resources.Requirement.parse(package)
            # get the standard package name if there is no underscore
            # underscores are discouraged in package names, but are still used in Anaconda channel
            # pkg_resources.Requirement.parse will convert all underscores to dashes
            package_name = (
                package if not use_local_version and "_" in package else package_req.key
            )
            package_dict[package] = (package_name, use_local_version, package_req)

        valid_packages = (
            {
                p[0]: json.loads(p[1])
                for p in self.table("information_schema.packages")
                .filter(
                    (col("language") == "python")
                    & (col("package_name").in_([v[0] for v in package_dict.values()]))
                )
                .group_by("package_name")
                .agg(array_agg("version"))
                ._internal_collect_with_tag()
            }
            if validate_package and package_dict
            else None
        )

        result_dict = (
            existing_packages_dict if existing_packages_dict is not None else {}
        )
        for package, package_info in package_dict.items():
            package_name, use_local_version, package_req = package_info
            package_version_req = package_req.specs[0][1] if package_req.specs else None

            if validate_package:
                unavailable_pkg_err_msg = (
                    "it is not available in Snowflake. Check information_schema.packages "
                    "to see available packages for UDFs. If this package is a "
                    '"pure-Python" package, you can find the directory of this package '
                    "and add it via session.add_import(). See details at "
                    "https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-udfs.html#using-third-party-packages-from-anaconda-in-a-udf."
                )

                if package_name not in valid_packages:
                    is_anaconda_terms_acknowledged = self._run_query(
                        "select system$are_anaconda_terms_acknowledged()"
                    )[0][0]
                    if is_anaconda_terms_acknowledged:
                        detailed_err_msg = unavailable_pkg_err_msg
                    else:
                        detailed_err_msg = (
                            "Anaconda terms must be accepted by ORGADMIN to use "
                            "Anaconda 3rd party packages. Please follow the instructions at "
                            "https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#using-third-party-packages-from-anaconda."
                        )

                    raise ValueError(
                        f"Cannot add package {package_name} because {detailed_err_msg}"
                    )
                elif package_version_req and not any(
                    v in package_req for v in valid_packages[package_name]
                ):
                    raise ValueError(
                        f"Cannot add package {package_name}=={package_version_req} because {unavailable_pkg_err_msg}"
                    )
                elif not use_local_version:
                    try:
                        package_client_version = pkg_resources.get_distribution(
                            package_name
                        ).version
                        if package_client_version not in valid_packages[package_name]:
                            _logger.warning(
                                "The version of package %s in the local environment is %s, "
                                "which does not fit the criteria for the requirement %s. "
                                "Your UDF might not work when the package version is different "
                                "between the server and your local environment",
                                package_name,
                                package_client_version,
                                package,
                            )
                    except pkg_resources.DistributionNotFound:
                        _logger.warning(
                            "package %s is not installed in the local environment"
                            "Your UDF might not work when the package is installed "
                            "on the server but not on your local environment.",
                            package_name,
                        )
                    except Exception as ex:
                        logging.warning(
                            "Failed to get the local distribution of package %s: %s",
                            package_name,
                            ex,
                        )

            if package_name in result_dict:
                if result_dict[package_name] != package:
                    raise ValueError(
                        f"Cannot add {package} because {result_dict[package_name]} "
                        "is already added"
                    )
            else:
                result_dict[package_name] = package

        def get_req_identifiers_list(
            modules: List[Union[str, ModuleType]]
        ) -> List[str]:
            res = []
            for m in modules:
                if isinstance(m, str) and m not in result_dict:
                    res.append(m)
                elif isinstance(m, ModuleType) and m.__name__ not in result_dict:
                    res.append(f"{m.__name__}=={m.__version__}")

            return res

        # always include cloudpickle
        extra_modules = [cloudpickle]
        if include_pandas:
            extra_modules.append("pandas")
        return list(result_dict.values()) + get_req_identifiers_list(extra_modules)

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
        return self._query_tag

    @query_tag.setter
    def query_tag(self, tag: str) -> None:
        if tag:
            self._conn.run_query(f"alter session set query_tag = {str_to_sql(tag)}")
        else:
            self._conn.run_query("alter session unset query_tag")
        self._query_tag = tag

    def table(self, name: Union[str, Iterable[str]]) -> Table:
        """
        Returns a Table that points the specified table.

        Args:
            name: A string or list of strings that specify the table name or
                fully-qualified object identifier (database name, schema name, and table name).

            Note:
                If your table name contains special characters, use double quotes to mark it like this, ``session.table('"my table"')``.
                For fully qualified names, you need to use double quotes separately like this, ``session.table('"my db"."my schema"."my.table"')``.
                Refer to `Identifier Requirements <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        Examples::

            >>> df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df1.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> session.table("my_table").collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
            >>> current_db = session.get_current_database()
            >>> current_schema = session.get_current_schema()
            >>> session.table([current_db, current_schema, "my_table"]).collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
        """

        if not isinstance(name, str) and isinstance(name, Iterable):
            name = ".".join(name)
        validate_object_name(name)
        t = Table(name, self)
        # Replace API call origin for table
        set_api_call_source(t, "Session.table")
        return t

    def table_function(
        self,
        func_name: Union[str, List[str], TableFunctionCall],
        *func_arguments: ColumnOrName,
        **func_named_arguments: ColumnOrName,
    ) -> DataFrame:
        """Creates a new DataFrame from the given snowflake SQL table function.

        References: `Snowflake SQL functions <https://docs.snowflake.com/en/sql-reference/functions-table.html>`_.

        Example 1
            Query a table function by function name:

            >>> from snowflake.snowpark.functions import lit
            >>> session.table_function("split_to_table", lit("split words to table"), lit(" ")).collect()
            [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]

        Example 2
            Define a table function variable and query it:

            >>> from snowflake.snowpark.functions import table_function, lit
            >>> split_to_table = table_function("split_to_table")
            >>> session.table_function(split_to_table(lit("split words to table"), lit(" "))).collect()
            [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]

        Example 3
            If you want to call a UDTF right after it's registered, the returned ``UserDefinedTableFunction`` is callable:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> class GeneratorUDTF:
            ...     def process(self, n):
            ...         for i in range(n):
            ...             yield (i, )
            >>> generator_udtf = udtf(GeneratorUDTF, output_schema=StructType([StructField("number", IntegerType())]), input_types=[IntegerType()])
            >>> session.table_function(generator_udtf(lit(3))).collect()
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]

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

        if self.sql_simplifier_enabled:
            d = DataFrame(
                self,
                SelectStatement(
                    from_=SelectTableFunction(func_expr, analyzer=self._analyzer),
                    analyzer=self._analyzer,
                ),
            )
        else:
            d = DataFrame(
                self,
                TableFunctionRelation(func_expr),
            )
        set_api_call_source(d, "Session.table_function")
        return d

    def sql(self, query: str) -> DataFrame:
        """
        Returns a new DataFrame representing the results of a SQL query.
        You can use this method to execute a SQL statement. Note that you still
        need to call :func:`DataFrame.collect` to execute this query in Snowflake.

        Args:
            query: The SQL statement to execute.

        Example::

            >>> # create a dataframe from a SQL query
            >>> df = session.sql("select 1/2")
            >>> # execute the query
            >>> df.collect()
            [Row(1/2=Decimal('0.500000'))]
        """

        if self.sql_simplifier_enabled:
            d = DataFrame(
                self,
                SelectStatement(
                    from_=SelectSQL(query, analyzer=self._analyzer),
                    analyzer=self._analyzer,
                ),
            )
        else:
            d = DataFrame(
                self,
                self._plan_builder.query(query, None),
            )
        set_api_call_source(d, "Session.sql")
        return d

    @property
    def read(self) -> "DataFrameReader":
        """Returns a :class:`DataFrameReader` that you can use to read data from various
        supported sources (e.g. a file in a stage) as a DataFrame."""
        return DataFrameReader(self)

    def _run_query(self, query: str, is_ddl_on_temp_object: bool = False) -> List[Any]:
        return self._conn.run_query(query, is_ddl_on_temp_object=is_ddl_on_temp_object)[
            "data"
        ]

    def _get_result_attributes(self, query: str) -> List[Attribute]:
        return self._conn.get_result_attributes(query)

    def get_session_stage(self) -> str:
        """
        Returns the name of the temporary stage created by the Snowpark library
        for uploading and storing temporary artifacts for this session.
        These artifacts include libraries and packages for UDFs that you define
        in this session via :func:`add_import`.
        """
        qualified_stage_name = (
            f"{self.get_fully_qualified_current_schema()}.{self._session_stage}"
        )
        if not self._stage_created:
            self._run_query(
                f"create {get_temp_type_for_object(self._use_scoped_temp_objects, True)} \
                stage if not exists {qualified_stage_name}",
                is_ddl_on_temp_object=True,
            )
            self._stage_created = True
        return f"{STAGE_PREFIX}{qualified_stage_name}"

    def write_pandas(
        self,
        df: "pandas.DataFrame",
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
        overwrite: bool = False,
        table_type: Literal["", "temp", "temporary", "transient"] = "",
    ) -> Table:
        """Writes a pandas DataFrame to a table in Snowflake and returns a
        Snowpark :class:`DataFrame` object referring to the table where the
        pandas DataFrame was written to.

        Args:
            df: The pandas DataFrame we'd like to write back.
            table_name: Name of the table we want to insert into.
            database: Database that the table is in. If not provided, the default one will be used.
            schema: Schema that the table is in. If not provided, the default one will be used.
            chunk_size: Number of elements to be inserted once. If not provided, all elements will be dumped once.
            compression: The compression used on the Parquet files: gzip or snappy. Gzip gives supposedly a
                better compression, while snappy is faster. Use whichever is more appropriate.
            on_error: Action to take when COPY INTO statements fail. See details at
                `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_.
            parallel: Number of threads to be used when uploading chunks. See details at
                `parallel parameter <https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters>`_.
            quote_identifiers: By default, identifiers, specifically database, schema, table and column names
                (from :attr:`DataFrame.columns`) will be quoted. If set to ``False``, identifiers
                are passed on to Snowflake without quoting, i.e. identifiers will be coerced to uppercase by Snowflake.
            auto_create_table: When true, automatically creates a table to store the passed in pandas DataFrame using the
                passed in ``database``, ``schema``, and ``table_name``. Note: there are usually multiple table configurations that
                would allow you to upload a particular pandas DataFrame successfully. If you don't like the auto created
                table, you can always create your own table before calling this function. For example, auto-created
                tables will store :class:`list`, :class:`tuple` and :class:`dict` as strings in a VARCHAR column.
            create_temp_table: (Deprecated) The to-be-created table will be temporary if this is set to ``True``. Note
                that to avoid breaking changes, currently when this is set to True, it overrides ``table_type``.
            overwrite: Default value is ``False`` and the Pandas DataFrame data is appended to the existing table. If set to ``True`` and if auto_create_table is also set to ``True``,
                then it drops the table. If set to ``True`` and if auto_create_table is set to ``False``,
                then it truncates the table. Note that in both cases (when overwrite is set to ``True``) it will replace the existing
                contents of the table with that of the passed in Pandas DataFrame.
            table_type: The table type of table to be created. The supported values are: ``temp``, ``temporary``,
                        and ``transient``. An empty string means to create a permanent table. Learn more about table
                        types `here <https://docs.snowflake.com/en/user-guide/tables-temp-transient.html>`_.

        Example::

            >>> import pandas as pd
            >>> pandas_df = pd.DataFrame([(1, "Steve"), (2, "Bob")], columns=["id", "name"])
            >>> snowpark_df = session.write_pandas(pandas_df, "write_pandas_table", auto_create_table=True, table_type="temp")
            >>> snowpark_df.to_pandas()
               id   name
            0   1  Steve
            1   2    Bob

            >>> pandas_df2 = pd.DataFrame([(3, "John")], columns=["id", "name"])
            >>> snowpark_df2 = session.write_pandas(pandas_df2, "write_pandas_table", auto_create_table=False)
            >>> snowpark_df2.to_pandas()
               id   name
            0   1  Steve
            1   2    Bob
            2   3   John

            >>> pandas_df3 = pd.DataFrame([(1, "Jane")], columns=["id", "name"])
            >>> snowpark_df3 = session.write_pandas(pandas_df3, "write_pandas_table", auto_create_table=False, overwrite=True)
            >>> snowpark_df3.to_pandas()
               id  name
            0   1  Jane

            >>> pandas_df4 = pd.DataFrame([(1, "Jane")], columns=["id", "name"])
            >>> snowpark_df4 = session.write_pandas(pandas_df4, "write_pandas_transient_table", auto_create_table=True, table_type="transient")
            >>> snowpark_df4.to_pandas()
               id  name
            0   1  Jane

        Note:
            Unless ``auto_create_table`` is ``True``, you must first create a table in
            Snowflake that the passed in pandas DataFrame can be written to. If
            your pandas DataFrame cannot be written to the specified table, an
            exception will be raised.
        """
        if create_temp_table:
            warning(
                "write_pandas.create_temp_table",
                "create_temp_table is deprecated. We still respect this parameter when it is True but "
                'please consider using `table_type="temporary"` instead.',
            )
            table_type = "temporary"

        if table_type and table_type.lower() not in SUPPORTED_TABLE_TYPES:
            raise ValueError(
                f"Unsupported table type. Expected table types: {SUPPORTED_TABLE_TYPES}"
            )

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
                df,
                table_name,
                database=database,
                schema=schema,
                chunk_size=chunk_size,
                compression=compression,
                on_error=on_error,
                parallel=parallel,
                quote_identifiers=quote_identifiers,
                auto_create_table=auto_create_table,
                overwrite=overwrite,
                table_type=table_type,
            )
        except ProgrammingError as pe:
            if pe.msg.endswith("does not exist"):
                raise SnowparkClientExceptionMessages.DF_PANDAS_TABLE_DOES_NOT_EXIST_EXCEPTION(
                    location
                ) from pe
            else:
                raise pe

        if success:
            t = self.table(location)
            set_api_call_source(t, "Session.write_pandas")
            return t
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

            >>> # create a dataframe with a schema
            >>> from snowflake.snowpark.types import IntegerType, StringType, StructField
            >>> schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
            >>> session.create_dataframe([[1, "snow"], [3, "flake"]], schema).collect()
            [Row(A=1, B='snow'), Row(A=3, B='flake')]

            >>> # create a dataframe by inferring a schema from the data
            >>> from snowflake.snowpark import Row
            >>> # infer schema
            >>> session.create_dataframe([1, 2, 3, 4], schema=["a"]).collect()
            [Row(A=1), Row(A=2), Row(A=3), Row(A=4)]
            >>> session.create_dataframe([[1, 2, 3, 4]], schema=["a", "b", "c", "d"]).collect()
            [Row(A=1, B=2, C=3, D=4)]
            >>> session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
            >>> session.create_dataframe([Row(a=1, b=2, c=3, d=4)]).collect()
            [Row(A=1, B=2, C=3, D=4)]
            >>> session.create_dataframe([{"a": 1}, {"b": 2}]).collect()
            [Row(A=1, B=None), Row(A=None, B=2)]

            >>> # create a dataframe from a pandas Dataframe
            >>> import pandas as pd
            >>> session.create_dataframe(pd.DataFrame([(1, 2, 3, 4)], columns=["a", "b", "c", "d"])).collect()
            [Row(a=1, b=2, c=3, d=4)]
        """
        if data is None:
            raise ValueError("data cannot be None.")

        # check the type of data
        if isinstance(data, Row):
            raise TypeError("create_dataframe() function does not accept a Row object.")

        if not isinstance(data, (list, tuple)) and (
            not installed_pandas
            or (installed_pandas and not isinstance(data, pandas.DataFrame))
        ):
            raise TypeError(
                "create_dataframe() function only accepts data as a list, tuple or a pandas DataFrame."
            )

        # check to see if it is a Pandas DataFrame and if so, write that to a temp
        # table and return as a DataFrame
        if installed_pandas and isinstance(data, pandas.DataFrame):
            table_name = escape_quotes(
                random_name_for_temp_object(TempObjectType.TABLE)
            )
            sf_database = self._conn._get_current_parameter("database", quoted=False)
            sf_schema = self._conn._get_current_parameter("schema", quoted=False)

            t = self.write_pandas(
                data,
                table_name,
                database=sf_database,
                schema=sf_schema,
                quote_identifiers=True,
                auto_create_table=True,
                create_temp_table=True,
            )
            set_api_call_source(t, "Session.create_dataframe[pandas]")
            return t

        # infer the schema based on the data
        names = None
        if isinstance(schema, StructType):
            new_schema = schema
        else:
            if not data:
                raise ValueError("Cannot infer schema from empty data")
            if isinstance(schema, list):
                names = schema
            new_schema = reduce(
                merge_type,
                (infer_schema(row, names) for row in data),
            )
        if len(new_schema.fields) == 0:
            raise ValueError(
                "The provided schema or inferred schema cannot be None or empty"
            )

        def convert_row_to_list(
            row: Union[Iterable[Any], Any], names: List[str]
        ) -> List:
            row_dict = None
            if row is None:
                row = [None]
            elif isinstance(row, (tuple, list)):
                if not row:
                    row = [None]
                elif getattr(row, "_fields", None):  # Row or namedtuple
                    row_dict = row.as_dict() if isinstance(row, Row) else row._asdict()
            elif isinstance(row, dict):
                row_dict = row.copy()
            else:
                row = [row]

            if row_dict:
                # fill None if the key doesn't exist
                row_dict = {quote_name(k): v for k, v in row_dict.items()}
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
        if not names:
            names = [f.name for f in new_schema.fields]
        quoted_names = [quote_name(name) for name in names]
        rows = [convert_row_to_list(row, quoted_names) for row in data]

        # get attributes and data types
        attrs, data_types = [], []
        for field, quoted_name in zip(new_schema.fields, quoted_names):
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
                        GeographyType,
                    ),
                )
                else field.datatype
            )
            attrs.append(Attribute(quoted_name, sf_type, field.nullable))
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
                elif isinstance(data_type, GeographyType):
                    converted_row.append(value)
                else:
                    raise TypeError(
                        f"Cannot cast {type(value)}({value}) to {str(data_type)}."
                    )
            converted.append(Row(*converted_row))

        # construct a project statement to convert string value back to variant
        project_columns = []
        for field, name in zip(new_schema.fields, names):
            if isinstance(field.datatype, DecimalType):
                project_columns.append(
                    to_decimal(
                        column(name),
                        field.datatype.precision,
                        field.datatype.scale,
                    ).as_(name)
                )
            elif isinstance(field.datatype, TimestampType):
                project_columns.append(to_timestamp(column(name)).as_(name))
            elif isinstance(field.datatype, TimeType):
                project_columns.append(to_time(column(name)).as_(name))
            elif isinstance(field.datatype, DateType):
                project_columns.append(to_date(column(name)).as_(name))
            elif isinstance(field.datatype, VariantType):
                project_columns.append(to_variant(parse_json(column(name))).as_(name))
            elif isinstance(field.datatype, GeographyType):
                project_columns.append(to_geography(column(name)).as_(name))
            elif isinstance(field.datatype, ArrayType):
                project_columns.append(to_array(parse_json(column(name))).as_(name))
            elif isinstance(field.datatype, MapType):
                project_columns.append(to_object(parse_json(column(name))).as_(name))
            else:
                project_columns.append(column(name))

        if self.sql_simplifier_enabled:
            df = DataFrame(
                self,
                SelectStatement(
                    from_=SelectSnowflakePlan(
                        SnowflakeValues(attrs, converted), analyzer=self._analyzer
                    ),
                    analyzer=self._analyzer,
                ),
            ).select(project_columns)
        else:
            df = DataFrame(self, SnowflakeValues(attrs, converted)).select(
                project_columns
            )
        set_api_call_source(df, "Session.create_dataframe[values]")
        return df

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

            >>> session.range(10).collect()
            [Row(ID=0), Row(ID=1), Row(ID=2), Row(ID=3), Row(ID=4), Row(ID=5), Row(ID=6), Row(ID=7), Row(ID=8), Row(ID=9)]
            >>> session.range(1, 10).collect()
            [Row(ID=1), Row(ID=2), Row(ID=3), Row(ID=4), Row(ID=5), Row(ID=6), Row(ID=7), Row(ID=8), Row(ID=9)]
            >>> session.range(1, 10, 2).collect()
            [Row(ID=1), Row(ID=3), Row(ID=5), Row(ID=7), Row(ID=9)]
        """
        range_plan = Range(0, start, step) if end is None else Range(start, end, step)

        if self.sql_simplifier_enabled:
            df = DataFrame(
                self,
                SelectStatement(
                    from_=SelectSnowflakePlan(range_plan, analyzer=self._analyzer),
                    analyzer=self._analyzer,
                ),
            )
        else:
            df = DataFrame(self, range_plan)
        set_api_call_source(df, "Session.range")
        return df

    @experimental(version="0.12.0")
    def create_async_job(self, query_id: str) -> AsyncJob:
        """
        Creates an :class:`AsyncJob` from a query ID.

        See also:
            :class:`AsyncJob`
        """
        if is_in_stored_procedure():
            raise NotImplementedError(
                "Async query is not supported in stored procedure yet"
            )
        return AsyncJob(query_id, None, self)

    def get_current_account(self) -> Optional[str]:
        """
        Returns the name of the current account for the Python connector session attached
        to this session.
        """
        return self._conn._get_current_parameter("account")

    def get_current_database(self) -> Optional[str]:
        """
        Returns the name of the current database for the Python connector session attached
        to this session. See the example in :meth:`table`.
        """
        return self._conn._get_current_parameter("database")

    def get_current_schema(self) -> Optional[str]:
        """
        Returns the name of the current schema for the Python connector session attached
        to this session. See the example in :meth:`table`.
        """
        return self._conn._get_current_parameter("schema")

    def get_fully_qualified_current_schema(self) -> str:
        """Returns the fully qualified name of the current schema for the session."""
        database = self.get_current_database()
        schema = self.get_current_schema()
        if database is None or schema is None:
            missing_item = "DATABASE" if not database else "SCHEMA"
            raise SnowparkClientExceptionMessages.SERVER_CANNOT_FIND_CURRENT_DB_OR_SCHEMA(
                missing_item, missing_item, missing_item
            )
        return database + "." + schema

    def get_current_warehouse(self) -> Optional[str]:
        """
        Returns the name of the warehouse in use for the current session.
        """
        return self._conn._get_current_parameter("warehouse")

    def get_current_role(self) -> Optional[str]:
        """
        Returns the name of the primary role in use for the current session.
        """
        return self._conn._get_current_parameter("role")

    def use_database(self, database: str) -> None:
        """Specifies the active/current database for the session.

        Args:
            database: The database name.
        """
        self._use_object(database, "database")

    def use_schema(self, schema: str) -> None:
        """Specifies the active/current schema for the session.

        Args:
            schema: The schema name.
        """
        self._use_object(schema, "schema")

    def use_warehouse(self, warehouse: str) -> None:
        """Specifies the active/current warehouse for the session.

        Args:
            warehouse: the warehouse name.
        """
        self._use_object(warehouse, "warehouse")

    def use_role(self, role: str) -> None:
        """Specifies the active/current primary role for the session.

        Args:
            role: the role name.
        """
        self._use_object(role, "role")

    def use_secondary_roles(self, roles: Optional[Literal["all", "none"]]) -> None:
        """
        Specifies the active/current secondary roles for the session.
        The currently-active secondary roles set the context that determines whether
        the current user has the necessary privileges to perform SQL actions.

        Args:
            roles: "all" or "none". ``None`` means "none".

        References: `Snowflake command USE SECONDARY ROLES <https://docs.snowflake.com/en/sql-reference/sql/use-secondary-roles.html>`_.
        """
        self._run_query(
            f"use secondary roles {'none' if roles is None else roles.lower()}"
        )

    def _use_object(self, object_name: str, object_type: str) -> None:
        if object_name:
            validate_object_name(object_name)
            self._run_query(f"use {object_type} {object_name}")
        else:
            raise ValueError(f"'{object_type}' must not be empty or None.")

    @property
    def telemetry_enabled(self) -> bool:
        """
        Returns whether telemetry is enabled. The default value is ``True`` and can
        be set to ``False`` to disable telemetry.

        Example::

            >>> session.telemetry_enabled
            True
            >>> session.telemetry_enabled = False
            >>> session.telemetry_enabled
            False
            >>> session.telemetry_enabled = True
            >>> session.telemetry_enabled
            True
        """
        return self._conn._conn.telemetry_enabled

    @telemetry_enabled.setter
    def telemetry_enabled(self, value):
        # Set both in-band and out-of-band telemetry to True/False
        if value:
            self._conn._conn.telemetry_enabled = True
            self._conn._telemetry_client.telemetry._enabled = True
        else:
            self._conn._conn.telemetry_enabled = False
            self._conn._telemetry_client.telemetry._enabled = False

    @property
    def file(self) -> FileOperation:
        """
        Returns a :class:`FileOperation` object that you can use to perform file operations on stages.
        See details of how to use this object in :class:`FileOperation`.
        """
        return self._file

    @property
    def udf(self) -> UDFRegistration:
        """
        Returns a :class:`udf.UDFRegistration` object that you can use to register UDFs.
        See details of how to use this object in :class:`udf.UDFRegistration`.
        """
        return self._udf_registration

    @property
    def udtf(self) -> UDTFRegistration:
        """
        Returns a :class:`udtf.UDTFRegistration` object that you can use to register UDTFs.
        See details of how to use this object in :class:`udtf.UDTFRegistration`.
        """
        return self._udtf_registration

    @property
    def sproc(self) -> StoredProcedureRegistration:
        """
        Returns a :class:`stored_procedure.StoredProcedureRegistration` object that you can use to register stored procedures.
        See details of how to use this object in :class:`stored_procedure.StoredProcedureRegistration`.
        """
        return self._sp_registration

    def call(self, sproc_name: str, *args: Any) -> Any:
        """Calls a stored procedure by name.

        Args:
            sproc_name: The name of stored procedure in Snowflake.
            args: Arguments should be basic Python types.

        Example::

            >>> import snowflake.snowpark
            >>> from snowflake.snowpark.functions import sproc
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>>
            >>> @sproc(name="my_copy_sp", replace=True)
            ... def my_copy(session: snowflake.snowpark.Session, from_table: str, to_table: str, count: int) -> str:
            ...     session.table(from_table).limit(count).write.save_as_table(to_table)
            ...     return "SUCCESS"
            >>> _ = session.sql("create or replace table test_from(test_str varchar) as select randstr(20, random()) from table(generator(rowCount => 100))").collect()
            >>> _ = session.sql("drop table if exists test_to").collect()
            >>> session.call("my_copy_sp", "test_from", "test_to", 10)
            'SUCCESS'
            >>> session.table("test_to").count()
            10
        """
        validate_object_name(sproc_name)

        sql_args = []
        for arg in args:
            if isinstance(arg, Column):
                sql_args.append(self._analyzer.analyze(arg._expression))
            else:
                sql_args.append(to_sql(arg, infer_type(arg)))
        df = self.sql(f"CALL {sproc_name}({', '.join(sql_args)})")
        set_api_call_source(df, "Session.call")
        return df.collect()[0][0]

    @deprecated(
        version="0.7.0",
        extra_warning_text="Use `Session.table_function()` instead.",
        extra_doc_string="Use :meth:`table_function` instead.",
    )
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
                The outermost element is to be flattened if path is empty or ``None``.
            outer: If ``False``, any input rows that cannot be expanded, either because they cannot be accessed in the ``path``
                or because they have zero fields or entries, are completely omitted from the output.
                Otherwise, exactly one row is generated for zero-row expansions
                (with NULL in the KEY, INDEX, and VALUE columns).
            recursive: If ``False``, only the element referenced by ``path`` is expanded.
                Otherwise, the expansion is performed for all sub-elements recursively.
            mode: Specifies which types should be flattened "OBJECT", "ARRAY", or "BOTH".

        Returns:
            A new :class:`DataFrame` that has the flattened new columns and new rows from the compound data.

        Example::

            >>> from snowflake.snowpark.functions import lit, parse_json
            >>> session.flatten(parse_json(lit('{"a":[1,2]}')), path="a", outer=False, recursive=False, mode="BOTH").show()
            -------------------------------------------------------
            |"SEQ"  |"KEY"  |"PATH"  |"INDEX"  |"VALUE"  |"THIS"  |
            -------------------------------------------------------
            |1      |NULL   |a[0]    |0        |1        |[       |
            |       |       |        |         |         |  1,    |
            |       |       |        |         |         |  2     |
            |       |       |        |         |         |]       |
            |1      |NULL   |a[1]    |1        |2        |[       |
            |       |       |        |         |         |  1,    |
            |       |       |        |         |         |  2     |
            |       |       |        |         |         |]       |
            -------------------------------------------------------
            <BLANKLINE>

        See Also:
            - :meth:`DataFrame.flatten`, which creates a new :class:`DataFrame` by exploding a VARIANT column of an existing :class:`DataFrame`.
            - :meth:`Session.table_function`, which can be used for any Snowflake table functions, including ``flatten``.
        """

        mode = mode.upper()
        if mode not in ("OBJECT", "ARRAY", "BOTH"):
            raise ValueError("mode must be one of ('OBJECT', 'ARRAY', 'BOTH')")
        if isinstance(input, str):
            input = col(input)
        df = DataFrame(
            self,
            TableFunctionRelation(
                FlattenFunction(input._expression, path, outer, recursive, mode)
            ),
        )
        set_api_call_source(df, "Session.flatten")
        return df

    def query_history(self) -> QueryHistory:
        """Create an instance of :class:`QueryHistory` as a context manager to record queries that are pushed down to the Snowflake database.

        >>> with session.query_history() as query_history:
        ...     df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        ...     df = df.filter(df.a == 1)
        ...     res = df.collect()
        >>> assert len(query_history.queries) == 1
        """
        query_listener = QueryHistory(self)
        self._conn.add_query_listener(query_listener)
        return query_listener

    def _table_exists(self, table_name: str):
        # implementation based upon: https://docs.snowflake.com/en/sql-reference/name-resolution.html
        validate_object_name(table_name)
        # note: object name could have dots, e.g, a table could be created via: create table "abc.abc" (id int)
        # currently validate_object_name does not allow it, but if in the future we want to support the case, we need to
        # update the implementation accordingly in this method
        qualified_table_name = table_name.split(".")
        if len(qualified_table_name) == 1:
            # name in the form of "table"
            tables = self._run_query(f"show tables like '{table_name}'")
        elif len(qualified_table_name) == 2:
            # name in the form of "schema.table" omitting database
            # schema: qualified_table_name[0]
            # table: qualified_table_name[1]
            tables = self._run_query(
                f"show tables like '{qualified_table_name[1]}' in schema {qualified_table_name[0]}"
            )
        elif len(qualified_table_name) == 3:
            # name in the form of "database.schema.table"
            # database: qualified_table_name[0]
            # schema: qualified_table_name[1]
            # table: qualified_table_name[2]
            condition = (
                f"database {qualified_table_name[0]}"
                if qualified_table_name[1] == ""
                else f"schema {qualified_table_name[0]}.{qualified_table_name[1]}"
            )
            tables = self._run_query(
                f"show tables like '{qualified_table_name[2]}' in {condition}"
            )
        else:
            # we do not support len(qualified_table_name) > 3 for now
            raise SnowparkClientExceptionMessages.GENERAL_INVALID_OBJECT_NAME(
                table_name
            )

        return tables is not None and len(tables) > 0

    def _explain_query(self, query: str) -> Optional[str]:
        try:
            return self._run_query(f"explain using text {query}")[0][0]
        # return None for queries which can't be explained
        except ProgrammingError:
            _logger.warning("query '%s' cannot be explained")
            return None

    createDataFrame = create_dataframe
