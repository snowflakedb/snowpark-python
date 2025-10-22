#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import atexit
import datetime
import decimal
import inspect
import json
import os
import re
import sys
import tempfile
import warnings
from array import array
from collections import defaultdict
from functools import reduce
from logging import getLogger
from threading import RLock
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    DefaultDict,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import cloudpickle
import importlib.metadata
from packaging.requirements import Requirement
from packaging.version import parse as parse_version

import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
import snowflake.snowpark.context as context
from snowflake.connector import ProgrammingError, SnowflakeConnection
from snowflake.connector.options import installed_pandas, pandas, pyarrow
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark._internal.analyzer import analyzer_utils
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    result_scan_statement,
    write_arrow,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import str_to_sql
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.select_statement import (
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
    GeneratorTableFunction,
    TableFunctionRelation,
)
from snowflake.snowpark._internal.analyzer.unary_expression import Cast
from snowflake.snowpark._internal.ast.batch import AstBatch
from snowflake.snowpark._internal.ast.utils import (
    add_intermediate_stmt,
    build_expr_from_python_val,
    build_indirect_table_fn_apply,
    build_proto_from_struct_type,
    build_table_name,
    with_src_position,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.packaging_utils import (
    DEFAULT_PACKAGES,
    ENVIRONMENT_METADATA_FILE_NAME,
    IMPLICIT_ZIP_FILE_NAME,
    delete_files_belonging_to_packages,
    detect_native_dependencies,
    get_signature,
    identify_supported_packages,
    map_python_packages_to_files_and_folders,
    parse_conda_environment_yaml_file,
    parse_requirements_text_file,
    pip_install_packages_to_target_folder,
    zip_directory_contents,
)
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark._internal.telemetry import set_api_call_source
from snowflake.snowpark._internal.temp_table_auto_cleaner import TempTableAutoCleaner
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    convert_sp_to_sf_type,
    infer_schema,
    infer_type,
    merge_type,
    type_string_to_type_object,
)
from snowflake.snowpark._internal.udf_utils import generate_call_python_sp_sql
from snowflake.snowpark._internal.utils import (
    MODULE_NAME_TO_PACKAGE_NAME_MAP,
    STAGE_PREFIX,
    SUPPORTED_TABLE_TYPES,
    XPATH_HANDLERS_FILE_PATH,
    XPATH_HANDLER_MAP,
    PythonObjJSONEncoder,
    TempObjectType,
    calculate_checksum,
    check_flatten_mode,
    create_rlock,
    create_thread_local,
    deprecated,
    escape_quotes,
    experimental,
    experimental_parameter,
    get_connector_version,
    get_os_name,
    get_python_version,
    get_stage_file_prefix_length,
    get_temp_type_for_object,
    get_version,
    import_or_missing_modin_pandas,
    is_in_stored_procedure,
    normalize_local_file,
    normalize_remote_file_or_dir,
    parse_positional_args_to_list,
    parse_positional_args_to_list_variadic,
    publicapi,
    quote_name,
    random_name_for_temp_object,
    strip_double_quotes_in_like_statement_in_table_name,
    unwrap_single_quote,
    unwrap_stage_location_single_quote,
    validate_object_name,
    warn_session_config_update_in_multithreaded_mode,
    warning,
    zip_file_or_directory_to_stream,
    set_ast_state,
    is_ast_enabled,
    AstFlagSource,
    AstMode,
)
from snowflake.snowpark.async_job import AsyncJob, _AsyncResultType
from snowflake.snowpark.column import Column
from snowflake.snowpark.context import (
    _is_execution_environment_sandboxed_for_client,
    _use_scoped_temp_objects,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.exceptions import (
    SnowparkClientException,
    SnowparkSessionException,
)
from snowflake.snowpark.file_operation import FileOperation
from snowflake.snowpark.functions import (
    to_file,
    array_agg,
    col,
    column,
    lit,
    parse_json,
    to_date,
    to_decimal,
    to_geography,
    to_geometry,
    to_time,
    to_timestamp,
    to_timestamp_ltz,
    to_timestamp_ntz,
    to_timestamp_tz,
    to_variant,
)
from snowflake.snowpark.lineage import Lineage
from snowflake.snowpark.mock._analyzer import MockAnalyzer
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.mock._nop_analyzer import NopAnalyzer
from snowflake.snowpark.mock._nop_connection import NopConnection
from snowflake.snowpark.mock._pandas_util import (
    _convert_dataframe_to_table,
    _extract_schema_and_data_from_pandas_df,
)
from snowflake.snowpark.mock._plan_builder import MockSnowflakePlanBuilder
from snowflake.snowpark.mock._stored_procedure import MockStoredProcedureRegistration
from snowflake.snowpark.mock._udaf import MockUDAFRegistration
from snowflake.snowpark.mock._udf import MockUDFRegistration
from snowflake.snowpark.mock._udtf import MockUDTFRegistration
from snowflake.snowpark.query_history import AstListener, QueryHistory
from snowflake.snowpark.row import Row
from snowflake.snowpark.stored_procedure import StoredProcedureRegistration
from snowflake.snowpark.stored_procedure_profiler import StoredProcedureProfiler
from snowflake.snowpark.dataframe_profiler import DataframeProfiler
from snowflake.snowpark.table import Table
from snowflake.snowpark.table_function import (
    TableFunctionCall,
    _create_table_function_expression,
)
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    FloatType,
    GeographyType,
    GeometryType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
    VectorType,
    YearMonthIntervalType,
    FileType,
    _AtomicType,
)
from snowflake.snowpark.udaf import UDAFRegistration
from snowflake.snowpark.udf import UDFRegistration
from snowflake.snowpark.udtf import UDTFRegistration

if TYPE_CHECKING:
    import modin.pandas  # pragma: no cover
    from snowflake.snowpark.udf import UserDefinedFunction  # pragma: no cover

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

_logger = getLogger(__name__)

_session_management_lock = RLock()
_active_sessions: Set["Session"] = set()
_PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING = (
    "PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS"
)
_PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING = "PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER"
_PYTHON_SNOWPARK_USE_LOGICAL_TYPE_FOR_CREATE_DATAFRAME_STRING = (
    "PYTHON_SNOWPARK_USE_LOGICAL_TYPE_FOR_CREATE_DATAFRAME"
)
# parameter used to turn off the whole new query compilation stage in one shot. If turned
# off, the plan won't go through the extra optimization and query generation steps.
_PYTHON_SNOWPARK_ENABLE_QUERY_COMPILATION_STAGE = (
    "PYTHON_SNOWPARK_COMPILATION_STAGE_ENABLED"
)
_PYTHON_SNOWPARK_USE_CTE_OPTIMIZATION_VERSION = (
    "PYTHON_SNOWPARK_USE_CTE_OPTIMIZATION_VERSION"
)
_PYTHON_SNOWPARK_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED = (
    "PYTHON_SNOWPARK_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED"
)
_PYTHON_SNOWPARK_AUTO_CLEAN_UP_TEMP_TABLE_ENABLED_VERSION = (
    "PYTHON_SNOWPARK_AUTO_CLEAN_UP_TEMP_TABLE_ENABLED_VERSION"
)
_PYTHON_SNOWPARK_REDUCE_DESCRIBE_QUERY_ENABLED = (
    "PYTHON_SNOWPARK_REDUCE_DESCRIBE_QUERY_ENABLED"
)
_PYTHON_SNOWPARK_USE_LARGE_QUERY_BREAKDOWN_OPTIMIZATION_VERSION = (
    "PYTHON_SNOWPARK_USE_LARGE_QUERY_BREAKDOWN_OPTIMIZATION_VERSION"
)
_PYTHON_SNOWPARK_LARGE_QUERY_BREAKDOWN_COMPLEXITY_UPPER_BOUND = (
    "PYTHON_SNOWPARK_LARGE_QUERY_BREAKDOWN_COMPLEXITY_UPPER_BOUND"
)
_PYTHON_SNOWPARK_LARGE_QUERY_BREAKDOWN_COMPLEXITY_LOWER_BOUND = (
    "PYTHON_SNOWPARK_LARGE_QUERY_BREAKDOWN_COMPLEXITY_LOWER_BOUND"
)
# Flag to controlling multithreading behavior
_PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION = (
    "PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION"
)
# Flag to control sending snowflake plan telemetry data from get_result_set
_PYTHON_SNOWPARK_COLLECT_TELEMETRY_AT_CRITICAL_PATH_VERSION = (
    "PYTHON_SNOWPARK_COLLECT_TELEMETRY_AT_CRITICAL_PATH_VERSION"
)
# Flag for controlling the usage of scoped temp read only table.
_PYTHON_SNOWPARK_ENABLE_SCOPED_TEMP_READ_ONLY_TABLE = (
    "PYTHON_SNOWPARK_ENABLE_SCOPED_TEMP_READ_ONLY_TABLE"
)
_PYTHON_SNOWPARK_DATAFRAME_JOIN_ALIAS_FIX_VERSION = (
    "PYTHON_SNOWPARK_DATAFRAME_JOIN_ALIAS_FIX_VERSION"
)
_PYTHON_SNOWPARK_CLIENT_AST_MODE = "PYTHON_SNOWPARK_CLIENT_AST_MODE"
_PYTHON_SNOWPARK_CLIENT_MIN_VERSION_FOR_AST = (
    "PYTHON_SNOWPARK_CLIENT_MIN_VERSION_FOR_AST"
)
_PYTHON_SNOWPARK_GENERATE_MULTILINE_QUERIES = (
    "PYTHON_SNOWPARK_GENERATE_MULTILINE_QUERIES"
)
_PYTHON_SNOWPARK_INTERNAL_TELEMETRY_ENABLED = "ENABLE_SNOWPARK_FIRST_PARTY_TELEMETRY"
_SNOWPARK_PANDAS_DUMMY_ROW_POS_OPTIMIZATION_ENABLED = (
    "SNOWPARK_PANDAS_DUMMY_ROW_POS_OPTIMIZATION_ENABLED"
)
_SNOWPARK_PANDAS_HYBRID_EXECUTION_ENABLED = "SNOWPARK_PANDAS_HYBRID_EXECUTION_ENABLED"

# AST encoding.
_PYTHON_SNOWPARK_USE_AST = "PYTHON_SNOWPARK_USE_AST"
# TODO SNOW-1677514: Add server-side flag and initialize value with it. Add telemetry support for flag.
_PYTHON_SNOWPARK_USE_AST_DEFAULT_VALUE = False
# The complexity score lower bound is set to match COMPILATION_MEMORY_LIMIT
# in Snowflake. This is the limit where we start seeing compilation errors.
DEFAULT_COMPLEXITY_SCORE_LOWER_BOUND = 10_000_000
DEFAULT_COMPLEXITY_SCORE_UPPER_BOUND = 12_000_000
WRITE_PANDAS_CHUNK_SIZE: int = 100000 if is_in_stored_procedure() else None
WRITE_ARROW_CHUNK_SIZE: int = 100000 if is_in_stored_procedure() else None


def _get_active_session() -> "Session":
    with _session_management_lock:
        if len(_active_sessions) == 1:
            return next(iter(_active_sessions))
        elif len(_active_sessions) > 1:
            raise SnowparkClientExceptionMessages.MORE_THAN_ONE_ACTIVE_SESSIONS()
        else:
            raise SnowparkClientExceptionMessages.SERVER_NO_DEFAULT_SESSION()


def _get_active_sessions() -> Set["Session"]:
    with _session_management_lock:
        if len(_active_sessions) >= 1:
            # TODO: This function is allowing unsafe access to a mutex protected data
            #  structure, we should ONLY use it in tests
            return _active_sessions
        else:
            raise SnowparkClientExceptionMessages.SERVER_NO_DEFAULT_SESSION()


def _add_session(session: "Session") -> None:
    with _session_management_lock:
        _active_sessions.add(session)


def _get_sandbox_conditional_active_session(session: "Session") -> "Session":
    # Precedence to checking sandbox to avoid any side effects
    if _is_execution_environment_sandboxed_for_client:
        session = None
    else:
        session = session or _get_active_session()
    return session


def _remove_session(session: "Session") -> None:
    with _session_management_lock:
        try:
            _active_sessions.remove(session)
        except KeyError:
            pass


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
        ...     "schema": "<schema_name>",
        ... }
        >>> session = Session.builder.configs(connection_parameters).create() # doctest: +SKIP

    To create a :class:`Session` object from an existing Python Connector connection::

        >>> session = Session.builder.configs({"connection": <your python connector connection>}).create() # doctest: +SKIP

    :class:`Session` contains functions to construct a :class:`DataFrame` like :meth:`table`,
    :meth:`sql` and :attr:`read`, etc.
    """

    class RuntimeConfig:
        def __init__(self, session: "Session", conf: Dict[str, Any]) -> None:
            self._session = session
            self._conf = {
                "use_constant_subquery_alias": True,
                "flatten_select_after_filter_and_orderby": True,
                "collect_stacktrace_in_query_tag": False,
                "use_simplified_query_generation": False,
            }  # For config that's temporary/to be removed soon
            self._lock = self._session._lock
            for key, val in conf.items():
                if self.is_mutable(key):
                    self.set(key, val)

        def get(self, key: str, default=None) -> Any:
            with self._lock:
                if hasattr(Session, key):
                    return getattr(self._session, key)
                if hasattr(self._session._conn._conn, key):
                    return getattr(self._session._conn._conn, key)
                return self._conf.get(key, default)

        def is_mutable(self, key: str) -> bool:
            with self._lock:
                if hasattr(Session, key) and isinstance(
                    getattr(Session, key), property
                ):
                    return getattr(Session, key).fset is not None
                if hasattr(SnowflakeConnection, key) and isinstance(
                    getattr(SnowflakeConnection, key), property
                ):
                    return getattr(SnowflakeConnection, key).fset is not None
                return key in self._conf

        def set(self, key: str, value: Any) -> None:
            with self._lock:
                if self.is_mutable(key):
                    if hasattr(Session, key):
                        setattr(self._session, key, value)
                    if hasattr(SnowflakeConnection, key):
                        setattr(self._session._conn._conn, key, value)
                    if key in self._conf:
                        self._conf[key] = value
                else:
                    raise AttributeError(
                        f'Configuration "{key}" does not exist or is not mutable in runtime'
                    )

    class SessionBuilder:
        """
        Provides methods to set connection parameters and create a :class:`Session`.
        """

        def __init__(self) -> None:
            self._options = {}
            self._app_name = None
            self._format_json = None

        def _remove_config(self, key: str) -> "Session.SessionBuilder":
            """Only used in test."""
            self._options.pop(key, None)
            return self

        def app_name(
            self, app_name: str, format_json: bool = False
        ) -> "Session.SessionBuilder":
            """
            Adds the app name to the :class:`SessionBuilder` to set in the query_tag after session creation

            Args:
                app_name: The name of the application.
                format_json: If set to `True`, it will add the app name to the session query tag in JSON format,
                    otherwise, it will add it using a key=value format.

            Returns:
                A :class:`SessionBuilder` instance.

            Example::
                >>> session = Session.builder.app_name("my_app").configs(db_parameters).create() # doctest: +SKIP
                >>> print(session.query_tag) # doctest: +SKIP
                APPNAME=my_app
                >>> session = Session.builder.app_name("my_app", format_json=True).configs(db_parameters).create() # doctest: +SKIP
                >>> print(session.query_tag) # doctest: +SKIP
                {"APPNAME": "my_app"}
            """
            self._app_name = app_name
            self._format_json = format_json
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
            if self._options.get("local_testing", False):
                session = Session(MockServerConnection(self._options), self._options)
                if "password" in self._options:
                    self._options["password"] = None
                _add_session(session)
            elif self._options.get("nop_testing", False):
                session = Session(NopConnection(self._options), self._options)
                if "password" in self._options:
                    self._options["password"] = None
                _add_session(session)
            else:
                session = self._create_internal(self._options.get("connection"))

            if self._app_name:
                if self._format_json:
                    app_name_tag = {"APPNAME": self._app_name}
                    session.update_query_tag(app_name_tag)
                else:
                    app_name_tag = f"APPNAME={self._app_name}"
                    session.append_query_tag(app_name_tag)

            return session

        def getOrCreate(self) -> "Session":
            """Gets the last created session or creates a new one if needed."""
            try:
                session = _get_active_session()
                if session._conn._conn.expired:
                    _remove_session(session)
                    return self.create()
                return session
            except SnowparkClientException as ex:
                if ex.error_code == "1403":  # No session, ok lets create one
                    return self.create()
                raise

        def _create_internal(
            self,
            conn: Optional[SnowflakeConnection] = None,
        ) -> "Session":
            # If no connection object and no connection parameter is provided,
            # we read from the default config file
            if not is_in_stored_procedure() and not conn and not self._options:
                from snowflake.connector.config_manager import (
                    _get_default_connection_params,
                )

                self._options = _get_default_connection_params()

            # Set paramstyle to qmark by default to be consistent with previous behavior
            if "paramstyle" not in self._options:
                self._options["paramstyle"] = "qmark"
            new_session = Session(
                ServerConnection({}, conn) if conn else ServerConnection(self._options),
                self._options,
            )

            if "password" in self._options:
                self._options["password"] = None
            _add_session(new_session)
            return new_session

        appName = app_name

        def __get__(self, obj, objtype=None):
            return Session.SessionBuilder()

    #: Returns a builder you can use to set configuration properties
    #: and create a :class:`Session` object.
    builder: SessionBuilder = SessionBuilder()

    def __init__(
        self,
        conn: Union[ServerConnection, MockServerConnection, NopConnection],
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        if (
            len(_active_sessions) >= 1
            and is_in_stored_procedure()
            and not conn._get_client_side_session_parameter(
                "ENABLE_CREATE_SESSION_IN_STORED_PROCS", False
            )
        ):
            raise SnowparkClientExceptionMessages.DONT_CREATE_SESSION_IN_SP()
        self._conn = conn
        self._query_tag = None
        self._import_paths: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        self._packages: Dict[str, str] = {}
        self._artifact_repository_packages: DefaultDict[
            str, Dict[str, str]
        ] = defaultdict(dict)
        self._session_id = self._conn.get_session_id()
        self._session_info = f"""
"version" : {get_version()},
"python.version" : {get_python_version()},
"python.connector.version" : {get_connector_version()},
"python.connector.session.id" : {self._session_id},
"os.name" : {get_os_name()}
"""
        self.version = get_version()
        self._session_stage = None

        if isinstance(conn, MockServerConnection):
            self._udf_registration = MockUDFRegistration(self)
            self._udtf_registration = MockUDTFRegistration(self)
            self._udaf_registration = MockUDAFRegistration(self)
            self._sp_registration = MockStoredProcedureRegistration(self)
        else:
            self._udf_registration = UDFRegistration(self)
            self._sp_registration = StoredProcedureRegistration(self)
            self._udtf_registration = UDTFRegistration(self)
            self._udaf_registration = UDAFRegistration(self)

        self._plan_builder = (
            SnowflakePlanBuilder(self)
            if isinstance(self._conn, ServerConnection)
            else MockSnowflakePlanBuilder(self)
        )
        self._last_action_id = 0
        self._last_canceled_id = 0
        self._use_scoped_temp_objects: bool = (
            _use_scoped_temp_objects
            and self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING, True
            )
        )
        self._use_scoped_temp_read_only_table: bool = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_ENABLE_SCOPED_TEMP_READ_ONLY_TABLE, False
            )
        )
        self._file = FileOperation(self)
        self._lineage = Lineage(self)
        self._sql_simplifier_enabled: bool = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING, True
            )
        )
        self._cte_optimization_enabled: bool = self.is_feature_enabled_for_version(
            _PYTHON_SNOWPARK_USE_CTE_OPTIMIZATION_VERSION
        )
        self._use_logical_type_for_create_df: bool = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_USE_LOGICAL_TYPE_FOR_CREATE_DATAFRAME_STRING, True
            )
        )
        self._eliminate_numeric_sql_value_cast_enabled: bool = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED, False
            )
        )
        self._auto_clean_up_temp_table_enabled: bool = (
            self.is_feature_enabled_for_version(
                _PYTHON_SNOWPARK_AUTO_CLEAN_UP_TEMP_TABLE_ENABLED_VERSION
            )
        )
        self._reduce_describe_query_enabled: bool = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_REDUCE_DESCRIBE_QUERY_ENABLED, False
            )
        )
        self._query_compilation_stage_enabled: bool = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_ENABLE_QUERY_COMPILATION_STAGE, False
            )
        )
        self._generate_multiline_queries: bool = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_GENERATE_MULTILINE_QUERIES, False
            )
        )
        if self._generate_multiline_queries:
            self._enable_multiline_queries()
        else:
            self._disable_multiline_queries()

        self._internal_telemetry_enabled: bool = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_INTERNAL_TELEMETRY_ENABLED, False
            )
        )

        self._large_query_breakdown_enabled: bool = self.is_feature_enabled_for_version(
            _PYTHON_SNOWPARK_USE_LARGE_QUERY_BREAKDOWN_OPTIMIZATION_VERSION
        )
        ast_mode_value: int = self._conn._get_client_side_session_parameter(
            _PYTHON_SNOWPARK_CLIENT_AST_MODE, None
        )
        if ast_mode_value is not None and isinstance(ast_mode_value, int):
            self._ast_mode = AstMode(ast_mode_value)
        else:
            self._ast_mode = None

        # If PYTHON_SNOWPARK_AST_MODE is available, it has precedence over PYTHON_SNOWPARK_USE_AST.
        if self._ast_mode is None:
            ast_enabled: bool = self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_USE_AST, _PYTHON_SNOWPARK_USE_AST_DEFAULT_VALUE
            )
            self._ast_mode = AstMode.SQL_AND_AST if ast_enabled else AstMode.SQL_ONLY
        else:
            if self._ast_mode == AstMode.AST_ONLY:
                _logger.warning(
                    "Snowpark python client does not support dataframe requests, downgrading to SQL_AND_AST."
                )
                self._ast_mode = AstMode.SQL_AND_AST

            ast_enabled = self._ast_mode == AstMode.SQL_AND_AST

        if self._ast_mode != AstMode.SQL_ONLY:
            if (
                self._conn._get_client_side_session_parameter(
                    _PYTHON_SNOWPARK_CLIENT_MIN_VERSION_FOR_AST, None
                )
                is not None
            ):
                ast_supported_version: bool = self.is_feature_enabled_for_version(
                    _PYTHON_SNOWPARK_CLIENT_MIN_VERSION_FOR_AST
                )
                if not ast_supported_version:
                    _logger.warning(
                        "Server side dataframe support requires minimum snowpark-python client version."
                    )
                    self._ast_mode = AstMode.SQL_ONLY
                    ast_enabled = False

        set_ast_state(AstFlagSource.SERVER, ast_enabled)
        # The complexity score lower bound is set to match COMPILATION_MEMORY_LIMIT
        # in Snowflake. This is the limit where we start seeing compilation errors.
        self._large_query_breakdown_complexity_bounds: Tuple[int, int] = (
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_LARGE_QUERY_BREAKDOWN_COMPLEXITY_LOWER_BOUND,
                DEFAULT_COMPLEXITY_SCORE_LOWER_BOUND,
            ),
            self._conn._get_client_side_session_parameter(
                _PYTHON_SNOWPARK_LARGE_QUERY_BREAKDOWN_COMPLEXITY_UPPER_BOUND,
                DEFAULT_COMPLEXITY_SCORE_UPPER_BOUND,
            ),
        )
        # TODO: SNOW-1951048 local testing diamond join fix
        self._join_alias_fix: bool = self.is_feature_enabled_for_version(
            _PYTHON_SNOWPARK_DATAFRAME_JOIN_ALIAS_FIX_VERSION
        )

        self._dummy_row_pos_optimization_enabled: bool = (
            self._conn._get_client_side_session_parameter(
                _SNOWPARK_PANDAS_DUMMY_ROW_POS_OPTIMIZATION_ENABLED, True
            )
        )

        if "modin" in sys.modules:
            try:
                from modin.config import AutoSwitchBackend

                pandas_hybrid_execution_enabled: Union[
                    bool, None
                ] = self._conn._get_client_side_session_parameter(
                    _SNOWPARK_PANDAS_HYBRID_EXECUTION_ENABLED, None
                )
                # Only set AutoSwitchBackend if the session parameter was already set.
                # snowflake.snowpark.modin.plugin sets AutoSwitchBackend to True if it was
                # not already set, so we should not change the variable if it's in its default state.
                if pandas_hybrid_execution_enabled is not None:
                    AutoSwitchBackend.put(pandas_hybrid_execution_enabled)
            except Exception:
                # Continue session initialization even if Modin configuration fails
                pass

        self._thread_store = create_thread_local(
            self._conn._thread_safe_session_enabled
        )
        self._lock = create_rlock(self._conn._thread_safe_session_enabled)

        # this lock is used to protect _packages. We use introduce a new lock because add_packages
        # launches a query to snowflake to get all version of packages available in snowflake. This
        # query can be slow and prevent other threads from moving on waiting for _lock.
        self._package_lock = create_rlock(self._conn._thread_safe_session_enabled)

        # this lock is used to protect race-conditions when evaluating critical lazy properties
        # of SnowflakePlan or Selectable objects
        self._plan_lock = create_rlock(self._conn._thread_safe_session_enabled)

        # this lock is used to protect XPath UDF cache to avoid race conditions during
        # UDF registration and retrieval
        self._xpath_udf_cache_lock = create_rlock(
            self._conn._thread_safe_session_enabled
        )

        self._custom_package_usage_config: Dict = {}

        # Cache for XPath UDFs (thread-safe)
        self._xpath_udf_cache: Dict[str, Any] = {}

        self._collect_snowflake_plan_telemetry_at_critical_path: bool = (
            self.is_feature_enabled_for_version(
                _PYTHON_SNOWPARK_COLLECT_TELEMETRY_AT_CRITICAL_PATH_VERSION
            )
        )
        self._conf = self.RuntimeConfig(self, options or {})
        self._runtime_version_from_requirement: str = None
        self._temp_table_auto_cleaner: TempTableAutoCleaner = TempTableAutoCleaner(self)
        self._sp_profiler = StoredProcedureProfiler(session=self)
        self._dataframe_profiler = DataframeProfiler(session=self)
        self._catalog = None

        self._ast_batch = AstBatch(self)

        _logger.info("Snowpark Session information: %s", self._session_info)

        # Register self._close_at_exit so it will be called at interpreter shutdown
        atexit.register(self._close_at_exit)

    def _close_at_exit(self) -> None:
        """
        This is the helper function to close the current session at interpreter shutdown.
        For example, when a jupyter notebook is shutting down, this will also close
        the current session and make sure send all telemetry to the server.
        """
        with _session_management_lock:
            try:
                self.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not is_in_stored_procedure():
            self.close()

    def __str__(self):
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}: account={self.get_current_account()}, "
            f"role={self.get_current_role()}, database={self.get_current_database()}, "
            f"schema={self.get_current_schema()}, warehouse={self.get_current_warehouse()}>"
        )

    def _enable_multiline_queries(self):
        import snowflake.snowpark._internal.analyzer.analyzer_utils as analyzer_utils

        self._generate_multiline_queries = True
        analyzer_utils.NEW_LINE = "\n"
        analyzer_utils.TAB = "    "

    def _disable_multiline_queries(self):
        import snowflake.snowpark._internal.analyzer.analyzer_utils as analyzer_utils

        self._generate_multiline_queries = False
        analyzer_utils.NEW_LINE = ""
        analyzer_utils.TAB = ""

    def is_feature_enabled_for_version(self, parameter_name: str) -> bool:
        """
        This method checks if a feature is enabled for the current session based on
        the server side parameter.
        """
        version = self._conn._get_client_side_session_parameter(parameter_name, "")
        return (
            isinstance(version, str)
            and version != ""
            and parse_version(self.version) >= parse_version(version)
        )

    def _generate_new_action_id(self) -> int:
        with self._lock:
            self._last_action_id += 1
            return self._last_action_id

    @property
    def _analyzer(self) -> Analyzer:
        if not hasattr(self._thread_store, "analyzer"):
            analyzer: Union[Analyzer, MockAnalyzer, NopAnalyzer, None] = None
            if isinstance(self._conn, NopConnection):
                analyzer = NopAnalyzer(self)
            elif isinstance(self._conn, MockServerConnection):
                analyzer = MockAnalyzer(self)
            else:
                analyzer = Analyzer(self)

            self._thread_store.analyzer = analyzer
        return self._thread_store.analyzer

    @classmethod
    def get_active_session(cls) -> Optional["Session"]:
        """Gets the active session if one is created. If no session is created, returns None."""
        try:
            return _get_active_session()
        except SnowparkClientException as ex:
            # If there is no active session, return None
            if ex.error_code == "1403":
                return None
            raise ex

    getActiveSession = get_active_session

    @property
    @experimental(version="1.27.0")
    def catalog(self):
        """Returns a :class:`Catalog` object rooted on current session."""
        from snowflake.snowpark.catalog import Catalog

        if self._catalog is None:
            if isinstance(self._conn, MockServerConnection):
                self._conn.log_not_supported_error(
                    external_feature_name="Session.catalog",
                    raise_error=NotImplementedError,
                )
            self._catalog = Catalog(self)
        return self._catalog

    def close(self) -> None:
        """Close this session."""
        if is_in_stored_procedure():
            _logger.warning("Closing a session in a stored procedure is a no-op.")
            return
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
                self._temp_table_auto_cleaner.stop()
                self._ast_batch.clear()
                self._conn.close()
                _logger.info("Closed session: %s", self._session_id)
            finally:
                _remove_session(self)

    @property
    def conf(self) -> RuntimeConfig:
        return self._conf

    @property
    def sql_simplifier_enabled(self) -> bool:
        """Set to ``True`` to use the SQL simplifier (defaults to ``True``).
        The generated SQLs from ``DataFrame`` transformations would have fewer layers of nested queries if the SQL simplifier is enabled.
        """
        return self._sql_simplifier_enabled

    @property
    def ast_enabled(self) -> bool:
        """
        Set to ``True`` to enable the AST (Abstract Syntax Tree) capture for ``DataFrame`` operations.

        This is an internal, experimental feature that is not yet fully supported. The value of the parameter is controlled by the Snowflake service.

        It is not possible to re-enable this feature if the system or a user explicitly disables it. Setting this property to ``True`` can result in no change if the setting was already set explicitly to ``False`` internally.

        Returns:
            The current value of the property.
        """
        return is_ast_enabled()

    @ast_enabled.setter
    @experimental_parameter(version="1.33.0")
    def ast_enabled(self, value: bool) -> None:
        # TODO: we could send here explicit telemetry if a user changes the behavior.
        # In addition, we could introduce a server-side parameter to enable AST capture or not.
        # self._conn._telemetry_client.send_ast_enabled_telemetry(
        #     self._session_id, value
        # )
        # try:
        #     self._conn._cursor.execute(
        #         f"alter session set {_PYTHON_SNOWPARK_USE_AST} = {value}"
        #     )
        # except Exception:
        #     pass

        # Auto temp cleaner has bad interactions with AST at the moment, disable when enabling AST.
        # This feature should get moved server-side anyways.
        if value and self._auto_clean_up_temp_table_enabled:
            _logger.warning(
                "TODO SNOW-1770278: Ensure auto temp table cleaner works with AST."
                " Disabling auto temp cleaner for full test suite due to buggy behavior."
            )
            self._auto_clean_up_temp_table_enabled = False
        set_ast_state(AstFlagSource.USER, value)

    @property
    def cte_optimization_enabled(self) -> bool:
        """Set to ``True`` to enable the CTE optimization (defaults to ``False``).
        The generated SQLs from ``DataFrame`` transformations would have duplicate subquery as CTEs if the CTE optimization is enabled.
        """
        return self._cte_optimization_enabled

    @property
    def eliminate_numeric_sql_value_cast_enabled(self) -> bool:
        return self._eliminate_numeric_sql_value_cast_enabled

    @property
    def auto_clean_up_temp_table_enabled(self) -> bool:
        """
        When setting this parameter to ``True``, Snowpark will automatically clean up temporary tables created by
        :meth:`DataFrame.cache_result` in the current session when the DataFrame is no longer referenced (i.e., gets garbage collected).
        The default value is ``False``.

        Note:
            Temporary tables will only be dropped if this parameter is enabled during garbage collection.
            If a temporary table is no longer referenced when the parameter is on, it will be dropped during garbage collection.
            However, if garbage collection occurs while the parameter is off, the table will not be removed.
            Note that Python's garbage collection is triggered opportunistically, with no guaranteed timing.
        """
        return self._auto_clean_up_temp_table_enabled

    @property
    def large_query_breakdown_enabled(self) -> bool:
        return self._large_query_breakdown_enabled

    @property
    def large_query_breakdown_complexity_bounds(self) -> Tuple[int, int]:
        return self._large_query_breakdown_complexity_bounds

    @property
    def reduce_describe_query_enabled(self) -> bool:
        """
        When setting this parameter to ``True``, Snowpark will infer the schema of DataFrame locally if possible,
        instead of issuing an internal `describe query
        <https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#retrieving-column-metadata>`_
        to get the schema from the Snowflake server. This optimization improves the performance of your workloads by
        reducing the number of describe queries issued to the server.
        The default value is ``False``.
        """
        return self._reduce_describe_query_enabled

    @property
    def dummy_row_pos_optimization_enabled(self) -> bool:
        """Set to ``True`` to enable the dummy row position optimization (defaults to ``True``).
        The generated SQLs from pandas transformations would potentially have fewer expensive window functions to compute the row position column.
        """
        return self._dummy_row_pos_optimization_enabled

    @property
    def pandas_hybrid_execution_enabled(self) -> bool:
        """Set to ``True`` to enable hybrid execution mode (has the same default as AutoSwitchBackend).
        When enabled, certain operations on smaller data will automatically execute in native pandas in-memory.
        This can significantly improve performance for operations that are more efficient in pandas than in Snowflake.
        """
        if not importlib.util.find_spec("modin"):
            raise ImportError(
                "The 'modin' package is required to enable this feature. Please install it first."
            )

        from modin.config import AutoSwitchBackend

        return AutoSwitchBackend().get()

    @property
    def custom_package_usage_config(self) -> Dict:
        """Get or set configuration parameters related to usage of custom Python packages in Snowflake.

        If enabled, pure Python packages that are not available in Snowflake will be installed locally via pip and made available
        as an import (see :func:`add_import` for more information on imports). You can speed up this process by mentioning
        a remote stage path as ``cache_path`` where unsupported pure Python packages will be persisted. To use a specific
        version of pip, you can set the environment variable ``PIP_PATH`` to point to your pip executable. To use custom
        Python packages which are not purely Python, specify the ``force_push`` configuration parameter (*note that using
        non-pure Python packages is not recommended!*).

        This feature is **experimental**, please do not use it in production!

        Configurations:
            - **enabled** (*bool*): Turn on usage of custom pure Python packages.
            - **force_push** (*bool*): Use Python packages regardless of whether the packages are pure Python or not.
            - **cache_path** (*str*): Cache custom Python packages on a stage directory. This parameter greatly reduces latency of custom package import.
            - **force_cache** (*bool*): Use this parameter if you specified a ``cache_path`` but wish to create a fresh cache of your environment.

        Args:
            config (dict): Dictionary containing configuration parameters mentioned above (defaults to empty dictionary).

        Example::

            >>> from snowflake.snowpark.functions import udf
            >>> session.custom_package_usage_config = {"enabled": True, "cache_path": "@my_permanent_stage/folder"} # doctest: +SKIP
            >>> session.add_packages("package_unavailable_in_snowflake") # doctest: +SKIP
            >>> @udf
            ... def use_my_custom_package() -> str:
            ...     import package_unavailable_in_snowflake
            ...     return "works"
            >>> session.clear_packages()
            >>> session.clear_imports()

        Note:
            - These configurations allow custom package addition via :func:`Session.add_requirements` and :func:`Session.add_packages`.
            - These configurations also allow custom package addition for all UDFs or stored procedures created later in the current session. If you only want to add custom packages for a specific UDF, you can use ``packages`` argument in :func:`functions.udf` or :meth:`session.udf.register() <snowflake.snowpark.udf.UDFRegistration.register>`.
        """
        return self._custom_package_usage_config

    @sql_simplifier_enabled.setter
    def sql_simplifier_enabled(self, value: bool) -> None:
        warn_session_config_update_in_multithreaded_mode("sql_simplifier_enabled")

        with self._lock:
            self._conn._telemetry_client.send_sql_simplifier_telemetry(
                self._session_id, value
            )
            try:
                self._conn._cursor.execute(
                    f"alter session set {_PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING} = {value}"
                )
            except Exception:
                pass
            self._sql_simplifier_enabled = value

    @cte_optimization_enabled.setter
    @experimental_parameter(version="1.15.0")
    def cte_optimization_enabled(self, value: bool) -> None:
        warn_session_config_update_in_multithreaded_mode("cte_optimization_enabled")

        with self._lock:
            if value:
                self._conn._telemetry_client.send_cte_optimization_telemetry(
                    self._session_id
                )
            self._cte_optimization_enabled = value

    @eliminate_numeric_sql_value_cast_enabled.setter
    @experimental_parameter(version="1.20.0")
    def eliminate_numeric_sql_value_cast_enabled(self, value: bool) -> None:
        """Set the value for eliminate_numeric_sql_value_cast_enabled"""
        warn_session_config_update_in_multithreaded_mode(
            "eliminate_numeric_sql_value_cast_enabled"
        )

        if value in [True, False]:
            with self._lock:
                self._conn._telemetry_client.send_eliminate_numeric_sql_value_cast_telemetry(
                    self._session_id, value
                )
                self._eliminate_numeric_sql_value_cast_enabled = value
        else:
            raise ValueError(
                "value for eliminate_numeric_sql_value_cast_enabled must be True or False!"
            )

    @auto_clean_up_temp_table_enabled.setter
    @experimental_parameter(version="1.21.0")
    def auto_clean_up_temp_table_enabled(self, value: bool) -> None:
        """Set the value for auto_clean_up_temp_table_enabled"""
        warn_session_config_update_in_multithreaded_mode(
            "auto_clean_up_temp_table_enabled"
        )

        if value in [True, False]:
            with self._lock:
                self._conn._telemetry_client.send_auto_clean_up_temp_table_telemetry(
                    self._session_id, value
                )
                self._auto_clean_up_temp_table_enabled = value
        else:
            raise ValueError(
                "value for auto_clean_up_temp_table_enabled must be True or False!"
            )

    @large_query_breakdown_enabled.setter
    @experimental_parameter(version="1.22.0")
    def large_query_breakdown_enabled(self, value: bool) -> None:
        """Set the value for large_query_breakdown_enabled. When enabled, the client will
        automatically detect large query plans and break them down into smaller partitions,
        materialize the partitions, and then combine them to execute the query to improve
        overall performance.
        """
        warn_session_config_update_in_multithreaded_mode(
            "large_query_breakdown_enabled"
        )

        if value in [True, False]:
            with self._lock:
                self._conn._telemetry_client.send_large_query_breakdown_telemetry(
                    self._session_id, value
                )
                self._large_query_breakdown_enabled = value
        else:
            raise ValueError(
                "value for large_query_breakdown_enabled must be True or False!"
            )

    @large_query_breakdown_complexity_bounds.setter
    def large_query_breakdown_complexity_bounds(self, value: Tuple[int, int]) -> None:
        """Set the lower and upper bounds for the complexity score used in large query breakdown optimization."""
        warn_session_config_update_in_multithreaded_mode(
            "large_query_breakdown_complexity_bounds"
        )

        if len(value) != 2:
            raise ValueError(
                f"Expecting a tuple of two integers. Got a tuple of length {len(value)}"
            )
        if value[0] >= value[1]:
            raise ValueError(
                f"Expecting a tuple of lower and upper bound with the lower bound less than the upper bound. Got (lower, upper) = ({value[0], value[1]})"
            )
        with self._lock:
            self._conn._telemetry_client.send_large_query_breakdown_update_complexity_bounds(
                self._session_id, value[0], value[1]
            )

            self._large_query_breakdown_complexity_bounds = value

    @reduce_describe_query_enabled.setter
    @experimental_parameter(version="1.24.0")
    def reduce_describe_query_enabled(self, value: bool) -> None:
        """Set the value for reduce_describe_query_enabled"""
        if value in [True, False]:
            self._conn._telemetry_client.send_reduce_describe_query_telemetry(
                self._session_id, value
            )
            self._reduce_describe_query_enabled = value
        else:
            raise ValueError(
                "value for reduce_describe_query_enabled must be True or False!"
            )

    @dummy_row_pos_optimization_enabled.setter
    def dummy_row_pos_optimization_enabled(self, value: bool) -> None:
        """Set the value for dummy_row_pos_optimization_enabled"""
        if value in [True, False]:
            self._dummy_row_pos_optimization_enabled = value
        else:
            raise ValueError(
                "value for dummy_row_pos_optimization_enabled must be True or False!"
            )

    @pandas_hybrid_execution_enabled.setter
    def pandas_hybrid_execution_enabled(self, value: bool) -> None:
        """Set the value for pandas_hybrid_execution_enabled"""
        if not importlib.util.find_spec("modin"):
            raise ImportError(
                "The 'modin' package is required to enable this feature. Please install it first."
            )

        from modin.config import AutoSwitchBackend

        if value in [True, False]:
            AutoSwitchBackend.put(value)
        else:
            raise ValueError(
                "value for pandas_hybrid_execution_enabled must be True or False!"
            )

    @custom_package_usage_config.setter
    @experimental_parameter(version="1.6.0")
    def custom_package_usage_config(self, config: Dict) -> None:
        with self._lock:
            self._custom_package_usage_config = {
                k.lower(): v for k, v in config.items()
            }

    def cancel_all(self) -> None:
        """
        Cancel all action methods that are running currently.
        This does not affect any action methods called in the future.
        """
        _logger.info("Canceling all running queries")
        with self._lock:
            self._last_canceled_id = self._last_action_id
        if not isinstance(self._conn, MockServerConnection):
            self._conn.run_query(
                f"select system$cancel_all_queries({self._session_id})"
            )

    def get_imports(self) -> List[str]:
        """
        Returns a list of imports added for user defined functions (UDFs).
        This list includes any Python or zip files that were added automatically by the library.
        """
        with self._lock:
            return list(self._import_paths.keys())

    def add_import(
        self,
        path: str,
        import_path: Optional[str] = None,
        chunk_size: int = 8192,
        whole_file_hash: bool = False,
    ) -> None:
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

            chunk_size: The number of bytes to hash per chunk of the uploaded files.

            whole_file_hash: By default only the first chunk of the uploaded import is hashed to save
                time. When this is set to True each uploaded file is fully hashed instead.

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
        if isinstance(self._conn, MockServerConnection):
            self.udf._import_file(path, import_path=import_path)
            self.sproc._import_file(path, import_path=import_path)

        path, checksum, leading_path = self._resolve_import_path(
            path, import_path, chunk_size, whole_file_hash
        )
        with self._lock:
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
        with self._lock:
            if abs_path not in self._import_paths:
                raise KeyError(f"{abs_path} is not found in the existing imports")
            else:
                self._import_paths.pop(abs_path)

    def clear_imports(self) -> None:
        """
        Clears all files in a stage or local files from the imports of a user-defined function (UDF).
        """
        if isinstance(self._conn, MockServerConnection):
            self.udf._clear_session_imports()
            self.sproc._clear_session_imports()
        with self._lock:
            self._import_paths.clear()

    @staticmethod
    def _resolve_import_path(
        path: str,
        import_path: Optional[str] = None,
        chunk_size: int = 8192,
        whole_file_hash: bool = False,
    ) -> Tuple[str, Optional[str], Optional[str]]:
        trimmed_path = path.strip()
        trimmed_import_path = import_path.strip() if import_path else None

        if not trimmed_path.startswith(STAGE_PREFIX):
            if not os.path.exists(trimmed_path):
                raise FileNotFoundError(f"{trimmed_path} is not found")
            if not os.path.isfile(trimmed_path) and not os.path.isdir(
                trimmed_path
            ):  # pragma: no cover
                # os.path.isfile() returns True when the passed in file is a symlink.
                # So this code might not be reachable. To avoid mistakes, keep it here for now.
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
                calculate_checksum(
                    abs_path,
                    additional_info=leading_path,
                    chunk_size=chunk_size,
                    whole_file_hash=whole_file_hash,
                ),
                leading_path,
            )
        else:
            return trimmed_path, None, None

    def _resolve_imports(
        self,
        import_only_stage: str,
        upload_and_import_stage: str,
        udf_level_import_paths: Optional[
            Dict[str, Tuple[Optional[str], Optional[str]]]
        ] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """Resolve the imports and upload local files (if any) to the stage."""
        resolved_stage_files = []
        stage_file_list = self._list_files_in_stage(
            import_only_stage, statement_params=statement_params
        )

        normalized_import_only_location = unwrap_stage_location_single_quote(
            import_only_stage
        )
        normalized_upload_and_import_location = unwrap_stage_location_single_quote(
            upload_and_import_stage
        )

        with self._lock:
            import_paths = udf_level_import_paths or self._import_paths.copy()
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
                        f"{filename} exists on {normalized_import_only_location}, skipped"
                    )
                    resolved_stage_files.append(
                        normalize_remote_file_or_dir(
                            f"{normalized_import_only_location}/{filename_with_prefix}"
                        )
                    )
                else:
                    # local directory or .py file
                    if os.path.isdir(path) or path.endswith(".py"):
                        with zip_file_or_directory_to_stream(
                            path, leading_path
                        ) as input_stream:
                            self._conn.upload_stream(
                                input_stream=input_stream,
                                stage_location=normalized_upload_and_import_location,
                                dest_filename=filename,
                                dest_prefix=prefix,
                                source_compression="DEFLATE",
                                compress_data=False,
                                overwrite=True,
                                is_in_udf=True,
                                skip_upload_on_content_match=True,
                                statement_params=statement_params,
                            )
                    # local file
                    else:
                        self._conn.upload_file(
                            path=path,
                            stage_location=normalized_upload_and_import_location,
                            dest_prefix=prefix,
                            compress_data=False,
                            overwrite=True,
                            skip_upload_on_content_match=True,
                        )
                    resolved_stage_files.append(
                        normalize_remote_file_or_dir(
                            f"{normalized_upload_and_import_location}/{filename_with_prefix}"
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
            self.sql(f"ls {normalized}", _emit_ast=False)
            .select('"name"', _emit_ast=False)
            ._internal_collect_with_tag(statement_params=statement_params)
        )
        prefix_length = get_stage_file_prefix_length(stage_location)
        return {str(row[0])[prefix_length:] for row in file_list}

    def get_packages(self, artifact_repository: Optional[str] = None) -> Dict[str, str]:
        """
        Returns a ``dict`` of packages added for user-defined functions (UDFs).
        The key of this ``dict`` is the package name and the value of this ``dict``
        is the corresponding requirement specifier.

        Args:
            artifact_repository: When set this will function will return the packages for a specific artifact repository.
        """
        with self._package_lock:
            if artifact_repository:
                return self._artifact_repository_packages[artifact_repository].copy()
            return self._packages.copy()

    def add_packages(
        self,
        *packages: Union[str, ModuleType, Iterable[Union[str, ModuleType]]],
        artifact_repository: Optional[str] = None,
    ) -> None:
        """
        Adds third-party packages as dependencies of a user-defined function (UDF).
        Use this method to add packages for UDFs as installing packages using
        `conda <https://docs.conda.io/en/latest/>`_. You can also find examples in
        :class:`~snowflake.snowpark.udf.UDFRegistration`. See details of
        `third-party Python packages in Snowflake <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html>`_.

        To use Python packages that are not available in Snowflake, refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.

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
            artifact_repository: When set this parameter specifies the artifact repository that packages will be added from. Only functions
                using that repository will use the packages. (Default None)

        Example::

            >>> import numpy as np
            >>> from snowflake.snowpark.functions import udf
            >>> import numpy
            >>> import pandas
            >>> import dateutil
            >>> # add numpy with the latest version on Snowflake Anaconda
            >>> # and pandas with the version "2.1.*"
            >>> # and dateutil with the local version in your environment
            >>> session.custom_package_usage_config = {"enabled": True}  # This is added because latest dateutil is not in snowflake yet
            >>> session.add_packages("numpy", "pandas==2.2.*", dateutil)
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
        self._resolve_packages(
            parse_positional_args_to_list(*packages),
            self._packages,
            artifact_repository=artifact_repository,
        )

    def remove_package(
        self,
        package: str,
        artifact_repository: Optional[str] = None,
    ) -> None:
        """
        Removes a third-party package from the dependency list of a user-defined function (UDF).

        Args:
            package: The package name.
            artifact_repository: When set this parameter specifies that the package should be removed
                from the default packages for a specific artifact repository.

        Examples::

            >>> session.clear_packages()
            >>> len(session.get_packages())
            0
            >>> session.add_packages("numpy", "pandas==2.2.*")
            >>> len(session.get_packages())
            2
            >>> session.remove_package("numpy")
            >>> len(session.get_packages())
            1
            >>> session.remove_package("pandas")
            >>> len(session.get_packages())
            0
        """
        package_name = Requirement(package).name
        with self._package_lock:
            if (
                artifact_repository is not None
                and package_name
                in self._artifact_repository_packages.get(artifact_repository, {})
            ):
                self._artifact_repository_packages[artifact_repository].pop(
                    package_name
                )
            elif package_name in self._packages:
                self._packages.pop(package_name)
            else:
                raise ValueError(f"{package_name} is not in the package list")

    def clear_packages(
        self,
        artifact_repository: Optional[str] = None,
    ) -> None:
        """
        Clears all third-party packages of a user-defined function (UDF). When artifact_repository
        is set packages are only clear from the specified repository.
        """
        with self._package_lock:
            if artifact_repository is not None:
                self._artifact_repository_packages.get(artifact_repository, {}).clear()
            else:
                self._packages.clear()

    def add_requirements(
        self,
        file_path: str,
        artifact_repository: Optional[str] = None,
    ) -> None:
        """
        Adds a `requirement file <https://pip.pypa.io/en/stable/user_guide/#requirements-files>`_
        that contains a list of packages as dependencies of a user-defined function (UDF). This function also supports
        addition of requirements via a `conda environment file <https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually>`_.

        To use Python packages that are not available in Snowflake, refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.

        Args:
            file_path: The path of a local requirement file.
            artifact_repository: When set this parameter specifies the artifact repository that packages will be added from. Only functions
                using that repository will use the packages. (Default None)

        Example::

            >>> from snowflake.snowpark.functions import udf
            >>> import numpy
            >>> import pandas
            >>> import sys
            >>> # test_requirements.txt contains "numpy" and "pandas"
            >>> file = "test_requirements.txt" if sys.version_info < (3, 13) else "test_requirements_py313.txt"
            >>> session.add_requirements(f"tests/resources/{file}")
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
        if file_path.endswith(".yml") or file_path.endswith(".yaml"):
            packages, runtime_version = parse_conda_environment_yaml_file(file_path)
            self._runtime_version_from_requirement = runtime_version
        else:
            packages, new_imports = parse_requirements_text_file(file_path)
            for import_path in new_imports:
                self.add_import(import_path)
        self.add_packages(packages, artifact_repository=artifact_repository)

    @experimental(version="1.7.0")
    def replicate_local_environment(
        self, ignore_packages: Set[str] = None, relax: bool = False
    ) -> None:
        """
        Adds all third-party packages in your local environment as dependencies of a user-defined function (UDF).
        Use this method to add packages for UDFs as installing packages using `conda <https://docs.conda.io/en/latest/>`_.
        You can also find examples in :class:`~snowflake.snowpark.udf.UDFRegistration`. See details of `third-party Python packages in Snowflake <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html>`_.

        If you find that certain packages are causing failures related to duplicate dependencies, try adding
        duplicate dependencies to the ``ignore_packages`` parameter. If your local environment contains Python packages
        that are not available in Snowflake, refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.

        This function is **experimental**, please do not use it in production!

        Example::

            >>> from snowflake.snowpark.functions import udf
            >>> import numpy
            >>> import pandas
            >>> # test_requirements.txt contains "numpy" and "pandas"
            >>> session.custom_package_usage_config = {"enabled": True, "force_push": True} # Recommended configuration
            >>> session.replicate_local_environment(ignore_packages={"snowflake-snowpark-python", "snowflake-connector-python", "urllib3", "tzdata", "numpy"}, relax=True)
            >>> @udf
            ... def get_package_name_udf() -> list:
            ...     return [numpy.__name__, pandas.__name__]
            >>> if sys.version_info <= (3, 11):
            ...     session.sql(f"select {get_package_name_udf.name}()").to_df("col1").show()  # doctest: +SKIP
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
            >>> session.clear_imports()

        Args:
            ignore_packages: Set of package names that will be ignored.
            relax: If set to True, package versions will not be considered.

        Note:
            1. This method will add packages for all UDFs created later in the current
            session. If you only want to add packages for a specific UDF, you can use
            ``packages`` argument in :func:`functions.udf` or
            :meth:`session.udf.register() <snowflake.snowpark.udf.UDFRegistration.register>`.

            2. We recommend you to `setup the local environment with Anaconda <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#local-development-and-testing>`_,
            to ensure the consistent experience of a UDF between your local environment
            and the Snowflake server.
        """
        ignore_packages = {} if ignore_packages is None else ignore_packages

        packages = []
        for package in importlib.metadata.distributions():
            if package.metadata["Name"].lower() in ignore_packages:
                _logger.info(
                    f"{package.metadata['Name'].lower()} found in environment, ignoring..."
                )
                continue
            if package.metadata["Name"].lower() in DEFAULT_PACKAGES:
                _logger.info(
                    f"{package.metadata['Name'].lower()} is available by default, ignoring..."
                )
                continue
            version_text = (
                "==" + package.version if package.version and not relax else ""
            )
            packages.append(f"{package.metadata['Name'].lower()}{version_text}")

        self.add_packages(packages)

    @staticmethod
    def _parse_packages(
        packages: List[Union[str, ModuleType]]
    ) -> Dict[str, Tuple[str, bool, Requirement]]:
        package_dict = dict()
        for package in packages:
            if isinstance(package, ModuleType):
                package_name = MODULE_NAME_TO_PACKAGE_NAME_MAP.get(
                    package.__name__, package.__name__
                )
                package = f"{package_name}=={importlib.metadata.version(package_name)}"
                use_local_version = True
            else:
                package = package.strip().lower()
                if package.startswith("#"):
                    continue
                use_local_version = False
            package_req = Requirement(package)
            # get the standard package name if there is no underscore
            # underscores are discouraged in package names, but are still used in Anaconda channel
            # packaging.requirements.Requirement will convert all underscores to dashes
            # the regexp is to deal with case that "_" is in the package requirement as well as version restrictions
            # we only extract the valid package name from the string by following:
            # https://packaging.python.org/en/latest/specifications/name-normalization/
            # A valid name consists only of ASCII letters and numbers, period, underscore and hyphen.
            # It must start and end with a letter or number.
            # however, we don't validate the pkg name as this is done by packaging.requirements.Requirement
            # find the index of the first char which is not an valid package name character
            package_name = package_req.name.lower()
            if not use_local_version and "_" in package:
                reg_match = re.search(r"[^0-9a-zA-Z\-_.]", package)
                package_name = package[: reg_match.start()] if reg_match else package

            package_dict[package] = (package_name, use_local_version, package_req)
        return package_dict

    def _get_dependency_packages(
        self,
        package_dict: Dict[str, Tuple[str, bool, Requirement]],
        validate_package: bool,
        package_table: str,
        current_packages: Dict[str, str],
        statement_params: Optional[Dict[str, str]] = None,
        suppress_local_package_warnings: bool = False,
    ) -> List[Requirement]:
        # Keep track of any package errors
        errors = []

        valid_packages = self._get_available_versions_for_packages(
            package_names=[v[0] for v in package_dict.values()],
            package_table_name=package_table,
            validate_package=validate_package,
            statement_params=statement_params,
        )

        with self._lock:
            custom_package_usage_config = self._custom_package_usage_config.copy()

        unsupported_packages: List[str] = []
        for package, package_info in package_dict.items():
            package_name, use_local_version, package_req = package_info
            # The `packaging.requirements.Requirement` object exposes a `packaging.requirements.SpecifierSet` object
            # that handles a set of version specifiers.
            package_specifier = package_req.specifier if package_req.specifier else None

            if validate_package:
                if package_name not in valid_packages or (
                    package_specifier
                    and not any(
                        package_specifier.contains(v)
                        for v in valid_packages[package_name]
                    )
                ):
                    version_text = (
                        f"(version {package_specifier})"
                        if package_specifier is not None
                        else ""
                    )
                    if is_in_stored_procedure():  # pragma: no cover
                        errors.append(
                            RuntimeError(
                                f"Cannot add package {package_name}{version_text} because it is not available in Snowflake "
                                f"and it cannot be installed via pip as you are executing this code inside a stored "
                                f"procedure. You can find the directory of these packages and add it via "
                                f"Session.add_import. See details at "
                                f"https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-udfs.html#using-third-party-packages-from-anaconda-in-a-udf."
                            )
                        )
                        continue
                    if (
                        package_name not in valid_packages
                        and not self._is_anaconda_terms_acknowledged()
                    ):
                        errors.append(
                            RuntimeError(
                                f"Cannot add package {package_name}{version_text} because Anaconda terms must be accepted "
                                "by ORGADMIN to use Anaconda 3rd party packages. Please follow the instructions at "
                                "https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#using-third-party-packages-from-anaconda."
                            )
                        )
                        continue
                    if not custom_package_usage_config.get("enabled", False):
                        errors.append(
                            RuntimeError(
                                f"Cannot add package {package_req} because it is not available in Snowflake "
                                f"and Session.custom_package_usage_config['enabled'] is not set to True. To upload these packages, you can "
                                f"set it to True or find the directory of these packages and add it via Session.add_import. See details at "
                                f"https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-udfs.html#using-third-party-packages-from-anaconda-in-a-udf."
                            )
                        )
                        continue
                    unsupported_packages.append(package)
                    continue
                elif not use_local_version:
                    try:
                        package_client_version = importlib.metadata.version(
                            package_name
                        )

                        def is_valid_version(
                            package_name, package_client_version, valid_packages
                        ):
                            if package_name == "snowflake-snowpark-python":
                                # bugfix versions are ignored as they do not introduce new features
                                client_major, client_minor, _ = map(
                                    int, package_client_version.split(".")
                                )
                                valid_versions = {
                                    tuple(map(int, v.split(".")[:2]))
                                    for v in valid_packages.get(package_name, [])
                                }
                                return (client_major, client_minor) in valid_versions
                            return package_client_version in valid_packages.get(
                                package_name, []
                            )

                        if not is_valid_version(
                            package_name, package_client_version, valid_packages
                        ):
                            if not suppress_local_package_warnings:
                                _logger.warning(
                                    f"The version of package '{package_name}' in the local environment is "
                                    f"{package_client_version}, which does not fit the criteria for the "
                                    f"requirement '{package}'. Your UDF might not work when the package version "
                                    f"is different between the server and your local environment."
                                )
                    except importlib.metadata.PackageNotFoundError:
                        if not suppress_local_package_warnings:
                            _logger.warning(
                                f"Package '{package_name}' is not installed in the local environment. "
                                f"Your UDF might not work when the package is installed on the server "
                                f"but not on your local environment."
                            )
                    except Exception as ex:  # pragma: no cover
                        if not suppress_local_package_warnings:
                            _logger.warning(
                                "Failed to get the local distribution of package %s: %s",
                                package_name,
                                ex,
                            )

            if package_name in current_packages:
                if current_packages[package_name] != package:
                    errors.append(
                        ValueError(
                            f"Cannot add package '{package}' because {current_packages[package_name]} "
                            "is already added."
                        )
                    )
            else:
                current_packages[package_name] = package

        # Raise all exceptions at once so users know all issues in a single invocation.
        if len(errors) == 1:
            raise errors[0]
        elif len(errors) > 0:
            raise RuntimeError(errors)

        dependency_packages: List[Requirement] = []
        if len(unsupported_packages) != 0:
            _logger.warning(
                f"The following packages are not available in Snowflake: {unsupported_packages}."
            )
            if custom_package_usage_config.get(
                "cache_path", False
            ) and not custom_package_usage_config.get("force_cache", False):
                cache_path = custom_package_usage_config["cache_path"]
                try:
                    environment_signature = get_signature(unsupported_packages)
                    dependency_packages = self._load_unsupported_packages_from_stage(
                        environment_signature, custom_package_usage_config["cache_path"]
                    )
                    if dependency_packages is None:
                        _logger.warning(
                            f"Unable to load environments from remote path {cache_path}, creating a fresh "
                            f"environment instead."
                        )
                except Exception as e:
                    _logger.warning(
                        f"Unable to load environments from remote path {cache_path}, creating a fresh "
                        f"environment instead. Error: {e.__repr__()}"
                    )

            if not dependency_packages:
                dependency_packages = self._upload_unsupported_packages(
                    unsupported_packages,
                    package_table,
                    current_packages,
                    custom_package_usage_config,
                )

        return dependency_packages

    @staticmethod
    def _get_req_identifiers_list(
        modules: List[Union[str, ModuleType]], result_dict: Dict[str, str]
    ) -> List[str]:
        res = []
        for m in modules:
            if isinstance(m, str) and m not in result_dict:
                res.append(m)
            elif isinstance(m, ModuleType) and m.__name__ not in result_dict:
                res.append(f"{m.__name__}=={m.__version__}")

        return res

    def _resolve_packages(
        self,
        packages: List[Union[str, ModuleType]],
        existing_packages_dict: Optional[Dict[str, str]] = None,
        validate_package: bool = True,
        include_pandas: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        artifact_repository: Optional[str] = None,
        **kwargs,
    ) -> List[str]:
        """
        Given a list of packages to add, this method will
        1. Check if the packages are supported by Snowflake
        2. Check if the package version if provided is supported by Snowflake
        3. Check if the package is already added
        4. Update existing packages dictionary with the new packages (*this is required for python sp to work*)

        When auto package upload is enabled, this method will also try to upload the packages
        unavailable in Snowflake to the stage.

        This function will raise error if any of the above conditions are not met.

        Returns:
            List[str]: List of package specifiers
        """
        # Always include cloudpickle
        extra_modules = [cloudpickle]
        if include_pandas:
            extra_modules.append("pandas")

        # Extract package names, whether they are local, and their associated Requirement objects
        package_dict = self._parse_packages(packages)
        if (
            isinstance(self._conn, MockServerConnection)
            or artifact_repository is not None
        ):
            # in local testing we don't resolve the packages, we just return what is added
            errors = []
            with self._package_lock:
                if artifact_repository is None:
                    result_dict = self._packages
                else:
                    result_dict = self._artifact_repository_packages[
                        artifact_repository
                    ]

                for pkg_name, _, pkg_req in package_dict.values():
                    if (
                        pkg_name in result_dict
                        and str(pkg_req) != result_dict[pkg_name]
                    ):
                        errors.append(
                            ValueError(
                                f"Cannot add package '{str(pkg_req)}' because {result_dict[pkg_name]} "
                                "is already added."
                            )
                        )
                    else:
                        result_dict[pkg_name] = str(pkg_req)
                if len(errors) == 1:
                    raise errors[0]
                elif len(errors) > 0:
                    raise RuntimeError(errors)

            return list(result_dict.values()) + self._get_req_identifiers_list(
                extra_modules, result_dict
            )

        package_table = "information_schema.packages"
        if not self.get_current_database():
            package_table = f"snowflake.{package_table}"

        # result_dict is a mapping of package name -> package_spec, example
        # {'pyyaml': 'pyyaml==6.0',
        #  'networkx': 'networkx==3.1',
        #  'numpy': 'numpy',
        #  'scikit-learn': 'scikit-learn==1.2.2',
        #  'python-dateutil': 'python-dateutil==2.8.2'}
        # Add to packages dictionary. Make a copy of existing packages
        # dictionary to avoid modifying it during intermediate steps.
        with self._package_lock:
            result_dict = (
                existing_packages_dict if existing_packages_dict is not None else {}
            )

            # Retrieve list of dependencies that need to be added
            dependency_packages = self._get_dependency_packages(
                package_dict,
                validate_package,
                package_table,
                result_dict,
                statement_params=statement_params,
                suppress_local_package_warnings=kwargs.get(
                    "_suppress_local_package_warnings", False
                ),
            )

            # Add dependency packages
            for package in dependency_packages:
                name = package.name
                package_specs = [
                    (spec.operator, spec.version) for spec in package.specifier
                ]
                version = package_specs[0][1] if package_specs else None

                if name in result_dict:
                    if version is not None:
                        added_package_has_version = "==" in result_dict[name]
                        if added_package_has_version and result_dict[name] != str(
                            package
                        ):
                            raise ValueError(
                                f"Cannot add dependency package '{name}=={version}' "
                                f"because {result_dict[name]} is already added."
                            )
                        result_dict[name] = str(package)
                else:
                    result_dict[name] = str(package)

            return list(result_dict.values()) + self._get_req_identifiers_list(
                extra_modules, result_dict
            )

    def _upload_unsupported_packages(
        self,
        packages: List[str],
        package_table: str,
        package_dict: Dict[str, str],
        custom_package_usage_config: Dict[str, Any],
    ) -> List[Requirement]:
        """
        Uploads a list of Pypi packages, which are unavailable in Snowflake, to session stage.

        Args:
            packages (List[str]): List of package names requested by the user, that are not present in Snowflake.
            package_table (str): Name of Snowflake table containing information about Anaconda packages.
            package_dict (Dict[str, str]): A dictionary of package name -> package spec of packages that have
                been added explicitly so far using add_packages() or other such methods.

        Returns:
            List[packaging.requirements.Requirement]: List of package dependencies (present in Snowflake) that would need to be added
            to the package dictionary.

        Raises:
            RuntimeError: If any failure occurs in the workflow.

        """
        if not custom_package_usage_config.get("cache_path", False):
            _logger.warning(
                "If you are adding package(s) unavailable in Snowflake, it is highly recommended that you "
                "include the 'cache_path' configuration parameter in order to reduce latency."
            )

        try:
            # Setup a temporary directory and target folder where pip install will take place.
            tmpdir_handler = tempfile.TemporaryDirectory()
            tmpdir = tmpdir_handler.name
            target = os.path.join(tmpdir, "unsupported_packages")
            if not os.path.exists(target):
                os.makedirs(target)

            pip_install_packages_to_target_folder(packages, target)

            # Create Requirement objects for packages installed, mapped to list of package files and folders.
            downloaded_packages_dict = map_python_packages_to_files_and_folders(target)

            # Fetch valid Snowflake Anaconda versions for all packages installed by pip (if present).
            valid_downloaded_packages = self._get_available_versions_for_packages(
                package_names=[
                    package.name for package in downloaded_packages_dict.keys()
                ],
                package_table_name=package_table,
            )

            # Detect packages which use native code.
            native_packages = detect_native_dependencies(
                target, downloaded_packages_dict
            )

            # Figure out which dependencies are available in Snowflake, and which native dependencies can be dropped.
            (
                supported_dependencies,
                dropped_dependencies,
                new_dependencies,
            ) = identify_supported_packages(
                list(downloaded_packages_dict.keys()),
                valid_downloaded_packages,
                native_packages,
                package_dict,
            )

            if len(native_packages) > 0 and not custom_package_usage_config.get(
                "force_push", False
            ):
                raise ValueError(
                    "Your code depends on packages that contain native code, it may not work on Snowflake! Set Session.custom_package_usage_config['force_push'] to True "
                    "if you wish to proceed with using them anyway."
                )

            # Delete files
            delete_files_belonging_to_packages(
                supported_dependencies + dropped_dependencies,
                downloaded_packages_dict,
                target,
            )

            # Zip and add to stage
            environment_signature: str = get_signature(packages)
            zip_file = f"{IMPLICIT_ZIP_FILE_NAME}_{environment_signature}.zip"
            zip_path = os.path.join(tmpdir, zip_file)
            zip_directory_contents(target, zip_path)

            # Add packages to stage
            stage_name = self.get_session_stage()

            if custom_package_usage_config.get("cache_path", False):
                # Switch the stage used for storing zip file.
                stage_name = custom_package_usage_config["cache_path"]

                # Download metadata dictionary using the technique mentioned here: https://docs.snowflake.com/en/user-guide/querying-stage
                metadata_file = f"{ENVIRONMENT_METADATA_FILE_NAME}.txt"
                normalized_metadata_path = normalize_remote_file_or_dir(
                    f"{stage_name}/{metadata_file}"
                )
                metadata = {
                    row[0]: row[1] if row[1] else []
                    for row in (
                        self.sql(
                            f"SELECT t.$1 as signature, t.$2 as packages from {normalized_metadata_path} t",
                            _emit_ast=False,
                        )._internal_collect_with_tag()
                    )
                }
                _logger.info(f"METADATA: {metadata}")

                # Add a new enviroment to the metadata, avoid commas while storing list of dependencies because commas are treated as default delimiters.
                metadata[environment_signature] = "|".join(
                    [
                        str(requirement)
                        for requirement in supported_dependencies + new_dependencies
                    ]
                )
                metadata_local_path = os.path.join(tmpdir_handler.name, metadata_file)
                with open(metadata_local_path, "w") as file:
                    for key, value in metadata.items():
                        file.write(f"{key},{value}\n")

                # Upload metadata file to stage
                # Note that the metadata file is not compressed, only the zip files are.
                self._conn.upload_file(
                    path=normalize_local_file(metadata_local_path),
                    stage_location=normalize_remote_file_or_dir(stage_name),
                    compress_data=False,
                    overwrite=True,
                )

            self._conn.upload_file(
                path=normalize_local_file(zip_path),
                stage_location=normalize_remote_file_or_dir(stage_name),
                compress_data=True,
                overwrite=True,
            )

            # Add zipped file as an import
            stage_zip_path = f"{stage_name}/{zip_file}"
            self.add_import(
                stage_zip_path
                if stage_zip_path.startswith(STAGE_PREFIX)
                else f"{STAGE_PREFIX}{stage_zip_path}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Unable to auto-upload packages: {packages}, Error: {e} | NOTE: Alternatively, you can find the "
                f"directory of these packages and add it via Session.add_import. See details at "
                f"https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-udfs.html#using"
                f"-third-party-packages-from-anaconda-in-a-udf."
            )
        finally:
            if tmpdir_handler:
                tmpdir_handler.cleanup()

        return supported_dependencies + new_dependencies

    def _is_anaconda_terms_acknowledged(self) -> bool:
        return self._run_query("select system$are_anaconda_terms_acknowledged()")[0][0]

    def _load_unsupported_packages_from_stage(
        self, environment_signature: str, cache_path: str
    ) -> List[Requirement]:
        """
        Uses specified stage path to auto-import a group of unsupported packages, along with its dependencies. This
        saves time spent on pip install, native package detection and zip upload to stage.

        A cached environment on a stage consists of two files:
        1. A metadata dictionary, pickled using cloudpickle, which maps environment signatures to a list of
        Anaconda-supported dependency packages required for that environment.
        2. Zip files named '{PACKAGES_ZIP_NAME}_<environment_signature>.zip.gz which contain the unsupported packages.

        Note that a cached environment is only useful if you wish to use packages unsupported in Snowflake! Supported
        packages will not be cached (and need not be cached).

        Also note that any changes to your package list, which does not involve changing the versions or names
        of unsupported packages, will not necessarily affect your environment signature. Your environment signature
        corresponds only to packages currently not supported in the Anaconda channel.

        Args:
            environment_signature (str): Unique hash signature for a set of unsupported packages, computed by hashing
            a sorted tuple of unsupported package requirements (package versioning included).
        Returns:
            Optional[List[packaging.requirements.Requirement]]: A list of package dependencies for the set of unsupported packages requested.
        """
        # Ensure that metadata file exists
        metadata_file = f"{ENVIRONMENT_METADATA_FILE_NAME}.txt"
        files: Set[str] = self._list_files_in_stage(cache_path)
        if metadata_file not in files:
            _logger.info(
                f"Metadata file named {metadata_file} not found at stage path {cache_path}."
            )
            return None  # We need the metadata file to obtain dependency package names.

        # Ensure that zipped package exists
        required_file = f"{IMPLICIT_ZIP_FILE_NAME}_{environment_signature}.zip.gz"
        if required_file not in files:
            _logger.info(
                f"Matching environment file not found at stage path {cache_path}."
            )
            return None  # We need the zipped packages folder.

        # Download metadata
        metadata_file_path = f"{cache_path}/{metadata_file}"
        metadata = {
            row[0]: row[1].split("|") if row[1] else []
            for row in (
                self.sql(
                    f"SELECT t.$1 as signature, t.$2 as packages from {normalize_remote_file_or_dir(metadata_file_path)} t",
                    _emit_ast=False,
                )
                .filter(col("signature") == environment_signature, _emit_ast=False)
                ._internal_collect_with_tag()
            )
        }

        dependency_packages = [
            Requirement(package) for package in metadata[environment_signature]
        ]
        _logger.info(
            f"Loading dependency packages list - {metadata[environment_signature]}."
        )

        import_path = (
            f"{cache_path}/{IMPLICIT_ZIP_FILE_NAME}_{environment_signature}.zip.gz"
        )
        self.add_import(
            import_path
            if import_path.startswith(STAGE_PREFIX)
            else f"{STAGE_PREFIX}{import_path}"
        )
        return dependency_packages

    def _get_available_versions_for_packages(
        self,
        package_names: List[str],
        package_table_name: str,
        validate_package: bool = True,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> Dict[str, List[str]]:
        package_to_version_mapping = (
            {
                p[0]: json.loads(p[1])
                for p in self.table(package_table_name, _emit_ast=False)
                .filter(
                    (col("language", _emit_ast=False) == "python")
                    & (
                        col("package_name", _emit_ast=False).in_(
                            package_names, _emit_ast=False
                        )
                    ),
                    _emit_ast=False,
                )
                .group_by("package_name", _emit_ast=False)
                .agg(array_agg("version", _emit_ast=False), _emit_ast=False)
                ._internal_collect_with_tag(statement_params=statement_params)
            }
            if validate_package and len(package_names) > 0
            else None
        )
        return package_to_version_mapping

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

        Note:
            The setter calls ``ALTER SESSION SET QUERY_TAG = <tag>`` which may be restricted
            in some environments such as Owner's rights stored procedures. Refer to
            `Owner's rights stored procedures <https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-rights#owner-s-rights-stored-procedures>`_.
            for more details.

        """
        return self._query_tag

    @query_tag.setter
    def query_tag(self, tag: str) -> None:
        with self._lock:
            if tag:
                self._conn.run_query(f"alter session set query_tag = {str_to_sql(tag)}")
            else:
                self._conn.run_query("alter session unset query_tag")
            self._query_tag = tag

    def _get_remote_query_tag(self) -> str:
        """
        Fetches the current sessions query tag.
        """
        remote_tag_rows = self.sql(
            "SHOW PARAMETERS LIKE 'QUERY_TAG'", _emit_ast=False
        )._internal_collect_with_tag_no_telemetry()

        # Check if the result has the expected schema
        # https://docs.snowflake.com/en/sql-reference/sql/show-parameters#examples
        if len(remote_tag_rows) != 1 or not hasattr(remote_tag_rows[0], "value"):
            raise ValueError(
                "Snowflake server side query tag parameter has unexpected schema."
            )
        return remote_tag_rows[0].value

    def append_query_tag(self, tag: str, separator: str = ",") -> None:
        """
        Appends a tag to the current query tag. The input tag is appended to the current sessions query tag with the given separator.

        Args:
            tag: The tag to append to the current query tag.
            separator: The string used to separate values in the query tag.
        Note:
            Assigning a value via session.query_tag will remove any appended query tags.

        Example::
            >>> session.query_tag = "tag1"
            >>> session.append_query_tag("tag2")
            >>> print(session.query_tag)
            tag1,tag2
            >>> session.query_tag = "new_tag"
            >>> print(session.query_tag)
            new_tag

        Example::
            >>> session.query_tag = ""
            >>> session.append_query_tag("tag1")
            >>> print(session.query_tag)
            tag1

        Example::
            >>> session.query_tag = "tag1"
            >>> session.append_query_tag("tag2", separator="|")
            >>> print(session.query_tag)
            tag1|tag2

        Example::
            >>> session.sql("ALTER SESSION SET QUERY_TAG = 'tag1'").collect()
            [Row(status='Statement executed successfully.')]
            >>> session.append_query_tag("tag2")
            >>> print(session.query_tag)
            tag1,tag2
        """
        if tag:
            remote_tag = self._get_remote_query_tag()
            new_tag = separator.join(t for t in [remote_tag, tag] if t)
            self.query_tag = new_tag

    def update_query_tag(self, tag: dict) -> None:
        """
        Updates a query tag that is a json encoded string. Throws an exception if the sessions current query tag is not a valid json string.


        Args:
            tag: The dict that provides updates to the current query tag dict.
        Note:
            Assigning a value via session.query_tag will remove any current query tag state.

        Example::
            >>> session.query_tag = '{"key1": "value1"}'
            >>> session.update_query_tag({"key2": "value2"})
            >>> print(session.query_tag)
            {"key1": "value1", "key2": "value2"}

        Example::
            >>> session.sql("ALTER SESSION SET QUERY_TAG = '{\\"key1\\": \\"value1\\"}'").collect()
            [Row(status='Statement executed successfully.')]
            >>> session.update_query_tag({"key2": "value2"})
            >>> print(session.query_tag)
            {"key1": "value1", "key2": "value2"}

        Example::
            >>> session.query_tag = ""
            >>> session.update_query_tag({"key1": "value1"})
            >>> print(session.query_tag)
            {"key1": "value1"}
        """
        if tag:
            tag_str = self._get_remote_query_tag() or "{}"
            try:
                tag_dict = json.loads(tag_str)
                tag_dict.update(tag)
                self.query_tag = json.dumps(tag_dict)
            except json.JSONDecodeError:
                raise ValueError(
                    f"Expected query tag to be valid json. Current query tag: {tag_str}"
                )

    @publicapi
    def table(
        self,
        name: Union[str, Iterable[str]],
        is_temp_table_for_cleanup: bool = False,
        _emit_ast: bool = True,
        *,
        time_travel_mode: Optional[Literal["at", "before"]] = None,
        statement: Optional[str] = None,
        offset: Optional[int] = None,
        timestamp: Optional[Union[str, datetime.datetime]] = None,
        timestamp_type: Optional[Union[str, TimestampTimeZone]] = None,
        stream: Optional[str] = None,
    ) -> Table:
        """
        Returns a Table that points the specified table.

        Args:
            name: A string or list of strings that specify the table name or
                fully-qualified object identifier (database name, schema name, and table name).

            _emit_ast: Whether to emit AST statements.

            time_travel_mode: Time travel mode, either 'at' or 'before'.
                Exactly one of statement, offset, timestamp, or stream must be provided when time_travel_mode is set.
            statement: Query ID for time travel.
            offset: Negative integer representing seconds in the past for time travel.
            timestamp: Timestamp string or datetime object.
            timestamp_type: Type of timestamp interpretation ('NTZ', 'LTZ', or 'TZ').
            stream: Stream name for time travel.

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

            >>> df_at_time = session.table("my_table", time_travel_mode="at", timestamp="2023-01-01 12:00:00", timestamp_type="LTZ") # doctest: +SKIP
            >>> df_before = session.table("my_table", time_travel_mode="before", statement="01234567-abcd-1234-5678-123456789012") # doctest: +SKIP
            >>> df_offset = session.table("my_table", time_travel_mode="at", offset=-3600) # doctest: +SKIP
            >>> df_stream = session.table("my_table", time_travel_mode="at", stream="my_stream") # doctest: +SKIP

            # timestamp_type automatically set to "TZ" due to timezone info
            >>> import datetime, pytz  # doctest: +SKIP
            >>> tz_aware = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)  # doctest: +SKIP
            >>> table1 = session.read.table("my_table", time_travel_mode="at", timestamp=tz_aware)  # doctest: +SKIP

            # timestamp_type remains "NTZ" (user's explicit choice respected)
            >>> table2 = session.read.table("my_table", time_travel_mode="at", timestamp=tz_aware, timestamp_type="NTZ")  # doctest: +SKIP
        """
        if _emit_ast:
            stmt = self._ast_batch.bind()
            ast = with_src_position(stmt.expr.table, stmt)
            build_table_name(ast.name, name)
            ast.variant.session_table = True
            ast.is_temp_table_for_cleanup = is_temp_table_for_cleanup
            if time_travel_mode is not None:
                ast.time_travel_mode.value = time_travel_mode
            if statement is not None:
                ast.statement.value = statement
            if offset is not None:
                ast.offset.value = offset
            if timestamp is not None:
                build_expr_from_python_val(ast.timestamp, timestamp)
            if timestamp_type is not None:
                ast.timestamp_type.value = str(timestamp_type)
            if stream is not None:
                ast.stream.value = stream
        else:
            stmt = None

        if not isinstance(name, str) and isinstance(name, Iterable):
            name = ".".join(name)
        validate_object_name(name)
        t = Table(
            name,
            session=self,
            is_temp_table_for_cleanup=is_temp_table_for_cleanup,
            _ast_stmt=stmt,
            _emit_ast=_emit_ast,
            time_travel_mode=time_travel_mode,
            statement=statement,
            offset=offset,
            timestamp=timestamp,
            timestamp_type=timestamp_type,
            stream=stream,
        )
        # Replace API call origin for table
        set_api_call_source(t, "Session.table")
        return t

    @publicapi
    def table_function(
        self,
        func_name: Union[str, List[str], Callable[..., Any], TableFunctionCall],
        *func_arguments: ColumnOrName,
        _emit_ast: bool = True,
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
            - :meth:`Session.generator`, which is used to instantiate a :class:`DataFrame` using Generator table function.
                Generator functions are not supported with :meth:`Session.table_function`.
        """
        # AST.
        stmt = None
        if _emit_ast:
            add_intermediate_stmt(self._ast_batch, func_name)
            stmt = self._ast_batch.bind()
            ast = with_src_position(stmt.expr.session_table_function, stmt)
            build_indirect_table_fn_apply(
                ast.fn,
                func_name,
                *func_arguments,
                **func_named_arguments,
            )

        # TODO: Support table_function in MockServerConnection.
        if isinstance(self._conn, MockServerConnection) and not isinstance(
            self._conn, NopConnection
        ):
            if self._conn._suppress_not_implemented_error:

                # TODO: Snowpark does not allow empty dataframes (no schema, no data). Have a dummy schema here.
                ans = self.createDataFrame(
                    [],
                    schema=StructType([StructField("row", IntegerType())]),
                    _emit_ast=False,
                )
                if _emit_ast:
                    ans._ast_id = stmt.uid
                return ans
            else:
                # TODO: Implement table_function properly in local testing mode.
                # self._conn.log_not_supported_error(
                #     external_feature_name="Session.table_function",
                #     raise_error=NotImplementedError,
                # )
                pass

        func_expr = _create_table_function_expression(
            func_name, *func_arguments, **func_named_arguments
        )

        if self.sql_simplifier_enabled:
            d = DataFrame(
                self,
                self._analyzer.create_select_statement(
                    from_=SelectTableFunction(func_expr, analyzer=self._analyzer),
                    analyzer=self._analyzer,
                ),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        else:
            d = DataFrame(
                self,
                TableFunctionRelation(func_expr),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        set_api_call_source(d, "Session.table_function")
        return d

    @publicapi
    def generator(
        self,
        *columns: Column,
        rowcount: int = 0,
        timelimit: int = 0,
        _emit_ast: bool = True,
    ) -> DataFrame:
        """Creates a new DataFrame using the Generator table function.

        References: `Snowflake Generator function <https://docs.snowflake.com/en/sql-reference/functions/generator.html>`_.

        Args:
            columns: List of data generation function that work in tandem with generator table function.
            rowcount: Resulting table with contain ``rowcount`` rows if only this argument is specified. Defaults to 0.
            timelimit: The query runs for ``timelimit`` seconds, generating as many rows as possible within the time frame. The
                exact row count depends on the system speed. Defaults to 0.

        Usage Notes:
                - When both ``rowcount`` and ``timelimit`` are specified, then:

                    + if the ``rowcount`` is reached before the ``timelimit``, the resulting table with contain ``rowcount`` rows.
                    + if the ``timelimit`` is reached before the ``rowcount``, the table will contain as many rows generated within this time.
                - If both ``rowcount`` and ``timelimit`` are not specified, 0 rows will be generated.

        Example 1
            >>> from snowflake.snowpark.functions import seq1, seq8, uniform
            >>> df = session.generator(seq1(1).as_("sequence one"), uniform(1, 10, 2).as_("uniform"), rowcount=3)
            >>> df.show()
            ------------------------------
            |"sequence one"  |"UNIFORM"  |
            ------------------------------
            |0               |3          |
            |1               |3          |
            |2               |3          |
            ------------------------------
            <BLANKLINE>

        Example 2
            >>> df = session.generator(seq8(0), uniform(1, 10, 2), timelimit=1).order_by(seq8(0)).limit(3)
            >>> df.show()
            -----------------------------------
            |"SEQ8(0)"  |"UNIFORM(1, 10, 2)"  |
            -----------------------------------
            |0          |3                    |
            |1          |3                    |
            |2          |3                    |
            -----------------------------------
            <BLANKLINE>

        Returns:
            A new :class:`DataFrame` with data from calling the generator table function.
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._ast_batch.bind()
            ast = with_src_position(stmt.expr.generator, stmt)
            col_names, ast.columns.variadic = parse_positional_args_to_list_variadic(
                *columns
            )
            for col_name in col_names:
                ast.columns.args.append(col_name._ast)
            ast.row_count = rowcount
            ast.time_limit_seconds = timelimit

        # TODO: Support generator in MockServerConnection.
        from snowflake.snowpark.mock._connection import MockServerConnection

        if (
            isinstance(self._conn, MockServerConnection)
            and self._conn._suppress_not_implemented_error
        ):
            # TODO: Snowpark does not allow empty dataframes (no schema, no data). Have a dummy schema here.
            ans = self.createDataFrame(
                [],
                schema=StructType([StructField("row", IntegerType())]),
                _emit_ast=False,
            )
            if _emit_ast:
                ans._ast_id = stmt.uid
            return ans

        if isinstance(self._conn, MockServerConnection):
            self._conn.log_not_supported_error(
                external_feature_name="DataFrame.generator",
                raise_error=NotImplementedError,
            )
        if not columns:
            raise ValueError("Columns cannot be empty for generator table function")
        named_args = {}
        if rowcount != 0:
            named_args["rowcount"] = lit(rowcount)._expression
        if timelimit != 0:
            named_args["timelimit"] = lit(timelimit)._expression

        operators = [self._analyzer.analyze(col._expression, {}) for col in columns]
        func_expr = GeneratorTableFunction(args=named_args, operators=operators)

        if self.sql_simplifier_enabled:
            d = DataFrame(
                self,
                SelectStatement(
                    from_=SelectTableFunction(
                        func_expr=func_expr, analyzer=self._analyzer
                    ),
                    analyzer=self._analyzer,
                ),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        else:
            d = DataFrame(
                self,
                TableFunctionRelation(func_expr),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        set_api_call_source(d, "Session.generator")
        return d

    @publicapi
    def sql(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        _ast_stmt: proto.Bind = None,
        _emit_ast: bool = True,
    ) -> DataFrame:
        """
        Returns a new DataFrame representing the results of a SQL query.

        Note:
            You can use this method to execute a SQL query lazily,
            which means the SQL is not executed until methods like :func:`DataFrame.collect`
            or :func:`DataFrame.to_pandas` evaluate the DataFrame.
            For **immediate execution**, chain the call with the collect method: `session.sql(query).collect()`.

        Args:
            query: The SQL statement to execute.
            params: binding parameters. We only support qmark bind variables. For more information, check
                https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#qmark-or-numeric-binding
            _ast_stmt: when invoked internally, supplies the AST to use for the resulting dataframe.

        Example::

            >>> # create a dataframe from a SQL query
            >>> df = session.sql("select 1/2")
            >>> # execute the query
            >>> df.collect()
            [Row(1/2=Decimal('0.500000'))]

            >>> # Use params to bind variables
            >>> session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]).sort("column1").collect()
            [Row(COLUMN1=1, COLUMN2='a'), Row(COLUMN1=2, COLUMN2='b')]
        """
        # AST.
        stmt = None
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._ast_batch.bind()
                expr = with_src_position(stmt.expr.sql, stmt)
                expr.query = query
                if params is not None:
                    for p in params:
                        build_expr_from_python_val(expr.params.add(), p)
            else:
                stmt = _ast_stmt

        if (
            isinstance(self._conn, MockServerConnection)
            and not self._conn._suppress_not_implemented_error
        ):
            if self._conn.is_closed():
                raise SnowparkSessionException(
                    "Cannot perform this operation because the session has been closed.",
                    error_code="1404",
                )
            self._conn.log_not_supported_error(
                external_feature_name="Session.sql",
                raise_error=NotImplementedError,
            )

        if self.sql_simplifier_enabled:
            d = DataFrame(
                self,
                self._analyzer.create_select_statement(
                    from_=SelectSQL(query, analyzer=self._analyzer, params=params),
                    analyzer=self._analyzer,
                ),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        else:
            d = DataFrame(
                self,
                self._analyzer.plan_builder.query(
                    query, source_plan=None, params=params
                ),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        set_api_call_source(d, "Session.sql")
        return d

    @property
    def read(self) -> "DataFrameReader":
        """Returns a :class:`DataFrameReader` that you can use to read data from various
        supported sources (e.g. a file in a stage) as a DataFrame."""
        return DataFrameReader(self)

    @property
    def session_id(self) -> int:
        """Returns an integer that represents the session ID of this session."""
        return self._session_id

    @property
    def connection(self) -> "SnowflakeConnection":
        """Returns a :class:`SnowflakeConnection` object that allows you to access the connection between the current session
        and Snowflake server."""
        return self._conn._conn

    def _run_query(
        self,
        query: str,
        is_ddl_on_temp_object: bool = False,
        log_on_exception: bool = True,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> List[Any]:
        return self._conn.run_query(
            query,
            is_ddl_on_temp_object=is_ddl_on_temp_object,
            log_on_exception=log_on_exception,
            _statement_params=statement_params,
        )["data"]

    def _get_result_attributes(
        self, query: str, query_params: Optional[Sequence[Any]] = None
    ) -> List[Attribute]:
        return self._conn.get_result_attributes(query, query_params)

    def get_session_stage(
        self,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Returns the name of the temporary stage created by the Snowpark library
        for uploading and storing temporary artifacts for this session.
        These artifacts include libraries and packages for UDFs that you define
        in this session via :func:`add_import`.

        Note:
            This temporary stage is created once under the current database and schema of a Snowpark session.
            Therefore, if you switch database or schema during the session, the stage will not be re-created
            in the new database or schema, and still references the stage in the old database or schema.
        """
        with self._lock:
            if not self._session_stage:
                full_qualified_stage_name = self.get_fully_qualified_name_if_possible(
                    random_name_for_temp_object(TempObjectType.STAGE)
                )
                self._run_query(
                    f"create {get_temp_type_for_object(self._use_scoped_temp_objects, True)} \
                    stage if not exists {full_qualified_stage_name}",
                    is_ddl_on_temp_object=True,
                    statement_params=statement_params,
                )
                # set the value after running the query to ensure atomicity
                self._session_stage = full_qualified_stage_name
        return f"{STAGE_PREFIX}{self._session_stage}"

    @experimental(version="1.28.0")
    @publicapi
    def write_arrow(
        self,
        table: "pyarrow.Table",
        table_name: str,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        chunk_size: Optional[int] = WRITE_ARROW_CHUNK_SIZE,
        compression: str = "gzip",
        on_error: str = "abort_statement",
        use_vectorized_scanner: bool = False,
        parallel: int = 4,
        quote_identifiers: bool = True,
        auto_create_table: bool = False,
        overwrite: bool = False,
        table_type: Literal["", "temp", "temporary", "transient"] = "",
        use_logical_type: Optional[bool] = None,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> Table:
        """Writes a pyarrow.Table to a Snowflake table.

        The pyarrow Table is written out to temporary files, uploaded to a temporary stage, and then copied into the final location.

        This function requires the optional dependenct snowflake-snowpark-python[pandas] be installed.

        Returns a Snowpark Table that references the table referenced by table_name.

        Args:
            table: The pyarrow Table that is written.
            table_name: Table name where we want to insert into.
            database: Database schema and table is in, if not provided the default one will be used (Default value = None).
            schema: Schema table is in, if not provided the default one will be used (Default value = None).
            chunk_size: Number of rows to be inserted once. If not provided, all rows will be dumped once.
                Default to None normally, 100,000 if inside a stored procedure.
                Note: If auto_create_table or overwrite is set to True, the chunk size may affect schema
                inference because different chunks might contain varying data types,
                especially when None values are present. This can lead to inconsistencies in inferred types.
            compression: The compression used on the Parquet files, can only be gzip, or snappy. Gzip gives a
                better compression, while snappy is faster. Use whichever is more appropriate (Default value = 'gzip').
            on_error: Action to take when COPY INTO statements fail, default follows documentation at:
                https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
                (Default value = 'abort_statement').
            use_vectorized_scanner: Boolean that specifies whether to use a vectorized scanner for loading Parquet files. See details at
                `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_.
            parallel: Number of threads to be used when uploading chunks, default follows documentation at:
                https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters (Default value = 4).
            quote_identifiers: By default, identifiers, specifically database, schema, table and column names
                (from df.columns) will be quoted. If set to False, identifiers are passed on to Snowflake without quoting.
                I.e. identifiers will be coerced to uppercase by Snowflake.  (Default value = True)
            auto_create_table: When true, will automatically create a table with corresponding columns for each column in
                the passed in DataFrame. The table will not be created if it already exists
            table_type: The table type of to-be-created table. The supported table types include ``temp``/``temporary``
                and ``transient``. Empty means permanent table as per SQL convention.
            use_logical_type: Boolean that specifies whether to use Parquet logical types. With this file format option,
                Snowflake can interpret Parquet logical types during data loading. To enable Parquet logical types,
                set use_logical_type as True. Set to None to use Snowflakes default. For more information, see:
                https://docs.snowflake.com/en/sql-reference/sql/create-file-format
        """
        cursor = self._conn._conn.cursor()

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

        success, _, _, ci_output = write_arrow(
            cursor=cursor,
            table=table,
            table_name=table_name,
            database=database,
            schema=schema,
            chunk_size=chunk_size,
            compression=compression,
            on_error=on_error,
            use_vectorized_scanner=use_vectorized_scanner,
            parallel=parallel,
            quote_identifiers=quote_identifiers,
            auto_create_table=auto_create_table,
            overwrite=overwrite,
            table_type=table_type,
            use_logical_type=use_logical_type,
            use_scoped_temp_object=self._use_scoped_temp_objects
            and is_in_stored_procedure(),
        )

        if success:
            table = self.table(location, _emit_ast=False)
            set_api_call_source(table, "Session.write_arrow")
            return table
        else:
            raise SnowparkSessionException(
                f"Failed to write arrow table to Snowflake. COPY INTO output {ci_output}"
            )

    def _write_modin_pandas_helper(
        self,
        df: Union[
            "modin.pandas.DataFrame",  # noqa: F821
            "modin.pandas.Series",  # noqa: F821
        ],
        table_name: str,
        location: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        quote_identifiers: bool = True,
        auto_create_table: bool = False,
        overwrite: bool = False,
        index: bool = False,
        index_label: Optional["IndexLabel"] = None,  # noqa: F821
        table_type: Literal["", "temp", "temporary", "transient"] = "",
    ) -> None:
        """A helper method used by `write_pandas` to write Snowpark pandas DataFrame or Series to a table by using
        :func:`modin.pandas.DataFrame.to_snowflake <modin.pandas.DataFrame.to_snowflake>` or
        :func:`modin.pandas.Series.to_snowflake <modin.pandas.Series.to_snowflake>` internally

        Args:
            df: The Snowpark pandas DataFrame or Series we'd like to write back.
            table_name: Name of the table we want to insert into.
            location: the location of the table in string.
            database: Database that the table is in. If not provided, the default one will be used.
            schema: Schema that the table is in. If not provided, the default one will be used.
            quote_identifiers: By default, identifiers, specifically database, schema, table and column names
                (from :attr:`DataFrame.columns`) will be quoted. `to_snowflake` always quote column names so
                quote_identifiers = False is not supported here.
            auto_create_table: When true, automatically creates a table to store the passed in pandas DataFrame using the
                passed in ``database``, ``schema``, and ``table_name``. Note: there are usually multiple table configurations that
                would allow you to upload a particular pandas DataFrame successfully. If you don't like the auto created
                table, you can always create your own table before calling this function. For example, auto-created
                tables will store :class:`list`, :class:`tuple` and :class:`dict` as strings in a VARCHAR column.
            overwrite: Default value is ``False`` and the pandas DataFrame data is appended to the existing table. If set to ``True`` and if auto_create_table is also set to ``True``,
                then it drops the table. If set to ``True`` and if auto_create_table is set to ``False``,
                then it truncates the table. Note that in both cases (when overwrite is set to ``True``) it will replace the existing
                contents of the table with that of the passed in pandas DataFrame.
            index: default True
                If true, save DataFrame index columns as table columns.
            index_label:
                Column label for index column(s). If None is given (default) and index is True,
                then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
            table_type: The table type of table to be created. The supported values are: ``temp``, ``temporary``,
                and ``transient``. An empty string means to create a permanent table. Learn more about table types
                `here <https://docs.snowflake.com/en/user-guide/tables-temp-transient.html>`_.
        """
        if not quote_identifiers:
            raise NotImplementedError(
                "quote_identifiers = False is not supported by `to_snowflake`."
            )

        def quote_id(id: str) -> str:
            return '"' + id + '"'

        name = [table_name]
        if schema:
            name = [quote_id(schema)] + name
        if database:
            name = [quote_id(database)] + name

        if not auto_create_table and not self._table_exists(name):
            raise SnowparkClientException(
                f"Cannot write Snowpark pandas DataFrame or Series to table {location} because it does not exist. Use "
                f"auto_create_table = True to create table before writing a Snowpark pandas DataFrame or Series"
            )
        if_exists = "replace" if overwrite else "append"
        df.to_snowflake(
            name=name,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            table_type=table_type,
        )

    @publicapi
    def write_pandas(
        self,
        df: Union[
            "pandas.DataFrame",
            "modin.pandas.DataFrame",  # noqa: F821
            "modin.pandas.Series",  # noqa: F821
        ],
        table_name: str,
        *,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        chunk_size: Optional[int] = WRITE_PANDAS_CHUNK_SIZE,
        compression: str = "gzip",
        on_error: str = "abort_statement",
        parallel: int = 4,
        quote_identifiers: bool = True,
        auto_create_table: bool = False,
        create_temp_table: bool = False,
        overwrite: bool = False,
        table_type: Literal["", "temp", "temporary", "transient"] = "",
        use_logical_type: Optional[bool] = None,
        use_vectorized_scanner: bool = False,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> Table:
        """Writes a pandas DataFrame to a table in Snowflake and returns a
        Snowpark :class:`DataFrame` object referring to the table where the
        pandas DataFrame was written to.

        Args:
            df: The pandas DataFrame or Snowpark pandas DataFrame or Series we'd like to write back.
            table_name: Name of the table we want to insert into.
            database: Database that the table is in. If not provided, the default one will be used.
            schema: Schema that the table is in. If not provided, the default one will be used.
            chunk_size: Number of rows to be inserted once. If not provided, all rows will be dumped once.
                Default to None normally, 100,000 if inside a stored procedure.
                Note: If auto_create_table or overwrite is set to True, the chunk size may affect schema
                inference because different chunks might contain varying data types,
                especially when None values are present. This can lead to inconsistencies in inferred types.
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
            overwrite: Default value is ``False`` and the pandas DataFrame data is appended to the existing table. If set to ``True`` and if auto_create_table is also set to ``True``,
                then it drops the table. If set to ``True`` and if auto_create_table is set to ``False``,
                then it truncates the table. Note that in both cases (when overwrite is set to ``True``) it will replace the existing
                contents of the table with that of the passed in pandas DataFrame.
            table_type: The table type of table to be created. The supported values are: ``temp``, ``temporary``,
                and ``transient``. An empty string means to create a permanent table. Learn more about table types
                `here <https://docs.snowflake.com/en/user-guide/tables-temp-transient.html>`_.
            use_logical_type: Boolean that specifies whether to use Parquet logical types when reading the parquet files
                for the uploaded pandas dataframe. With this file format option, Snowflake can interpret Parquet logical
                types during data loading. To enable Parquet logical types, set use_logical_type as True. Set to None to
                use Snowflakes default. For more information, see:
                `file format options: <https://docs.snowflake.com/en/sql-reference/sql/create-file-format#type-parquet>`_.
            use_vectorized_scanner: Boolean that specifies whether to use a vectorized scanner for loading Parquet files. See details at
                `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_.

        Example::

            >>> import pandas as pd
            >>> pandas_df = pd.DataFrame([(1, "Steve"), (2, "Bob")], columns=["id", "name"])
            >>> snowpark_df = session.write_pandas(pandas_df, "write_pandas_table", auto_create_table=True, table_type="temp")
            >>> snowpark_df.sort('"id"').to_pandas()
               id   name
            0   1  Steve
            1   2    Bob

            >>> pandas_df2 = pd.DataFrame([(3, "John")], columns=["id", "name"])
            >>> snowpark_df2 = session.write_pandas(pandas_df2, "write_pandas_table", auto_create_table=False)
            >>> snowpark_df2.sort('"id"').to_pandas()
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
            1. Unless ``auto_create_table`` is ``True``, you must first create a table in
            Snowflake that the passed in pandas DataFrame can be written to. If
            your pandas DataFrame cannot be written to the specified table, an
            exception will be raised.

            2. If the dataframe is Snowpark pandas :class:`~modin.pandas.DataFrame`
            or :class:`~modin.pandas.Series`, it will call
            :func:`modin.pandas.DataFrame.to_snowflake <modin.pandas.DataFrame.to_snowflake>`
            or :func:`modin.pandas.Series.to_snowflake <modin.pandas.Series.to_snowflake>`
            internally to write a Snowpark pandas DataFrame into a Snowflake table.

            3. If the input pandas DataFrame has `datetime64[ns, tz]` columns and `auto_create_table` is set to `True`,
            they will be converted to `TIMESTAMP_LTZ` in the output Snowflake table by default.
            If `TIMESTAMP_TZ` is needed for those columns instead, please manually create the table before loading data.
        """

        if isinstance(self._conn, MockServerConnection):
            self._conn.log_not_supported_error(
                external_feature_name="Session.write_pandas",
                raise_error=NotImplementedError,
            )

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

        if hasattr(df, "columns") and len(df.columns) == 0:
            raise ProgrammingError(
                "The provided schema or inferred schema cannot be None or empty"
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

            if use_logical_type is not None:
                signature = inspect.signature(write_pandas)
                use_logical_type_supported = "use_logical_type" in signature.parameters
                if use_logical_type_supported:
                    kwargs["use_logical_type"] = use_logical_type
                else:
                    # raise warning to upgrade python connector
                    warnings.warn(
                        "use_logical_type will be ignored because current python "
                        "connector version does not support it. Please upgrade "
                        "snowflake-connector-python to 3.4.0 or above.",
                        stacklevel=1,
                    )

            modin_pandas, modin_is_imported = import_or_missing_modin_pandas()
            if modin_is_imported and isinstance(
                df, (modin_pandas.DataFrame, modin_pandas.Series)
            ):
                # use_logical_type should be ignored for Snowpark pandas
                kwargs.pop("use_logical_type", None)
                self._write_modin_pandas_helper(
                    df,
                    table_name,
                    location,
                    database=database,
                    schema=schema,
                    quote_identifiers=quote_identifiers,
                    auto_create_table=auto_create_table,
                    overwrite=overwrite,
                    table_type=table_type,
                    **kwargs,
                )
                success, ci_output = True, ""
            else:
                if isinstance(self._conn, MockServerConnection):
                    # TODO: Implement here write_pandas correctly.
                    success, ci_output = True, []
                else:
                    success, _, _, ci_output = write_pandas(
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
                        use_vectorized_scanner=use_vectorized_scanner,
                        **kwargs,
                    )
        except ProgrammingError as pe:
            if pe.msg.endswith("does not exist"):
                raise SnowparkClientExceptionMessages.DF_PANDAS_TABLE_DOES_NOT_EXIST_EXCEPTION(
                    location
                ) from pe
            else:
                raise pe

        if success:
            table = self.table(location, _emit_ast=False)
            set_api_call_source(table, "Session.write_pandas")

            # AST.
            if _emit_ast:
                # Create AST statement.
                stmt = self._ast_batch.bind()
                ast = with_src_position(stmt.expr.write_pandas, stmt)  # noqa: F841

                ast.auto_create_table = auto_create_table
                if chunk_size is not None and chunk_size != WRITE_PANDAS_CHUNK_SIZE:
                    ast.chunk_size.value = chunk_size
                ast.compression = compression
                ast.create_temp_table = create_temp_table
                if isinstance(df, pandas.DataFrame):
                    build_table_name(
                        ast.df.dataframe_data__pandas.v.temp_table, table.table_name
                    )
                else:
                    raise NotImplementedError(
                        f"Only pandas DataFrame supported, but not {type(df)}"
                    )
                if kwargs:
                    for k, v in kwargs.items():
                        t = ast.kwargs.add()
                        t._1 = k
                        build_expr_from_python_val(t._2, v)
                ast.on_error = on_error
                ast.overwrite = overwrite
                ast.parallel = parallel
                ast.quote_identifiers = quote_identifiers

                # Convert to [...] location.
                table_location = table_name
                if schema is not None:
                    table_location = [schema, table_location]
                if database is not None:
                    if schema is None:
                        # TODO: unify API with other APIs using [...] syntax. Default schema is PUBLIC
                        raise ValueError("Need to set schema when using database.")
                    table_location = [database] + table_location

                build_table_name(ast.table_name, table_location)
                ast.table_type = table_type

                table._ast_id = stmt.uid

            return table
        else:
            raise SnowparkClientExceptionMessages.DF_PANDAS_GENERAL_EXCEPTION(
                str(ci_output)
            )

    @publicapi
    def create_dataframe(
        self,
        data: Union[List, Tuple, "pandas.DataFrame", "pyarrow.Table"],
        schema: Optional[Union[StructType, Iterable[str], str]] = None,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> DataFrame:
        """Creates a new DataFrame containing the specified values from the local data.

        If creating a new DataFrame from a pandas Dataframe or a PyArrow Table, we will store
        the data in a temporary table and return a DataFrame pointing to that temporary
        table for you to then do further transformations on. This temporary table will be
        dropped at the end of your session. If you would like to save the pandas DataFrame or PyArrow Table,
        use the :meth:`write_pandas` or :meth:`write_arrow` method instead.
        Note: When ``data`` is a pandas DataFrame or pyarrow Table, schema inference may be affected by chunk size.
        You can control it by passing the ``chunk_size`` keyword argument. For details, see :meth:`write_pandas`
        or :meth:`write_arrow`, which are used internally by this function.

        Args:
            data: The local data for building a :class:`DataFrame`. ``data`` can only
                be a :class:`list`, :class:`tuple` or pandas DataFrame. Every element in
                ``data`` will constitute a row in the DataFrame.
            schema: A :class:`~snowflake.snowpark.types.StructType` containing names and
                data types of columns, or a list of column names, or ``None``.

                - When passing a **string**, it can be either an *explicit* struct
                  (e.g. ``"struct<a: int, b: string>"``) or an *implicit* struct
                  (e.g. ``"a: int, b: string"``). Internally, the string is parsed and
                  converted into a :class:`StructType` using Snowpark's type parsing.
                - When ``schema`` is a list of column names or ``None``, the schema of the
                  DataFrame will be inferred from the data across all rows.

                To improve performance, provide a schema. This avoids the need to infer data types
                with large data sets.
            **kwargs: Additional keyword arguments passed to :meth:`write_pandas` or :meth:`write_arrow`
                when ``data`` is a pandas DataFrame or pyarrow Table, respectively. These can include
                options such as chunk_size or compression.

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

            >>> # create a dataframe using an implicit struct schema string
            >>> session.create_dataframe([[10, 20], [30, 40]], schema="x: int, y: int").collect()
            [Row(X=10, Y=20), Row(X=30, Y=40)]

        Note:
            When `data` is a pandas DataFrame, `snowflake.connector.pandas_tools.write_pandas` is called, which
            requires permission to (1) CREATE STAGE (2) CREATE TABLE and (3) CREATE FILE FORMAT under the current
            database and schema.
        """
        if data is None:
            raise ValueError("data cannot be None.")

        # check the type of data
        if isinstance(data, Row):
            raise TypeError("create_dataframe() function does not accept a Row object.")

        if not isinstance(data, (list, tuple)) and (
            not installed_pandas
            or (
                installed_pandas
                and not isinstance(data, (pandas.DataFrame, pyarrow.Table))
            )
        ):
            raise TypeError(
                "create_dataframe() function only accepts data as a list, tuple or a pandas DataFrame."
            )

        # If data is a pandas dataframe, the schema will be detected from the dataframe itself and schema ignored.
        # Warn user to acknowledge this.
        if (
            installed_pandas
            and isinstance(data, (pandas.DataFrame, pyarrow.Table))
            and schema is not None
        ):
            warnings.warn(
                "data is a pandas DataFrame, parameter schema is ignored. To silence this warning pass schema=None.",
                UserWarning,
                stacklevel=2,
            )

        # check to see if it is a pandas DataFrame and if so, write that to a temp
        # table and return as a DataFrame
        origin_data = data
        if installed_pandas and isinstance(data, (pandas.DataFrame, pyarrow.Table)):
            temp_table_name = escape_quotes(
                random_name_for_temp_object(TempObjectType.TABLE)
            )
            if isinstance(self._conn, MockServerConnection):
                schema, data = _extract_schema_and_data_from_pandas_df(data)
                # we do not return here as live connection and keep using the data frame logic and compose table
            else:
                sf_database = self._conn._get_current_parameter(
                    "database", quoted=False
                )
                sf_schema = self._conn._get_current_parameter("schema", quoted=False)

                if isinstance(data, pyarrow.Table):
                    table = self.write_arrow(
                        data,
                        temp_table_name,
                        database=sf_database,
                        schema=sf_schema,
                        quote_identifiers=True,
                        auto_create_table=True,
                        table_type="temporary",
                        use_logical_type=self._use_logical_type_for_create_df,
                        _emit_ast=False,
                        **kwargs,
                    )
                    set_api_call_source(table, "Session.create_dataframe[arrow]")
                else:
                    table = self.write_pandas(
                        data,
                        temp_table_name,
                        database=sf_database,
                        schema=sf_schema,
                        quote_identifiers=True,
                        auto_create_table=True,
                        table_type="temporary",
                        use_logical_type=self._use_logical_type_for_create_df,
                        _emit_ast=False,
                        **kwargs,
                    )
                    set_api_call_source(table, "Session.create_dataframe[pandas]")

                if _emit_ast:
                    stmt = self._ast_batch.bind()
                    ast = with_src_position(stmt.expr.create_dataframe, stmt)
                    # Save temp table and schema of it in AST (dataframe).
                    build_table_name(
                        ast.data.dataframe_data__pandas.v.temp_table, temp_table_name
                    )
                    build_proto_from_struct_type(
                        table.schema, ast.schema.dataframe_schema__struct.v
                    )
                    table._ast_id = stmt.uid

                return table

        # infer the schema based on the data
        names = None
        schema_query = None
        if isinstance(schema, str):
            schema = type_string_to_type_object(schema)
            if not isinstance(schema, StructType):
                raise ValueError(
                    f"Invalid schema string: {schema}. "
                    f"You should provide a valid schema string representing a struct type."
                )
        create_temp_table = False
        if isinstance(schema, StructType):
            new_schema = schema
            create_temp_table = True
        else:
            if not data:
                raise ValueError("Cannot infer schema from empty data")
            if isinstance(schema, Iterable):
                names = list(schema)
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
                        ArrayType,
                        DateType,
                        DayTimeIntervalType,
                        GeographyType,
                        GeometryType,
                        MapType,
                        StructType,
                        TimeType,
                        TimestampType,
                        VariantType,
                        VectorType,
                        YearMonthIntervalType,
                        FileType,
                    ),
                )
                else field.datatype
            )
            attrs.append(Attribute(quoted_name, sf_type, field.nullable))
            data_types.append(field.datatype)

        if create_temp_table:
            # SELECT query has an undefined behavior for nullability, so if the schema requires non-nullable column and
            # all columns are primitive type columns, we use a temp table to lock in the nullabilities.
            # TODO(SNOW-1015527): Support non-primitive type
            if (
                not isinstance(self._conn, MockServerConnection)
                and any([field.nullable is False for field in schema.fields])
                and all([field.datatype.is_primitive() for field in schema.fields])
            ):
                temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
                schema_string = analyzer_utils.attribute_to_schema_string(attrs)
                try:
                    self._run_query(
                        f"CREATE SCOPED TEMP TABLE {temp_table_name} ({schema_string})"
                    )
                    schema_query = f"SELECT * FROM {self.get_fully_qualified_name_if_possible(temp_table_name)}"
                except ProgrammingError as e:
                    _logger.debug(
                        f"Cannot create temp table for specified non-nullable schema, fall back to using schema "
                        f"string from select query. Exception: {str(e)}"
                    )

        # convert all variant/time/geospatial/array/map data to string
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
                elif isinstance(data_type, YearMonthIntervalType):
                    converted_row.append(value)
                elif isinstance(data_type, DayTimeIntervalType):
                    converted_row.append(value)
                elif isinstance(data_type, _AtomicType):  # consider inheritance
                    converted_row.append(value)
                elif isinstance(value, (list, tuple, array)) and isinstance(
                    data_type, ArrayType
                ):
                    converted_row.append(json.dumps(value, cls=PythonObjJSONEncoder))
                elif isinstance(value, dict) and isinstance(
                    data_type, (MapType, StructType)
                ):
                    converted_row.append(json.dumps(value, cls=PythonObjJSONEncoder))
                elif (
                    isinstance(value, Row)
                    and isinstance(data_type, StructType)
                    and context._should_use_structured_type_semantics()
                ):
                    converted_row.append(
                        json.dumps(value.as_dict(), cls=PythonObjJSONEncoder)
                    )
                elif isinstance(data_type, VariantType):
                    converted_row.append(json.dumps(value, cls=PythonObjJSONEncoder))
                elif isinstance(data_type, GeographyType):
                    converted_row.append(value)
                elif isinstance(data_type, GeometryType):
                    converted_row.append(value)
                elif isinstance(data_type, FileType):
                    converted_row.append(value)
                elif isinstance(data_type, VectorType):
                    converted_row.append(json.dumps(value, cls=PythonObjJSONEncoder))
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
                tz = field.datatype.tz
                if tz == TimestampTimeZone.NTZ:
                    to_timestamp_func = to_timestamp_ntz
                elif tz == TimestampTimeZone.LTZ:
                    to_timestamp_func = to_timestamp_ltz
                elif tz == TimestampTimeZone.TZ:
                    to_timestamp_func = to_timestamp_tz
                else:
                    to_timestamp_func = to_timestamp
                project_columns.append(to_timestamp_func(column(name)).as_(name))
            elif isinstance(field.datatype, TimeType):
                project_columns.append(to_time(column(name)).as_(name))
            elif isinstance(field.datatype, DateType):
                project_columns.append(to_date(column(name)).as_(name))
            elif isinstance(field.datatype, VariantType):
                project_columns.append(to_variant(parse_json(column(name))).as_(name))
            elif isinstance(field.datatype, GeographyType):
                project_columns.append(to_geography(column(name)).as_(name))
            elif isinstance(field.datatype, GeometryType):
                project_columns.append(to_geometry(column(name)).as_(name))
            elif isinstance(field.datatype, (ArrayType, MapType, StructType)):
                project_columns.append(
                    parse_json(column(name)).cast(field.datatype).as_(name)
                )
            elif isinstance(field.datatype, VectorType):
                project_columns.append(
                    parse_json(column(name)).cast(field.datatype).as_(name)
                )
            elif isinstance(field.datatype, FileType):
                project_columns.append(to_file(column(name)).as_(name))
            elif isinstance(field.datatype, YearMonthIntervalType):
                project_columns.append(column(name).cast(field.datatype).as_(name))
            elif isinstance(field.datatype, DayTimeIntervalType):
                project_columns.append(column(name).cast(field.datatype).as_(name))
            else:
                project_columns.append(column(name))

        # Create AST statement.
        stmt = self._ast_batch.bind() if _emit_ast else None

        df = (
            DataFrame(
                self,
                self._analyzer.create_select_statement(
                    from_=self._analyzer.create_select_snowflake_plan(
                        SnowflakeValues(attrs, converted, schema_query=schema_query),
                        analyzer=self._analyzer,
                    ),
                    analyzer=self._analyzer,
                ),
                _emit_ast=False,
            ).select(project_columns, _emit_ast=False)
            if self.sql_simplifier_enabled
            else DataFrame(
                self,
                SnowflakeValues(attrs, converted, schema_query=schema_query),
                _emit_ast=False,
            ).select(project_columns, _emit_ast=False)
        )
        set_api_call_source(df, "Session.create_dataframe[values]")

        if _emit_ast:
            df._ast_id = stmt.uid

        if (
            installed_pandas
            and isinstance(origin_data, pandas.DataFrame)
            and isinstance(self._conn, MockServerConnection)
        ):
            # MockServerConnection internally creates a table, and returns Table object (which inherits from Dataframe).
            table = _convert_dataframe_to_table(df, temp_table_name, self)

            # AST.
            if _emit_ast:
                ast = with_src_position(stmt.expr.create_dataframe, stmt)

                # Save temp table and schema of it in AST (dataframe).
                build_table_name(
                    ast.data.dataframe_data__pandas.v.temp_table, temp_table_name
                )
                build_proto_from_struct_type(
                    table.schema, ast.schema.dataframe_schema__struct.v
                )

                table._ast_id = stmt.uid

            return table

        # AST.
        if _emit_ast:
            ast = with_src_position(stmt.expr.create_dataframe, stmt)

            if isinstance(origin_data, tuple):
                for row in origin_data:
                    build_expr_from_python_val(
                        ast.data.dataframe_data__tuple.vs.add(), row
                    )
            elif isinstance(origin_data, list):
                for row in origin_data:
                    build_expr_from_python_val(
                        ast.data.dataframe_data__list.vs.add(), row
                    )
            # Note: pandas.DataFrame handled above.
            else:
                raise TypeError(
                    f"Unsupported type {type(origin_data)} in create_dataframe."
                )

            if schema is not None:
                if isinstance(schema, list):
                    for name in schema:
                        ast.schema.dataframe_schema__list.vs.append(name)
                elif isinstance(schema, StructType):
                    build_proto_from_struct_type(
                        schema, ast.schema.dataframe_schema__struct.v
                    )

            df._ast_id = stmt.uid

        return df

    @publicapi
    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        _emit_ast: bool = True,
    ) -> DataFrame:
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

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._ast_batch.bind()
            ast = with_src_position(stmt.expr.range, stmt)
            ast.start = start
            if end:
                ast.end.value = end
            ast.step.value = step

        if self.sql_simplifier_enabled:
            df = DataFrame(
                self,
                self._analyzer.create_select_statement(
                    from_=self._analyzer.create_select_snowflake_plan(
                        range_plan, analyzer=self._analyzer
                    ),
                    analyzer=self._analyzer,
                ),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        else:
            df = DataFrame(self, range_plan, _ast_stmt=stmt, _emit_ast=_emit_ast)
        set_api_call_source(df, "Session.range")
        return df

    def create_async_job(self, query_id: str) -> AsyncJob:
        """
        Creates an :class:`AsyncJob` from a query ID.

        See also:
            :class:`AsyncJob`
        """
        if (
            is_in_stored_procedure()
            and not self._conn._get_client_side_session_parameter(
                "ENABLE_ASYNC_QUERY_IN_PYTHON_STORED_PROCS", False
            )
        ):  # pragma: no cover
            raise NotImplementedError(
                "Async query is not supported in stored procedure yet"
            )
        if isinstance(self._conn, MockServerConnection):
            self._conn.log_not_supported_error(
                external_feature_name="Session.create_async_job",
                raise_error=NotImplementedError,
            )
        return AsyncJob(query_id, None, self)

    def get_current_account(self) -> Optional[str]:
        """
        Returns the name of the current account for the Python connector session attached
        to this session.
        """
        return self._conn._get_current_parameter("account")

    def get_current_user(self) -> Optional[str]:
        """
        Returns the name of the user in the connection to Snowflake attached
        to this session.
        """
        return self._conn._get_current_parameter("user")

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
        # NOTE: For snowpark development, consider using get_fully_qualified_name_if_possible instead. Given this is
        # a public API and could be widely used, we won't deprecate it, but the internal usages are all moved to
        # get_fully_qualified_name_if_possible.
        return self.get_fully_qualified_name_if_possible("")[:-1]

    def get_fully_qualified_name_if_possible(self, name: str) -> str:
        """
        Returns the fully qualified object name if current database/schema exists, otherwise returns the object name
        """
        with self._lock:
            database = self.get_current_database()
            schema = self.get_current_schema()
        if database and schema:
            return f"{database}.{schema}.{name}"

        # In stored procedure, there are scenarios like bundle where we allow empty current schema
        if not is_in_stored_procedure():
            missing_item = "DATABASE" if not database else "SCHEMA"
            raise SnowparkClientExceptionMessages.SERVER_CANNOT_FIND_CURRENT_DB_OR_SCHEMA(
                missing_item, missing_item, missing_item
            )
        return name

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
            query = f"use {object_type} {object_name}"
            if isinstance(self._conn, MockServerConnection):
                use_ddl_pattern = (
                    r"^\s*use\s+(warehouse|database|schema|role)\s+(.+)\s*$"
                )

                if match := re.match(use_ddl_pattern, query):
                    # if the query is "use xxx", then the object name is already verified by the upper stream
                    # we do not validate here
                    object_type = match.group(1)
                    object_name = match.group(2)
                    mock_conn_lock = self._conn.get_lock()
                    with mock_conn_lock:
                        setattr(self._conn, f"_active_{object_type}", object_name)
                else:
                    self._run_query(query)
            else:
                self._run_query(query)
        else:
            raise ValueError(f"'{object_type}' must not be empty or None.")

    @property
    def telemetry_enabled(self) -> bool:
        """Controls whether or not the Snowpark client sends usage telemetry to Snowflake.
        This typically includes information like the API calls invoked, libraries used in conjunction with Snowpark,
        and information that will let us better diagnose and fix client side errors.

        The default value is ``True``.

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
        return self._conn._telemetry_client._enabled

    @telemetry_enabled.setter
    def telemetry_enabled(self, value):
        # Set both in-band and out-of-band telemetry to True/False
        if value:
            self._conn._telemetry_client._enabled = True
            if is_in_stored_procedure() and not self._internal_telemetry_enabled:
                _logger.debug(
                    "Client side parameter ENABLE_SNOWPARK_FIRST_PARTY_TELEMETRY is set to False, telemetry could not be enabled"
                )
                self._conn._telemetry_client._enabled = False

        else:
            self._conn._telemetry_client._enabled = False

    @property
    def file(self) -> FileOperation:
        """
        Returns a :class:`FileOperation` object that you can use to perform file operations on stages.
        See details of how to use this object in :class:`FileOperation`.
        """
        return self._file

    @property
    def lineage(self) -> Lineage:
        """
        Returns a :class:`Lineage` object that you can use to explore lineage of snowflake entities.
        See details of how to use this object in :class:`Lineage`.
        """
        return self._lineage

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
        # TODO: Test udtf support properly.
        return self._udtf_registration

    @property
    def udaf(self) -> UDAFRegistration:
        """
        Returns a :class:`udaf.UDAFRegistration` object that you can use to register UDAFs.
        See details of how to use this object in :class:`udaf.UDAFRegistration`.
        """
        return self._udaf_registration

    @property
    def sproc(self) -> StoredProcedureRegistration:
        """
        Returns a :class:`stored_procedure.StoredProcedureRegistration` object that you can use to register stored procedures.
        See details of how to use this object in :class:`stored_procedure.StoredProcedureRegistration`.
        """
        return self._sp_registration

    @property
    def stored_procedure_profiler(self) -> StoredProcedureProfiler:
        """
        Returns a :class:`stored_procedure_profiler.StoredProcedureProfiler` object that you can use to profile stored procedures.
        See details of how to use this object in :class:`stored_procedure_profiler.StoredProcedureProfiler`.
        """
        return self._sp_profiler

    @property
    def dataframe_profiler(self) -> DataframeProfiler:
        """
        Returns a :class:`dataframe_profiler.DataframeProfiler` object that you can use to profile dataframe operations.
        See details of how to use this object in :class:`dataframe_profiler.DataframeProfiler`.
        """
        return self._dataframe_profiler

    def _infer_is_return_table(
        self, sproc_name: str, *args: Any, log_on_exception: bool = False
    ) -> bool:
        if sproc_name.strip().upper().startswith("SYSTEM$"):
            # Built-in stored procedures do not have schema and cannot be described
            # Currently all SYSTEM$ stored procedures are scalar so return false.
            return False
        func_signature = ""
        try:
            arg_types = []
            for arg in args:
                if isinstance(arg, Column):
                    expr = arg._expression
                    if isinstance(expr, Cast):
                        arg_types.append(convert_sp_to_sf_type(expr.to))
                    else:
                        arg_types.append(convert_sp_to_sf_type(expr.datatype))
                else:
                    arg_types.append(convert_sp_to_sf_type(infer_type(arg)))
            func_signature = f"{sproc_name.upper()}({', '.join(arg_types)})"

            # describe procedure returns two column table with columns - property and value
            # the second row in the sproc_desc is property=returns and value=<return type of procedure>
            # when no procedure of the signature is found, SQL exception is raised
            sproc_desc = self._run_query(
                f"describe procedure {func_signature}",
                log_on_exception=log_on_exception,
            )
            return_type = sproc_desc[1][1]
            return return_type.upper().startswith("TABLE")
        except Exception as exc:
            _logger.info(
                f"Could not describe procedure {func_signature} due to exception {exc}"
            )
        return False

    @publicapi
    def call(
        self,
        sproc_name: str,
        *args: Any,
        statement_params: Optional[Dict[str, Any]] = None,
        block: bool = True,
        log_on_exception: bool = False,
        return_dataframe: Optional[bool] = None,
        _emit_ast: bool = True,
    ) -> Union[Any, AsyncJob]:
        """Calls a stored procedure by name.

        Args:
            sproc_name: The name of stored procedure in Snowflake.
            args: Arguments should be basic Python types.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: Whether to block until the result is available. When it is ``False``, this function
                executes the stored procedure asynchronously and returns an :class:`AsyncJob`.
            log_on_exception: Log warnings if they arise when trying to determine if the stored procedure
                as a table return type.
            return_dataframe: When set to True, the return value of this function is a DataFrame object.
                This is useful when the given stored procedure's return type is a table.

        Returns:
            When block=True: The stored procedure result (scalar value or DataFrame)
            When block=False: An AsyncJob object for retrieving results later

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

        Example::

            >>> from snowflake.snowpark.dataframe import DataFrame
            >>>
            >>> @sproc(name="my_table_sp", replace=True)
            ... def my_table(session: snowflake.snowpark.Session, x: int, y: int, col1: str, col2: str) -> DataFrame:
            ...     return session.sql(f"select {x} as {col1}, {y} as {col2}")
            >>> session.call("my_table_sp", 1, 2, "a", "b").show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            -------------
            <BLANKLINE>
        """
        return self._call(
            sproc_name,
            *args,
            statement_params=statement_params,
            block=block,
            log_on_exception=log_on_exception,
            is_return_table=return_dataframe,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def call_nowait(
        self,
        sproc_name: str,
        *args: Any,
        statement_params: Optional[Dict[str, Any]] = None,
        log_on_exception: bool = False,
        return_dataframe: Optional[bool] = None,
        _emit_ast: bool = True,
    ) -> AsyncJob:
        """Calls a stored procedure by name asynchronously and returns an AsyncJob.
        It is equivalent to ``call(block=False)``.

        Args:
            sproc_name: The name of stored procedure in Snowflake.
            args: Arguments should be basic Python types.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            log_on_exception: Log warnings if they arise when trying to determine if the stored procedure
                as a table return type.
            return_dataframe: When set to True, the return value of this function is a DataFrame object.
                This is useful when the given stored procedure's return type is a table.

        Returns:
            An AsyncJob object that can be used to retrieve results or check status.

        Example::

            >>> import snowflake.snowpark
            >>> from snowflake.snowpark.functions import sproc
            >>> import time
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>>
            >>> @sproc(name="simple_add_sp", replace=True)
            ... def simple_add(session: snowflake.snowpark.Session, a: int, b: int) -> int:
            ...     return a + b
            >>> async_job = session.call_nowait("simple_add_sp", 1, 2)
            >>> while not async_job.is_done():
            ...     time.sleep(1.0)
            >>> async_job.is_done()
            True
            >>> async_job.is_failed()
            False
            >>> async_job.status()
            'SUCCESS'
            >>> result = async_job.result()
            >>> print(result)
            3
        """
        return self.call(
            sproc_name,
            *args,
            statement_params=statement_params,
            block=False,
            log_on_exception=log_on_exception,
            return_dataframe=return_dataframe,
            _emit_ast=_emit_ast,
        )

    def _call(
        self,
        sproc_name: str,
        *args: Any,
        statement_params: Optional[Dict[str, Any]] = None,
        is_return_table: Optional[bool] = None,
        log_on_exception: bool = False,
        _emit_ast: bool = True,
        block: bool = True,
    ) -> Union[Any, AsyncJob]:
        """Private implementation of session.call

        Args:
            sproc_name: The name of stored procedure in Snowflake.
            args: Arguments should be basic Python types.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            is_return_table: When set to a non-null value, it signifies whether the return type of sproc_name
                is a table return type. This skips infer check and returns a dataframe with appropriate sql call.
        """

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._ast_batch.bind()
            expr = with_src_position(stmt.expr.apply_expr, stmt)
            expr.fn.stored_procedure.name.name.name_flat.name = sproc_name
            for arg in args:
                build_expr_from_python_val(expr.pos_args.add(), arg)
            if statement_params is not None:
                for k in statement_params:
                    entry = expr.named_args.add()
                    entry._1 = k
                    build_expr_from_python_val(entry._2, statement_params[k])
            expr.fn.stored_procedure.log_on_exception.value = log_on_exception
            self._ast_batch.eval(stmt)

        if isinstance(self._sp_registration, MockStoredProcedureRegistration):
            return self._sp_registration.call(
                sproc_name,
                *args,
                session=self,
                statement_params=statement_params,
                _emit_ast=False,
            )

        validate_object_name(sproc_name)
        query = generate_call_python_sp_sql(self, sproc_name, *args)

        if is_return_table is None:
            is_return_table = self._infer_is_return_table(
                sproc_name, *args, log_on_exception=log_on_exception
            )

        return self._execute_sproc_internal(
            query=query,
            is_return_table=is_return_table,
            block=block,
            statement_params=statement_params,
            ast_stmt=stmt,
            log_on_exception=log_on_exception,
        )

    @deprecated(
        version="0.7.0",
        extra_warning_text="Use `Session.table_function()` instead.",
        extra_doc_string="Use :meth:`table_function` instead.",
    )
    @publicapi
    def flatten(
        self,
        input: ColumnOrName,
        path: Optional[str] = None,
        outer: bool = False,
        recursive: bool = False,
        mode: str = "BOTH",
        _emit_ast: bool = True,
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

        check_flatten_mode(mode)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._ast_batch.bind()
            expr = with_src_position(stmt.expr.flatten, stmt)
            build_expr_from_python_val(expr.input, input)
            if path is not None:
                expr.path.value = path
            expr.outer = outer
            expr.recursive = recursive
            if mode.upper() == "OBJECT":
                expr.mode.flatten_mode_object = True
            elif mode.upper() == "ARRAY":
                expr.mode.flatten_mode_array = True
            else:
                expr.mode.flatten_mode_both = True

        if isinstance(self._conn, MockServerConnection):
            if self._conn._suppress_not_implemented_error:
                return None
            else:
                self._conn.log_not_supported_error(
                    external_feature_name="Session.flatten",
                    raise_error=NotImplementedError,
                )
        if isinstance(input, str):
            input = col(input)
        df = DataFrame(
            self,
            TableFunctionRelation(
                FlattenFunction(input._expression, path, outer, recursive, mode)
            ),
            _ast_stmt=stmt,
            _emit_ast=_emit_ast,
        )
        set_api_call_source(df, "Session.flatten")
        return df

    def query_history(
        self,
        include_describe: bool = False,
        include_thread_id: bool = False,
        include_error: bool = False,
        include_dataframe_profiling: bool = False,
    ) -> QueryHistory:
        """Create an instance of :class:`QueryHistory` as a context manager to record queries that are pushed down to the Snowflake database.

        Args:
            include_describe: Include query notifications for describe queries
            include_thread_id: Include thread id where queries are called
            include_error: record queries that have error during execution

        >>> with session.query_history(True) as query_history:
        ...     df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        ...     df = df.filter(df.a == 1)
        ...     res = df.collect()
        >>> assert len(query_history.queries) == 2
        >>> assert query_history.queries[0].is_describe
        >>> assert not query_history.queries[1].is_describe
        """
        query_listener = QueryHistory(
            self,
            include_describe,
            include_thread_id,
            include_error,
            include_dataframe_profiling,
        )
        self._conn.add_query_listener(query_listener)
        return query_listener

    def ast_listener(self, include_error: bool = False) -> AstListener:
        """
        Creates an instance of :class:`AstListener` as a context manager to capture ast batches flushed.
        Returns: AstListener instance holding base64 encoded batches.

        Args:
            include_error: Include ast objects that may have failed previous execution.
        """
        al = AstListener(self, include_error)
        self._conn.add_query_listener(al)
        return al

    def _table_exists(self, raw_table_name: Iterable[str]):
        """ """
        # implementation based upon: https://docs.snowflake.com/en/sql-reference/name-resolution.html
        qualified_table_name = list(raw_table_name)
        if isinstance(self._conn, MockServerConnection):
            return self._conn.entity_registry.is_existing_table(qualified_table_name)
        else:
            if len(qualified_table_name) == 1:
                # name in the form of "table"
                tables = self._run_query(
                    f"show tables like '{strip_double_quotes_in_like_statement_in_table_name(qualified_table_name[0])}'"
                )
            elif len(qualified_table_name) == 2:
                # name in the form of "schema.table" omitting database
                # schema: qualified_table_name[0]
                # table: qualified_table_name[1]
                tables = self._run_query(
                    f"show tables like '{strip_double_quotes_in_like_statement_in_table_name(qualified_table_name[1])}' in schema {qualified_table_name[0]}"
                )
            elif len(qualified_table_name) == 3:
                # name in the form of "database.schema.table"
                # database: qualified_table_name[0]
                # schema: qualified_table_name[1]
                # table: qualified_table_name[2]
                # special case:  (''<database_name>..<object_name>''), by following
                # https://docs.snowflake.com/en/sql-reference/name-resolution#resolution-when-schema-omitted-double-dot-notation
                # The two dots indicate that the schema name is not specified.
                # The PUBLIC default schema is always referenced.
                condition = (
                    f"schema {qualified_table_name[0]}.PUBLIC"
                    if qualified_table_name[1] == ""
                    else f"schema {qualified_table_name[0]}.{qualified_table_name[1]}"
                )
                tables = self._run_query(
                    f"show tables like '{strip_double_quotes_in_like_statement_in_table_name(qualified_table_name[2])}' in {condition}"
                )
            else:
                # we do not support len(qualified_table_name) > 3 for now
                raise SnowparkClientExceptionMessages.GENERAL_INVALID_OBJECT_NAME(
                    ".".join(raw_table_name)
                )
            return tables is not None and len(tables) > 0

    def _explain_query(self, query: str) -> Optional[str]:
        try:
            return self._run_query(f"explain using text {query}")[0][0]
        # return None for queries which can't be explained
        except ProgrammingError:
            _logger.warning("query `%s` cannot be explained", query)
            return None

    def _get_or_register_xpath_udf(
        self, return_type: Literal["array", "string", "boolean", "int", "float"]
    ) -> "UserDefinedFunction":
        """
        Get or register an XPath UDF for the specified return type.

        This function manages UDF registration for XPath evaluation. It uses session-level
        caching to avoid re-registering the same UDF multiple times. The cache is stored
        in the session object for thread safety.
        """
        # Use session's xpath UDF cache with thread safety
        with self._xpath_udf_cache_lock:
            cache_key = return_type

            if cache_key not in self._xpath_udf_cache:
                # Determine Snowpark return type
                return_type_map = {
                    "array": ArrayType(StringType()),
                    "string": StringType(),
                    "boolean": BooleanType(),
                    "int": IntegerType(),
                    "float": FloatType(),
                }

                # Get the handler function name for this return type
                handler_name = XPATH_HANDLER_MAP[return_type]

                # Handle stored procedure case
                if is_in_stored_procedure():  # pragma: no cover
                    # create a temp stage for UDF import files
                    # we have to use "temp" object instead of "scoped temp" object in stored procedure
                    # so we need to upload the file to the temp stage first to use register_from_file
                    temp_stage = random_name_for_temp_object(TempObjectType.STAGE)
                    sql_create_temp_stage = (
                        f"create temp stage if not exists {temp_stage}"
                    )
                    self.sql(sql_create_temp_stage, _emit_ast=False).collect(
                        _emit_ast=False
                    )
                    self._conn.upload_file(
                        XPATH_HANDLERS_FILE_PATH,
                        temp_stage,
                        compress_data=False,
                        overwrite=True,
                        skip_upload_on_content_match=True,
                    )
                    python_file_path = f"{STAGE_PREFIX}{temp_stage}/{os.path.basename(XPATH_HANDLERS_FILE_PATH)}"
                else:
                    python_file_path = XPATH_HANDLERS_FILE_PATH

                # Register UDF from file
                xpath_udf = self.udf.register_from_file(
                    python_file_path,
                    handler_name,
                    return_type=return_type_map[return_type],
                    input_types=[StringType(), StringType()],
                    packages=["snowflake-snowpark-python", "lxml<6"],
                    replace=True,
                    _emit_ast=False,
                    _suppress_local_package_warnings=True,
                )

                self._xpath_udf_cache[cache_key] = xpath_udf

            return self._xpath_udf_cache[cache_key]

    createDataFrame = create_dataframe

    def _execute_sproc_internal(
        self,
        query: str,
        is_return_table: bool,
        block: bool,
        statement_params: Optional[Dict[str, Any]] = None,
        ast_stmt=None,
        log_on_exception: bool = False,
    ) -> Union[Any, AsyncJob]:
        """Unified internal executor for sync/async, table/scalar SPROCs."""
        if not block:
            results_cursor = self._conn.execute_async_and_notify_query_listener(
                query, _statement_params=statement_params
            )
            return AsyncJob(
                results_cursor["queryId"],
                query,
                self,
                _AsyncResultType.ROW if is_return_table else _AsyncResultType.COUNT,
                log_on_exception=log_on_exception,
            )

        if is_return_table:
            qid = self._conn.execute_and_get_sfqid(
                query, statement_params=statement_params
            )
            df = self.sql(result_scan_statement(qid), _ast_stmt=ast_stmt)
            set_api_call_source(df, "Session.call")
            return df
        else:
            # TODO SNOW-1672561: This here needs to emit an eval as well.
            df = self.sql(query, _ast_stmt=ast_stmt)
            set_api_call_source(df, "Session.call")
            # Note the collect is implicit within the stored procedure call, so should not emit_ast here.
            return df.collect(statement_params=statement_params, _emit_ast=False)[0][0]

    def directory(self, stage_name: str, _emit_ast: bool = True) -> DataFrame:
        """
        Returns a DataFrame representing the results of a directory table query on the specified stage.

        A directory table query retrieves file-level metadata about the data files in a Snowflake stage.
        This includes information like relative path, file size, last modified timestamp, file URL, and checksums.

        Note:
            The stage must have a directory table enabled for this method to work. The query is
            executed lazily, which means the SQL is not executed until methods like
            :func:`DataFrame.collect` or :func:`DataFrame.to_pandas` evaluate the DataFrame.

        Args:
            stage_name: The name of the stage to query. The stage name should not include the '@' prefix
                as it will be added automatically.

        Returns:
            A DataFrame containing metadata about files in the stage with the following columns:

            - ``RELATIVE_PATH``: Path to the files to access using the file URL
            - ``SIZE``: Size of the file in bytes
            - ``LAST_MODIFIED``: Timestamp when the file was last updated in the stage
            - ``MD5``: MD5 checksum for the file
            - ``ETAG``: ETag header for the file
            - ``FILE_URL``: Snowflake file URL to access the file

        Examples::
            >>> # Get all file metadata from a stage named 'test_stage'
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE test_stage DIRECTORY = (ENABLE = TRUE)").collect()
            >>> _ = session.file.put("tests/resources/testCSV.csv", "@test_stage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/testJson.json", "@test_stage", auto_compress=False)
            >>> _ = session.sql("ALTER STAGE test_stage REFRESH").collect()

            >>> # List all files in the stage
            >>> df = session.directory('test_stage')
            >>> df.count()
            2

            >>> # Get file URLs for CSV files only
            >>> csv_files = session.directory('test_stage').filter(
            ...     col('RELATIVE_PATH').like('%.csv%')
            ... ).select('RELATIVE_PATH')
            >>> csv_files.show()
            -------------------
            |"RELATIVE_PATH"  |
            -------------------
            |testCSV.csv      |
            -------------------
            <BLANKLINE>

        For details, see the Snowflake documentation on
        `Snowflake Directory Tables Documentation <https://docs.snowflake.com/en/user-guide/data-load-dirtables-query>`_
        """
        stage_name = (
            stage_name
            if stage_name.startswith(STAGE_PREFIX)
            else f"{STAGE_PREFIX}{stage_name}"
        )
        stmt = None
        if _emit_ast:
            stmt = self._ast_batch.bind()
            ast = with_src_position(stmt.expr.directory, stmt)
            ast.stage_name = stage_name

        # string "@<stage_name>" does not work with parameter binding
        return self.sql(
            f"SELECT * FROM DIRECTORY({stage_name})",
            _ast_stmt=stmt,
            _emit_ast=_emit_ast,
        )

    @publicapi
    def begin_transaction(
        self, name: Optional[str] = None, _emit_ast: bool = True
    ) -> None:
        """
        Begins a new transaction in the current session.

        Args:
            name: Optional string that assigns a name to the transaction. A name helps
                identify a transaction, but is not required and does not need to be unique.

        Example::
            >>> # Begin an anonymous transaction
            >>> session.begin_transaction()

            >>> # Begin a named transaction
            >>> session.begin_transaction("my_transaction")
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._ast_batch.bind()
            ast = with_src_position(stmt.expr.begin_transaction, stmt)
            if name is not None:
                ast.name.value = name

        query = f"BEGIN TRANSACTION {('NAME ' + name) if name else ''}"
        self.sql(query, _ast_stmt=stmt, _emit_ast=_emit_ast).collect(_emit_ast=False)

    @publicapi
    def commit(self, _emit_ast: bool = True) -> None:
        """
        Commits an open transaction in the current session.

        Example::
            >>> session.begin_transaction()
            >>> session.commit()
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._ast_batch.bind()
            with_src_position(stmt.expr.commit, stmt)

        self.sql("COMMIT", _ast_stmt=stmt, _emit_ast=_emit_ast).collect(_emit_ast=False)

    @publicapi
    def rollback(self, _emit_ast: bool = True) -> None:
        """
        Rolls back an open transaction in the current session.

        Example::
            >>> session.begin_transaction()
            >>> session.rollback()
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._ast_batch.bind()
            with_src_position(stmt.expr.rollback, stmt)

        self.sql("ROLLBACK", _ast_stmt=stmt, _emit_ast=_emit_ast).collect(
            _emit_ast=False
        )
