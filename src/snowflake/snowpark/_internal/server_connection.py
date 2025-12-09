#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools
import importlib
import inspect
import os
import sys
import threading
from logging import getLogger
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from snowflake.connector import SnowflakeConnection, connect
from snowflake.connector.constants import ENV_VAR_PARTNER, FIELD_ID_TO_NAME
from snowflake.connector.cursor import ResultMetadata, SnowflakeCursor
from snowflake.connector.errors import Error, NotSupportedError, ProgrammingError
from snowflake.connector.network import ReauthenticationRequest
from snowflake.connector.options import pandas
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import str_to_sql
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.schema_utils import (
    cached_analyze_attributes,
    convert_result_meta_to_attribute,
    get_new_description,
    run_new_describe,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    BatchInsertQuery,
    PlanQueryType,
    Query,
    SnowflakePlan,
)
from snowflake.snowpark._internal.ast.utils import DATAFRAME_AST_PARAMETER
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import (
    TelemetryClient,
    get_plan_telemetry_metrics,
)
from snowflake.snowpark._internal.utils import (
    create_rlock,
    create_thread_local,
    escape_quotes,
    is_ast_enabled,
    get_application_name,
    get_version,
    is_in_stored_procedure,
    is_sql_select_statement,
    measure_time,
    normalize_local_file,
    normalize_remote_file_or_dir,
    result_set_to_iter,
    result_set_to_rows,
    unwrap_stage_location_single_quote,
)
from snowflake.snowpark.async_job import AsyncJob, _AsyncResultType
from snowflake.snowpark.query_history import QueryListener, QueryRecord
from snowflake.snowpark.row import Row

if TYPE_CHECKING:
    try:
        from snowflake.connector.cursor import ResultMetadataV2
    except ImportError:
        ResultMetadataV2 = ResultMetadata

logger = getLogger(__name__)

# parameters needed for usage tracking
PARAM_APPLICATION = "application"
PARAM_INTERNAL_APPLICATION_NAME = "internal_application_name"
PARAM_INTERNAL_APPLICATION_VERSION = "internal_application_version"
DEFAULT_STRING_SIZE = 16777216
MAX_STRING_SIZE = 134217728


def _build_target_path(stage_location: str, dest_prefix: str = "") -> str:
    qualified_stage_name = unwrap_stage_location_single_quote(stage_location)
    dest_prefix_name = (
        dest_prefix
        if not dest_prefix or dest_prefix.startswith("/")
        else f"/{dest_prefix}"
    )
    return f"{qualified_stage_name}{dest_prefix_name if dest_prefix_name else ''}"


def _build_put_statement(
    local_path: str,
    stage_location: str,
    dest_prefix: str = "",
    parallel: int = 4,
    compress_data: bool = True,
    source_compression: str = "AUTO_DETECT",
    overwrite: bool = False,
) -> str:
    target_path = normalize_remote_file_or_dir(
        _build_target_path(stage_location, dest_prefix)
    )
    parallel_str = f"PARALLEL = {parallel}"
    compress_str = f"AUTO_COMPRESS = {str(compress_data).upper()}"
    source_compression_str = f"SOURCE_COMPRESSION = {source_compression.upper()}"
    overwrite_str = f"OVERWRITE = {str(overwrite).upper()}"
    final_statement = f"PUT {local_path} {target_path} {parallel_str} {compress_str} {source_compression_str} {overwrite_str}"
    return final_statement


class ServerConnection:
    class _Decorator:
        @classmethod
        def wrap_exception(cls, func):
            def wrap(*args, **kwargs):
                # self._conn.is_closed()
                if args[0]._conn.is_closed():
                    raise SnowparkClientExceptionMessages.SERVER_SESSION_HAS_BEEN_CLOSED()
                try:
                    return func(*args, **kwargs)
                except ReauthenticationRequest as ex:
                    raise SnowparkClientExceptionMessages.SERVER_SESSION_EXPIRED(
                        ex.cause
                    )
                except Exception as ex:
                    raise ex

            return wrap

        @classmethod
        def log_msg_and_perf_telemetry(cls, msg):
            def log_and_telemetry(func):
                @functools.wraps(func)
                def wrap(*args, **kwargs):
                    logger.debug(msg)
                    with measure_time() as query_duration:
                        result = func(*args, **kwargs)
                    sfqid = result["sfqid"] if result and "sfqid" in result else None
                    # If we don't have a query id, then its pretty useless to send perf telemetry
                    if sfqid:
                        args[0]._telemetry_client.send_upload_file_perf_telemetry(
                            func.__name__, query_duration(), sfqid
                        )
                    logger.debug(f"Finished in {query_duration():.4f} secs")
                    return result

                return wrap

            return log_and_telemetry

    def __init__(
        self,
        options: Dict[str, Union[int, str]],
        conn: Optional[SnowflakeConnection] = None,
    ) -> None:
        self._lower_case_parameters = {k.lower(): v for k, v in options.items()}
        self._add_application_parameters()
        self._conn = conn if conn else connect(**self._lower_case_parameters)
        self.max_string_size = DEFAULT_STRING_SIZE
        if self._conn._session_parameters:
            try:
                self.max_string_size = int(
                    self._conn._session_parameters.get(
                        "VARCHAR_AND_BINARY_MAX_SIZE_IN_RESULT", self.max_string_size
                    )
                )
            except TypeError:
                pass

        # thread safe param protection
        self._thread_safe_session_enabled = self._get_client_side_session_parameter(
            "PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION", False
        )
        self._lock = create_rlock(self._thread_safe_session_enabled)
        self._thread_store = create_thread_local(self._thread_safe_session_enabled)

        if "password" in self._lower_case_parameters:
            self._lower_case_parameters["password"] = None
        self._telemetry_client = TelemetryClient(self._conn)
        self._query_listeners: Set[QueryListener] = set()
        # The session in this case refers to a Snowflake session, not a
        # Snowpark session
        self._telemetry_client.send_session_created_telemetry(not bool(conn))

        # check if cursor.execute supports _skip_upload_on_content_match
        signature = inspect.signature(self._cursor.execute)
        self._supports_skip_upload_on_content_match = (
            "_skip_upload_on_content_match" in signature.parameters
        )

    @property
    def _cursor(self) -> SnowflakeCursor:
        if not hasattr(self._thread_store, "cursor"):
            self._thread_store.cursor = self._conn.cursor()
            self._telemetry_client.send_cursor_created_telemetry(
                self.get_session_id(), threading.get_ident()
            )
        return self._thread_store.cursor

    def _add_application_parameters(self) -> None:
        if PARAM_APPLICATION not in self._lower_case_parameters:
            # Mirrored from snowflake-connector-python/src/snowflake/connector/connection.py#L295
            if ENV_VAR_PARTNER in os.environ.keys():
                self._lower_case_parameters[PARAM_APPLICATION] = os.environ[
                    ENV_VAR_PARTNER
                ]
            else:
                applications = []
                if importlib.util.find_spec("streamlit"):
                    applications.append("streamlit")
                if importlib.util.find_spec("snowflake.ml"):
                    applications.append("SnowparkML")
                if importlib.util.find_spec("snowbook"):
                    applications = ["Snowflake Web App (snowsight_notebook)"]
                self._lower_case_parameters[PARAM_APPLICATION] = (
                    ":".join(applications) or get_application_name()
                )

        if PARAM_INTERNAL_APPLICATION_NAME not in self._lower_case_parameters:
            self._lower_case_parameters[
                PARAM_INTERNAL_APPLICATION_NAME
            ] = get_application_name()
        if PARAM_INTERNAL_APPLICATION_VERSION not in self._lower_case_parameters:
            self._lower_case_parameters[
                PARAM_INTERNAL_APPLICATION_VERSION
            ] = get_version()

    def add_query_listener(self, listener: QueryListener) -> None:
        with self._lock:
            self._query_listeners.add(listener)

    def remove_query_listener(self, listener: QueryListener) -> None:
        with self._lock:
            self._query_listeners.remove(listener)

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def is_closed(self) -> bool:
        return self._conn.is_closed()

    @_Decorator.wrap_exception
    def get_session_id(self) -> int:
        return self._conn.session_id

    @_Decorator.wrap_exception
    def _get_current_parameter(self, param: str, quoted: bool = True) -> Optional[str]:
        name = getattr(self._conn, param) or self._get_string_datum(
            f"SELECT CURRENT_{param.upper()}()"
        )
        return (
            (quote_name_without_upper_casing(name) if quoted else escape_quotes(name))
            if name
            else None
        )

    def _get_string_datum(self, query: str) -> Optional[str]:
        rows = result_set_to_rows(self.run_query(query)["data"])
        return rows[0][0] if len(rows) > 0 else None

    def get_result_attributes(
        self, query: str, query_params: Optional[Sequence[Any]] = None
    ) -> List[Attribute]:
        return convert_result_meta_to_attribute(
            self._run_new_describe(self._cursor, query, query_params=query_params),
            self.max_string_size,
        )

    def _run_new_describe(
        self,
        cursor: SnowflakeCursor,
        query: str,
        query_params: Optional[Sequence[Any]] = None,
        **kwargs: dict,
    ) -> Union[List[ResultMetadata], List["ResultMetadataV2"]]:
        result_metadata = run_new_describe(cursor, query, query_params)

        with self._lock:
            for listener in filter(
                lambda listener: hasattr(listener, "include_describe")
                and listener.include_describe,
                self._query_listeners,
            ):
                thread_id = (
                    threading.get_ident()
                    if getattr(listener, "include_thread_id", False)
                    else None
                )
                query_record = QueryRecord(
                    cursor.sfqid, query, True, thread_id=thread_id
                )
                listener._notify(query_record, **kwargs)

        return result_metadata

    @_Decorator.log_msg_and_perf_telemetry("Uploading file to stage")
    def upload_file(
        self,
        path: str,
        stage_location: str,
        dest_prefix: str = "",
        parallel: int = 4,
        compress_data: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
        skip_upload_on_content_match: bool = False,
    ) -> Optional[Dict[str, Any]]:
        if is_in_stored_procedure():  # pragma: no cover
            file_name = os.path.basename(path)
            target_path = _build_target_path(stage_location, dest_prefix)
            try:
                # upload_stream directly consume stage path, so we don't need to normalize it
                self._cursor.upload_stream(
                    open(path, "rb"), f"{target_path}/{file_name}"
                )
            except ProgrammingError as pe:
                tb = sys.exc_info()[2]
                ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                    pe
                )
                raise ne.with_traceback(tb) from None
        else:
            uri = normalize_local_file(path)
            if self._supports_skip_upload_on_content_match:
                kwargs = {"_skip_upload_on_content_match": skip_upload_on_content_match}
            else:
                kwargs = {}
            return self.run_query(
                _build_put_statement(
                    uri,
                    stage_location,
                    dest_prefix,
                    parallel,
                    compress_data,
                    source_compression,
                    overwrite,
                ),
                **kwargs,
            )

    @_Decorator.log_msg_and_perf_telemetry("Uploading stream to stage")
    def upload_stream(
        self,
        input_stream: IO[bytes],
        stage_location: str,
        dest_filename: str,
        dest_prefix: str = "",
        parallel: int = 4,
        compress_data: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
        is_in_udf: bool = False,
        skip_upload_on_content_match: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        uri = normalize_local_file(f"/tmp/placeholder/{dest_filename}")
        try:
            if is_in_stored_procedure():  # pragma: no cover
                input_stream.seek(0)
                target_path = _build_target_path(stage_location, dest_prefix)
                try:
                    # upload_stream directly consume stage path, so we don't need to normalize it
                    self._cursor.upload_stream(
                        input_stream, f"{target_path}/{dest_filename}"
                    )
                except ProgrammingError as pe:
                    tb = sys.exc_info()[2]
                    ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                        pe
                    )
                    raise ne.with_traceback(tb) from None
            else:
                if self._supports_skip_upload_on_content_match:
                    kwargs = {
                        "_skip_upload_on_content_match": skip_upload_on_content_match,
                        "file_stream": input_stream,
                    }
                else:
                    kwargs = {"file_stream": input_stream}
                return self.run_query(
                    _build_put_statement(
                        uri,
                        stage_location,
                        dest_prefix,
                        parallel,
                        compress_data,
                        source_compression,
                        overwrite,
                    ),
                    _statement_params=statement_params,
                    **kwargs,
                )
        # If ValueError is raised and the stream is closed, we throw the error.
        # https://docs.python.org/3/library/io.html#io.IOBase.close
        except ValueError as ex:
            if input_stream.closed:
                if is_in_udf:
                    raise SnowparkClientExceptionMessages.SERVER_UDF_UPLOAD_FILE_STREAM_CLOSED(
                        dest_filename
                    )
                else:
                    raise SnowparkClientExceptionMessages.SERVER_UPLOAD_FILE_STREAM_CLOSED(
                        dest_filename
                    )
            else:
                raise ex

    def notify_query_listeners(
        self, query_record: QueryRecord, is_error: bool = False, **kwargs
    ) -> None:
        with self._lock:
            for listener in self._query_listeners:
                # if listener is not set to record error query, skip
                if is_error and not getattr(listener, "include_error", False):
                    continue
                if getattr(listener, "include_thread_id", False):
                    new_record = QueryRecord(
                        query_record.query_id,
                        query_record.sql_text,
                        query_record.is_describe,
                        thread_id=threading.get_ident(),
                    )
                    listener._notify(new_record, **kwargs)
                else:
                    listener._notify(query_record, **kwargs)

    def execute_and_notify_query_listener(
        self, query: str, **kwargs: Any
    ) -> SnowflakeCursor:
        notify_kwargs = {}
        if DATAFRAME_AST_PARAMETER in kwargs and is_ast_enabled():
            notify_kwargs["dataframeAst"] = kwargs[DATAFRAME_AST_PARAMETER]
        if "_statement_params" in kwargs and kwargs["_statement_params"]:
            statement_params = kwargs["_statement_params"]
            if "_PLAN_UUID" in statement_params:
                notify_kwargs["dataframe_uuid"] = statement_params["_PLAN_UUID"]
        try:
            results_cursor = self._cursor.execute(query, **kwargs)
        except Exception as ex:
            notify_kwargs["requestId"] = None
            notify_kwargs["exception"] = ex
            sfqid = ex.sfqid if isinstance(ex, Error) else None
            err_query = ex.query if isinstance(ex, Error) else query
            self.notify_query_listeners(
                QueryRecord(sfqid, err_query, False), is_error=True, **notify_kwargs
            )
            raise ex

        notify_kwargs["requestId"] = str(results_cursor._request_id)
        self.notify_query_listeners(
            QueryRecord(results_cursor.sfqid, results_cursor.query), **notify_kwargs
        )
        return results_cursor

    def execute_async_and_notify_query_listener(
        self, query: str, **kwargs: Any
    ) -> Dict[str, Any]:
        notify_kwargs = {}

        if "_statement_params" in kwargs and kwargs["_statement_params"]:
            statement_params = kwargs["_statement_params"]
            if "_PLAN_UUID" in statement_params:
                notify_kwargs["dataframe_uuid"] = statement_params["_PLAN_UUID"]

        try:
            results_cursor = self._cursor.execute_async(query, **kwargs)
        except Error as err:
            self.notify_query_listeners(
                QueryRecord(err.sfqid, err.query), is_error=True, **notify_kwargs
            )
            raise err
        self.notify_query_listeners(
            QueryRecord(results_cursor["queryId"], query), **notify_kwargs
        )
        return results_cursor

    def execute_and_get_sfqid(
        self,
        query: str,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> str:
        results_cursor = self.execute_and_notify_query_listener(
            query, _statement_params=statement_params
        )
        return results_cursor.sfqid

    @_Decorator.wrap_exception
    def run_query(
        self,
        query: str,
        to_pandas: bool = False,
        to_iter: bool = False,
        is_ddl_on_temp_object: bool = False,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        async_job_plan: Optional[
            SnowflakePlan
        ] = None,  # this argument is currently only used by AsyncJob
        log_on_exception: bool = False,
        case_sensitive: bool = True,
        params: Optional[Sequence[Any]] = None,
        num_statements: Optional[int] = None,
        ignore_results: bool = False,
        async_post_actions: Optional[List[Query]] = None,
        to_arrow: bool = False,
        **kwargs,
    ) -> Union[Dict[str, Any], AsyncJob]:
        try:
            # Set SNOWPARK_SKIP_TXN_COMMIT_IN_DDL to True to avoid DDL commands to commit the open transaction
            if is_ddl_on_temp_object:
                if not kwargs.get("_statement_params"):
                    kwargs["_statement_params"] = {}
                kwargs["_statement_params"]["SNOWPARK_SKIP_TXN_COMMIT_IN_DDL"] = True
            if not is_sql_select_statement(query):
                cached_analyze_attributes.clear_cache()
            if block:
                results_cursor = self.execute_and_notify_query_listener(
                    query, params=params, **kwargs
                )
                logger.debug(f"Execute query [queryID: {results_cursor.sfqid}] {query}")
            else:
                results_cursor = self.execute_async_and_notify_query_listener(
                    query, params=params, num_statements=num_statements, **kwargs
                )
                logger.debug(
                    f"Execute async query [queryID: {results_cursor['queryId']}] {query}"
                )
        except Exception as ex:
            if log_on_exception:
                query_id_log = f" [queryID: {ex.sfqid}]" if hasattr(ex, "sfqid") else ""
                logger.error(f"Failed to execute query{query_id_log} {query}\n{ex}")
            raise ex

        # fetch_pandas_all/batches() only works for SELECT statements
        # We call fetchall() if fetch_pandas_all/batches() fails,
        # because when the query plan has multiple queries, it will
        # have non-select statements, and it shouldn't fail if the user
        # calls to_pandas() to execute the query.
        if block:
            if ignore_results:
                return {"data": None, "sfqid": results_cursor.sfqid}
            return self._to_data_or_iter(
                results_cursor=results_cursor,
                to_pandas=to_pandas,
                to_iter=to_iter,
                to_arrow=to_arrow,
            )
        else:
            return AsyncJob(
                results_cursor["queryId"],
                query,
                async_job_plan.session,
                data_type,
                async_post_actions,
                log_on_exception,
                case_sensitive=case_sensitive,
                num_statements=num_statements,
                **kwargs,
            )

    def _to_data_or_iter(
        self,
        results_cursor: SnowflakeCursor,
        to_pandas: bool = False,
        to_iter: bool = False,
        to_arrow: bool = False,
    ) -> Dict[str, Any]:
        qid = results_cursor.sfqid
        if to_iter:
            new_cursor = results_cursor.connection.cursor()
            new_cursor.get_results_from_sfqid(qid)
            results_cursor = new_cursor

        if to_pandas:
            try:
                data_or_iter = (
                    map(
                        functools.partial(
                            _fix_pandas_df_fixed_type, results_cursor=results_cursor
                        ),
                        results_cursor.fetch_pandas_batches(split_blocks=True),
                    )
                    if to_iter
                    else _fix_pandas_df_fixed_type(
                        results_cursor.fetch_pandas_all(split_blocks=True),
                        results_cursor,
                    )
                )
            except NotSupportedError:
                data_or_iter = (
                    iter(results_cursor) if to_iter else results_cursor.fetchall()
                )
            except KeyboardInterrupt:
                raise
            except BaseException as ex:
                raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_PANDAS(
                    str(ex)
                )
        elif to_arrow:
            data_or_iter = (
                results_cursor.fetch_arrow_batches()
                if to_iter
                else results_cursor.fetch_arrow_all(True)
            )
        else:
            data_or_iter = (
                iter(results_cursor) if to_iter else results_cursor.fetchall()
            )

        return {"data": data_or_iter, "sfqid": qid}

    def execute(
        self,
        plan: SnowflakePlan,
        to_pandas: bool = False,
        to_iter: bool = False,
        to_arrow: bool = False,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
        **kwargs,
    ) -> Union[
        List[Row], "pandas.DataFrame", Iterator[Row], Iterator["pandas.DataFrame"]
    ]:
        if (
            is_in_stored_procedure()
            and not block
            and not self._get_client_side_session_parameter(
                "ENABLE_ASYNC_QUERY_IN_PYTHON_STORED_PROCS", False
            )
        ):  # pragma: no cover
            raise NotImplementedError(
                "Async query is not supported in stored procedure yet"
            )
        result_set, result_meta = self.get_result_set(
            plan,
            to_pandas,
            to_iter,
            **kwargs,
            block=block,
            data_type=data_type,
            log_on_exception=log_on_exception,
            case_sensitive=case_sensitive,
            to_arrow=to_arrow,
        )
        if not block:
            return result_set
        elif to_pandas or to_arrow:
            return result_set["data"]
        else:
            if to_iter:
                return result_set_to_iter(
                    result_set["data"], result_meta, case_sensitive=case_sensitive
                )
            else:
                return result_set_to_rows(
                    result_set["data"], result_meta, case_sensitive=case_sensitive
                )

    @SnowflakePlan.Decorator.wrap_exception
    def get_result_set(
        self,
        plan: SnowflakePlan,
        to_pandas: bool = False,
        to_iter: bool = False,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
        ignore_results: bool = False,
        to_arrow: bool = False,
        **kwargs,
    ) -> Tuple[
        Dict[
            str,
            Union[
                List[Any],
                "pandas.DataFrame",
                SnowflakeCursor,
                Iterator["pandas.DataFrame"],
                str,
            ],
        ],
        Union[List[ResultMetadata], List["ResultMetadataV2"]],
    ]:
        action_id = plan.session._generate_new_action_id()
        plan_queries = plan.execution_queries
        result, result_meta = None, None
        statement_params = kwargs.get("_statement_params", None) or {}
        statement_params["_PLAN_UUID"] = plan.uuid
        kwargs["_statement_params"] = statement_params
        try:
            main_queries = plan_queries[PlanQueryType.QUERIES]
            post_actions = plan_queries[PlanQueryType.POST_ACTIONS]
            placeholders = {}
            is_batch_insert = False
            for q in main_queries:
                if isinstance(q, BatchInsertQuery):
                    is_batch_insert = True
                    break
            # since batch insert does not support async execution (? in the query), we handle it separately here
            if len(main_queries) > 1 and not block and not is_batch_insert:
                params = []
                final_queries = []
                last_place_holder = None
                for q in main_queries:
                    final_queries.append(
                        q.sql.replace(f"'{last_place_holder}'", "LAST_QUERY_ID()")
                        if last_place_holder
                        else q.sql
                    )
                    last_place_holder = q.query_id_place_holder
                    params.extend(q.params)

                result = self.run_query(
                    ";".join(final_queries),
                    to_pandas,
                    to_iter,
                    is_ddl_on_temp_object=main_queries[0].is_ddl_on_temp_object,
                    block=block,
                    data_type=data_type,
                    async_job_plan=plan,
                    log_on_exception=log_on_exception,
                    case_sensitive=case_sensitive,
                    num_statements=len(main_queries),
                    params=params,
                    ignore_results=ignore_results,
                    async_post_actions=post_actions,
                    to_arrow=to_arrow,
                    **kwargs,
                )

                # since we will return a AsyncJob instance, result_meta is not needed, we will create result_meta in
                # AsyncJob instance when needed
                result_meta = None
                if action_id < plan.session._last_canceled_id:
                    raise SnowparkClientExceptionMessages.SERVER_QUERY_IS_CANCELLED()
            else:
                # Only send dataframe AST on the last query.
                if DATAFRAME_AST_PARAMETER in kwargs:
                    dataframe_ast = kwargs[DATAFRAME_AST_PARAMETER]
                    del kwargs[DATAFRAME_AST_PARAMETER]
                else:
                    dataframe_ast = None

                for i, query in enumerate(main_queries):
                    if isinstance(query, BatchInsertQuery):
                        self.run_batch_insert(query.sql, query.rows, **kwargs)
                    else:
                        is_last = i == len(main_queries) - 1 and not block
                        final_query = query.sql
                        for holder, id_ in placeholders.items():
                            final_query = final_query.replace(holder, id_)
                        if i == len(main_queries) - 1 and dataframe_ast:
                            kwargs[DATAFRAME_AST_PARAMETER] = dataframe_ast
                        is_final_query = i == len(main_queries) - 1
                        result = self.run_query(
                            final_query,
                            to_pandas,
                            to_iter and is_final_query,
                            is_ddl_on_temp_object=query.is_ddl_on_temp_object,
                            block=not is_last,
                            data_type=data_type,
                            async_job_plan=plan,
                            log_on_exception=log_on_exception,
                            case_sensitive=case_sensitive,
                            params=query.params,
                            ignore_results=ignore_results,
                            async_post_actions=post_actions,
                            to_arrow=to_arrow and is_final_query,
                            **kwargs,
                        )
                        placeholders[query.query_id_place_holder] = (
                            result["sfqid"] if not is_last else result.query_id
                        )
                        result_meta = get_new_description(self._cursor)
                    if action_id < plan.session._last_canceled_id:
                        raise SnowparkClientExceptionMessages.SERVER_QUERY_IS_CANCELLED()
        finally:
            # delete created tmp object
            if block:
                if DATAFRAME_AST_PARAMETER in kwargs:
                    del kwargs[DATAFRAME_AST_PARAMETER]
                for action in post_actions:
                    self.run_query(
                        action.sql,
                        is_ddl_on_temp_object=action.is_ddl_on_temp_object,
                        block=block,
                        log_on_exception=log_on_exception,
                        case_sensitive=case_sensitive,
                        **kwargs,
                    )

            if plan.session._collect_snowflake_plan_telemetry_at_critical_path:
                self._telemetry_client.send_plan_metrics_telemetry(
                    session_id=self.get_session_id(),
                    data=get_plan_telemetry_metrics(plan),
                )

        if result is None:
            raise SnowparkClientExceptionMessages.SQL_LAST_QUERY_RETURN_RESULTSET()

        return result, result_meta

    def get_result_and_metadata(
        self, plan: SnowflakePlan, **kwargs
    ) -> Tuple[List[Row], List[Attribute]]:
        result_set, result_meta = self.get_result_set(plan, **kwargs)
        result = result_set_to_rows(result_set["data"])
        attributes = convert_result_meta_to_attribute(result_meta, self.max_string_size)
        return result, attributes

    def get_result_query_id(self, plan: SnowflakePlan, **kwargs) -> str:
        # get the iterator such that the data is not fetched
        result_set, _ = self.get_result_set(plan, ignore_results=True, **kwargs)
        return result_set["sfqid"]

    @_Decorator.wrap_exception
    def run_batch_insert(self, query: str, rows: List[Row], **kwargs) -> None:
        # with qmark, Python data type will be dynamically mapped to Snowflake data type
        # https://docs.snowflake.com/en/user-guide/python-connector-api.html#data-type-mappings-for-qmark-and-numeric-bindings
        params = [list(row) for row in rows]
        statement_params = kwargs.get("_statement_params")
        query_tag = (
            statement_params["QUERY_TAG"]
            if statement_params is not None
            and "QUERY_TAG" in statement_params
            and not is_in_stored_procedure()
            else None
        )
        if query_tag:
            self.execute_and_notify_query_listener(
                f"alter session set query_tag = {str_to_sql(query_tag)}"
            )
        try:
            results_cursor = self._cursor.executemany(query, params)
        except Error as err:
            self.notify_query_listeners(
                QueryRecord(err.sfqid, err.query), is_error=True
            )
            raise err
        self.notify_query_listeners(
            QueryRecord(results_cursor.sfqid, results_cursor.query)
        )
        if query_tag:
            self.execute_and_notify_query_listener("alter session unset query_tag")
        logger.debug("Execute batch insertion query %s", query)

    def _get_client_side_session_parameter(self, name: str, default_value: Any) -> Any:
        """It doesn't go to Snowflake to retrieve the session parameter.
        Use this only when you know the Snowflake session parameter is sent to the client when a session/connection is created.
        """
        return (
            self._conn._session_parameters.get(name, default_value)
            if self._conn._session_parameters
            else default_value
        )


def _fix_pandas_df_fixed_type(
    pd_df: "pandas.DataFrame", results_cursor: SnowflakeCursor
) -> "pandas.DataFrame":
    """The compiler does not make any guarantees about the return types - only that they will be large enough for the result.
    As a result, the ResultMetadata may contain precision=38, scale=0 for result of a column which may only contain single
    digit numbers. Then the returned pandas DataFrame has dtype "object" with a str value for that column instead of int64.

    Based on the Result Metadata characteristics, this functions tries to make a best effort conversion to int64 without losing
    precision.

    We need to get rid of this workaround because this causes a performance hit.
    """
    for column_metadata, pandas_dtype, pandas_col_name in zip(
        results_cursor.description, pd_df.dtypes, pd_df.columns
    ):
        if (
            FIELD_ID_TO_NAME.get(column_metadata.type_code) == "FIXED"
            and column_metadata.precision is not None
        ):
            if column_metadata.scale == 0 and not str(pandas_dtype).startswith("int"):
                # When scale = 0 and precision values are between 10-20, the integers fit into int64.
                # If we rely only on pandas.to_numeric, it loses precision value on large integers, therefore
                # we try to strictly use astype("int64") in this scenario. If the values are too large to
                # fit in int64, an OverflowError is thrown and we rely on to_numeric to choose and appropriate
                # floating datatype to represent the number.
                if (
                    column_metadata.precision > 10
                    and not pd_df[pandas_col_name].hasnans
                ):
                    try:
                        pd_df[pandas_col_name] = pd_df[pandas_col_name].astype("int64")
                    except OverflowError:
                        pd_df[pandas_col_name] = pandas.to_numeric(
                            pd_df[pandas_col_name]
                        )
                else:
                    pd_df[pandas_col_name] = pandas.to_numeric(
                        pd_df[pandas_col_name], downcast="integer"
                    )
            elif column_metadata.scale > 0 and not str(pandas_dtype).startswith(
                "float"
            ):
                # For decimal columns, we want to cast it into float64 because pandas doesn't
                # recognize decimal type.
                pandas.to_numeric(pd_df[pandas_col_name], downcast="float")
                if pd_df[pandas_col_name].dtype == "O":
                    pd_df[pandas_col_name] = pd_df[pandas_col_name].astype("float64")

    return pd_df
