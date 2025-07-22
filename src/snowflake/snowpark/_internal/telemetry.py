#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools
import json
import threading
from enum import Enum, unique
import time
from typing import Any, Dict, List, Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.telemetry import (
    TelemetryClient as PCTelemetryClient,
    TelemetryData as PCTelemetryData,
    TelemetryField as PCTelemetryField,
)
from snowflake.connector.time_util import get_time_millis
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanState,
    get_complexity_score,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.compiler.telemetry_constants import (
    CompilationStageTelemetryField,
)
from snowflake.snowpark._internal.analyzer.metadata_utils import (
    DescribeQueryTelemetryField,
)
from snowflake.snowpark._internal.utils import (
    get_application_name,
    get_os_name,
    get_python_version,
    get_version,
    is_in_stored_procedure,
    is_interactive,
)

try:
    import psutil

    PS_UTIL_AVAILABLE = True
except ImportError:
    PS_UTIL_AVAILABLE = False


@unique
class TelemetryField(Enum):
    # constants
    MESSAGE = "message"
    NAME = "name"
    ERROR_CODE = "error_code"
    STACK_TRACE = "stack_trace"
    # Types of telemetry
    TYPE_PERFORMANCE_DATA = "snowpark_performance_data"
    TYPE_FUNCTION_USAGE = "snowpark_function_usage"
    TYPE_SESSION_CREATED = "snowpark_session_created"
    TYPE_CURSOR_CREATED = "snowpark_cursor_created"
    TYPE_SQL_SIMPLIFIER_ENABLED = "snowpark_sql_simplifier_enabled"
    TYPE_CTE_OPTIMIZATION_ENABLED = "snowpark_cte_optimization_enabled"
    # telemetry for optimization that eliminates the extra cast expression generated for expressions
    TYPE_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED = (
        "snowpark_eliminate_numeric_sql_value_cast_enabled"
    )
    TYPE_AUTO_CLEAN_UP_TEMP_TABLE_ENABLED = "snowpark_auto_clean_up_temp_table_enabled"
    TYPE_LARGE_QUERY_BREAKDOWN_ENABLED = "snowpark_large_query_breakdown_enabled"
    TYPE_REDUCE_DESCRIBE_QUERY_ENABLED = "snowpark_reduce_describe_query_enabled"
    TYPE_ERROR = "snowpark_error"
    # Message keys for telemetry
    KEY_START_TIME = "start_time"
    KEY_DURATION = "duration"
    KEY_FUNC_NAME = "func_name"
    KEY_MSG = "msg"
    KEY_WALL_TIME = "wall_time"
    KEY_CPU_TIME = "cpu_time"
    KEY_NETWORK_SENT_KIB = "network_bytes_sent_kib"
    KEY_NETWORK_RECV_KIB = "network_bytes_recv_kib"
    KEY_MEMORY_RSS_KIB = "memory_rss_kib"
    KEY_ERROR_MSG = "error_msg"
    KEY_VERSION = "version"
    KEY_PYTHON_VERSION = "python_version"
    KEY_CLIENT_LANGUAGE = "client_language"
    KEY_OS = "operating_system"
    KEY_IS_INTERACTIVE = "interactive"
    KEY_DATA = "data"
    KEY_CATEGORY = "category"
    KEY_CREATED_BY_SNOWPARK = "created_by_snowpark"
    KEY_API_CALLS = "api_calls"
    KEY_SFQIDS = "sfqids"
    KEY_SUBCALLS = "subcalls"
    KEY_SAVED_TABLE_NAME = "saved_table_name"
    # function categories
    FUNC_CAT_ACTION = "action"
    FUNC_CAT_USAGE = "usage"
    FUNC_CAT_JOIN = "join"
    FUNC_CAT_COPY = "copy"
    FUNC_CAT_CREATE = "create"
    # performance categories
    PERF_CAT_UPLOAD_FILE = "upload_file"
    PERF_CAT_DATA_SOURCE = "data_source"
    # optimizations
    SESSION_ID = "session_id"
    SQL_SIMPLIFIER_ENABLED = "sql_simplifier_enabled"
    CTE_OPTIMIZATION_ENABLED = "cte_optimization_enabled"
    LARGE_QUERY_BREAKDOWN_ENABLED = "large_query_breakdown_enabled"
    # temp table cleanup
    TYPE_TEMP_TABLE_CLEANUP = "snowpark_temp_table_cleanup"
    NUM_TEMP_TABLES_CLEANED = "num_temp_tables_cleaned"
    NUM_TEMP_TABLES_CREATED = "num_temp_tables_created"
    TEMP_TABLE_CLEANER_ENABLED = "temp_table_cleaner_enabled"
    TEMP_TABLE_CLEANUP_ABNORMAL_EXCEPTION_TABLE_NAME = (
        "temp_table_cleanup_abnormal_exception_table_name"
    )
    TEMP_TABLE_CLEANUP_ABNORMAL_EXCEPTION_MESSAGE = (
        "temp_table_cleanup_abnormal_exception_message"
    )
    # multi-threading
    THREAD_IDENTIFIER = "thread_ident"
    # data source


# These DataFrame APIs call other DataFrame APIs
# and so we remove those API calls and move them
# inside the original API call
API_CALLS_TO_ADJUST = {
    "to_df": 1,
    "select_expr": 1,
    "agg": 2,
    "with_column": 1,
    "with_columns": 1,
    "with_column_renamed": 1,
}
APIS_WITH_MULTIPLE_CALLS = list(API_CALLS_TO_ADJUST.keys())


class ResourceUsageCollector:
    """
    A context manager to collect resource usage metrics such as CPU time, wall time, and memory usage.
    """

    RESOURCE_USAGE_KEYS = frozenset(
        [
            TelemetryField.KEY_WALL_TIME.value,
            TelemetryField.KEY_CPU_TIME.value,
            TelemetryField.KEY_NETWORK_SENT_KIB.value,
            TelemetryField.KEY_NETWORK_RECV_KIB.value,
            TelemetryField.KEY_MEMORY_RSS_KIB.value,
        ]
    )

    def __init__(self) -> None:
        pass

    def __enter__(self):
        try:
            self._start_time = time.time()
            self._start_cpu_time = time.process_time()
            if PS_UTIL_AVAILABLE:
                self._start_net_io_counters = psutil.net_io_counters()
                self._start_rss = psutil.Process().memory_info().rss
        except Exception:
            pass

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._end_time = time.time()
            self._end_cpu_time = time.process_time()
            if PS_UTIL_AVAILABLE:
                self._end_net_io_counters = psutil.net_io_counters()
                self._end_rss = psutil.Process().memory_info().rss
        except Exception:
            pass

    def get_resource_usage(self) -> Dict[str, Any]:
        """
        Returns:
            A dictionary containing the resource usage metrics.
        """
        try:
            resource_usage = {}
            wall_time = self._end_time - self._start_time
            cpu_time = self._end_cpu_time - self._start_cpu_time
            resource_usage = {
                TelemetryField.KEY_WALL_TIME.value: wall_time,
                TelemetryField.KEY_CPU_TIME.value: cpu_time,
            }
            if PS_UTIL_AVAILABLE:
                network_sent = (
                    self._end_net_io_counters.bytes_sent
                    - self._start_net_io_counters.bytes_sent
                ) / 1024.0
                network_recv = (
                    self._end_net_io_counters.bytes_recv
                    - self._start_net_io_counters.bytes_recv
                ) / 1024.0
                memory_rss = (self._end_rss - self._start_rss) / 1024.0
                resource_usage.update(
                    {
                        TelemetryField.KEY_NETWORK_SENT_KIB.value: network_sent,
                        TelemetryField.KEY_NETWORK_RECV_KIB.value: network_recv,
                        TelemetryField.KEY_MEMORY_RSS_KIB.value: memory_rss,
                    }
                )
        except Exception:
            pass

        return resource_usage

    @staticmethod
    def aggregate_usage_from_subcalls(subcalls: List[Dict]) -> Dict:
        resource_usage = {}
        for subcall in subcalls:
            for key, value in subcall.items():
                if key in ResourceUsageCollector.RESOURCE_USAGE_KEYS:
                    resource_usage[key] = resource_usage.get(key, 0) + value
        return resource_usage


# Adjust API calls into subcalls for certain APIs that call other APIs
def adjust_api_subcalls(
    df,
    func_name: str,
    len_subcalls: Optional[int] = None,
    precalls: Optional[List[Dict]] = None,
    subcalls: Optional[List[Dict]] = None,
    resource_usage: Optional[Dict] = None,
) -> None:
    plan = df._select_statement or df._plan
    if len_subcalls:
        plan.api_calls = [
            *plan.api_calls[:-len_subcalls],
            {
                TelemetryField.NAME.value: func_name,
                TelemetryField.KEY_SUBCALLS.value: [*plan.api_calls[-len_subcalls:]],
            },
        ]
    elif precalls is not None and subcalls is not None:
        plan.api_calls = [
            *precalls,
            {
                TelemetryField.NAME.value: func_name,
                TelemetryField.KEY_SUBCALLS.value: [*subcalls],
            },
        ]
    # Overwrite the resource usage metrics if the function is
    # provided with its own resource usage metrics otherwise
    # aggregate this info from the subcalls
    if resource_usage is None:
        subcalls = plan.api_calls[-1].get(TelemetryField.KEY_SUBCALLS.value, [])
        resource_usage = ResourceUsageCollector.aggregate_usage_from_subcalls(subcalls)
    plan.api_calls[-1].update(resource_usage)


def add_api_call(df, func_name: str, resource_usage: Optional[Dict] = None) -> None:
    plan = df._select_statement or df._plan
    resource_usage = resource_usage or {}
    plan.api_calls.append({TelemetryField.NAME.value: func_name, **resource_usage})


def set_api_call_source(df, func_name: str) -> None:
    plan = df._select_statement or df._plan
    plan.api_calls = [{TelemetryField.NAME.value: func_name}]


# A decorator to use in the Telemetry client to make sure operations
# don't cause exceptions to be raised
def safe_telemetry(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception:
            # We don't really care if telemetry fails, just want to be safe for the user
            pass

    return wrap


# Action telemetry decorator for DataFrame class
def df_collect_api_telemetry(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        session = args[0]._session
        resource_usage = dict()
        with session.query_history() as query_history:
            try:
                with ResourceUsageCollector() as resource_usage_collector:
                    result = func(*args, **kwargs)
                resource_usage = resource_usage_collector.get_resource_usage()
            finally:
                if not session._collect_snowflake_plan_telemetry_at_critical_path:
                    session._conn._telemetry_client.send_plan_metrics_telemetry(
                        session_id=session.session_id,
                        data=get_plan_telemetry_metrics(args[0]._plan),
                    )
        plan = args[0]._select_statement or args[0]._plan
        api_calls = [
            *plan.api_calls,
            {TelemetryField.NAME.value: f"DataFrame.{func.__name__}", **resource_usage},
        ]
        # The first api call will indicate following:
        # - sql simplifier is enabled.
        api_calls[0][
            TelemetryField.SQL_SIMPLIFIER_ENABLED.value
        ] = session.sql_simplifier_enabled
        api_calls[0][TelemetryField.THREAD_IDENTIFIER.value] = threading.get_ident()
        session._conn._telemetry_client.send_function_usage_telemetry(
            f"action_{func.__name__}",
            TelemetryField.FUNC_CAT_ACTION.value,
            api_calls=api_calls,
            sfqids=[q.query_id for q in query_history.queries],
        )
        return result

    return wrap


def dfw_collect_api_telemetry(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        session = args[0]._dataframe._session
        resource_usage = dict()
        with session.query_history() as query_history:
            try:
                with ResourceUsageCollector() as resource_usage_collector:
                    result = func(*args, **kwargs)
                resource_usage = resource_usage_collector.get_resource_usage()
            finally:
                if not session._collect_snowflake_plan_telemetry_at_critical_path:
                    session._conn._telemetry_client.send_plan_metrics_telemetry(
                        session_id=session.session_id,
                        data=get_plan_telemetry_metrics(args[0]._dataframe._plan),
                    )
        plan = args[0]._dataframe._select_statement or args[0]._dataframe._plan
        table_name = (
            args[1]
            if len(args) > 1
            and isinstance(args[1], str)
            and func.__name__ == "save_as_table"
            else None
        )
        if table_name is None:
            table_name = kwargs.get("table_name", None)
        api_calls = [
            *plan.api_calls,
            {
                TelemetryField.NAME.value: f"DataFrameWriter.{func.__name__}",
                TelemetryField.KEY_SAVED_TABLE_NAME.value: table_name,
                **resource_usage,
            },
        ]
        session._conn._telemetry_client.send_function_usage_telemetry(
            f"action_{func.__name__}",
            TelemetryField.FUNC_CAT_ACTION.value,
            api_calls=api_calls,
            sfqids=[q.query_id for q in query_history.queries],
        )
        return result

    return wrap


def df_api_usage(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        with ResourceUsageCollector() as resource_usage_collector:
            r = func(*args, **kwargs)
        plan = r._select_statement or r._plan
        # Some DataFrame APIs call other DataFrame APIs, so we need to remove the extra call
        if (
            func.__name__ in APIS_WITH_MULTIPLE_CALLS
            and len(plan.api_calls) >= API_CALLS_TO_ADJUST[func.__name__]
        ):
            len_api_calls_to_adjust = API_CALLS_TO_ADJUST[func.__name__]
            subcalls = plan.api_calls[-len_api_calls_to_adjust:]
            # remove inner calls
            plan.api_calls = plan.api_calls[:-len_api_calls_to_adjust]
            # Add in new API call and subcalls
            plan.api_calls.append(
                {
                    TelemetryField.NAME.value: f"DataFrame.{func.__name__}",
                    TelemetryField.KEY_SUBCALLS.value: subcalls,
                    **resource_usage_collector.get_resource_usage(),
                }
            )
        elif plan is not None:
            plan.api_calls.append(
                {
                    TelemetryField.NAME.value: f"DataFrame.{func.__name__}",
                    **resource_usage_collector.get_resource_usage(),
                }
            )
        return r

    return wrap


def df_to_relational_group_df_api_usage(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        with ResourceUsageCollector() as resource_usage_collector:
            r = func(*args, **kwargs)
        r._df_api_call = {
            TelemetryField.NAME.value: f"DataFrame.{func.__name__}",
            **resource_usage_collector.get_resource_usage(),
        }
        return r

    return wrap


# For relational-grouped dataframe
def relational_group_df_api_usage(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        with ResourceUsageCollector() as resource_usage_collector:
            r = func(*args, **kwargs)
        plan = r._select_statement or r._plan
        if args[0]._df_api_call:
            plan.api_calls.append(args[0]._df_api_call)
        plan.api_calls.append(
            {
                TelemetryField.NAME.value: f"RelationalGroupedDataFrame.{func.__name__}",
                **resource_usage_collector.get_resource_usage(),
            }
        )
        return r

    return wrap


def get_plan_telemetry_metrics(plan: SnowflakePlan) -> Dict[str, Any]:
    data = {}
    try:
        data[CompilationStageTelemetryField.PLAN_UUID.value] = plan.uuid
        # plan state
        plan_state = plan.plan_state
        data[CompilationStageTelemetryField.QUERY_PLAN_HEIGHT.value] = plan_state[
            PlanState.PLAN_HEIGHT
        ]
        data[
            CompilationStageTelemetryField.QUERY_PLAN_NUM_SELECTS_WITH_COMPLEXITY_MERGED.value
        ] = plan_state[PlanState.NUM_SELECTS_WITH_COMPLEXITY_MERGED]
        data[
            CompilationStageTelemetryField.QUERY_PLAN_NUM_DUPLICATE_NODES.value
        ] = plan_state[PlanState.NUM_CTE_NODES]
        data[
            CompilationStageTelemetryField.QUERY_PLAN_DUPLICATED_NODE_COMPLEXITY_DISTRIBUTION.value
        ] = plan_state[PlanState.DUPLICATED_NODE_COMPLEXITY_DISTRIBUTION]

        # plan complexity score
        data[CompilationStageTelemetryField.QUERY_PLAN_COMPLEXITY.value] = {
            key.value: value for key, value in plan.cumulative_node_complexity.items()
        }
        data[
            CompilationStageTelemetryField.COMPLEXITY_SCORE_BEFORE_COMPILATION.value
        ] = get_complexity_score(plan)
    except Exception as e:
        data[CompilationStageTelemetryField.ERROR_MESSAGE.value] = str(e)

    return data


class TelemetryClient:
    def __init__(self, conn: SnowflakeConnection) -> None:
        self.telemetry: PCTelemetryClient = (
            None if is_in_stored_procedure() else conn._telemetry
        )
        self.source: str = get_application_name()
        self.version: str = get_version()
        self.python_version: str = get_python_version()
        self.os: str = get_os_name()
        self.is_interactive = is_interactive()

    def send(self, msg: Dict, timestamp: Optional[int] = None):
        if self.telemetry:
            if not timestamp:
                timestamp = get_time_millis()
            telemetry_data = PCTelemetryData(message=msg, timestamp=timestamp)
            self.telemetry.try_add_log_to_batch(telemetry_data)

    def _create_basic_telemetry_data(self, telemetry_type: str) -> Dict[str, Any]:
        message = {
            PCTelemetryField.KEY_SOURCE.value: self.source,
            TelemetryField.KEY_VERSION.value: self.version,
            TelemetryField.KEY_PYTHON_VERSION.value: self.python_version,
            TelemetryField.KEY_OS.value: self.os,
            PCTelemetryField.KEY_TYPE.value: telemetry_type,
            TelemetryField.KEY_IS_INTERACTIVE.value: PCTelemetryData.TRUE
            if self.is_interactive
            else PCTelemetryData.FALSE,
        }
        return message

    @safe_telemetry
    def send_session_created_telemetry(self, created_by_snowpark: bool):
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_SESSION_CREATED.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.KEY_START_TIME.value: get_time_millis(),
                TelemetryField.KEY_CREATED_BY_SNOWPARK.value: PCTelemetryData.TRUE
                if created_by_snowpark
                else PCTelemetryData.FALSE,
            },
        }
        self.send(message)

    @safe_telemetry
    def send_performance_telemetry(
        self, category: str, func_name: str, duration: float, sfqid: str = None
    ):
        """
        Sends performance telemetry data.

        Parameters:
            category (str): The category of the telemetry (upload file or data source).
            func_name (str): The name of the function.
            duration (float): The duration of the operation.
            sfqid (str, optional): The SFQID for upload file category. Defaults to None.
        """
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_PERFORMANCE_DATA.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.KEY_CATEGORY.value: category,
                TelemetryField.KEY_FUNC_NAME.value: func_name,
                TelemetryField.KEY_DURATION.value: duration,
                TelemetryField.THREAD_IDENTIFIER.value: threading.get_ident(),
                **({PCTelemetryField.KEY_SFQID.value: sfqid} if sfqid else {}),
            },
        }
        self.send(message)

    @safe_telemetry
    def send_upload_file_perf_telemetry(
        self, func_name: str, duration: float, sfqid: str
    ):
        self.send_performance_telemetry(
            category=TelemetryField.PERF_CAT_UPLOAD_FILE.value,
            func_name=func_name,
            duration=duration,
            sfqid=sfqid,
        )

    @safe_telemetry
    def send_data_source_perf_telemetry(self, telemetry_json_string: dict):
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_PERFORMANCE_DATA.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.KEY_CATEGORY.value: TelemetryField.PERF_CAT_DATA_SOURCE.value,
                TelemetryField.MESSAGE.value: json.dumps(telemetry_json_string),
            },
        }
        self.send(message)

    @safe_telemetry
    def send_function_usage_telemetry(
        self,
        func_name: str,
        function_category: str,
        api_calls: Optional[List[str]] = None,
        sfqids: Optional[List[str]] = None,
    ):
        data = {
            TelemetryField.KEY_FUNC_NAME.value: func_name,
            TelemetryField.KEY_CATEGORY.value: function_category,
        }
        if api_calls is not None:
            data[TelemetryField.KEY_API_CALLS.value] = api_calls
        if sfqids is not None:
            data[TelemetryField.KEY_SFQIDS.value] = sfqids
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_FUNCTION_USAGE.value
            ),
            TelemetryField.KEY_DATA.value: data,
        }
        self.send(message)

    def send_alias_in_join_telemetry(self):
        self.send_function_usage_telemetry(
            "name_alias_in_join", TelemetryField.FUNC_CAT_JOIN.value
        )

    def send_copy_pattern_telemetry(self):
        self.send_function_usage_telemetry(
            "copy_pattern", TelemetryField.FUNC_CAT_COPY.value
        )

    def send_sql_simplifier_telemetry(
        self, session_id: str, sql_simplifier_enabled: bool
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_SQL_SIMPLIFIER_ENABLED.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.SQL_SIMPLIFIER_ENABLED.value: sql_simplifier_enabled,
            },
        }
        self.send(message)

    def send_cte_optimization_telemetry(self, session_id: str) -> None:
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_CTE_OPTIMIZATION_ENABLED.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.CTE_OPTIMIZATION_ENABLED.value: True,
            },
        }
        self.send(message)

    def send_eliminate_numeric_sql_value_cast_telemetry(
        self, session_id: str, value: bool
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.TYPE_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED.value: value,
            },
        }
        self.send(message)

    def send_auto_clean_up_temp_table_telemetry(
        self, session_id: str, value: bool
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_AUTO_CLEAN_UP_TEMP_TABLE_ENABLED.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.TYPE_AUTO_CLEAN_UP_TEMP_TABLE_ENABLED.value: value,
            },
        }
        self.send(message)

    def send_large_query_breakdown_telemetry(
        self, session_id: str, value: bool
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_LARGE_QUERY_BREAKDOWN_ENABLED.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.LARGE_QUERY_BREAKDOWN_ENABLED.value: value,
            },
        }
        self.send(message)

    def send_query_compilation_summary_telemetry(
        self,
        session_id: int,
        plan_uuid: str,
        compilation_stage_summary: Dict[str, Any],
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                CompilationStageTelemetryField.TYPE_COMPILATION_STAGE_STATISTICS.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.KEY_CATEGORY.value: CompilationStageTelemetryField.CAT_COMPILATION_STAGE_STATS.value,
                CompilationStageTelemetryField.PLAN_UUID.value: plan_uuid,
                **compilation_stage_summary,
            },
        }
        self.send(message)

    def send_query_compilation_stage_failed_telemetry(
        self, session_id: int, plan_uuid: str, error_type: str, error_message: str
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                CompilationStageTelemetryField.TYPE_COMPILATION_STAGE_STATISTICS.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.KEY_CATEGORY.value: CompilationStageTelemetryField.CAT_COMPILATION_STAGE_ERROR.value,
                CompilationStageTelemetryField.PLAN_UUID.value: plan_uuid,
                CompilationStageTelemetryField.ERROR_TYPE.value: error_type,
                CompilationStageTelemetryField.ERROR_MESSAGE.value: error_message,
            },
        }
        self.send(message)

    def send_plan_metrics_telemetry(
        self, session_id: int, data: Dict[str, Any]
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                CompilationStageTelemetryField.TYPE_COMPILATION_STAGE_STATISTICS.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.KEY_CATEGORY.value: CompilationStageTelemetryField.CAT_SNOWFLAKE_PLAN_METRICS.value,
                **data,
            },
        }
        self.send(message)

    def send_temp_table_cleanup_telemetry(
        self,
        session_id: str,
        temp_table_cleaner_enabled: bool,
        num_temp_tables_cleaned: int,
        num_temp_tables_created: int,
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_TEMP_TABLE_CLEANUP.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.TEMP_TABLE_CLEANER_ENABLED.value: temp_table_cleaner_enabled,
                TelemetryField.NUM_TEMP_TABLES_CLEANED.value: num_temp_tables_cleaned,
                TelemetryField.NUM_TEMP_TABLES_CREATED.value: num_temp_tables_created,
            },
        }
        self.send(message)

    def send_temp_table_cleanup_abnormal_exception_telemetry(
        self,
        session_id: str,
        table_name: str,
        exception_message: str,
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_TEMP_TABLE_CLEANUP.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.TEMP_TABLE_CLEANUP_ABNORMAL_EXCEPTION_TABLE_NAME.value: table_name,
                TelemetryField.TEMP_TABLE_CLEANUP_ABNORMAL_EXCEPTION_MESSAGE.value: exception_message,
            },
        }
        self.send(message)

    def send_large_query_breakdown_update_complexity_bounds(
        self, session_id: int, lower_bound: int, upper_bound: int
    ):
        message = {
            **self._create_basic_telemetry_data(
                CompilationStageTelemetryField.TYPE_LARGE_QUERY_BREAKDOWN_UPDATE_COMPLEXITY_BOUNDS.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.KEY_DATA.value: {
                    CompilationStageTelemetryField.COMPLEXITY_SCORE_BOUNDS.value: (
                        lower_bound,
                        upper_bound,
                    ),
                },
            },
        }
        self.send(message)

    def send_cursor_created_telemetry(self, session_id: int, thread_id: int):
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_CURSOR_CREATED.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.THREAD_IDENTIFIER.value: thread_id,
            },
        }
        self.send(message)

    def send_reduce_describe_query_telemetry(
        self, session_id: str, value: bool
    ) -> None:
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_REDUCE_DESCRIBE_QUERY_ENABLED.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                TelemetryField.TYPE_REDUCE_DESCRIBE_QUERY_ENABLED.value: value,
            },
        }
        self.send(message)

    def send_describe_query_details(
        self,
        session_id: int,
        sql_text: str,
        e2e_time: float,
        stack_trace: Optional[List[Optional[str]]],
    ):
        message = {
            **self._create_basic_telemetry_data(
                DescribeQueryTelemetryField.TYPE_DESCRIBE_QUERY_DETAILS.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.SESSION_ID.value: session_id,
                DescribeQueryTelemetryField.SQL_TEXT.value: sql_text,
                DescribeQueryTelemetryField.E2E_TIME.value: e2e_time,
                DescribeQueryTelemetryField.STACK_TRACE.value: stack_trace,
            },
        }
        self.send(message)
