#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import functools
from enum import Enum, unique
from typing import Any, Dict, List, Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.telemetry import (
    TelemetryClient as PCTelemetryClient,
    TelemetryData as PCTelemetryData,
    TelemetryField as PCTelemetryField,
)
from snowflake.connector.time_util import get_time_millis
from snowflake.snowpark._internal.utils import (
    get_application_name,
    get_os_name,
    get_python_version,
    get_version,
    is_in_stored_procedure,
)


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
    TYPE_SQL_SIMPLIFIER_ENABLED = "snowpark_sql_simplifier_enabled"
    TYPE_CTE_OPTIMIZATION_ENABLED = "snowpark_cte_optimization_enabled"
    # telemetry for optimization that eliminates the extra cast expression generated for expressions
    TYPE_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED = (
        "snowpark_eliminate_numeric_sql_value_cast_enabled"
    )
    TYPE_ERROR = "snowpark_error"
    # Message keys for telemetry
    KEY_START_TIME = "start_time"
    KEY_DURATION = "duration"
    KEY_FUNC_NAME = "func_name"
    KEY_MSG = "msg"
    KEY_ERROR_MSG = "error_msg"
    KEY_VERSION = "version"
    KEY_PYTHON_VERSION = "python_version"
    KEY_CLIENT_LANGUAGE = "client_language"
    KEY_OS = "operating_system"
    KEY_DATA = "data"
    KEY_CATEGORY = "category"
    KEY_CREATED_BY_SNOWPARK = "created_by_snowpark"
    KEY_API_CALLS = "api_calls"
    KEY_SFQIDS = "sfqids"
    KEY_SUBCALLS = "subcalls"
    # function categories
    FUNC_CAT_ACTION = "action"
    FUNC_CAT_USAGE = "usage"
    FUNC_CAT_JOIN = "join"
    FUNC_CAT_COPY = "copy"
    FUNC_CAT_CREATE = "create"
    # performance categories
    PERF_CAT_UPLOAD_FILE = "upload_file"
    # sql simplifier
    SESSION_ID = "session_id"
    SQL_SIMPLIFIER_ENABLED = "sql_simplifier_enabled"
    CTE_OPTIMIZATION_ENABLED = "cte_optimization_enabled"
    # dataframe query stats
    QUERY_PLAN_HEIGHT = "query_plan_height"
    QUERY_PLAN_NUM_DUPLICATE_NODES = "query_plan_num_duplicate_nodes"
    QUERY_PLAN_COMPLEXITY = "query_plan_complexity"


# These DataFrame APIs call other DataFrame APIs
# and so we remove those API calls and move them
# inside the original API call
API_CALLS_TO_ADJUST = {
    "to_df": 1,
    "select_expr": 1,
    "drop": 1,
    "agg": 2,
    "distinct": 2,
    "with_column": 1,
    "with_columns": 1,
    "with_column_renamed": 1,
}
APIS_WITH_MULTIPLE_CALLS = list(API_CALLS_TO_ADJUST.keys())


# Adjust API calls into subcalls for certain APIs that call other APIs
def adjust_api_subcalls(
    df,
    func_name: str,
    len_subcalls: Optional[int] = None,
    precalls: Optional[List[Dict]] = None,
    subcalls: Optional[List[Dict]] = None,
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


def add_api_call(df, func_name: str) -> None:
    plan = df._select_statement or df._plan
    plan.api_calls.append({TelemetryField.NAME.value: func_name})


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
        with args[0]._session.query_history() as query_history:
            result = func(*args, **kwargs)
        plan = args[0]._select_statement or args[0]._plan
        api_calls = [
            *plan.api_calls,
            {TelemetryField.NAME.value: f"DataFrame.{func.__name__}"},
        ]
        # The first api call will indicate following:
        # - sql simplifier is enabled.
        # - height of the query plan
        # - number of unique duplicate subtrees in the query plan
        api_calls[0][TelemetryField.SQL_SIMPLIFIER_ENABLED.value] = args[
            0
        ]._session.sql_simplifier_enabled
        try:
            api_calls[0][TelemetryField.QUERY_PLAN_HEIGHT.value] = plan.plan_height
            api_calls[0][
                TelemetryField.QUERY_PLAN_NUM_DUPLICATE_NODES.value
            ] = plan.num_duplicate_nodes
            api_calls[0][TelemetryField.QUERY_PLAN_COMPLEXITY.value] = {
                key.value: value
                for key, value in plan.cumulative_node_complexity.items()
            }
        except Exception:
            pass
        args[0]._session._conn._telemetry_client.send_function_usage_telemetry(
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
        with args[0]._dataframe._session.query_history() as query_history:
            result = func(*args, **kwargs)
        plan = args[0]._dataframe._select_statement or args[0]._dataframe._plan
        api_calls = [
            *plan.api_calls,
            {TelemetryField.NAME.value: f"DataFrameWriter.{func.__name__}"},
        ]
        args[
            0
        ]._dataframe._session._conn._telemetry_client.send_function_usage_telemetry(
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
                }
            )
        else:
            plan.api_calls.append(
                {TelemetryField.NAME.value: f"DataFrame.{func.__name__}"}
            )
        return r

    return wrap


def df_to_relational_group_df_api_usage(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        r = func(*args, **kwargs)
        r._df_api_call = {TelemetryField.NAME.value: f"DataFrame.{func.__name__}"}
        return r

    return wrap


# For relational-grouped dataframe
def relational_group_df_api_usage(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        r = func(*args, **kwargs)
        plan = r._select_statement or r._plan
        if args[0]._df_api_call:
            plan.api_calls.append(args[0]._df_api_call)
        plan.api_calls.append(
            {TelemetryField.NAME.value: f"RelationalGroupedDataFrame.{func.__name__}"}
        )
        return r

    return wrap


class TelemetryClient:
    def __init__(self, conn: SnowflakeConnection) -> None:
        self.telemetry: PCTelemetryClient = (
            None if is_in_stored_procedure() else conn._telemetry
        )
        self.source: str = get_application_name()
        self.version: str = get_version()
        self.python_version: str = get_python_version()
        self.os: str = get_os_name()

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
    def send_upload_file_perf_telemetry(
        self, func_name: str, duration: float, sfqid: str
    ):
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_PERFORMANCE_DATA.value
            ),
            TelemetryField.KEY_DATA.value: {
                PCTelemetryField.KEY_SFQID.value: sfqid,
                TelemetryField.KEY_CATEGORY.value: TelemetryField.PERF_CAT_UPLOAD_FILE.value,
                TelemetryField.KEY_FUNC_NAME.value: func_name,
                TelemetryField.KEY_DURATION.value: duration,
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
                TelemetryField.SQL_SIMPLIFIER_ENABLED.value: True,
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
