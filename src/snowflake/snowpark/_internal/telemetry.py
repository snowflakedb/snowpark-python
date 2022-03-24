#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import functools
from datetime import datetime
from enum import Enum, unique
from typing import Any, Dict

from snowflake.connector import SnowflakeConnection
from snowflake.connector.telemetry import (
    TelemetryClient as PCTelemetryClient,
    TelemetryData as PCTelemetryData,
    TelemetryField as PCTelemetryField,
)
from snowflake.snowpark import DataFrame, DataFrameWriter
from snowflake.snowpark._internal.utils import Utils


@unique
class TelemetryField(Enum):
    # constants
    START_TIME: str = "start_time"
    MESSAGE: str = "message"
    NAME: str = "name"
    ERROR_CODE: str = "error_code"
    STACK_TRACE: str = "stack_trace"
    # Types of telemetry
    TYPE_PERFORMANCE_DATA = "snowpark_performance_data"
    TYPE_FUNCTION_USAGE = "snowpark_function_usage"
    TYPE_SESSION_CREATED = "snowpark_session_created"
    TYPE_ERROR = "snowpark_error"
    # Message keys for telemetry
    KEY_DURATION = "duration"
    KEY_FUNC_NAME = "func_name"
    KEY_MSG = "msg"
    KEY_VERSION = "version"
    KEY_PYTHON_VERSION = "python_version"
    KEY_CLIENT_LANGUAGE = "client_language"
    KEY_OS = "operating_system"
    KEY_DATA = "data"
    KEY_CATEGORY = "category"
    # function categories
    CAT_ACTION = "action"
    CAT_USAGE = "usage"
    CAT_JOIN = "join"
    CAT_COPY = "copy"


def safe_telemetry(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception:
            # We don't really care if telemetry fails, just want to be safe for the user
            pass

    return wrap


def action_telemetry(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        result = func(*args, **kwargs)
        obj = args[0]
        if isinstance(obj, DataFrame):
            session = obj.session
        elif isinstance(obj, DataFrameWriter):
            session = obj._dataframe.session
        else:
            # Skip telemetry if we can't tell what the parent object is
            return result
        session._conn._telemetry_client.send_function_usage_telemetry(
            f"action_{func.__name__}", TelemetryField.CAT_ACTION.value
        )
        return result

    return wrap


def usage_telemetry(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        result = func(*args, **kwargs)
        obj = args[0]
        if isinstance(obj, DataFrame):
            session = obj.session
        elif isinstance(obj, DataFrameWriter):
            session = obj._dataframe.session
        else:
            # Skip telemetry if we can't tell what the parent object is
            return result
        session._conn._telemetry_client.send_function_usage_telemetry(
            f"usage_{func.__name__}", TelemetryField.CAT_USAGE.value
        )
        return result

    return wrap


class TelemetryClient:
    def __init__(self, conn: SnowflakeConnection):
        self.telemetry: PCTelemetryClient = conn._telemetry
        self.source: str = Utils.get_application_name()
        self.version: str = Utils.get_version()
        self.python_version: str = Utils.get_python_version()
        self.os: str = Utils.get_os_name()

    def send(self, msg, timestamp=None):
        if not timestamp:
            timestamp = datetime.now()
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
    def send_performance_telemetry(
        self, func_name: str, duration: float, msg: str, sfqid: str
    ):
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_PERFORMANCE_DATA.value
            ),
            PCTelemetryField.KEY_SFQID.value: sfqid,
            TelemetryField.KEY_FUNC_NAME.value: func_name,
            TelemetryField.KEY_DURATION.value: duration,
        }
        self.send(message)

    @safe_telemetry
    def send_function_usage_telemetry(self, func_name: str, function_category: str):
        message = {
            **self._create_basic_telemetry_data(
                TelemetryField.TYPE_FUNCTION_USAGE.value
            ),
            TelemetryField.KEY_DATA.value: {
                TelemetryField.KEY_FUNC_NAME.value: func_name,
                TelemetryField.KEY_CATEGORY.value: function_category,
            },
        }
        self.send(message)

    def send_alias_in_join_telemetry(self):
        self.send_function_usage_telemetry(
            "name_alias_in_join", TelemetryField.CAT_JOIN.value
        )

    def send_copy_pattern_telemetry(self):
        self.send_function_usage_telemetry(
            "copy_pattern", TelemetryField.CAT_COPY.value
        )
