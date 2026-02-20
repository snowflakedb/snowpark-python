#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import functools

import pytest

from snowflake.snowpark._internal.open_telemetry import (
    build_method_chain,
    decorator_count,
)
from snowflake.snowpark._internal import event_table_telemetry as ett

from ..conftest import opentelemetry_installed

pytestmark = [
    pytest.mark.udf,
    pytest.mark.skipif(
        not opentelemetry_installed,
        reason="opentelemetry is not installed",
    ),
]


def dummy_decorator(func):
    @functools.wraps(func)
    def wrapper(*arg, **kwarg):
        return func(*arg, **kwarg)

    return wrapper


@dummy_decorator
def dummy_function1():
    return


def dummy_function2():
    return


api_calls = [
    {"name": "Session.create_dataframe[values]"},
    {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
]


def test_decorator_count():
    decorator_number1 = decorator_count(dummy_function1)
    decorator_number2 = decorator_count(dummy_function2)
    assert decorator_number1 == 1
    assert decorator_number2 == 0


def test_build_method_chain():
    method_chain = build_method_chain(api_calls, "DataFrame.collect")
    assert method_chain == "DataFrame.to_df().collect()"


def test_snowflake_trace_id_generator_packs_timestamp_and_retries_invalid(monkeypatch):
    # This test is only meaningful when the full event-table telemetry otel deps are installed.
    if not ett.installed_opentelemetry:
        pytest.skip(
            "event-table telemetry opentelemetry dependencies are not installed"
        )

    from opentelemetry import trace

    # First attempt produces INVALID_TRACE_ID (all-zero bytes), second attempt succeeds.
    times = iter([0, 60])  # seconds since epoch -> minutes are 0 then 1
    suffixes = iter([0, int.from_bytes(b"\x11" * 12, "big")])

    monkeypatch.setattr(ett.time, "time", lambda: next(times))
    monkeypatch.setattr(ett.random, "getrandbits", lambda _n: next(suffixes))

    gen = ett.ForkedSnowflakeTraceIdGenerator()
    trace_id = gen.generate_trace_id()

    assert trace_id != trace.INVALID_TRACE_ID
    assert trace_id.to_bytes(16, byteorder="big", signed=False) == (
        (1).to_bytes(4, byteorder="big", signed=False) + (b"\x11" * 12)
    )
