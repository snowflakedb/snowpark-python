#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import functools
import inspect
import os
from unittest import mock

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import snowflake.snowpark.session
from snowflake.snowpark._internal.open_telemetry import (
    build_method_chain,
    decorator_count,
)
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.functions import udf
from snowflake.snowpark.session import _add_session
from snowflake.snowpark.types import IntegerType


def spans_to_dict(spans):
    res = {}
    for span in spans:
        res[span.name] = span
    return res


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

resource = Resource(attributes={SERVICE_NAME: "snowpark-python-open-telemetry"})
trace_provider = TracerProvider(resource=resource)
dict_exporter = InMemorySpanExporter()
processor = SimpleSpanProcessor(dict_exporter)
trace_provider.add_span_processor(processor)
trace.set_tracer_provider(trace_provider)


def test_register_udf_from_file():
    test_file = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "../resources",
            "test_udf_dir",
            "test_udf_file.py",
        )
    )
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()

    session.udf.register_from_file(
        test_file,
        "mod5",
        name="mod5_function",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        immutable=True,
    )
    lineno = inspect.currentframe().f_lineno - 8
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register_from_file" in spans
    span = spans["register_from_file"]
    print(span.attributes)
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["function.name"] == "mod5_function"
    assert span.attributes["function.handler_name"] == "mod5"


def test_inline_register_udf():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    _add_session(session)
    session._conn._telemetry_client = mock.MagicMock()
    # test register with udf.register

    def add_udf(x: int, y: int) -> int:
        return x + y

    session.udf.register(
        add_udf,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
        stage_location="@test_stage",
        name="add",
    )
    lineno = inspect.currentframe().f_lineno - 3
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register" in spans
    span = spans["register"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["function.name"] == "add"
    assert span.attributes["function.handler_name"] == "add_udf"

    # test register with decorator @udf
    @udf(name="minus")
    def minus_udf(x: int, y: int) -> int:
        return x - y

    lineno = inspect.currentframe().f_lineno - 3
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register" in spans
    span = spans["register"]
    print(span.attributes)
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["function.name"] == "minus"
    assert span.attributes["function.handler_name"] == "minus_udf"


def test_open_telemetry_span_from_dataframe_writer_and_dataframe():
    # set up exporter
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df.write.mode("overwrite").save_as_table("saved_table", table_type="temporary")
    lineno = inspect.currentframe().f_lineno - 1

    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "save_as_table" in spans
    span = spans["save_as_table"]
    assert span.attributes["method.chain"] == "DataFrame.to_df().save_as_table()"
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    dict_exporter.clear()


def test_open_telemetry_span_from_dataframe_writer():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df.collect()
    lineno = inspect.currentframe().f_lineno - 1

    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "collect" in spans
    span = spans["collect"]
    assert span.attributes["method.chain"] == "DataFrame.to_df().collect()"
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno


def test_decorator_count():
    decorator_number1 = decorator_count(dummy_function1)
    decorator_number2 = decorator_count(dummy_function2)
    assert decorator_number1 == 1
    assert decorator_number2 == 0


def test_build_method_chain():
    method_chain = build_method_chain(api_calls, "DataFrame.collect")
    assert method_chain == "DataFrame.to_df().collect()"
