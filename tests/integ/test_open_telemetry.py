#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import inspect
import os

import pytest
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

pytestmark = [
    pytest.mark.udf,
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="UDTF not supported in Local Testing",
    ),
]


def spans_to_dict(spans):
    res = {}
    for span in spans:
        res[span.name] = span
    return res


@pytest.fixture(scope="module")
def dict_exporter():
    resource = Resource(attributes={SERVICE_NAME: "snowpark-python-open-telemetry"})
    trace_provider = TracerProvider(resource=resource)
    dict_exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(dict_exporter)
    trace_provider.add_span_processor(processor)
    trace.set_tracer_provider(trace_provider)
    yield dict_exporter
    dict_exporter.shutdown()


def test_open_telemetry_in_table_stored_proc(session, dict_exporter):
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df._execute_and_get_query_id()
    lineno = inspect.currentframe().f_lineno - 1

    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "_execute_and_get_query_id" in spans
    span = spans["_execute_and_get_query_id"]
    assert (
        span.attributes["method.chain"]
        == "DataFrame.to_df()._execute_and_get_query_id()"
    )
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    dict_exporter.clear()


def test_open_telemetry_span_from_dataframe_writer_and_dataframe(
    session, dict_exporter
):
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


def test_open_telemetry_span_from_dataframe_writer(session, dict_exporter):
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
    dict_exporter.clear()
