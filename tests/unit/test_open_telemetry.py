#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import inspect
import os
import time
from unittest import mock

import pytest

import snowflake.snowpark.session

from snowflake.snowpark._internal.server_connection import ServerConnection
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter


class DictExporter(SpanExporter):
    def __init__(self):
        self.exported_spans = {}

    def export(self, spans) -> None:
        for span in spans:
            self.exported_spans[span.name] = {
                "name": span.name,
                "attributes": dict(span.attributes),
            }


def test_open_telemetry_span_from_dataframe_writer_and_dataframe():
    # set up exporter
    resource = Resource(attributes={SERVICE_NAME: "snowpark-python-open-telemetry"})
    trace_provider = TracerProvider(resource=resource)
    dict_exporter = DictExporter()
    processor = BatchSpanProcessor(dict_exporter)
    trace_provider.add_span_processor(processor)
    trace.set_tracer_provider(trace_provider)

    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    # test dataframe writer
    df.write.mode("overwrite").save_as_table("saved_table", table_type="temporary")
    lineno = str(inspect.currentframe().f_lineno - 1)
    # wait for open telemetry to capture the span
    while "DataFrameWriter.save_as_table" not in dict_exporter.exported_spans:
        time.sleep(1)
    span = dict_exporter.exported_spans["DataFrameWriter.save_as_table"]
    assert span["attributes"]["method.chain"] == "DataFrame.to_df().save_as_table()"
    assert os.path.basename(span["attributes"]["code.filepath"]) == "test_open_telemetry.py"
    assert span["attributes"]["code.lineno"] == lineno

    # test from dataframe
    df.collect()
    lineno = str(inspect.currentframe().f_lineno - 1)
    # wait for open telemetry to capture the span
    while "DataFrame.collect" not in dict_exporter.exported_spans:
        time.sleep(1)
    span = dict_exporter.exported_spans["DataFrame.collect"]
    assert span["attributes"]["method.chain"] == "DataFrame.to_df().collect()"
    assert os.path.basename(span["attributes"]["code.filepath"]) == "test_open_telemetry.py"
    assert span["attributes"]["code.lineno"] == lineno

