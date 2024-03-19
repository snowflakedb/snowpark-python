#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
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
        self.exported_spans = []

    def export(self, spans) -> None:
        for span in spans:

            span_dict = {
                "name": span.name,
                "attributes": dict(span.attributes),
            }
            self.exported_spans.append(span_dict)


def test_open_telemetry_create(sql_simplifier_enabled):
    # initialize exporter, this is required to acquire span data
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
    df.write.mode("overwrite").save_as_table("saved_table", table_type="temporary")
    # wait for open telemetry to capture the span
    while len(dict_exporter.exported_spans) == 0:
        time.sleep(1)
    span = dict_exporter.exported_spans[0]
    assert span["attributes"]["method.chain"] == "Dataframe.to_df().save_as_table()"
    assert span["attributes"]["code.filepath"].split("/")[-1] == "test_open_telemetry.py"
    assert span["attributes"]["code.lineno"] == "47"
