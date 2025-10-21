#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import threading
from unittest.mock import patch, MagicMock
import pytest

from snowflake.snowpark._internal.event_table_telemetry import EventTableTelemetry
from tests.utils import RUNNING_ON_GH

try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource  # noqa: F401
    from opentelemetry.sdk.trace import TracerProvider  # noqa: F401
    from opentelemetry.sdk.trace.export import (  # noqa: F401
        BatchSpanProcessor,
        SpanExportResult,
    )  # noqa: F401
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry._logs import set_logger_provider  # noqa: F401
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler  # noqa: F401
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor  # noqa: F401
    from opentelemetry.exporter.otlp.proto.http._log_exporter import (
        OTLPLogExporter,
    )
    from opentelemetry.exporter.otlp.proto.http import Compression  # noqa: F401
    from opentelemetry.sdk._logs._internal.export import LogExportResult

    dependencies_missing = False
except Exception:
    dependencies_missing = True

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="feature not available in local testing",
    ),
    pytest.mark.skipif(
        RUNNING_ON_GH,
        reason="tests only suppose to run on snowfort",
    ),
    pytest.mark.skipif(
        dependencies_missing,
        reason="opentelemetry is not installed",
    ),
]

mock_tracer_results = []
mock_log_results = []
lock = threading.RLock()


class MockOTLPLogExporter(OTLPLogExporter):
    def export(self, batch):
        if self._shutdown:
            return LogExportResult.FAILURE

        with lock:
            mock_log_results.extend(batch)
            return LogExportResult.SUCCESS


class MockOTLPSpanExporter(OTLPSpanExporter):
    def export(self, batch):
        if self._shutdown:
            return SpanExportResult.FAILURE
        with lock:
            mock_tracer_results.extend(batch)
            return SpanExportResult.SUCCESS


class FakeAttestation:
    def __init__(self) -> None:
        self.credential = "mock_cred"


# mock exporter
def make_mock_trace_exporter(*args, **kwargs):
    exporter = MockOTLPSpanExporter(*args, **kwargs)
    return exporter


def make_mock_log_exporter(*args, **kwargs):
    exporter = MockOTLPLogExporter(*args, **kwargs)
    return exporter


def create_mock_response(current_endpoint):
    # mock authentication
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.text = current_endpoint
    return fake_response


@pytest.fixture(scope="module", autouse=True)
def mock_session(session):
    session.connection.auth_class.provider = "mock_provider"
    session.connection.auth_class.entra_resource = "mock_resource"
    session.connection.auth_class.token = "mock_token"
    return session


def test_end_to_end(session):
    external_telemetry = session.client_telemetry

    mock_response = create_mock_response("https://fake_endpoint")

    # test with mock exporter and authentication
    with (
        patch(
            "snowflake.snowpark._internal.external_telemetry.create_attestation",
            return_value=FakeAttestation(),
        ),
        patch("requests.get", return_value=mock_response),
        patch(
            "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
            side_effect=make_mock_trace_exporter,
        ),
        patch(
            "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
            side_effect=make_mock_log_exporter,
        ),
    ):
        # out of scope trace and log
        tracer = trace.get_tracer("external_telemetry")
        with tracer.start_as_current_span("code_store") as span:
            span.set_attribute("code.pos", "before_enable")
            logging.info("log before enable")

        assert len(mock_tracer_results) == 0
        assert len(mock_log_results) == 0

        external_telemetry.enable_event_table_telemetry_collection(
            "db.sc.tb", logging.INFO, True
        )

        # in scope trace and log
        tracer = trace.get_tracer("external_telemetry")
        with tracer.start_as_current_span("code_store") as span:
            span.set_attribute("code.lineno", "21")
            span.set_attribute("code.content", "session.sql(...)")
            logging.info("Trace being sent to event table")
            logging.info("second log recorded")

        external_telemetry.disable_event_table_telemetry_collection()
        # force batch processor to send telemetry
        external_telemetry._proxy_tracer_provider.force_flush(1000)
        external_telemetry._proxy_log_provider.force_flush(1000)
        assert mock_tracer_results[0].attributes == {
            "code.lineno": "21",
            "code.content": "session.sql(...)",
        }
        assert mock_log_results[0].log_record.body == "Trace being sent to event table"
        assert mock_log_results[1].log_record.body == "second log recorded"

        # clean up after disable
        mock_log_results.clear()
        mock_tracer_results.clear()

        # out of scope trace and log
        with tracer.start_as_current_span("code_store") as span:
            span.set_attribute("code.pos", "after_enable")
            logging.info("log after enable")

        assert len(mock_tracer_results) == 0
        assert len(mock_log_results) == 0

        # re-enable external telemetry
        external_telemetry.enable_event_table_telemetry_collection(
            "db.sc.tb", logging.INFO, True
        )

        tracer = trace.get_tracer("external_telemetry")
        with tracer.start_as_current_span("code_store") as span:
            span.set_attribute("code.lineno", "21")
            span.set_attribute("code.content", "session.sql(...)")
            logging.info("Trace being sent to event table")
            logging.info("second log recorded")

        # force batch processor to send telemetry
        external_telemetry._proxy_tracer_provider.force_flush(1000)
        external_telemetry._proxy_log_provider.force_flush(1000)
        assert mock_tracer_results[0].attributes == {
            "code.lineno": "21",
            "code.content": "session.sql(...)",
        }
        assert mock_log_results[0].log_record.body == "Trace being sent to event table"
        assert mock_log_results[1].log_record.body == "second log recorded"


def test_negative_case(session, caplog):
    external_telemetry = EventTableTelemetry(session)
    external_telemetry.enable_event_table_telemetry_collection("db.sc.tb", None, False)
    assert (
        "Snowpark python log_level and trace_level are not enabled to collect telemetry into event table:"
        in caplog.text
    )

    external_telemetry.enable_event_table_telemetry_collection(
        "no_fully_qualified", logging.INFO, True
    )
    assert "Input event table is converted to fully qualified name:" in caplog.text

    with patch(
        "snowflake.snowpark._internal.external_telemetry.installed_opentelemetry", False
    ):
        external_telemetry.enable_event_table_telemetry_collection(
            "db.sc.tb", logging.INFO, True
        )
        assert (
            "Opentelemetry dependencies are missing, no telemetry export into event table:"
            in caplog.text
        )
