#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import threading
from unittest.mock import patch, MagicMock
import pytest

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

mock_tracer_results = {}
mock_log_results = {}
lock = threading.RLock()


class MockOTLPLogExporter(OTLPLogExporter):
    def export(self, batch):
        if self._shutdown:
            return LogExportResult.FAILURE

        with lock:
            if self._endpoint in mock_log_results:
                mock_log_results[self._endpoint].extend(batch)
            else:
                mock_log_results[self._endpoint] = batch
            return LogExportResult.SUCCESS


class MockOTLPSpanExporter(OTLPSpanExporter):
    def export(self, batch):
        if self._shutdown:
            return LogExportResult.FAILURE
        with lock:
            if self._endpoint in mock_tracer_results:
                mock_tracer_results[self._endpoint].extend(batch)
            else:
                mock_tracer_results[self._endpoint] = batch
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


def test_basic_end_to_end(session, request):
    current_endpoint = request.node.name
    log_endpoint = f"https://{current_endpoint}/v1/logs"
    trace_endpoint = f"https://{current_endpoint}/v1/traces"

    mock_response = create_mock_response(current_endpoint)

    # test with mock exporter and authentication
    with (
        patch(
            "snowflake.snowpark.session.create_attestation",
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
        session.enable_external_telemetry("db.sc.tb", logging.INFO, True)

        tracer = trace.get_tracer("external_telemetry")
        with tracer.start_as_current_span("code_store") as span:
            span.set_attribute("code.lineno", "21")
            span.set_attribute("code.content", "session.sql(...)")
            logging.info("Trace being sent to event table")
            logging.info("second log recorded")

        session.disable_external_telemetry()
    # force batch processor to send telemetry
    session._proxy_tracer_provider.force_flush(1000)
    session._proxy_log_provider.force_flush(1000)
    assert mock_tracer_results[trace_endpoint][0].attributes == {
        "code.lineno": "21",
        "code.content": "session.sql(...)",
    }
    assert (
        mock_log_results[log_endpoint][0].log_record.body
        == "Trace being sent to event table"
    )
    assert mock_log_results[log_endpoint][1].log_record.body == "second log recorded"


def test_send_telemetry_out_of_scope(session, request):
    current_endpoint = request.node.name
    log_endpoint = f"https://{current_endpoint}/v1/logs"
    trace_endpoint = f"https://{current_endpoint}/v1/traces"

    # mock authentication
    mock_response = create_mock_response(current_endpoint)

    # test with mock exporter and authentication
    with (
        patch(
            "snowflake.snowpark.session.create_attestation",
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
        tracer = trace.get_tracer("external_telemetry")
        with tracer.start_as_current_span("code_store") as span:
            span.set_attribute("code.pos", "before_enable")
            logging.info("log before enable")
        session.enable_external_telemetry("db.sc.tb", logging.INFO, True)
        session.disable_external_telemetry()
        with tracer.start_as_current_span("code_store") as span:
            span.set_attribute("code.pos", "after_enable")
            logging.info("log after enable")
    # force batch processor to send telemetry
    session._proxy_tracer_provider.force_flush(1000)
    session._proxy_log_provider.force_flush(1000)

    assert trace_endpoint not in mock_tracer_results
    assert log_endpoint not in mock_log_results


def test_re_enable(session, request):
    current_endpoint = request.node.name
    log_endpoint = f"https://{current_endpoint}/v1/logs"
    trace_endpoint = f"https://{current_endpoint}/v1/traces"

    mock_response = create_mock_response(current_endpoint)

    # test with mock exporter and authentication
    with (
        patch(
            "snowflake.snowpark.session.create_attestation",
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
        session.enable_external_telemetry("db.sc.tb", logging.INFO, True)
        session.disable_external_telemetry()
        session.enable_external_telemetry("db.sc.tb", logging.INFO, True)
        tracer = trace.get_tracer("external_telemetry")
        with tracer.start_as_current_span("code_store") as span:
            span.set_attribute("code.lineno", "21")
            span.set_attribute("code.content", "session.sql(...)")
            logging.info("Trace being sent to event table")
            logging.info("second log recorded")

    # force batch processor to send telemetry
    session._proxy_tracer_provider.force_flush(1000)
    session._proxy_log_provider.force_flush(1000)
    assert mock_tracer_results[trace_endpoint][0].attributes == {
        "code.lineno": "21",
        "code.content": "session.sql(...)",
    }
    assert (
        mock_log_results[log_endpoint][0].log_record.body
        == "Trace being sent to event table"
    )
    assert mock_log_results[log_endpoint][1].log_record.body == "second log recorded"


def test_negative_case(session, caplog):
    session.enable_external_telemetry("db.sc.tb", None, False)
    assert (
        "Snowpark python log_level and trace_level are not enabled to collect telemetry into event table:"
        in caplog.text
    )

    session.enable_external_telemetry("no_fully_qualified", logging.INFO, True)
    assert "Input event table is converted to fully qualified name:" in caplog.text

    with patch("snowflake.snowpark.session.installed_opentelemery", False):
        session.enable_external_telemetry("db.sc.tb", logging.INFO, True)
        assert (
            "Opentelemetry dependencies are missing, no telemetry export into event table:"
            in caplog.text
        )
