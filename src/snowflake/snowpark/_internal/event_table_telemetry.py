#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import importlib
import logging
from abc import ABC
from logging import getLogger
from typing import Dict, Optional, Tuple
from snowflake.connector.options import MissingOptionalDependency, ModuleLikeObject

import snowflake.snowpark
import requests

from snowflake.snowpark._internal.utils import parse_table_name

_logger = getLogger(__name__)

DEFAULT_EVENT_TABLE = "snowflake.telemetry.events"
SERVICE_NAME = "snow.snowpark.client"


class MissingOpenTelemetry(MissingOptionalDependency):
    _dep_name = "opentelemetry"


def _import_or_missing_opentelemetry() -> Tuple[ModuleLikeObject, bool]:
    try:
        opentelemetry = importlib.import_module("opentelemetry")
        importlib.import_module("opentelemetry.sdk")
        importlib.import_module("opentelemetry.exporter.otlp")
        importlib.import_module("opentelemetry._logs")
        return opentelemetry, True
    except ImportError:
        return MissingOpenTelemetry(), False


opentelemetry, installed_opentelemetry = _import_or_missing_opentelemetry()

BaseLogProvider = opentelemetry._logs.LoggerProvider if installed_opentelemetry else ABC
BaseTraceProvider = (
    opentelemetry.trace.TracerProvider if installed_opentelemetry else ABC
)
Attributes = opentelemetry.util.types.Attributes if installed_opentelemetry else ABC


class RetryWithTokenRefreshAdapter(requests.adapters.HTTPAdapter):
    def __init__(
        self,
        session_instance: "snowflake.snowpark.Session",
        header: Dict,
        max_retries: int = 3,
    ) -> None:
        super().__init__()
        self.snowpark_session = session_instance
        self.max_retries = max_retries
        self.header = header
        self.retryable_status_code = [401]

    def send(self, request, **kwargs):
        """Send request with retry logic and token refresh on failure"""
        for attempt in range(self.max_retries + 1):
            try:
                request.headers.update(self.header)

                response = super().send(request, **kwargs)

                # If successful, return the response
                if (
                    response.status_code in self.retryable_status_code
                    and attempt < self.max_retries
                ):
                    self.header = (
                        self.snowpark_session._get_external_telemetry_auth_token()
                    )
                    continue
                else:
                    return response

            except (requests.exceptions.RequestException, Exception) as e:
                if attempt < self.max_retries:
                    self.header = (
                        self.snowpark_session._get_external_telemetry_auth_token()
                    )
                    continue
                else:
                    # Re-raise the exception if we've exhausted retries
                    raise e


class ProxyTracerProvider(BaseTraceProvider):
    def __init__(self, real_provider=None) -> None:
        super().__init__()
        self._real_provider = real_provider
        self._enabled = real_provider is not None

    def set_real_provider(self, provider):
        self._real_provider = provider
        self._enabled = provider is not None

    def disable(self):
        self._enabled = False

    def enable(self):
        self._enabled = True

    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Attributes] = None,
    ) -> "opentelemetry.trace.Tracer":
        if self._enabled and self._real_provider:
            return self._real_provider.get_tracer(
                instrumenting_module_name,
                instrumenting_library_version,
                schema_url,
                attributes,
            )
        else:
            # Return a no-op tracer when disabled
            return opentelemetry.trace.NoOpTracer()

    def shutdown(self):
        if self._real_provider:
            self._real_provider.shutdown()
        self._real_provider = None
        self._enabled = False

    # Delegate span processor methods to real provider
    def add_span_processor(self, processor):
        if self._real_provider:
            self._real_provider.add_span_processor(processor)

    def force_flush(self, timeout_millis=None):
        if self._real_provider:
            self._real_provider.force_flush(timeout_millis)


class ProxyLogProvider(BaseLogProvider):
    def __init__(self, real_provider=None) -> None:
        super().__init__()
        self._real_provider = real_provider
        self._enabled = real_provider is not None

    def set_real_provider(self, provider):
        self._real_provider = provider
        self._enabled = provider is not None

    def disable(self):
        self._enabled = False

    def enable(self):
        self._enabled = True

    def get_logger(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Attributes] = None,
    ) -> "opentelemetry._logs.Logger":
        if self._enabled and self._real_provider:
            return self._real_provider.get_logger(
                instrumenting_module_name,
                instrumenting_library_version,
                schema_url,
                attributes,
            )
        else:
            # Return a no-op logger when disabled
            return opentelemetry._logs.NoOpLogger(name="noop")

    def shutdown(self):
        if self._real_provider:
            self._real_provider.shutdown()
        self._real_provider = None
        self._enabled = False

    def add_log_record_processor(self, processor):
        if self._real_provider:
            self._real_provider.add_log_record_processor(processor)

    def force_flush(self, timeout_millis=None):
        if self._real_provider:
            self._real_provider.force_flush(timeout_millis)


class EventTableTelemetry:
    def __init__(self, session: "snowflake.snowpark.Session") -> None:
        self._tracer_provider = None
        self._span_processor = None
        self._proxy_tracer_provider = None
        self._proxy_log_provider = None
        self._logger_provider = None
        self._log_processor = None
        self._log_handler = None
        self._attestation = None
        self._event_table = None
        self._tracer_provider_enabled = False
        self._logger_provider_enabled = False
        self.session = session

    def enable_event_table_telemetry_collection(
        self,
        event_table: str = DEFAULT_EVENT_TABLE,
        log_level: int = None,
        enable_trace_level: bool = False,
    ) -> None:
        """
        Enable user to send telemetry to designated event table when necessary dependencies are installed.

        Only traces and logs between `client_telemetry.enable_event_table_telemetry_collection` and
        `client_telemetry.disable_event_table_telemetry_collection` will be sent to event table.
        You can call `client_telemetry.enable_event_table_telemetry_collection` again to re-enable external
        telemetry after it is turned off.

        Note:
            This function requires the `opentelemetry` extra from Snowpark.
            Install it via pip:
                .. code-block:: bash

                pip install "snowflake-snowpark-python[opentelemetry]"

        Examples 1
            .. code-block:: python

            ext = session.client_telemetry
            ext.enable_event_table_telemetry_collection("snowflake.telemetry.events", logging.INFO, True)
            tracer = trace.get_tracer("my_tracer")
            with tracer.start_as_current_span("code_store") as span:
                span.set_attribute("code.lineno", "21")
                span.set_attribute("code.content", "session.sql(...)")
                logging.info("Trace being sent to event table")
            ext.disable_event_table_telemetry_collection()

        Examples 2
            .. code-block:: python

            ext = session.client_telemetry
            logging.info("log before enable event table telemetry collection") # this log is not sent to event table
            ext.enable_event_table_telemetry_collection("snowflake.telemetry.events", logging.INFO, True)
            tracer = trace.get_tracer("my_tracer")
            with tracer.start_as_current_span("code_store") as span:
                span.set_attribute("code.lineno", "21")
                span.set_attribute("code.content", "session.sql(...)")
                logging.info("Trace being sent to event table")
            ext.disable_event_table_telemetry_collection()
            logging.info("out of scope log")  # this log is not sent to event table
            ext.enable_event_table_telemetry_collection("snowflake.telemetry.events", logging.DEBUG, True)
            logging.debug("debug log") # this log is sent to event table because event table telemetry collection is re-enabled
            ext.disable_event_table_telemetry_collection()

        """
        if not installed_opentelemetry:
            _logger.debug(
                f"Opentelemetry dependencies are missing, no telemetry export into event table: {event_table}"
            )
            return

        if log_level is None and not enable_trace_level:
            _logger.warning(
                f"Snowpark python log_level and trace_level are not enabled to collect telemetry into event table: {event_table}."
            )
            return

        if len(parse_table_name(event_table)) != 3:
            event_table = self.session.get_fully_qualified_name_if_possible(event_table)
            _logger.warning(
                f"Input event table is converted to fully qualified name: {event_table}."
            )

        self._event_table = event_table

        try:

            url = f"https://{self.session.connection.host}:{self.session.connection.port}/observability/event-table/hostname"
            response = requests.get(
                url, headers=self._get_external_telemetry_auth_token()
            )
            if response.status_code != 200:
                response.raise_for_status()
            endpoint = response.text
        except Exception as e:
            _logger.debug(
                f"failed to acquire event table endpoint with:{str(e)}, no external telemetry will be collected"
            )
            return

        resource = opentelemetry.sdk.resources.Resource.create(
            {"service.name": SERVICE_NAME}
        )

        header = self._get_external_telemetry_auth_token()

        if enable_trace_level and self._proxy_tracer_provider is None:
            self._init_trace_level(endpoint, header, resource)
        elif (
            enable_trace_level
            and self._proxy_tracer_provider
            and not self._tracer_provider_enabled
        ):
            self._enable_tracer_provider()

        if log_level is not None and self._log_handler is None:
            self._init_log_level(endpoint, header, resource, log_level)
        elif (
            log_level is not None
            and self._logger_provider
            and not self._logger_provider_enabled
        ):
            self._enable_logger_provider()

    def disable_event_table_telemetry_collection(self) -> None:
        if self._tracer_provider:
            self._disable_tracer_provider()

        if self._logger_provider:
            self._disable_logger_provider()

    def _get_external_telemetry_auth_token(self) -> Dict:
        from snowflake.connector.wif_util import create_attestation

        self._attestation = create_attestation(
            self.session.connection.auth_class.provider,
            self.session.connection.auth_class.entra_resource,
            self.session.connection.auth_class.token,
            session_manager=(
                self.session.connection._session_manager.clone(max_retries=0)
                if self.session.connection
                else None
            ),
        )
        headers = {
            "Authorization": f"Bearer WIF.AWS.{self._attestation.credential}",
        }
        if self._event_table is not None:
            headers["event-table"] = self._event_table

        return headers

    def _disable_tracer_provider(self) -> None:
        if self._proxy_tracer_provider and self._tracer_provider_enabled:
            self._proxy_tracer_provider.disable()
            self._tracer_provider_enabled = False

    def _enable_tracer_provider(self) -> None:
        # Clear the batch processor's internal queue so that span collected during disable is not exported
        if self._span_processor._batch_processor and hasattr(
            self._span_processor._batch_processor, "_queue"
        ):
            self._span_processor._batch_processor._queue.clear()
        if self._proxy_tracer_provider and not self._tracer_provider_enabled:
            self._proxy_tracer_provider.enable()
            self._tracer_provider_enabled = True

    def _disable_logger_provider(self) -> None:
        if self._proxy_log_provider and self._logger_provider_enabled:
            self._proxy_log_provider.disable()
            self._logger_provider_enabled = False

    def _enable_logger_provider(self) -> None:
        if self._log_processor._batch_processor and hasattr(
            self._log_processor._batch_processor, "_queue"
        ):
            self._log_processor._batch_processor._queue.clear()
        if self._proxy_log_provider and not self._logger_provider_enabled:
            self._proxy_log_provider.enable()
            self._logger_provider_enabled = True

    def _opentelemetry_shutdown(self) -> None:
        if self._span_processor is not None:
            self._span_processor.shutdown()
        if self._log_processor is not None:
            self._log_processor.shutdown()
        if self._tracer_provider is not None:
            self._proxy_tracer_provider.shutdown()
        if self._logger_provider is not None:
            self._proxy_log_provider.shutdown()

    def _init_trace_level(
        self,
        endpoint: str,
        header: dict,
        resource: "opentelemetry.sdk.resources.Resource",
    ) -> None:
        url = f"https://{endpoint}/v1/traces"

        self._proxy_tracer_provider = ProxyTracerProvider()
        opentelemetry.trace.set_tracer_provider(self._proxy_tracer_provider)

        self._tracer_provider = opentelemetry.sdk.trace.TracerProvider(
            resource=resource
        )

        trace_session = requests.Session()
        trace_session.headers.update(header)
        trace_session.mount(
            "https://", RetryWithTokenRefreshAdapter(self.session, header)
        )
        trace_session.mount(
            "http://", RetryWithTokenRefreshAdapter(self.session, header)
        )

        exporter = (
            opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter(
                endpoint=url, session=trace_session
            )
        )
        self._span_processor = opentelemetry.sdk.trace.export.BatchSpanProcessor(
            exporter
        )
        self._tracer_provider.add_span_processor(self._span_processor)
        self._proxy_tracer_provider.set_real_provider(self._tracer_provider)
        self._proxy_tracer_provider.enable()

        self._tracer_provider_enabled = True

    def _init_log_level(
        self,
        endpoint: str,
        header: dict,
        resource: "opentelemetry.sdk.resources.Resource",
        log_level: int,
    ) -> None:
        url = f"https://{endpoint}/v1/logs"
        self._proxy_log_provider = ProxyLogProvider()
        opentelemetry._logs.set_logger_provider(self._proxy_log_provider)

        self._logger_provider = opentelemetry.sdk._logs.LoggerProvider(
            resource=resource
        )

        log_session = requests.Session()
        log_session.headers.update(header)
        log_session.mount(
            "https://", RetryWithTokenRefreshAdapter(self.session, header)
        )
        log_session.mount("http://", RetryWithTokenRefreshAdapter(self.session, header))

        exporter = opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter(
            endpoint=url, session=log_session
        )
        self._log_processor = opentelemetry.sdk._logs.export.BatchLogRecordProcessor(
            exporter
        )
        self._logger_provider.add_log_record_processor(self._log_processor)

        self._proxy_log_provider.set_real_provider(self._logger_provider)
        self._proxy_log_provider.enable()

        self._log_handler = opentelemetry.sdk._logs.LoggingHandler(
            logger_provider=self._proxy_log_provider, level=log_level
        )
        logging.getLogger().addHandler(self._log_handler)

        self._logger_provider_enabled = True
