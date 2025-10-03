#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, Optional

from opentelemetry.trace import TracerProvider, NoOpTracer
from opentelemetry._logs import LoggerProvider, NoOpLogger
from opentelemetry.util.types import Attributes
import snowflake.snowpark
import requests
import opentelemetry


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


class ProxyTracerProvider(TracerProvider):
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
            return NoOpTracer()

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


class ProxyLogProvider(LoggerProvider):
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
    ) -> "opentelemetry._log.Logger":
        if self._enabled and self._real_provider:
            return self._real_provider.get_logger(
                instrumenting_module_name,
                instrumenting_library_version,
                schema_url,
                attributes,
            )
        else:
            # Return a no-op logger when disabled
            return NoOpLogger()

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
