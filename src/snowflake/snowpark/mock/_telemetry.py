#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import atexit
import json
import logging
import os
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from snowflake.connector.compat import OK
from snowflake.connector.secret_detector import SecretDetector
from snowflake.connector.telemetry_oob import TelemetryService
from snowflake.snowpark._internal.utils import (
    get_os_name,
    get_python_version,
    get_version,
)

from .exceptions import SnowparkLocalTestingException

REQUESTS_AVAILABLE = True
try:
    # by default in stored procedure requests is not imported
    import requests
except ImportError:
    REQUESTS_AVAILABLE = False

# 3 seconds setting in the connector oob could be too short that the oob service is unable to handle the request,
# 5 seconds is more tolerant
REQUEST_TIMEOUT = 5

logger = logging.getLogger(__name__)

OS_VERSION = get_os_name()
PYTHON_VERSION = get_python_version()
SNOWPARK_PYTHON_VERSION = get_version()

TELEMETRY_VALUE_SNOWPARK_EVENT_TYPE = "Snowpark Python"
TELEMETRY_KEY_TYPE = "Type"
TELEMETRY_KEY_LOWER_TYPE = "type"
TELEMETRY_KEY_UUID = "UUID"
TELEMETRY_KEY_CREATED_ON = "Created_on"
TELEMETRY_KEY_MESSAGE = "Message"
TELEMETRY_KEY_TAGS = "Tags"
TELEMETRY_KEY_PROPERTIES = "properties"
TELEMETRY_KEY_SNOWPARK_VERSION = "Snowpark_Version"
TELEMETRY_KEY_OS_VERSION = "OS_Version"
TELEMETRY_KEY_PYTHON_VERSION = "Python_Version"
TELEMETRY_KEY_EVENT_TYPE = "Event_type"
TELEMETRY_KEY_CONN_UUID = "Connection_UUID"
TELEMETRY_KEY_FEATURE_NAME = "feature_name"
TELEMETRY_KEY_PARAMETERS_INFO = "parameters_info"
TELEMETRY_KEY_ERROR_MESSAGE = "error_message"
TELEMETRY_KEY_IS_INTERNAL = "is_internal"


def generate_base_oob_telemetry_data_dict(
    connection_uuid: Optional[str] = None,
) -> dict:
    return {
        TELEMETRY_KEY_TYPE: TELEMETRY_VALUE_SNOWPARK_EVENT_TYPE,
        TELEMETRY_KEY_UUID: str(uuid.uuid4()),
        TELEMETRY_KEY_CREATED_ON: str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        TELEMETRY_KEY_MESSAGE: {TELEMETRY_KEY_CONN_UUID: connection_uuid},
        TELEMETRY_KEY_TAGS: {
            TELEMETRY_KEY_SNOWPARK_VERSION: SNOWPARK_PYTHON_VERSION,
            TELEMETRY_KEY_OS_VERSION: OS_VERSION,
            TELEMETRY_KEY_PYTHON_VERSION: PYTHON_VERSION,
        },
        TELEMETRY_KEY_PROPERTIES: {
            TELEMETRY_KEY_LOWER_TYPE: TELEMETRY_VALUE_SNOWPARK_EVENT_TYPE
        },
    }


class LocalTestTelemetryEventType(Enum):
    UNSUPPORTED = "unsupported"
    SUPPORTED = "supported"
    SESSION_CONNECTION = "session"


class LocalTestOOBTelemetryService(TelemetryService):
    PROD = "https://client-telemetry.snowflakecomputing.com/enqueue"

    def __init__(self) -> None:
        super().__init__()
        self._is_internal_usage = bool(
            os.getenv("SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY", False)
        )
        self._deployment_url = self.PROD
        self._enable = True

    def _upload_payload(self, payload) -> None:
        if not REQUESTS_AVAILABLE:
            logger.debug(
                "request module is not available",
            )
            return
        success = True
        response = None
        try:
            with requests.Session() as session:
                response = session.post(
                    self._deployment_url,
                    data=payload,
                    timeout=REQUEST_TIMEOUT,
                    headers={"Content-type": "application/json"},
                )
                if (
                    response.status_code == OK
                    and json.loads(response.text).get("statusCode", 0) == OK
                ):
                    logger.debug(
                        "telemetry server request success: %d", response.status_code
                    )
                else:
                    logger.debug(
                        "telemetry server request error: %d", response.status_code
                    )
                    success = False
        except Exception as e:
            logger.debug(
                "Telemetry request failed, Exception response: %s, exception: %s",
                response,
                str(e),
            )
            success = False
        finally:
            logger.debug("Telemetry request success=%s", success)

    def add(self, event) -> None:
        """Adds a telemetry event to the queue."""
        if not self.enabled:
            return

        self.queue.put(event)
        if self.queue.qsize() > self.batch_size:
            payload = self.export_queue_to_string()
            if payload is None:
                return
            self._upload_payload(payload)

    @property
    def enabled(self) -> bool:
        """Whether the Telemetry service is enabled or not."""
        return self._enabled

    def enable(self) -> None:
        """Enable Telemetry Service."""
        self._enabled = True

    def disable(self) -> None:
        """Disable Telemetry Service."""
        self._enabled = False

    def export_queue_to_string(self):
        logs = list()
        while not self.queue.empty():
            logs.append(self.queue.get())
        # We may get an exception trying to serialize a python object to JSON
        try:
            payload = json.dumps(logs)
        except Exception:
            logger.debug(
                "Failed to generate a JSON dump from the passed in telemetry OOB events. String representation of "
                "logs: %s " % str(logs),
                exc_info=True,
            )
            payload = None
        _, masked_text, _ = SecretDetector.mask_secrets(payload)
        return masked_text

    def log_session_creation(self, connection_uuid: Optional[str] = None):
        try:
            telemetry_data = generate_base_oob_telemetry_data_dict(
                connection_uuid=connection_uuid
            )
            telemetry_data[TELEMETRY_KEY_TAGS][
                TELEMETRY_KEY_EVENT_TYPE
            ] = LocalTestTelemetryEventType.SESSION_CONNECTION.value
            telemetry_data[TELEMETRY_KEY_MESSAGE][TELEMETRY_KEY_IS_INTERNAL] = (
                1 if self._is_internal_usage else 0
            )
            self.add(telemetry_data)
        except Exception:
            logger.debug("Failed to log session creation", exc_info=True)

    def log_not_supported_error(
        self,
        external_feature_name: Optional[str] = None,
        internal_feature_name: Optional[str] = None,
        parameters_info: Optional[dict] = None,
        error_message: Optional[str] = None,
        connection_uuid: Optional[str] = None,
        raise_error: Optional[type] = None,
        warning_logger: Optional[logging.Logger] = None,
    ):
        if not external_feature_name and not error_message:
            raise ValueError(
                "At least one of external_feature_name or"
                " error_message should be provided to raise user facing error"
            )

        error_message = f"[Local Testing] {error_message or f'{external_feature_name} is not supported.'}"
        try:
            telemetry_data = generate_base_oob_telemetry_data_dict(
                connection_uuid=connection_uuid
            )
            telemetry_data[TELEMETRY_KEY_TAGS][
                TELEMETRY_KEY_EVENT_TYPE
            ] = LocalTestTelemetryEventType.UNSUPPORTED.value
            telemetry_data[TELEMETRY_KEY_MESSAGE][TELEMETRY_KEY_IS_INTERNAL] = (
                1 if self._is_internal_usage else 0
            )
            telemetry_data[TELEMETRY_KEY_MESSAGE][TELEMETRY_KEY_FEATURE_NAME] = (
                internal_feature_name or external_feature_name
            )
            telemetry_data[TELEMETRY_KEY_MESSAGE][
                TELEMETRY_KEY_PARAMETERS_INFO
            ] = parameters_info
            self.add(telemetry_data)
        except Exception:
            logger.debug(
                "[Local Testing] Failed to log not supported feature call",
                exc_info=True,
            )

        if warning_logger:
            warning_logger.warning(error_message)
        if raise_error:
            if raise_error in (NotImplementedError, SnowparkLocalTestingException):
                raise raise_error(error_message)
            else:
                SnowparkLocalTestingException.raise_from_error(
                    raise_error(error_message), error_message
                )


atexit.register(LocalTestOOBTelemetryService.get_instance().close)
