#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import json
import logging
import uuid
from enum import Enum
from typing import List, Optional, Union

from snowflake.connector.compat import OK
from snowflake.connector.secret_detector import SecretDetector
from snowflake.connector.telemetry_oob import REQUEST_TIMEOUT, TelemetryService
from snowflake.connector.time_util import get_time_millis
from snowflake.connector.vendored import requests
from snowflake.snowpark._internal.utils import (
    get_os_name,
    get_python_version,
    get_version,
)

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


def generate_base_oob_telemetry_data_dict(
    connection_uuid: Optional[str] = None,
) -> dict:
    return {
        TELEMETRY_KEY_TYPE: TELEMETRY_VALUE_SNOWPARK_EVENT_TYPE,
        TELEMETRY_KEY_UUID: str(uuid.uuid4()),
        TELEMETRY_KEY_CREATED_ON: get_time_millis(),
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
    DEV = "https://client-telemetry.ordevmisc1.us-west-2.aws-dev.app.snowflake.com/enqueue"
    PROD = "https://client-telemetry.c1.us-west-2.aws.app.snowflake.com/enqueue"

    def __init__(self) -> None:
        super().__init__()
        self._deployment_url = self.PROD

    def _upload_payload(self, payload) -> None:
        success = True
        response = None
        try:
            with requests.Session() as session:
                response = session.post(
                    self._deployment_url,
                    data=payload,
                    timeout=REQUEST_TIMEOUT,
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

    def export_queue_to_string(self):
        logs = list()
        while not self._queue.empty():
            logs.append(self._queue.get())
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
        telemetry_data = generate_base_oob_telemetry_data_dict(
            connection_uuid=connection_uuid
        )
        telemetry_data[TELEMETRY_KEY_TAGS][
            TELEMETRY_KEY_EVENT_TYPE
        ] = LocalTestTelemetryEventType.SESSION_CONNECTION.value
        self.add(telemetry_data)

    def log_not_supported_feature_call(
        self,
        feature_name: str,
        parameter_names: Optional[Union[List[str], str]],
        additional_info: Optional[str] = None,
        connection_uuid: Optional[str] = None,
    ):
        telemetry_data = generate_base_oob_telemetry_data_dict(
            connection_uuid=connection_uuid
        )
        telemetry_data[TELEMETRY_KEY_TAGS][
            TELEMETRY_KEY_EVENT_TYPE
        ] = LocalTestTelemetryEventType.UNSUPPORTED.value
        # TODO: fill more details
        self.add(telemetry_data)

    def log_supported_feature_call(
        self,
        feature_name: str,
        parameter_names: Optional[Union[List[str], str]],
        additional_info: Optional[str] = None,
        connection_uuid: Optional[str] = None,
    ):
        telemetry_data = generate_base_oob_telemetry_data_dict(
            connection_uuid=connection_uuid
        )
        telemetry_data[TELEMETRY_KEY_TAGS][
            TELEMETRY_KEY_EVENT_TYPE
        ] = LocalTestTelemetryEventType.SUPPORTED.value
        # TODO: fill more details
        self.add(telemetry_data)
