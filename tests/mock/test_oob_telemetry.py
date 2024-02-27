#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import uuid

from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService


def test_unit_connection_telemetry():
    oob_service = LocalTestOOBTelemetryService.get_instance()
    connection_uuid = str(uuid.uuid4())
    oob_service.log_session_creation()
    oob_service.log_session_creation(connection_uuid=connection_uuid)
    assert oob_service.size() == 2
