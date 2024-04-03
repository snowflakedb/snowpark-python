#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import json
import logging
import re
import uuid
from datetime import datetime, timedelta

import pytest

from snowflake.snowpark._internal.utils import (
    get_os_name,
    get_python_version,
    get_version,
)
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService
from snowflake.snowpark.session import Session
from tests.utils import IS_IN_STORED_PROC

pytestmark = pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="OOB Telemetry not available in stored procedure"
)


def test_unit_oob_connection_telemetry(caplog, local_testing_telemetry_setup):
    oob_service = LocalTestOOBTelemetryService.get_instance()
    # clean up queue first
    oob_service.export_queue_to_string()
    with caplog.at_level(logging.DEBUG, logger="snowflake.snowpark.mock._telemetry"):
        connection_uuid = str(uuid.uuid4())
        oob_service.log_session_creation()
        assert oob_service.size() == 1
        oob_service.log_session_creation(connection_uuid=connection_uuid)
        assert oob_service.size() == 2
        payload = oob_service.export_queue_to_string()
        unpacked_payload = json.loads(payload)
        now_time = datetime.now().replace(microsecond=0)

        # test generated payload
        for idx, event in enumerate(unpacked_payload):
            assert len(event) == 6
            assert len(event["properties"]) == 1
            assert len(event["Message"]) == 2
            assert len(event["Tags"]) == 4
            assert event["Type"] == event["properties"]["type"] == "Snowpark Python"
            assert event["UUID"]
            assert datetime.strptime(
                event["Created_on"], "%Y-%m-%d %H:%M:%S"
            ) == pytest.approx(now_time, abs=timedelta(seconds=1))
            assert event["Message"]["Connection_UUID"] == (
                None if idx == 0 else connection_uuid
            )
            assert event["Message"]["is_internal"] == 1
            assert event["Tags"]["Snowpark_Version"] == get_version()
            assert event["Tags"]["OS_Version"] == get_os_name()
            assert event["Tags"]["Python_Version"] == get_python_version()
            assert event["Tags"]["Event_type"] == "session"
        assert oob_service.size() == 0
        assert not caplog.record_tuples

        # test sending successfully
        oob_service.log_session_creation()
        oob_service.log_session_creation(connection_uuid=connection_uuid)
        assert oob_service.size() == 2
        oob_service.flush()
        assert oob_service.size() == 0
        assert len(caplog.record_tuples) >= 2
        assert (
            "telemetry server request success: 200" in caplog.text
            and "Telemetry request success=True" in caplog.text
        )


def test_unit_oob_log_not_implemented_error(caplog, local_testing_telemetry_setup):
    oob_service = LocalTestOOBTelemetryService.get_instance()
    # clean up queue first
    oob_service.export_queue_to_string()
    connection_uuid = str(uuid.uuid4())
    logger = logging.getLogger("LocalTestLogger")

    unraise_feature_name = "Test Feature No Raise Error"
    with caplog.at_level(logging.WARNING, logger="LocalTestLogger"):
        # test basic case + warning
        oob_service.log_not_supported_error(
            external_feature_name=unraise_feature_name,
            warning_logger=logger,
        )
        assert (
            f"[Local Testing] {unraise_feature_name} is not supported." in caplog.text
        )
    assert oob_service.size() == 1

    # test raising error
    raise_feature_name = "Test Feature With Raise Error"
    with pytest.raises(
        NotImplementedError,
        match=re.escape(f"[Local Testing] {raise_feature_name} is not supported."),
    ):
        oob_service.log_not_supported_error(
            external_feature_name=raise_feature_name,
            internal_feature_name="module_a.function_b",
            parameters_info={"param_c": "value_d"},
            connection_uuid=connection_uuid,
            raise_error=NotImplementedError,
        )
    assert oob_service.size() == 2
    payload = oob_service.export_queue_to_string()
    unpacked_payload = json.loads(payload)
    now_time = datetime.now().replace(microsecond=0)

    # test generated payload
    for idx, event in enumerate(unpacked_payload):
        assert len(event) == 6
        assert len(event["properties"]) == 1
        assert len(event["Message"]) == 4
        assert len(event["Tags"]) == 4
        assert event["Type"] == event["properties"]["type"] == "Snowpark Python"
        assert event["UUID"]
        assert datetime.strptime(
            event["Created_on"], "%Y-%m-%d %H:%M:%S"
        ) == pytest.approx(now_time, abs=timedelta(seconds=1))
        assert event["Message"]["Connection_UUID"] == (
            None if idx == 0 else connection_uuid
        )
        assert event["Message"]["is_internal"] == 1
        assert event["Message"]["feature_name"] == (
            unraise_feature_name if idx == 0 else "module_a.function_b"
        )
        assert event["Message"]["parameters_info"] == (
            None if idx == 0 else {"param_c": "value_d"}
        )
        assert event["Tags"]["Snowpark_Version"] == get_version()
        assert event["Tags"]["OS_Version"] == get_os_name()
        assert event["Tags"]["Python_Version"] == get_python_version()
        assert event["Tags"]["Event_type"] == "unsupported"
    assert oob_service.size() == 0

    # test sending successfully
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="snowflake.snowpark.mock._telemetry"):
        oob_service.log_not_supported_error(
            external_feature_name=unraise_feature_name,
        )
        try:
            oob_service.log_not_supported_error(
                external_feature_name=raise_feature_name,
                internal_feature_name="module_a.function_b",
                parameters_info={"param_c": "value_d"},
                connection_uuid=connection_uuid,
                raise_error=NotImplementedError,
            )
        except Exception:
            pass
        assert oob_service.size() == 2
        oob_service.flush()
        assert oob_service.size() == 0
        assert len(caplog.record_tuples) == 2
        assert (
            "telemetry server request success: 200" in caplog.text
            and "Telemetry request success=True" in caplog.text
        )

    # test sending empty raise error
    with pytest.raises(ValueError):
        assert oob_service.log_not_supported_error()


def test_unit_connection(caplog, local_testing_telemetry_setup):
    # clean up queue first
    LocalTestOOBTelemetryService.get_instance().export_queue_to_string()
    conn = MockServerConnection()
    # creating a mock connection will send connection telemetry
    error_message = "Error Message"
    external_feature_name = "Test Feature"
    assert conn._oob_telemetry.get_instance().size() == 1
    with pytest.raises(
        NotImplementedError, match=re.escape(error_message)
    ), caplog.at_level(level=logging.DEBUG):
        conn.log_not_supported_error(
            external_feature_name=external_feature_name,
            error_message=error_message,
            internal_feature_name="module_a.function_b",
            parameters_info={"param_c": "value_d"},
            raise_error=NotImplementedError,
        )
    assert not caplog.record_tuples
    conn._oob_telemetry.flush()
    assert conn._oob_telemetry.get_instance().size() == 0


def test_unit_connection_disable_telemetry(caplog, local_testing_telemetry_setup):
    # clean up queue first
    LocalTestOOBTelemetryService.get_instance().export_queue_to_string()
    disabled_telemetry_conn = MockServerConnection(
        options={"disable_local_testing_telemetry": True}
    )
    # creating a mock connection will send connection telemetry, after disable, there should no telemetry event
    assert disabled_telemetry_conn._oob_telemetry.get_instance().size() == 0

    # test sending empty raise error
    with pytest.raises(ValueError):
        disabled_telemetry_conn.log_not_supported_error()

    # test error raised but no telemetry sent
    error_message = "Error Message"
    with pytest.raises(TypeError, match=re.escape(error_message)):
        disabled_telemetry_conn.log_not_supported_error(
            raise_error=TypeError, error_message=error_message
        )
    assert disabled_telemetry_conn._oob_telemetry.get_instance().size() == 0


def test_snowpark_telemetry(caplog, local_testing_telemetry_setup):
    # clean up queue first
    LocalTestOOBTelemetryService.get_instance().export_queue_to_string()
    with Session.builder.configs(options={"local_testing": True}).create() as session:
        assert session._conn._oob_telemetry.get_instance().size() == 1
        with pytest.raises(
            NotImplementedError,
            match=re.escape("[Local Testing] Session.sql is not supported."),
        ):
            session.sql("select 1")
            assert session._conn._oob_telemetry.get_instance().size() == 2
        payload = session._conn._oob_telemetry.export_queue_to_string()
        unpacked_payload = json.loads(payload)
        assert unpacked_payload[0]["Tags"]["Event_type"] == "session"
        assert unpacked_payload[1]["Tags"]["Event_type"] == "unsupported"

    # test sending successfully
    with caplog.at_level(logging.DEBUG, logger="snowflake.snowpark.mock._telemetry"):
        with Session.builder.configs(
            options={"local_testing": True}
        ).create() as session:
            assert session._conn._oob_telemetry.get_instance().size() == 1
            with pytest.raises(
                NotImplementedError,
                match=re.escape("[Local Testing] Session.sql is not supported."),
            ):
                session.sql("select 1")

            session._conn._oob_telemetry.flush()
            assert session._conn._oob_telemetry.get_instance().size() == 0
            assert (
                "telemetry server request success: 200" in caplog.text
                and "Telemetry request success=True" in caplog.text
            )
