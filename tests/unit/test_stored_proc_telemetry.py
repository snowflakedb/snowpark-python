#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import threading
from unittest.mock import patch, MagicMock
from typing import Dict
import json

from snowflake.snowpark._internal.telemetry import TelemetryClient
from snowflake.snowpark import Session


class MockGauge:
    def __init__(self, name: str, unit: str = "", description: str = "") -> None:
        self.name = name
        self.unit = unit
        self.description = description
        self._value = None

    def set(self, value):
        self._value = value
        return self

    def get_value(self):
        return self._value


class MockMeter:
    def __init__(self) -> None:
        self._instrument_id_instrument: Dict[str, MockGauge] = {}
        self._instrument_id_instrument_lock = threading.Lock()

    def create_gauge(self, name: str, unit: str = "", description: str = ""):
        instrument = MockGauge(
            name,
            unit,
            description,
        )
        instrument_id = f"{name},{unit},{description}"

        with self._instrument_id_instrument_lock:
            self._instrument_id_instrument[instrument_id] = instrument
            return instrument


@patch("snowflake.snowpark._internal.telemetry.is_in_stored_procedure")
def test_telemetry_client_with_mock_meter(mock_is_in_stored_proc):
    mock_is_in_stored_proc.return_value = True
    mock_meter = MockMeter()
    mock_conn = MagicMock()
    client = TelemetryClient(mock_conn)
    client._enabled = True
    client.telemetry = None
    client.stored_proc_meter = mock_meter
    client.clean_up_stored_proc_meter_interval = 200
    test_message = {"test": "data", "func_name": "test_function"}
    client.send(test_message)
    assert len(mock_meter._instrument_id_instrument) == 1
    gauge = list(mock_meter._instrument_id_instrument.values())[0]
    assert "snowflake.snowpark.client.gauge" in gauge.name
    assert gauge.unit == "data"
    assert gauge.description == json.dumps(
        test_message, ensure_ascii=False, separators=(",", ":")
    )
    assert gauge.get_value() == 200


@patch("snowflake.snowpark._internal.telemetry.is_in_stored_procedure")
def test_telemetry_client_multiple_sends(mock_is_in_stored_proc):
    mock_is_in_stored_proc.return_value = True
    mock_meter = MockMeter()
    mock_conn = MagicMock()
    client = TelemetryClient(mock_conn)
    client._enabled = True
    client.telemetry = None
    client.stored_proc_meter = mock_meter
    client.clean_up_stored_proc_meter_interval = 200
    for i in range(100):
        client.send({"test": f"data_{i}"})
    assert len(mock_meter._instrument_id_instrument) == 100
    gauges = list(mock_meter._instrument_id_instrument.values())
    for gauge in gauges:
        assert "snowflake.snowpark.client.gauge" in gauge.name
        assert gauge.get_value() == 200


@patch("snowflake.snowpark._internal.telemetry.is_in_stored_procedure")
def test_telemetry_client_cleanup(mock_is_in_stored_proc):
    """Test TelemetryClient cleanup mechanism"""
    mock_is_in_stored_proc.return_value = True
    mock_meter = MockMeter()
    mock_conn = MagicMock()
    client = TelemetryClient(mock_conn)
    client._enabled = True
    client.telemetry = None
    client.stored_proc_meter = mock_meter
    client.clean_up_stored_proc_meter_interval = 100
    for i in range(client.clean_up_stored_proc_meter_interval):
        client.send({"test": f"data_{i}"})
    assert len(mock_meter._instrument_id_instrument) == 0
    client.send({"test": "data_after_cleanup"})
    assert len(mock_meter._instrument_id_instrument) == 1


@patch("snowflake.snowpark.session.is_in_stored_procedure")
@patch("snowflake.snowpark.session._logger")
def test_internal_telemetry_disabled(mock_logger, mock_is_in_stored_proc):
    """Test that Session logs debug message when telemetry is enabled but stored proc telemetry is disabled"""
    mock_is_in_stored_proc.return_value = True
    mock_session = MagicMock(spec=Session)
    mock_session._stored_proc_telemetry_enabled = False
    mock_session._conn = MagicMock()
    mock_session._conn._conn = MagicMock()
    mock_session._conn._telemetry_client = MagicMock()
    Session.telemetry_enabled.fset(mock_session, True)
    mock_logger.debug.assert_called_once_with(
        "Client side parameter ENABLE_SNOWPARK_FIRST_PARTY_TELEMETRY is set to False, telemetry could not be enabled"
    )


@patch("snowflake.snowpark._internal.telemetry.is_in_stored_procedure")
def test_telemetry_client_internal_metrics_import_fails(mock_is_in_stored_proc):
    """Test TelemetryClient handles ImportError when _snowflake.internal_metrics import fails"""
    mock_is_in_stored_proc.return_value = True
    mock_conn = MagicMock()
    with patch(
        "builtins.__import__", side_effect=ImportError("No module named '_snowflake'")
    ):
        client = TelemetryClient(mock_conn)
        assert client is not None
        assert client._enabled is True
        assert client.stored_proc_meter is None
        test_message = {"test": "data", "func_name": "test_function"}
        client.send(test_message)


@patch("snowflake.snowpark._internal.telemetry.is_in_stored_procedure")
def test_telemetry_client_disabled(mock_is_in_stored_proc):
    """Test that no message is sent when telemetry client is not enabled"""
    mock_is_in_stored_proc.return_value = True
    mock_meter = MockMeter()
    mock_conn = MagicMock()
    client = TelemetryClient(mock_conn)
    client._enabled = False
    client.telemetry = None
    client.stored_proc_meter = mock_meter
    client.clean_up_stored_proc_meter_interval = 200
    test_message = {"test": "data", "func_name": "test_function"}
    client.send(test_message)
    assert len(mock_meter._instrument_id_instrument) == 0
