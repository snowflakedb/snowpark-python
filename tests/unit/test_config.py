#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import logging
import pytest
import threading
import time

from dataclasses import replace
from unittest import mock

from snowflake.snowpark._internal.config import (
    Setting,
    SettingGroup,
    SessionParameter,
    VersionedSessionParameter,
    SettingStore,
)


def test_setting(caplog):
    setting = Setting("basic", "description", default=True, experimental_since="1.0.0")

    # Default value returned if value not set
    assert setting.value is True and setting._value is None

    # Changing the value to default does not emit a warning
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        setting.value = True
    assert caplog.text == ""

    # Changing to non-default value emits a warning
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        setting.value = False
    assert (
        "snowflake.snowpark:utils.py:762 Parameter basic is experimental since 1.0.0. Do not use it in production."
        in caplog.text
    )
    assert setting.value is False

    # Assigning invalid type throughs error
    with pytest.raises(
        ValueError, match="Value for parameter basic must be of type bool."
    ):
        setting.value = 1

    # Read only settings raise an error when written to
    ro_setting = replace(setting, read_only=True)
    with pytest.raises(
        RuntimeError,
        match="setting basic is read_only and cannot be changed at runtime.",
    ):
        ro_setting.value = False


def test_setting_gruop():
    setting = Setting("basic", "description")
    group = SettingGroup(
        "settings_group",
        default=1,
        settings=[setting],
    )

    # Value defaults to parent default or value
    assert setting.value == 1
    group.value = 2
    assert setting.value == group.value == 2

    # setting value is used if available
    setting.value = 3
    assert setting.value == 3 and group.value == 2


def test_session_parameter(caplog):
    mock_session = mock.Mock()
    mock_session._conn._thread_safe_session_enabled = True
    mock_session._session_id = "mock_id"
    mock_telemetry = mock.Mock()

    # Omitting required parameters fails
    with pytest.raises(
        ValueError, match="session is a required parameter for SessionParameter"
    ):
        SessionParameter("bad_param", default=True)
    with pytest.raises(
        ValueError, match="parameter_name is a required parameter for SessionParameter"
    ):
        SessionParameter("bad_param", session=mock_session, default=True)

    # First attempt to access parameter retrieves it from the connector
    param = SessionParameter(
        "param", session=mock_session, parameter_name="session_param", default=True
    )
    mock_session._conn._get_client_side_session_parameter.return_value = False
    assert param.value is False
    mock_session._conn._get_client_side_session_parameter.assert_called_with(
        "session_param", True
    )

    # Second attempt does not try to retrieve value
    mock_session._conn._get_client_side_session_parameter.reset_mock()
    assert param.value is False
    mock_session._conn._get_client_side_session_parameter.assert_not_called()

    # Changing the value emits a warning for thread safe mode
    caplog.clear()
    thread = threading.Thread(target=lambda: time.sleep(1))
    thread.start()
    with caplog.at_level(logging.WARNING):
        param.value = False
    assert (
        "You might have more than one threads sharing the Session object trying to update param."
        in caplog.text
    )
    thread.join()

    # Telemetry and synchronization ignored when not configured
    mock_telemetry.assert_not_called()
    mock_session._conn._cursor.execute.assert_not_called()

    # Telemetry sent when configured
    telemetry_param = replace(param, telemetry_hook=mock_telemetry)
    telemetry_param.value = False
    mock_telemetry.assert_called_with("mock_id", False)

    # Server side param synchronized when configured
    sync_param = replace(param, synchronize=True)
    sync_param.value = False
    mock_session._conn._cursor.execute.assert_called_with(
        "alter session set session_param = False"
    )

    # ro_param throws error on assignment
    ro_param = replace(param, read_only=True)
    with pytest.raises(
        RuntimeError, match="session parameter param cannot be changed at runtime."
    ):
        ro_param.value = False


def test_versioned_session_param():
    mock_session = mock.Mock()
    mock_session.version = "1.0.0"
    mock_session._lock = threading.Lock()

    # When parameter is missing return false
    param1 = VersionedSessionParameter(
        "param", session=mock_session, parameter_name="session_param"
    )
    mock_session._conn._get_client_side_session_parameter.return_value = ""
    assert param1.value is False

    # When version is newer return false
    param2 = VersionedSessionParameter(
        "param", session=mock_session, parameter_name="session_param"
    )
    mock_session._conn._get_client_side_session_parameter.return_value = "99.99.99"
    assert param2.value is False

    # When version is compatible return True
    param3 = VersionedSessionParameter(
        "param", session=mock_session, parameter_name="session_param"
    )
    mock_session._conn._get_client_side_session_parameter.return_value = "0.0.1"
    assert param3.value is True


def test_setting_store():
    parent_config = SettingStore([Setting("parent_setting", default=1)])
    child_config = SettingStore(
        [Setting("child_setting", default=10)], extend_from=parent_config
    )

    # Parent setting is available to both objects
    assert parent_config.get("parent_setting") == 1
    assert child_config.get("parent_setting") == 1

    # Can also access in dict-like fashion
    assert parent_config["parent_setting"] == 1
    assert child_config["parent_setting"] == 1

    # Change in parent is reflected in child
    parent_config.set("parent_setting", 2)
    assert child_config["parent_setting"] == 2

    # And vice-versa
    child_config.set("parent_setting", 3)
    assert parent_config["parent_setting"] == 3

    # Child param not available in parent
    assert parent_config.get("child_setting", None) is None

    # Can also setting in dict-like fashion
    child_config["parent_setting"] = 4
    assert parent_config.get("parent_setting") == 4
    assert parent_config["parent_setting"] == 4
    assert child_config.get("parent_setting") == 4
    assert child_config["parent_setting"] == 4

    # Updating multiple items uses the same rules
    # Missing config is ignored
    child_config.update({"parent_setting": 5, "child_setting": 11, "missing": True})

    assert parent_config.get("parent_setting") == 5
    assert child_config.get("parent_setting") == 5
    assert parent_config.get("child_setting", None) is None
    assert child_config.get("child_setting") == 11

    # Adding a config later works
    parent_config.add(Setting("new_setting", default="new_setting"))
    assert parent_config.get("new_setting") == "new_setting"
    assert child_config.get("new_setting") == "new_setting"

    # Adding a third layer is also supported
    grand_child_config = SettingStore(
        [Setting("grand_child_setting", default=100)], extend_from=child_config
    )
    assert grand_child_config.get("parent_setting") == 5
    assert grand_child_config.get("child_setting") == 11
    assert grand_child_config.get("new_setting") == "new_setting"
    assert grand_child_config.get("grand_child_setting") == 100

    grand_child_config.set("parent_setting", 6)
    assert parent_config.get("parent_setting") == 6
    assert child_config.get("parent_setting") == 6
    assert grand_child_config.get("parent_setting") == 6
