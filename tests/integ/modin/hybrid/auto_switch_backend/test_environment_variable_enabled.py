#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


def test_auto_switch_backend_environment_variable_enabled_SNOW_2171718(monkeypatch):
    """Test that importing snowflake.snowpark.modin.plugin does not override AutoSwitchBackend=True set via environment variable"""

    monkeypatch.setenv("MODIN_AUTO_SWITCH_BACKENDS", "True")

    from modin.config import AutoSwitchBackend

    assert AutoSwitchBackend.get() is True

    import snowflake.snowpark.modin.plugin  # noqa: F401

    assert AutoSwitchBackend.get() is True
