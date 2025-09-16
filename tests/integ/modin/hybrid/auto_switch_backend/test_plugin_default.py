#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


def test_importing_plugin_always_turns_on_auto_switch_backend():
    """Test that importing snowflake.snowpark.modin.plugin always turns AutoSwitchBackend off."""

    from modin.config import AutoSwitchBackend

    import snowflake.snowpark.modin.plugin  # noqa: F401

    assert AutoSwitchBackend.get() is False
