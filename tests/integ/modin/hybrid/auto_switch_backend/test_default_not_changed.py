#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


def test_auto_switch_backend_default_not_changed():
    """Test that importing snowflake.snowpark.modin.plugin respects AutoSwitchBackend default state when user hasn't changed it"""

    from modin.config import AutoSwitchBackend

    assert AutoSwitchBackend.get() is AutoSwitchBackend.default

    import snowflake.snowpark.modin.plugin  # noqa: F401

    assert AutoSwitchBackend.get() is AutoSwitchBackend.default
