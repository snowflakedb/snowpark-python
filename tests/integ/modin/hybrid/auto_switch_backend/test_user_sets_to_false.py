#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


def test_auto_switch_backend_user_sets_to_False():
    """Test that importing snowflake.snowpark.modin.plugin does not override user's AutoSwitchBackend setting"""

    from modin.config import AutoSwitchBackend

    AutoSwitchBackend.disable()
    assert AutoSwitchBackend.get() is False

    import snowflake.snowpark.modin.plugin  # noqa: F401

    assert AutoSwitchBackend.get() is False
