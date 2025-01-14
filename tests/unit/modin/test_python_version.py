#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys

import pytest


def test_error_on_import():
    # On Python 3.8, we must run pytest with --noconftest, since various conftest.py files
    # will error when importing modin.pandas.
    # We therefore cannot test any Snowpark DataFrame operations, since we don't have the ability
    # to set up a session here; we can only test that imports work as expected.
    if sys.version_info.major == 3 and sys.version_info.minor == 8:
        # Importing snowpark (without pandas) should not error
        import snowflake.snowpark  # noqa: F401

        with pytest.raises(RuntimeError):
            # Importing snowpark pandas should error
            import snowflake.snowpark.modin.plugin  # noqa: F401
    else:
        # Should not error
        import snowflake.snowpark  # noqa: F401
        import snowflake.snowpark.modin.plugin  # noqa: F401
