#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib
import sys

import pytest

import snowflake.snowpark


def test_py38_deprecation():
    if not (sys.version_info.major == 3 and sys.version_info.minor == 8):
        pytest.skip("This test is for Python 3.8 only")

    with pytest.warns(
        DeprecationWarning, match="Python Runtime 3.8 reached its End-Of-Life"
    ) as record:
        importlib.reload(snowflake.snowpark)

    assert len(record) == 1

    # import again won't trigger the warning
    with pytest.warns(None) as record:
        importlib.import_module("snowflake.snowpark")
    assert len(record) == 0
