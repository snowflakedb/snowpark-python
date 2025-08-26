#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import pytest
from pathlib import Path


def pytest_collection_modifyitems(config, items):
    """Skip all tests in this directory unless environment variable is set."""
    if os.getenv("TEST_SNOWPARK_PANDAS_AUTO_SWITCH_BACKEND") == "True":
        return  # Run tests when env var is set

    # Each test case here should run in separate process because it
    # uses global imports and/or makes global configuration changes.
    skip_marker = pytest.mark.skip(
        reason="Set TEST_SNOWPARK_PANDAS_AUTO_SWITCH_BACKEND=True to run these tests"
    )

    current_dir = Path(__file__).parent
    for item in items:
        test_file_dir = Path(item.fspath).parent
        if test_file_dir == current_dir:
            item.add_marker(skip_marker)
