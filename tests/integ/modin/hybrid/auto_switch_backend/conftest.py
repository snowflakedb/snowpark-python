#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("TEST_SNOWPARK_PANDAS_AUTO_SWITCH_BACKEND") != "True",
    reason="These tests are only run when TEST_SNOWPARK_PANDAS_AUTO_SWITCH_BACKEND=True",
)
