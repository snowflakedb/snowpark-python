#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("TEST_SNOWPARK_PANDAS_AUTO_SWITCH_BACKEND") != "True",
    reason="Each test case here should run in separate process because it "
    + "uses global imports and/or makes global configuration changes.",
)
