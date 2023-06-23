#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark.context import get_active_session

pytestmark = pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')",
    reason="no active session in local testing",
)


def test_get_active_session(session):
    assert session == get_active_session()
