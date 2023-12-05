#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.context import get_active_session


def test_get_active_session(session):
    assert session == get_active_session()
