#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark.session import _get_active_session
from snowflake.snowpark import Session

def get_active_session() -> "Session":
    return _get_active_session()