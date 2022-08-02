#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.session import Session, _get_active_session


def get_active_session() -> Session:
    """Returns the current active snowpark session. Throws an error when
    multiple sessions are active or when no sessions are created."""
    return _get_active_session()
