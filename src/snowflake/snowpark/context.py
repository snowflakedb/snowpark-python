#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""Context module for Snowpark."""
from snowflake.snowpark.session import Session, _get_active_session


def get_active_session() -> Session:
    """Returns the current active snowpark session.

    Raises: SnowparkSessionException: If there are more than one active sessions or no active sessions.

    Returns:
        A :class:`Session` object for current session.
    """
    return _get_active_session()
