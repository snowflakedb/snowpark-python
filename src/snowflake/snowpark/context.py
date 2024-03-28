#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
from typing import Callable, Optional

import snowflake.snowpark

_use_scoped_temp_objects = True

# This is an internal-only function, used to interrupt registration of UDxF or SPROC objects with Snowflake.
_internal_only_interrupt_registration: Optional[Callable] = None

# This is an internal-only global flag, used to determine whether to execute code in a client's local sandbox or connect to a Snowflake account.
_execute_in_local_sandbox: bool = False


def get_execute_in_local_sandbox() -> bool:
    return _execute_in_local_sandbox


def get_active_session() -> "snowflake.snowpark.Session":
    """Returns the current active Snowpark session.

    Raises: SnowparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return snowflake.snowpark.session._get_active_session()
