#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
from typing import Callable, Optional

import snowflake.snowpark
import threading

_use_scoped_temp_objects = True

# This is an internal-only global flag, used to determine whether to execute code in a client's local sandbox or connect to a Snowflake account.
# If this is True, then the session instance is forcibly set to None to avoid any interaction with a Snowflake account.
_is_execution_environment_sandboxed_for_client: bool = False

# This callback, assigned by the caller environment outside Snowpark, can be used to share information about the extension function to be registered.
# It should also return a decision on whether to proceed with registring the extension function with the Snowflake account.
# If _should_continue_registration is None, i.e. a caller environment never assigned it an alternate callable, then we want to continue registration as part of the regular Snowpark workflow.
# If _should_continue_registration is not None, i.e. a caller environment has assigned it an alternate callable, then the callback is responsible for determining the rest of the Snowpark workflow.
_should_continue_registration: Optional[Callable[..., bool]] = None


# Internal-only global flag that determines if structured type semantics should be used
_use_structured_type_semantics = False
_use_structured_type_semantics_lock = threading.RLock()

# This is an internal-only global flag, used to determine whether the api code which will be executed is compatible with snowflake.snowpark_connect
_is_snowpark_connect_compatible_mode = False


def _should_use_structured_type_semantics():
    global _use_structured_type_semantics
    global _use_structured_type_semantics_lock
    with _use_structured_type_semantics_lock:
        return _use_structured_type_semantics


def get_active_session() -> "snowflake.snowpark.Session":
    """Returns the current active Snowpark session.

    Raises: SnowparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return snowflake.snowpark.session._get_active_session()
