#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
from typing import Callable, Optional

import snowflake.snowpark

_use_scoped_temp_objects = True

# This is an internal-only global flag, used to determine whether to execute code in a client's local sandbox or connect to a Snowflake account.
# If this is True, then the session instance is forcibly set to None to avoid any interaction with a Snowflake account.
_is_execution_environment_sandboxed_for_client: bool = False

# This callback, assigned by the caller environment outside Snowpark, can be used to share information about the extension function to be registered.
# It should also return a decision on whether to proceed with registring the extension function with the Snowflake account.
# If _should_continue_registration is None, i.e. a caller environment never assigned it an alternate callable, then we want to continue registration as part of the regular Snowpark workflow.
# If _should_continue_registration is not None, i.e. a caller environment has assigned it an alternate callable, then the callback is responsible for determining the rest of the Snowpark workflow.
_should_continue_registration: Optional[Callable[..., bool]] = None


# If True, submits all queries (i.e. collect() calls) made in stored procedures asynchronously. If
# the original invocation was intended to be synchronous, we will poll for the result.
_async_query_submission_in_stored_procedures: bool = True


# Amount of time, in nanoseconds, to wait between polls, if
# _async_query_submission_in_stored_procedures=True.
_async_query_submission_polling_interval_ns: float = 1


def get_active_session() -> "snowflake.snowpark.Session":
    """Returns the current active Snowpark session.

    Raises: SnowparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return snowflake.snowpark.session._get_active_session()
