#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
from enum import Enum
from typing import Callable, Optional

import snowflake.snowpark

_use_scoped_temp_objects = True


class ObjectRegistrationDecision(Enum):
    REGISTER_WITH_SNOWFLAKE = "REGISTER_WITH_SNOWFLAKE"
    DO_NOT_REGISTER_WITH_SNOWFLAKE = "DO_NOT_REGISTER_WITH_SNOWFLAKE"


# This is an internal-only global flag, used to determine whether to execute code in a client's local sandbox or connect to a Snowflake account.
# If this is True, then all sessions will be created using MockServerConnection which mocks connection to Snowflake, instead of creating a real connection
# which allows interacting with Snowflake.
_is_execution_environment_sandboxed: bool = False

# This callback, assigned by the caller environment outside Snowpark, can be used to share information about the UDxF/Sproc object to be registered.
# It should also return a decision on whether to proceed with registring the UDxF/SPROC object with the Snowflake account.
# If _should_continue_registration is None, i.e. a caller environment never assigned it an alternate callable, then we want to continue registration as part of the regular Snowpark workflow.
# If _should_continue_registration is not None, i.e. a caller environment has assigned it an alternate callable, then the callback is responsible for determining the rest of the Snowpark workflow.
_should_continue_registration: Optional[Callable] = None


def get_active_session() -> "snowflake.snowpark.Session":
    """Returns the current active Snowpark session.

    Raises: SnowparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return snowflake.snowpark.session._get_active_session()
