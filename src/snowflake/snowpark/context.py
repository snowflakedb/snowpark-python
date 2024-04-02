#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
from enum import Enum
from typing import Any, Callable, Optional

import snowflake.snowpark
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages

_use_scoped_temp_objects = True


class ObjectRegistrationDecision(Enum):
    REGISTER_WITH_SNOWFLAKE = "REGISTER_WITH_SNOWFLAKE"
    DO_NOT_REGISTER_WITH_SNOWFLAKE = "DO_NOT_REGISTER_WITH_SNOWFLAKE"
    IN_SANDBOX_DO_NOT_REGISTER_WITH_SNOWFLAKE = (
        "IN_SANDBOX_DO_NOT_REGISTER_WITH_SNOWFLAKE"
    )
    COULD_NOT_BE_DETERMINED = "COULD_NOT_BE_DETERMINED"


def noop(*_args, **_kwargs):
    pass


# This is an internal-only global flag, used to determine whether to execute code in a client's local sandbox or connect to a Snowflake account.
# If this is True, then user's provided or active session is not used, and a new session should instead be created with the MockConnectionServer connection,
# which does not interact with Snowflake.
_is_execution_environment_sandboxed: bool = False

# This flag, assigned by the caller environment outside Snowpark, helps determine if UDxF/Sproc should be registered with Snowflake.
_interrupt_registration: bool = False

# This callback, assigned by the caller environment outside Snowpark, is used to share information about the UDxF/Sproc object to be registered.
_share_registration_info_with_caller: Optional[Callable] = noop


# This function determines if a UDF/UDTF/UDAF/SPROC object should be registered with Snowflake.
def _get_decision_to_register_udf_or_sproc():
    if _is_execution_environment_sandboxed:
        if _interrupt_registration:
            return ObjectRegistrationDecision.IN_SANDBOX_DO_NOT_REGISTER_WITH_SNOWFLAKE
        else:
            return (
                ObjectRegistrationDecision.COULD_NOT_BE_DETERMINED
            )  # Since sandboxing and connecting to snowflake to continue registration is not possible
    else:
        if _interrupt_registration:
            return ObjectRegistrationDecision.DO_NOT_REGISTER_WITH_SNOWFLAKE
        else:  # This is the default flow of object registration
            return ObjectRegistrationDecision.REGISTER_WITH_SNOWFLAKE


# This is an internal-only function to decide whether to proceed with registring an object with Snowflake, and if not, then the action that should be taken in place of registration.
def _get_decision_to_register_with_snowflake(registration_info: Any) -> None:

    decision_to_register: ObjectRegistrationDecision = (
        _get_decision_to_register_udf_or_sproc()
    )

    if ObjectRegistrationDecision.COULD_NOT_BE_DETERMINED:
        raise SnowparkClientExceptionMessages.LOCAL_SANDBOX_CONNECTION_FAILURE()
    else:
        _share_registration_info_with_caller(registration_info)
        return decision_to_register


def get_active_session() -> "snowflake.snowpark.Session":
    """Returns the current active Snowpark session.

    Raises: SnowparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return snowflake.snowpark.session._get_active_session()
