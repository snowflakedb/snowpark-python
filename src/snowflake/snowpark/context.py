#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
import logging
from typing import Callable, Optional

import snowflake.snowpark
import threading

_logger = logging.getLogger(__name__)

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

# Following are internal-only global flags, used to enable development features.
_enable_dataframe_trace_on_error = False
_debug_eager_schema_validation = False

# This is an internal-only global flag, used to determine whether to enable query line tracking for tracing sql compilation errors.
_enable_trace_sql_errors_to_dataframe = False


def configure_development_features(
    *,
    enable_eager_schema_validation: bool = False,
    enable_dataframe_trace_on_error: bool = False,
    enable_trace_sql_errors_to_dataframe: bool = False,
) -> None:
    """
    Configure development features for the session.

    Args:
        enable_eager_schema_validation: If True, dataframe schemas are eagerly validated by querying
            for column metadata after every dataframe operation. This adds additional query overhead.
        enable_dataframe_trace_on_error: If True, upon failure, we will add most recent dataframe
            operations to the error trace. This enables the AST collection in the session.
        enable_trace_sql_errors_to_dataframe: If True, we will enable tracing sql compilation errors
            to the associated dataframe operations. This enables the AST collection in the session.
    Note:
        This feature is experimental since 1.33.0. Do not use it in production.
    """
    _logger.warning(
        "configure_development_features() is experimental since 1.33.0. Do not use it in production.",
    )
    global _debug_eager_schema_validation
    global _enable_dataframe_trace_on_error
    global _enable_trace_sql_errors_to_dataframe
    _debug_eager_schema_validation = enable_eager_schema_validation

    if enable_dataframe_trace_on_error or enable_trace_sql_errors_to_dataframe:
        try:
            session = get_active_session()
            if session is None:
                _logger.warning(
                    "No active session found. Please create a session first and call "
                    "`configure_development_features()` after creating the session.",
                )
                return
            _enable_dataframe_trace_on_error = enable_dataframe_trace_on_error
            _enable_trace_sql_errors_to_dataframe = enable_trace_sql_errors_to_dataframe
            session.ast_enabled = True
        except Exception as e:
            _logger.warning(
                f"Cannot enable AST collection in the session due to {str(e)}. Some development features may not work as expected.",
            )
    else:
        _enable_dataframe_trace_on_error = False
        _enable_trace_sql_errors_to_dataframe = False


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
