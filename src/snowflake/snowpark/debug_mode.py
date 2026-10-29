#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
import os
from typing import List, Optional

import snowflake.snowpark
import snowflake.snowpark.context as context
from snowflake.snowpark._internal.utils import (
    warn_session_config_update_in_multithreaded_mode,
)

_logger = logging.getLogger(__name__)

EAGER_SCHEMA_VALIDATION_ENV_VAR = "SNOWPARK_EAGER_SCHEMA_VALIDATION"


class EagerSchemaValidation:
    """
    Controls eager schema validation for one session. When it is enabled, the schema of a
    dataframe is validated by querying for column metadata after every dataframe operation,
    so that an invalid column reference is raised where it is written. This adds query
    overhead, so it is disabled by default and meant for debugging.

    The setting of a session is, in this order:

    1. what you set with :meth:`enable` or :meth:`disable`, or inside a ``with`` block;
    2. the environment variable ``SNOWPARK_EAGER_SCHEMA_VALIDATION`` (``true`` or ``false``,
       case-insensitive), read when the session is created;
    3. the process-wide setting made by
       ``snowflake.snowpark.context.configure_development_features(enable_eager_schema_validation=...)``.

    Use it through :attr:`Session.eager_schema_validation <snowflake.snowpark.Session.eager_schema_validation>`.

    Example::

        >>> session.eager_schema_validation.enable()  # doctest: +SKIP
        >>> with session.eager_schema_validation:  # doctest: +SKIP
        ...     df = session.table("T").filter("A > 1")

    Note:
        This feature is experimental. Do not use it in production.
    """

    def __init__(self, session: "snowflake.snowpark.Session") -> None:
        self._session = session
        self._setting: Optional[bool] = None
        self._saved_settings: List[Optional[bool]] = []

        env_value = os.environ.get(EAGER_SCHEMA_VALIDATION_ENV_VAR, "").strip().lower()
        if env_value in ("true", "false"):
            self._setting = env_value == "true"
            if self._setting:
                _logger.warning(
                    f"Eager schema validation is enabled by {EAGER_SCHEMA_VALIDATION_ENV_VAR}. "
                    "It adds queries to every dataframe operation, disable it before production."
                )

    @property
    def enabled(self) -> bool:
        """Whether eager schema validation is enabled for the session."""
        if self._setting is not None:
            return self._setting
        return context._debug_eager_schema_validation

    def enable(self) -> None:
        """Enables eager schema validation for the session."""
        self._set(True)
        _logger.info("Eager schema validation is enabled.")

    def disable(self) -> None:
        """Disables eager schema validation for the session."""
        self._set(False)
        _logger.info("Eager schema validation is disabled.")

    def _set(self, value: Optional[bool]) -> None:
        warn_session_config_update_in_multithreaded_mode("eager_schema_validation")
        with self._session._lock:
            self._setting = value

    def __enter__(self) -> "EagerSchemaValidation":
        with self._session._lock:
            self._saved_settings.append(self._setting)
            self._setting = True
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        with self._session._lock:
            self._setting = self._saved_settings.pop()


class DebugMode:
    """
    Turns the debugging features on or off together: eager schema validation of the session and
    the two tracing features of
    ``snowflake.snowpark.context.configure_development_features``
    (``enable_dataframe_trace_on_error`` and ``enable_trace_sql_errors_to_dataframe``).

    The tracing features are process-wide, not per session, because they rely on the AST
    collection which is process-wide.

    Use it through :attr:`Session.debug <snowflake.snowpark.Session.debug>`.

    Note:
        This feature is experimental. Do not use it in production.
    """

    def __init__(self, session: "snowflake.snowpark.Session") -> None:
        self._session = session

    def enable_all(self) -> None:
        """Enables eager schema validation and both tracing features."""
        self._session.eager_schema_validation.enable()
        self._configure_tracing(True)

    def disable_all(self) -> None:
        """Disables eager schema validation and both tracing features."""
        self._session.eager_schema_validation.disable()
        self._configure_tracing(False)

    @staticmethod
    def _configure_tracing(value: bool) -> None:
        # configure_development_features also sets the process-wide eager validation setting
        # from its argument, so pass the current one to leave it unchanged.
        context.configure_development_features(
            enable_eager_schema_validation=context._debug_eager_schema_validation,
            enable_dataframe_trace_on_error=value,
            enable_trace_sql_errors_to_dataframe=value,
        )
