#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Per-session configuration of the debug features (``session.debug``)."""

import threading
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session  # pragma: no cover

# The debug features, by name, and whether each one is a trace feature (it records the DataFrame
# trace while it is on).
_FEATURES: Dict[str, bool] = {
    # Eagerly validates the schema of each dataframe when it is created.
    "eager_schema_validation": False,
    # Maps SQL compilation errors to the Python lines of the dataframe operations, and adds the
    # context of missing and existing objects to the errors.
    "trace_sql_errors_to_dataframe": True,
    # Adds the most recent dataframe operations (the lineage) of a failed dataframe to the error.
    "dataframe_trace_on_error": True,
}

# The number of sessions with a trace feature on, so the public API wrapper, which has no
# session, checks one integer while every session is off.
_tracing_sessions = 0

_lock = threading.RLock()


def enabled(session: "Session", name: str) -> bool:
    """Whether the debug feature ``name`` is on in ``session``. Objects that are not sessions
    (e.g. mocks in unit tests) have every feature off."""
    config = getattr(session, "_debug", None)
    return isinstance(config, DebugConfig) and config._enabled.get(name, False)


class DebugConfig:
    """The debug features of a session.

    Each feature has ``enable_<feature>()`` and ``disable_<feature>()`` methods and a read-only
    ``<feature>`` property; :meth:`set` changes several features at once.

    Example::

        session.debug.set(eager_schema_validation=True)
        session.debug.eager_schema_validation  # True
        session.debug.set()  # turns every debug feature off

    Note:
        This feature is experimental. Do not use it in production.
    """

    def __init__(self, session: "Session") -> None:
        from snowflake.snowpark.mock._connection import MockServerConnection

        # Local testing sessions do not record the DataFrame trace.
        self._can_trace = not isinstance(
            getattr(session, "_conn", None), MockServerConnection
        )
        self._tracing = False
        self._closed = False
        with _lock:
            self._enabled: Dict[str, bool] = {name: False for name in _FEATURES}
            self._update_tracing_count()

    def set(self, **features: Optional[bool]) -> None:
        """Sets the debug features of this session, as
        :func:`snowflake.snowpark.context.configure_development_features` does for every session.

        Only the features passed explicitly are changed: ``True`` turns a feature on, ``False``
        turns it off and ``None`` keeps its current setting. Calling it without any argument turns
        every debug feature off.
        """
        unknown = sorted(set(features) - set(_FEATURES))
        if unknown:
            raise ValueError(
                f"Unknown debug features {unknown}; the debug features are {sorted(_FEATURES)}"
            )
        if not features:
            features = {name: False for name in _FEATURES}
        self._apply({name: on for name, on in features.items() if on is not None})

    @property
    def eager_schema_validation(self) -> bool:
        """Whether the schema of each dataframe is validated when the dataframe is created."""
        return self._get("eager_schema_validation")

    def enable_eager_schema_validation(self) -> None:
        """Validates the schema of each dataframe of this session when the dataframe is created,
        by querying its column metadata. This adds queries."""
        self._apply({"eager_schema_validation": True})

    def disable_eager_schema_validation(self) -> None:
        """Turns off the eager schema validation of this session."""
        self._apply({"eager_schema_validation": False})

    @property
    def trace_sql_errors_to_dataframe(self) -> bool:
        """Whether SQL compilation errors are mapped to the dataframe operations."""
        return self._get("trace_sql_errors_to_dataframe")

    def enable_trace_sql_errors_to_dataframe(self) -> None:
        """Maps the SQL compilation errors of this session to the Python lines of the dataframe
        operations that generated the SQL, and adds the context of missing and existing objects to
        the errors."""
        self._apply({"trace_sql_errors_to_dataframe": True})

    def disable_trace_sql_errors_to_dataframe(self) -> None:
        """Turns off the SQL error tracing of this session."""
        self._apply({"trace_sql_errors_to_dataframe": False})

    @property
    def dataframe_trace_on_error(self) -> bool:
        """Whether the errors of a dataframe show its most recent dataframe operations."""
        return self._get("dataframe_trace_on_error")

    def enable_dataframe_trace_on_error(self) -> None:
        """Adds the most recent dataframe operations of a failed dataframe of this session to the
        error."""
        self._apply({"dataframe_trace_on_error": True})

    def disable_dataframe_trace_on_error(self) -> None:
        """Turns off the dataframe trace on error of this session."""
        self._apply({"dataframe_trace_on_error": False})

    def _get(self, name: str) -> bool:
        with _lock:
            return self._enabled.get(name, False)

    def _apply(self, values: Dict[str, bool]) -> None:
        with _lock:
            self._enabled.update(values)
            self._update_tracing_count()

    def _update_tracing_count(self) -> None:
        global _tracing_sessions
        tracing = (
            self._can_trace
            and not self._closed
            and any(on and _FEATURES.get(name) for name, on in self._enabled.items())
        )
        if tracing != self._tracing:
            _tracing_sessions += 1 if tracing else -1
            self._tracing = tracing
