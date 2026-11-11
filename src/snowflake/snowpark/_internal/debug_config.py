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
_FEATURES: Dict[str, bool] = {}

# The number of sessions with a trace feature on, so the public API wrapper, which has no
# session, checks one integer while every session is off.
_tracing_sessions = 0

_lock = threading.RLock()


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
