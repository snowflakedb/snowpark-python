#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import threading
from unittest import mock

import pytest

from snowflake.snowpark import session as session_module
from snowflake.snowpark._internal import debug_config
from snowflake.snowpark._internal.debug_config import (
    DebugConfig,
    _apply_to_all_sessions,
    tracing_sessions,
)
from snowflake.snowpark.mock._connection import MockServerConnection


@pytest.fixture(autouse=True)
def features():
    """Two features for the tests: ``check`` and the trace feature ``trace``."""
    with mock.patch.dict(debug_config._FEATURES, clear=True), mock.patch.dict(
        debug_config._defaults, clear=True
    ), mock.patch.object(debug_config, "_tracing_sessions", 0):
        debug_config._register_feature("check")
        debug_config._register_feature("trace", trace=True)
        yield


class _Session:
    def __init__(self, conn) -> None:
        self._conn = conn
        self._debug = self.debug = DebugConfig(self)


def _session(conn=None):
    return _Session(conn or mock.Mock())


def _status(s):
    return s.debug._get("check"), s.debug._get("trace")


def test_features_are_off_and_per_session():
    a, b = _session(), _session()
    assert _status(a) == (False, False)
    a.debug.set(check=True)
    assert _status(a) == (True, False)
    assert _status(b) == (False, False)


def test_set_changes_only_the_passed_features():
    s = _session()
    s.debug.set(check=True, trace=True)
    assert _status(s) == (True, True)
    s.debug.set(check=False)
    assert _status(s) == (False, True)
    # None keeps the current setting
    s.debug.set(check=True, trace=None)
    assert _status(s) == (True, True)


def test_set_without_arguments_turns_everything_off():
    s = _session()
    s.debug.set(check=True, trace=True)
    s.debug.set()
    assert _status(s) == (False, False)


def test_unknown_features_raise():
    s = _session()
    s.debug.set(check=True)
    with pytest.raises(ValueError, match="Unknown debug features"):
        s.debug.set(check=False, nope=True)
    # nothing changed
    assert _status(s) == (True, False)


def test_apply_to_all_sessions_reaches_every_session_and_new_ones():
    a, b = _session(), _session()
    with mock.patch.object(session_module, "_active_sessions", {a, b}):
        _apply_to_all_sessions({"check": True})
    assert _status(a) == (True, False) and _status(b) == (True, False)
    assert _status(_session()) == (True, False)
    _apply_to_all_sessions({"check": False})
    assert _status(_session()) == (False, False)


def test_tracing_count():
    a, b = _session(), _session()
    assert tracing_sessions() == 0
    a.debug.set(check=True)
    assert tracing_sessions() == 0  # not a trace feature
    a.debug.set(trace=True)
    b.debug.set(trace=True)
    assert tracing_sessions() == 2
    a.debug.set(trace=True)  # no change
    assert tracing_sessions() == 2
    a.debug.set(trace=False)
    assert tracing_sessions() == 1
    b.debug._close()
    assert tracing_sessions() == 0
    # a closed session is not counted again
    b.debug.set(trace=True)
    assert tracing_sessions() == 0


def test_local_testing_sessions_are_not_tracing():
    s = _session(conn=mock.create_autospec(MockServerConnection))
    s.debug.set(trace=True)
    assert _status(s) == (False, True)
    assert tracing_sessions() == 0


def test_concurrent_changes_keep_the_count():
    sessions = [_session() for _ in range(8)]

    def flip(s):
        for _ in range(200):
            s.debug.set(trace=True)
            s.debug.set(trace=False)
        s.debug.set(trace=True)

    threads = [threading.Thread(target=flip, args=(s,)) for s in sessions]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert tracing_sessions() == len(sessions)
    for s in sessions:
        s.debug._close()
    assert tracing_sessions() == 0


def test_session_has_debug_config():
    from snowflake.snowpark import Session

    assert isinstance(Session.debug, property)
