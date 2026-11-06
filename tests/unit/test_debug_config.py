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


def test_features_are_off_and_per_session():
    a, b = _session(), _session()
    assert a.debug.check is False and a.debug.trace is False
    a.debug.enable(check=True)
    assert a.debug.check is True
    assert b.debug.check is False


def test_enable_and_disable():
    s = _session()
    s.debug.enable(check=True, trace=True)
    assert (s.debug.check, s.debug.trace) == (True, True)
    s.debug.disable(check=True)
    assert (s.debug.check, s.debug.trace) == (False, True)
    # False means "leave it as it is"
    s.debug.enable(check=False)
    assert s.debug.check is False
    s.debug.enable(check=True)
    s.debug.disable()
    assert (s.debug.check, s.debug.trace) == (False, False)


def test_unknown_features_raise():
    s = _session()
    with pytest.raises(ValueError, match="Unknown debug features"):
        s.debug.enable(nope=True)
    with pytest.raises(ValueError, match="Unknown debug features"):
        s.debug.disable(nope=True)
    with pytest.raises(AttributeError):
        s.debug.nope


def test_apply_to_all_sessions_reaches_every_session_and_new_ones():
    a, b = _session(), _session()
    with mock.patch.object(session_module, "_active_sessions", {a, b}):
        _apply_to_all_sessions({"check": True})
    assert a.debug.check is True and b.debug.check is True
    assert _session().debug.check is True
    _apply_to_all_sessions({"check": False})
    assert _session().debug.check is False


def test_tracing_count():
    a, b = _session(), _session()
    assert tracing_sessions() == 0
    a.debug.enable(check=True)
    assert tracing_sessions() == 0  # not a trace feature
    a.debug.enable(trace=True)
    b.debug.enable(trace=True)
    assert tracing_sessions() == 2
    a.debug.enable(trace=True)  # no change
    assert tracing_sessions() == 2
    a.debug.disable(trace=True)
    assert tracing_sessions() == 1
    b.debug._close()
    assert tracing_sessions() == 0
    # a closed session is not counted again
    b.debug.enable(trace=True)
    assert tracing_sessions() == 0


def test_local_testing_sessions_are_not_tracing():
    s = _session(conn=mock.create_autospec(MockServerConnection))
    s.debug.enable(trace=True)
    assert s.debug.trace is True
    assert tracing_sessions() == 0


def test_concurrent_changes_keep_the_count():
    sessions = [_session() for _ in range(8)]

    def flip(s):
        for _ in range(200):
            s.debug.enable(trace=True)
            s.debug.disable(trace=True)
        s.debug.enable(trace=True)

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


def test_enabled_is_false_for_objects_that_are_not_sessions():
    s = _session()
    s.debug.enable(check=True)
    assert debug_config.enabled(s, "check") is True
    assert debug_config.enabled(s, "trace") is False
    # e.g. a mock session of a unit test
    assert debug_config.enabled(mock.Mock(), "check") is False
    assert debug_config.enabled(object(), "check") is False
