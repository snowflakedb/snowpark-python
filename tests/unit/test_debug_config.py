#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import threading
from unittest import mock

import pytest

from snowflake.snowpark import Session, context
from snowflake.snowpark import session as session_module
from snowflake.snowpark._internal import debug_config
from snowflake.snowpark._internal.debug_config import DebugConfig
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.mock._connection import MockServerConnection


@pytest.fixture(autouse=True)
def features():
    """Two features for the tests: ``check`` and the trace feature ``trace``."""
    with mock.patch.dict(
        debug_config._FEATURES, {"check": False, "trace": True}, clear=True
    ), mock.patch.dict(
        context._development_feature_settings, clear=True
    ), mock.patch.object(
        debug_config, "_tracing_sessions", 0
    ):
        yield


class _Session:
    def __init__(self, conn) -> None:
        self._conn = conn
        self._debug = self.debug = DebugConfig(self)


def _session(conn=None):
    return _Session(conn or mock.Mock())


def tracing_sessions():
    return debug_config._tracing_sessions


def _real_session():
    """A Session on a fake connection, so that Session.close() runs."""
    conn = mock.create_autospec(ServerConnection)
    conn._conn = mock.Mock()
    conn._conn._session_parameters = None
    conn._thread_safe_session_enabled = True
    conn._get_client_side_session_parameter = (
        lambda x, y: ServerConnection._get_client_side_session_parameter(conn, x, y)
    )
    return Session(conn)


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


def test_tracing_count():
    a, b = _real_session(), _real_session()
    assert tracing_sessions() == 0
    a._debug.set(check=True)
    assert tracing_sessions() == 0  # not a trace feature
    a._debug.set(trace=True)
    b._debug.set(trace=True)
    assert tracing_sessions() == 2
    a._debug.set(trace=True)  # no change
    assert tracing_sessions() == 2
    a._debug.set(trace=False)
    assert tracing_sessions() == 1
    b.close()
    assert tracing_sessions() == 0
    # a closed session is not counted again
    b._debug.set(trace=True)
    assert tracing_sessions() == 0
    a.close()


def test_local_testing_sessions_are_not_tracing():
    s = _session(conn=mock.create_autospec(MockServerConnection))
    s.debug.set(trace=True)
    assert _status(s) == (False, True)
    assert tracing_sessions() == 0


def test_concurrent_changes_keep_the_count():
    sessions = [_real_session() for _ in range(8)]

    def flip(s):
        for _ in range(200):
            s._debug.set(trace=True)
            s._debug.set(trace=False)
        s._debug.set(trace=True)

    threads = [threading.Thread(target=flip, args=(s,)) for s in sessions]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert tracing_sessions() == len(sessions)
    for s in sessions:
        s.close()
    assert tracing_sessions() == 0


def test_session_has_debug_config():
    session = _real_session()
    assert isinstance(session.debug, DebugConfig)
    session.close()


def test_a_session_created_while_configure_runs_gets_its_settings():
    # The order of the race: the session is built, configure_development_features updates the
    # settings and the active sessions, and only then the session is added to them.
    session = _real_session()
    assert _status(session) == (False, False)
    with session_module._session_management_lock:
        context._development_feature_settings.update({"check": True})
    session_module._add_session(session)
    try:
        assert _status(session) == (True, False)
    finally:
        session.close()


def test_adding_an_active_session_again_keeps_its_settings():
    context._development_feature_settings.update({"check": True})
    session = _real_session()
    session_module._add_session(session)
    try:
        session._debug.set(check=False)
        session_module._add_session(session)
        assert _status(session) == (False, False)
    finally:
        session.close()
