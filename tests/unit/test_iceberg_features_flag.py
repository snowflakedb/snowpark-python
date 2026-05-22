#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2026 Snowflake Computing Inc. All rights reserved.
#
"""Unit tests for the `Session.iceberg_features_enabled` umbrella flag and
the `version=` / `snapshot-id` time-travel gates it controls.

These tests use mocked connections so they don't require a live Snowflake
account. End-to-end validation against an unmanaged Iceberg table lives in
`tests/integ/test_dataframe.py::test_iceberg_snapshot_id_time_travel*`
(skipped by default — see TODO there).
"""

from types import SimpleNamespace
from unittest import mock

import pytest

import snowflake.snowpark.session
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.exceptions import SnowparkClientException
from snowflake.snowpark.table import Table


def _build_session(*, iceberg_features_enabled=False):
    """Create a Session bound to an autospec'd ServerConnection.

    Mirrors the pattern in `tests/unit/test_dataframe.py::test_table_source_plan`.
    The session has a real `_ast_batch`, so AST emission code paths execute
    against real protos.
    """
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    mock_connection._thread_safe_session_enabled = True
    session = snowflake.snowpark.session.Session(mock_connection)
    session.iceberg_features_enabled = iceberg_features_enabled
    return session


# ---------------------------------------------------------------------------
# Flag plumbing: default, setter, public property.
# ---------------------------------------------------------------------------
def test_iceberg_features_enabled_default_is_false():
    """Default must be ``False`` so existing Snowpark users see no behavior
    change. SCOS / snowpark-connect opt in per-session."""
    session = _build_session()
    assert session.iceberg_features_enabled is False
    assert session._iceberg_features_enabled is False


def test_iceberg_features_enabled_setter_round_trip():
    """Public setter accepts truthy/falsy values and coerces to bool."""
    session = _build_session()

    session.iceberg_features_enabled = True
    assert session.iceberg_features_enabled is True

    session.iceberg_features_enabled = False
    assert session.iceberg_features_enabled is False

    # Truthy non-bool is coerced.
    session.iceberg_features_enabled = 1
    assert session.iceberg_features_enabled is True
    session.iceberg_features_enabled = 0
    assert session.iceberg_features_enabled is False


def test_require_iceberg_features_enabled_raises_when_off():
    """Helper raises a clear SnowparkClientException pointing to the flag."""
    session = _build_session(iceberg_features_enabled=False)
    with pytest.raises(SnowparkClientException) as exc_info:
        session._require_iceberg_features_enabled(feature="X")
    assert "iceberg_features_enabled" in str(exc_info.value)
    assert "X" in str(exc_info.value)


def test_require_iceberg_features_enabled_noop_when_on():
    """Helper returns without raising when the flag is on."""
    session = _build_session(iceberg_features_enabled=True)
    session._require_iceberg_features_enabled(feature="X")  # no raise


# ---------------------------------------------------------------------------
# Session.table gate.
# ---------------------------------------------------------------------------
def test_session_table_rejects_version_when_flag_off():
    """`Session.table(name, version=N)` must raise with the flag off."""
    session = _build_session(iceberg_features_enabled=False)
    with pytest.raises(SnowparkClientException, match="iceberg_features_enabled"):
        session.table("MY_TABLE", time_travel_mode="at", version=12345)


def test_session_table_does_not_raise_without_version():
    """Plain `session.table("...")` still works with the flag off."""
    session = _build_session(iceberg_features_enabled=False)
    # Not asserting on the returned Table internals here — just that the
    # gate doesn't fire when ``version`` is not supplied.
    t = session.table("MY_TABLE")
    assert isinstance(t, Table)


# ---------------------------------------------------------------------------
# DataFrameReader.table gate (covers both ``version=`` kwarg and the Spark
# Iceberg ``snapshot-id`` reader option path).
# ---------------------------------------------------------------------------
def test_dataframe_reader_table_rejects_version_kwarg_when_flag_off():
    session = _build_session(iceberg_features_enabled=False)
    with pytest.raises(SnowparkClientException, match="iceberg_features_enabled"):
        session.read.table("MY_TABLE", time_travel_mode="at", version=1)


@pytest.mark.parametrize("opt_key", ["snapshot-id", "snapshot_id"])
def test_dataframe_reader_table_rejects_snapshot_id_option_when_flag_off(opt_key):
    """The Spark-compat ``snapshot-id`` / ``snapshot_id`` option must also be
    gated — otherwise a user could bypass the flag through `.option(...)`."""
    session = _build_session(iceberg_features_enabled=False)
    with pytest.raises(SnowparkClientException, match="iceberg_features_enabled"):
        session.read.option(opt_key, 12345).table("MY_TABLE")


# ---------------------------------------------------------------------------
# Table.__init__ gate (direct construction).
# ---------------------------------------------------------------------------
def test_table_init_rejects_version_when_flag_off():
    session = _build_session(iceberg_features_enabled=False)
    with pytest.raises(SnowparkClientException, match="iceberg_features_enabled"):
        Table("MY_TABLE", session=session, time_travel_mode="at", version=1)


# ---------------------------------------------------------------------------
# AST `version` emission coverage. The Snowpark AST proto's Table /
# ReadTable messages don't yet carry a `version` field; the source guards
# the assignment with ``hasattr(ast, "version")``. To exercise the
# assignment (and credit coverage to the line) we patch ``with_src_position``
# to return a stand-in object that *does* have a `version` attribute, so the
# hasattr check is True.
# ---------------------------------------------------------------------------
def _make_ast_sentinel():
    """Return a SimpleNamespace masquerading as an AST proto with a
    `version` field that has a writable `.value`.

    All other fields fall through to a MagicMock so unrelated assignments
    (e.g. `ast.time_travel_mode.value = ...`) succeed without raising.
    """
    base = mock.MagicMock()
    base.version = SimpleNamespace(value=None)
    return base


def test_session_table_emits_version_into_ast_when_proto_supports_it():
    """Covers `session.py` ``ast.version.value = version`` branch.

    ``_emit_ast=True`` is passed explicitly because the ``@publicapi``
    decorator otherwise overrides it to ``is_ast_enabled()`` (False on a
    plain mock-connected session).
    """
    session = _build_session(iceberg_features_enabled=True)
    sentinel = _make_ast_sentinel()
    with mock.patch(
        "snowflake.snowpark.session.with_src_position", return_value=sentinel
    ):
        session.table("MY_TABLE", _emit_ast=True, time_travel_mode="at", version=98765)
    assert sentinel.version.value == 98765


def test_dataframe_reader_table_emits_version_into_ast_when_proto_supports_it():
    """Covers `dataframe_reader.py` ``ast.version.value = version`` branch.

    The reader's AST block additionally requires ``self._ast is not None``;
    AST is globally disabled on a plain mock-connected session, so we both
    pass ``_emit_ast=True`` and assign a non-None ``_ast`` placeholder on the
    reader.
    """
    session = _build_session(iceberg_features_enabled=True)
    reader = session.read
    reader._ast = mock.MagicMock()  # satisfy `self._ast is not None` guard

    sentinel = _make_ast_sentinel()
    with mock.patch(
        "snowflake.snowpark.dataframe_reader.with_src_position",
        return_value=sentinel,
    ):
        reader.table("MY_TABLE", _emit_ast=True, time_travel_mode="at", version=42)
    assert sentinel.version.value == 42


def test_table_init_emits_version_into_ast_when_proto_supports_it():
    """Covers `table.py` ``ast.version.value = version`` branch."""
    session = _build_session(iceberg_features_enabled=True)
    sentinel = _make_ast_sentinel()
    with mock.patch(
        "snowflake.snowpark.table.with_src_position", return_value=sentinel
    ):
        Table(
            "MY_TABLE",
            session=session,
            _emit_ast=True,
            time_travel_mode="at",
            version=7,
        )
    assert sentinel.version.value == 7
