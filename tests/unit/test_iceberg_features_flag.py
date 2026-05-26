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


@pytest.mark.parametrize("opt_key", ["snapshot-id", "snapshot_id", "version"])
def test_dataframe_reader_table_rejects_snapshot_id_option_when_flag_off(opt_key):
    """Every reader option that resolves to ``version=`` must be gated —
    ``snapshot-id`` / ``snapshot_id`` (Spark Iceberg compat) AND the
    Snowpark-native ``version`` key. Otherwise a user could bypass the
    flag through ``.option(...)``."""
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


# NOTE: AST coverage for ``version`` was intentionally removed. The Snowpark
# AST proto's Table / ReadTable messages don't carry a ``version`` field and
# this feature is parameter-protected (gated behind ``iceberg_features_enabled``
# and consumed only by Snowpark Connect), so the AST emission is left out by
# design. When the proto is extended, restore one assignment line per call
# site and add coverage tests then.
