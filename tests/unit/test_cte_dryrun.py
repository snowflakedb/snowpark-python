#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Unit tests for the CTE dry-run guard.

Covers:
  - _cte_dryrun_check closure: return values, lru_cache behaviour, fallback
    counter, telemetry, and auto-disable logic.
  - PlanCompiler.compile / should_start_query_compilation: cte_enabled override.
"""

from unittest.mock import MagicMock

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark._internal.server_connection import (
    _CTE_DRYRUN_AUTO_DISABLE_THRESHOLD,
)
from snowflake.snowpark.session import Session

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_WITH_SQL = "WITH cte AS (SELECT 1 AS a) SELECT * FROM cte"


@pytest.fixture
def cte_session():
    """Fresh local-testing session per test with CTE optimisation enabled.

    Each test gets its own session, so the per-session lru_cache starts empty.
    """
    with Session.builder.config("local_testing", True).create() as s:
        s._cte_optimization_enabled = True
        s._cte_dryrun_fallback_count = 0
        # Ensure cache is empty (defence against any startup side-effects).
        s._cte_dryrun_check.cache_clear()
        yield s


# ---------------------------------------------------------------------------
# Return values
# ---------------------------------------------------------------------------


def test_dryrun_check_returns_true_on_success(cte_session):
    cte_session._conn._cursor._describe_internal = MagicMock()
    assert cte_session._cte_dryrun_check(_WITH_SQL) is True


def test_dryrun_check_returns_false_on_programming_error(cte_session):
    cte_session._conn._cursor._describe_internal = MagicMock(
        side_effect=ProgrammingError("CTE compilation error")
    )
    assert cte_session._cte_dryrun_check(_WITH_SQL) is False


# ---------------------------------------------------------------------------
# lru_cache behaviour
# ---------------------------------------------------------------------------


def test_dryrun_check_success_result_is_cached(cte_session):
    """Second call with the same SQL must not re-issue the round-trip."""
    describe = MagicMock()
    cte_session._conn._cursor._describe_internal = describe

    cte_session._cte_dryrun_check(_WITH_SQL)
    cte_session._cte_dryrun_check(_WITH_SQL)  # cache hit

    describe.assert_called_once()


def test_dryrun_check_failure_result_is_cached(cte_session):
    """A failing SQL must not be re-executed on subsequent calls."""
    describe = MagicMock(side_effect=ProgrammingError("err"))
    cte_session._conn._cursor._describe_internal = describe

    cte_session._cte_dryrun_check(_WITH_SQL)
    cte_session._cte_dryrun_check(_WITH_SQL)  # cache hit — no re-execution

    describe.assert_called_once()
    assert cte_session._cte_dryrun_fallback_count == 1  # incremented only once


def test_dryrun_check_different_sqls_are_independent_cache_entries(cte_session):
    """Two distinct SQL strings each trigger exactly one describe call."""
    describe = MagicMock()
    cte_session._conn._cursor._describe_internal = describe

    sql_a = "WITH a AS (SELECT 1) SELECT * FROM a"
    sql_b = "WITH b AS (SELECT 2) SELECT * FROM b"

    cte_session._cte_dryrun_check(sql_a)
    cte_session._cte_dryrun_check(sql_b)
    cte_session._cte_dryrun_check(sql_a)  # cached
    cte_session._cte_dryrun_check(sql_b)  # cached

    assert describe.call_count == 2


# ---------------------------------------------------------------------------
# Fallback counter
# ---------------------------------------------------------------------------


def test_dryrun_fallback_count_increments_per_unique_failing_sql(cte_session):
    cte_session._conn._cursor._describe_internal = MagicMock(
        side_effect=ProgrammingError("err")
    )

    sql_a = "WITH a AS (SELECT 1) SELECT * FROM a"
    sql_b = "WITH b AS (SELECT 2) SELECT * FROM b"

    cte_session._cte_dryrun_check(sql_a)
    cte_session._cte_dryrun_check(sql_a)  # cached — no extra increment
    cte_session._cte_dryrun_check(sql_b)

    assert cte_session._cte_dryrun_fallback_count == 2


# ---------------------------------------------------------------------------
# Auto-disable
# ---------------------------------------------------------------------------


def test_dryrun_auto_disable_triggered_at_threshold(cte_session):
    cte_session._conn._cursor._describe_internal = MagicMock(
        side_effect=ProgrammingError("err")
    )

    for i in range(_CTE_DRYRUN_AUTO_DISABLE_THRESHOLD):
        cte_session._cte_dryrun_check(f"WITH cte AS (SELECT {i}) SELECT * FROM cte")

    assert cte_session._cte_optimization_enabled is False
    assert cte_session._cte_dryrun_fallback_count == _CTE_DRYRUN_AUTO_DISABLE_THRESHOLD


def test_dryrun_auto_disable_not_triggered_below_threshold(cte_session):
    cte_session._conn._cursor._describe_internal = MagicMock(
        side_effect=ProgrammingError("err")
    )

    for i in range(_CTE_DRYRUN_AUTO_DISABLE_THRESHOLD - 1):
        cte_session._cte_dryrun_check(f"WITH cte AS (SELECT {i}) SELECT * FROM cte")

    assert cte_session._cte_optimization_enabled is True


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------


def test_dryrun_fallback_telemetry_sent_once_per_unique_sql(cte_session):
    cte_session._conn._cursor._describe_internal = MagicMock(
        side_effect=ProgrammingError("err")
    )

    cte_session._cte_dryrun_check(_WITH_SQL)
    cte_session._cte_dryrun_check(_WITH_SQL)  # cached — no second telemetry call

    cte_session._conn._telemetry_client.send_cte_dryrun_fallback_telemetry.assert_called_once_with(
        session_id=cte_session._session_id,
        error_type="ProgrammingError",
        fallback_count=1,
    )


def test_dryrun_auto_disable_telemetry_sent_once_at_threshold(cte_session):
    cte_session._conn._cursor._describe_internal = MagicMock(
        side_effect=ProgrammingError("err")
    )

    for i in range(_CTE_DRYRUN_AUTO_DISABLE_THRESHOLD):
        cte_session._cte_dryrun_check(f"WITH cte AS (SELECT {i}) SELECT * FROM cte")

    cte_session._conn._telemetry_client.send_cte_dryrun_auto_disabled_telemetry.assert_called_once_with(
        session_id=cte_session._session_id,
        fallback_count=_CTE_DRYRUN_AUTO_DISABLE_THRESHOLD,
    )
    # One fallback telemetry event per unique failing SQL.
    assert (
        cte_session._conn._telemetry_client.send_cte_dryrun_fallback_telemetry.call_count
        == _CTE_DRYRUN_AUTO_DISABLE_THRESHOLD
    )


# ---------------------------------------------------------------------------
# PlanCompiler.compile / should_start_query_compilation: cte_enabled override
# ---------------------------------------------------------------------------


def _make_cte_plan(*, cte_enabled: bool, lqb_enabled: bool = False):
    """Return a MagicMock plan whose session has cte/lqb as specified."""
    plan = MagicMock()
    plan.session.cte_optimization_enabled = cte_enabled
    plan.session._cte_optimization_enabled = cte_enabled
    plan.session.large_query_breakdown_enabled = lqb_enabled
    plan.session._query_compilation_stage_enabled = True
    # Simulate a non-mock connection so MockServerConnection check passes.
    plan.session._conn.__class__ = object
    return plan


def test_plan_compiler_cte_enabled_false_skips_cte():
    from snowflake.snowpark._internal.compiler.plan_compiler import PlanCompiler

    compiler = PlanCompiler(_make_cte_plan(cte_enabled=True))

    assert compiler.should_start_query_compilation(cte_enabled=False) is False


def test_plan_compiler_cte_enabled_none_uses_session_setting():
    from snowflake.snowpark._internal.compiler.plan_compiler import PlanCompiler

    compiler_on = PlanCompiler(_make_cte_plan(cte_enabled=True))
    compiler_off = PlanCompiler(_make_cte_plan(cte_enabled=False))

    assert compiler_on.should_start_query_compilation(cte_enabled=None) is True
    assert compiler_off.should_start_query_compilation(cte_enabled=None) is False


def test_plan_compiler_cte_enabled_false_but_lqb_still_compiles():
    """LQB enabled means compilation still runs even with cte_enabled=False."""
    from snowflake.snowpark._internal.compiler.plan_compiler import PlanCompiler

    compiler = PlanCompiler(_make_cte_plan(cte_enabled=True, lqb_enabled=True))

    assert compiler.should_start_query_compilation(cte_enabled=False) is True
