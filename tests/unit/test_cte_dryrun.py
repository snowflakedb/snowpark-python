#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Unit tests for the CTE dry-run guard.

Covers:
  - PlanCompiler.cte_dryrun_check: return values.
  - _record_cte_dryrun_fallback: counter, telemetry, auto-disable.
  - PlanCompiler.compile / should_start_query_compilation: cte_enabled override.
"""

from unittest.mock import MagicMock

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark._internal.compiler.plan_compiler import PlanCompiler
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
    """Fresh local-testing session per test with CTE optimisation enabled."""
    with Session.builder.config("local_testing", True).create() as s:
        s._cte_optimization_enabled = True
        s._cte_dryrun_fallback_count = 0
        yield s


@pytest.fixture
def mock_plan(cte_session):
    """A MagicMock plan wired to the local-testing session."""
    plan = MagicMock()
    plan.session = cte_session
    return plan


# ---------------------------------------------------------------------------
# PlanCompiler.cte_dryrun_check: return values
# ---------------------------------------------------------------------------


def test_dryrun_check_returns_none_on_success(mock_plan):
    mock_plan.session._conn._cursor._describe_internal = MagicMock()
    assert PlanCompiler.cte_dryrun_check(mock_plan, _WITH_SQL) is None


def test_dryrun_check_returns_error_on_programming_error(mock_plan):
    mock_plan.session._conn._cursor._describe_internal = MagicMock(
        side_effect=ProgrammingError("CTE compilation error")
    )
    result = PlanCompiler.cte_dryrun_check(mock_plan, _WITH_SQL)
    assert isinstance(result, ProgrammingError)


def test_dryrun_check_returns_error_on_non_programming_error(mock_plan):
    """Any exception from _describe_internal is caught — no crash."""
    mock_plan.session._conn._cursor._describe_internal = MagicMock(
        side_effect=RuntimeError("unexpected connection error")
    )
    result = PlanCompiler.cte_dryrun_check(mock_plan, _WITH_SQL)
    assert isinstance(result, RuntimeError)


def test_dryrun_check_does_not_increment_counter(mock_plan):
    """cte_dryrun_check must never touch the fallback counter — that is
    the responsibility of _record_cte_dryrun_fallback."""
    mock_plan.session._conn._cursor._describe_internal = MagicMock(
        side_effect=ProgrammingError("err")
    )
    PlanCompiler.cte_dryrun_check(mock_plan, _WITH_SQL)
    assert mock_plan.session._cte_dryrun_fallback_count == 0


# ---------------------------------------------------------------------------
# _record_cte_dryrun_fallback: counter and auto-disable
# ---------------------------------------------------------------------------


def test_record_fallback_increments_counter_for_programming_error(cte_session):
    cte_session._record_cte_dryrun_fallback(ProgrammingError("err"))
    assert cte_session._cte_dryrun_fallback_count == 1


def test_record_fallback_does_not_increment_counter_for_non_programming_error(
    cte_session,
):
    cte_session._record_cte_dryrun_fallback(RuntimeError("err"))
    assert cte_session._cte_dryrun_fallback_count == 0


def test_record_fallback_sends_telemetry_with_actual_error_type(cte_session):
    cte_session._record_cte_dryrun_fallback(ProgrammingError("err"))
    cte_session._conn._telemetry_client.send_cte_dryrun_fallback_telemetry.assert_called_once_with(
        session_id=cte_session._session_id,
        error_type="ProgrammingError",
        fallback_count=1,
    )


def test_record_fallback_sends_telemetry_for_non_programming_error(cte_session):
    cte_session._record_cte_dryrun_fallback(RuntimeError("err"))
    cte_session._conn._telemetry_client.send_cte_dryrun_fallback_telemetry.assert_called_once_with(
        session_id=cte_session._session_id,
        error_type="RuntimeError",
        fallback_count=1,
    )


def test_record_fallback_auto_disables_at_threshold(cte_session):
    for _ in range(_CTE_DRYRUN_AUTO_DISABLE_THRESHOLD):
        cte_session._record_cte_dryrun_fallback(ProgrammingError("err"))

    assert cte_session._cte_optimization_enabled is False
    assert cte_session._cte_dryrun_fallback_count == _CTE_DRYRUN_AUTO_DISABLE_THRESHOLD
    cte_session._conn._telemetry_client.send_cte_dryrun_auto_disabled_telemetry.assert_called_once_with(
        session_id=cte_session._session_id,
        fallback_count=_CTE_DRYRUN_AUTO_DISABLE_THRESHOLD,
    )


def test_record_fallback_not_auto_disabled_below_threshold(cte_session):
    for _ in range(_CTE_DRYRUN_AUTO_DISABLE_THRESHOLD - 1):
        cte_session._record_cte_dryrun_fallback(ProgrammingError("err"))

    assert cte_session._cte_optimization_enabled is True


def test_record_fallback_non_programming_errors_dont_count_toward_threshold(
    cte_session,
):
    """RuntimeError fallbacks should never trigger auto-disable."""
    for _ in range(_CTE_DRYRUN_AUTO_DISABLE_THRESHOLD + 5):
        cte_session._record_cte_dryrun_fallback(RuntimeError("err"))

    assert cte_session._cte_optimization_enabled is True
    assert cte_session._cte_dryrun_fallback_count == 0


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
    plan.session._conn.__class__ = object
    return plan


def test_plan_compiler_cte_enabled_false_skips_cte():
    compiler = PlanCompiler(_make_cte_plan(cte_enabled=True))

    assert compiler.should_start_query_compilation(cte_enabled=False) is False


def test_plan_compiler_cte_enabled_none_uses_session_setting():
    compiler_on = PlanCompiler(_make_cte_plan(cte_enabled=True))
    compiler_off = PlanCompiler(_make_cte_plan(cte_enabled=False))

    assert compiler_on.should_start_query_compilation(cte_enabled=None) is True
    assert compiler_off.should_start_query_compilation(cte_enabled=None) is False


def test_plan_compiler_cte_enabled_false_but_lqb_still_compiles():
    """LQB enabled means compilation still runs even with cte_enabled=False."""
    compiler = PlanCompiler(_make_cte_plan(cte_enabled=True, lqb_enabled=True))

    assert compiler.should_start_query_compilation(cte_enabled=False) is True
