#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Unit tests for the CTE execution fallback (retry) mechanism.

These tests are pure-unit: no Snowflake connection is required.  All I/O and
compilation are mocked so the tests run without any external dependencies.
"""

from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark._internal.analyzer.snowflake_plan import PlanQueryType, Query
from snowflake.snowpark._internal.server_connection import (
    ServerConnection,
    _CTE_FALLBACK_AUTO_DISABLE_THRESHOLD,
    _should_retry_cte_error,
)
from snowflake.snowpark.session import _PYTHON_SNOWPARK_GENERATE_MULTILINE_QUERIES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_query(sql="SELECT 1 AS v"):
    q = MagicMock(spec=Query)
    q.sql = sql
    q.params = []
    q.query_id_place_holder = "__QUERY_ID_PLACEHOLDER__"
    q.is_ddl_on_temp_object = False
    q.temp_obj_name_placeholder = None
    return q


def _make_plan_queries(query=None, post_actions=None):
    return {
        PlanQueryType.QUERIES: [query or _make_query()],
        PlanQueryType.POST_ACTIONS: post_actions or [],
    }


def _make_mock_session(cte_enabled=True, fallback_count=0):
    session = MagicMock()
    session.cte_optimization_enabled = cte_enabled
    session._cte_optimization_enabled = cte_enabled
    session._cte_optimization_fallback_count = fallback_count
    session._last_canceled_id = -1
    session._generate_new_action_id.return_value = 0
    session.session_id = "fake_session_id"
    session._collect_snowflake_plan_telemetry_at_critical_path = False
    return session


def _make_mock_plan(session=None, cte_queries=None, post_actions=None):
    session = session or _make_mock_session()
    plan = MagicMock()
    plan.session = session
    plan.uuid = "fake_plan_uuid"
    plan.api_calls = []
    plan.execution_queries = _make_plan_queries(
        query=cte_queries[0] if cte_queries else None,
        post_actions=post_actions,
    )
    return plan


def _make_server_connection():
    fake_conn = mock.create_autospec(SnowflakeConnection)
    fake_conn._conn = MagicMock()
    fake_conn._telemetry = None
    fake_conn._session_parameters = {_PYTHON_SNOWPARK_GENERATE_MULTILINE_QUERIES: True}
    fake_conn.cursor.return_value = mock.create_autospec(SnowflakeCursor)
    fake_conn.is_closed.return_value = False
    sc = ServerConnection({}, fake_conn)
    sc._telemetry_client = MagicMock()
    return sc


def _run_query_success(sql="SELECT 1 AS v"):
    """Return value for a successful blocking run_query call."""
    return {"sfqid": "fake_sfqid", "data": [[1]]}


# ---------------------------------------------------------------------------
# Tests: _should_retry_cte_error
# ---------------------------------------------------------------------------


def test_should_retry_cte_error_true_for_any_programming_error():
    assert _should_retry_cte_error(ProgrammingError("any error")) is True


def test_should_retry_cte_error_exposes_errno_and_sqlstate():
    """errno / sqlstate are accessible for future targeted filtering."""
    err = ProgrammingError("msg", errno=1234, sqlstate="42000")
    assert err.errno == 1234
    assert err.sqlstate == "42000"
    # Still returns True — no filtering yet.
    assert _should_retry_cte_error(err) is True


# ---------------------------------------------------------------------------
# Tests: _CTE_FALLBACK_AUTO_DISABLE_THRESHOLD module constant
# ---------------------------------------------------------------------------


def test_auto_disable_threshold_is_positive_int():
    assert isinstance(_CTE_FALLBACK_AUTO_DISABLE_THRESHOLD, int)
    assert _CTE_FALLBACK_AUTO_DISABLE_THRESHOLD > 0


# ---------------------------------------------------------------------------
# Tests: get_result_set — retry eligibility guards
# ---------------------------------------------------------------------------


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_no_retry_when_no_error(mock_desc):
    """Happy path: successful execution never touches the retry path."""
    sc = _make_server_connection()
    plan = _make_mock_plan()
    success_result = _run_query_success()

    with patch.object(sc, "run_query", return_value=success_result):
        result, _ = sc.get_result_set(plan, block=True)

    assert result == success_result
    sc._telemetry_client.send_cte_execution_fallback_telemetry.assert_not_called()
    sc._telemetry_client.send_cte_auto_disabled_telemetry.assert_not_called()


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_no_retry_when_block_false(mock_desc):
    """block=False (async) is not eligible for retry."""
    sc = _make_server_connection()
    cte_query = _make_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
    session = _make_mock_session(cte_enabled=True)
    plan = _make_mock_plan(session=session, cte_queries=[cte_query])

    prog_err = ProgrammingError("CTE error")
    prog_err.sfqid = "bad_sfqid"

    with patch.object(sc, "run_query", side_effect=prog_err):
        with pytest.raises(ProgrammingError, match="CTE error"):
            sc.get_result_set(plan, block=False)

    sc._telemetry_client.send_cte_execution_fallback_telemetry.assert_not_called()


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_no_retry_when_cte_disabled(mock_desc):
    """CTE optimization was off — no fallback attempt."""
    sc = _make_server_connection()
    session = _make_mock_session(cte_enabled=False)
    plan = _make_mock_plan(session=session)

    prog_err = ProgrammingError("some SQL error")

    with patch.object(sc, "run_query", side_effect=prog_err):
        with pytest.raises(ProgrammingError):
            sc.get_result_set(plan, block=True)

    sc._telemetry_client.send_cte_execution_fallback_telemetry.assert_not_called()


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_no_retry_when_multiple_main_queries(mock_desc):
    """Multi-query plans are not retried (partial side-effects risk)."""
    sc = _make_server_connection()
    session = _make_mock_session(cte_enabled=True)

    q1 = _make_query("CREATE TABLE t AS SELECT 1")
    q2 = _make_query("SELECT * FROM t")
    plan = MagicMock()
    plan.session = session
    plan.uuid = "uuid"
    plan.api_calls = []
    plan.execution_queries = {
        PlanQueryType.QUERIES: [q1, q2],
        PlanQueryType.POST_ACTIONS: [],
    }

    prog_err = ProgrammingError("CTE error")

    with patch.object(sc, "run_query", side_effect=prog_err):
        with pytest.raises(ProgrammingError):
            sc.get_result_set(plan, block=True)

    sc._telemetry_client.send_cte_execution_fallback_telemetry.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: get_result_set — successful retry
# ---------------------------------------------------------------------------


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_retry_succeeds_returns_non_cte_result(mock_desc):
    """When the CTE query fails but the non-CTE retry succeeds, the retry
    result is returned and the original error is suppressed."""
    sc = _make_server_connection()
    session = _make_mock_session(cte_enabled=True)
    cte_query = _make_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
    plan = _make_mock_plan(session=session, cte_queries=[cte_query])

    prog_err = ProgrammingError("CTE error")
    prog_err.sfqid = "failed_sfqid"
    retry_result = {"sfqid": "retry_sfqid", "data": [[1]]}
    non_cte_query = _make_query("SELECT 1")  # no CTE

    retry_plan_queries = {
        PlanQueryType.QUERIES: [non_cte_query],
        PlanQueryType.POST_ACTIONS: [],
    }

    with patch.object(sc, "run_query", side_effect=[prog_err, retry_result]), patch(
        "snowflake.snowpark._internal.server_connection.PlanCompiler"
    ) as MockCompiler:
        MockCompiler.return_value.compile.return_value = retry_plan_queries
        result, _ = sc.get_result_set(plan, block=True)

    assert result == retry_result
    MockCompiler.assert_called_once_with(plan)
    MockCompiler.return_value.compile.assert_called_once_with(cte_enabled=False)


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_retry_increments_fallback_count(mock_desc):
    sc = _make_server_connection()
    session = _make_mock_session(cte_enabled=True, fallback_count=0)
    plan = _make_mock_plan(session=session)

    prog_err = ProgrammingError("CTE error")
    prog_err.sfqid = "sfqid1"
    retry_result = _run_query_success()

    retry_plan_queries = _make_plan_queries()

    with patch.object(sc, "run_query", side_effect=[prog_err, retry_result]), patch(
        "snowflake.snowpark._internal.server_connection.PlanCompiler"
    ) as MockCompiler:
        MockCompiler.return_value.compile.return_value = retry_plan_queries
        sc.get_result_set(plan, block=True)

    assert session._cte_optimization_fallback_count == 1


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_retry_sends_fallback_telemetry_on_success(mock_desc):
    sc = _make_server_connection()
    session = _make_mock_session(cte_enabled=True, fallback_count=0)
    plan = _make_mock_plan(session=session)

    prog_err = ProgrammingError("CTE error")
    prog_err.sfqid = "bad_sfqid"
    retry_result = _run_query_success()
    retry_plan_queries = _make_plan_queries()

    with patch.object(sc, "run_query", side_effect=[prog_err, retry_result]), patch(
        "snowflake.snowpark._internal.server_connection.PlanCompiler"
    ) as MockCompiler:
        MockCompiler.return_value.compile.return_value = retry_plan_queries
        sc.get_result_set(plan, block=True)

    sc._telemetry_client.send_cte_execution_fallback_telemetry.assert_called_once_with(
        session_id="fake_session_id",
        plan_uuid="fake_plan_uuid",
        sfqid="bad_sfqid",
        error_type="ProgrammingError",
        error_message=str(prog_err),
        api_calls=[],
        retry_succeeded=True,
        fallback_count=1,
    )


# ---------------------------------------------------------------------------
# Tests: get_result_set — failed retry
# ---------------------------------------------------------------------------


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_retry_fails_reraises_original_error(mock_desc):
    """If the non-CTE retry also fails, the ORIGINAL error is re-raised (not
    the retry error), preserving the original sfqid for debugging."""
    sc = _make_server_connection()
    session = _make_mock_session(cte_enabled=True)
    plan = _make_mock_plan(session=session)

    original_err = ProgrammingError("original CTE error")
    original_err.sfqid = "original_sfqid"
    retry_err = ProgrammingError("retry also failed")

    retry_plan_queries = _make_plan_queries()

    with patch.object(sc, "run_query", side_effect=[original_err, retry_err]), patch(
        "snowflake.snowpark._internal.server_connection.PlanCompiler"
    ) as MockCompiler:
        MockCompiler.return_value.compile.return_value = retry_plan_queries
        with pytest.raises(ProgrammingError) as exc_info:
            sc.get_result_set(plan, block=True)

    assert exc_info.value is original_err


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_retry_fails_sends_fallback_telemetry_with_retry_succeeded_false(mock_desc):
    sc = _make_server_connection()
    session = _make_mock_session(cte_enabled=True, fallback_count=0)
    plan = _make_mock_plan(session=session)

    original_err = ProgrammingError("original error")
    original_err.sfqid = "orig_sfqid"
    retry_err = ProgrammingError("retry error")
    retry_plan_queries = _make_plan_queries()

    with patch.object(sc, "run_query", side_effect=[original_err, retry_err]), patch(
        "snowflake.snowpark._internal.server_connection.PlanCompiler"
    ) as MockCompiler:
        MockCompiler.return_value.compile.return_value = retry_plan_queries
        with pytest.raises(ProgrammingError):
            sc.get_result_set(plan, block=True)

    sc._telemetry_client.send_cte_execution_fallback_telemetry.assert_called_once()
    _, kwargs = sc._telemetry_client.send_cte_execution_fallback_telemetry.call_args
    assert kwargs["retry_succeeded"] is False
    assert kwargs["sfqid"] == "orig_sfqid"


# ---------------------------------------------------------------------------
# Tests: auto-disable
# ---------------------------------------------------------------------------


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_auto_disable_triggers_at_threshold(mock_desc):
    """After _CTE_FALLBACK_AUTO_DISABLE_THRESHOLD successful retries the
    session's CTE flag is permanently turned off."""
    sc = _make_server_connection()
    # Start just below the threshold so this call pushes it over.
    session = _make_mock_session(
        cte_enabled=True,
        fallback_count=_CTE_FALLBACK_AUTO_DISABLE_THRESHOLD - 1,
    )
    plan = _make_mock_plan(session=session)

    prog_err = ProgrammingError("CTE error")
    prog_err.sfqid = "sfqid"
    retry_result = _run_query_success()
    retry_plan_queries = _make_plan_queries()

    with patch.object(sc, "run_query", side_effect=[prog_err, retry_result]), patch(
        "snowflake.snowpark._internal.server_connection.PlanCompiler"
    ) as MockCompiler:
        MockCompiler.return_value.compile.return_value = retry_plan_queries
        sc.get_result_set(plan, block=True)

    assert session._cte_optimization_enabled is False


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_auto_disable_sends_separate_telemetry_event(mock_desc):
    sc = _make_server_connection()
    session = _make_mock_session(
        cte_enabled=True,
        fallback_count=_CTE_FALLBACK_AUTO_DISABLE_THRESHOLD - 1,
    )
    plan = _make_mock_plan(session=session)

    prog_err = ProgrammingError("CTE error")
    prog_err.sfqid = "sfqid"
    retry_result = _run_query_success()
    retry_plan_queries = _make_plan_queries()

    with patch.object(sc, "run_query", side_effect=[prog_err, retry_result]), patch(
        "snowflake.snowpark._internal.server_connection.PlanCompiler"
    ) as MockCompiler:
        MockCompiler.return_value.compile.return_value = retry_plan_queries
        sc.get_result_set(plan, block=True)

    # auto-disable event is a separate call from the per-fallback event
    sc._telemetry_client.send_cte_execution_fallback_telemetry.assert_called_once()
    sc._telemetry_client.send_cte_auto_disabled_telemetry.assert_called_once_with(
        session_id="fake_session_id",
        fallback_count=_CTE_FALLBACK_AUTO_DISABLE_THRESHOLD,
    )


@patch(
    "snowflake.snowpark._internal.server_connection.get_new_description",
    return_value=[],
)
def test_no_auto_disable_below_threshold(mock_desc):
    """fallback_count below threshold: CTE stays enabled."""
    sc = _make_server_connection()
    session = _make_mock_session(cte_enabled=True, fallback_count=0)
    plan = _make_mock_plan(session=session)

    prog_err = ProgrammingError("CTE error")
    prog_err.sfqid = "sfqid"
    retry_result = _run_query_success()
    retry_plan_queries = _make_plan_queries()

    with patch.object(sc, "run_query", side_effect=[prog_err, retry_result]), patch(
        "snowflake.snowpark._internal.server_connection.PlanCompiler"
    ) as MockCompiler:
        MockCompiler.return_value.compile.return_value = retry_plan_queries
        sc.get_result_set(plan, block=True)

    # Only triggered when count reaches threshold (3), count is now 1.
    assert session._cte_optimization_enabled is True
    sc._telemetry_client.send_cte_auto_disabled_telemetry.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: PlanCompiler.compile cte_enabled override
# ---------------------------------------------------------------------------


def test_plan_compiler_cte_enabled_false_skips_cte_step():
    """compile(cte_enabled=False) must not apply CTE optimization even when
    session.cte_optimization_enabled is True."""
    from snowflake.snowpark._internal.compiler.plan_compiler import PlanCompiler

    plan = MagicMock()
    plan.session.cte_optimization_enabled = True
    plan.session._cte_optimization_enabled = True
    plan.session.large_query_breakdown_enabled = False
    plan.session._query_compilation_stage_enabled = True

    compiler = PlanCompiler(plan)
    # When should_start_query_compilation is called with cte_enabled=False and
    # large_query_breakdown_enabled=False, it must return False.
    assert compiler.should_start_query_compilation(cte_enabled=False) is False


def test_plan_compiler_cte_enabled_none_uses_session_setting():
    """compile() with no override reads from session as before."""
    from snowflake.snowpark._internal.compiler.plan_compiler import PlanCompiler

    plan = MagicMock()
    plan.session.cte_optimization_enabled = True
    plan.session._cte_optimization_enabled = True
    plan.session.large_query_breakdown_enabled = False
    plan.session._query_compilation_stage_enabled = True
    plan.source_plan = MagicMock()

    plan.session._conn.__class__ = object  # not MockServerConnection

    compiler = PlanCompiler(plan)
    # cte_enabled=None => falls back to session.cte_optimization_enabled=True
    # source_plan is set and query_compilation_stage_enabled=True => True
    assert compiler.should_start_query_compilation(cte_enabled=None) is True


def test_plan_compiler_cte_enabled_false_with_lqb_still_compiles():
    """If LQB is enabled, compilation still proceeds even with cte_enabled=False
    because large_query_breakdown_enabled stays True."""
    from snowflake.snowpark._internal.compiler.plan_compiler import PlanCompiler

    plan = MagicMock()
    plan.session.cte_optimization_enabled = True
    plan.session._cte_optimization_enabled = True
    plan.session.large_query_breakdown_enabled = True
    plan.session._query_compilation_stage_enabled = True
    plan.source_plan = MagicMock()

    plan.session._conn.__class__ = object  # not MockServerConnection

    compiler = PlanCompiler(plan)
    assert compiler.should_start_query_compilation(cte_enabled=False) is True
