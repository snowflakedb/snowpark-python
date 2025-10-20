#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
from types import SimpleNamespace
from unittest import mock
from unittest.mock import MagicMock
from enum import Enum

import pytest

from snowflake.connector.errors import DatabaseError
from snowflake.snowpark.async_job import AsyncJob
from snowflake.snowpark.session import Session


class TsetOnlyCancelQueryErrorType(Enum):
    VALID_JSON_BUT_FAIL = 0
    INVALID_JSON = 1


def _make_session_with_cursor() -> tuple[Session, mock.MagicMock]:
    server_conn = MagicMock()
    inner_conn = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    inner_conn.cursor.return_value = mock_cursor
    server_conn._conn = inner_conn

    # Return True for async feature flag.
    def _param_side_effect(name, default_value):
        if name and name.upper() == "ENABLE_ASYNC_QUERY_IN_PYTHON_STORED_PROCS":
            return True
        return default_value

    server_conn._get_client_side_session_parameter.side_effect = _param_side_effect
    session = Session(server_conn)
    return session, mock_cursor


def test_async_job_cancel_executes_sys_func_in_regular_client(monkeypatch):
    session, mock_cursor = _make_session_with_cursor()

    # Test with regular client / non-sproc path.
    monkeypatch.setattr(
        "snowflake.snowpark.async_job.is_in_stored_procedure", lambda: False
    )

    job = AsyncJob(query_id="qid-123", query=None, session=session)
    job.cancel()

    mock_cursor.execute.assert_called_once_with("select SYSTEM$CANCEL_QUERY('qid-123')")


def test_async_job_cancel_in_sproc_success(monkeypatch):
    session, mock_cursor = _make_session_with_cursor()

    # Test with sproc path.
    monkeypatch.setattr(
        "snowflake.snowpark.async_job.is_in_stored_procedure", lambda: True
    )

    # Inject a fake _snowflake module with a mock cancel_query.
    cancel_mock = mock.MagicMock(return_value='{"success": true}')
    fake_mod = SimpleNamespace(cancel_query=cancel_mock)
    monkeypatch.setitem(sys.modules, "_snowflake", fake_mod)

    qid = "qid-abc"
    job = AsyncJob(query_id=qid, query=None, session=session)
    job.cancel()  # should not raise

    # Verify that _snowflake.cancel_query is called with given query id.
    cancel_mock.assert_called_once_with(qid)
    # Verify that the system function is not called.
    mock_cursor.execute.assert_not_called()


@pytest.mark.parametrize(
    "fake_response_and_type",
    [
        # Valid JSON, but success=false.
        (
            '{"success": false, "error": "boom"}',
            TsetOnlyCancelQueryErrorType.VALID_JSON_BUT_FAIL,
        ),
        # Invalid JSON.
        ("{", TsetOnlyCancelQueryErrorType.INVALID_JSON),
    ],
)
def test_async_job_cancel_in_sproc_failure_raises(monkeypatch, fake_response_and_type):
    session, _ = _make_session_with_cursor()

    # Test with sproc path.
    monkeypatch.setattr(
        "snowflake.snowpark.async_job.is_in_stored_procedure", lambda: True
    )

    # Mock _snowflake.cancel_query to return error response as instructed to.
    fake_response, error_type = fake_response_and_type
    fake_mod = SimpleNamespace(cancel_query=lambda qid: fake_response)
    monkeypatch.setitem(sys.modules, "_snowflake", fake_mod)

    job = AsyncJob(query_id="qid-fail", query=None, session=session)
    with pytest.raises(DatabaseError, match="Failed to cancel query") as exc_info:
        job.cancel()
    if error_type == TsetOnlyCancelQueryErrorType.VALID_JSON_BUT_FAIL:
        assert "Error parsing response" not in str(exc_info.value)
    elif error_type == TsetOnlyCancelQueryErrorType.INVALID_JSON:
        assert "Error parsing response" in str(exc_info.value)
    else:
        raise ValueError(f"Invalid test case: {fake_response_and_type}")
