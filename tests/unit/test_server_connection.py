#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io
import logging
import os
from unittest import mock
from unittest.mock import MagicMock

import pytest

from snowflake.snowpark import Session
from snowflake.connector.errors import ProgrammingError

from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    BatchInsertQuery,
    PlanQueryType,
    Query,
    SnowflakePlan,
)
from snowflake.snowpark._internal.utils import IS_V5_DRIVER

if IS_V5_DRIVER:
    from snowflake.connector.errors import ReauthenticationRequest
else:
    from snowflake.connector.network import ReauthenticationRequest
from snowflake.snowpark.exceptions import (
    SnowparkFetchDataException,
    SnowparkQueryCancelledException,
    SnowparkSessionException,
    SnowparkSQLException,
    SnowparkUploadFileException,
    SnowparkUploadUdfFileException,
)


def test_wrap_exception(mock_server_connection):
    with mock.patch.object(
        mock_server_connection._cursor,
        "execute",
        side_effect=ReauthenticationRequest("fake exception"),
    ):
        with pytest.raises(
            SnowparkSessionException, match="Your Snowpark session has expired"
        ):
            mock_server_connection.run_query("fake query")

    with mock.patch.object(
        mock_server_connection._cursor,
        "execute",
        side_effect=Exception("fake exception"),
    ):
        with pytest.raises(Exception, match="fake exception"):
            mock_server_connection.run_query("fake query")

    with mock.patch.object(
        mock_server_connection._conn, "is_closed", return_value=True
    ):
        with pytest.raises(
            SnowparkSessionException, match="the session has been closed"
        ):
            mock_server_connection.get_session_id()


def test_upload_stream_exceptions(mock_server_connection):
    with mock.patch.object(
        mock_server_connection._cursor,
        "execute",
        side_effect=ValueError("fake exception"),
    ):
        input_stream = io.BytesIO(b"fake stream")
        with pytest.raises(ValueError, match="fake exception"):
            mock_server_connection.upload_stream(
                input_stream, "fake_state", "fake_file_name"
            )

        input_stream.close()
        with pytest.raises(
            SnowparkUploadUdfFileException,
            match="A file stream was closed when uploading UDF files",
        ):
            mock_server_connection.upload_stream(
                input_stream, "fake_state", "fake_file_name", is_in_udf=True
            )

        with pytest.raises(
            SnowparkUploadFileException,
            match="A file stream was closed when uploading files to the server",
        ):
            mock_server_connection.upload_stream(
                input_stream, "fake_state", "fake_file_name"
            )


def test_run_query_exceptions(mock_server_connection, caplog):
    with mock.patch.object(
        mock_server_connection._cursor,
        "execute",
        side_effect=Exception("fake exception"),
    ):
        with pytest.raises(Exception, match="fake exception"):
            with caplog.at_level(logging.ERROR):
                mock_server_connection.run_query("fake query", log_on_exception=True)
        assert "Failed to execute query" in caplog.text

    mock_server_connection._cursor.execute.return_value = mock_server_connection._cursor
    mock_server_connection._cursor.sfqid = "fake id"
    mock_server_connection._cursor.query = "fake query"
    mock_server_connection._cursor._query_result_format = "arrow"
    if IS_V5_DRIVER:
        mock_server_connection._cursor.request_id = "1234"
    else:
        mock_server_connection._cursor._request_id = "1234"
    with mock.patch.object(
        mock_server_connection._cursor,
        "fetch_pandas_all",
        side_effect=KeyboardInterrupt("fake exception"),
    ):
        with pytest.raises(KeyboardInterrupt, match="fake exception"):
            mock_server_connection.run_query("fake query", to_pandas=True)

    with mock.patch.object(
        mock_server_connection._cursor,
        "fetch_pandas_all",
        side_effect=BaseException("fake exception"),
    ):
        with pytest.raises(
            SnowparkFetchDataException, match="Failed to fetch a pandas Dataframe"
        ):
            mock_server_connection.run_query("fake query", to_pandas=True)


def test_get_result_set_exception(mock_server_connection):
    fake_session = mock.create_autospec(Session)
    fake_session._collect_snowflake_plan_telemetry_at_critical_path = True
    fake_session._generate_new_action_id.return_value = 1
    fake_session._last_canceled_id = 100
    fake_session._conn = mock_server_connection
    fake_session._cte_optimization_enabled = False
    fake_session._join_alias_fix = False
    fake_session._query_compilation_stage_enabled = False
    fake_plan = SnowflakePlan(
        queries=[Query("fake query 1"), Query("fake query 2")],
        schema_query="fake schema query",
        session=fake_session,
    )
    with pytest.raises(
        SnowparkQueryCancelledException,
        match="The query has been cancelled by the user",
    ):
        mock_server_connection.get_result_set(fake_plan)

    with pytest.raises(
        SnowparkQueryCancelledException,
        match="The query has been cancelled by the user",
    ):
        mock_server_connection.get_result_set(fake_plan, block=False)

    fake_session._last_canceled_id = 0
    with mock.patch.object(mock_server_connection, "run_query", return_value=None):
        with pytest.raises(SnowparkSQLException, match="doesn't return a ResultSet"):
            mock_server_connection.get_result_set(fake_plan, block=False)


def test_run_query_when_ignore_results_true(mock_server_connection):
    mock_cursor1 = MagicMock()
    mock_cursor1.sfqid = "ignore_results is True"

    mock_server_connection.execute_and_notify_query_listener = MagicMock()
    mock_server_connection.execute_and_notify_query_listener.return_value = mock_cursor1

    mock_server_connection._to_data_or_iter = MagicMock()
    mock_server_connection._to_data_or_iter.return_value = {
        "sfqid": "ignore_results is False"
    }

    result = mock_server_connection.run_query(
        "select * from fake_table", ignore_results=True
    )
    mock_server_connection._to_data_or_iter.assert_not_called()
    assert "sfqid" in result and result["sfqid"] == "ignore_results is True"

    result = mock_server_connection.run_query(
        "select * from fake_table", ignore_results=False
    )
    mock_server_connection._to_data_or_iter.assert_called()
    assert "sfqid" in result and result["sfqid"] == "ignore_results is False"


def test_add_application_parameters_with_multiple_applications(mock_server_connection):
    """Test that snowbook works alongside other application detections."""
    with mock.patch("importlib.util.find_spec") as mock_find_spec:
        mock_find_spec.side_effect = (
            lambda name: mock.MagicMock()
            if name in ["streamlit", "snowflake.ml"]
            else None
        )

        mock_server_connection._lower_case_parameters = {}
        mock_server_connection._add_application_parameters()

        assert (
            mock_server_connection._lower_case_parameters["application"]
            == "streamlit:SnowparkML"
        )


def test_snowbook_detection_overrides_application(mock_server_connection):
    """Test that 'notebook' is added when snowbook is available."""
    with mock.patch("importlib.util.find_spec") as mock_find_spec:
        mock_find_spec.side_effect = (
            lambda name: mock.MagicMock()
            if name in ["streamlit", "snowflake.ml", "snowbook"]
            else None
        )

        mock_server_connection._lower_case_parameters = {}
        mock_server_connection._add_application_parameters()

        assert (
            mock_server_connection._lower_case_parameters["application"]
            == "Snowflake Web App (snowsight_notebook)"
        )


def test_env_var_partner_takes_precedence(mock_server_connection):
    """Test that ENV_VAR_PARTNER takes precedence over module detection."""
    with mock.patch.dict(os.environ, {"SF_PARTNER": "custom_partner"}):
        with mock.patch("importlib.util.find_spec", return_value=mock.MagicMock()):
            mock_server_connection._lower_case_parameters = {}
            mock_server_connection._add_application_parameters()

            assert (
                mock_server_connection._lower_case_parameters["application"]
                == "custom_partner"
            )


def test_existing_application_param_not_overwritten(mock_server_connection):
    """Test that existing application parameter is preserved."""
    with mock.patch("importlib.util.find_spec") as mock_find_spec:
        mock_find_spec.side_effect = (
            lambda name: mock.MagicMock()
            if name in ["streamlit", "snowflake.ml"]
            else None
        )

        mock_server_connection._lower_case_parameters = {"application": "existing_app"}
        mock_server_connection._add_application_parameters()

        assert (
            mock_server_connection._lower_case_parameters["application"]
            == "existing_app"
        )


@pytest.mark.skipif(not IS_V5_DRIVER, reason="force_json_fallback only applies to v5+")
@pytest.mark.parametrize(
    "from_query_id,expected_pandas_fetch",
    [(False, False), (True, True)],
)
def test_to_data_or_iter_exempts_query_id_results_from_json_fallback(
    mock_server_connection, from_query_id, expected_pandas_fetch
):
    """A JSON-format result loaded by query id is fetched as pandas, not through the fallback.

    Pre-v5 drivers load such a result by re-running RESULT_SCAN, so their format
    reads "arrow" there and the fallback never ran.
    """
    cursor = MagicMock()
    cursor.sfqid = "fake id"
    cursor._query_result_format = "json"

    result = mock_server_connection._to_data_or_iter(
        cursor, to_pandas=True, from_query_id=from_query_id
    )["data"]

    assert cursor.fetch_pandas_all.called is expected_pandas_fetch
    assert cursor.fetchall.called is not expected_pandas_fetch
    if expected_pandas_fetch:
        assert result is not cursor.fetchall.return_value


def _fake_plan_for_execute(mock_server_connection, queries):
    fake_session = mock.create_autospec(Session)
    fake_session._collect_snowflake_plan_telemetry_at_critical_path = False
    fake_session._last_canceled_id = 0
    fake_session._conn = mock_server_connection
    fake_session._join_alias_fix = False
    return SnowflakePlan(
        queries=queries, schema_query="fake schema query", session=fake_session
    )


@pytest.mark.parametrize("failing_index", [0, 1, 2])
def test_execute_queries_tags_failing_query(mock_server_connection, failing_index):
    queries = [Query("q0"), Query("q1"), Query("q2")]
    plan = _fake_plan_for_execute(mock_server_connection, queries)
    post_action = Query("drop q")

    def run_query(sql, *args, **kwargs):
        if sql == f"q{failing_index}":
            raise ProgrammingError(msg="fake error")
        return {"sfqid": f"id_{sql}"}

    with mock.patch.object(
        mock_server_connection, "run_query", side_effect=run_query
    ) as mocked_run_query, mock.patch(
        "snowflake.snowpark._internal.server_connection.get_new_description"
    ):
        with pytest.raises(ProgrammingError) as exc_info:
            mock_server_connection._execute_queries(
                plan,
                {
                    PlanQueryType.QUERIES: queries,
                    PlanQueryType.POST_ACTIONS: [post_action],
                },
                action_id=1,
            )
    assert exc_info.value._snowpark_failed_query is queries[failing_index]
    assert exc_info.value._snowpark_failed_query_is_last is (failing_index == 2)
    # post actions still run after the failure
    assert mocked_run_query.call_args_list[-1].args[0] == "drop q"


def test_execute_queries_tags_failing_batch_insert(mock_server_connection):
    queries = [Query("create"), BatchInsertQuery("insert", []), Query("select")]
    plan = _fake_plan_for_execute(mock_server_connection, queries)
    with mock.patch.object(
        mock_server_connection, "run_query", return_value={"sfqid": "id"}
    ), mock.patch.object(
        mock_server_connection,
        "run_batch_insert",
        side_effect=ProgrammingError(msg="fake error"),
    ), mock.patch(
        "snowflake.snowpark._internal.server_connection.get_new_description"
    ):
        with pytest.raises(ProgrammingError) as exc_info:
            mock_server_connection._execute_queries(
                plan,
                {PlanQueryType.QUERIES: queries, PlanQueryType.POST_ACTIONS: []},
                action_id=1,
            )
    assert exc_info.value._snowpark_failed_query is queries[1]
    assert exc_info.value._snowpark_failed_query_is_last is False


def test_execute_queries_does_not_tag_async_multi_statement(mock_server_connection):
    # All queries are sent as one multi-statement request, so the failing one is unknown.
    queries = [Query("q0"), Query("q1")]
    plan = _fake_plan_for_execute(mock_server_connection, queries)
    with mock.patch.object(
        mock_server_connection,
        "run_query",
        side_effect=ProgrammingError(msg="fake error"),
    ):
        with pytest.raises(ProgrammingError) as exc_info:
            mock_server_connection._execute_queries(
                plan,
                {PlanQueryType.QUERIES: queries, PlanQueryType.POST_ACTIONS: []},
                action_id=1,
                block=False,
            )
    assert not hasattr(exc_info.value, "_snowpark_failed_query")
