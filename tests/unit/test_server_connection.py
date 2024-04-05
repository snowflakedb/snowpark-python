#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import io
import logging
from unittest import mock

import pytest

from snowflake.connector.network import ReauthenticationRequest
from snowflake.snowpark import Session
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
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
    fake_session._generate_new_action_id.return_value = 1
    fake_session._last_canceled_id = 100
    fake_session._conn = mock_server_connection
    fake_session._cte_optimization_enabled = False
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
