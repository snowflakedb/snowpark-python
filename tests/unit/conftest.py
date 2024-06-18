#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.session import Session


@pytest.fixture
def mock_server_connection() -> ServerConnection:
    fake_snowflake_connection = mock.create_autospec(SnowflakeConnection)
    fake_snowflake_connection._telemetry = None
    fake_snowflake_connection._session_parameters = {}
    fake_snowflake_connection.cursor.return_value = mock.create_autospec(
        SnowflakeCursor
    )
    fake_snowflake_connection.is_closed.return_value = False
    return ServerConnection({}, fake_snowflake_connection)


@pytest.fixture(scope="module")
def mock_analyzer() -> Analyzer:
    fake_analyzer = mock.create_autospec(Analyzer)
    return fake_analyzer


@pytest.fixture(scope="module")
def mock_session(mock_analyzer) -> Session:
    fake_session = mock.create_autospec(Session)
    fake_session._analyzer = mock_analyzer
    mock_analyzer.session = fake_session
    return fake_session


@pytest.fixture(scope="module")
def mock_query():
    fake_query = mock.create_autospec(Query)
    fake_query.sql = "dummy sql"
    fake_query.params = "dummy params"
    return fake_query
