#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.session import Session


@pytest.fixture
def mock_server_connection() -> ServerConnection:
    fake_snowflake_connection = mock.create_autospec(SnowflakeConnection)
    fake_snowflake_connection._conn = mock.MagicMock()
    fake_snowflake_connection._telemetry = None
    fake_snowflake_connection._session_parameters = {}
    fake_snowflake_connection._thread_safe_session_enabled = True
    fake_snowflake_connection.cursor.return_value = mock.create_autospec(
        SnowflakeCursor
    )
    fake_snowflake_connection.is_closed.return_value = False
    return ServerConnection({}, fake_snowflake_connection)


@pytest.fixture
def closed_mock_server_connection() -> ServerConnection:
    fake_snowflake_connection = mock.create_autospec(SnowflakeConnection)
    fake_snowflake_connection._conn = mock.MagicMock()
    fake_snowflake_connection._telemetry = None
    fake_snowflake_connection._session_parameters = {}
    fake_snowflake_connection._thread_safe_session_enabled = True
    fake_snowflake_connection.is_closed = mock.MagicMock(return_value=False)
    fake_snowflake_connection.cursor.return_value = mock.create_autospec(
        SnowflakeCursor
    )
    fake_snowflake_connection.is_closed.return_value = False
    return ServerConnection({}, fake_snowflake_connection)


@pytest.fixture(scope="module")
def mock_query():
    fake_query = mock.create_autospec(Query)
    fake_query.sql = "dummy sql"
    fake_query.params = "dummy params"
    return fake_query


@pytest.fixture(scope="module")
def mock_snowflake_plan(mock_query) -> Analyzer:
    fake_snowflake_plan = mock.create_autospec(SnowflakePlan)
    fake_snowflake_plan._id = "dummy id"
    fake_snowflake_plan.expr_to_alias = {}
    fake_snowflake_plan.df_aliased_col_name_to_real_col_name = {}
    fake_snowflake_plan.queries = [mock_query]
    fake_snowflake_plan.post_actions = []
    fake_snowflake_plan.api_calls = []
    return fake_snowflake_plan


@pytest.fixture(scope="module")
def mock_analyzer(mock_snowflake_plan) -> Analyzer:
    def mock_resolve(x):
        mock_snowflake_plan.source_plan = x
        return mock_snowflake_plan

    fake_analyzer = mock.create_autospec(Analyzer)
    fake_analyzer.resolve.side_effect = mock_resolve
    return fake_analyzer


@pytest.fixture(scope="module")
def mock_session(mock_analyzer) -> Session:
    fake_session = mock.create_autospec(Session)
    fake_session._cte_optimization_enabled = False
    fake_session._analyzer = mock_analyzer
    fake_session._plan_lock = mock.MagicMock()
    mock_analyzer.session = fake_session
    return fake_session
