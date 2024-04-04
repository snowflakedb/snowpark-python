#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.snowpark._internal.server_connection import ServerConnection


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
