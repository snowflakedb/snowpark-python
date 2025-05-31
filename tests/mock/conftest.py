#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.session import _add_session, _remove_session


@pytest.fixture(scope="function")
def mock_server_connection():
    s = MockServerConnection()
    yield s
    s.close()


@pytest.fixture(scope="function")
def session(mock_server_connection):
    with Session(mock_server_connection) as s:
        _add_session(s)
        yield s
        _remove_session(s)


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
