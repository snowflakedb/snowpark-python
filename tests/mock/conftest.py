#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import os

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.mock._connection import MockServerConnection


@pytest.fixture(scope="function")
def mock_server_connection(multithreading_mode_enabled):
    options = {
        "session_parameters": {
            "PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION": multithreading_mode_enabled
        }
    }
    s = MockServerConnection(options)
    yield s
    s.close()


@pytest.fixture(scope="function")
def session(mock_server_connection):
    with Session(mock_server_connection) as s:
        yield s


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
