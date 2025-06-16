#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os

import pytest
from snowflake.snowpark import Session
from snowflake.snowpark.mock._connection import MockServerConnection


@pytest.fixture(scope="function")
def mock_server_connection():
    s = MockServerConnection()
    yield s
    s.close()


@pytest.fixture(scope="function")
def session(mock_server_connection):
    with Session.builder.config("local_testing", True).create() as s:
        yield s


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
