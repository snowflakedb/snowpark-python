#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import os

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.mock._connection import MockServerConnection


@pytest.fixture(scope="function")
def session():
    options = {
        "server_parameters": {"PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION": True}
    }
    with Session(MockServerConnection(options)) as s:
        yield s


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
