#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import os

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.mock._connection import MockServerConnection


@pytest.fixture(scope="module")
def session():
    with Session(MockServerConnection()) as s:
        yield s


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
