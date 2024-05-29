#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.mock._connection import MockServerConnection

def pytest_addoption(parser):
    parser.addoption(
        "--update-expectations",
        action="store_true",
        default=False,
        help="If set, overwrite test files with the actual output as the expected output",
    )


def pytest_configure(config):
    pytest.update_expectations = config.getoption("--update-expectations")


@pytest.fixture(scope="function")
def session():
    with Session(MockServerConnection()) as s:
        yield s


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
