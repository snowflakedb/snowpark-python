#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.mock._connection import MockServerConnection


def default_unparser_path():
    explicit = os.getenv("SNOWPARK_UNPARSER_JAR")
    default_default = f"{os.getenv('HOME')}/Snowflake/trunk/Snowpark/unparser/target/scala-2.13/unparser-assembly-0.1.jar"
    return explicit or default_default


def pytest_addoption(parser):
    parser.addoption(
        "--unparser-jar",
        action="store",
        default=default_unparser_path(),
        type=str,
        help="Path to the Unparser JAR built in the monorepo. To build it, run `sbt assembly` from the unparser directory.",
    )
    parser.addoption(
        "--update-expectations",
        action="store_true",
        default=False,
        help="If set, overwrite test files with the actual output as the expected output.",
    )


def pytest_configure(config):
    pytest.unparser_jar = config.getoption("--unparser-jar")
    if not os.path.exists(pytest.unparser_jar):
        pytest.unparser_jar = None
    pytest.update_expectations = config.getoption("--update-expectations")


@pytest.fixture(scope="function")
def session():
    with Session(MockServerConnection()) as s:
        yield s


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
