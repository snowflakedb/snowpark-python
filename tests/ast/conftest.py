#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import logging
import os

import pytest

from snowflake.snowpark import Session


def default_unparser_path():
    explicit = os.getenv("SNOWPARK_UNPARSER_JAR")
    default_default = f"{os.getenv('HOME')}/Snowflake/trunk/Snowpark/unparser/target/scala-2.13/unparser-assembly-0.1.jar"
    return explicit or default_default


def default_encoding():
    return "json"


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
    parser.addoption(
        "--encoding",
        action="store",
        default=default_encoding(),
        type=str,
        help="Encoding for AST expectation output, values can be 'json' (default) or 'b64' for base64.",
    )


def pytest_configure(config):
    pytest.unparser_jar = config.getoption("--unparser-jar")
    if not os.path.exists(pytest.unparser_jar):
        pytest.unparser_jar = None
        logging.error(
            f"Unparser JAR not found at {pytest.unparser_jar}. "
            f"Please set the correct path with --unparser-jar or SNOWPARK_UNPARSER_JAR."
        )
    pytest.update_expectations = config.getoption("--update-expectations")
    pytest.encoding = config.getoption("--encoding", default="json")
    if pytest.encoding not in ["json", "b64"]:
        logging.error(
            f"Unrecognized encoding {pytest.encoding}, ignoring.  Using default {default_encoding()}."
        )
        pytest.encoding = default_encoding()


@pytest.fixture(scope="function")
def session():
    # Note: Do NOT use Session(MockServerConnection()), as this doesn't setup the correct registrations throughout snowpark.
    # Need to use the Session.builder to properly register this as active session etc.
    with Session.builder.config("local_testing", True).create() as s:
        s.ast_enabled = True
        yield s


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
