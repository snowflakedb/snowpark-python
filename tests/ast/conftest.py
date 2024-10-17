#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import logging
import os
from functools import cached_property

import pytest

from snowflake.snowpark import Session


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
        logging.error(
            f"Unparser JAR not found at {pytest.unparser_jar}. "
            f"Please set the correct path with --unparser-jar or SNOWPARK_UNPARSER_JAR."
        )
    pytest.update_expectations = config.getoption("--update-expectations")


class Tables:
    def __init__(self, session) -> None:
        self._session = session

    @cached_property
    def table1(self) -> str:
        table_name: str = "table1"
        df = self._session.create_dataframe(
            [
                [1, "one"],
                [2, "two"],
                [3, "three"],
            ],
            schema=["num", "str"],
            _emit_ast=False,
        )
        logging.debug("Creating table %s", table_name)
        df.write.save_as_table(table_name, _emit_ast=False)
        return table_name

    @cached_property
    def double_quoted_table(self) -> str:
        table_name: str = '"the#qui.ck#bro.wn#""Fox""won\'t#jump!"'
        df = self._session.create_dataframe(
            [
                [1, "one"],
                [2, "two"],
                [3, "three"],
            ],
            schema=["num", 'Owner\'s""opinion.s'],
            _emit_ast=False,
        )
        logging.debug("Creating table %s", table_name)
        df.write.save_as_table(table_name, _emit_ast=False)
        return table_name


@pytest.fixture(scope="function")
def session():
    # Note: Do NOT use Session(MockServerConnection()), as this doesn't setup the correct registrations throughout snowpark.
    # Need to use the Session.builder to properly register this as active session etc.
    with Session.builder.config("local_testing", True).create() as s:
        s.ast_enabled = True
        yield s


@pytest.fixture(scope="function")
def tables(session):
    return Tables(session)


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"
