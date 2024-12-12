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
    parser.addoption(
        "--decoder",
        action="store_true",
        default=False,
        help="If set, run the decoder tests. This compares the results of the original Snowpark code with the results "
        "obtained from the generated base64 Protobuf string.",
    )


def pytest_configure(config):
    pytest.unparser_jar = config.getoption("--unparser-jar")
    pytest.decoder = config.getoption("--decoder")
    if not os.path.exists(pytest.unparser_jar):
        pytest.unparser_jar = None
    pytest.update_expectations = config.getoption("--update-expectations")

    if pytest.unparser_jar is None and pytest.update_expectations:
        raise RuntimeError(
            f"Unparser JAR not found at {pytest.unparser_jar}. "
            f"Please set the correct path with --unparser-jar or SNOWPARK_UNPARSER_JAR."
        )


class TestTables:
    def __init__(self, session) -> None:
        self._session = session

    @cached_property
    def table1(self) -> str:
        table_name: str = "table1"
        return self._save_table(
            table_name,
            [
                [1, "one"],
                [2, "two"],
                [3, "three"],
            ],
            schema=["num", "str"],
        )

    @cached_property
    def table2(self) -> str:
        table_name: str = "table2"
        return self._save_table(
            table_name,
            [
                [1, [1, 2, 3], {"Ashi Garami": "Single Leg X"}, "Kimura"],
                [2, [11, 22], {"Sankaku": "Triangle"}, "Coffee"],
                [3, [], {}, "Tea"],
            ],
            schema=["idx", "lists", "maps", "strs"],
        )

    @cached_property
    def df1_table(self) -> str:
        table_name: str = "df1"
        return self._save_table(
            table_name,
            [
                [1, 2],
                [3, 4],
            ],
            schema=["a", "b"],
        )

    @cached_property
    def df2_table(self) -> str:
        table_name: str = "df2"
        return self._save_table(
            table_name,
            [
                [0, 1],
                [3, 4],
            ],
            schema=["c", "d"],
        )

    @cached_property
    def df3_table(self) -> str:
        table_name: str = "df3"
        return self._save_table(
            table_name,
            [
                [1, 2],
            ],
            schema=["a", "b"],
        )

    @cached_property
    def df4_table(self) -> str:
        table_name: str = "df4"
        return self._save_table(
            table_name,
            [
                [2, 1],
            ],
            schema=["b", "a"],
        )

    @cached_property
    def double_quoted_table(self) -> str:
        table_name: str = '"the#qui.ck#bro.wn#""Fox""won\'t#jump!"'
        return self._save_table(
            table_name,
            [
                [1, "one"],
                [2, "two"],
                [3, "three"],
            ],
            schema=["num", 'Owner\'s""opinion.s'],
        )

    def _save_table(self, name: str, *args, **kwargs):
        kwargs.pop("_emit_ast", None)
        kwargs.pop("_ast_stmt", None)
        kwargs.pop("_ast", None)
        df = self._session.create_dataframe(*args, _emit_ast=False, **kwargs)
        logging.debug("Creating table %s", name)
        df.write.save_as_table(name, _emit_ast=False)
        return name


# For test performance (especially integration tests), it would be very valuable to create the Snowpark session and the
# temporary tables only once per test session. Unfortunately, the local testing features don't work well with any scope
# setting above "function" (e.g. "module" or "session").
# TODO: SNOW-1748311 use scope="module"
@pytest.fixture(scope="function")
def session(local_testing_mode):
    # Note: Do NOT use Session(MockServerConnection()), as this doesn't setup the correct registrations throughout snowpark.
    # Need to use the Session.builder to properly register this as active session etc.
    with Session.builder.config("local_testing", local_testing_mode).config(
        "nop_testing", True
    ).create() as s:
        s.ast_enabled = True
        s.sql_simplifier_enabled = False
        yield s


@pytest.fixture(scope="function")
def tables(session):
    return TestTables(session)
