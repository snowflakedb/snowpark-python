#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
import os
from functools import cached_property

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
import snowflake.snowpark.types as T


def default_unparser_path():
    explicit = os.getenv("MONOREPO_DIR")
    default_default = os.path.join(os.getenv("HOME"), "Snowflake/trunk")
    base_dir = explicit or default_default
    unparser_dir = os.path.join(base_dir, "bazel-bin/Snowpark/frontend/unparser")

    # Grab all *.jar files from the subtree.
    jars = []
    for path, _, files in os.walk(unparser_dir):
        jars.extend(
            [os.path.join(path, file) for file in files if file.endswith(".jar")]
        )
    return ":".join(jars)


def pytest_addoption(parser):
    parser.addoption(
        "--unparser-jar",
        action="store",
        default=default_unparser_path(),
        type=str,
        help="Path to the Unparser JAR built in the monorepo.",
    )
    parser.addoption(
        "--update-expectations",
        action="store_true",
        default=False,
        help="If set, overwrite test files with the actual output as the expected output.",
    )


def pytest_configure(config):
    unparser_jar = config.getoption("--unparser-jar")
    pytest.unparser_jar = (
        unparser_jar
        if all(os.path.exists(file) for file in unparser_jar.split(":"))
        else None
    )
    pytest.update_expectations = config.getoption("--update-expectations")

    if pytest.unparser_jar is None and pytest.update_expectations:
        raise RuntimeError(
            f"Unparser JAR not found at {unparser_jar}. "
            f"Please set the correct path with --unparser-jar or the MONOREPO_DIR environment variable."
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

    @cached_property
    def visits(self) -> str:
        table_name: str = "visits"
        return self._save_table(
            table_name,
            [
                [
                    -1,
                    -1,
                    "2025-01-31 01:02:03.004",
                    "2025-01-31 02:03:04.005",
                    "https://secure-access.globalcorp.org/account/settings",
                    "USA",
                ]
            ],
            schema=T.StructType(
                [
                    T.StructField("visit_id", T.IntegerType()),
                    T.StructField("user_id", T.IntegerType()),
                    T.StructField("start_time", T.TimestampType()),
                    T.StructField("end_time", T.TimestampType()),
                    T.StructField("page_url", T.StringType()),
                    T.StructField("country", T.StringType()),
                ]
            ),
        )

    @cached_property
    def user_profiles(self) -> str:
        table_name: str = "user_profiles"
        return self._save_table(
            table_name,
            [[-1, "John", "Doe", "1980-01-31", "Palladium"]],
            schema=T.StructType(
                [
                    T.StructField("user_id", T.IntegerType()),
                    T.StructField("first_name", T.StringType()),
                    T.StructField("last_name", T.StringType()),
                    T.StructField("dob", T.DateType()),
                    T.StructField("membership_type", T.StringType()),
                ]
            ),
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
    AST_ENABLED = True
    set_ast_state(AstFlagSource.TEST, AST_ENABLED)
    with Session.builder.config("local_testing", local_testing_mode).config(
        "nop_testing", True
    ).create() as s:
        s.ast_enabled = AST_ENABLED
        s.sql_simplifier_enabled = False
        yield s


@pytest.fixture(scope="function")
def tables(session):
    return TestTables(session)


@pytest.fixture(scope="session")
def resources_path() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), "../resources"))
