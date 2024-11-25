#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import doctest
import logging
import os
import sys
import uuid

import pytest

from snowflake.snowpark import Session

logging.getLogger("snowflake.connector").setLevel(logging.ERROR)

RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"
TEST_SCHEMA = "GH_JOB_{}".format(str(uuid.uuid4()).replace("-", "_"))
LOCAL_TESTING_MODE = False


def pytest_addoption(parser):
    if not any(
        "--local_testing_mode" in opt.names() for opt in parser._anonymous.options
    ):
        parser.addoption("--local_testing_mode", action="store_true", default=False)


def pytest_runtest_makereport(item, call):
    from _pytest.runner import pytest_runtest_makereport as _pytest_runtest_makereport

    tr = _pytest_runtest_makereport(item, call)

    if call.excinfo is not None and LOCAL_TESTING_MODE:
        if call.excinfo.type == doctest.UnexpectedException and isinstance(
            call.excinfo.value.exc_info[1], NotImplementedError
        ):
            tr.outcome = "skipped"
            tr.wasxfail = "[Local Testing] Function has not been implemented yet."

    return tr


# scope is module so that we ensure we delete the session before
# moving onto running the tests in the tests dir. Having only one
# session is important to certain UDF tests to pass , since they
# use the @udf decorator
@pytest.fixture(autouse=True, scope="module")
def add_snowpark_session(doctest_namespace, pytestconfig):
    global LOCAL_TESTING_MODE
    LOCAL_TESTING_MODE = pytestconfig.getoption("local_testing_mode")
    sys.path.append("tests/")
    with open("tests/parameters.py", encoding="utf-8") as f:
        exec(f.read(), globals())
    with Session.builder.configs(globals()["CONNECTION_PARAMETERS"]).config(
        "local_testing", LOCAL_TESTING_MODE
    ).create() as session:
        session.sql_simplifier_enabled = (
            os.environ.get("USE_SQL_SIMPLIFIER") == "1" or LOCAL_TESTING_MODE
        )
        if RUNNING_ON_GH:
            session.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}").collect()
            # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
            session.sql(
                f"GRANT ALL PRIVILEGES ON SCHEMA {TEST_SCHEMA} TO ROLE PUBLIC"
            ).collect()
            session.use_schema(TEST_SCHEMA)
        doctest_namespace["session"] = session
        yield
        LOCAL_TESTING_MODE = False
        if RUNNING_ON_GH:
            session.sql(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA}").collect()
