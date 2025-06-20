#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import doctest
import logging
import os
import sys
import uuid

import pytest
from _pytest.doctest import DoctestItem

from snowflake.snowpark import Session
from snowflake.snowpark.functions import to_timestamp

logging.getLogger("snowflake.connector").setLevel(logging.ERROR)

RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"
TEST_SCHEMA = "GH_JOB_{}".format(str(uuid.uuid4()).replace("-", "_"))
LOCAL_TESTING_MODE = False

sys.path.append("tests/")
with open("tests/parameters.py", encoding="utf-8") as f:
    exec(f.read(), globals())
conn_params = globals()["CONNECTION_PARAMETERS"]


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
    with Session.builder.configs(conn_params).config(
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


@pytest.fixture(autouse=True, scope="module")
def add_doctest_imports(doctest_namespace) -> None:
    """
    Make `to_timestamp` name available for doctests.
    """
    doctest_namespace["to_timestamp"] = to_timestamp


def pytest_collection_modifyitems(config, items):
    """
    This function is used to skip some doctests on certain conditions.
    For example, some functions in PrPr might not be available on Azure and GCP.
    """
    host = (
        conn_params.get("host") or conn_params.get("HOST") or conn_params.get("account")
    )
    if any(platform in host.split(".") for platform in ["gcp", "azure"]):
        skip = pytest.mark.skip(reason="Skipping doctest for Azure and GCP deployment")
        disabled_doctests = ["ai_classify"]  # Add any test names that should be skipped
        for item in items:
            # identify doctest items
            if isinstance(item, DoctestItem):
                # match by the test’s “name” (module.name)
                if any(test_name in item.name for test_name in disabled_doctests):
                    item.add_marker(skip)
