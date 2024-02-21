#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import functools
import os
import uuid
from contextlib import contextmanager
from typing import Callable, Dict

import pytest

import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.session import _get_active_session
from tests.parameters import CONNECTION_PARAMETERS
from tests.utils import Utils

RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"
TEST_SCHEMA = "GH_JOB_{}".format(str(uuid.uuid4()).replace("-", "_"))


def running_on_public_ci() -> bool:
    """Whether or not tests are currently running on one of our public CIs."""
    return RUNNING_ON_GH


def print_help() -> None:
    print(
        """Connection parameter must be specified in parameters.py,
    for example:
CONNECTION_PARAMETERS = {
    'account': 'testaccount',
    'user': 'user1',
    'password': 'test',
    'database': 'testdb',
    'schema': 'public',
}
"""
    )


@pytest.fixture(scope="session")
def db_parameters() -> Dict[str, str]:
    # If its running on our public CI, replace the schema
    if running_on_public_ci():
        CONNECTION_PARAMETERS["schema"] = TEST_SCHEMA
    return CONNECTION_PARAMETERS


@pytest.fixture(scope="session")
def resources_path() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), "../resources"))


@pytest.fixture(scope="session")
def connection(db_parameters):
    ret = db_parameters
    with snowflake.connector.connect(
        user=ret["user"],
        password=ret["password"],
        host=ret["host"],
        port=ret["port"],
        database=ret["database"],
        account=ret["account"],
        protocol=ret["protocol"],
    ) as con:
        yield con


@pytest.fixture(scope="session")
def is_sample_data_available(connection) -> bool:
    return (
        len(
            connection.cursor()
            .execute("show databases like 'SNOWFLAKE_SAMPLE_DATA'")
            .fetchall()
        )
        > 0
    )


@pytest.fixture(scope="session", autouse=True)
def test_schema(connection) -> None:
    """Set up and tear down the test schema. This is automatically called per test session."""
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
        # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
        cursor.execute(f"GRANT ALL PRIVILEGES ON SCHEMA {TEST_SCHEMA} TO ROLE PUBLIC")
        yield
        cursor.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA}")


@pytest.fixture(scope="module")
def session(db_parameters, resources_path):
    session = Session.builder.configs(db_parameters).create()
    session.sql("alter session set FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY=true")
    yield session
    session.close()


@pytest.fixture(scope="module")
def temp_schema(connection, session) -> None:
    """Set up and tear down a temp schema for cross-schema test.
    This is automatically called per test module."""
    temp_schema_name = Utils.get_fully_qualified_temp_schema(session)
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {temp_schema_name}")
        # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
        cursor.execute(
            f"GRANT ALL PRIVILEGES ON SCHEMA {temp_schema_name} TO ROLE PUBLIC"
        )
        yield temp_schema_name
        cursor.execute(f"DROP SCHEMA IF EXISTS {temp_schema_name}")
