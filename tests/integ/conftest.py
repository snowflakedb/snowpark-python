#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
import uuid
from typing import Dict

import pytest

import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.mock.mock_connection import MockServerConnection
from tests.parameters import CONNECTION_PARAMETERS
from tests.utils import Utils

RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"
RUNNING_ON_JENKINS = "JENKINS_HOME" in os.environ
TEST_SCHEMA = f"GH_JOB_{(str(uuid.uuid4()).replace('-', '_'))}"
if RUNNING_ON_JENKINS:
    TEST_SCHEMA = f"JENKINS_JOB_{(str(uuid.uuid4()).replace('-', '_'))}"


def running_on_public_ci() -> bool:
    """Whether or not tests are currently running on one of our public CIs."""
    return RUNNING_ON_GH


def running_on_jenkins() -> bool:
    """Whether or not tests are currently running on a Jenkins node."""
    return RUNNING_ON_JENKINS


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
    # If its running on our public CI or Jenkins, replace the schema
    if running_on_public_ci() or running_on_jenkins():
        # tests related to external access integration requires secrets, network rule to be created ahead
        # we keep the information of the existing schema to refer to those objects
        CONNECTION_PARAMETERS["schema_with_secret"], CONNECTION_PARAMETERS["schema"] = (
            CONNECTION_PARAMETERS["schema"],
            TEST_SCHEMA,
        )
    else:
        CONNECTION_PARAMETERS["schema_with_secret"] = CONNECTION_PARAMETERS["schema"]
    return CONNECTION_PARAMETERS


@pytest.fixture(scope="session")
def resources_path() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), "../resources"))


@pytest.fixture(scope="session")
def connection(db_parameters, local_testing_mode):
    if local_testing_mode:
        yield MockServerConnection()
    else:
        _keys = [
            "user",
            "password",
            "host",
            "port",
            "database",
            "account",
            "protocol",
            "role",
        ]
        with snowflake.connector.connect(
            **{k: db_parameters[k] for k in _keys if k in db_parameters}
        ) as con:
            yield con


@pytest.fixture(scope="session")
def is_sample_data_available(connection, local_testing_mode) -> bool:
    if local_testing_mode:
        return False
    return (
        len(
            connection.cursor()
            .execute("show databases like 'SNOWFLAKE_SAMPLE_DATA'")
            .fetchall()
        )
        > 0
    )


@pytest.fixture(scope="session", autouse=True)
def test_schema(connection, local_testing_mode) -> None:
    """Set up and tear down the test schema. This is automatically called per test session."""
    if local_testing_mode:
        yield
    else:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
            # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON SCHEMA {TEST_SCHEMA} TO ROLE PUBLIC"
            )
            yield
            cursor.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA}")


@pytest.fixture(scope="module")
def session(db_parameters, resources_path, sql_simplifier_enabled, local_testing_mode):
    if local_testing_mode:
        session = Session(MockServerConnection())
        yield session
    else:
        session = Session.builder.configs(db_parameters).create()
        session.sql_simplifier_enabled = sql_simplifier_enabled
        yield session
        session.close()


@pytest.fixture(scope="module")
def temp_schema(connection, session, local_testing_mode) -> None:
    """Set up and tear down a temp schema for cross-schema test.
    This is automatically called per test module."""
    temp_schema_name = Utils.get_fully_qualified_temp_schema(session)
    if local_testing_mode:
        yield temp_schema_name
    else:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {temp_schema_name}")
            # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON SCHEMA {temp_schema_name} TO ROLE PUBLIC"
            )
            yield temp_schema_name
            cursor.execute(f"DROP SCHEMA IF EXISTS {temp_schema_name}")
