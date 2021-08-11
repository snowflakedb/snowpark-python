#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import os
import uuid
from contextlib import contextmanager
from test.parameters import CONNECTION_PARAMETERS
from typing import Callable, Dict

import pytest

import snowflake.connector
from snowflake.connector.connection import DefaultConverterClass
from snowflake.snowpark.session import Session

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


@pytest.fixture(scope="module")
def before_all():
    def do():
        pass

    return do


@pytest.fixture(scope="module")
def after_all():
    def do():
        pass

    return do


@pytest.fixture(scope="module", autouse=True)
def init_session(request, db_parameters, resources_path, before_all, after_all):
    conn_params = db_parameters.copy()
    if not conn_params.get("timezone"):
        conn_params["timezone"] = "UTC"
    if not conn_params.get("converter_class"):
        conn_params["converter_class"] = DefaultConverterClass()
    Session.builder.configs(db_parameters).create()
    before_all()

    def fin():
        after_all()
        active_session = Session._get_active_session()
        if active_session:
            active_session.close()

    request.addfinalizer(fin)


@pytest.fixture(scope="session", autouse=True)
def init_test_schema(request, db_parameters) -> None:
    """Initializes and Deinitializes the test schema. This is automatically called per test session."""
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
        con.cursor().execute("CREATE SCHEMA IF NOT EXISTS {}".format(TEST_SCHEMA))
        # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
        con.cursor().execute(
            "GRANT ALL PRIVILEGES TO SCHEMA {} TO ROLE PUBLIC".format(TEST_SCHEMA)
        )

    def fin():
        ret1 = db_parameters
        with snowflake.connector.connect(
            user=ret1["user"],
            password=ret1["password"],
            host=ret1["host"],
            port=ret1["port"],
            database=ret1["database"],
            account=ret1["account"],
            protocol=ret1["protocol"],
        ) as con1:
            con1.cursor().execute("DROP SCHEMA IF EXISTS {}".format(TEST_SCHEMA))

    request.addfinalizer(fin)


@pytest.fixture(scope="session")
def db_parameters() -> Dict[str, str]:
    # If its running on our public CI, replace the schema
    if running_on_public_ci():
        CONNECTION_PARAMETERS["schema"] = TEST_SCHEMA
    return CONNECTION_PARAMETERS


@pytest.fixture(scope="session")
def resources_path() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), "../resources"))


@pytest.fixture(scope="module")
def session_cnx() -> Callable[..., "Session"]:
    return get_session


@contextmanager
def get_session(conn_params=None):
    if conn_params or not Session._get_active_session():
        session = Session.builder.configs(conn_params or CONNECTION_PARAMETERS).create()
    else:
        session = Session._get_active_session()
    yield session
