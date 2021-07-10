#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from contextlib import contextmanager
from test.parameters import CONNECTION_PARAMETERS
from typing import Callable, Dict

import os
import pytest

from snowflake.connector.connection import DefaultConverterClass
from snowflake.snowpark.session import Session


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
    return CONNECTION_PARAMETERS


@pytest.fixture()
def session_cnx() -> Callable[..., "Session"]:
    return get_session


@pytest.fixture()
def resources_path() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), '../resources'))

@contextmanager
def get_session(conn_params):
    if not conn_params.get("timezone"):
        conn_params["timezone"] = "UTC"
    if not conn_params.get("converter_class"):
        conn_params["converter_class"] = DefaultConverterClass()
    session = Session.builder().configs(conn_params).create()
    try:
        yield session
    finally:
        session.close()
