#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import uuid
from contextlib import contextmanager
from typing import Callable, Dict
import pytest
from snowflake.connector.connection import DefaultConverterClass

# TODO fix '.src'
from src.snowflake.snowpark.session import Session
from ..parameters import CONNECTION_PARAMETERS


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


class Utils:
    @staticmethod
    def random_name() -> str:
        return "SN_TEST_OBJECT_{}".format(str(uuid.uuid4()).replace("-", "_"))

    @staticmethod
    def create_table(session: 'Session', name: str, schema: str):
        session._run_query("create or replace table {name} ({schema})".format(name=name, schema=schema))

    @staticmethod
    def drop_table(session: 'Session', name: str):
        session._run_query("drop table if exists {name}".format(name=name))

    @staticmethod
    def equals_ignore_case(a: str, b: str) -> bool:
        return a.lower() == b.lower()


@pytest.fixture()
def utils():
    return Utils


@pytest.fixture(scope="session")
def db_parameters() -> Dict[str, str]:
    return CONNECTION_PARAMETERS


@pytest.fixture()
def session_cnx() -> Callable[..., "Session"]:
    return get_session


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
