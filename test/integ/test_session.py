#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

# r
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from test.utils import Utils

import pytest
from snowflake.connector.errors import DatabaseError

from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session


def test_select_1(session_cnx):
    with session_cnx() as session:
        res = session.sql("select 1").collect()
        assert res == [Row([1])]


def test_invalid_configs(db_parameters):
    with pytest.raises(DatabaseError) as ex_info:
        session = (
            Session.builder()
            .configs(db_parameters)
            .config("user", "invalid_user")
            .config("password", "invalid_pwd")
            .config("login_timeout", 5)
            .create()
        )
        assert "Incorrect username or password was specified" in str(ex_info)
        session.close()


def test_no_default_database_and_schema(session_cnx, db_parameters):
    invalid_parameters = db_parameters.copy()
    invalid_parameters.pop("database")
    invalid_parameters.pop("schema")
    with session_cnx(invalid_parameters) as session:
        assert not session.getDefaultDatabase()
        assert not session.getDefaultSchema()
        session.close()


def test_default_and_current_database_and_schema(session_cnx):
    with session_cnx() as session:
        default_database = session.getDefaultDatabase()
        default_schema = session.getDefaultSchema()

        assert Utils.equals_ignore_case(default_database, session.getCurrentDatabase())
        assert Utils.equals_ignore_case(default_schema, session.getCurrentSchema())

        schema_name = Utils.random_name()
        session._run_query("create schema {}".format(schema_name))

        assert Utils.equals_ignore_case(default_database, session.getDefaultDatabase())
        assert Utils.equals_ignore_case(default_schema, session.getDefaultSchema())

        assert Utils.equals_ignore_case(default_database, session.getCurrentDatabase())
        assert Utils.equals_ignore_case(
            AnalyzerPackage.quote_name(schema_name), session.getCurrentSchema()
        )

        session._run_query("drop schema {}".format(schema_name))


def test_quote_all_database_and_schema_names(session_cnx):
    with session_cnx() as session:

        def is_quoted(name: str) -> bool:
            return name[0] == '"' and name[-1] == '"'

        assert is_quoted(session.getDefaultDatabase())
        assert is_quoted(session.getDefaultSchema())
        assert is_quoted(session.getCurrentDatabase())
        assert is_quoted(session.getCurrentSchema())


def test_active_session(session_cnx):
    with session_cnx() as session:
        assert session == Session._get_active_session()
