#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import pytest
from snowflake.connector.errors import DatabaseError

# TODO fix 'src.' in imports
from src.snowflake.snowpark.row import Row
from src.snowflake.snowpark.session import Session
from src.snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage


def test_select_1(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        res = session.sql('select 1').collect()
        assert res == [Row([1])]


def test_invalid_configs(db_parameters):
    invalid_parameters = db_parameters.copy()
    invalid_parameters['user'] = "invalid_user"
    invalid_parameters['password'] = "invalid_pwd"
    invalid_parameters['login_timeout'] = 5
    with pytest.raises(DatabaseError):
        Session.builder().configs(invalid_parameters).create()


def test_no_default_database_and_schema(session_cnx, db_parameters):
    invalid_parameters = db_parameters.copy()
    invalid_parameters.pop('database')
    invalid_parameters.pop('schema')
    with session_cnx(invalid_parameters) as session:
        assert not session.get_default_database()
        assert not session.get_default_schema()


def test_default_and_current_database_and_schema(session_cnx, db_parameters, utils):
    with session_cnx(db_parameters) as session:
        default_database = session.get_default_database()
        default_schema = session.get_default_schema()

        assert utils.equals_ignore_case(default_database, session.get_current_database())
        assert utils.equals_ignore_case(default_schema, session.get_current_schema())

        schema_name = utils.random_name()
        session._run_query("create schema {}".format(schema_name))

        assert utils.equals_ignore_case(default_database, session.get_default_database())
        assert utils.equals_ignore_case(default_schema, session.get_default_schema())

        assert utils.equals_ignore_case(default_database, session.get_current_database())
        assert utils.equals_ignore_case(AnalyzerPackage.quote_name(schema_name), session.get_current_schema())

        session._run_query("drop schema {}".format(schema_name))


def test_quote_all_database_and_schema_names(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        def is_quote(name: str) -> bool:
            return name[0] == '"' and name[-1] == '"'

        assert is_quote(session.get_default_database())
        assert is_quote(session.get_default_schema())
        assert is_quote(session.get_current_database())
        assert is_quote(session.get_current_schema())
