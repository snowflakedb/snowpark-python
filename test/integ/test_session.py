#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import os
from test.utils import TestFiles, Utils

import pytest

from snowflake.connector.errors import DatabaseError
from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session


def test_select_1(session_cnx):
    with session_cnx() as session:
        res = session.sql("select 1").collect()
        assert res == [Row(1)]


def test_invalid_configs(db_parameters):
    with pytest.raises(DatabaseError) as ex_info:
        session = (
            Session.builder.configs(db_parameters)
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

        try:
            schema_name = Utils.random_name()
            session._run_query("create schema {}".format(schema_name))

            assert Utils.equals_ignore_case(
                default_database, session.getDefaultDatabase()
            )
            assert Utils.equals_ignore_case(default_schema, session.getDefaultSchema())

            assert Utils.equals_ignore_case(
                default_database, session.getCurrentDatabase()
            )
            assert Utils.equals_ignore_case(
                AnalyzerPackage.quote_name(schema_name), session.getCurrentSchema()
            )
        finally:
            # restore
            session._run_query("drop schema if exists {}".format(schema_name))
            session._run_query("use schema {}".format(default_schema))


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


def test_list_files_in_stage(session_cnx, resources_path):
    stage_name = Utils.random_stage_name()
    special_name = f'"{stage_name}/aa"'
    test_files = TestFiles(resources_path)
    with session_cnx() as session:
        try:
            Utils.create_stage(session, stage_name)
            Utils.upload_to_stage(
                session, stage_name, test_files.test_file_avro, compress=False
            )
            files = session._list_files_in_stage(stage_name)
            assert len(files) == 1
            assert os.path.basename(test_files.test_file_avro) in files

            full_name = f"{session.getFullyQualifiedCurrentSchema()}.{stage_name}"
            files2 = session._list_files_in_stage(full_name)
            assert len(files2) == 1
            assert os.path.basename(test_files.test_file_avro) in files2

            prefix = "/prefix/prefix2"
            with_prefix = stage_name + prefix
            Utils.upload_to_stage(
                session, with_prefix, test_files.test_file_avro, compress=False
            )
            files3 = session._list_files_in_stage(with_prefix)
            assert len(files3) == 1
            assert os.path.basename(test_files.test_file_avro) in files3

            # TODO: SNOW-425907 the following three test cases are not working
            # because currently list_files_in_stage function only supports
            # the simple parsing rule, for session stage in particular
            # uncomment them once we figure out how to get the normalized
            # stage location

            # quoted_name = f'"{stage_name}"{prefix}'
            # files4 = session._list_files_in_stage(quoted_name)
            # assert len(files4) == 1
            # assert os.path.basename(test_files.test_file_avro) in files4

            # full_name_with_prefix = (
            #     f"{session.getFullyQualifiedCurrentSchema()}.{quoted_name}"
            # )
            # files5 = session._list_files_in_stage(full_name_with_prefix)
            # assert len(files5) == 1
            # assert os.path.basename(test_files.test_file_avro) in files5
            #
            # Utils.create_stage(session, special_name)
            # Utils.upload_to_stage(
            #     session, special_name, test_files.test_file_csv, compress=False
            # )
            # files6 = session._list_files_in_stage(special_name)
            # assert len(files6) == 1
            # assert os.path.basename(test_files.test_file_csv) in files6
        finally:
            Utils.drop_stage(session, stage_name)
            Utils.drop_stage(session, special_name)
