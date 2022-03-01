#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import os

import pytest

import snowflake.connector
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.session import _active_sessions, _get_active_session
from tests.utils import TestFiles, Utils


def test_select_1(session):
    res = session.sql("select 1").collect()
    assert res == [Row(1)]


def test_active_session(session):
    assert session == _get_active_session()


def test_session_builder(session):
    builder1 = session.builder
    builder2 = session.builder
    assert builder1 != builder2


def test_session_cancel_all(session):
    session.cancel_all()
    qid = session._conn._cursor.sfqid
    session._conn._cursor.get_results_from_sfqid(qid)
    assert "cancelled" in session._conn._cursor.fetchall()[0][0]


def test_multiple_sessions(session, db_parameters):
    with Session.builder.configs(db_parameters).create():
        with pytest.raises(SnowparkSessionException) as exec_info:
            _get_active_session()
        assert exec_info.value.error_code == "1409"


def test_no_default_session():
    sessions_backup = list(_active_sessions)
    _active_sessions.clear()
    try:
        with pytest.raises(SnowparkSessionException) as exec_info:
            _get_active_session()
        assert exec_info.value.error_code == "1403"
    finally:
        _active_sessions.update(sessions_backup)


def test_create_session_in_sp(session, db_parameters):
    from snowflake.snowpark._internal.utils import Utils as SourceCodeUtils

    original_is_in_stored_procedure = SourceCodeUtils.is_in_stored_procedure
    SourceCodeUtils.is_in_stored_procedure = lambda: True
    try:
        with pytest.raises(SnowparkSessionException) as exec_info:
            Session.builder.configs(db_parameters).create()
        assert exec_info.value.error_code == "1410"
    finally:
        SourceCodeUtils.is_in_stored_procedure = original_is_in_stored_procedure


def test_close_session_in_sp(session):
    from snowflake.snowpark._internal.utils import Utils as SourceCodeUtils

    original_is_in_stored_procedure = SourceCodeUtils.is_in_stored_procedure
    SourceCodeUtils.is_in_stored_procedure = lambda: True
    try:
        with pytest.raises(SnowparkSessionException) as exec_info:
            session.close()
        assert exec_info.value.error_code == "1411"
    finally:
        SourceCodeUtils.is_in_stored_procedure = original_is_in_stored_procedure


def test_list_files_in_stage(session, resources_path):
    stage_name = Utils.random_stage_name()
    special_name = f'"{stage_name}/aa"'
    single_quoted_name = f"'{stage_name}/b\\' b'"
    test_files = TestFiles(resources_path)

    try:
        Utils.create_stage(session, stage_name)
        Utils.upload_to_stage(
            session, stage_name, test_files.test_file_avro, compress=False
        )
        files = session._list_files_in_stage(stage_name)
        assert len(files) == 1
        assert os.path.basename(test_files.test_file_avro) in files

        full_name = f"{session.get_fully_qualified_current_schema()}.{stage_name}"
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

        quoted_name = f'"{stage_name}"{prefix}'
        files4 = session._list_files_in_stage(quoted_name)
        assert len(files4) == 1
        assert os.path.basename(test_files.test_file_avro) in files4

        full_name_with_prefix = (
            f"{session.get_fully_qualified_current_schema()}.{quoted_name}"
        )
        files5 = session._list_files_in_stage(full_name_with_prefix)
        assert len(files5) == 1
        assert os.path.basename(test_files.test_file_avro) in files5

        Utils.create_stage(session, special_name, is_temporary=False)
        Utils.upload_to_stage(
            session, special_name, test_files.test_file_csv, compress=False
        )
        files6 = session._list_files_in_stage(special_name)
        assert len(files6) == 1
        assert os.path.basename(test_files.test_file_csv) in files6

        Utils.create_stage(session, single_quoted_name, is_temporary=False)
        Utils.upload_to_stage(
            session, single_quoted_name, test_files.test_file_csv, compress=False
        )
        files7 = session._list_files_in_stage(single_quoted_name)
        assert len(files7) == 1
        assert os.path.basename(test_files.test_file_csv) in files7
    finally:
        Utils.drop_stage(session, stage_name)
        Utils.drop_stage(session, special_name)
        Utils.drop_stage(session, single_quoted_name)


def test_create_session_from_connection(db_parameters):
    connection = snowflake.connector.connect(**db_parameters)
    new_session = Session.builder.configs({"connection": connection}).create()
    try:
        df = new_session.createDataFrame([[1, 2]], schema=["a", "b"])
        Utils.check_answer(df, [Row(1, 2)])
    finally:
        new_session.close()


def test_create_session_from_connection_with_noise_parameters(db_parameters):
    connection = snowflake.connector.connect(**db_parameters)
    new_session = Session.builder.configs(
        {**db_parameters, "connection": connection}
    ).create()
    try:
        df = new_session.createDataFrame([[1, 2]], schema=["a", "b"])
        Utils.check_answer(df, [Row(1, 2)])
        assert new_session._conn._conn == connection
    finally:
        new_session.close()


def test_table_exists(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    assert session._table_exists(table_name) is False
    session.sql(f'create temp table "{table_name}"(col_a varchar)').collect()
    assert session._table_exists(table_name) is True


def test_use_database(db_parameters):
    parameters = db_parameters.copy()
    del parameters["database"]
    del parameters["schema"]
    del parameters["warehouse"]
    with Session.builder.configs(parameters).create() as session:
        db_name = db_parameters["database"]
        session.use_database(db_name)
        assert session.get_current_database(unquoted=True) == db_name.upper()


def test_use_schema(db_parameters):
    parameters = db_parameters.copy()
    del parameters["schema"]
    del parameters["warehouse"]
    with Session.builder.configs(parameters).create() as session:
        schema_name = db_parameters["schema"]
        session.use_schema(schema_name)
        assert session.get_current_schema(unquoted=True) == schema_name.upper()


def test_use_warehouse(db_parameters):
    parameters = db_parameters.copy()
    del parameters["database"]
    del parameters["schema"]
    del parameters["warehouse"]
    with Session.builder.configs(db_parameters).create() as session:
        warehouse_name = db_parameters["warehouse"]
        session.use_warehouse(warehouse_name)
        assert session.get_current_warehouse(unquoted=True) == warehouse_name.upper()


def test_use_role(db_parameters):
    role_name = "PUBLIC"
    with Session.builder.configs(db_parameters).create() as session:
        session.use_role(role_name)
        assert session.get_current_role(unquoted=True) == role_name


def test_use_negative_tests(session):
    with pytest.raises(ValueError) as exec_info:
        session.use_database(None)
    assert exec_info.value.args[0] == "'database' must not be empty or None."

    with pytest.raises(ValueError) as exec_info:
        session.use_schema(None)
    assert exec_info.value.args[0] == "'schema' must not be empty or None."

    with pytest.raises(ValueError) as exec_info:
        session.use_warehouse(None)
    assert exec_info.value.args[0] == "'warehouse' must not be empty or None."

    with pytest.raises(ValueError) as exec_info:
        session.use_role(None)
    assert exec_info.value.args[0] == "'role' must not be empty or None."
