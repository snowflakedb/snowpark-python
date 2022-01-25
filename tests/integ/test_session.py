#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import os

import snowflake.connector
from snowflake.snowpark import Row, Session
from tests.utils import TestFiles, Utils


def test_select_1(session):
    res = session.sql("select 1").collect()
    assert res == [Row(1)]


def test_active_session(session):
    assert session == Session._get_active_session()


def test_session_builder(session):
    builder1 = session.builder
    builder2 = session.builder
    assert builder1 != builder2


def test_session_cancel_all(session):
    session.cancel_all()
    qid = session._conn._cursor.sfqid
    session._conn._cursor.get_results_from_sfqid(qid)
    assert "cancelled" in session._conn._cursor.fetchall()[0][0]


def test_list_files_in_stage(session, resources_path):
    stage_name = Utils.random_stage_name()
    special_name = f'"{stage_name}/aa"'
    test_files = TestFiles(resources_path)

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

        quoted_name = f'"{stage_name}"{prefix}'
        files4 = session._list_files_in_stage(quoted_name)
        assert len(files4) == 1
        assert os.path.basename(test_files.test_file_avro) in files4

        full_name_with_prefix = (
            f"{session.getFullyQualifiedCurrentSchema()}.{quoted_name}"
        )
        files5 = session._list_files_in_stage(full_name_with_prefix)
        assert len(files5) == 1
        assert os.path.basename(test_files.test_file_avro) in files5

        Utils.create_stage(session, special_name)
        Utils.upload_to_stage(
            session, special_name, test_files.test_file_csv, compress=False
        )
        files6 = session._list_files_in_stage(special_name)
        assert len(files6) == 1
        assert os.path.basename(test_files.test_file_csv) in files6
    finally:
        Utils.drop_stage(session, stage_name)
        Utils.drop_stage(session, special_name)


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
    table_name = Utils.random_name()
    assert session._table_exists(table_name) is False
    session.sql(f'create temp table "{table_name}"(col_a varchar)').collect()
    assert session._table_exists(table_name) is True
