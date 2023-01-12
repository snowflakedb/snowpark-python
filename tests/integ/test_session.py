#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import os

import pytest

import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import (
    SnowparkClientException,
    SnowparkInvalidObjectNameException,
    SnowparkSessionException,
)
from snowflake.snowpark.session import (
    _PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING,
    _active_sessions,
    _get_active_session,
)
from tests.utils import IS_IN_STORED_PROC, IS_IN_STORED_PROC_LOCALFS, TestFiles, Utils


def test_select_1(session):
    res = session.sql("select 1").collect()
    assert res == [Row(1)]


def test_active_session(session):
    assert session == _get_active_session()


def test_session_builder(session):
    builder1 = session.builder
    builder2 = session.builder
    assert builder1 != builder2


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Query called from a stored procedure contains a function with side effects [SYSTEM$CANCEL_ALL_QUERIES]",
)
def test_session_cancel_all(session):
    session.cancel_all()
    qid = session._conn._cursor.sfqid
    session._conn._cursor.get_results_from_sfqid(qid)
    assert "cancelled" in session._conn._cursor.fetchall()[0][0]


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
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


def test_create_session_in_sp(session):
    import snowflake.snowpark._internal.utils as internal_utils

    original_platform = internal_utils.PLATFORM
    internal_utils.PLATFORM = "XP"
    try:
        with pytest.raises(SnowparkSessionException) as exec_info:
            Session(session._conn)
        assert exec_info.value.error_code == "1410"
    finally:
        internal_utils.PLATFORM = original_platform


def test_close_session_in_sp(session):
    import snowflake.snowpark._internal.utils as internal_utils

    original_platform = internal_utils.PLATFORM
    internal_utils.PLATFORM = "XP"
    try:
        with pytest.raises(SnowparkSessionException) as exec_info:
            session.close()
        assert exec_info.value.error_code == "1411"
    finally:
        internal_utils.PLATFORM = original_platform


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="need resources")
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


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_create_session_from_parameters(db_parameters, sql_simplifier_enabled):
    session_builder = Session.builder.configs(db_parameters)
    new_session = session_builder.create()
    new_session.sql_simplifier_enabled = sql_simplifier_enabled
    try:
        df = new_session.createDataFrame([[1, 2]], schema=["a", "b"])
        Utils.check_answer(df, [Row(1, 2)])
        assert session_builder._options["password"] is None
        assert new_session._conn._lower_case_parameters["password"] is None
        assert new_session._conn._conn._password is None
    finally:
        new_session.close()


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_create_session_from_connection(db_parameters, sql_simplifier_enabled):
    connection = snowflake.connector.connect(**db_parameters)
    session_builder = Session.builder.configs({"connection": connection})
    new_session = session_builder.create()
    new_session.sql_simplifier_enabled = sql_simplifier_enabled
    try:
        df = new_session.createDataFrame([[1, 2]], schema=["a", "b"])
        Utils.check_answer(df, [Row(1, 2)])
        assert "password" not in session_builder._options
        assert "password" not in new_session._conn._lower_case_parameters
        assert new_session._conn._conn._password is None
    finally:
        new_session.close()


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_create_session_from_connection_with_noise_parameters(
    db_parameters, sql_simplifier_enabled
):
    connection = snowflake.connector.connect(**db_parameters)
    session_builder = Session.builder.configs(
        {**db_parameters, "connection": connection}
    )
    new_session = session_builder.create()
    new_session.sql_simplifier_enabled = sql_simplifier_enabled
    try:
        df = new_session.createDataFrame([[1, 2]], schema=["a", "b"])
        Utils.check_answer(df, [Row(1, 2)])
        assert new_session._conn._conn == connection
        # Even if we don't use the password field to connect, we should still
        # erase it if it exists
        assert session_builder._options["password"] is None
        assert "password" not in new_session._conn._lower_case_parameters
        assert new_session._conn._conn._password is None
    finally:
        new_session.close()


def test_table_exists(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    assert session._table_exists(table_name) is False
    session.sql(f'create temp table "{table_name}"(col_a varchar)').collect()
    assert session._table_exists(table_name) is True

    # name in the form of "database.schema.table"
    schema = session.get_current_schema().replace('"', "")
    database = session.get_current_database().replace('"', "")
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    qualified_table_name = f"{database}.{schema}.{table_name}"
    assert session._table_exists(qualified_table_name) is False
    session.sql(f'create temp table "{table_name}"(col_a varchar)').collect()
    assert session._table_exists(qualified_table_name) is True

    # name in the form of "database..table"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    qualified_table_name = f"{database}..{table_name}"
    assert session._table_exists(qualified_table_name) is False
    session.sql(f'create temp table "{table_name}"(col_a varchar)').collect()
    assert session._table_exists(qualified_table_name) is True

    # name in the form of "schema.table"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    qualified_table_name = f"{schema}.{table_name}"
    assert session._table_exists(qualified_table_name) is False
    session.sql(f'create temp table "{table_name}"(col_a varchar)').collect()
    assert session._table_exists(qualified_table_name) is True

    # negative cases
    with pytest.raises(SnowparkClientException):
        # invalid qualified name
        session._table_exists("a.b.c.d")

    random_database = Utils.random_temp_database()
    random_schema = Utils.random_temp_schema()
    with pytest.raises(ProgrammingError):
        session._table_exists(f"{random_database}.{random_schema}.{table_name}")
    with pytest.raises(ProgrammingError):
        session._table_exists(f"{random_database}..{table_name}")
    with pytest.raises(ProgrammingError):
        session._table_exists(f"{random_schema}.{table_name}")


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_use_database(db_parameters, sql_simplifier_enabled):
    parameters = db_parameters.copy()
    del parameters["database"]
    del parameters["schema"]
    del parameters["warehouse"]
    with Session.builder.configs(parameters).create() as session:
        session.sql_simplifier_enabled = sql_simplifier_enabled
        db_name = db_parameters["database"]
        session.use_database(db_name)
        assert session.get_current_database() == f'"{db_name.upper()}"'


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_use_schema(db_parameters, sql_simplifier_enabled):
    parameters = db_parameters.copy()
    del parameters["schema"]
    del parameters["warehouse"]
    with Session.builder.configs(parameters).create() as session:
        session.sql_simplifier_enabled = sql_simplifier_enabled
        schema_name = db_parameters["schema"]
        session.use_schema(schema_name)
        assert session.get_current_schema() == f'"{schema_name.upper()}"'


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_use_warehouse(db_parameters, sql_simplifier_enabled):
    parameters = db_parameters.copy()
    del parameters["database"]
    del parameters["schema"]
    del parameters["warehouse"]
    with Session.builder.configs(db_parameters).create() as session:
        session.sql_simplifier_enabled = sql_simplifier_enabled
        warehouse_name = db_parameters["warehouse"]
        session.use_warehouse(warehouse_name)
        assert session.get_current_warehouse() == f'"{warehouse_name.upper()}"'


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_use_role(db_parameters, sql_simplifier_enabled):
    role_name = "PUBLIC"
    with Session.builder.configs(db_parameters).create() as session:
        session.sql_simplifier_enabled = sql_simplifier_enabled
        session.use_role(role_name)
        assert session.get_current_role() == f'"{role_name}"'


@pytest.mark.parametrize("obj", [None, "'object'", "obje\\ct", "obj\nect", r"\uobject"])
def test_use_negative_tests(session, obj):
    if obj:
        error_type = SnowparkInvalidObjectNameException
        err_msg = f"The object name '{obj}' is invalid."
    else:
        error_type = ValueError
        err_msg = "must not be empty or None."

    with pytest.raises(error_type) as exec_info:
        session.use_database(obj)
    assert err_msg in exec_info.value.args[0]

    with pytest.raises(error_type) as exec_info:
        session.use_schema(obj)
    assert err_msg in exec_info.value.args[0]

    with pytest.raises(error_type) as exec_info:
        session.use_warehouse(obj)
    assert err_msg in exec_info.value.args[0]

    with pytest.raises(error_type) as exec_info:
        session.use_role(obj)
    assert err_msg in exec_info.value.args[0]


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="use schema is not allowed in stored proc (owner mode)"
)
def test_get_current_schema(session):
    def check(schema_name: str, expected_name: str) -> None:
        try:
            session._run_query(f"create or replace schema {schema_name}")
            session.use_schema(schema_name)
            assert session.get_current_schema() == expected_name
        finally:
            session._run_query(f"drop schema if exists {schema_name}")

    check("a", '"A"')
    check("A", '"A"')
    check('"a b"', '"a b"')
    check('"a""b"', '"a""b"')


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="use secondary role is not allowed in stored proc (owner mode)",
)
def test_use_secondary_roles(session):
    session.use_secondary_roles("all")
    session.use_secondary_roles("none")
    session.use_secondary_roles("ALL")
    session.use_secondary_roles("NONE")
    session.use_secondary_roles(None)

    with pytest.raises(
        ProgrammingError,
        match="unexpected '<EOF>'",
    ):
        session.use_secondary_roles("")

    current_role = session.get_current_role()
    with pytest.raises(
        ProgrammingError,
        match="Object does not exist, or operation cannot be performed",
    ):
        session.use_secondary_roles(current_role)

    # after stripping the quotes actually it works - but it's not documented in Snowflake
    session.use_secondary_roles(current_role[1:-1])


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="SP doesn't allow to close a session.")
def test_close_session_twice(db_parameters):
    new_session = Session.builder.configs(db_parameters).create()
    new_session.close()
    new_session.close()  # no exception


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Can't create a session in SP")
@pytest.mark.skip(
    reason="This test passed with a local dev Snowflake env. "
    "Will be enabled soon once Snowflake publicize the sql simplifier parameter."
)
def test_sql_simplifier_disabled_on_session(db_parameters):
    with Session.builder.configs(db_parameters).create() as new_session:
        assert new_session.sql_simplifier_enabled is True
        new_session.sql_simplifier_enabled = False
        assert new_session.sql_simplifier_enabled is False
        new_session.sql_simplifier_enabled = True
        assert new_session.sql_simplifier_enabled is True

    parameters = db_parameters.copy()
    parameters["session_parameters"] = {
        _PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING: False
    }
    with Session.builder.configs(parameters).create() as new_session2:
        assert new_session2.sql_simplifier_enabled is False
