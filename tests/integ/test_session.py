#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
from functools import partial

import pytest
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import TempObjectType, parse_table_name
from snowflake.snowpark.exceptions import (
    SnowparkClientException,
    SnowparkInvalidObjectNameException,
    SnowparkSessionException,
)
from snowflake.snowpark.session import (
    _PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING,
    _active_sessions,
    _get_active_session,
    _get_active_sessions,
)

from tests.utils import IS_IN_STORED_PROC, IS_IN_STORED_PROC_LOCALFS, TestFiles, Utils


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_runtime_config(db_parameters):
    session = (
        Session.builder.configs(db_parameters)
        .config("client_prefetch_threads", 10)
        .config("sql_simplifier_enabled", False)
        .config("use_constant_subquery_alias", False)
        .create()
    )
    # test conf.get
    assert not session.conf.get("nonexistent_client_side_fix", default=False)
    assert session.conf.get("client_prefetch_threads") == 10
    assert not session.sql_simplifier_enabled
    assert not session.conf.get("sql_simplifier_enabled")
    assert not session.conf.get("use_constant_subquery_alias")
    assert session.conf.get("password") is None

    # test conf.is_mutable
    assert session.conf.is_mutable("telemetry_enabled")
    assert session.conf.is_mutable("sql_simplifier_enabled")
    assert session.conf.is_mutable("use_constant_subquery_alias")
    assert not session.conf.is_mutable("host")
    assert not session.conf.is_mutable("is_pyformat")

    # test conf.set
    session.conf.set("sql_simplifier_enabled", True)
    assert session.sql_simplifier_enabled
    assert session.conf.get("sql_simplifier_enabled")
    session.conf.set("use_constant_subquery_alias", True)
    assert session.conf.get("use_constant_subquery_alias")
    with pytest.raises(AttributeError) as err:
        session.conf.set("use_openssl_only", False)
    assert (
        'Configuration "use_openssl_only" does not exist or is not mutable in runtime'
        in err.value.args[0]
    )

    session.close()


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot alter session in SP")
def test_update_query_tag(session):
    store_tag = session.query_tag

    try:
        session.query_tag = "tag1"
        with pytest.raises(
            ValueError,
            match="Expected query tag to be valid json. Current query tag: tag1",
        ):
            session.update_query_tag({"key2": "value2"})
    finally:
        session.query_tag = store_tag


def test_select_1(session):
    res = session.sql("select 1").collect()
    assert res == [Row(1)]


def test_sql_select_with_params(session):
    res = (
        session.sql("EXECUTE IMMEDIATE $$ SELECT ? AS x $$", [1]).select("x").collect()
    )
    assert res == [Row(1)]


def test_active_session(session):
    assert session == _get_active_session()
    assert not session._conn._conn.expired


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_multiple_active_sessions(session, db_parameters):
    with Session.builder.configs(db_parameters).create() as session2:
        assert {session, session2} == _get_active_sessions()


def test_get_or_create(session):
    # because there is already a session it should report the same
    new_session = Session.builder.getOrCreate()
    assert session == new_session


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_get_or_create_no_previous(db_parameters, session):
    # Test getOrCreate error. In this case we want to make sure that
    # if there was not a session the session gets created
    sessions_backup = list(_active_sessions)
    _active_sessions.clear()
    try:
        new_session = Session.builder.configs(db_parameters).getOrCreate()
        # A new session is created
        assert new_session != session
        new_session.close()
    finally:
        _active_sessions.update(sessions_backup)
    # Test getOrCreate another error. In this case we want to make sure
    # that when an error happens creating a new session the error gets sent
    new_session1 = None
    new_session2 = None
    try:
        new_session1 = Session.builder.configs(db_parameters).create()
        new_session2 = Session.builder.configs(db_parameters).create()
        with pytest.raises(SnowparkSessionException) as exec_info:
            new_session = Session.builder.configs(db_parameters).getOrCreate()
        assert exec_info.value.error_code == "1409"
    finally:
        if new_session1:
            new_session1.close()
        if new_session2:
            new_session2.close()


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

        with pytest.raises(SnowparkSessionException) as exec_info:
            _get_active_sessions()
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
        session.close()
        assert not session.connection.is_closed()
    finally:
        internal_utils.PLATFORM = original_platform


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="need resources")
def test_list_files_in_stage(session, resources_path, local_testing_mode):
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
        if not local_testing_mode:
            # TODO: session.file.put has a bug that it can not add '@' to single quoted name stage when normalizing path
            session._conn.upload_file(
                stage_location=single_quoted_name,
                path=test_files.test_file_csv,
                compress_data=False,
            )
        else:
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
        assert session_builder._options.get("password") is None
        assert new_session._conn._lower_case_parameters.get("password") is None
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
        assert session_builder._options.get("password") is None
        assert new_session._conn._lower_case_parameters.get("password") is None
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
        assert session_builder._options.get("password") is None
        assert new_session._conn._lower_case_parameters.get("password") is None
        assert new_session._conn._conn._password is None
    finally:
        new_session.close()


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_session_builder_app_name(session, db_parameters):
    builder = session.builder
    app_name = "my_app"
    expected_query_tag = f"APPNAME={app_name}"
    same_session = builder.app_name(app_name).getOrCreate()
    new_session = builder.app_name(app_name).configs(db_parameters).create()
    try:
        assert session == same_session
        assert same_session.query_tag is None
        assert new_session.query_tag == expected_query_tag
    finally:
        new_session.close()


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="The test creates temporary tables of which the names do not follow the rules of temp object on purposes.",
)
def test_table_exists(session):
    get_random_str = partial(Utils.random_name_for_temp_object, TempObjectType.TABLE)
    database = session.get_current_database().replace('"', "")
    schema = f"schema_{Utils.random_alphanumeric_str(10)}"
    double_quoted_schema = f'"{schema}.{schema}"'

    def check_temp_table(table_name_str):
        parsed_table_name = parse_table_name(table_name_str)
        assert session._table_exists(parsed_table_name) is False
        session.sql(f"create temp table {table_name_str}(col_a varchar)").collect()
        assert session._table_exists(parsed_table_name) is True

    def check_table_and_drop(table_name_str):
        # in stored proc the name of temp table must start with SNOWPARK..., however, we want to test
        # table name start with `"` which doesn't meet the requirement, thus we have this separate process
        # to create and drop temporary table
        try:
            parsed_table_name = parse_table_name(table_name_str)
            assert session._table_exists(parsed_table_name) is False
            session.sql(f"create table {table_name_str}(col_a varchar)").collect()
            assert session._table_exists(parsed_table_name) is True
        finally:
            session._run_query(f"drop table if exists {table_name_str}")

    try:
        Utils.create_schema(session, schema)
        Utils.create_schema(session, double_quoted_schema)
        # unquoted identifier - table name
        # e.g. table
        check_temp_table(get_random_str())

        # unquoted identifier - schema name and table name
        # e.g. schema.table
        check_temp_table(f"{schema}.{get_random_str()}")

        # unquoted identifier - db name, schema name and table name
        # e.g. db.schema.table
        check_temp_table(f"{database}.{schema}.{get_random_str()}")

        # unquoted identifier - db name and schema name
        # e.g. db..table
        check_temp_table(f"{database}..{get_random_str()}")

        check_table_and_drop("a")
        # double-quoted identifier
        # quoted table
        # e.g. "a.b"
        check_table_and_drop(f'"{get_random_str()}.{get_random_str()}"')

        # schema and quoted table
        # e.g. schema."a.b"
        check_table_and_drop(f'{schema}."{get_random_str()}.{get_random_str()}"')

        # quoted schema and quoted table
        # e.g. "schema"."a.b"
        check_table_and_drop(
            f'{double_quoted_schema}."{get_random_str()}.{get_random_str()}"'
        )

        # db, schema, and quoted table
        # e.g. db.schema."a.b"
        check_table_and_drop(
            f'{database}.{schema}."{get_random_str()}.{get_random_str()}"'
        )

        # db, quoted schema and quoted table
        # e.g. db."schema"."a.b"
        check_table_and_drop(
            f'{database}.{double_quoted_schema}."{get_random_str()}.{get_random_str()}"'
        )

        # db, no schema and quoted table
        # e.g. db.."a.b"
        check_table_and_drop(f'{database}.."{get_random_str()}.{get_random_str()}"')

        # handle escaping double quotes
        # e.g. """a..b.c"""
        check_table_and_drop(f'"""{get_random_str()}.{get_random_str()}"""')

        # negative cases
        with pytest.raises(SnowparkClientException):
            # invalid qualified name
            session._table_exists(["a", "b", "c", "d"])

        table_name = get_random_str()
        random_database = Utils.random_temp_database()
        random_schema = Utils.random_temp_schema()
        with pytest.raises(ProgrammingError):
            session._table_exists([random_database, random_schema, table_name])
        with pytest.raises(ProgrammingError):
            session._table_exists([random_database, "", table_name])
        with pytest.raises(ProgrammingError):
            session._table_exists([random_schema, table_name])
    finally:
        # drop schema
        Utils.drop_schema(session, schema)
        Utils.drop_schema(session, double_quoted_schema)


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


@pytest.mark.xfail(reason="SNOW-754082 flaky test", strict=False)
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

    suffix = Utils.random_alphanumeric_str(5)
    check(f"a_{suffix}", f'"A_{suffix.upper()}"')
    check(f"A_{suffix}", f'"A_{suffix.upper()}"')
    check(f'"a b_{suffix}"', f'"a b_{suffix}"')
    check(f'"a""b_{suffix}"', f'"a""b_{suffix}"')


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


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_create_session_from_default_config_file(monkeypatch, db_parameters):
    import tomlkit

    doc = tomlkit.document()
    default_con = tomlkit.table()
    try:
        # If anything unexpected fails here, don't want to expose password
        for k, v in db_parameters.items():
            default_con[k] = v
        doc["default"] = default_con
        with monkeypatch.context() as m:
            m.setenv("SNOWFLAKE_CONNECTIONS", tomlkit.dumps(doc))
            m.setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "default")
            with Session.builder.create() as new_session:
                _ = new_session.sql("select 1").collect()[0][0]
    except Exception:
        # This is my way of guaranteeing that we'll not expose the
        # sensitive information that this test needs to handle.
        # db_parameter contains passwords.
        pytest.fail("something failed", pytrace=False)
