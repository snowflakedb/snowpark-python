#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import copy
import io
from functools import partial
from unittest.mock import patch

import pytest

import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import (
    StructType,
    StructField,
    LongType,
    TimestampType,
    TimestampTimeZone,
    StringType,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    get_version,
    parse_table_name,
)
from snowflake.snowpark.exceptions import (
    SnowparkClientException,
    SnowparkInvalidObjectNameException,
    SnowparkSessionException,
)
from snowflake.snowpark.session import (
    _PYTHON_SNOWPARK_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED,
    _PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING,
    _PYTHON_SNOWPARK_USE_CTE_OPTIMIZATION_VERSION,
    _active_sessions,
    _get_active_session,
    _get_active_sessions,
)
from tests.utils import IS_IN_STORED_PROC, IS_IN_STORED_PROC_LOCALFS, TestFiles, Utils


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1374069: fix RuntimeConfig for Local Testing",
)
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot alter session in SP")
def test_collect_stacktrace_in_query_tag(session):
    from snowflake.snowpark._internal.analyzer import analyzer

    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    original_collect_stacktrace_in_query_tag = session.conf.get(
        "collect_stacktrace_in_query_tag"
    )
    try:
        session.conf.set("collect_stacktrace_in_query_tag", False)
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df = session.createDataFrame([[1, 2], [3, 4]], ["a", "b"])
        with session.query_history() as history:
            df.collect()
        assert len(history.queries) == 4
        assert history.queries[0].sql_text.startswith(
            "CREATE  OR  REPLACE  SCOPED TEMPORARY"
        )
        assert history.queries[1].sql_text.startswith(
            "INSERT  INTO SNOWPARK_TEMP_TABLE"
        )
        assert Utils.normalize_sql(history.queries[2].sql_text).startswith(
            'SELECT "A", "B" FROM'
        )
        assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")

        session.conf.set("collect_stacktrace_in_query_tag", True)
        with session.query_history() as history:
            df.collect()
        assert len(history.queries) == 6
        assert history.queries[0].sql_text.startswith(
            "CREATE  OR  REPLACE  SCOPED TEMPORARY"
        )
        assert history.queries[1].sql_text.startswith("alter session set query_tag")
        assert history.queries[2].sql_text.startswith(
            "INSERT  INTO SNOWPARK_TEMP_TABLE"
        )
        assert history.queries[3].sql_text.startswith("alter session unset query_tag")
        assert Utils.normalize_sql(history.queries[4].sql_text).startswith(
            'SELECT "A", "B" FROM'
        )
        assert history.queries[5].sql_text.startswith("DROP  TABLE  If  EXISTS")
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold
        session.conf.set(
            "collect_stacktrace_in_query_tag", original_collect_stacktrace_in_query_tag
        )


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
def test_select_1(session):
    res = session.sql("select 1").collect()
    assert res == [Row(1)]


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
def test_sql_select_with_params(session):
    res = (
        session.sql("EXECUTE IMMEDIATE $$ SELECT ? AS x $$", [1]).select("x").collect()
    )
    assert res == [Row(1)]


def test_active_session(session):
    assert session == _get_active_session()
    assert not session._conn._conn.expired


def test_session_version(session):
    assert session.version == get_version()


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_multiple_active_sessions(session, db_parameters):
    with Session.builder.configs(db_parameters).create() as session2:
        assert {session, session2} == _get_active_sessions()


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_get_active_session(session, db_parameters):
    assert Session.get_active_session() == session
    assert Session.getActiveSession() == session

    with Session.builder.configs(db_parameters).create():
        with pytest.raises(
            SnowparkClientException, match="More than one active session is detected"
        ) as ex:
            Session.get_active_session()
        assert ex.value.error_code == "1409"


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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
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
        if not session._conn._get_client_side_session_parameter(
            "ENABLE_CREATE_SESSION_IN_STORED_PROCS", False
        ):
            with pytest.raises(SnowparkSessionException) as exec_info:
                Session(session._conn)
            assert exec_info.value.error_code == "1410"
        with patch.object(
            session._conn, "_get_client_side_session_parameter", return_value=True
        ):
            try:
                Session(session._conn)
            except SnowparkSessionException as e:
                pytest.fail(f"Unexpected exception {e} was raised")
    finally:
        internal_utils.PLATFORM = original_platform


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="BUG: Mock object is closed undefined",
)
def test_close_session_in_sp(session):
    # TODO: local testing support SNOW-1331149 mocking connector connection
    import snowflake.snowpark._internal.utils as internal_utils

    original_platform = internal_utils.PLATFORM
    internal_utils.PLATFORM = "XP"
    try:
        session.close()
        assert not session.connection.is_closed()
    finally:
        internal_utils.PLATFORM = original_platform


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
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

        full_name = session.get_fully_qualified_name_if_possible(stage_name)
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

        full_name_with_prefix = session.get_fully_qualified_name_if_possible(
            quoted_name
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
        # TODO: session.file.put has a bug that it can not add '@' to single quoted name stage when normalizing path
        session._conn.upload_file(
            stage_location=single_quoted_name,
            path=test_files.test_file_csv,
            compress_data=False,
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
def test_create_session_from_connection(
    db_parameters, sql_simplifier_enabled, local_testing_mode
):
    connection = snowflake.connector.connect(**db_parameters)
    session_builder = Session.builder.configs({"connection": connection})
    new_session = session_builder.create()
    new_session.sql_simplifier_enabled = sql_simplifier_enabled
    try:
        df = new_session.createDataFrame([[1, 2]], schema=["a", "b"])
        Utils.check_answer(df, [Row(1, 2)])
        assert session_builder._options.get("password") is None
        if not local_testing_mode:
            assert new_session._conn._lower_case_parameters.get("password") is None
        assert new_session._conn._conn._password is None
    finally:
        new_session.close()


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is testing live connection feature",
    run=False,
)
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Query tag is a SQL feature",
    run=False,
)
@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
@pytest.mark.parametrize(
    "app_name,format_json,expected_query_tag",
    [
        ("my_app_name", False, "APPNAME=my_app_name"),
        ("my_app_name", True, '{"APPNAME": "my_app_name"}'),
    ],
)
def test_session_builder_app_name(
    session, db_parameters, app_name, format_json, expected_query_tag
):
    builder = session.builder
    same_session = builder.app_name(app_name, format_json=format_json).getOrCreate()
    new_session = (
        builder.app_name(app_name, format_json=format_json)
        .configs(db_parameters)
        .create()
    )
    try:
        assert session == same_session
        assert same_session.query_tag is None
        assert new_session.query_tag == expected_query_tag
    finally:
        new_session.close()


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
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
    parameters = copy.deepcopy(db_parameters)
    del parameters["database"]
    del parameters["schema"]
    del parameters["warehouse"]
    with Session.builder.configs(parameters).create() as session:
        session.sql_simplifier_enabled = sql_simplifier_enabled
        db_name = db_parameters["database"]
        session.use_database(db_name)
        assert session.get_current_database() == f'"{db_name.upper()}"'


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_use_schema(db_parameters, sql_simplifier_enabled, local_testing_mode):
    parameters = copy.deepcopy(db_parameters)
    del parameters["schema"]
    del parameters["warehouse"]
    parameters["local_testing"] = local_testing_mode
    with Session.builder.configs(parameters).create() as session:
        quoted_schema_name = f'"SCHEMA_{Utils.random_alphanumeric_str(5)}_schema"'
        try:
            session.sql_simplifier_enabled = sql_simplifier_enabled
            schema_name = db_parameters["schema"]
            session.use_schema(schema_name)
            assert session.get_current_schema() == f'"{schema_name.upper()}"'
            if not local_testing_mode:
                session.sql(f"CREATE OR REPLACE SCHEMA {quoted_schema_name}").collect()
            session.use_schema(quoted_schema_name)
            assert session.get_current_schema() == quoted_schema_name
        finally:
            if not local_testing_mode:
                session.sql(f"DROP SCHEMA IF EXISTS {quoted_schema_name}").collect()


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_use_warehouse(db_parameters, sql_simplifier_enabled):
    parameters = copy.deepcopy(db_parameters)
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
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


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Can't create a session in SP")
def test_close_session_twice(db_parameters):
    new_session = Session.builder.configs(db_parameters).create()
    new_session.close()
    new_session.close()  # no exception


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Can't create a session in SP")
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="reading server side parameter is not supported in local testing",
)
def test_sql_simplifier_disabled_on_session(db_parameters):
    # TODO: SNOW-2196637, db_parameters["session_parameters"] is unexpectedly mutated during test execution;
    # it should remain immutable.
    params = copy.deepcopy(db_parameters)
    params.pop("session_parameters")
    with Session.builder.configs(params).create() as new_session:
        assert new_session.sql_simplifier_enabled is True
        new_session.sql_simplifier_enabled = False
        assert new_session.sql_simplifier_enabled is False
        new_session.sql_simplifier_enabled = True
        assert new_session.sql_simplifier_enabled is True

    parameters = copy.deepcopy(db_parameters)
    parameters["session_parameters"] = {
        _PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING: False
    }
    with Session.builder.configs(parameters).create() as new_session2:
        assert new_session2.sql_simplifier_enabled is False


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Can't create a session in SP")
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="reading server side parameter is not supported in local testing",
)
def test_cte_optimization_enabled_on_session(session, db_parameters):
    with Session.builder.configs(db_parameters).create() as new_session:
        default_value = new_session.cte_optimization_enabled
        new_session.cte_optimization_enabled = not default_value
        assert new_session.cte_optimization_enabled is not default_value
        new_session.cte_optimization_enabled = default_value
        assert new_session.cte_optimization_enabled is default_value

    parameters = copy.deepcopy(db_parameters)
    parameters["session_parameters"] = {
        _PYTHON_SNOWPARK_USE_CTE_OPTIMIZATION_VERSION: get_version()
        if default_value
        else ""
    }
    with Session.builder.configs(parameters).create() as new_session2:
        assert new_session2.cte_optimization_enabled is default_value


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Can't create a session in SP")
@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="reading server side parameter is not supported in local testing",
)
@pytest.mark.parametrize("server_parameter_enabled", [True, False])
def test_eliminate_numeric_sql_value_cast_optimization_enabled_on_session(
    db_parameters, server_parameter_enabled
):
    parameters = copy.deepcopy(db_parameters)
    parameters["session_parameters"] = {
        _PYTHON_SNOWPARK_ELIMINATE_NUMERIC_SQL_VALUE_CAST_ENABLED: server_parameter_enabled
    }
    with Session.builder.configs(parameters).create() as new_session:
        assert (
            new_session.eliminate_numeric_sql_value_cast_enabled
            is server_parameter_enabled
        )
        new_session.eliminate_numeric_sql_value_cast_enabled = True
        assert new_session.eliminate_numeric_sql_value_cast_enabled is True
        new_session.eliminate_numeric_sql_value_cast_enabled = False
        assert new_session.eliminate_numeric_sql_value_cast_enabled is False
        with pytest.raises(ValueError):
            new_session.eliminate_numeric_sql_value_cast_enabled = None


def test_large_query_breakdown_complexity_bounds(session):
    original_bounds = session.large_query_breakdown_complexity_bounds
    try:
        with pytest.raises(ValueError, match="Expecting a tuple of two integers"):
            session.large_query_breakdown_complexity_bounds = (1, 2, 3)

        with pytest.raises(
            ValueError, match="Expecting a tuple of lower and upper bound"
        ):
            session.large_query_breakdown_complexity_bounds = (3, 2)

        with patch.object(
            session._conn._telemetry_client,
            "send_large_query_breakdown_update_complexity_bounds",
        ) as patch_send:
            session.large_query_breakdown_complexity_bounds = (1, 2)
            assert session.large_query_breakdown_complexity_bounds == (1, 2)
            assert patch_send.call_count == 1
            assert patch_send.call_args[0][0] == session.session_id
            assert patch_send.call_args[0][1] == 1
            assert patch_send.call_args[0][2] == 2
    finally:
        session.large_query_breakdown_complexity_bounds = original_bounds


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


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="use schema is not allowed in stored proc (owner mode)"
)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="running sql query is not supported in local testing",
)
def test_get_session_stage(session):
    with session.query_history() as history:
        session_stage = session.get_session_stage()
    assert len(history.queries) == 1
    assert "stage if not exists" in history.queries[0].sql_text
    # check if session_stage is a fully qualified name
    assert session.get_current_database() in session_stage
    assert session.get_current_schema() in session_stage

    # no create stage sql again
    with session.query_history() as history:
        session_stage = session.get_session_stage()
    assert len(history.queries) == 0

    # switch database and schema, session stage will not be created again
    new_schema = f"schema_{Utils.random_alphanumeric_str(10)}"
    new_database = f"db_{Utils.random_alphanumeric_str(10)}"
    current_schema = session.get_current_schema()
    current_database = session.get_current_database()
    try:
        session._run_query(f"create schema if not exists {new_schema}")
        session.use_schema(new_schema)
        new_session_stage = session.get_session_stage()
        assert session_stage == new_session_stage
    finally:
        Utils.drop_schema(session, new_schema)
        session.use_schema(current_schema)

    try:
        session._run_query(f"create database if not exists {new_database}")
        session.use_database(new_database)
        new_session_stage = session.get_session_stage()
        assert session_stage == new_session_stage
    finally:
        Utils.drop_database(session, new_database)
        session.use_database(current_database)
        session.use_schema(current_schema)


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="sproc disallows new connection creation")
def test_session_atexit(db_parameters):
    exit_funcs = []
    with patch("atexit.register", lambda func: exit_funcs.append(func)):
        with Session.builder.configs(db_parameters).create():
            pass

    # the first exit function is from the connector
    # the second exit function is from the Snowpark session
    # the order matters so Snowpark session is closed first
    assert len(exit_funcs) == 2
    assert exit_funcs[0].__module__.startswith("snowflake.connector")
    assert exit_funcs[1].__module__.startswith("snowflake.snowpark")


def test_directory(session):
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    temp_stage_special = f'"{Utils.random_name_for_temp_object(TempObjectType.STAGE)}_special.name-with.dots"'

    try:
        # Create temp stages with directory table enabled
        session.sql(
            f"CREATE TEMP STAGE {temp_stage} DIRECTORY = (ENABLE = TRUE)"
        ).collect()
        session.sql(
            f"CREATE TEMP STAGE {temp_stage_special} DIRECTORY = (ENABLE = TRUE)"
        ).collect()

        # File 1: Simple text file
        file1_stream = io.BytesIO(b"Hello, World!\nThis is file 1 content.")
        session.file.put_stream(file1_stream, f"@{temp_stage}/file1.txt")

        # File 2: JSON-like content
        file2_stream = io.BytesIO(
            b'{"key": "value", "number": 123, "array": [1, 2, 3]}'
        )
        session.file.put_stream(file2_stream, f"@{temp_stage}/data.json")

        # Upload to special stage name
        file3_stream = io.BytesIO(b"File in special named stage with dots and hyphens")
        session.file.put_stream(file3_stream, f"@{temp_stage_special}/special_file.txt")

        # Refresh the stages to ensure metadata is up-to-date
        session.sql(f"ALTER STAGE {temp_stage} REFRESH").collect()
        session.sql(f"ALTER STAGE {temp_stage_special} REFRESH").collect()

        EXPECTED_SCHEMA = StructType(
            [
                StructField("RELATIVE_PATH", StringType(), nullable=True),
                StructField("SIZE", LongType(), nullable=True),
                StructField(
                    "LAST_MODIFIED",
                    TimestampType(timezone=TimestampTimeZone.TZ),
                    nullable=True,
                ),
                StructField("MD5", StringType(), nullable=True),
                StructField("ETAG", StringType(), nullable=True),
                StructField("FILE_URL", StringType(), nullable=True),
            ]
        )

        # Test directory() method on main stage
        directory_df = session.directory(f"@{temp_stage}")
        assert (
            len(directory_df.collect()) == 2 and directory_df.schema == EXPECTED_SCHEMA
        )
        df_filtered = directory_df.where(
            col("RELATIVE_PATH").startswith("file1")
        ).select(col("SIZE"))
        assert len(df_filtered.collect()) == 1 and df_filtered.schema == StructType(
            [StructField("SIZE", LongType(), nullable=True)]
        )

        # Test directory() method on special stage name (with quotes)
        directory_df_special = session.directory(temp_stage_special)
        assert directory_df_special.schema == EXPECTED_SCHEMA
        assert (
            len(directory_df_special.collect()) == 1
            and len(directory_df_special.limit(0).collect()) == 0
        )

    finally:
        # Clean up - drop the temp stages
        try:
            session.sql(f"DROP STAGE IF EXISTS {temp_stage}").collect()
        except Exception:
            pass  # Ignore errors during cleanup

        try:
            session.sql(f"DROP STAGE IF EXISTS {temp_stage_special}").collect()
        except Exception:
            pass  # Ignore errors during cleanup
