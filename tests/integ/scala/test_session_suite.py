#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import NamedTuple

import pytest

from snowflake.connector.errors import DatabaseError
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    get_application_name,
    get_python_version,
    get_version,
    quote_name,
)
from snowflake.snowpark.exceptions import (
    SnowparkInvalidObjectNameException,
    SnowparkMissingDbOrSchemaException,
    SnowparkSessionException,
)
from snowflake.snowpark.session import _get_active_session
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
from tests.utils import IS_IN_STORED_PROC, IS_IN_STORED_PROC_LOCALFS, Utils


@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')",
    reason="Testing session parameters",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="creating new session is not allowed in stored proc"
)
def test_invalid_configs(session, db_parameters):
    with pytest.raises(DatabaseError) as ex_info:
        new_session = (
            Session.builder.configs(db_parameters)
            .config("user", "invalid_user")
            .config("password", "invalid_pwd")
            .config("login_timeout", 5)
            .create()
        )
        new_session.sql_simplifier_enabled = session.sql_simplifier_enabled
        with new_session:
            assert "Incorrect username or password was specified" in str(ex_info)


@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')",
    reason="Testing database specific operations",
)
@pytest.mark.skipif(IS_IN_STORED_PROC, reason="db_parameters is not available")
def test_current_database_and_schema(session, db_parameters):
    database = quote_name(db_parameters["database"])
    schema = quote_name(db_parameters["schema"])
    assert Utils.equals_ignore_case(database, session.get_current_database())
    assert Utils.equals_ignore_case(schema, session.get_current_schema())

    schema_name = Utils.random_temp_schema()
    try:
        session._run_query(f"create schema {schema_name}")

        assert Utils.equals_ignore_case(database, session.get_current_database())
        assert Utils.equals_ignore_case(
            quote_name(schema_name), session.get_current_schema()
        )
    finally:
        # restore
        session._run_query(f"drop schema if exists {schema_name}")
        session._run_query(f"use schema {schema}")


def test_quote_all_database_and_schema_names(session):
    def is_quoted(name: str) -> bool:
        return name[0] == '"' and name[-1] == '"'

    assert is_quoted(session.get_current_database())
    assert is_quoted(session.get_current_schema())


def test_create_dataframe_sequence(session):
    df = session.create_dataframe([[1, "one", 1.0], [2, "two", 2.0]])
    assert [field.name for field in df.schema.fields] == ["_1", "_2", "_3"]
    assert df.collect() == [Row(1, "one", 1.0), Row(2, "two", 2.0)]

    df = session.create_dataframe([1, 2])
    assert [field.name for field in df.schema.fields] == ["_1"]
    assert df.collect() == [Row(1), Row(2)]

    df = session.create_dataframe(["one", "two"])
    assert [field.name for field in df.schema.fields] == ["_1"]

    assert df.collect() == [Row("one"), Row("two")]


def test_create_dataframe_namedtuple(session):
    class P1(NamedTuple):
        a: int
        b: str
        c: float

    df = session.create_dataframe([P1(1, "one", 1.0), P1(2, "two", 2.0)])
    assert [field.name for field in df.schema.fields] == ["A", "B", "C"]


# this test requires the parameters used for connection has `public role`,
# and the public role has the privilege to access the current database and
# schema of the current role
@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Not enough privilege to run this test")
@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')",
    reason="Testing database specific operations",
)
def test_get_schema_database_works_after_use_role(session):
    current_role = session._conn._get_string_datum("select current_role()")
    try:
        db = session.get_current_database()
        schema = session.get_current_schema()
        session._run_query("use role public")
        assert session.get_current_database() == db
        assert session.get_current_schema() == schema
    finally:
        session._run_query(f"use role {current_role}")


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="creating new session is not allowed in stored proc"
)
def test_negative_test_for_missing_required_parameter_schema(
    db_parameters, sql_simplifier_enabled
):
    new_session = (
        Session.builder.configs(db_parameters)._remove_config("schema").create()
    )
    new_session.sql_simplifier_enabled = sql_simplifier_enabled
    with new_session:
        with pytest.raises(SnowparkMissingDbOrSchemaException) as ex_info:
            new_session.get_fully_qualified_current_schema()
        assert "The SCHEMA is not set for the current session." in str(ex_info)


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="client is regression test specific")
@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')",
    reason="Testing database specific operations",
)
def test_select_current_client(session):
    current_client = session.sql("select current_client()")._show_string(10)
    assert get_application_name() in current_client
    assert get_version() in current_client


@pytest.mark.localtest
def test_negative_test_to_invalid_table_name(session):
    with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
        session.table("negative.test.invalid.table.name")
    assert "The object name 'negative.test.invalid.table.name' is invalid." in str(
        ex_info
    )


# TODO: should be enabled after emulating snowflake types
def test_create_dataframe_from_seq_none(session):
    assert session.create_dataframe([None, 1]).to_df("int").collect() == [
        Row(None),
        Row(1),
    ]
    assert session.create_dataframe([None, [[1, 2]]]).to_df("arr").collect() == [
        Row(None),
        Row("[\n  1,\n  2\n]"),
    ]


# should be enabled after emulating snowflake types
def test_create_dataframe_from_array(session):
    data = [Row(1, "a"), Row(2, "b")]
    schema = StructType(
        [StructField("num", IntegerType()), StructField("str", StringType())]
    )
    df = session.create_dataframe(data, schema)
    assert df.collect() == data

    # negative
    data1 = [Row("a", 1), Row(2, "b")]
    with pytest.raises(TypeError) as ex_info:
        session.create_dataframe(data1, schema)
    assert "Unsupported datatype" in str(ex_info)


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="creating new session is not allowed in stored proc"
)
def test_dataframe_created_before_session_close_are_not_usable_after_closing_session(
    session, db_parameters
):
    new_session = Session.builder.configs(db_parameters).create()
    new_session.sql_simplifier_enabled = session.sql_simplifier_enabled
    with new_session:
        df = new_session.range(10)
        read = new_session.read

    with pytest.raises(SnowparkSessionException) as ex_info:
        df.collect()
    assert ex_info.value.error_code == "1404"
    with pytest.raises(SnowparkSessionException) as ex_info:
        read.json("@mystage/prefix")
    assert ex_info.value.error_code == "1404"


@pytest.mark.localtest
def test_load_table_from_array_multipart_identifier(session):
    name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        [], schema=StructType([StructField("col", IntegerType())])
    ).write.save_as_table(name, table_type="temporary")
    db = session.get_current_database()
    sc = session.get_current_schema()
    multipart = [db, sc, name]
    assert len(session.table(multipart).schema.fields) == 1


def test_session_info(session):
    session_info = session._session_info
    assert get_version() in session_info
    assert get_python_version() in session_info
    assert str(session._conn.get_session_id()) in session_info
    assert "python.connector.version" in session_info


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="creating new session is not allowed in stored proc"
)
@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')",
    reason="Testing database specific operations",
)
def test_dataframe_close_session(session, db_parameters):
    new_session = Session.builder.configs(db_parameters).create()
    new_session.sql_simplifier_enabled = session.sql_simplifier_enabled
    try:
        with pytest.raises(SnowparkSessionException) as ex_info:
            _get_active_session()
        assert ex_info.value.error_code == "1409"
    finally:
        new_session.close()

    with pytest.raises(SnowparkSessionException) as ex_info:
        new_session.sql("select current_timestamp()").collect()
    assert ex_info.value.error_code == "1404"
    with pytest.raises(SnowparkSessionException) as ex_info:
        new_session.range(10).collect()
    assert ex_info.value.error_code == "1404"


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="Large result")
@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')",
    reason="Testing database specific operations",
)
def test_create_temp_table_no_commit(session):
    # test large local relation
    session.sql("begin").collect()
    assert Utils.is_active_transaction(session)
    session.range(1000000).to_df("id").collect()
    assert Utils.is_active_transaction(session)
    session.sql("commit").collect()
    assert not Utils.is_active_transaction(session)

    # test cache result
    test_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, test_table, "c1 int", is_temporary=True)
        session.sql(f"insert into {test_table} values (1), (2)").collect()
        session.sql("begin").collect()
        assert Utils.is_active_transaction(session)
        session.table(test_table).cache_result()
        assert Utils.is_active_transaction(session)
        session.sql("commit").collect()
        assert not Utils.is_active_transaction(session)
    finally:
        Utils.drop_table(session, test_table)
