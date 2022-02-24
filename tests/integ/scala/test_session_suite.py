#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import NamedTuple

import pytest

from snowflake.connector.errors import DatabaseError
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark._internal.utils import TempObjectType, Utils as snowpark_utils
from snowflake.snowpark.exceptions import (
    SnowparkInvalidObjectNameException,
    SnowparkMissingDbOrSchemaException,
    SnowparkSessionException,
)
from snowflake.snowpark.session import _get_active_session
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
from tests.utils import Utils


def test_invalid_configs(session, db_parameters):
    with pytest.raises(DatabaseError) as ex_info:
        new_session = (
            Session.builder.configs(db_parameters)
            .config("user", "invalid_user")
            .config("password", "invalid_pwd")
            .config("login_timeout", 5)
            .create()
        )
        with new_session:
            assert "Incorrect username or password was specified" in str(ex_info)


def test_no_default_database_and_schema(session, db_parameters):
    new_session = (
        Session.builder.configs(db_parameters)
        ._remove_config("database")
        ._remove_config("schema")
        .create()
    )
    try:
        assert not new_session.getDefaultDatabase()
        assert not new_session.getDefaultSchema()
    finally:
        new_session.close()


def test_default_and_current_database_and_schema(session):
    default_database = session.getDefaultDatabase()
    default_schema = session.getDefaultSchema()

    assert Utils.equals_ignore_case(default_database, session.get_current_database())
    assert Utils.equals_ignore_case(default_schema, session.get_current_schema())

    try:
        schema_name = Utils.random_temp_schema()
        session._run_query("create schema {}".format(schema_name))

        assert Utils.equals_ignore_case(default_database, session.getDefaultDatabase())
        assert Utils.equals_ignore_case(default_schema, session.getDefaultSchema())

        assert Utils.equals_ignore_case(
            default_database, session.get_current_database()
        )
        assert Utils.equals_ignore_case(
            AnalyzerPackage.quote_name(schema_name), session.get_current_schema()
        )
    finally:
        # restore
        session._run_query("drop schema if exists {}".format(schema_name))
        session._run_query("use schema {}".format(default_schema))


def test_quote_all_database_and_schema_names(session):
    def is_quoted(name: str) -> bool:
        return name[0] == '"' and name[-1] == '"'

    assert is_quoted(session.getDefaultDatabase())
    assert is_quoted(session.getDefaultSchema())
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
    P1 = NamedTuple("P1", [("a", int), ("b", str), ("c", float)])
    df = session.create_dataframe([P1(1, "one", 1.0), P1(2, "two", 2.0)])
    assert [field.name for field in df.schema.fields] == ["A", "B", "C"]


# this test requires the parameters used for connection has `public role`,
# and the public role has the privilege to access the current database and
# schema of the current role
def test_get_schema_database_works_after_use_role(session):
    current_role = session._conn._get_string_datum("select current_role()")
    try:
        db = session.get_current_database()
        schema = session.get_current_schema()
        session._run_query("use role public")
        assert session.get_current_database() == db
        assert session.get_current_schema() == schema
    finally:
        session._run_query("use role {}".format(current_role))


def test_negative_test_for_missing_required_parameter_schema(db_parameters):
    new_session = (
        Session.builder.configs(db_parameters)._remove_config("schema").create()
    )
    with new_session:
        with pytest.raises(SnowparkMissingDbOrSchemaException) as ex_info:
            new_session.get_fully_qualified_current_schema()
        assert "The SCHEMA is not set for the current session." in str(ex_info)


def test_select_current_client(session):
    current_client = session.sql("select current_client()")._show_string(10)
    assert snowpark_utils.get_application_name() in current_client
    assert snowpark_utils.get_version() in current_client


def test_negative_test_to_invalid_table_name(session):
    with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
        session.table("negative.test.invalid.table.name")
    assert "The object name 'negative.test.invalid.table.name' is invalid." in str(
        ex_info
    )


def test_create_dataframe_from_seq_none(session):
    assert session.create_dataframe([None, 1]).to_df("int").collect() == [
        Row(None),
        Row(1),
    ]
    assert session.create_dataframe([None, [[1, 2]]]).to_df("arr").collect() == [
        Row(None),
        Row("[\n  1,\n  2\n]"),
    ]


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


def test_dataframe_created_before_session_close_are_not_usable_after_closing_session(
    session,
    db_parameters,
):
    new_session = Session.builder.configs(db_parameters).create()
    with new_session:
        df = new_session.range(10)
        read = new_session.read

    with pytest.raises(SnowparkSessionException) as ex_info:
        df.collect()
    assert ex_info.value.error_code == "1404"
    with pytest.raises(SnowparkSessionException) as ex_info:
        read.json("@mystage/prefix")
    assert ex_info.value.error_code == "1404"


def test_load_table_from_array_multipart_identifier(session):
    name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, name, "col int")
        db = session.get_current_database()
        sc = session.get_current_schema()
        multipart = [db, sc, name]
        assert len(session.table(multipart).schema.fields) == 1
    finally:
        Utils.drop_table(session, name)


def test_session_info(session):
    session_info = session._session_info
    assert snowpark_utils.get_version() in session_info
    assert snowpark_utils.get_python_version() in session_info
    assert str(session._conn.get_session_id()) in session_info
    assert "python.connector.version" in session_info


def test_dataframe_close_session(
    session,
    db_parameters,
):
    new_session = Session.builder.configs(db_parameters).create()
    try:
        with pytest.raises(SnowparkSessionException) as ex_info:
            _get_active_session()
        assert ex_info.value.error_code == "1409"
    finally:
        new_session.close()

    # TODO: currently we need to call collect() to trigger error (scala doesn't)
    #  because Python doesn't have to query parameter value for lazy analysis
    with pytest.raises(SnowparkSessionException) as ex_info:
        new_session.sql("select current_timestamp()").collect()
    assert ex_info.value.error_code == "1404"
    with pytest.raises(SnowparkSessionException) as ex_info:
        new_session.range(10).collect()
    assert ex_info.value.error_code == "1404"
