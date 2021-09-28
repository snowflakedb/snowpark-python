#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from test.utils import Utils
from typing import NamedTuple

import pytest

from snowflake.snowpark import Row, Session
from snowflake.snowpark.exceptions import (
    SnowparkInvalidObjectNameException,
    SnowparkMissingDbOrSchemaException,
    SnowparkSessionException,
)
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType


def test_create_dataframe_sequence(session):
    df = session.createDataFrame([[1, "one", 1.0], [2, "two", 2.0]])
    assert [field.name for field in df.schema.fields] == ["_1", "_2", "_3"]
    assert df.collect() == [Row(1, "one", 1.0), Row(2, "two", 2.0)]

    df = session.createDataFrame([1, 2])
    assert [field.name for field in df.schema.fields] == ["VALUES"]
    assert df.collect() == [Row(1), Row(2)]

    df = session.createDataFrame(["one", "two"])
    assert [field.name for field in df.schema.fields] == ["VALUES"]
    assert df.collect() == [Row("one"), Row("two")]


def test_create_dataframe_namedtuple(session):
    P1 = NamedTuple("P1", [("a", int), ("b", str), ("c", float)])
    df = session.createDataFrame([P1(1, "one", 1.0), P1(2, "two", 2.0)])
    assert [field.name for field in df.schema.fields] == ["A", "B", "C"]


# this test requires the parameters used for connection has `public role`,
# and the public role has the privilege to access the current database and
# schema of the current role
def test_get_schema_database_works_after_use_role(session):
    current_role = session.conn._get_string_datum("select current_role()")
    try:
        db = session.getCurrentDatabase()
        schema = session.getCurrentSchema()
        session._run_query("use role public")
        assert session.getCurrentDatabase() == db
        assert session.getCurrentSchema() == schema
    finally:
        session._run_query("use role {}".format(current_role))


def test_negative_test_for_missing_required_parameter_schema(db_parameters):
    session = Session.builder.configs(db_parameters)._remove_config("schema").create()
    with pytest.raises(SnowparkMissingDbOrSchemaException) as ex_info:
        session.getFullyQualifiedCurrentSchema()
    assert "The SCHEMA is not set for the current session." in str(ex_info)


def test_negative_test_to_invalid_table_name(session):
    with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
        session.table("negative.test.invalid.table.name")
    assert "The object name 'negative.test.invalid.table.name' is invalid." in str(
        ex_info
    )


def test_create_dataframe_from_seq_none(session):
    assert session.createDataFrame([None, 1]).collect() == [Row(None), Row(1)]


def test_create_dataframe_from_array(session):
    data = [Row(1, "a"), Row(2, "b")]
    schema = StructType(
        [StructField("num", IntegerType()), StructField("str", StringType())]
    )
    df = session.createDataFrame(data, schema)
    assert df.collect() == data

    # negative
    data1 = [Row("a", 1), Row(2, "b")]
    with pytest.raises(TypeError) as ex_info:
        session.createDataFrame(data1, schema)
    assert "Unsupported datatype" in str(ex_info)


def test_dataframe_created_before_session_close_are_not_usable_after_closing_session(
    db_parameters,
):
    new_session = Session.builder.configs(db_parameters).create()
    df = new_session.range(10)
    read = new_session.read
    new_session.close()

    with pytest.raises(SnowparkSessionException) as ex_info:
        df.collect()
    assert ex_info.value.error_code == "1404"
    with pytest.raises(SnowparkSessionException) as ex_info:
        read.json("@mystage/prefix")
    assert ex_info.value.error_code == "1404"


def test_load_table_from_array_multipart_identifier(session):
    name = Utils.random_name()
    try:
        Utils.create_table(session, name, "col int")
        db = session.getCurrentDatabase()
        sc = session.getCurrentSchema()
        multipart = [db, sc, name]
        assert len(session.table(multipart).schema.fields) == 1
    finally:
        Utils.drop_table(session, name)
