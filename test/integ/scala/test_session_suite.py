#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from typing import NamedTuple

import pytest

from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from snowflake.snowpark.snowpark_client_exception import (
    SnowparkInvalidObjectNameException,
    SnowparkMissingDbOrSchemaException,
)


def test_createDataFrame_sequence(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, "one", 1.0], [2, "two", 2.0]])
        assert [field.name for field in df.schema.fields] == ["_1", "_2", "_3"]
        assert df.collect() == [Row(1, "one", 1.0), Row(2, "two", 2.0)]

        df = session.createDataFrame([1, 2])
        assert [field.name for field in df.schema.fields] == ["VALUES"]
        assert df.collect() == [Row(1), Row(2)]

        df = session.createDataFrame(["one", "two"])
        assert [field.name for field in df.schema.fields] == ["VALUES"]
        assert df.collect() == [Row("one"), Row("two")]


def test_create_dataFrame_namedtuple(session_cnx):
    P1 = NamedTuple("P1", [("a", int), ("b", str), ("c", float)])
    with session_cnx() as session:
        df = session.createDataFrame([P1(1, "one", 1.0), P1(2, "two", 2.0)])
        assert [field.name for field in df.schema.fields] == ["A", "B", "C"]


# this test requires the parameters used for connection has `public role`,
# and the public role has the privilege to access the current database and
# schema of the current role
def test_get_schema_database_works_after_use_role(session_cnx):
    with session_cnx() as session:
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


def test_negative_test_to_invalid_table_name(session_cnx):
    with session_cnx() as session:
        with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
            session.table("negative.test.invalid.table.name")
        assert "The object name 'negative.test.invalid.table.name' is invalid." in str(
            ex_info
        )


def test_create_dataframe_from_seq_none(session_cnx):
    with session_cnx() as session:
        assert session.createDataFrame([None, 1]).collect() == [Row(None), Row(1)]
