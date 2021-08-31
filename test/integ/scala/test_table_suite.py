#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from test.utils import Utils

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session


@pytest.fixture(scope="function")
def table_name_1(session: Session):
    table_name = Utils.random_name()
    Utils.create_table(session, table_name, "num int")
    session._run_query(f"insert into {table_name} values (1), (2), (3)")
    yield table_name
    Utils.drop_table(session, table_name)


@pytest.fixture(scope="function")
def table_name_4(session: Session):
    table_name = Utils.random_name()
    Utils.create_table(session, table_name, "num int")
    session._run_query(f"insert into {table_name} values (1), (2), (3)")
    yield table_name
    Utils.drop_table(session, table_name)


@pytest.mark.parametrize("mode_place", ["method", "param"])
def test_save_as_snowflake_table(session, table_name_1, mode_place):
    df = session.table(table_name_1)
    assert df.collect() == [Row(1), Row(2), Row(3)]
    table_name_2 = Utils.random_name()
    table_name_3 = Utils.random_name()
    try:
        # copy table_name_1 to table_name_2, default mode
        df.write.saveAsTable(table_name_2)

        df2 = session.table(table_name_2)
        assert df2.collect() == [Row(1), Row(2), Row(3)]

        # append mode
        if mode_place == "method":
            df.write.mode("append").saveAsTable(table_name_2)
        else:
            df.write.saveAsTable(table_name_2, "append")
        df4 = session.table(table_name_2)
        assert df4.collect() == [Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)]

        # ignore mode
        if mode_place == "method":
            df.write.mode("IGNORE").saveAsTable(table_name_2)
        else:
            df.write.saveAsTable(table_name_2, "IGNORE")
        df3 = session.table(table_name_2)
        assert df3.collect() == [Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)]

        # overwrite mode
        if mode_place == "method":
            df.write.mode("OvErWrItE").saveAsTable(table_name_2)
        else:
            df.write.saveAsTable(table_name_2, "OvErWrItE")
        df5 = session.table(table_name_2)
        assert df5.collect() == [Row(1), Row(2), Row(3)]

        # test for append when the original table does not exist
        # need to create the table before insertion
        if mode_place == "method":
            df.write.mode("aPpEnD").saveAsTable(table_name_3)
        else:
            df.write.saveAsTable(table_name_3, "aPpEnD")
        df6 = session.table(table_name_3)
        assert df6.collect() == [Row(1), Row(2), Row(3)]

        # errorifexists mode
        with pytest.raises(ProgrammingError):
            if mode_place == "method":
                df.write.mode("errorifexists").saveAsTable(table_name_2)
            else:
                df.write.saveAsTable(table_name_2, "errorifexists")
    finally:
        Utils.drop_table(session, table_name_2)
        Utils.drop_table(session, table_name_3)


@pytest.mark.skip(
    "Python doesn't have non-string argument for mode. Scala has this test but python doesn't need to."
)
def test_save_as_snowflake_table_string_argument(table_name_4):
    """
    Scala's `DataFrameWriter.mode()` accepts both enum values of SaveMode and str.
    It's conventional that python uses str and pyspark does use str only. So the above test method
    `test_save_as_snowflake_table` already tests the string argument. This test will be the same as
    `test_save_as_snowflake_table` if ported from Scala so it's omitted.
    """


def test_multipart_identifier(session, table_name_1):
    name1 = table_name_1
    name2 = session.getCurrentSchema() + "." + name1
    name3 = session.getCurrentDatabase() + "." + name2

    expected = [Row(1), Row(2), Row(3)]
    assert session.table(name1).collect() == expected
    assert session.table(name2).collect() == expected
    assert session.table(name3).collect() == expected

    name4 = Utils.random_name()
    name5 = session.getCurrentSchema() + "." + name4
    name6 = session.getCurrentDatabase() + "." + name5

    session.table(name1).write.mode("Overwrite").saveAsTable(name4)
    try:
        assert session.table(name4).collect() == expected
    finally:
        Utils.drop_table(session, name4)

    session.table(name1).write.mode("Overwrite").saveAsTable(name5)
    try:
        assert session.table(name4).collect() == expected
    finally:
        Utils.drop_table(session, name5)

    session.table(name1).write.mode("Overwrite").saveAsTable(name6)
    try:
        assert session.table(name6).collect() == expected
    finally:
        Utils.drop_table(session, name5)


def test_write_table_to_different_schema(session, temp_schema, table_name_1):
    name1 = table_name_1
    name2 = temp_schema + "." + name1
    session.table(name1).write.saveAsTable(name2)
    try:
        assert session.table(name2).collect() == [Row(1), Row(2), Row(3)]
    finally:
        Utils.drop_table(session, name2)


def test_consistent_table_name_behaviors(session):
    table_name = Utils.random_name()
    db = session.getCurrentDatabase()
    sc = session.getCurrentSchema()
    df = session.createDataFrame([[1], [2], [3]]).toDF("a")
    df.write.mode("overwrite").saveAsTable(table_name)
    table_names = [
        table_name,
        [table_name],
        [sc, table_name],
        [db, sc, table_name],
        f"{db}.{sc}.{table_name}",
    ]
    try:
        for tn in table_names:
            Utils.check_answer(session.table(tn), [Row(1), Row(2), Row(3)])
    finally:
        Utils.drop_table(session, table_name)

    for tn in table_names:
        df.write.mode("Overwrite").saveAsTable(tn)
        try:
            Utils.check_answer(session.table(table_name), [Row(1), Row(2), Row(3)])
        finally:
            Utils.drop_table(session, table_name)
