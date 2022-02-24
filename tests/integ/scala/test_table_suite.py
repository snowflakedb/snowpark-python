#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.types import (
    ArrayType,
    GeographyType,
    MapType,
    StringType,
    VariantType,
)
from tests.utils import Utils


@pytest.fixture(scope="function")
def table_name_1(session: Session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "num int")
    session._run_query(f"insert into {table_name} values (1), (2), (3)")
    yield table_name
    Utils.drop_table(session, table_name)


@pytest.fixture(scope="function")
def table_name_4(session: Session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "num int")
    session._run_query(f"insert into {table_name} values (1), (2), (3)")
    yield table_name
    Utils.drop_table(session, table_name)


@pytest.fixture(scope="function")
def semi_structured_table(session: Session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "a1 array, o1 object, v1 variant, g1 geography"
    )
    query = (
        f"insert into {table_name} select parse_json(a), parse_json(b), "
        "parse_json(a), to_geography(c) from values('[1,2]', '{a:1}', 'POINT(-122.35 37.55)'),"
        "('[1,2,3]', '{b:2}', 'POINT(-12 37)') as T(a,b,c)"
    )
    session._run_query(query)
    yield table_name
    Utils.drop_table(session, table_name)


@pytest.fixture(scope="function")
def temp_table_name(session: Session, temp_schema: str):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name_with_schema = f"{temp_schema}.{table_name}"
    Utils.create_table(session, table_name_with_schema, "str string")
    session._run_query(f"insert into {table_name_with_schema} values ('abc')")
    yield table_name
    Utils.drop_table(session, table_name_with_schema)


@pytest.fixture(scope="function")
def table_with_time(session: Session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "time time")
    session._run_query(
        f"insert into {table_name} select to_time(a) from values('09:15:29'),"
        f"('09:15:29.99999999') as T(a)"
    )
    yield table_name
    Utils.drop_table(session, table_name)


def test_read_snowflake_table(session, table_name_1):
    df = session.table(table_name_1)
    Utils.check_answer(df, [Row(1), Row(2), Row(3)])

    df1 = session.read.table(table_name_1)
    Utils.check_answer(df1, [Row(1), Row(2), Row(3)])

    db_schema_table_name = f"{session.get_current_database()}.{session.get_current_schema()}.{table_name_1}"
    df2 = session.table(db_schema_table_name)
    Utils.check_answer(df2, [Row(1), Row(2), Row(3)])

    df3 = session.read.table(db_schema_table_name)
    Utils.check_answer(df3, [Row(1), Row(2), Row(3)])


def test_save_as_snowflake_table(session, table_name_1):
    df = session.table(table_name_1)
    assert df.collect() == [Row(1), Row(2), Row(3)]
    table_name_2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name_3 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        # copy table_name_1 to table_name_2, default mode
        df.write.save_as_table(table_name_2)

        df2 = session.table(table_name_2)
        assert df2.collect() == [Row(1), Row(2), Row(3)]

        # append mode
        df.write.mode("append").save_as_table(table_name_2)
        df4 = session.table(table_name_2)
        assert df4.collect() == [Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)]

        # ignore mode
        df.write.mode("IGNORE").save_as_table(table_name_2)
        df3 = session.table(table_name_2)
        assert df3.collect() == [Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)]

        # overwrite mode
        df.write.mode("OvErWrItE").save_as_table(table_name_2)
        df5 = session.table(table_name_2)
        assert df5.collect() == [Row(1), Row(2), Row(3)]

        # test for append when the original table does not exist
        # need to create the table before insertion
        df.write.mode("aPpEnD").save_as_table(table_name_3)
        df6 = session.table(table_name_3)
        assert df6.collect() == [Row(1), Row(2), Row(3)]

        # errorifexists mode
        with pytest.raises(ProgrammingError):
            df.write.mode("errorifexists").save_as_table(table_name_2)
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
    name2 = session.get_current_schema() + "." + name1
    name3 = session.get_current_database() + "." + name2

    expected = [Row(1), Row(2), Row(3)]
    assert session.table(name1).collect() == expected
    assert session.table(name2).collect() == expected
    assert session.table(name3).collect() == expected

    name4 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    name5 = session.get_current_schema() + "." + name4
    name6 = session.get_current_database() + "." + name5

    session.table(name1).write.mode("Overwrite").save_as_table(name4)
    try:
        assert session.table(name4).collect() == expected
    finally:
        Utils.drop_table(session, name4)

    session.table(name1).write.mode("Overwrite").save_as_table(name5)
    try:
        assert session.table(name4).collect() == expected
    finally:
        Utils.drop_table(session, name5)

    session.table(name1).write.mode("Overwrite").save_as_table(name6)
    try:
        assert session.table(name6).collect() == expected
    finally:
        Utils.drop_table(session, name5)


def test_write_table_to_different_schema(session, temp_schema, table_name_1):
    name1 = table_name_1
    name2 = temp_schema + "." + name1
    session.table(name1).write.save_as_table(name2)
    try:
        assert session.table(name2).collect() == [Row(1), Row(2), Row(3)]
    finally:
        Utils.drop_table(session, name2)


def test_read_from_different_schema(session, temp_schema, temp_table_name):
    table_from_different_schema = f"{temp_schema}.{temp_table_name}"
    df = session.table(table_from_different_schema)
    Utils.check_answer(df, [Row("abc")])


def test_quotes_upper_and_lower_case_name(session, table_name_1):
    tested_table_names = [
        '"' + table_name_1 + '"',
        table_name_1.lower(),
        table_name_1.upper(),
    ]
    for table_name in tested_table_names:
        Utils.check_answer(session.table(table_name), [Row(1), Row(2), Row(3)])


def test_table_with_semi_structured_types(session, semi_structured_table):
    df = session.table(semi_structured_table)
    types = [s.datatype for s in df.schema.fields]
    assert len(types) == 4
    expected_types = [
        ArrayType(StringType()),
        MapType(StringType(), StringType()),
        VariantType(),
        GeographyType(),
    ]
    assert all([t in expected_types for t in types])
    Utils.check_answer(
        df,
        [
            Row(
                "[\n  1,\n  2\n]",
                '{\n  "a": 1\n}',
                "[\n  1,\n  2\n]",
                '{\n  "coordinates": [\n    -122.35,\n    37.55\n  ],\n  "type": "Point"\n}',
            ),
            Row(
                "[\n  1,\n  2,\n  3\n]",
                '{\n  "b": 2\n}',
                "[\n  1,\n  2,\n  3\n]",
                '{\n  "coordinates": [\n    -12,\n    37\n  ],\n  "type": "Point"\n}',
            ),
        ],
        sort=False,
    )


@pytest.mark.skip("To port from scala after Python implements Geography")
def test_row_with_geography(session):
    pass


def test_table_with_time_type(session, table_with_time):
    df = session.table(table_with_time)
    # snowflake time has accuracy to 0.99999999. Python has accuracy to 0.999999.
    Utils.check_answer(
        df,
        [Row(datetime.time(9, 15, 29)), Row(datetime.time(9, 15, 29, 999999))],
        sort=False,
    )


def test_consistent_table_name_behaviors(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    db = session.get_current_database()
    sc = session.get_current_schema()
    df = session.create_dataframe([[1], [2], [3]]).to_df("a")
    df.write.mode("overwrite").save_as_table(table_name)
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
        df.write.mode("Overwrite").save_as_table(tn)
        try:
            Utils.check_answer(session.table(table_name), [Row(1), Row(2), Row(3)])
        finally:
            Utils.drop_table(session, table_name)
