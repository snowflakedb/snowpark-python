#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime

import pytest

from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.types import (
    ArrayType,
    GeographyType,
    GeometryType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimeType,
    VariantType,
)
from tests.utils import Utils


@pytest.fixture(scope="function")
def table_name_1(session: Session, local_testing_mode: bool):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    if not local_testing_mode:
        Utils.create_table(session, table_name, "num int")
        session._run_query(f"insert into {table_name} values (1), (2), (3)")
    else:
        session.create_dataframe(
            [[1], [2], [3]], schema=StructType([StructField("num", IntegerType())])
        ).write.save_as_table(table_name)
    yield table_name
    session.table(table_name).drop_table()


@pytest.fixture(scope="function")
def table_name_4(session: Session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "num int")
    session._run_query(f"insert into {table_name} values (1), (2), (3)")
    yield table_name
    session.table(table_name).drop_table()


@pytest.fixture(scope="function")
def semi_structured_table(session: Session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session,
        table_name,
        "a1 array, o1 object, v1 variant, g1 geography, g2 geometry",
    )
    query = (
        f"insert into {table_name} select parse_json(a), parse_json(b), "
        "parse_json(a), to_geography(c), to_geometry(c) from values('[1,2]', '{a:1}', "
        "'POINT(-122.35 37.55)'), ('[1,2,3]', '{b:2}', 'POINT(-12 37)') as T(a,b,c)"
    )
    session._run_query(query)
    yield table_name
    session.table(table_name).drop_table()


@pytest.fixture(scope="function")
def temp_table_name(session: Session, temp_schema: str, local_testing_mode: bool):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name_with_schema = f"{temp_schema}.{table_name}"
    if not local_testing_mode:
        Utils.create_table(session, table_name_with_schema, "str string")
        session._run_query(f"insert into {table_name_with_schema} values ('abc')")
    else:
        session.create_dataframe(
            [["abc"]], schema=StructType([StructField("str", StringType())])
        ).write.saveAsTable(table_name_with_schema)
    yield table_name
    session.table(table_name_with_schema).drop_table()


@pytest.fixture(scope="function")
def table_with_time(session: Session, local_testing_mode: bool):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    if not local_testing_mode:
        Utils.create_table(session, table_name, "time time")
        session._run_query(
            f"insert into {table_name} select to_time(a) from values('09:15:29'),"
            f"('09:15:29.99999999') as T(a)"
        )
    else:
        session.create_dataframe(
            [[datetime.time(9, 15, 29)], [datetime.time(9, 15, 29, 999999)]],
            schema=StructType([StructField("time", TimeType())]),
        ).write.saveAsTable(table_name)
    yield table_name
    session.table(table_name).drop_table()


@pytest.mark.localtest
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


@pytest.mark.localtest
def test_save_as_snowflake_table(session, table_name_1, local_testing_mode):
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
        with pytest.raises(SnowparkSQLException):
            df.write.mode("errorifexists").save_as_table(table_name_2)
    finally:
        session.table(table_name_2).drop_table()
        session.table(table_name_3).drop_table()


@pytest.mark.localtest
def test_multipart_identifier(session, table_name_1, local_testing_mode):
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
        session.table(name4).drop_table()
    session.table(name1).write.mode("Overwrite").save_as_table(name5)
    try:
        assert session.table(name4).collect() == expected
    finally:
        session.table(name5).drop_table()

    session.table(name1).write.mode("Overwrite").save_as_table(name6)
    try:
        assert session.table(name6).collect() == expected
    finally:
        session.table(name6).drop_table()


@pytest.mark.localtest
def test_write_table_to_different_schema(
    session, temp_schema, table_name_1, local_testing_mode
):
    name1 = table_name_1
    name2 = temp_schema + "." + name1
    session.table(name1).write.save_as_table(name2)
    try:
        assert session.table(name2).collect() == [Row(1), Row(2), Row(3)]
    finally:
        session.table(name2).drop_table()


@pytest.mark.localtest
def test_read_from_different_schema(session, temp_schema, temp_table_name):
    table_from_different_schema = f"{temp_schema}.{temp_table_name}"
    df = session.table(table_from_different_schema)
    Utils.check_answer(df, [Row("abc")])


@pytest.mark.localtest
def test_quotes_upper_and_lower_case_name(session, table_name_1):
    tested_table_names = [
        '"' + table_name_1 + '"',
        table_name_1.lower(),
        table_name_1.upper(),
    ]
    for table_name in tested_table_names:
        Utils.check_answer(session.table(table_name), [Row(1), Row(2), Row(3)])


# TODO: enable for local testing after emulating snowflake data types
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="geometry and geopgrahy types not yet supported in local testing mode.",
)
def test_table_with_semi_structured_types(session, semi_structured_table):
    df = session.table(semi_structured_table)
    types = [s.datatype for s in df.schema.fields]
    assert len(types) == 5
    expected_types = [
        ArrayType(StringType()),
        MapType(StringType(), StringType()),
        VariantType(),
        GeographyType(),
        GeometryType(),
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
                '{\n  "coordinates": [\n    -1.223500000000000e+02,\n    3.755000000000000e+01\n  ],\n  "type": "Point"\n}',
            ),
            Row(
                "[\n  1,\n  2,\n  3\n]",
                '{\n  "b": 2\n}',
                "[\n  1,\n  2,\n  3\n]",
                '{\n  "coordinates": [\n    -12,\n    37\n  ],\n  "type": "Point"\n}',
                '{\n  "coordinates": [\n    -1.200000000000000e+01,\n    3.700000000000000e+01\n  ],\n  "type": "Point"\n}',
            ),
        ],
        sort=False,
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1374013: Local testing fails to parse time '09:15:29.999999'",
)
def test_table_with_time_type(session, table_with_time):
    df = session.table(table_with_time)
    # snowflake time has accuracy to 0.99999999. Python has accuracy to 0.999999.
    Utils.check_answer(
        df,
        [Row(datetime.time(9, 15, 29)), Row(datetime.time(9, 15, 29, 999999))],
        sort=False,
    )


@pytest.mark.localtest
def test_consistent_table_name_behaviors(session, local_testing_mode):
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
        session.table(table_name).drop_table()

    for tn in table_names:
        df.write.mode("Overwrite").save_as_table(tn)
        try:
            Utils.check_answer(session.table(table_name), [Row(1), Row(2), Row(3)])
        finally:
            session.table(table_name).drop_table()
