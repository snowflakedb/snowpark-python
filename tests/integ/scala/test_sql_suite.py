#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import os

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import LongType, StructField, StructType
from tests.utils import TestFiles, Utils


def test_non_select_queries(session):
    try:
        stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
        Utils.create_stage(session, stage_name)
        res = session.sql(f"show stages like '{stage_name}'").collect()
        assert len(res) == 1
        # verify result is not empty
        assert f"{stage_name}" in str(res[0])

        table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(session, table_name1, "num int")
        res = session.sql(f"show tables like '{table_name1}'").collect()
        assert len(res) == 1
        # verify result is not empty
        assert f"{table_name1}" in str(res[0])

        res = session.sql("alter session set lock_timeout = 3600").collect()
        assert len(res) == 1
        # verify result is not empty
        assert "Statement executed successfully" in str(res[0])

    finally:
        Utils.drop_stage(session, stage_name)
        Utils.drop_table(session, table_name1)


def test_put_get_should_not_be_performed_when_preparing(
    session, resources_path, tmpdir
):
    test_files = TestFiles(resources_path)
    path = test_files.test_file_csv
    file_name = "testCSV.csv"

    tmp_stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)

    # add spaces to the query
    put_query = f"put file:///{path} @{tmp_stage_name}"

    try:
        Utils.create_stage(session, tmp_stage_name)

        put = session.sql(put_query)
        assert len(put.schema.fields) > 0
        # should upload nothing
        assert session.sql(f"ls @{tmp_stage_name}").collect() == []

        put.collect()

        # unescaped_path = os.path.join(tmpdir.strpath, Utils.random_name())
        output_path = Utils.escape_path(tmpdir.strpath)

        # add spaces to the query
        get_query = f"get @{tmp_stage_name}/{file_name}.gz file://{output_path}/"
        get = session.sql(get_query)
        # should download nothing
        assert len(get.schema.fields) > 0

        get.collect()
        assert os.path.exists(os.path.join(output_path, f"{file_name}.gz"))
    finally:
        Utils.drop_stage(session, tmp_stage_name)


def test_run_sql_query(session):
    df1 = session.sql("select * from values (1),(2),(3)")
    assert df1.collect() == [Row(1), Row(2), Row(3)]

    df2 = session.sql(
        "select variance(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b)"
    )
    assert str(df2.collect()[0][0]) == "0.800000"

    df3 = session.sql("select * from values (1),(2),(3) as T(id)").filter(col("id") < 3)
    assert df3.collect() == [Row(1), Row(2)]

    df4 = session.sql("select * from values (1,1),(2,1),(3,1) as T(a,b)")
    df5 = session.sql("select * from values (1,2),(2,2),(3,2) as T(a,b)")
    df6 = df4.union(df5).filter(col("a") < 3)

    res = df6.collect()
    res.sort(key=lambda x: (x[0], x[1]))
    assert res == [Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2)]

    with pytest.raises(ProgrammingError) as ex_info:
        session.sql("select * from (1)").collect()

    with pytest.raises(ProgrammingError) as ex_info:
        session.sql("select sum(a) over () from values 1.0, 2.0 T(a)").collect()


def test_create_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    other_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        table = session.sql(f"create or replace table {table_name} (num int)")
        assert len(table.schema.fields) > 0

        # assert the table is not created before collect
        with pytest.raises(ProgrammingError) as ex_info:
            session.sql(f"select * from {table_name}").collect()

        table.collect()

        # drop table
        drop_table = session.sql(f"drop table {table_name}")
        assert len(drop_table.schema.fields) > 0
        drop_table.collect()
        # assert that the table is already dropped
        with pytest.raises(ProgrammingError) as ex_info:
            session.sql(f"select * from {table}").collect()

        # test when create/drop table fails
        # throws exception during prepare
        session.sql(f"create or replace table {other_name}")
        with pytest.raises(ProgrammingError) as ex_info:
            session.sql(f"create or replace table {other_name}").collect()
        session.sql(f"drop table {other_name}")
        with pytest.raises(ProgrammingError) as ex_info:
            session.sql(f"drop table {other_name}").collect()

    finally:
        Utils.drop_table(session, table_name)
        Utils.drop_table(session, other_name)


def test_insert_into_table(session):
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name1, "num int")
        insert = session.sql(f"insert into {table_name1} values(1),(2),(3)")
        expected_schema = StructType(
            [StructField('"number of rows inserted"', LongType(), nullable=False)]
        )
        assert insert.schema == expected_schema
        insert.collect()
        df = session.sql(f"select * from {table_name1}")
        assert df.collect() == [Row(1), Row(2), Row(3)]

        # test for insertion to a non-existing table
        # no error
        session.sql(f"insert into {table_name2} values(1),(2),(3)")
        with pytest.raises(ProgrammingError):
            session.sql(f"insert into {table_name2} values(1),(2),(3)").collect()

        # test for insertion with wrong type of data, throws exception when collect
        insert2 = session.sql(f"insert into {table_name1} values(1.4),('test')")
        with pytest.raises(ProgrammingError):
            insert2.collect()

    finally:
        Utils.drop_table(session, table_name1)
        Utils.drop_table(session, table_name2)


def test_show(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name, "num int")
        table_sql_res = session.sql("SHOW TABLES")
        res = table_sql_res.collect()
        assert len(res) > 0
    finally:
        Utils.drop_table(session, table_name)

    # test when input is a wrong show command, throws exception when prepare
    # no error
    session.sql("SHOW TABLE")
    with pytest.raises(ProgrammingError):
        session.sql("SHOW TABLE").collect()
