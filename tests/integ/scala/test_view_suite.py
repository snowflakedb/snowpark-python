#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from decimal import Decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType, quote_name
from snowflake.snowpark.exceptions import SnowparkCreateViewException
from snowflake.snowpark.functions import col, sql_expr, sum
from snowflake.snowpark.types import (
    DecimalType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    StringType,
    DoubleType,
)
from tests.utils import TestData, Utils, TestFiles, IS_IN_STORED_PROC_LOCALFS


def test_create_view(session, local_testing_mode):
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    try:
        TestData.integer1(session).create_or_replace_view(view_name)

        res = session.table(view_name).collect()
        # don't sort
        assert res == [Row(1), Row(2), Row(3)]

        # Test replace
        TestData.double1(session).create_or_replace_view(view_name)
        res = session.table(view_name).collect()
        # don't sort
        assert res == [
            Row(Decimal("1.111")),
            Row(Decimal("2.222")),
            Row(Decimal("3.333")),
        ]
    finally:
        if not local_testing_mode:
            Utils.drop_view(session, view_name)


@pytest.mark.parametrize("is_temp", [True, False])
def test_comment_on_view(session, local_testing_mode, is_temp):
    df = session.create_dataframe([(1,), (2,)], schema=["a"])
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    comment = f"COMMENT_{Utils.random_alphanumeric_str(6)}"
    try:
        if is_temp:
            df.create_or_replace_temp_view(view_name, comment=comment)
        else:
            df.create_or_replace_view(view_name, comment=comment)

        if not local_testing_mode:
            ddl_sql = f"select get_ddl('VIEW', '{view_name}')"
            assert comment in session.sql(ddl_sql).collect()[0][0]
    finally:
        if not local_testing_mode:
            Utils.drop_view(session, view_name)


def test_view_name_with_special_character(session, local_testing_mode):
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    try:
        TestData.column_has_special_char(session).create_or_replace_view(view_name)

        res = session.table(quote_name(view_name)).collect()
        # don't sort
        assert res == [Row(1, 2), Row(3, 4)]
    finally:
        if not local_testing_mode:
            Utils.drop_view(session, view_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL is not supported in Local Testing",
    run=False,
)
def test_view_with_with_sql_statement(session):
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    try:
        TestData.sql_using_with_select_statement(session).create_or_replace_view(
            view_name
        )

        res = session.sql(f"select * from {quote_name(view_name)}").collect()
        assert res == [Row(1, 2)]
    finally:
        Utils.drop_view(session, view_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is a SQL test",
    run=False,
)
def test_only_works_on_select(session):
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    with pytest.raises(SnowparkCreateViewException):
        session.sql("show tables").create_or_replace_view(view_name)


def test_consistent_view_name_behaviors(session, local_testing_mode):
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    sc = session.get_current_schema()
    db = session.get_current_database()

    name_parts = [db, sc, view_name]

    df = session.create_dataframe([1, 2, 3]).to_df("a")

    try:
        df.create_or_replace_view(view_name)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        df.create_or_replace_view(name_parts)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        df.create_or_replace_view([sc, view_name])
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        df.create_or_replace_view([view_name])
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        df.create_or_replace_view(f"{db}.{sc}.{view_name}")
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        # create temp view
        df.create_or_replace_temp_view(view_name)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        df.create_or_replace_temp_view(name_parts)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        df.create_or_replace_temp_view([sc, view_name])
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        df.create_or_replace_temp_view([view_name])
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        df.create_or_replace_temp_view(f"{db}.{sc}.{view_name}")
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

    finally:
        if not local_testing_mode:
            Utils.drop_view(session, view_name)


def test_create_temp_view_on_functions(session, local_testing_mode):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)

    try:
        session.create_dataframe(
            [],
            schema=StructType(
                [StructField("id", IntegerType()), StructField("val", IntegerType())]
            ),
        ).write.save_as_table(table_name)
        t = session.table(table_name)
        if not local_testing_mode:  # Use of sql_expr is not supported in local testing
            a = t.group_by(col("id")).agg(sql_expr("max(val)"))
            a.create_or_replace_temp_view(view_name)
            schema = session.table(view_name).schema
            assert len(schema.fields) == 2
            assert schema.fields[0].datatype == LongType()
            assert schema.fields[0].name == "ID"
            assert schema.fields[1].datatype == LongType()
            assert schema.fields[1].name == '"MAX(VAL)"'

        a2 = t.group_by(col("id")).agg(sum(col("val")))
        a2.create_or_replace_temp_view(view_name)
        schema1 = session.table(view_name).schema
        assert len(schema1.fields) == 2
        assert schema1.fields[0].datatype == LongType()
        assert schema1.fields[0].name == "ID"
        assert schema1.fields[1].datatype == LongType()
        assert schema1.fields[1].name == '"SUM(VAL)"'

        a3 = t.group_by(col("id")).agg(sum(col("val")) + 1)
        a3.create_or_replace_temp_view(view_name)
        schema2 = session.table(view_name).schema
        assert len(schema2.fields) == 2
        assert schema2.fields[0].datatype == LongType()
        assert schema2.fields[0].name == "ID"
        assert schema2.fields[1].datatype in (LongType(), DecimalType(38, 0))
        assert schema2.fields[1].name == '"ADD(SUM(VAL), LITERAL())"'

    finally:
        Utils.drop_table(session, table_name)
        if not local_testing_mode:
            Utils.drop_view(session, view_name)


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="need resources")
def test_create_or_replace_temp_view_with_file(session, resources_path):
    original_value = session._conn._thread_safe_session_enabled
    try:
        session._conn.set_thread_safe_session_enabled = True
        tmp_stage_name = Utils.random_stage_name()
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
        test_files = TestFiles(resources_path)
        Utils.upload_to_stage(
            session, f"@{tmp_stage_name}", test_files.test_file_csv, compress=False
        )
        test_file_on_stage = f"@{tmp_stage_name}/testCSV.csv"
        user_schema = StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", StringType()),
                StructField("c", DoubleType()),
            ]
        )

        df = (
            session.read.option("purge", False)
            .schema(user_schema)
            .csv(test_file_on_stage)
        )
        view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
        df.create_or_replace_temp_view(view_name)
        Utils.check_answer(
            session.table(view_name),
            [Row(A=1, B="one", C=1.2), Row(A=2, B="two", C=2.2)],
        )

        # no post action
        assert len(df.queries["post_actions"]) == 0

        # for sub-dataframes, we have a new temp table so even if it's dropped,
        # it won't affect the created temp view
        df.select("a").collect()
        Utils.check_answer(
            session.table(view_name),
            [Row(A=1, B="one", C=1.2), Row(A=2, B="two", C=2.2)],
        )
    finally:
        session._conn.set_thread_safe_session_enabled = original_value
