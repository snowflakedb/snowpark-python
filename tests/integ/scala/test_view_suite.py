#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from decimal import Decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkCreateViewException
from snowflake.snowpark.functions import col, sql_expr, sum
from snowflake.snowpark.types import (
    DecimalType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)
from tests.utils import TestData, Utils


@pytest.mark.localtest
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


@pytest.mark.localtest
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


@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')", reason="sql is not supported"
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


@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')", reason="sql use is not supported"
)
def test_only_works_on_select(session):
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    with pytest.raises(SnowparkCreateViewException):
        session.sql("show tables").create_or_replace_view(view_name)


@pytest.mark.localtest
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


@pytest.mark.localtest
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
        # assert schema1.fields[0].datatype == LongType() TODO: fix aggregation bug
        assert schema1.fields[0].name == "ID"
        assert schema1.fields[1].datatype == LongType()
        assert schema1.fields[1].name == '"SUM(VAL)"'

        a3 = t.group_by(col("id")).agg(sum(col("val")) + 1)
        a3.create_or_replace_temp_view(view_name)
        schema2 = session.table(view_name).schema
        assert len(schema2.fields) == 2
        # assert schema2.fields[0].datatype == LongType()
        assert schema2.fields[0].name == "ID"
        assert schema2.fields[1].datatype in (LongType(), DecimalType(38, 0))
        assert schema2.fields[1].name == '"ADD(SUM(VAL), LITERAL())"'

    finally:
        Utils.drop_table(session, table_name)
        if not local_testing_mode:
            Utils.drop_view(session, view_name)
