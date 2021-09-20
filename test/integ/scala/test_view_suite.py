#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from decimal import Decimal

from snowflake.snowpark.functions import col, sql_expr, sum
from snowflake.snowpark.types.sf_types import LongType
from test.utils import TestData, Utils

import pytest

from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException


def test_create_view(session):
    try:
        view_name = Utils.random_name()
        TestData.integer1(session).createOrReplaceView(view_name)

        res = session.sql(f"select * from {view_name}").collect()
        # don't sort
        assert res == [Row(1), Row(2), Row(3)]

        # Test replace
        TestData.double1(session).createOrReplaceView(view_name)
        res = session.sql(f"select * from {view_name}").collect()
        # don't sort
        assert res == [
            Row(Decimal("1.111")),
            Row(Decimal("2.222")),
            Row(Decimal("3.333")),
        ]
    finally:
        Utils.drop_view(session, view_name)


def test_view_name_with_special_character(session):
    try:
        view_name = Utils.random_name()
        TestData.column_has_special_char(session).createOrReplaceView(view_name)

        res = session.sql(
            f"select * from {AnalyzerPackage.quote_name(view_name)}"
        ).collect()
        # don't sort
        assert res == [Row(1, 2), Row(3, 4)]
    finally:
        Utils.drop_view(session, view_name)


def test_only_works_on_select(session):
    view_name = Utils.random_name()
    with pytest.raises(SnowparkClientException) as ex_info:
        session.sql("show tables").createOrReplaceView(view_name)


def test_consistent_view_name_behaviors(session):
    view_name = Utils.random_name()
    sc = session.getCurrentSchema()
    db = session.getCurrentDatabase()

    name_parts = [db, sc, view_name]

    df = session.createDataFrame([1, 2, 3]).toDF("a")

    try:
        df.createOrReplaceView(view_name)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        df.createOrReplaceView(name_parts)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        df.createOrReplaceView([sc, view_name])
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        df.createOrReplaceView([view_name])
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        df.createOrReplaceView(f"{db}.{sc}.{view_name}")
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        # create temp view
        df.createOrReplaceTempView(view_name)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        df.createOrReplaceTempView(name_parts)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        df.createOrReplaceTempView([sc, view_name])
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        df.createOrReplaceTempView([view_name])
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

        df.createOrReplaceTempView(f"{db}.{sc}.{view_name}")
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]
        Utils.drop_view(session, view_name)

    finally:
        Utils.drop_view(session, view_name)


def test_create_temp_view_on_functions(session):
    table_name = Utils.random_name()
    view_name = Utils.random_name()

    try:
        Utils.create_table(session, table_name, "id int, val int")
        t = session.table(table_name)
        a = t.groupBy(col("id")).agg(sql_expr("max(val)"))
        a.createOrReplaceTempView(view_name)
        schema = session.table(view_name).schema
        assert len(schema.fields) == 2
        assert schema.fields[0].datatype == LongType()
        assert schema.fields[0].name == "ID"
        assert schema.fields[1].datatype == LongType()
        assert schema.fields[1].name == "\"MAX(VAL)\""

        a2 = t.groupBy(col("id")).agg(sum(col("val")))
        a2.createOrReplaceTempView(view_name)
        schema1 = session.table(view_name).schema
        assert len(schema1.fields) == 2
        assert schema1.fields[0].datatype == LongType()
        assert schema1.fields[0].name == "ID"
        assert schema1.fields[1].datatype == LongType()
        assert schema1.fields[1].name == "\"SUM(VAL)\""

        a3 = t.groupBy(col("id")).agg(sum(col("val")) + 1)
        a3.createOrReplaceTempView(view_name)
        schema2 = session.table(view_name).schema
        assert len(schema.fields) == 2
        assert schema2.fields[0].datatype == LongType()
        assert schema2.fields[0].name == "ID"
        assert schema2.fields[1].datatype == LongType()
        assert schema2.fields[1].name == "\"ADD(SUM(VAL), LITERAL())\""

    finally:
        Utils.drop_table(session, table_name)
        Utils.drop_view(session, view_name)
