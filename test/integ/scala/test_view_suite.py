#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from decimal import Decimal

import pytest

from src.snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from src.snowflake.snowpark.row import Row
from src.snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from test.utils import TestData, Utils


def test_create_view(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        try:
            view_name = Utils.random_name()
            TestData.integer1(session).createOrReplaceView(view_name)

            res = session.sql(f"select * from {view_name}").collect()
            # don't sort
            assert res == [Row([1]), Row([2]), Row([3])]

            # Test replace
            TestData.double1(session).createOrReplaceView(view_name)
            res = session.sql(f"select * from {view_name}").collect()
            # don't sort
            assert res == [Row([Decimal('1.111')]), Row([Decimal('2.222')]), Row([Decimal('3.333')])]
        finally:
            Utils.drop_view(session, view_name)


def test_view_name_with_special_character(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        try:
            view_name = Utils.random_name()
            TestData.column_has_special_char(session).createOrReplaceView(view_name)

            res = session.sql(f"select * from {AnalyzerPackage.quote_name(view_name)}").collect()
            # don't sort
            assert res == [Row([1, 2]), Row([3, 4])]
        finally:
            Utils.drop_view(session, view_name)


def test_only_works_on_select(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        view_name = Utils.random_name()
        with pytest.raises(SnowparkClientException) as ex_info:
            session.sql('show tables').createOrReplaceView(view_name)


def test_consistent_view_name_behaviors(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        view_name = Utils.random_name()
        sc = session.get_current_schema()
        db = session.get_current_database()

        name_parts = [db, sc, view_name]

        df = session.createDataFrame([1, 2, 3]).toDF('a')

        try:
            df.createOrReplaceView(view_name)
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            df.createOrReplaceView(name_parts)
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            df.createOrReplaceView([sc, view_name])
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            df.createOrReplaceView([view_name])
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            df.createOrReplaceView(f"{db}.{sc}.{view_name}")
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            # create temp view
            df.createOrReplaceTempView(view_name)
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            df.createOrReplaceTempView(name_parts)
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            df.createOrReplaceTempView([sc, view_name])
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            df.createOrReplaceTempView([view_name])
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

            df.createOrReplaceTempView(f"{db}.{sc}.{view_name}")
            res = session.table(view_name).collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1]), Row([2]), Row([3])]
            Utils.drop_view(session, view_name)

        finally:
            Utils.drop_view(session, view_name)



