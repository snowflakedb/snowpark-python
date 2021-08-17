#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from decimal import Decimal
from test.utils import TestData

from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    count_distinct,
    lit,
    max,
    mean,
    min,
    sum,
    sum_distinct,
)
from snowflake.snowpark.row import Row


def test_col(session_cnx):
    with session_cnx() as session:
        res = TestData.test_data1(session).select(col("bool")).collect()
        assert res == [Row(True), Row(False)]


def test_lit(session_cnx):
    with session_cnx() as session:
        res = TestData.test_data1(session).select(lit(1)).collect()
        assert res == [Row(1), Row(1)]


def test_avg(session_cnx):
    with session_cnx() as session:
        res = TestData.duplicated_numbers(session).select(avg(col("A"))).collect()
        assert res == [Row(Decimal("2.2"))]

        # same as above, but pass str instead of Column
        res = TestData.duplicated_numbers(session).select(avg("A")).collect()
        assert res == [Row(Decimal("2.2"))]


def test_count(session_cnx):
    with session_cnx() as session:
        res = TestData.duplicated_numbers(session).select(count(col("A"))).collect()
        assert res == [Row(5)]

        df = TestData.duplicated_numbers(session).select(count_distinct(col("A")))
        assert df.collect() == [Row(3)]

        # same as above, but pass str instead of Column
        res = TestData.duplicated_numbers(session).select(count("A")).collect()
        assert res == [Row(5)]

        df = TestData.duplicated_numbers(session).select(count_distinct("A"))
        assert df.collect() == [Row(3)]


def test_max_min_mean(session_cnx):
    with session_cnx() as session:
        df = TestData.xyz(session).select(max(col("X")), min(col("Y")), mean(col("Z")))
        assert df.collect() == [Row([2, 1, Decimal("3.6")])]

        # same as above, but pass str instead of Column
        df = TestData.xyz(session).select(max("X"), min("Y"), mean("Z"))
        assert df.collect() == [Row([2, 1, Decimal("3.6")])]


def test_sum(session_cnx):
    with session_cnx() as session:
        df = TestData.duplicated_numbers(session).groupBy("A").agg(sum(col("A")))
        assert df.collect() == [Row([3, 6]), Row([2, 4]), Row([1, 1])]

        df = (
            TestData.duplicated_numbers(session)
            .groupBy("A")
            .agg(sum_distinct(col("A")))
        )
        assert df.collect() == [Row([3, 3]), Row([2, 2]), Row([1, 1])]

        # same as above, but pass str instead of Column
        df = TestData.duplicated_numbers(session).groupBy("A").agg(sum("A"))
        assert df.collect() == [Row([3, 6]), Row([2, 4]), Row([1, 1])]

        df = (
            TestData.duplicated_numbers(session).groupBy("A").agg(sum_distinct("A"))
        )
        assert df.collect() == [Row([3, 3]), Row([2, 2]), Row([1, 1])]
