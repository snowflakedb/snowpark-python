#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from decimal import Decimal
from test.utils import TestData, Utils

from snowflake.snowpark.functions import (
    avg,
    coalesce,
    col,
    count,
    count_distinct,
    kurtosis,
    lit,
    max,
    mean,
    min,
    sum,
    sum_distinct,
    var_pop,
    var_samp,
    variance,
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


def test_kurtosis(session_cnx):
    with session_cnx() as session:
        df = TestData.xyz(session).select(
            kurtosis(col("X")), kurtosis(col("Y")), kurtosis(col("Z"))
        )
        Utils.check_answer(
            df,
            [
                Row(
                    [
                        Decimal("-3.333333333333"),
                        Decimal("5.0"),
                        Decimal("3.613736609956"),
                    ]
                )
            ],
        )

        # same as above, but pass str instead of Column
        df = TestData.xyz(session).select(kurtosis("X"), kurtosis("Y"), kurtosis("Z"))
        Utils.check_answer(
            df,
            [
                Row(
                    [
                        Decimal("-3.333333333333"),
                        Decimal("5.0"),
                        Decimal("3.613736609956"),
                    ]
                )
            ],
        )


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

        df = TestData.duplicated_numbers(session).groupBy("A").agg(sum_distinct("A"))
        assert df.collect() == [Row([3, 3]), Row([2, 2]), Row([1, 1])]


def test_variance(session_cnx):
    with session_cnx() as session:
        df = (
            TestData.xyz(session)
            .groupBy("X")
            .agg([variance(col("Y")), var_pop(col("Z")), var_samp(col("Z"))])
        )
        Utils.check_answer(
            df,
            [
                Row([Decimal(1), Decimal(0.00), Decimal(1.0), Decimal(2.0)]),
                Row(
                    [
                        Decimal(2),
                        Decimal("0.333333"),
                        Decimal("14.888889"),
                        Decimal("22.333333"),
                    ]
                ),
            ],
        )

        # same as above, but pass str instead of Column
        df = (
            TestData.xyz(session)
            .groupBy("X")
            .agg([variance("Y"), var_pop("Z"), var_samp("Z")])
        )
        Utils.check_answer(
            df,
            [
                Row([Decimal(1), Decimal(0.00), Decimal(1.0), Decimal(2.0)]),
                Row(
                    [
                        Decimal(2),
                        Decimal("0.333333"),
                        Decimal("14.888889"),
                        Decimal("22.333333"),
                    ]
                ),
            ],
        )


def test_coalesce(session_cnx):
    with session_cnx() as session:
        Utils.check_answer(
            TestData.null_data2(session).select(coalesce(col("A"), col("B"), col("C"))),
            [Row(1), Row(2), Row(3), Row(None), Row(1), Row(1), Row(1)],
            sort=False,
        )

        # same as above, but pass str instead of Column
        Utils.check_answer(
            TestData.null_data2(session).select(coalesce("A", "B", "C")),
            [Row(1), Row(2), Row(3), Row(None), Row(1), Row(1), Row(1)],
            sort=False,
        )
