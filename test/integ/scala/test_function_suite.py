#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from datetime import date, datetime
from decimal import Decimal
from test.utils import TestData, Utils

from snowflake.snowpark.functions import (
    avg,
    builtin,
    coalesce,
    col,
    count,
    count_distinct,
    equal_nan,
    is_null,
    kurtosis,
    lit,
    max,
    mean,
    min,
    parse_json,
    skew,
    stddev,
    stddev_pop,
    stddev_samp,
    sum,
    sum_distinct,
    to_array,
    to_date,
    to_object,
    to_timestamp,
    to_variant,
    var_pop,
    var_samp,
    variance,
)
from snowflake.snowpark.row import Row


def test_col(session):
    test_data1 = TestData.test_data1(session)
    Utils.check_answer(test_data1.select(col("bool")), [Row(True), Row(False)])
    Utils.check_answer(test_data1.select(col("num")), [Row(1), Row(2)])

    # same as above, but pass str instead of Column
    Utils.check_answer(test_data1.select(col("bool")), [Row(True), Row(False)])
    Utils.check_answer(test_data1.select(col("num")), [Row(1), Row(2)])


def test_lit(session):
    res = TestData.test_data1(session).select(lit(1)).collect()
    assert res == [Row(1), Row(1)]


def test_avg(session):
    res = TestData.duplicated_numbers(session).select(avg(col("A"))).collect()
    assert res == [Row(Decimal("2.2"))]

    # same as above, but pass str instead of Column
    res = TestData.duplicated_numbers(session).select(avg("A")).collect()
    assert res == [Row(Decimal("2.2"))]


def test_count(session):
    res = TestData.duplicated_numbers(session).select(count(col("A"))).collect()
    assert res == [Row(5)]

    df = TestData.duplicated_numbers(session).select(count_distinct(col("A")))
    assert df.collect() == [Row(3)]

    # same as above, but pass str instead of Column
    res = TestData.duplicated_numbers(session).select(count("A")).collect()
    assert res == [Row(5)]

    df = TestData.duplicated_numbers(session).select(count_distinct("A"))
    assert df.collect() == [Row(3)]


def test_kurtosis(session):
    df = TestData.xyz(session).select(
        kurtosis(col("X")), kurtosis(col("Y")), kurtosis(col("Z"))
    )
    Utils.check_answer(
        df,
        [
            Row(
                Decimal("-3.333333333333"),
                Decimal("5.0"),
                Decimal("3.613736609956"),
            )
        ],
    )

    # same as above, but pass str instead of Column
    df = TestData.xyz(session).select(kurtosis("X"), kurtosis("Y"), kurtosis("Z"))
    Utils.check_answer(
        df,
        [
            Row(
                Decimal("-3.333333333333"),
                Decimal("5.0"),
                Decimal("3.613736609956"),
            )
        ],
    )


def test_max_min_mean(session):
    df = TestData.xyz(session).select(max(col("X")), min(col("Y")), mean(col("Z")))
    assert df.collect() == [Row(2, 1, Decimal("3.6"))]

    # same as above, but pass str instead of Column
    df = TestData.xyz(session).select(max("X"), min("Y"), mean("Z"))
    assert df.collect() == [Row(2, 1, Decimal("3.6"))]


def test_skew(session):
    xyz = TestData.xyz(session)
    Utils.check_answer(
        xyz.select(skew(col("X")), skew(col("Y")), skew(col("Z"))),
        Row(-0.6085811063146803, -2.236069766354172, 1.8414236309018863),
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        xyz.select(skew("X"), skew("Y"), skew("Z")),
        Row(-0.6085811063146803, -2.236069766354172, 1.8414236309018863),
    )


def test_stddev(session):
    xyz = TestData.xyz(session)
    Utils.check_answer(
        xyz.select(stddev(col("X")), stddev_samp(col("Y")), stddev_pop(col("Z"))),
        Row(0.5477225575051661, 0.4472135954999579, 3.3226495451672298),
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        xyz.select(stddev("X"), stddev_samp("Y"), stddev_pop("Z")),
        Row(0.5477225575051661, 0.4472135954999579, 3.3226495451672298),
    )


def test_sum(session):
    df = TestData.duplicated_numbers(session).groupBy("A").agg(sum(col("A")))
    assert df.collect() == [Row(3, 6), Row(2, 4), Row(1, 1)]

    df = TestData.duplicated_numbers(session).groupBy("A").agg(sum_distinct(col("A")))
    assert df.collect() == [Row(3, 3), Row(2, 2), Row(1, 1)]

    # same as above, but pass str instead of Column
    df = TestData.duplicated_numbers(session).groupBy("A").agg(sum("A"))
    assert df.collect() == [Row(3, 6), Row(2, 4), Row(1, 1)]

    df = TestData.duplicated_numbers(session).groupBy("A").agg(sum_distinct("A"))
    assert df.collect() == [Row(3, 3), Row(2, 2), Row(1, 1)]


def test_variance(session):
    df = (
        TestData.xyz(session)
        .groupBy("X")
        .agg([variance(col("Y")), var_pop(col("Z")), var_samp(col("Z"))])
    )
    Utils.check_answer(
        df,
        [
            Row(Decimal(1), Decimal(0.00), Decimal(1.0), Decimal(2.0)),
            Row(
                Decimal(2),
                Decimal("0.333333"),
                Decimal("14.888889"),
                Decimal("22.333333"),
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
            Row(Decimal(1), Decimal(0.00), Decimal(1.0), Decimal(2.0)),
            Row(
                Decimal(2),
                Decimal("0.333333"),
                Decimal("14.888889"),
                Decimal("22.333333"),
            ),
        ],
    )


def test_coalesce(session):
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


def test_nan_and_null(session):
    nan_data1 = TestData.nan_data1(session)
    Utils.check_answer(
        nan_data1.select(equal_nan(col("A")), is_null(col("A"))),
        [Row(False, False), Row(True, False), Row(None, True), Row(False, False)],
        False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        nan_data1.select(equal_nan("A"), is_null("A")),
        [Row(False, False), Row(True, False), Row(None, True), Row(False, False)],
        False,
    )


def test_builtin_function(session):
    repeat = builtin("repeat")
    string1 = TestData.string1(session)
    Utils.check_answer(
        string1.select(repeat(col("B"), 3)),
        [Row("aaa"), Row("bbb"), Row("ccc")],
        sort=False,
    )


def test_to_timestamp(session):
    long1 = TestData.long1(session)
    Utils.check_answer(
        long1.select(to_timestamp(col("A"))),
        [
            Row(datetime(2019, 6, 25, 16, 19, 17)),
            Row(datetime(2019, 8, 10, 23, 25, 57)),
            Row(datetime(2006, 10, 22, 1, 12, 37)),
        ],
    )

    df = session.sql("select * from values('04/05/2020 01:02:03') as T(a)")

    Utils.check_answer(
        df.select(to_timestamp(col("A"), lit("mm/dd/yyyy hh24:mi:ss"))),
        Row(datetime(2020, 4, 5, 1, 2, 3)),
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        long1.select(to_timestamp("A")),
        [
            Row(datetime(2019, 6, 25, 16, 19, 17)),
            Row(datetime(2019, 8, 10, 23, 25, 57)),
            Row(datetime(2006, 10, 22, 1, 12, 37)),
        ],
    )

    Utils.check_answer(
        df.select(to_timestamp("A", lit("mm/dd/yyyy hh24:mi:ss"))),
        Row(datetime(2020, 4, 5, 1, 2, 3)),
    )


def test_to_date(session):
    df = session.sql("select * from values('2020-05-11') as T(a)")
    Utils.check_answer(df.select(to_date(col("A"))), [Row(date(2020, 5, 11))])

    # same as above, but pass str instead of Column
    Utils.check_answer(df.select(to_date("A")), [Row(date(2020, 5, 11))])

    df = session.sql("select * from values('2020.07.23') as T(a)")
    Utils.check_answer(
        df.select(to_date(col("A"), lit("YYYY.MM.DD"))), [Row(date(2020, 7, 23))]
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        df.select(to_date("A", lit("YYYY.MM.DD"))), [Row(date(2020, 7, 23))]
    )


def test_parse_json(session):
    null_json1 = TestData.null_json1(session)
    Utils.check_answer(
        null_json1.select(parse_json(col("v"))),
        [Row('{\n  "a": null\n}'), Row('{\n  "a": "foo"\n}'), Row(None)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        null_json1.select(parse_json("v")),
        [Row('{\n  "a": null\n}'), Row('{\n  "a": "foo"\n}'), Row(None)],
        sort=False,
    )


def test_to_array(session):
    integer1 = TestData.integer1(session)
    Utils.check_answer(
        integer1.select(to_array(col("a"))),
        [Row("[\n  1\n]"), Row("[\n  2\n]"), Row("[\n  3\n]")],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        integer1.select(to_array("a")),
        [Row("[\n  1\n]"), Row("[\n  2\n]"), Row("[\n  3\n]")],
        sort=False,
    )


def test_to_object(session):
    variant1 = TestData.variant1(session)
    Utils.check_answer(
        variant1.select(to_object(col("obj1"))), [Row('{\n  "Tree": "Pine"\n}')]
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        variant1.select(to_object("obj1")), [Row('{\n  "Tree": "Pine"\n}')]
    )


def test_to_variant(session):
    integer1 = TestData.integer1(session)
    Utils.check_answer(
        integer1.select(to_variant(col("a"))),
        [Row("1"), Row("2"), Row("3")],
        sort=False,
    )
    # TODO this should be Variant(1)
    assert integer1.select(to_variant(col("a"))).collect()[0][0] == "1"

    # same as above, but pass str instead of Column
    assert integer1.select(to_variant("a")).collect()[0][0] == "1"
