#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from datetime import date, datetime
from decimal import Decimal
from test.utils import TestData, Utils

from snowflake.snowpark import Row
from snowflake.snowpark.functions import (
    abs,
    array_agg,
    avg,
    builtin,
    ceil,
    char,
    coalesce,
    col,
    contains,
    count,
    count_distinct,
    equal_nan,
    exp,
    floor,
    is_null,
    kurtosis,
    lit,
    log,
    max,
    mean,
    min,
    negate,
    not_,
    parse_json,
    parse_xml,
    pow,
    random,
    skew,
    split,
    sqrt,
    startswith,
    stddev,
    stddev_pop,
    stddev_samp,
    substring,
    sum,
    sum_distinct,
    to_array,
    to_date,
    to_json,
    to_object,
    to_timestamp,
    to_variant,
    to_xml,
    translate,
    var_pop,
    var_samp,
    variance,
)


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


def test_negate_and_not(session):
    df = session.sql("select * from values(1, true),(-2,false) as T(a,b)")
    Utils.check_answer(
        df.select(negate(col("A")), not_(col("B"))), [Row(-1, False), Row(2, True)]
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        df.select(negate("A"), not_("B")), [Row(-1, False), Row(2, True)]
    )


def test_random(session):
    df = session.sql("select 1")
    df.select(random(123)).collect()
    df.select(random()).collect()


def test_sqrt(session):
    Utils.check_answer(
        TestData.test_data1(session).select(sqrt(col("NUM"))),
        [Row(1.0), Row(1.4142135623730951)],
        False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.test_data1(session).select(sqrt("NUM")),
        [Row(1.0), Row(1.4142135623730951)],
        False,
    )


def test_abs(session):
    Utils.check_answer(
        TestData.number2(session).select(abs(col("X"))), [Row(1), Row(0), Row(5)], False
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.number2(session).select(abs("X")), [Row(1), Row(0), Row(5)], False
    )


def test_ceil_floor(session):
    double1 = TestData.double1(session)
    Utils.check_answer(double1.select(ceil(col("A"))), [Row(2), Row(3), Row(4)])
    Utils.check_answer(double1.select(floor(col("A"))), [Row(1), Row(2), Row(3)])
    # same as above, but pass str instead of Column
    Utils.check_answer(double1.select(ceil("A")), [Row(2), Row(3), Row(4)])
    Utils.check_answer(double1.select(floor("A")), [Row(1), Row(2), Row(3)])


def test_exp(session):
    Utils.check_answer(
        TestData.number2(session).select(exp(col("X")), exp(col("X"))),
        [
            Row(2.718281828459045, 2.718281828459045),
            Row(1.0, 1.0),
            Row(0.006737946999085467, 0.006737946999085467),
        ],
        sort=False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.number2(session).select(exp("X"), exp("X")),
        [
            Row(2.718281828459045, 2.718281828459045),
            Row(1.0, 1.0),
            Row(0.006737946999085467, 0.006737946999085467),
        ],
        sort=False,
    )


def test_log(session):
    Utils.check_answer(
        TestData.integer1(session).select(log(lit(2), col("A")), log(lit(4), col("A"))),
        [Row(0.0, 0.0), Row(1.0, 0.5), Row(1.5849625007211563, 0.7924812503605781)],
        sort=False,
    )
    # same as above, but pass int & str instead of Column
    Utils.check_answer(
        TestData.integer1(session).select(log(2, "A"), log(4, "A")),
        [Row(0.0, 0.0), Row(1.0, 0.5), Row(1.5849625007211563, 0.7924812503605781)],
        sort=False,
    )


def test_pow(session):
    Utils.check_answer(
        TestData.double2(session).select(pow(col("A"), col("B"))),
        [Row(0.31622776601683794), Row(0.3807307877431757), Row(0.4305116202499342)],
        sort=False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.double2(session).select(pow("A", "B")),
        [Row(0.31622776601683794), Row(0.3807307877431757), Row(0.4305116202499342)],
        sort=False,
    )


def test_builtin_function(session):
    repeat = builtin("repeat")
    string1 = TestData.string1(session)
    Utils.check_answer(
        string1.select(repeat(col("B"), 3)),
        [Row("aaa"), Row("bbb"), Row("ccc")],
        sort=False,
    )


def test_sub_string(session):
    Utils.check_answer(
        TestData.string1(session).select(substring(col("A"), lit(2), lit(4))),
        [Row("est1"), Row("est2"), Row("est3")],
        sort=False,
    )
    # same as above, but pass int & str instead of Column
    Utils.check_answer(
        TestData.string1(session).select(substring("A", 2, 4)),
        [Row("est1"), Row("est2"), Row("est3")],
        sort=False,
    )


def test_translate(session):
    Utils.check_answer(
        TestData.string3(session).select(translate(col("A"), lit("ab "), lit("XY"))),
        [Row("XYcYX"), Row("X12321X")],
        sort=False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.string3(session).select(translate("A", lit("ab "), lit("XY"))),
        [Row("XYcYX"), Row("X12321X")],
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


def test_split(session):
    assert (
        TestData.string5(session)
        .select(split(col("A"), lit(",")))
        .collect()[0][0]
        .replace(" ", "")
        .replace("\n", "")
        == '["1","2","3","4","5"]'
    )


def test_contains(session):
    Utils.check_answer(
        TestData.string4(session).select(contains(col("a"), lit("app"))),
        [Row(True), Row(False), Row(False)],
        sort=False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.string4(session).select(contains("a", lit("app"))),
        [Row(True), Row(False), Row(False)],
        sort=False,
    )


def test_startswith(session):
    Utils.check_answer(
        TestData.string4(session).select(startswith(col("a"), lit("ban"))),
        [Row(False), Row(True), Row(False)],
        sort=False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.string4(session).select(startswith("a", lit("ban"))),
        [Row(False), Row(True), Row(False)],
        sort=False,
    )


def test_char(session):
    df = session.createDataFrame([(84, 85), (96, 97)]).toDF("A", "B")

    Utils.check_answer(
        df.select(char(col("A")), char(col("B"))),
        [Row("T", "U"), Row("`", "a")],
        sort=False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        df.select(char("A"), char("B")), [Row("T", "U"), Row("`", "a")], sort=False
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


def test_parse_xml(session):
    null_xml1 = TestData.null_xml1(session)

    Utils.check_answer(
        null_xml1.select(parse_xml(col("v"))),
        [
            Row("<t1>\n  foo\n  <t2>bar</t2>\n  <t3></t3>\n</t1>"),
            Row("<t1></t1>"),
            Row(None),
            Row(None),
        ],
        sort=False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        null_xml1.select(parse_xml("v")),
        [
            Row("<t1>\n  foo\n  <t2>bar</t2>\n  <t3></t3>\n</t1>"),
            Row("<t1></t1>"),
            Row(None),
            Row(None),
        ],
        sort=False,
    )


def test_array_agg(session):
    assert (
        str(
            TestData.monthly_sales(session)
            .select(array_agg(col("amount")))
            .collect()[0][0]
        )
        == "[\n  10000,\n  400,\n  4500,\n  35000,\n  5000,\n  3000,\n  200,\n  90500,\n  6000,\n  "
        + "5000,\n  2500,\n  9500,\n  8000,\n  10000,\n  800,\n  4500\n]"
    )
    # same as above, but pass str instead of Column
    assert (
        str(TestData.monthly_sales(session).select(array_agg("amount")).collect()[0][0])
        == "[\n  10000,\n  400,\n  4500,\n  35000,\n  5000,\n  3000,\n  200,\n  90500,\n  6000,\n  "
        + "5000,\n  2500,\n  9500,\n  8000,\n  10000,\n  800,\n  4500\n]"
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


def test_to_json(session):
    Utils.check_answer(
        TestData.integer1(session).select(to_json(col("a"))),
        [Row("1"), Row("2"), Row("3")],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(to_json(col("time1"))),
        [Row('"20:57:01"')],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.integer1(session).select(to_json("a")),
        [Row("1"), Row("2"), Row("3")],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(to_json("time1")),
        [Row('"20:57:01"')],
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


def test_to_xml(session):
    Utils.check_answer(
        TestData.integer1(session).select(to_xml(col("a"))),
        [
            Row('<SnowflakeData type="INTEGER">1</SnowflakeData>'),
            Row('<SnowflakeData type="INTEGER">2</SnowflakeData>'),
            Row('<SnowflakeData type="INTEGER">3</SnowflakeData>'),
        ],
        sort=False,
    )
    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.integer1(session).select(to_xml("a")),
        [
            Row('<SnowflakeData type="INTEGER">1</SnowflakeData>'),
            Row('<SnowflakeData type="INTEGER">2</SnowflakeData>'),
            Row('<SnowflakeData type="INTEGER">3</SnowflakeData>'),
        ],
        sort=False,
    )
