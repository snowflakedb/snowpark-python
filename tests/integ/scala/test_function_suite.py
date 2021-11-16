#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from datetime import date, datetime, time
from decimal import Decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import (
    abs,
    array_agg,
    array_append,
    array_cat,
    array_compact,
    array_construct,
    array_construct_compact,
    array_contains,
    array_insert,
    array_intersection,
    array_position,
    array_prepend,
    array_size,
    array_slice,
    array_to_string,
    arrays_overlap,
    as_array,
    as_binary,
    as_char,
    as_date,
    as_decimal,
    as_double,
    as_integer,
    as_number,
    as_object,
    as_real,
    as_time,
    as_timestamp_ltz,
    as_timestamp_ntz,
    as_timestamp_tz,
    as_varchar,
    avg,
    builtin,
    ceil,
    char,
    check_json,
    check_xml,
    coalesce,
    col,
    contains,
    count,
    count_distinct,
    dateadd,
    datediff,
    equal_nan,
    exp,
    floor,
    get,
    get_ignore_case,
    get_path,
    is_array,
    is_binary,
    is_boolean,
    is_char,
    is_date,
    is_date_value,
    is_decimal,
    is_double,
    is_integer,
    is_null,
    is_null_value,
    is_object,
    is_real,
    is_time,
    is_timestamp_ltz,
    is_timestamp_ntz,
    is_timestamp_tz,
    is_varchar,
    json_extract_path_text,
    kurtosis,
    lit,
    log,
    max,
    mean,
    min,
    negate,
    not_,
    object_agg,
    object_construct,
    object_construct_keep_null,
    object_delete,
    object_insert,
    object_keys,
    object_pick,
    parse_json,
    parse_xml,
    pow,
    random,
    skew,
    split,
    sql_expr,
    sqrt,
    startswith,
    stddev,
    stddev_pop,
    stddev_samp,
    strip_null_value,
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
    typeof,
    var_pop,
    var_samp,
    variance,
    xmlget,
)
from tests.utils import TestData, Utils


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


def test_datediff(session):
    Utils.check_answer(
        [Row(1), Row(1)],
        TestData.timestamp1(session)
        .select(col("a"), dateadd("year", lit(1), col("a")).as_("b"))
        .select(datediff("year", col("a"), col("b"))),
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        [Row(1), Row(1)],
        TestData.timestamp1(session)
        .select("a", dateadd("year", lit(1), "a").as_("b"))
        .select(datediff("year", "a", "b")),
    )


def test_dateadd(session):
    Utils.check_answer(
        [Row(date(2021, 8, 1)), Row(date(2011, 12, 1))],
        TestData.date1(session).select(dateadd("year", lit(1), col("a"))),
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        [Row(date(2021, 8, 1)), Row(date(2011, 12, 1))],
        TestData.date1(session).select(dateadd("year", lit(1), "a")),
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


def test_arrays_overlap(session):
    Utils.check_answer(
        TestData.array1(session).select(arrays_overlap(col("ARR1"), col("ARR2"))),
        [Row(True), Row(False)],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array1(session).select(arrays_overlap("ARR1", "ARR2")),
        [Row(True), Row(False)],
        sort=False,
    )


def test_array_intersection(session):
    Utils.check_answer(
        TestData.array1(session).select(array_intersection(col("ARR1"), col("ARR2"))),
        [Row("[\n  3\n]"), Row("[]")],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array1(session).select(array_intersection("ARR1", "ARR2")),
        [Row("[\n  3\n]"), Row("[]")],
        sort=False,
    )


def test_is_array(session):
    Utils.check_answer(
        TestData.array1(session).select(is_array(col("ARR1"))),
        [Row(True), Row(True)],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            is_array(col("arr1")), is_array(col("bool1")), is_array(col("str1"))
        ),
        [Row(True, False, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array1(session).select(is_array("ARR1")),
        [Row(True), Row(True)],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            is_array("arr1"), is_array("bool1"), is_array("str1")
        ),
        [Row(True, False, False)],
        sort=False,
    )


def test_is_boolean(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_boolean(col("arr1")), is_boolean(col("bool1")), is_boolean(col("str1"))
        ),
        [Row(False, True, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_boolean("arr1"), is_boolean("bool1"), is_boolean("str1")
        ),
        [Row(False, True, False)],
        sort=False,
    )


def test_is_binary(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_binary(col("bin1")), is_binary(col("bool1")), is_binary(col("str1"))
        ),
        [Row(True, False, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_binary("bin1"), is_binary("bool1"), is_binary("str1")
        ),
        [Row(True, False, False)],
        sort=False,
    )


def test_is_char_is_varchar(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_char(col("str1")), is_char(col("bin1")), is_char(col("bool1"))
        ),
        [Row(True, False, False)],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            is_varchar(col("str1")), is_varchar(col("bin1")), is_varchar(col("bool1"))
        ),
        [Row(True, False, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_char("str1"), is_char("bin1"), is_char("bool1")
        ),
        [Row(True, False, False)],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            is_varchar("str1"), is_varchar("bin1"), is_varchar("bool1")
        ),
        [Row(True, False, False)],
        sort=False,
    )


def test_is_date_is_date_value(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_date(col("date1")), is_date(col("time1")), is_date(col("bool1"))
        ),
        [Row(True, False, False)],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            is_date_value(col("date1")),
            is_date_value(col("time1")),
            is_date_value(col("str1")),
        ),
        [Row(True, False, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_date("date1"), is_date("time1"), is_date("bool1")
        ),
        [Row(True, False, False)],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            is_date_value("date1"),
            is_date_value("time1"),
            is_date_value("str1"),
        ),
        [Row(True, False, False)],
        sort=False,
    )


def test_is_decimal(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_decimal(col("decimal1")),
            is_decimal(col("double1")),
            is_decimal(col("num1")),
        ),
        [Row(True, False, True)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_decimal("decimal1"),
            is_decimal("double1"),
            is_decimal("num1"),
        ),
        [Row(True, False, True)],
        sort=False,
    )


def test_is_double_is_real(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_double(col("decimal1")),
            is_double(col("double1")),
            is_double(col("num1")),
            is_double(col("bool1")),
        ),
        [Row(True, True, True, False)],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            is_real(col("decimal1")),
            is_real(col("double1")),
            is_real(col("num1")),
            is_real(col("bool1")),
        ),
        [Row(True, True, True, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_double("decimal1"),
            is_double("double1"),
            is_double("num1"),
            is_double("bool1"),
        ),
        [Row(True, True, True, False)],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            is_real("decimal1"),
            is_real("double1"),
            is_real("num1"),
            is_real("bool1"),
        ),
        [Row(True, True, True, False)],
        sort=False,
    )


def test_is_integer(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_integer(col("decimal1")),
            is_integer(col("double1")),
            is_integer(col("num1")),
            is_integer(col("bool1")),
        ),
        [Row(False, False, True, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_integer("decimal1"),
            is_integer("double1"),
            is_integer("num1"),
            is_integer("bool1"),
        ),
        [Row(False, False, True, False)],
        sort=False,
    )


def test_is_null_value(session):
    Utils.check_answer(
        TestData.null_json1(session).select(is_null_value(sql_expr("v:a"))),
        [Row(True), Row(False), Row(None)],
        sort=False,
    )

    # Pass str instead of Column. We can't use the same query as above
    # since "v:a" is an expression and we represent them as columns
    # differently
    Utils.check_answer(
        TestData.variant1(session).select(is_null_value("str1")),
        [Row(False)],
        sort=False,
    )


def test_is_object(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_object(col("obj1")), is_object(col("arr1")), is_object(col("str1"))
        ),
        [Row(True, False, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_object("obj1"), is_object("arr1"), is_object("str1")
        ),
        [Row(True, False, False)],
        sort=False,
    )


def test_is_time(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_time(col("time1")), is_time(col("date1")), is_time(col("timestamp_tz1"))
        ),
        [Row(True, False, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_time("time1"), is_time("date1"), is_time("timestamp_tz1")
        ),
        [Row(True, False, False)],
        sort=False,
    )


def test_is_timestamp_all(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            is_timestamp_ntz(col("timestamp_ntz1")),
            is_timestamp_ntz(col("timestamp_tz1")),
            is_timestamp_ntz(col("timestamp_ltz1")),
        ),
        [Row(True, False, False)],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            is_timestamp_ltz(col("timestamp_ntz1")),
            is_timestamp_ltz(col("timestamp_tz1")),
            is_timestamp_ltz(col("timestamp_ltz1")),
        ),
        [Row(False, False, True)],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            is_timestamp_tz(col("timestamp_ntz1")),
            is_timestamp_tz(col("timestamp_tz1")),
            is_timestamp_tz(col("timestamp_ltz1")),
        ),
        [Row(False, True, False)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            is_timestamp_ntz("timestamp_ntz1"),
            is_timestamp_ntz("timestamp_tz1"),
            is_timestamp_ntz("timestamp_ltz1"),
        ),
        [Row(True, False, False)],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            is_timestamp_ltz("timestamp_ntz1"),
            is_timestamp_ltz("timestamp_tz1"),
            is_timestamp_ltz("timestamp_ltz1"),
        ),
        [Row(False, False, True)],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            is_timestamp_tz("timestamp_ntz1"),
            is_timestamp_tz("timestamp_tz1"),
            is_timestamp_tz("timestamp_ltz1"),
        ),
        [Row(False, True, False)],
        sort=False,
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


def test_check_json(session):
    Utils.check_answer(
        TestData.null_json1(session).select(check_json(col("v"))),
        [Row(None), Row(None), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.invalid_json1(session).select(check_json(col("v"))),
        [
            Row("incomplete object value, pos 11"),
            Row("missing colon, pos 7"),
            Row("unfinished string, pos 5"),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.null_json1(session).select(check_json("v")),
        [Row(None), Row(None), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.invalid_json1(session).select(check_json("v")),
        [
            Row("incomplete object value, pos 11"),
            Row("missing colon, pos 7"),
            Row("unfinished string, pos 5"),
        ],
        sort=False,
    )


def test_check_xml(session):
    Utils.check_answer(
        TestData.null_xml1(session).select(check_xml(col("v"))),
        [Row(None), Row(None), Row(None), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.invalid_xml1(session).select(check_xml(col("v"))),
        [
            Row("no opening tag for </t>, pos 8"),
            Row("missing closing tags: </t1></t1>, pos 8"),
            Row("bad character in XML tag name: '<', pos 4"),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.null_xml1(session).select(check_xml("v")),
        [Row(None), Row(None), Row(None), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.invalid_xml1(session).select(check_xml("v")),
        [
            Row("no opening tag for </t>, pos 8"),
            Row("missing closing tags: </t1></t1>, pos 8"),
            Row("bad character in XML tag name: '<', pos 4"),
        ],
        sort=False,
    )


def test_json_extract_path_text(session):
    Utils.check_answer(
        TestData.valid_json1(session).select(
            json_extract_path_text(col("v"), col("k"))
        ),
        [Row(None), Row("foo"), Row(None), Row(None)],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.valid_json1(session).select(json_extract_path_text("v", "k")),
        [Row(None), Row("foo"), Row(None), Row(None)],
        sort=False,
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


def test_strip_null_value(session):
    Utils.check_answer(
        TestData.null_json1(session).select(sql_expr("v:a")),
        [Row("null"), Row('"foo"'), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.null_json1(session).select(strip_null_value(sql_expr("v:a"))),
        [Row(None), Row('"foo"'), Row(None)],
        sort=False,
    )

    # This test needs columns to be passed and can't be replicated by passing strings


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


def test_array_append(session):
    Utils.check_answer(
        [
            Row('[\n  1,\n  2,\n  3,\n  "amount",\n  3.221000000000000e+01\n]'),
            Row('[\n  6,\n  7,\n  8,\n  "amount",\n  3.221000000000000e+01\n]'),
        ],
        TestData.array1(session).select(
            array_append(array_append(col("arr1"), lit("amount")), lit(32.21))
        ),
        sort=False,
    )

    # Get array result in List[Variant1]
    result_set = (
        TestData.array1(session)
        .select(array_append(array_append(col("arr1"), lit("amount")), lit(32.21)))
        .collect()
    )
    row1 = ["1", "2", "3", '"amount"', "3.221000000000000e+01"]
    assert [s.strip() for s in result_set[0][0][1:-1].split(",")] == row1
    row2 = ["6", "7", "8", '"amount"', "3.221000000000000e+01"]
    assert [s.strip() for s in result_set[1][0][1:-1].split(",")] == row2

    Utils.check_answer(
        [
            Row('[\n  1,\n  2,\n  3,\n  2,\n  "e1"\n]'),
            Row('[\n  6,\n  7,\n  8,\n  1,\n  "e2"\n]'),
        ],
        TestData.array2(session).select(
            array_append(array_append(col("arr1"), col("d")), col("e"))
        ),
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        [
            Row('[\n  1,\n  2,\n  3,\n  "amount",\n  3.221000000000000e+01\n]'),
            Row('[\n  6,\n  7,\n  8,\n  "amount",\n  3.221000000000000e+01\n]'),
        ],
        TestData.array1(session).select(
            array_append(array_append("arr1", lit("amount")), lit(32.21))
        ),
        sort=False,
    )

    # Get array result in List[Variant1]
    result_set = (
        TestData.array1(session)
        .select(array_append(array_append("arr1", lit("amount")), lit(32.21)))
        .collect()
    )
    row1 = ["1", "2", "3", '"amount"', "3.221000000000000e+01"]
    assert [s.strip() for s in result_set[0][0][1:-1].split(",")] == row1
    row2 = ["6", "7", "8", '"amount"', "3.221000000000000e+01"]
    assert [s.strip() for s in result_set[1][0][1:-1].split(",")] == row2

    Utils.check_answer(
        [
            Row('[\n  1,\n  2,\n  3,\n  2,\n  "e1"\n]'),
            Row('[\n  6,\n  7,\n  8,\n  1,\n  "e2"\n]'),
        ],
        TestData.array2(session).select(array_append(array_append("arr1", "d"), "e")),
    )


def test_array_cat(session):
    Utils.check_answer(
        TestData.array1(session).select(array_cat(col("arr1"), col("arr2"))),
        [
            Row("[\n  1,\n  2,\n  3,\n  3,\n  4,\n  5\n]"),
            Row("[\n  6,\n  7,\n  8,\n  9,\n  0,\n  1\n]"),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array1(session).select(array_cat("arr1", "arr2")),
        [
            Row("[\n  1,\n  2,\n  3,\n  3,\n  4,\n  5\n]"),
            Row("[\n  6,\n  7,\n  8,\n  9,\n  0,\n  1\n]"),
        ],
        sort=False,
    )


def test_array_compact(session):
    Utils.check_answer(
        TestData.null_array1(session).select(array_compact(col("arr1"))),
        [Row("[\n  1,\n  3\n]"), Row("[\n  6,\n  8\n]")],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.null_array1(session).select(array_compact("arr1")),
        [Row("[\n  1,\n  3\n]"), Row("[\n  6,\n  8\n]")],
        sort=False,
    )


def test_array_construct(session):
    assert (
        TestData.zero1(session)
        .select(array_construct(lit(1), lit(1.2), lit("string"), lit(""), lit(None)))
        .collect()[0][0]
        == '[\n  1,\n  1.200000000000000e+00,\n  "string",\n  "",\n  undefined\n]'
    )

    assert TestData.zero1(session).select(array_construct()).collect()[0][0] == "[]"

    Utils.check_answer(
        TestData.integer1(session).select(
            array_construct(col("a"), lit(1.2), lit(None))
        ),
        [
            Row("[\n  1,\n  1.200000000000000e+00,\n  undefined\n]"),
            Row("[\n  2,\n  1.200000000000000e+00,\n  undefined\n]"),
            Row("[\n  3,\n  1.200000000000000e+00,\n  undefined\n]"),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.integer1(session).select(array_construct("a", lit(1.2), lit(None))),
        [
            Row("[\n  1,\n  1.200000000000000e+00,\n  undefined\n]"),
            Row("[\n  2,\n  1.200000000000000e+00,\n  undefined\n]"),
            Row("[\n  3,\n  1.200000000000000e+00,\n  undefined\n]"),
        ],
        sort=False,
    )


def test_array_construct_compact(session):
    assert (
        TestData.zero1(session)
        .select(
            array_construct_compact(lit(1), lit(1.2), lit("string"), lit(""), lit(None))
        )
        .collect()[0][0]
        == '[\n  1,\n  1.200000000000000e+00,\n  "string",\n  ""\n]'
    )

    assert (
        TestData.zero1(session).select(array_construct_compact()).collect()[0][0]
        == "[]"
    )

    Utils.check_answer(
        TestData.integer1(session).select(
            array_construct_compact(col("a"), lit(1.2), lit(None))
        ),
        [
            Row("[\n  1,\n  1.200000000000000e+00\n]"),
            Row("[\n  2,\n  1.200000000000000e+00\n]"),
            Row("[\n  3,\n  1.200000000000000e+00\n]"),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.integer1(session).select(
            array_construct_compact("a", lit(1.2), lit(None))
        ),
        [
            Row("[\n  1,\n  1.200000000000000e+00\n]"),
            Row("[\n  2,\n  1.200000000000000e+00\n]"),
            Row("[\n  3,\n  1.200000000000000e+00\n]"),
        ],
        sort=False,
    )


def test_array_contains(session):
    assert (
        TestData.zero1(session)
        .select(
            array_contains(lit(1), array_construct(lit(1), lit(1.2), lit("string")))
        )
        .collect()[0][0]
    )

    assert (
        not TestData.zero1(session)
        .select(
            array_contains(lit(-1), array_construct(lit(1), lit(1.2), lit("string")))
        )
        .collect()[0][0]
    )

    Utils.check_answer(
        TestData.integer1(session).select(
            array_contains(col("a"), array_construct(lit(1), lit(1.2), lit("string")))
        ),
        [Row(True), Row(False), Row(False)],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.integer1(session).select(
            array_contains("a", array_construct(lit(1), lit(1.2), lit("string")))
        ),
        [Row(True), Row(False), Row(False)],
        sort=False,
    )


def test_array_insert(session):
    Utils.check_answer(
        TestData.array2(session).select(array_insert(col("arr1"), col("d"), col("e"))),
        [Row('[\n  1,\n  2,\n  "e1",\n  3\n]'), Row('[\n  6,\n  "e2",\n  7,\n  8\n]')],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array2(session).select(array_insert("arr1", "d", "e")),
        [Row('[\n  1,\n  2,\n  "e1",\n  3\n]'), Row('[\n  6,\n  "e2",\n  7,\n  8\n]')],
        sort=False,
    )


def test_array_position(session):
    Utils.check_answer(
        TestData.array2(session).select(array_position(col("d"), col("arr1"))),
        [Row(1), Row(None)],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array2(session).select(array_position("d", "arr1")),
        [Row(1), Row(None)],
        sort=False,
    )


def test_array_prepend(session):
    Utils.check_answer(
        TestData.array1(session).select(
            array_prepend(array_prepend(col("arr1"), lit("amount")), lit(32.21))
        ),
        [
            Row('[\n  3.221000000000000e+01,\n  "amount",\n  1,\n  2,\n  3\n]'),
            Row('[\n  3.221000000000000e+01,\n  "amount",\n  6,\n  7,\n  8\n]'),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.array2(session).select(
            array_prepend(array_prepend(col("arr1"), col("d")), col("e"))
        ),
        [
            Row('[\n  "e1",\n  2,\n  1,\n  2,\n  3\n]'),
            Row('[\n  "e2",\n  1,\n  6,\n  7,\n  8\n]'),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array1(session).select(
            array_prepend(array_prepend("arr1", lit("amount")), lit(32.21))
        ),
        [
            Row('[\n  3.221000000000000e+01,\n  "amount",\n  1,\n  2,\n  3\n]'),
            Row('[\n  3.221000000000000e+01,\n  "amount",\n  6,\n  7,\n  8\n]'),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.array2(session).select(array_prepend(array_prepend("arr1", "d"), "e")),
        [
            Row('[\n  "e1",\n  2,\n  1,\n  2,\n  3\n]'),
            Row('[\n  "e2",\n  1,\n  6,\n  7,\n  8\n]'),
        ],
        sort=False,
    )


def test_array_size(session):
    Utils.check_answer(
        TestData.array2(session).select(array_size(col("arr1"))),
        [Row(3), Row(3)],
        sort=False,
    )

    Utils.check_answer(
        TestData.array2(session).select(array_size(col("d"))),
        [Row(None), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.array2(session).select(array_size(parse_json(col("f")))),
        [Row(1), Row(2)],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array2(session).select(array_size("arr1")),
        [Row(3), Row(3)],
        sort=False,
    )

    Utils.check_answer(
        TestData.array2(session).select(array_size("d")),
        [Row(None), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.array2(session).select(array_size(parse_json("f"))),
        [Row(1), Row(2)],
        sort=False,
    )


def test_array_slice(session):
    Utils.check_answer(
        TestData.array3(session).select(array_slice(col("arr1"), col("d"), col("e"))),
        [Row("[\n  2\n]"), Row("[\n  5\n]"), Row("[\n  6,\n  7\n]")],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array3(session).select(array_slice("arr1", "d", "e")),
        [Row("[\n  2\n]"), Row("[\n  5\n]"), Row("[\n  6,\n  7\n]")],
        sort=False,
    )


def test_array_to_string(session):
    Utils.check_answer(
        TestData.array3(session).select(array_to_string(col("arr1"), col("f"))),
        [Row("1,2,3"), Row("4, 5, 6"), Row("6;7;8")],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array3(session).select(array_to_string("arr1", "f")),
        [Row("1,2,3"), Row("4, 5, 6"), Row("6;7;8")],
        sort=False,
    )


def test_objectagg(session):
    Utils.check_answer(
        TestData.object1(session).select(object_agg(col("key"), col("value"))),
        [Row('{\n  "age": 21,\n  "zip": 94401\n}')],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.object1(session).select(object_agg("key", "value")),
        [Row('{\n  "age": 21,\n  "zip": 94401\n}')],
        sort=False,
    )


def test_object_construct(session):
    Utils.check_answer(
        TestData.object1(session).select(object_construct(col("key"), col("value"))),
        [Row('{\n  "age": 21\n}'), Row('{\n  "zip": 94401\n}')],
        sort=False,
    )

    Utils.check_answer(
        TestData.object1(session).select(object_construct()),
        [Row("{}"), Row("{}")],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.object1(session).select(object_construct("key", "value")),
        [Row('{\n  "age": 21\n}'), Row('{\n  "zip": 94401\n}')],
        sort=False,
    )


def test_object_construct_keep_null(session):
    Utils.check_answer(
        TestData.object3(session).select(
            object_construct_keep_null(col("key"), col("value"))
        ),
        [Row("{}"), Row('{\n  "zip": null\n}')],
        sort=False,
    )

    Utils.check_answer(
        TestData.object1(session).select(object_construct_keep_null()),
        [Row("{}"), Row("{}")],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.object3(session).select(object_construct_keep_null("key", "value")),
        [Row("{}"), Row('{\n  "zip": null\n}')],
        sort=False,
    )


def test_object_delete(session):
    Utils.check_answer(
        TestData.object2(session).select(
            object_delete(col("obj"), col("k"), lit("name"), lit("non-exist-key"))
        ),
        [Row('{\n  "zip": 21021\n}'), Row('{\n  "age": 26,\n  "zip": 94021\n}')],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.object2(session).select(
            object_delete("obj", "k", lit("name"), lit("non-exist-key"))
        ),
        [Row('{\n  "zip": 21021\n}'), Row('{\n  "age": 26,\n  "zip": 94021\n}')],
        sort=False,
    )


def test_object_insert(session):
    Utils.check_answer(
        TestData.object2(session).select(
            object_insert(col("obj"), lit("key"), lit("v"))
        ),
        [
            Row('{\n  "age": 21,\n  "key": "v",\n  "name": "Joe",\n  "zip": 21021\n}'),
            Row('{\n  "age": 26,\n  "key": "v",\n  "name": "Jay",\n  "zip": 94021\n}'),
        ],
        sort=False,
    )

    resultSet = (
        TestData.object2(session)
        .select(object_insert(col("obj"), lit("key"), lit("v")))
        .collect()
    )
    row1 = {'"age"': "21", '"key"': '"v"', '"name"': '"Joe"', '"zip"': "21021"}
    assert (
        dict(
            [
                list(map(str.strip, rs.split(":")))
                for rs in resultSet[0][0][1:-1].split(",")
            ]
        )
        == row1
    )
    row2 = {'"age"': "26", '"key"': '"v"', '"name"': '"Jay"', '"zip"': "94021"}
    assert (
        dict(
            [
                list(map(str.strip, rs.split(":")))
                for rs in resultSet[1][0][1:-1].split(",")
            ]
        )
        == row2
    )

    Utils.check_answer(
        TestData.object2(session).select(
            object_insert(col("obj"), col("k"), col("v"), col("flag"))
        ),
        [
            Row('{\n  "age": 0,\n  "name": "Joe",\n  "zip": 21021\n}'),
            Row('{\n  "age": 26,\n  "key": 0,\n  "name": "Jay",\n  "zip": 94021\n}'),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.object2(session).select(object_insert("obj", lit("key"), lit("v"))),
        [
            Row('{\n  "age": 21,\n  "key": "v",\n  "name": "Joe",\n  "zip": 21021\n}'),
            Row('{\n  "age": 26,\n  "key": "v",\n  "name": "Jay",\n  "zip": 94021\n}'),
        ],
        sort=False,
    )

    resultSet = (
        TestData.object2(session)
        .select(object_insert("obj", lit("key"), lit("v")))
        .collect()
    )
    row1 = {'"age"': "21", '"key"': '"v"', '"name"': '"Joe"', '"zip"': "21021"}
    assert (
        dict(
            [
                list(map(str.strip, rs.split(":")))
                for rs in resultSet[0][0][1:-1].split(",")
            ]
        )
        == row1
    )
    row2 = {'"age"': "26", '"key"': '"v"', '"name"': '"Jay"', '"zip"': "94021"}
    assert (
        dict(
            [
                list(map(str.strip, rs.split(":")))
                for rs in resultSet[1][0][1:-1].split(",")
            ]
        )
        == row2
    )

    Utils.check_answer(
        TestData.object2(session).select(object_insert("obj", "k", "v", "flag")),
        [
            Row('{\n  "age": 0,\n  "name": "Joe",\n  "zip": 21021\n}'),
            Row('{\n  "age": 26,\n  "key": 0,\n  "name": "Jay",\n  "zip": 94021\n}'),
        ],
        sort=False,
    )


def test_object_pick(session):
    Utils.check_answer(
        TestData.object2(session).select(
            object_pick(col("obj"), col("k"), lit("name"), lit("non-exist-key"))
        ),
        [Row('{\n  "age": 21,\n  "name": "Joe"\n}'), Row('{\n  "name": "Jay"\n}')],
        sort=False,
    )

    Utils.check_answer(
        TestData.object2(session).select(
            object_pick(col("obj"), array_construct(lit("name"), lit("zip")))
        ),
        [
            Row('{\n  "name": "Joe",\n  "zip": 21021\n}'),
            Row('{\n  "name": "Jay",\n  "zip": 94021\n}'),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.object2(session).select(
            object_pick("obj", "k", lit("name"), lit("non-exist-key"))
        ),
        [Row('{\n  "age": 21,\n  "name": "Joe"\n}'), Row('{\n  "name": "Jay"\n}')],
        sort=False,
    )

    Utils.check_answer(
        TestData.object2(session).select(
            object_pick("obj", array_construct(lit("name"), lit("zip")))
        ),
        [
            Row('{\n  "name": "Joe",\n  "zip": 21021\n}'),
            Row('{\n  "name": "Jay",\n  "zip": 94021\n}'),
        ],
        sort=False,
    )


def test_as_array(session):
    Utils.check_answer(
        TestData.array1(session).select(as_array(col("ARR1"))),
        [Row("[\n  1,\n  2,\n  3\n]"), Row("[\n  6,\n  7,\n  8\n]")],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            as_array(col("arr1")), as_array(col("bool1")), as_array(col("str1"))
        ),
        [
            Row('[\n  "Example"\n]', None, None),
        ],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.array1(session).select(as_array("ARR1")),
        [Row("[\n  1,\n  2,\n  3\n]"), Row("[\n  6,\n  7,\n  8\n]")],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            as_array("arr1"), as_array("bool1"), as_array("str1")
        ),
        [
            Row('[\n  "Example"\n]', None, None),
        ],
        sort=False,
    )


def test_as_binary(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_binary(col("bin1")), as_binary(col("bool1")), as_binary(col("str1"))
        ),
        [
            Row(bytearray([115, 110, 111, 119]), None, None),
        ],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_binary("bin1"), as_binary("bool1"), as_binary("str1")
        ),
        [
            Row(bytearray([115, 110, 111, 119]), None, None),
        ],
        sort=False,
    )


def test_as_char_as_varchar(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_char(col("str1")), as_char(col("bin1")), as_char(col("bool1"))
        ),
        [Row("X", None, None)],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            as_varchar(col("str1")), as_varchar(col("bin1")), as_varchar(col("bool1"))
        ),
        [Row("X", None, None)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_char("str1"), as_char("bin1"), as_char("bool1")
        ),
        [Row("X", None, None)],
        sort=False,
    )
    Utils.check_answer(
        TestData.variant1(session).select(
            as_varchar("str1"), as_varchar("bin1"), as_varchar("bool1")
        ),
        [Row("X", None, None)],
        sort=False,
    )


def test_as_date(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_date(col("date1")), as_date(col("time1")), as_date(col("bool1"))
        ),
        [Row(date(2017, 2, 24), None, None)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_date("date1"), as_date("time1"), as_date("bool1")
        ),
        [Row(date(2017, 2, 24), None, None)],
        sort=False,
    )


def test_as_decimal_as_number(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_decimal(col("decimal1")),
            as_decimal(col("double1")),
            as_decimal(col("num1")),
        ),
        [
            Row(1, None, 15),
        ],
        sort=False,
    )

    assert (
        TestData.variant1(session)
        .select(as_decimal(col("decimal1"), 6))
        .collect()[0][0]
        == 1
    )

    assert (
        float(
            TestData.variant1(session)
            .select(as_decimal(col("decimal1"), 6, 3))
            .collect()[0][0]
        )
        == 1.23
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            as_number(col("decimal1")),
            as_number(col("double1")),
            as_number(col("num1")),
        ),
        [
            Row(1, None, 15),
        ],
        sort=False,
    )

    assert (
        TestData.variant1(session).select(as_number(col("decimal1"), 6)).collect()[0][0]
        == 1
    )

    assert (
        float(
            TestData.variant1(session)
            .select(as_number(col("decimal1"), 6, 3))
            .collect()[0][0]
        )
        == 1.23
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_decimal("decimal1"),
            as_decimal("double1"),
            as_decimal("num1"),
        ),
        [
            Row(1, None, 15),
        ],
        sort=False,
    )

    assert (
        TestData.variant1(session).select(as_decimal("decimal1", 6)).collect()[0][0]
        == 1
    )

    assert (
        float(
            TestData.variant1(session)
            .select(as_decimal("decimal1", 6, 3))
            .collect()[0][0]
        )
        == 1.23
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            as_number("decimal1"),
            as_number("double1"),
            as_number("num1"),
        ),
        [
            Row(1, None, 15),
        ],
        sort=False,
    )

    assert (
        TestData.variant1(session).select(as_number("decimal1", 6)).collect()[0][0] == 1
    )

    assert (
        float(
            TestData.variant1(session)
            .select(as_number("decimal1", 6, 3))
            .collect()[0][0]
        )
        == 1.23
    )


def test_as_double_as_real(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_double(col("decimal1")),
            as_double(col("double1")),
            as_double(col("num1")),
            as_double(col("bool1")),
        ),
        [Row(1.23, 3.21, 15.0, None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            as_real(col("decimal1")),
            as_real(col("double1")),
            as_real(col("num1")),
            as_real(col("bool1")),
        ),
        [Row(1.23, 3.21, 15.0, None)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_double("decimal1"),
            as_double("double1"),
            as_double("num1"),
            as_double("bool1"),
        ),
        [Row(1.23, 3.21, 15.0, None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            as_real("decimal1"),
            as_real("double1"),
            as_real("num1"),
            as_real("bool1"),
        ),
        [Row(1.23, 3.21, 15.0, None)],
        sort=False,
    )


def test_as_integer(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_integer(col("decimal1")),
            as_integer(col("double1")),
            as_integer(col("num1")),
            as_integer(col("bool1")),
        ),
        [Row(1, None, 15, None)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_integer("decimal1"),
            as_integer("double1"),
            as_integer("num1"),
            as_integer("bool1"),
        ),
        [Row(1, None, 15, None)],
        sort=False,
    )


def test_as_object(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_object(col("obj1")), as_object(col("arr1")), as_object(col("str1"))
        ),
        [Row('{\n  "Tree": "Pine"\n}', None, None)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_object("obj1"), as_object("arr1"), as_object("str1")
        ),
        [Row('{\n  "Tree": "Pine"\n}', None, None)],
        sort=False,
    )


def test_as_time(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_time(col("time1")), as_time(col("date1")), as_time(col("timestamp_tz1"))
        ),
        [Row(time(20, 57, 1, 123456), None, None)],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_time("time1"), as_time("date1"), as_time("timestamp_tz1")
        ),
        [Row(time(20, 57, 1, 123456), None, None)],
        sort=False,
    )


def test_as_timestamp_all(session):
    Utils.check_answer(
        TestData.variant1(session).select(
            as_timestamp_ntz(col("timestamp_ntz1")),
            as_timestamp_ntz(col("timestamp_tz1")),
            as_timestamp_ntz(col("timestamp_ltz1")),
        ),
        [
            Row(
                datetime.strptime("2017-02-24 12:00:00.456", "%Y-%m-%d %H:%M:%S.%f"),
                None,
                None,
            ),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            as_timestamp_ltz(col("timestamp_ntz1")),
            as_timestamp_ltz(col("timestamp_tz1")),
            as_timestamp_ltz(col("timestamp_ltz1")),
        ),
        [
            Row(
                None,
                None,
                datetime.strptime(
                    "2017-02-24 12:00:00.123", "%Y-%m-%d %H:%M:%S.%f"
                ).astimezone(),
            ),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            as_timestamp_tz(col("timestamp_ntz1")),
            as_timestamp_tz(col("timestamp_tz1")),
            as_timestamp_tz(col("timestamp_ltz1")),
        ),
        [
            Row(
                None,
                datetime.strptime(
                    "2017-02-24 13:00:00.123 +0100", "%Y-%m-%d %H:%M:%S.%f %z"
                ),
                None,
            ),
        ],
        sort=False,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.variant1(session).select(
            as_timestamp_ntz("timestamp_ntz1"),
            as_timestamp_ntz("timestamp_tz1"),
            as_timestamp_ntz("timestamp_ltz1"),
        ),
        [
            Row(
                datetime.strptime("2017-02-24 12:00:00.456", "%Y-%m-%d %H:%M:%S.%f"),
                None,
                None,
            ),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            as_timestamp_ltz("timestamp_ntz1"),
            as_timestamp_ltz("timestamp_tz1"),
            as_timestamp_ltz("timestamp_ltz1"),
        ),
        [
            Row(
                None,
                None,
                datetime.strptime(
                    "2017-02-24 12:00:00.123", "%Y-%m-%d %H:%M:%S.%f"
                ).astimezone(),
            ),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.variant1(session).select(
            as_timestamp_tz("timestamp_ntz1"),
            as_timestamp_tz("timestamp_tz1"),
            as_timestamp_tz("timestamp_ltz1"),
        ),
        [
            Row(
                None,
                datetime.strptime(
                    "2017-02-24 13:00:00.123 +0100", "%Y-%m-%d %H:%M:%S.%f %z"
                ),
                None,
            ),
        ],
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


@pytest.mark.parametrize("a", ["a", col("a")])
def test_typeof(session, a):
    Utils.check_answer(
        TestData.integer1(session).select(typeof(a)),
        [Row("INTEGER")] * 3,
        sort=False,
    )


@pytest.mark.parametrize("obj, k", [("obj", "k"), (col("obj"), col("k"))])
def test_get_ignore_case(session, obj, k):
    Utils.check_answer(
        TestData.object2(session).select(get_ignore_case(obj, k)),
        [Row("21"), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.object2(session).select(get_ignore_case(obj, lit("AGE"))),
        [Row("21"), Row("26")],
        sort=False,
    )


@pytest.mark.parametrize("column", ["obj", col("obj")])
def test_object_keys(session, column):
    Utils.check_answer(
        TestData.object2(session).select(object_keys(column)),
        [
            Row('[\n  "age",\n  "name",\n  "zip"\n]'),
            Row('[\n  "age",\n  "name",\n  "zip"\n]'),
        ],
        sort=False,
    )


@pytest.mark.parametrize(
    "v, t2, t3, instance",
    [("v", "t2", "t3", "instance"), (col("v"), col("t2"), col("t3"), col("instance"))],
)
def test_xmlget(session, v, t2, t3, instance):
    Utils.check_answer(
        TestData.valid_xml1(session).select(get_ignore_case(xmlget(v, t2), lit("$"))),
        [Row('"bar"'), Row(None), Row('"foo"')],
        sort=False,
    )

    # Scala assert getVariant() here. snowpark-python doesn't have class Variant so skip it.

    Utils.check_answer(
        TestData.valid_xml1(session).select(
            get_ignore_case(xmlget(v, t3, lit("0")), lit("@"))
        ),
        [Row('"t3"'), Row(None), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        TestData.valid_xml1(session).select(
            get_ignore_case(xmlget(col("v"), t2, instance), lit("$"))
        ),
        [Row('"bar"'), Row(None), Row('"bar"')],
        sort=False,
    )


@pytest.mark.parametrize("v, k", [("v", "k"), (col("v"), col("k"))])
def test_get_path(session, v, k):
    Utils.check_answer(
        TestData.valid_json1(session).select(get_path(v, k)),
        [Row("null"), Row('"foo"'), Row(None), Row(None)],
        sort=False,
    )


def test_get(session):
    Utils.check_answer(
        [Row("21"), Row(None)],
        TestData.object2(session).select(get(col("obj"), col("k"))),
        sort=False,
    )
    Utils.check_answer(
        [Row(None), Row(None)],
        TestData.object2(session).select(get(col("obj"), lit("AGE"))),
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        [Row("21"), Row(None)],
        TestData.object2(session).select(get("obj", "k")),
        sort=False,
    )

    Utils.check_answer(
        [Row(None), Row(None)],
        TestData.object2(session).select(get("obj", lit("AGE"))),
        sort=False,
    )
