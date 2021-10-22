#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from datetime import date, datetime, time
from decimal import Decimal
from test.utils import TestData, Utils

from snowflake.snowpark import Row
from snowflake.snowpark._internal.sp_expressions import UnresolvedAttribute
from snowflake.snowpark.functions import (
    abs,
    array_agg,
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
    equal_nan,
    exp,
    floor,
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
