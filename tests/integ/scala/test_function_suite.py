#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import json
from contextlib import contextmanager
from datetime import date, datetime, time
from decimal import Decimal
from functools import partial

import pytest
import pytz

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    _columns_from_timestamp_parts,
    _timestamp_from_parts_internal,
    abs,
    acos,
    approx_count_distinct,
    approx_percentile,
    approx_percentile_accumulate,
    approx_percentile_combine,
    approx_percentile_estimate,
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
    ascii,
    asin,
    atan,
    atan2,
    avg,
    builtin,
    ceil,
    char,
    charindex,
    check_json,
    check_xml,
    coalesce,
    col,
    collate,
    collation,
    contains,
    convert_timezone,
    corr,
    cos,
    cosh,
    count,
    count_distinct,
    covar_pop,
    covar_samp,
    cume_dist,
    date_part,
    date_trunc,
    dateadd,
    datediff,
    degrees,
    dense_rank,
    div0,
    endswith,
    equal_nan,
    exp,
    factorial,
    first_value,
    floor,
    get,
    get_ignore_case,
    get_path,
    iff,
    initcap,
    insert,
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
    lag,
    last_value,
    lead,
    left,
    length,
    listagg,
    lit,
    log,
    lower,
    lpad,
    ltrim,
    max,
    md5,
    mean,
    min,
    mode,
    negate,
    not_,
    ntile,
    object_agg,
    object_construct,
    object_construct_keep_null,
    object_delete,
    object_insert,
    object_keys,
    object_pick,
    parse_json,
    parse_xml,
    percent_rank,
    pow,
    radians,
    random,
    rank,
    regexp_count,
    repeat,
    replace,
    right,
    round,
    row_number,
    rpad,
    rtrim,
    sha1,
    sha2,
    sin,
    sinh,
    skew,
    soundex,
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
    tan,
    tanh,
    time_from_parts,
    timestamp_tz_from_parts,
    to_array,
    to_date,
    to_double,
    to_geography,
    to_geometry,
    to_json,
    to_object,
    to_timestamp,
    to_timestamp_ltz,
    to_timestamp_ntz,
    to_timestamp_tz,
    to_variant,
    to_xml,
    translate,
    trim,
    trunc,
    typeof,
    upper,
    var_pop,
    var_samp,
    variance,
    xmlget,
)
from snowflake.snowpark.mock._functions import LocalTimezone
from snowflake.snowpark.types import (
    DateType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
)
from snowflake.snowpark.window import Window
from tests.utils import IS_IN_STORED_PROC, TestData, Utils


@contextmanager
def parameter_override(session, parameter, value, enabled=True):
    """
    Context manager that overrides a session parameter when enabled.
    """
    try:
        if enabled:
            quoted = f'"{value}"' if isinstance(value, str) else value
            session.sql(f"alter session set {parameter}={quoted}").collect()
        yield
    finally:
        if enabled:
            session.sql(f"alter session unset {parameter}").collect()


@pytest.mark.localtest
def test_col(session):
    test_data1 = TestData.test_data1(session)
    Utils.check_answer(test_data1.select(col("bool")), [Row(True), Row(False)])
    Utils.check_answer(test_data1.select(col("num")), [Row(1), Row(2)])

    # same as above, but pass str instead of Column
    Utils.check_answer(test_data1.select(col("bool")), [Row(True), Row(False)])
    Utils.check_answer(test_data1.select(col("num")), [Row(1), Row(2)])


@pytest.mark.localtest
def test_lit(session):
    res = TestData.test_data1(session).select(lit(1)).collect()
    assert res == [Row(1), Row(1)]


def test_avg(session):
    res = TestData.duplicated_numbers(session).select(avg(col("A"))).collect()
    assert res == [Row(Decimal("2.2"))]

    # same as above, but pass str instead of Column
    res = TestData.duplicated_numbers(session).select(avg("A")).collect()
    assert res == [Row(Decimal("2.2"))]


@pytest.mark.parametrize(
    "k, v1, v2", [("K", "V1", "V2"), (col("K"), col("V1"), col("V2"))]
)
def test_corr(session, k, v1, v2):
    Utils.check_answer(
        TestData.number1(session).group_by(k).agg(corr(v1, v2)),
        [Row(1, None), Row(2, 0.40367115665231024)],
    )


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


@pytest.mark.parametrize(
    "k, v1, v2", [("K", "V1", "V2"), (col("K"), col("V1"), col("V2"))]
)
def test_covariance(session, k, v1, v2):
    Utils.check_answer(
        TestData.number1(session).group_by(k).agg(covar_pop(v1, v2)),
        [Row(1, 0.0), Row(2, 38.75)],
    )

    Utils.check_answer(
        TestData.number1(session).group_by("K").agg(covar_samp("V1", col("V2"))),
        [Row(1, None), Row(2, 51.666666666666664)],
    )


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


def test_mode(session):
    xyz = TestData.xyz(session)
    df_col = xyz.select(mode(col("X")), mode(col("Y")), mode(col("Z"))).collect()
    # Z column has two most frequent values 1 and 3, and either could be returned
    try:
        Utils.check_answer(df_col, Row(2, 2, 1))
    except AssertionError:
        Utils.check_answer(df_col, Row(2, 2, 3))

    # same as above, but pass str instead of Column
    df_str = xyz.select(mode("X"), mode("Y"), mode("Z")).collect()
    try:
        Utils.check_answer(df_str, Row(2, 2, 1))
    except AssertionError:
        Utils.check_answer(df_str, Row(2, 2, 3))


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
    df = TestData.duplicated_numbers(session).group_by("A").agg(sum(col("A")))
    assert df.collect() == [Row(3, 6), Row(2, 4), Row(1, 1)]

    df = TestData.duplicated_numbers(session).group_by("A").agg(sum_distinct(col("A")))
    assert df.collect() == [Row(3, 3), Row(2, 2), Row(1, 1)]

    # same as above, but pass str instead of Column
    df = TestData.duplicated_numbers(session).group_by("A").agg(sum("A"))
    assert df.collect() == [Row(3, 6), Row(2, 4), Row(1, 1)]

    df = TestData.duplicated_numbers(session).group_by("A").agg(sum_distinct("A"))
    assert df.collect() == [Row(3, 3), Row(2, 2), Row(1, 1)]


def test_variance(session):
    df = (
        TestData.xyz(session)
        .group_by("X")
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
        .group_by("X")
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


@pytest.mark.localtest
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


@pytest.mark.localtest
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


@pytest.mark.localtest
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


def test_datediff_negative(session):
    with pytest.raises(ValueError, match="part must be a string"):
        TestData.timestamp1(session).select(dateadd(7, lit(1), col("a")))


@pytest.mark.localtest
@pytest.mark.parametrize(
    "part,expected",
    [
        ("yyy", [Row(date(2021, 8, 1)), Row(date(2011, 12, 1))]),
        ("qtr", [Row(date(2020, 11, 1)), Row(date(2011, 3, 1))]),
        ("mon", [Row(date(2020, 9, 1)), Row(date(2011, 1, 1))]),
        ("wk", [Row(date(2020, 8, 8)), Row(date(2010, 12, 8))]),
        ("day", [Row(date(2020, 8, 2)), Row(date(2010, 12, 2))]),
        ("hrs", [Row(datetime(2020, 8, 1, 1, 0)), Row(datetime(2010, 12, 1, 1, 0))]),
        ("min", [Row(datetime(2020, 8, 1, 0, 1)), Row(datetime(2010, 12, 1, 0, 1))]),
        (
            "sec",
            [Row(datetime(2020, 8, 1, 0, 0, 1)), Row(datetime(2010, 12, 1, 0, 0, 1))],
        ),
        (
            "msec",
            [
                Row(datetime(2020, 8, 1, 0, 0, 0, 1000)),
                Row(datetime(2010, 12, 1, 0, 0, 0, 1000)),
            ],
        ),
        (
            "usec",
            [
                Row(datetime(2020, 8, 1, 0, 0, 0, 1)),
                Row(datetime(2010, 12, 1, 0, 0, 0, 1)),
            ],
        ),
        (
            "nsec",
            [
                Row(datetime(2020, 8, 1, 0, 0, 0, 10)),
                Row(datetime(2010, 12, 1, 0, 0, 0, 10)),
            ],
        ),
    ],
)
def test_dateadd(part, expected, session):
    val = 10000 if part == "nsec" else 1

    df = TestData.date1(session)

    Utils.check_answer(
        df.select(dateadd(part, lit(val), col("a"))),
        expected,
        sort=False,
    )

    # Test with name string instead of Column
    Utils.check_answer(
        df.select(dateadd(part, lit(val), "a")),
        expected,
        sort=False,
    )


@pytest.mark.localtest
@pytest.mark.parametrize(
    "part,expected",
    [
        (
            "yyy",
            [
                Row(
                    datetime(2025, 2, 1, 12, 0),
                    datetime(2018, 2, 24, 12, 0, 0, 456000),
                    datetime(
                        2018, 2, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2018, 2, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "qtr",
            [
                Row(
                    datetime(2024, 5, 1, 12, 0),
                    datetime(2017, 5, 24, 12, 0, 0, 456000),
                    datetime(
                        2017, 5, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+7")
                    ),
                    datetime(
                        2017, 5, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "mon",
            [
                Row(
                    datetime(2024, 3, 1, 12, 0),
                    datetime(2017, 3, 24, 12, 0, 0, 456000),
                    datetime(
                        2017, 3, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+7")
                    ),
                    datetime(
                        2017, 3, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "wk",
            [
                Row(
                    datetime(2024, 2, 8, 12, 0),
                    datetime(2017, 3, 3, 12, 0, 0, 456000),
                    datetime(
                        2017, 3, 3, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2017, 3, 3, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "day",
            [
                Row(
                    datetime(2024, 2, 2, 12, 0),
                    datetime(2017, 2, 25, 12, 0, 0, 456000),
                    datetime(
                        2017, 2, 25, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2017, 2, 25, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "hrs",
            [
                Row(
                    datetime(2024, 2, 1, 13, 0),
                    datetime(2017, 2, 24, 13, 0, 0, 456000),
                    datetime(
                        2017, 2, 24, 5, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2017, 2, 24, 15, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "min",
            [
                Row(
                    datetime(2024, 2, 1, 12, 1),
                    datetime(2017, 2, 24, 12, 1, 0, 456000),
                    datetime(
                        2017, 2, 24, 4, 1, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2017, 2, 24, 14, 1, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "sec",
            [
                Row(
                    datetime(2024, 2, 1, 12, 0, 1),
                    datetime(2017, 2, 24, 12, 0, 1, 456000),
                    datetime(
                        2017, 2, 24, 4, 0, 1, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2017, 2, 24, 14, 0, 1, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "msec",
            [
                Row(
                    datetime(2024, 2, 1, 12, 0, 0, 1000),
                    datetime(2017, 2, 24, 12, 0, 0, 457000),
                    datetime(
                        2017, 2, 24, 4, 0, 0, 124000, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2017, 2, 24, 14, 0, 0, 790000, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "usec",
            [
                Row(
                    datetime(2024, 2, 1, 12, 0, 0, 1),
                    datetime(2017, 2, 24, 12, 0, 0, 456001),
                    datetime(
                        2017, 2, 24, 4, 0, 0, 123001, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2017, 2, 24, 14, 0, 0, 789001, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
        (
            "nsec",
            [
                Row(
                    datetime(2024, 2, 1, 12, 0, 0, 10),
                    datetime(2017, 2, 24, 12, 0, 0, 456010),
                    datetime(
                        2017, 2, 24, 4, 0, 0, 123010, tzinfo=pytz.timezone("Etc/GMT+8")
                    ),
                    datetime(
                        2017, 2, 24, 14, 0, 0, 789010, tzinfo=pytz.timezone("Etc/GMT-1")
                    ),
                )
            ],
        ),
    ],
)
def test_dateadd_timestamp(part, expected, session, local_testing_mode):
    val = 10000 if part == "nsec" else 1

    with parameter_override(
        session,
        "timezone",
        "America/Los_Angeles",
        not IS_IN_STORED_PROC and not local_testing_mode,
    ):
        LocalTimezone.set_local_timezone(pytz.timezone("US/Pacific"))

        df = TestData.datetime_primitives1(session).select(
            ["timestamp", "timestamp_ntz", "timestamp_ltz", "timestamp_tz"]
        )

        Utils.check_answer(
            df.select(*[dateadd(part, lit(val), column) for column in df.columns]),
            expected,
            sort=False,
        )

        LocalTimezone.set_local_timezone()


@pytest.mark.localtest
@pytest.mark.parametrize(
    "part",
    [
        "yyy",
        "qtr",
        "mon",
        "wk",
        "day",
        "hrs",
        "min",
        "sec",
        "msec",
        "usec",
        "nsec",
    ],
)
@pytest.mark.parametrize(
    "tz_type,tzinfo",
    [
        (TimestampTimeZone.DEFAULT, None),
        (TimestampTimeZone.NTZ, None),
        (TimestampTimeZone.LTZ, pytz.UTC),
        (TimestampTimeZone.TZ, pytz.UTC),
    ],
)
def test_dateadd_tz(tz_type, tzinfo, part, session):
    data = [
        (datetime(2020, 8, 1, tzinfo=tzinfo)),
        (datetime(2010, 12, 1, tzinfo=tzinfo)),
    ]
    schema = StructType([StructField("a", TimestampType(tz_type))])
    tz_df = session.create_dataframe(data, schema)

    # Test that tz information is not corrupted when transformation is applied
    Utils.check_answer(
        tz_df.select(dateadd(part, lit(0), "a")),
        [Row(d) for d in data],
        sort=False,
    )


@pytest.mark.localtest
@pytest.mark.parametrize(
    "part,expected",
    [
        ("yyy", [2024, 2017, 2017, 2017]),
        ("qtr", [1, 1, 1, 1]),
        ("mon", [2, 2, 2, 2]),
        ("wk", [5, 8, 8, 8]),
        ("day", [1, 24, 24, 24]),
        ("hrs", [12, 12, 4, 14]),
        ("min", [0, 0, 0, 0]),
        ("sec", [0, 0, 0, 0]),
        ("nsec", [0, 456000000, 123000000, 789000000]),
        ("weekday", [4, 5, 5, 5]),
        ("dow_iso", [4, 5, 5, 5]),
        ("doy", [32, 55, 55, 55]),
        ("week_iso", [5, 8, 8, 8]),
        ("yearofweek", [2024, 2017, 2017, 2017]),
        ("yearofweekiso", [2024, 2017, 2017, 2017]),
        ("epoch", [1706788800, 1487937600, 1487937600, 1487941200]),
        (
            "epoch_milliseconds",
            [1706788800000, 1487937600456, 1487937600123, 1487941200789],
        ),
        (
            "epoch_microseconds",
            [1706788800000000, 1487937600456000, 1487937600123000, 1487941200789000],
        ),
        (
            "epoch_nanoseconds",
            [
                1706788800000000000,
                1487937600456000000,
                1487937600123000000,
                1487941200789000000,
            ],
        ),
        ("tzh", [0, 0, -8, 1]),
        ("tzm", [0, 0, 0, 0]),
    ],
)
def test_date_part_timestamp(part, expected, session):
    LocalTimezone.set_local_timezone(pytz.timezone("Etc/GMT+8"))

    df = TestData.datetime_primitives1(session)
    Utils.check_answer(
        df.select(
            *[
                date_part(part, col)
                for col in [
                    "timestamp",
                    "timestamp_ntz",
                    "timestamp_ltz",
                    "timestamp_tz",
                ]
            ]
        ),
        [Row(*expected)],
        sort=False,
    )

    LocalTimezone.set_local_timezone()


@pytest.mark.localtest
@pytest.mark.parametrize(
    "part,expected",
    [
        ("yyy", [2024]),
        ("qtr", [1]),
        ("mon", [2]),
        ("wk", [5]),
        ("day", [1]),
        ("weekday", [4]),
        ("dow_iso", [4]),
        ("doy", [32]),
        ("week_iso", [5]),
        ("yearofweek", [2024]),
        ("yearofweekiso", [2024]),
        ("epoch", [1706745600]),
    ],
)
def test_date_part_date(part, expected, session):
    LocalTimezone.set_local_timezone(pytz.timezone("Etc/GMT+8"))

    df = TestData.datetime_primitives1(session)
    Utils.check_answer(
        df.select(date_part(part, "date")),
        [Row(*expected)],
        sort=False,
    )

    LocalTimezone.set_local_timezone()


@pytest.mark.localtest
@pytest.mark.parametrize(
    "part,expected",
    [
        (
            "dd",
            [
                date(2024, 2, 1),
                datetime(2024, 2, 1, 0, 0),
                datetime(2017, 2, 24, 0, 0),
                datetime(2017, 2, 24, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2017, 2, 24, 0, 0, tzinfo=pytz.timezone("Etc/GMT-1")),
            ],
        ),
        (
            "hh",
            [
                date(2024, 2, 1),
                datetime(2024, 2, 1, 12, 0),
                datetime(2017, 2, 24, 12, 0),
                datetime(2017, 2, 24, 4, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2017, 2, 24, 14, 0, tzinfo=pytz.timezone("Etc/GMT-1")),
            ],
        ),
        (
            "usec",
            [
                date(2024, 2, 1),
                datetime(2024, 2, 1, 12, 0),
                datetime(2017, 2, 24, 12, 0, 0, 456000),
                datetime(
                    2017, 2, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
                datetime(
                    2017, 2, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                ),
            ],
        ),
        (
            "msec",
            [
                date(2024, 2, 1),
                datetime(2024, 2, 1, 12, 0),
                datetime(2017, 2, 24, 12, 0, 0, 456000),
                datetime(
                    2017, 2, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
                datetime(
                    2017, 2, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                ),
            ],
        ),
        (
            "min",
            [
                date(2024, 2, 1),
                datetime(2024, 2, 1, 12, 0),
                datetime(2017, 2, 24, 12, 0),
                datetime(2017, 2, 24, 4, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2017, 2, 24, 14, 0, tzinfo=pytz.timezone("Etc/GMT-1")),
            ],
        ),
        (
            "mm",
            [
                date(2024, 2, 1),
                datetime(2024, 2, 1, 0, 0),
                datetime(2017, 2, 1, 0, 0),
                datetime(2017, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2017, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT-1")),
            ],
        ),
        (
            "nsec",
            [
                date(2024, 2, 1),
                datetime(2024, 2, 1, 12, 0),
                datetime(2017, 2, 24, 12, 0, 0, 456000),
                datetime(
                    2017, 2, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
                datetime(
                    2017, 2, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                ),
            ],
        ),
        (
            "qtr",
            [
                date(2024, 1, 1),
                datetime(2024, 1, 1, 0, 0),
                datetime(2017, 1, 1, 0, 0),
                datetime(2017, 1, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2017, 1, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT-1")),
            ],
        ),
        (
            "sec",
            [
                date(2024, 2, 1),
                datetime(2024, 2, 1, 12, 0),
                datetime(2017, 2, 24, 12, 0),
                datetime(2017, 2, 24, 4, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2017, 2, 24, 14, 0, tzinfo=pytz.timezone("Etc/GMT-1")),
            ],
        ),
        (
            "wk",
            [
                date(2024, 1, 29),
                datetime(2024, 1, 29, 0, 0),
                datetime(2017, 2, 20, 0, 0),
                datetime(2017, 2, 20, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2017, 2, 20, 0, 0, tzinfo=pytz.timezone("Etc/GMT-1")),
            ],
        ),
        (
            "yyy",
            [
                date(2024, 1, 1),
                datetime(2024, 1, 1, 0, 0),
                datetime(2017, 1, 1, 0, 0),
                datetime(2017, 1, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2017, 1, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT-1")),
            ],
        ),
    ],
)
def test_date_trunc(part, expected, session, local_testing_mode):
    with parameter_override(
        session,
        "timezone",
        "America/Los_Angeles",
        not IS_IN_STORED_PROC and not local_testing_mode,
    ):
        LocalTimezone.set_local_timezone(pytz.timezone("Etc/GMT+8"))

        df = TestData.datetime_primitives1(session)
        trunc_df = df.select(
            *[
                date_trunc(part, col)
                for col in [
                    "date",
                    "timestamp",
                    "timestamp_ntz",
                    "timestamp_ltz",
                    "timestamp_tz",
                ]
            ]
        )
        Utils.check_answer(
            trunc_df,
            [Row(*expected)],
            sort=False,
        )
        types = [field.datatype for field in trunc_df.schema.fields]
        assert isinstance(types[0], DateType), "Datetype should remain Datetype"
        assert all(
            isinstance(d, TimestampType) for d in types[1:]
        ), "All timestamp type values should remain timestamps"
        assert [t.tz for t in types[2:]] == [
            TimestampTimeZone.NTZ,
            TimestampTimeZone.LTZ,
            TimestampTimeZone.TZ,
        ], "Timestamps with tz type specified should match"

        LocalTimezone.set_local_timezone()


@pytest.mark.localtest
def test_date_trunc_negative(session, local_testing_mode):
    if local_testing_mode:
        err = ValueError
    else:
        err = SnowparkSQLException

    df = TestData.datetime_primitives1(session)

    # Invalid date part
    with pytest.raises(err):
        df.select(date_trunc("foobar", "date")).collect()

    # Unsupported date part
    with pytest.raises(err):
        df.select(date_trunc("dow", "date")).collect()


@pytest.mark.localtest
def test_dateadd_negative(session):
    with pytest.raises(ValueError, match="part must be a string"):
        TestData.date1(session).select(dateadd(7, lit(1), "a"))


@pytest.mark.localtest
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

    df = session.create_dataframe(
        [("04/05/2020 01:02:03",), ("04/05/2020 02:03:04",)]
    ).to_df("a")

    Utils.check_answer(
        df.select(to_timestamp(col("A"), lit("mm/dd/yyyy hh24:mi:ss"))),
        [
            Row(datetime(2020, 4, 5, 1, 2, 3)),
            Row(datetime(2020, 4, 5, 2, 3, 4)),
        ],
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
        [
            Row(datetime(2020, 4, 5, 1, 2, 3)),
            Row(datetime(2020, 4, 5, 2, 3, 4)),
        ],
    )

    # Check that a string value can be passed as the format string
    Utils.check_answer(
        df.select(to_timestamp("A", "mm/dd/yyyy hh24:mi:ss")),
        [
            Row(datetime(2020, 4, 5, 1, 2, 3)),
            Row(datetime(2020, 4, 5, 2, 3, 4)),
        ],
    )


@pytest.mark.localtest
@pytest.mark.parametrize(
    "to_type,expected",
    [
        (
            to_timestamp_ntz,
            Row(
                datetime(2024, 2, 1, 8, 0, 1),
                datetime(2024, 2, 1, 8, 0),
                datetime(2024, 2, 1, 0, 0),
                datetime(2024, 2, 1, 0, 0),
                datetime(2024, 2, 1, 0, 0),
                datetime(2024, 2, 1, 12, 0),
                datetime(2017, 2, 24, 12, 0, 0, 456000),
                datetime(2017, 2, 24, 4, 0, 0, 123000),
                datetime(2017, 2, 24, 14, 0, 0, 789000),
            ),
        ),
        (
            to_timestamp_ltz,
            Row(
                datetime(2024, 2, 1, 0, 0, 1, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 1, 31, 22, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 2, 1, 12, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(
                    2017, 2, 24, 12, 0, 0, 456000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
                datetime(
                    2017, 2, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
                datetime(
                    2017, 2, 24, 5, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
            ),
        ),
        (
            to_timestamp_tz,
            Row(
                datetime(2024, 2, 1, 0, 0, 1, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+6")),
                datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(2024, 2, 1, 12, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                datetime(
                    2017, 2, 24, 12, 0, 0, 456000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
                datetime(
                    2017, 2, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
                datetime(
                    2017, 2, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                ),
            ),
        ),
    ],
)
def test_to_timestamp_all(to_type, expected, session, local_testing_mode):
    with parameter_override(
        session,
        "timezone",
        "America/Los_Angeles",
        not IS_IN_STORED_PROC and not local_testing_mode,
    ):
        LocalTimezone.set_local_timezone(pytz.timezone("Etc/GMT+8"))

        df = TestData.datetime_primitives1(session)

        # Query as string column
        Utils.check_answer(
            df.select(*[to_type(column) for column in df.columns]),
            expected,
            sort=False,
        )

        # Query with column objects
        Utils.check_answer(
            df.select(*[to_type(col(column)) for column in df.columns]),
            expected,
            sort=False,
        )

        LocalTimezone.set_local_timezone()


@pytest.mark.localtest
@pytest.mark.parametrize(
    "to_type,expected",
    [
        (
            to_timestamp_tz,
            [
                Row(
                    datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 2, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 3, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
            ],
        ),
        (
            to_timestamp_ntz,
            [
                Row(datetime(2024, 2, 1, 0, 0)),
                Row(datetime(2024, 2, 2, 0, 0)),
                Row(datetime(2024, 2, 3, 0, 0)),
            ],
        ),
        (
            to_timestamp_ltz,
            [
                Row(
                    datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 2, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 3, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
            ],
        ),
        (
            to_timestamp_tz,
            [
                Row(
                    datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 2, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 3, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
            ],
        ),
    ],
)
def test_to_timestamp_fmt_string(to_type, expected, session, local_testing_mode):
    with parameter_override(
        session,
        "timezone",
        "America/Los_Angeles",
        not IS_IN_STORED_PROC and not local_testing_mode,
    ):
        LocalTimezone.set_local_timezone(pytz.timezone("Etc/GMT+8"))
        data = [
            ("2024-02-01 00:00:00.000000",),
            ("2024-02-02 00:00:00.000000",),
            ("2024-02-03 00:00:00.000000",),
        ]
        df = session.create_dataframe(data).to_df(["str"])

        Utils.check_answer(
            df.select(to_type(col("str"), "YYYY-MM-DD HH24:MI:SS.FF")),
            expected,
            sort=False,
        )
        LocalTimezone.set_local_timezone()


@pytest.mark.localtest
@pytest.mark.parametrize(
    "to_type,expected",
    [
        (
            to_timestamp_tz,
            [
                Row(
                    datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 2, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 3, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
            ],
        ),
        (
            to_timestamp_ntz,
            [
                Row(datetime(2024, 2, 1, 0, 0)),
                Row(datetime(2024, 2, 2, 0, 0)),
                Row(datetime(2024, 2, 3, 0, 0)),
            ],
        ),
        (
            to_timestamp_ltz,
            [
                Row(
                    datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 2, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 3, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
            ],
        ),
        (
            to_timestamp_tz,
            [
                Row(
                    datetime(2024, 2, 1, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 2, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
                Row(
                    datetime(2024, 2, 3, 0, 0, tzinfo=pytz.timezone("Etc/GMT+8")),
                ),
            ],
        ),
    ],
)
def test_to_timestamp_fmt_column(to_type, expected, session, local_testing_mode):
    with parameter_override(
        session,
        "timezone",
        "America/Los_Angeles",
        not IS_IN_STORED_PROC and not local_testing_mode,
    ):
        LocalTimezone.set_local_timezone(pytz.timezone("Etc/GMT+8"))
        data = [
            ("2024-02-01 00:00:00.000000", "YYYY-MM-DD HH24:MI:SS.FF"),
            ("20240202000000000000", "YYYYMMDDHH24MISSFF"),
            ("03 Feb 2024 00:00:00", "DD mon YYYY HH24:MI:SS"),
        ]
        df = session.create_dataframe(data).to_df(["str", "fmt"])

        Utils.check_answer(
            df.select(to_type(col("str"), col("fmt"))),
            expected,
            sort=False,
        )
        LocalTimezone.set_local_timezone()


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


@pytest.mark.localtest
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


@pytest.mark.localtest
@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_startswith(session, col_a):
    Utils.check_answer(
        TestData.string4(session).select(startswith(col_a, lit("ban"))),
        [Row(False), Row(True), Row(False)],
        sort=False,
    )


@pytest.mark.localtest
@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_endswith(session, col_a):
    Utils.check_answer(
        TestData.string4(session).select(endswith(col_a, lit("ana"))),
        [Row(False), Row(True), Row(False)],
        sort=False,
    )


def test_char(session):
    df = session.create_dataframe([(84, 85), (96, 97)]).to_df("A", "B")

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


@pytest.mark.localtest
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


@pytest.mark.localtest
def test_strip_null_value(session):
    df = TestData.null_json1(session)

    Utils.check_answer(
        df.select(df.v["a"]),
        [Row("null"), Row('"foo"'), Row(None)],
        sort=False,
    )

    Utils.check_answer(
        df.select(strip_null_value(df.v["a"])),
        [Row(None), Row('"foo"'), Row(None)],
        sort=False,
    )


@pytest.mark.parametrize("col_amount", ["amount", col("amount")])
def test_array_agg(session, col_amount):
    assert sorted(
        json.loads(
            TestData.monthly_sales(session)
            .select(array_agg(col_amount))
            .collect()[0][0]
        )
    ) == [
        200,
        400,
        800,
        2500,
        3000,
        4500,
        4500,
        5000,
        5000,
        6000,
        8000,
        9500,
        10000,
        10000,
        35000,
        90500,
    ]

    assert sorted(
        json.loads(
            TestData.monthly_sales(session)
            .select(array_agg(col_amount, is_distinct=True))
            .collect()[0][0]
        )
    ) == [200, 400, 800, 2500, 3000, 4500, 5000, 6000, 8000, 9500, 10000, 35000, 90500]


def test_array_agg_within_group(session):
    assert json.loads(
        TestData.monthly_sales(session)
        .select(array_agg(col("amount")).within_group("amount"))
        .collect()[0][0]
    ) == [
        200,
        400,
        800,
        2500,
        3000,
        4500,
        4500,
        5000,
        5000,
        6000,
        8000,
        9500,
        10000,
        10000,
        35000,
        90500,
    ]


def test_array_agg_within_group_order_by_desc(session):
    assert json.loads(
        TestData.monthly_sales(session)
        .select(array_agg(col("amount")).within_group(col("amount").desc()))
        .collect()[0][0]
    ) == [
        90500,
        35000,
        10000,
        10000,
        9500,
        8000,
        6000,
        5000,
        5000,
        4500,
        4500,
        3000,
        2500,
        800,
        400,
        200,
    ]


def test_array_agg_within_group_order_by_multiple_columns(session):
    sort_columns = [col("month").asc(), col("empid").desc(), col("amount")]
    amount_values = (
        TestData.monthly_sales(session).sort(sort_columns).select("amount").collect()
    )
    expected = [a[0] for a in amount_values]
    assert (
        json.loads(
            TestData.monthly_sales(session)
            .select(array_agg(col("amount")).within_group(sort_columns))
            .collect()[0][0]
        )
        == expected
    )


def test_window_function_array_agg_within_group(session):
    value1 = "[\n  1,\n  3\n]"
    value2 = "[\n  1,\n  3,\n  10\n]"
    Utils.check_answer(
        TestData.xyz(session).select(
            array_agg("Z").within_group(["Z", "Y"]).over(Window.partitionBy("X"))
        ),
        [Row(value1), Row(value1), Row(value2), Row(value2), Row(value2)],
    )


def test_array_append(session):
    Utils.check_answer(
        [
            Row('[\n  1,\n  2,\n  3,\n  "amount",\n  32.21\n]'),
            Row('[\n  6,\n  7,\n  8,\n  "amount",\n  32.21\n]'),
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
    row1 = ["1", "2", "3", '"amount"', "32.21"]
    assert [s.strip() for s in result_set[0][0][1:-1].split(",")] == row1
    row2 = ["6", "7", "8", '"amount"', "32.21"]
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
            Row('[\n  1,\n  2,\n  3,\n  "amount",\n  32.21\n]'),
            Row('[\n  6,\n  7,\n  8,\n  "amount",\n  32.21\n]'),
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
    row1 = ["1", "2", "3", '"amount"', "32.21"]
    assert [s.strip() for s in result_set[0][0][1:-1].split(",")] == row1
    row2 = ["6", "7", "8", '"amount"', "32.21"]
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
        == '[\n  1,\n  1.2,\n  "string",\n  "",\n  undefined\n]'
    )

    assert TestData.zero1(session).select(array_construct()).collect()[0][0] == "[]"

    Utils.check_answer(
        TestData.integer1(session).select(
            array_construct(col("a"), lit(1.2), lit(None))
        ),
        [
            Row("[\n  1,\n  1.2,\n  undefined\n]"),
            Row("[\n  2,\n  1.2,\n  undefined\n]"),
            Row("[\n  3,\n  1.2,\n  undefined\n]"),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.integer1(session).select(array_construct("a", lit(1.2), lit(None))),
        [
            Row("[\n  1,\n  1.2,\n  undefined\n]"),
            Row("[\n  2,\n  1.2,\n  undefined\n]"),
            Row("[\n  3,\n  1.2,\n  undefined\n]"),
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
        == '[\n  1,\n  1.2,\n  "string",\n  ""\n]'
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
            Row("[\n  1,\n  1.2\n]"),
            Row("[\n  2,\n  1.2\n]"),
            Row("[\n  3,\n  1.2\n]"),
        ],
        sort=False,
    )

    # Same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.integer1(session).select(
            array_construct_compact("a", lit(1.2), lit(None))
        ),
        [
            Row("[\n  1,\n  1.2\n]"),
            Row("[\n  2,\n  1.2\n]"),
            Row("[\n  3,\n  1.2\n]"),
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
            Row('[\n  32.21,\n  "amount",\n  1,\n  2,\n  3\n]'),
            Row('[\n  32.21,\n  "amount",\n  6,\n  7,\n  8\n]'),
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
            Row('[\n  32.21,\n  "amount",\n  1,\n  2,\n  3\n]'),
            Row('[\n  32.21,\n  "amount",\n  6,\n  7,\n  8\n]'),
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


def test_timestamp_tz_from_parts(session):
    try:
        if not IS_IN_STORED_PROC:
            session.sql('alter session set timezone="America/Los_Angeles"').collect()
        df = session.create_dataframe(
            [[2022, 4, 1, 11, 11, 0, "America/Los_Angeles"]],
            schema=["year", "month", "day", "hour", "minute", "second", "timezone"],
        )
        Utils.check_answer(
            df.select(
                timestamp_tz_from_parts(
                    "year",
                    "month",
                    "day",
                    "hour",
                    "minute",
                    "second",
                    nanoseconds=987654321,
                    timezone="timezone",
                )
            ),
            [
                Row(
                    datetime.strptime(
                        "2022-04-01 11:11:00.987654 -07:00", "%Y-%m-%d %H:%M:%S.%f %z"
                    )
                )
            ],
        )

        Utils.check_answer(
            df.select(
                timestamp_tz_from_parts(
                    "year",
                    "month",
                    "day",
                    "hour",
                    "minute",
                    "second",
                    nanoseconds=987654321,
                )
            ),
            [
                Row(
                    datetime.strptime(
                        "2022-04-01 11:11:00.987654 -07:00", "%Y-%m-%d %H:%M:%S.%f %z"
                    )
                )
            ],
        )

        Utils.check_answer(
            df.select(
                timestamp_tz_from_parts(
                    "year",
                    "month",
                    "day",
                    "hour",
                    "minute",
                    "second",
                    timezone="timezone",
                )
            ),
            [
                Row(
                    datetime.strptime(
                        "2022-04-01 11:11:00 -07:00", "%Y-%m-%d %H:%M:%S %z"
                    )
                )
            ],
        )

        Utils.check_answer(
            df.select(
                timestamp_tz_from_parts(
                    "year", "month", "day", "hour", "minute", "second"
                )
            ),
            [
                Row(
                    datetime.strptime(
                        "2022-04-01 11:11:00 -07:00", "%Y-%m-%d %H:%M:%S %z"
                    )
                )
            ],
        )
    finally:
        if not IS_IN_STORED_PROC:
            session.sql("alter session unset timezone").collect()


@pytest.mark.localtest
def test_convert_timezone(session, local_testing_mode):
    with parameter_override(
        session,
        "timezone",
        "America/Los_Angeles",
        not IS_IN_STORED_PROC and not local_testing_mode,
    ):
        LocalTimezone.set_local_timezone(pytz.timezone("Etc/GMT+8"))

        df = TestData.datetime_primitives1(session).select(
            "timestamp", "timestamp_ntz", "timestamp_ltz", "timestamp_tz"
        )

        Utils.check_answer(
            df.select(*[convert_timezone(lit("UTC"), col) for col in df.columns]),
            [
                Row(
                    datetime(2024, 2, 1, 20, 0, tzinfo=pytz.UTC),
                    datetime(2017, 2, 24, 20, 0, 0, 456000, tzinfo=pytz.UTC),
                    datetime(2017, 2, 24, 12, 0, 0, 123000, tzinfo=pytz.UTC),
                    datetime(2017, 2, 24, 13, 0, 0, 789000, tzinfo=pytz.UTC),
                )
            ],
        )

        LocalTimezone.set_local_timezone()


def test_time_from_parts(session):
    df = session.create_dataframe(
        [[11, 11, 0, 987654321]], schema=["hour", "minute", "second", "nanoseconds"]
    )

    Utils.check_answer(
        df.select(
            time_from_parts("hour", "minute", "second", nanoseconds="nanoseconds")
        ),
        [Row(time(11, 11, 0, 987654))],
    )

    Utils.check_answer(
        df.select(time_from_parts("hour", "minute", "second")), [Row(time(11, 11, 0))]
    )


@pytest.mark.localtest
def test_columns_from_timestamp_parts():
    func_name = "test _columns_from_timestamp_parts"
    y, m, d = _columns_from_timestamp_parts(func_name, "year", "month", 8)
    assert y._expression.name == '"YEAR"'
    assert m._expression.name == '"MONTH"'
    assert d._expression.value == 8

    y, m, d, h, min_, s = _columns_from_timestamp_parts(
        func_name, "year", "month", "day", 24, 60, 17
    )
    assert y._expression.name == '"YEAR"'
    assert m._expression.name == '"MONTH"'
    assert d._expression.name == '"DAY"'
    assert h._expression.value == 24
    assert min_._expression.value == 60
    assert s._expression.value == 17


@pytest.mark.localtest
def test_columns_from_timestamp_parts_negative():
    with pytest.raises(ValueError, match="Incorrect number of args passed"):
        _columns_from_timestamp_parts("neg test", "year", "month")


@pytest.mark.localtest
def test_timestamp_from_parts_internal():
    func_name = "test _timestamp_from_parts_internal"
    date_expr, time_expr = _timestamp_from_parts_internal(func_name, "date", "time")
    assert date_expr._expression.name == '"DATE"'
    assert time_expr._expression.name == '"TIME"'

    # ns and tz active
    y, m, d, h, min_, s, ns, tz = _timestamp_from_parts_internal(
        "timestamp_from_parts", "y", "m", "d", "h", "min", "s", "ns", "tz"
    )
    assert y._expression.name == '"Y"'
    assert m._expression.name == '"M"'
    assert d._expression.name == '"D"'
    assert h._expression.name == '"H"'
    assert min_._expression.name == '"MIN"'
    assert s._expression.name == '"S"'
    assert ns._expression.name == '"NS"'
    assert tz._expression.name == "tz"

    # ns active | tz non-active
    y, m, d, h, min_, s, ns = _timestamp_from_parts_internal(
        func_name, "y", "m", "d", "h", "min", "s", "ns"
    )
    assert y._expression.name == '"Y"'
    assert m._expression.name == '"M"'
    assert d._expression.name == '"D"'
    assert h._expression.name == '"H"'
    assert min_._expression.name == '"MIN"'
    assert s._expression.name == '"S"'
    assert ns._expression.name == '"NS"'

    # ns non-active | tz active
    y, m, d, h, min_, s, ns, tz = _timestamp_from_parts_internal(
        "timestamp_from_parts", "y", "m", "d", "h", "min", "s", timezone="tz"
    )
    assert y._expression.name == '"Y"'
    assert m._expression.name == '"M"'
    assert d._expression.name == '"D"'
    assert h._expression.name == '"H"'
    assert min_._expression.name == '"MIN"'
    assert s._expression.name == '"S"'
    assert ns._expression.value == 0
    assert tz._expression.name == "tz"

    # ns non-active | tz non-active
    y, m, d, h, min_, s = _timestamp_from_parts_internal(
        func_name, "y", "m", "d", "h", "min", "s"
    )
    assert y._expression.name == '"Y"'
    assert m._expression.name == '"M"'
    assert d._expression.name == '"D"'
    assert h._expression.name == '"H"'
    assert min_._expression.name == '"MIN"'
    assert s._expression.name == '"S"'


@pytest.mark.localtest
def test_timestamp_from_parts_internal_negative():
    func_name = "negative test"
    with pytest.raises(ValueError, match="expected 2 or 6 required arguments"):
        _timestamp_from_parts_internal(func_name, 1)

    with pytest.raises(ValueError, match="does not accept timezone as an argument"):
        _timestamp_from_parts_internal(func_name, 1, 2, 3, 4, 5, 6, 7, 8)


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


@pytest.mark.parametrize(
    "as_type,expected",
    [
        (
            as_timestamp_ntz,
            Row(
                None,
                None,
                None,
                None,
                None,
                datetime(2024, 2, 1, 12, 0),
                datetime(2017, 2, 24, 12, 0, 0, 456000),
                None,
                None,
            ),
        ),
        (
            as_timestamp_ltz,
            Row(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                datetime(
                    2017, 2, 24, 4, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT+8")
                ),
                None,  # not using America/Los_Angeles because pytz assign -7:53 timezone offset to it
            ),
        ),
        (
            as_timestamp_tz,
            Row(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                datetime(
                    2017, 2, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                ),
            ),
        ),
    ],
)
def test_as_timestamp_all(as_type, expected, session, local_testing_mode):
    with parameter_override(
        session,
        "timezone",
        "America/Los_Angeles",
        not IS_IN_STORED_PROC and not local_testing_mode,
    ):
        LocalTimezone.set_local_timezone(pytz.timezone("Etc/GMT+8"))
        df = TestData.variant_datetimes1(session)

        # Query as string column
        Utils.check_answer(
            df.select(*[as_type(column) for column in df.columns]),
            expected,
            sort=False,
        )

        # Query with column objects
        Utils.check_answer(
            df.select(*[as_type(col(column)) for column in df.columns]),
            expected,
            sort=False,
        )
        LocalTimezone.set_local_timezone()


@pytest.mark.localtest
def test_to_double(session, local_testing_mode):
    if not local_testing_mode:
        # Local testing only covers partial implementation of to_double
        df = session.create_dataframe([["1.2", "2.34-", "9.99MI"]]).to_df(
            ["a", "b", "fmt"]
        )

        Utils.check_answer(
            df.select(
                to_double("a"), to_double("b", "9.99MI"), to_double("b", col("fmt"))
            ),
            [Row(1.2, -2.34, -2.34)],
            sort=False,
        )

    df = session.create_dataframe([["1.2", "-2.34"]]).to_df(["a", "b"])

    Utils.check_answer(
        df.select(to_double("a"), to_double("b")),
        [Row(1.2, -2.34)],
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


def test_to_geography(session):
    geography_string = """{
  "coordinates": [
    30,
    10
  ],
  "type": "Point"
}"""
    geography = TestData.geography(session)
    Utils.check_answer(
        geography.select(to_geography(col("a"))),
        [Row(geography_string)],
        sort=False,
    )
    assert geography.select(to_geography(col("a"))).collect()[0][0] == geography_string

    # same as above, but pass str instead of Column
    assert geography.select(to_geography("a")).collect()[0][0] == geography_string


def test_to_geometry(session):
    geometry_string = """{
  "coordinates": [
    3.000000000000000e+01,
    1.000000000000000e+01
  ],
  "type": "Point"
}"""
    geometry = TestData.geometry(session)
    Utils.check_answer(
        geometry.select(to_geometry(col("a"))),
        [Row(geometry_string)],
        sort=False,
    )
    assert geometry.select(to_geometry(col("a"))).collect()[0][0] == geometry_string

    # same as above, but pass str instead of Column
    assert geometry.select(to_geometry("a")).collect()[0][0] == geometry_string


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
    "v, t2, t3, instance, zero",
    [
        ("v", "t2", "t3", "instance", 0),
        (col("v"), col("t2"), col("t3"), col("instance"), lit(0)),
    ],
)
def test_xmlget(session, v, t2, t3, instance, zero):
    Utils.check_answer(
        TestData.valid_xml1(session).select(get_ignore_case(xmlget(v, t2), lit("$"))),
        [Row('"bar"'), Row(None), Row('"foo"')],
        sort=False,
    )

    Utils.check_answer(
        TestData.valid_xml1(session).select(
            get_ignore_case(xmlget(v, t2, zero), lit("$"))
        ),
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


@pytest.mark.parametrize("col_a", ["A", col("A")])
def test_approx_count_distinct(session, col_a):
    Utils.check_answer(
        TestData.duplicated_numbers(session).select(approx_count_distinct(col_a)),
        [Row(3)],
    )


@pytest.mark.parametrize("col_a", ["A", col("A")])
def test_approx_percentile(session, col_a):
    Utils.check_answer(
        TestData.approx_numbers(session).select(approx_percentile(col_a, 0.5)),
        [Row(4.5)],
    )


@pytest.mark.parametrize("col_a", ["A", col("A")])
def test_approx_percentile_accumulate(session, col_a):
    Utils.check_answer(
        TestData.approx_numbers(session).select(approx_percentile_accumulate(col_a)),
        [
            Row(
                '{\n  "state": [\n    0.000000000000000e+00,\n    1.000000000000000e+00,\n    '
                + "1.000000000000000e+00,\n    1.000000000000000e+00,\n    2.000000000000000e+00,\n    "
                + "1.000000000000000e+00,\n    3.000000000000000e+00,\n    1.000000000000000e+00,\n    "
                + "4.000000000000000e+00,\n    1.000000000000000e+00,\n    5.000000000000000e+00,\n    "
                + "1.000000000000000e+00,\n    6.000000000000000e+00,\n    1.000000000000000e+00,\n    "
                + "7.000000000000000e+00,\n    1.000000000000000e+00,\n    8.000000000000000e+00,\n    "
                + "1.000000000000000e+00,\n    9.000000000000000e+00,\n    1.000000000000000e+00\n  ],\n  "
                + '"type": "tdigest",\n  "version": 1\n}'
            )
        ],
    )


@pytest.mark.parametrize("col_a", ["A", col("A")])
def test_approx_percentile_estimate(session, col_a):
    Utils.check_answer(
        TestData.approx_numbers(session).select(
            approx_percentile_estimate(approx_percentile_accumulate(col_a), 0.5)
        ),
        TestData.approx_numbers(session).select(approx_percentile(col_a, 0.5)),
    )


@pytest.mark.parametrize("col_a, col_b", [("A", "B"), (col("A"), col("B"))])
def test_approx_percentile_combine(session, col_a, col_b):
    df1 = (
        TestData.approx_numbers(session)
        .select(col_a)
        .where(col("a") >= lit(3))
        .select(approx_percentile_accumulate(col_a).as_("b"))
    )
    df2 = TestData.approx_numbers(session).select(
        approx_percentile_accumulate(col_a).as_("b")
    )
    df = df1.union(df2)
    Utils.check_answer(
        df.select(approx_percentile_combine(col_b)),
        [
            Row(
                '{\n  "state": [\n    0.000000000000000e+00,\n    1.000000000000000e+00,\n    '
                + "1.000000000000000e+00,\n    1.000000000000000e+00,\n    2.000000000000000e+00,\n    "
                + "1.000000000000000e+00,\n    3.000000000000000e+00,\n    1.000000000000000e+00,\n    "
                + "3.000000000000000e+00,\n    1.000000000000000e+00,\n    4.000000000000000e+00,\n    "
                + "1.000000000000000e+00,\n    4.000000000000000e+00,\n    1.000000000000000e+00,\n    "
                + "5.000000000000000e+00,\n    1.000000000000000e+00,\n    5.000000000000000e+00,\n    "
                + "1.000000000000000e+00,\n    6.000000000000000e+00,\n    1.000000000000000e+00,\n    "
                + "6.000000000000000e+00,\n    1.000000000000000e+00,\n    7.000000000000000e+00,\n    "
                + "1.000000000000000e+00,\n    7.000000000000000e+00,\n    1.000000000000000e+00,\n    "
                + "8.000000000000000e+00,\n    1.000000000000000e+00,\n    8.000000000000000e+00,\n    "
                + "1.000000000000000e+00,\n    9.000000000000000e+00,\n    1.000000000000000e+00,\n    "
                + '9.000000000000000e+00,\n    1.000000000000000e+00\n  ],\n  "type": "tdigest",\n  '
                + '"version": 1\n}'
            )
        ],
    )


@pytest.mark.localtest
def test_iff(session, local_testing_mode):
    df = session.create_dataframe(
        [(True, 2, 2, 4), (False, 12, 12, 14), (True, 22, 23, 24)],
        schema=["a", "b", "c", "d"],
    )
    Utils.check_answer(
        df.select("a", "b", "d", iff(col("a"), col("b"), col("d"))),
        [Row(True, 2, 4, 2), Row(False, 12, 14, 14), Row(True, 22, 24, 22)],
        sort=False,
    )
    Utils.check_answer(
        df.select("b", "c", "d", iff(col("b") == col("c"), col("b"), col("d"))),
        [Row(2, 2, 4, 2), Row(12, 12, 14, 12), Row(22, 23, 24, 24)],
        sort=False,
    )

    if not local_testing_mode:
        # accept sql expression
        Utils.check_answer(
            df.select("b", "c", "d", iff("b = c", col("b"), col("d"))),
            [Row(2, 2, 4, 2), Row(12, 12, 14, 12), Row(22, 23, 24, 24)],
            sort=False,
        )


def test_cume_dist(session):
    Utils.check_answer(
        TestData.xyz(session).select(
            cume_dist().over(Window.partition_by(col("X")).order_by(col("Y")))
        ),
        [Row(0.3333333333333333), Row(1.0), Row(1.0), Row(1.0), Row(1.0)],
        sort=False,
    )


def test_dense_rank(session):
    Utils.check_answer(
        TestData.xyz(session).select(dense_rank().over(Window.order_by(col("X")))),
        [Row(1), Row(1), Row(2), Row(2), Row(2)],
        sort=False,
    )


@pytest.mark.localtest
@pytest.mark.parametrize("col_z", ["Z", col("Z")])
def test_lag(session, col_z, local_testing_mode):
    Utils.check_answer(
        TestData.xyz(session).select(
            lag(col_z, 1, 0).over(Window.partition_by(col("X")).order_by(col("X")))
        ),
        [Row(0), Row(10), Row(1), Row(0), Row(1)],
        sort=local_testing_mode,
    )

    Utils.check_answer(
        TestData.xyz(session).select(
            lag(col_z, 1).over(Window.partition_by(col("X")).order_by(col("X")))
        ),
        [Row(None), Row(10), Row(1), Row(None), Row(1)],
        sort=local_testing_mode,
    )

    Utils.check_answer(
        TestData.xyz(session).select(
            lag(col_z).over(Window.partition_by(col("X")).order_by(col("X")))
        ),
        [Row(None), Row(10), Row(1), Row(None), Row(1)],
        sort=local_testing_mode,
    )


@pytest.mark.localtest
@pytest.mark.parametrize("col_z", ["Z", col("Z")])
def test_lead(session, col_z, local_testing_mode):
    Utils.check_answer(
        TestData.xyz(session).select(
            lead(col_z, 1, 0).over(Window.partition_by(col("X")).order_by(col("X")))
        ),
        [Row(1), Row(3), Row(0), Row(3), Row(0)],
        sort=local_testing_mode,
    )

    Utils.check_answer(
        TestData.xyz(session).select(
            lead(col_z, 1).over(Window.partition_by(col("X")).order_by(col("X")))
        ),
        [Row(1), Row(3), Row(None), Row(3), Row(None)],
        sort=local_testing_mode,
    )

    Utils.check_answer(
        TestData.xyz(session).select(
            lead(col_z).over(Window.partition_by(col("X")).order_by(col("X")))
        ),
        [Row(1), Row(3), Row(None), Row(3), Row(None)],
        sort=local_testing_mode,
    )


@pytest.mark.localtest
@pytest.mark.parametrize("col_z", ["Z", col("Z")])
def test_last_value(session, col_z):
    Utils.check_answer(
        TestData.xyz(session).select(
            last_value(col_z).over(Window.partition_by(col("X")).order_by(col("Z")))
        ),
        [Row(3), Row(3), Row(10), Row(10), Row(10)],
        sort=False,
    )


@pytest.mark.localtest
@pytest.mark.parametrize("col_z", ["Z", col("Z")])
def test_first_value(session, col_z):
    Utils.check_answer(
        TestData.xyz(session).select(
            first_value(col_z).over(Window.partition_by(col("X")).order_by(col("Z")))
        ),
        [Row(1), Row(1), Row(1), Row(1), Row(1)],
        sort=False,
    )


@pytest.mark.parametrize("col_n", ["n", col("n"), 4, 0])
def test_ntile(session, col_n):
    df = TestData.xyz(session)
    if not isinstance(col_n, int):
        df = df.with_column("n", lit(4))
    if isinstance(col_n, int) and col_n == 0:
        with pytest.raises(
            SnowparkSQLException, match="NTILE argument must be at least 1"
        ):
            df.select(
                ntile(col_n).over(Window.partition_by(col("X")).order_by(col("Y")))
            ).collect()
    else:
        Utils.check_answer(
            df.select(
                ntile(col_n).over(Window.partition_by(col("X")).order_by(col("Y")))
            ),
            [Row(1), Row(2), Row(3), Row(1), Row(2)],
            sort=False,
        )


def test_percent_rank(session):
    Utils.check_answer(
        TestData.xyz(session).select(
            percent_rank().over(Window.partition_by(col("X")).order_by(col("Y")))
        ),
        [Row(0.0), Row(0.5), Row(0.5), Row(0.0), Row(0.0)],
        sort=False,
    )


def test_rank(session):
    Utils.check_answer(
        TestData.xyz(session).select(
            rank().over(Window.partition_by(col("X")).order_by(col("Y")))
        ),
        [Row(1), Row(2), Row(2), Row(1), Row(1)],
        sort=False,
    )


def test_row_number(session):
    Utils.check_answer(
        TestData.xyz(session).select(
            row_number().over(Window.partition_by(col("X")).order_by(col("Y")))
        ),
        [Row(1), Row(2), Row(3), Row(1), Row(2)],
        sort=False,
    )


def test_listagg(session):
    df = session.create_dataframe([1, 2, 3, 2, 4, 5], schema=["col"])
    Utils.check_answer(
        df.select(listagg(df["col"]).within_group(df["col"].asc())), [Row("122345")]
    )
    Utils.check_answer(
        df.select(listagg("col", ",").within_group(df["col"].asc())),
        [Row("1,2,2,3,4,5")],
    )
    Utils.check_answer(
        df.select(listagg(df.col("col"), ",", True).within_group(df["col"].asc())),
        [Row("1,2,3,4,5")],
    )
    Utils.check_answer(
        df.select(listagg("col", "'", True).within_group(df["col"].asc())),
        [Row("1'2'3'4'5")],
    )


@pytest.mark.parametrize(
    "col_expr, col_scale", [("expr", 1), (col("expr"), col("scale"))]
)
def test_trunc(session, col_expr, col_scale):
    df = session.create_dataframe([(3.14, 1)], schema=["expr", "scale"])
    Utils.check_answer(df.select(trunc(col_expr, col_scale)), [Row(3.1)])


@pytest.mark.parametrize("col_A, col_scale", [("A", 0), (col("A"), lit(0))])
def test_round(session, col_A, col_scale):
    Utils.check_answer(
        TestData.double1(session).select(round(col_A, col_scale)),
        [Row(1.0), Row(2.0), Row(3.0)],
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_sin_sinh(session, col_A):
    Utils.check_answer(
        TestData.double2(session).select(sin(col_A), sinh(col_A)),
        [
            Row(0.09983341664682815, 0.10016675001984403),
            Row(0.19866933079506122, 0.20133600254109402),
            Row(0.29552020666133955, 0.3045202934471426),
        ],
        sort=False,
    )


@pytest.mark.parametrize("col_A, col_B", [("A", "B"), (col("A"), col("B"))])
def test_cos_cosh(session, col_A, col_B):
    Utils.check_answer(
        TestData.double2(session).select(cos(col_A), cosh(col_B)),
        [
            Row(0.9950041652780258, 1.1276259652063807),
            Row(0.9800665778412416, 1.1854652182422676),
            Row(0.955336489125606, 1.255169005630943),
        ],
        sort=False,
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_tan_tanh(session, col_A):
    Utils.check_answer(
        TestData.double2(session).select(tan(col_A), tanh(col_A)),
        [
            Row(0.10033467208545055, 0.09966799462495582),
            Row(0.2027100355086725, 0.197375320224904),
            Row(0.30933624960962325, 0.2913126124515909),
        ],
        sort=False,
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_asin_acos(session, col_A):
    Utils.check_answer(
        TestData.double2(session).select(acos(col_A), asin(col_A)),
        [
            Row(1.4706289056333368, 0.1001674211615598),
            Row(1.369438406004566, 0.2013579207903308),
            Row(1.2661036727794992, 0.3046926540153975),
        ],
        sort=False,
    )


@pytest.mark.parametrize("col_A, col_B", [("A", "B"), (col("A"), col("B"))])
def test_atan_atan2(session, col_A, col_B):
    Utils.check_answer(
        TestData.double2(session).select(atan(col_B), atan(col_A)),
        [
            Row(0.4636476090008061, 0.09966865249116204),
            Row(0.5404195002705842, 0.19739555984988078),
            Row(0.6107259643892086, 0.2914567944778671),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.double2(session).select(atan2(col_B, col_A)),
        [Row(1.373400766945016), Row(1.2490457723982544), Row(1.1659045405098132)],
        sort=False,
    )


@pytest.mark.parametrize("col_A, col_B", [("A", "B"), (col("A"), col("B"))])
def test_degrees(session, col_A, col_B):
    Utils.check_answer(
        TestData.double2(session).select(degrees(col_A), degrees(col_B)),
        [
            Row(5.729577951308233, 28.64788975654116),
            Row(11.459155902616466, 34.37746770784939),
            Row(17.188733853924695, 40.10704565915762),
        ],
        sort=False,
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_radians(session, col_A):
    Utils.check_answer(
        TestData.double1(session).select(radians(col_A)),
        [
            Row(0.019390607989657),
            Row(0.038781215979314),
            Row(0.058171823968971005),
        ],
        sort=False,
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_factorial(session, col_A):
    Utils.check_answer(
        TestData.integer1(session).select(factorial(col_A)),
        [Row(1), Row(2), Row(6)],
        sort=False,
    )


@pytest.mark.parametrize("col_0, col_2, col_4", [(0, 2, 4), (lit(0), lit(2), lit(4))])
def test_div0(session, col_0, col_2, col_4):
    Utils.check_answer(
        TestData.zero1(session).select(div0(col_2, col_0), div0(col_4, col_2)),
        [Row(0.0, 2.0)],
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_md5_sha1_sha2(session, col_A):
    Utils.check_answer(
        TestData.string1(session).select(md5(col_A), sha1(col_A), sha2(col_A, 224)),
        [
            Row(
                "5a105e8b9d40e1329780d62ea2265d8a",
                "b444ac06613fc8d63795be9ad0beaf55011936ac",
                "aff3c83c40e2f1ae099a0166e1f27580525a9de6acd995f21717e984",
            ),
            Row(
                "ad0234829205b9033196ba818f7a872b",
                "109f4b3c50d7b0df729d299bc6f8e9ef9066971f",
                "35f757ad7f998eb6dd3dd1cd3b5c6de97348b84a951f13de25355177",
            ),
            Row(
                "8ad8757baa8564dc136c1e07507f4a98",
                "3ebfa301dc59196f18593c45e519287a23297589",
                "d2d5c076b2435565f66649edd604dd5987163e8a8240953144ec652f",
            ),
        ],
        sort=False,
    )


@pytest.mark.parametrize("col_B", ["B", col("B")])
def test_ascii(session, col_B):
    Utils.check_answer(
        TestData.string1(session).select(ascii(col_B)),
        [Row(97), Row(98), Row(99)],
        sort=False,
    )


@pytest.mark.localtest
@pytest.mark.parametrize(
    "func,expected",
    [
        (
            initcap,
            [
                Row(
                    "Foo-Bar;Baz",
                    "Qwer,Dvor>Azer",
                    "Lower",
                    "Upper",
                    "Chief Variable Officer",
                    "Lorem Ipsum Dolor Sit Amet",
                )
            ],
        ),
        (
            partial(initcap, delimiters=lit("-")),
            [
                Row(
                    "Foo-Bar;baz",
                    "Qwer,dvor>azer",
                    "Lower",
                    "Upper",
                    "Chief variable officer",
                    "Lorem ipsum dolor sit amet",
                )
            ],
        ),
        (length, [Row(11, 14, 5, 5, 22, 26)]),
        (
            lower,
            [
                Row(
                    "foo-bar;baz",
                    "qwer,dvor>azer",
                    "lower",
                    "upper",
                    "chief variable officer",
                    "lorem ipsum dolor sit amet",
                )
            ],
        ),
        (
            upper,
            [
                Row(
                    "FOO-BAR;BAZ",
                    "QWER,DVOR>AZER",
                    "LOWER",
                    "UPPER",
                    "CHIEF VARIABLE OFFICER",
                    "LOREM IPSUM DOLOR SIT AMET",
                )
            ],
        ),
    ],
)
@pytest.mark.parametrize("use_col", [True, False])
def test_initcap_length_lower_upper(func, expected, use_col, session):
    df = TestData.string8(session)
    Utils.check_answer(
        df.select(*[func(col(c) if use_col else c) for c in df.columns]),
        expected,
        sort=False,
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_lpad_rpad(session, col_A):
    Utils.check_answer(
        TestData.string2(session).select(
            lpad(col_A, 8, lit("X")), rpad(col_A, 9, lit("S"))
        ),
        [
            Row("XXXasdFg", "asdFgSSSS"),
            Row("XXXXXqqq", "qqqSSSSSS"),
            Row("XXXXXXQw", "QwSSSSSSS"),
        ],
        sort=False,
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_ltrim_rtrim_trim(session, col_A):
    Utils.check_answer(
        TestData.string3(session).select(ltrim(col_A), rtrim(col_A), trim(col_A)),
        [Row("abcba  ", "  abcba", "abcba"), Row("a12321a   ", " a12321a", "a12321a")],
        sort=False,
    )

    Utils.check_answer(
        TestData.string3(session).select(
            ltrim(col_A, lit(" a")), rtrim(col_A, lit(" a")), trim(col_A, lit("a "))
        ),
        [Row("bcba  ", "  abcb", "bcb"), Row("12321a   ", " a12321", "12321")],
        sort=False,
    )


@pytest.mark.parametrize("col_B", ["B", col("B")])
def test_repeat(session, col_B):
    Utils.check_answer(
        TestData.string1(session).select(repeat(col_B, 3)),
        [Row("aaa"), Row("bbb"), Row("ccc")],
        sort=False,
    )


@pytest.mark.parametrize("col_A", ["A", col("A")])
def test_soundex(session, col_A):
    Utils.check_answer(
        TestData.string4(session).select(soundex(col_A)),
        [Row("a140"), Row("b550"), Row("p200")],
        sort=False,
    )


@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_insert(session, col_a):
    Utils.check_answer(
        TestData.string4(session).select(insert(col_a, 2, 3, lit("abc"))),
        [Row("aabce"), Row("babcna"), Row("pabch")],
        sort=False,
    )


@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_left(session, col_a):
    Utils.check_answer(
        TestData.string4(session).select(left(col_a, 2)),
        [Row("ap"), Row("ba"), Row("pe")],
        sort=False,
    )


@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_right(session, col_a):
    Utils.check_answer(
        TestData.string4(session).select(right(col_a, 2)),
        [Row("le"), Row("na"), Row("ch")],
        sort=False,
    )


@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_regexp_count(session, col_a):
    Utils.check_answer(
        TestData.string4(session).select(regexp_count(col_a, "a")),
        [Row(1), Row(3), Row(1)],
        sort=False,
    )

    Utils.check_answer(
        TestData.string4(session).select(regexp_count(col_a, "a", 2, "c")),
        [Row(0), Row(3), Row(1)],
        sort=False,
    )


@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_replace(session, col_a):
    Utils.check_answer(
        TestData.string4(session).select(replace(col_a, "a")),
        [Row("pple"), Row("bnn"), Row("pech")],
        sort=False,
    )

    Utils.check_answer(
        TestData.string4(session).select(replace(col_a, "a", "z")),
        [Row("zpple"), Row("bznznz"), Row("pezch")],
        sort=False,
    )


@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_charindex(session, col_a):
    Utils.check_answer(
        TestData.string4(session).select(charindex(lit("na"), col_a)),
        [Row(0), Row(3), Row(0)],
        sort=False,
    )

    Utils.check_answer(
        TestData.string4(session).select(charindex(lit("na"), col_a, 4)),
        [Row(0), Row(5), Row(0)],
        sort=False,
    )


@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_collate(session, col_a):
    Utils.check_answer(
        TestData.string3(session).where(collate(col_a, "en_US-trim") == "abcba"),
        [Row("  abcba  ")],
        sort=False,
    )


def test_collation(session):
    Utils.check_answer(
        TestData.zero1(session).select(collation(lit("f").collate("de"))),
        [Row("de")],
    )
