#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import math

import pytest

from snowflake.snowpark import DataFrame, Row, Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.mock.connection import MockServerConnection

session = Session(MockServerConnection())


@pytest.mark.localtest
def test_basic_filter():
    origin_df: DataFrame = session.create_dataframe(
        [
            [1, 2, "abc"],
            [3, 4, "def"],
            [6, 5, "ghi"],
            [8, 7, "jkl"],
            [100, 200, "mno"],
            [400, 300, "pqr"],
        ],
        schema=["a", "b", "c"],
    ).select("a", "b", "c")

    # equal
    assert origin_df.filter(col("a") == 1).collect() == [Row(1, 2, "abc")]

    # not equal
    assert origin_df.filter(col("a") != 1).collect() == [
        Row(3, 4, "def"),
        Row(6, 5, "ghi"),
        Row(8, 7, "jkl"),
        Row(100, 200, "mno"),
        Row(400, 300, "pqr"),
    ]

    # greater
    assert origin_df.filter(col("a") > 8).collect() == [
        Row(100, 200, "mno"),
        Row(400, 300, "pqr"),
    ]

    # greater or equal
    assert origin_df.filter(col("a") >= 8).collect() == [
        Row(8, 7, "jkl"),
        Row(100, 200, "mno"),
        Row(400, 300, "pqr"),
    ]

    # less
    assert origin_df.filter(col("a") < 8).collect() == [
        Row(1, 2, "abc"),
        Row(3, 4, "def"),
        Row(6, 5, "ghi"),
    ]

    # less or equal
    assert origin_df.filter(col("a") <= 8).collect() == [
        Row(1, 2, "abc"),
        Row(3, 4, "def"),
        Row(6, 5, "ghi"),
        Row(8, 7, "jkl"),
    ]

    # and expression
    assert origin_df.filter((col("a") >= 4) & (col("b") == 300)).collect() == [
        Row(400, 300, "pqr")
    ]

    # or expression
    assert origin_df.filter((col("a") > 300) | (col("c") == "ghi")).collect() == [
        Row(6, 5, "ghi"),
        Row(400, 300, "pqr"),
    ]

    assert origin_df.filter(origin_df["a"].between(6, 100)).collect() == [
        Row(6, 5, "ghi"),
        Row(8, 7, "jkl"),
        Row(100, 200, "mno"),
    ]

    # in expression
    assert origin_df.filter(col("a").in_([1, 6, 100])).collect() == [
        Row(1, 2, "abc"),
        Row(6, 5, "ghi"),
        Row(100, 200, "mno"),
    ]

    # not expression
    assert origin_df.filter(~col("a").in_([1, 6, 100])).collect() == [
        Row(3, 4, "def"),
        Row(8, 7, "jkl"),
        Row(400, 300, "pqr"),
    ]


@pytest.mark.localtest
def test_null_nan_filter():
    origin_df: DataFrame = session.create_dataframe(
        [
            [float("nan"), 2, "abc"],
            [3.0, 4, "def"],
            [6.0, 5, "ghi"],
            [8.0, 7, None],
            [float("nan"), 200, None],
        ],
        schema=["a", "b", "c"],
    ).select("a", "b", "c")

    # is null
    res = origin_df.filter(origin_df["c"].is_null()).collect()
    assert len(res) == 2
    assert res[0] == Row(8.0, 7, None)
    assert math.isnan(res[1][0])
    assert res[1][1] == 200 and res[1][2] is None

    # is not null
    res = origin_df.filter(origin_df["c"].is_not_null()).collect()
    assert len(res) == 3
    assert math.isnan(res[0][0]) and res[0][1] == 2 and res[0][2] == "abc"
    assert res[1] == Row(3.0, 4, "def") and res[2] == Row(6.0, 5, "ghi")

    # equal na
    res = origin_df.filter(origin_df["a"].equal_nan()).collect()
    assert len(res) == 2
    assert math.isnan(res[0][0]) and res[0][1] == 2 and res[0][2] == "abc"
    assert math.isnan(res[1][0]) and res[1][1] == 200 and res[1][2] is None

    res = origin_df.filter(origin_df["c"].is_null()).collect()
    assert len(res) == 2
    assert res[0] == Row(8.0, 7, None)
    assert math.isnan(res[1][0])
    assert res[1][1] == 200 and res[1][2] is None

    # equal_null
    origin_df: DataFrame = session.create_dataframe(
        [
            [float("nan"), float("nan")],
            [float("nan"), 15.0],
            [1.0, 1.0],
            [1.0, 2.0],
            [99.0, 100.0],
            [None, None],
        ],
        schema=["a", "b"],
    ).select("a", "b")
    res = origin_df.filter(origin_df["a"].equal_null(origin_df["b"])).collect()
    assert len(res) == 3
    assert math.isnan(res[0][0]) and math.isnan(res[0][1])
    assert res[1] == Row(1.0, 1.0)
    assert res[2] == Row(None, None)


@pytest.mark.localtest
def test_chain_filter():
    origin_df: DataFrame = session.create_dataframe(
        [
            [1, 2, "abc"],
            [3, 4, "def"],
            [6, 5, "ghi"],
            [8, 7, "jkl"],
            [100, 200, "mno"],
            [400, 300, "pqr"],
        ],
        schema=["a", "b", "c"],
    ).select("a", "b", "c")

    assert origin_df.filter(col("a") > 8).filter(col("c") == "pqr").collect() == [
        Row(400, 300, "pqr"),
    ]


@pytest.mark.localtest
def test_like_filter():
    origin_df: DataFrame = session.create_dataframe(
        [["test"], ["tttest"], ["tett"], ["ess"], ["es#!s"], ["es#)s"]], schema=["a"]
    ).select("a")

    assert origin_df.filter(col("a").like("test")).collect() == [Row("test")]
    assert origin_df.filter(col("a").like("%est%")).collect() == [
        Row("test"),
        Row("tttest"),
    ]
    assert origin_df.filter(col("a").like(".e.*")).collect() == []
    assert origin_df.filter(col("a").like("es__s")).collect() == [
        Row("es#!s"),
        Row("es#)s"),
    ]
    assert origin_df.filter(col("a").like("es___s")).collect() == []
    assert origin_df.filter(col("a").like("es%s")).collect() == [
        Row("ess"),
        Row("es#!s"),
        Row("es#)s"),
    ]
    assert origin_df.filter(col("a").like("%tt%")).collect() == [
        Row("tttest"),
        Row("tett"),
    ]


@pytest.mark.localtest
def test_regex_filter():
    origin_df: DataFrame = session.create_dataframe(
        [["test"], ["tttest"], ["tett"], ["ess"], ["es#%s"]], schema=["a"]
    ).select("a")

    assert origin_df.filter(col("a").regexp("test")).collect() == [Row("test")]
    assert origin_df.filter(col("a").regexp("^est")).collect() == []
    assert origin_df.filter(col("a").regexp(".e.*")).collect() == [
        Row("test"),
        Row("tett"),
    ]
    assert origin_df.filter(col("a").regexp(".*s")).collect() == [
        Row("ess"),
        Row("es#%s"),
    ]
    assert origin_df.filter(col("a").regexp("...%.")).collect() == [Row("es#%s")]
