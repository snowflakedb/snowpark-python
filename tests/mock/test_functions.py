#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import datetime
import math

from snowflake.snowpark import DataFrame, Row, Session
from snowflake.snowpark.functions import date_format  # alias of to-date
from snowflake.snowpark.functions import (  # count,; is_null,;
    abs,
    asc,
    col,
    contains,
    count,
    desc,
    is_null,
    lit,
    max,
    min,
    to_date,
)
from snowflake.snowpark.mock.mock_connection import MockServerConnection

session = Session(MockServerConnection())


def test_col():
    origin_df: DataFrame = session.create_dataframe(
        [
            [1, "a", True],
            [6, "c", False],
            [None, None, None],
        ],
        schema=["m", "n", "o"],
    )
    assert origin_df.select(col("m")).collect() == [Row(1), Row(6), Row(None)]
    assert origin_df.select(col("n")).collect() == [Row("a"), Row("c"), Row(None)]
    assert origin_df.select(col("o")).collect() == [Row(True), Row(False), Row(None)]


def test_max():
    origin_df: DataFrame = session.create_dataframe(
        [
            ["a", "ddd", 11.0, None, None, True, math.nan],
            ["a", "ddd", 22.0, None, None, True, math.nan],
            ["b", None, 99.0, None, math.nan, False, math.nan],
            ["b", "g", None, None, math.nan, None, math.nan],
        ],
        schema=["m", "n", "o", "p", "q", "r", "s"],
    )

    assert origin_df.select(max("m").as_("m")).collect() == [Row("b")]
    assert origin_df.select(max("n").as_("n")).collect() == [Row("g")]
    assert origin_df.select(max("o").as_("o")).collect() == [Row(99.0)]
    assert origin_df.select(max("p").as_("p")).collect() == [Row(None)]
    assert math.isnan(origin_df.select(max("q").as_("q")).collect()[0][0])
    assert origin_df.select(max("r").as_("r")).collect() == [Row(True)]
    assert math.isnan(origin_df.select(max("s").as_("s")).collect()[0][0])


def test_min():
    origin_df: DataFrame = session.create_dataframe(
        [
            ["a", "ddd", 11.0, None, None, True, math.nan],
            ["a", "ddd", 22.0, None, None, True, math.nan],
            ["b", None, 99.0, None, math.nan, False, math.nan],
            ["b", "g", None, None, math.nan, None, math.nan],
        ],
        schema=["m", "n", "o", "p", "q", "r", "s"],
    )

    assert origin_df.select(min("m").as_("m")).collect() == [Row("a")]
    assert origin_df.select(min("n").as_("n")).collect() == [Row("ddd")]
    assert origin_df.select(min("o").as_("o")).collect() == [Row(11.0)]
    assert origin_df.select(min("p").as_("p")).collect() == [Row(None)]
    assert math.isnan(origin_df.select(min("q").as_("q")).collect()[0][0])
    assert origin_df.select(min("r").as_("r")).collect() == [Row(False)]
    assert math.isnan(origin_df.select(min("s").as_("s")).collect()[0][0])


def test_date_format():
    origin_df: DataFrame = session.create_dataframe(
        ["2013-05-17", "31536000000000"],
        schema=["m"],
    )

    assert origin_df.select(to_date("m")).collect() == [
        Row(datetime.date(2013, 5, 17)),
        Row(datetime.date(1971, 1, 1)),
    ]

    origin_df: DataFrame = session.create_dataframe(
        [
            "12-31-1989",
            "04-03-2023",
        ],
        schema=["m"],
    )

    assert origin_df.select(date_format("m", lit("MM-DD-YYYY"))).collect() == [
        Row(datetime.date(1989, 12, 31)),
        Row(datetime.date(2023, 4, 3)),
    ]

    origin_df: DataFrame = session.create_dataframe(
        [
            "DEC-31-1989",
            "APR-03-2023",
        ],
        schema=["m"],
    )

    assert origin_df.select(date_format("m", lit("MON-DD-YYYY"))).collect() == [
        Row(datetime.date(1989, 12, 31)),
        Row(datetime.date(2023, 4, 3)),
    ]


def test_contains():
    origin_df: DataFrame = session.create_dataframe(
        [
            ["1", "2"],
            ["3", "4"],
            ["5", "5"],
        ],
        schema=["m", "n"],
    )

    assert origin_df.select(contains(col("m"), col("n"))).collect() == [
        Row(False),
        Row(False),
        Row(True),
    ]

    origin_df: DataFrame = session.create_dataframe(
        [
            ["abcd", "bc"],
            ["defgg", "gg"],
            ["xx", "zz"],
        ],
        schema=["m", "n"],
    )

    assert origin_df.select(contains(col("m"), col("n"))).collect() == [
        Row(True),
        Row(True),
        Row(False),
    ]

    assert origin_df.select(contains(col("m"), lit("xx"))).collect() == [
        Row(False),
        Row(False),
        Row(True),
    ]


def test_abs():
    origin_df: DataFrame = session.create_dataframe(
        [
            [1, -4],
            [-1, -5],
            [2, -6],
        ],
        schema=["m", "n"],
    )
    assert origin_df.select(abs(col("m"))).collect() == [Row(1), Row(1), Row(2)]


def test_asc_and_desc():
    origin_df: DataFrame = session.create_dataframe(
        [
            [1],
            [8],
            [6],
            [3],
            [100],
            [400],
        ],
        schema=["v"],
    )
    expected = [Row(1), Row(3), Row(6), Row(8), Row(100), Row(400)]
    assert origin_df.sort(asc(col("v"))).collect() == expected
    expected.reverse()
    assert origin_df.sort(desc(col("v"))).collect() == expected


def test_count():
    origin_df: DataFrame = session.create_dataframe(
        [
            [1],
            [8],
            [6],
            [3],
            [100],
            [400],
        ],
        schema=["v"],
    )
    assert origin_df.select(count("v")).collect() == [Row(6)]


def test_is_null():
    origin_df: DataFrame = session.create_dataframe(
        [
            [float("nan"), 2, "abc"],
            [3.0, 4, "def"],
            [6.0, 5, "ghi"],
            [8.0, 7, None],
            [float("nan"), 200, None],
        ],
        schema=["a", "b", "c"],
    )
    assert origin_df.select(is_null("a"), is_null("b"), is_null("c")).collect() == [
        Row(False, False, False),
        Row(False, False, False),
        Row(False, False, False),
        Row(False, False, True),
        Row(False, False, True),
    ]


def test_take_first():
    origin_df: DataFrame = session.create_dataframe(
        [
            [float("nan"), 2, "abc"],
            [3.0, 4, "def"],
            [6.0, 5, "ghi"],
            [8.0, 7, None],
            [float("nan"), 200, None],
        ],
        schema=["a", "b", "c"],
    )
    assert (
        math.isnan(origin_df.select("a").first()[0])
        and len(origin_df.select("a").first()) == 1
    )
    assert origin_df.select("a", "c").order_by("c", ascending=False).first(2) == [
        Row(6.0, "ghi"),
        Row(3.0, "def"),
    ]

    res = origin_df.select("a", "b", "c").take(10)
    assert len(res) == 5
    assert math.isnan(res[0][0]) and res[0][1] == 2 and res[0][2] == "abc"
    assert res[1:4] == [
        Row(3.0, 4, "def"),
        Row(6.0, 5, "ghi"),
        Row(8.0, 7, None),
    ]
    assert math.isnan(res[4][0]) and res[4][1] == 200 and res[4][2] is None

    res = origin_df.select("a", "b", "c").take(-1)
    assert len(res) == 5
    assert math.isnan(res[0][0]) and res[0][1] == 2 and res[0][2] == "abc"
    assert res[1:4] == [
        Row(3.0, 4, "def"),
        Row(6.0, 5, "ghi"),
        Row(8.0, 7, None),
    ]
    assert math.isnan(res[4][0]) and res[4][1] == 200 and res[4][2] is None


def test_show():
    origin_df: DataFrame = session.create_dataframe(
        [
            [float("nan"), 2, "abc"],
            [3.0, 4, "def"],
            [6.0, 5, "ghi"],
            [8.0, 7, None],
            [float("nan"), 200, None],
        ],
        schema=["a", "b", "c"],
    )

    origin_df.show()
    assert (
        origin_df._show_string()
        == """
--------------------
|"A"  |"B"  |"C"   |
--------------------
|nan  |2    |abc   |
|3.0  |4    |def   |
|6.0  |5    |ghi   |
|8.0  |7    |NULL  |
|nan  |200  |NULL  |
--------------------\n""".lstrip()
    )

    assert (
        origin_df._show_string(2, 2)
        == """
----------------
|"A...|"B...|"C...|
----------------
|na...|2   |ab...|
|3....|4   |de...|
----------------\n""".lstrip()
    )
