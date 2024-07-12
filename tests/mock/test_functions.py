#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import datetime
import math

import pytest

from snowflake.snowpark import DataFrame, Row
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.column import Column, _to_col_if_str
from snowflake.snowpark.functions import (  # count,; is_null,;
    abs,
    asc,
    builtin,
    col,
    contains,
    count,
    desc,
    is_null,
    lit,
    max,
    min,
    to_char,
    to_date,
)
from snowflake.snowpark.mock._functions import MockedFunctionRegistry, patch
from snowflake.snowpark.mock._snowflake_data_type import ColumnEmulator


def test_col(session):
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


def test_max(session):
    origin_df: DataFrame = session.create_dataframe(
        [
            ["a", "ddd", 11.0, None, None, True, math.nan],
            ["a", "ddd", 22.0, None, None, True, math.nan],
            ["b", None, 99.0, None, math.nan, False, math.nan],
            ["b", "g", None, None, math.nan, None, math.nan],
        ],
        schema=["m", "n", "o", "p", "q", "r", "s"],
    )
    # JIRA for same name alias support: https://snowflakecomputing.atlassian.net/browse/SNOW-845619
    assert origin_df.select(max("m").as_("a")).collect() == [Row("b")]
    assert origin_df.select(max("n").as_("b")).collect() == [Row("g")]
    assert origin_df.select(max("o").as_("c")).collect() == [Row(99.0)]
    assert origin_df.select(max("p").as_("d")).collect() == [Row(None)]
    assert math.isnan(origin_df.select(max("q").as_("e")).collect()[0][0])
    assert origin_df.select(max("r").as_("f")).collect() == [Row(True)]
    assert math.isnan(origin_df.select(max("s").as_("g")).collect()[0][0])


def test_min(session):
    origin_df: DataFrame = session.create_dataframe(
        [
            ["a", "ddd", 11.0, None, None, True, math.nan],
            ["a", "ddd", 22.0, None, None, True, math.nan],
            ["b", None, 99.0, None, math.nan, False, math.nan],
            ["b", "g", None, None, math.nan, None, math.nan],
        ],
        schema=["m", "n", "o", "p", "q", "r", "s"],
    )

    # JIRA for same name alias support: https://snowflakecomputing.atlassian.net/browse/SNOW-845619
    assert origin_df.select(min("m").as_("a")).collect() == [Row("a")]
    assert origin_df.select(min("n").as_("b")).collect() == [Row("ddd")]
    assert origin_df.select(min("o").as_("c")).collect() == [Row(11.0)]
    assert origin_df.select(min("p").as_("d")).collect() == [Row(None)]
    assert math.isnan(origin_df.select(min("q").as_("e")).collect()[0][0])
    assert origin_df.select(min("r").as_("f")).collect() == [Row(False)]
    assert math.isnan(origin_df.select(min("s").as_("g")).collect()[0][0])


def test_to_date(session):
    origin_df: DataFrame = session.create_dataframe(
        ["2013-05-17", "31536000000000"],
        schema=["m"],
    )

    assert origin_df.select(to_date("m")).collect() == [
        Row(datetime.date(2013, 5, 17)),
        Row(datetime.date(1971, 1, 1)),
    ]


def test_contains(session):
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


def test_abs(session):
    origin_df: DataFrame = session.create_dataframe(
        [
            [1, -4],
            [-1, -5],
            [2, -6],
        ],
        schema=["m", "n"],
    )
    assert origin_df.select(abs(col("m"))).collect() == [Row(1), Row(1), Row(2)]


def test_asc_and_desc(session):
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


def test_count(session):
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


def test_is_null(session):
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


def test_take_first(session):
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


def test_show(session):
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


def test_to_char_is_row_index_agnostic(session):
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
    assert df.filter(col("a") > 3).select(to_char(col("a")), col("b")).collect() == [
        Row("5", 6)
    ]


@pytest.mark.skipif(
    "not config.getoption('local_testing_mode', default=True)",
    reason="Only test local testing code in local testing mode.",
)
def test_function_mock_and_call_neg(session):
    def foobar(e: ColumnOrName) -> Column:
        c = _to_col_if_str(e, "foobar")
        return builtin("foobar")(c)

    # Patching function that does not exist will fail when called
    @patch("foobar")
    def mock_foobar(column: ColumnEmulator):
        return column

    with pytest.raises(NotImplementedError):
        df = session.create_dataframe([[1]]).to_df(["a"])
        df.select(foobar("a")).collect()


@pytest.mark.skipif(
    "not config.getoption('local_testing_mode', default=True)",
    reason="Only test local testing code in local testing mode.",
)
def test_function_register_unregister(session):
    registry = MockedFunctionRegistry()

    def _abs(x):
        return math.abs(x)

    # Try register/unregister using actual function
    assert registry.get_function("abs") is None
    mocked = registry.register(abs, _abs)
    assert registry.get_function("abs") == mocked
    registry.unregister(abs)
    assert registry.get_function("abs") is None

    # Try register/unregister using function name
    assert registry.get_function("abs") is None
    mocked = registry.register("abs", _abs)
    assert registry.get_function("abs") == mocked
    registry.unregister("abs")
    assert registry.get_function("abs") is None
