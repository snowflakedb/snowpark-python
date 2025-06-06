#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import datetime
import math
import unittest.mock
from typing import Iterable

import pytest

from snowflake.snowpark import DataFrame, Row
from snowflake.snowpark.functions import (
    abs,
    array_agg,
    array_construct,
    asc,
    call_function,
    col,
    concat_ws,
    contains,
    count,
    current_date,
    current_time,
    current_timestamp,
    desc,
    get,
    is_null,
    lit,
    max,
    min,
    rank,
    row_number,
    to_char,
    to_date,
)
from snowflake.snowpark.mock._functions import MockedFunctionRegistry, patch
from snowflake.snowpark.mock._snowflake_data_type import ColumnEmulator, ColumnType
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.window import Window

from tests.utils import Utils


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
        origin_df._show_string(_emit_ast=False)
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
        origin_df._show_string(2, 2, _emit_ast=False)
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


def test_current_time(session):
    df = session.create_dataframe([1], schema=["a"])
    now_datetime = datetime.datetime(2024, 8, 13, 10, 1, 50)
    with unittest.mock.patch("snowflake.snowpark.mock._functions.datetime") as dt:
        dt.datetime.now.return_value = now_datetime
        assert df.select(
            current_timestamp(), current_date(), current_time()
        ).collect() == [Row(now_datetime, now_datetime.date(), now_datetime.time())]


def test_patch_unsupported_function(session):
    df = session.create_dataframe([[3, 1], [3, 2], [4, 3]], schema=["a", "b"])
    with pytest.raises(NotImplementedError):
        df.select(
            call_function("greatest_ignore_nulls", df["a"], df["b"]).alias("greatest")
        ).collect()

    @patch("greatest_ignore_nulls")
    def mock_greatest_ignore_nulls(
        *columns: Iterable[ColumnEmulator],
    ) -> ColumnEmulator:
        return ColumnEmulator(
            [1] * len(columns[0]), sf_type=ColumnType(IntegerType(), False)
        )

    assert df.select(
        call_function("greatest_ignore_nulls", df["a"], df["b"]).alias("greatest")
    ).collect() == [Row(1), Row(1), Row(1)]

    @patch("greatest_ignore_nulls")
    def mock_wrong_patch(columns: Iterable[ColumnEmulator]) -> ColumnEmulator:
        return ColumnEmulator(
            [1] * len(columns[0]), sf_type=ColumnType(IntegerType(), False)
        )

    with pytest.raises(SnowparkLocalTestingException) as exc:
        df.select(
            call_function("greatest_ignore_nulls", df["a"], df["b"]).alias("greatest")
        ).collect()
    assert "Please ensure the implementation follows specifications" in str(exc.value)


def test_row_number(session):
    df = session.create_dataframe(
        [
            (1, datetime.datetime(2020, 1, 1, 1, 1, 1)),
            (1, datetime.datetime(2020, 1, 1, 1, 1, 2)),
            (2, datetime.datetime(2020, 1, 1, 1, 1, 3)),
            (2, datetime.datetime(2020, 1, 1, 1, 1, 4)),
            (3, datetime.datetime(2020, 1, 1, 1, 1, 5)),
            (3, datetime.datetime(2020, 1, 1, 1, 1, 6)),
            (3, datetime.datetime(2020, 1, 1, 1, 1, 7)),
            (3, datetime.datetime(2020, 1, 1, 1, 1, 8)),
        ],
        schema=["a", "b"],
    )

    window = Window.partitionBy("a").order_by("b")
    Utils.check_answer(
        df.withColumn("c", row_number().over(window)),
        [
            Row(1, datetime.datetime(2020, 1, 1, 1, 1, 1), 1),
            Row(1, datetime.datetime(2020, 1, 1, 1, 1, 2), 2),
            Row(2, datetime.datetime(2020, 1, 1, 1, 1, 3), 1),
            Row(2, datetime.datetime(2020, 1, 1, 1, 1, 4), 2),
            Row(3, datetime.datetime(2020, 1, 1, 1, 1, 5), 1),
            Row(3, datetime.datetime(2020, 1, 1, 1, 1, 6), 2),
            Row(3, datetime.datetime(2020, 1, 1, 1, 1, 7), 3),
            Row(3, datetime.datetime(2020, 1, 1, 1, 1, 8), 4),
        ],
    )


def test_rank(session):
    df_to_test = session.create_dataframe(
        [
            ("A", 1, 2),
            ("A", 1, 2),
            ("A", 2, 4),
            ("A", 3, 4),
            ("B", 3, 1),
            ("B", 2, 2),
            ("B", 3, 3),
            ("B", 1, 4),
            ("B", 4, 5),
        ],
        ["cat", "val", "val2"],
    )

    # Test ascending single column
    window_spec_asc = Window.partition_by(col("cat")).order_by(col("val").asc())
    df_ranked_asc = df_to_test.with_column("rank", rank().over(window_spec_asc))
    Utils.check_answer(
        df_ranked_asc,
        [
            Row("A", 1, 2, 1),
            Row("A", 1, 2, 1),
            Row("A", 2, 4, 3),
            Row("A", 3, 4, 4),
            Row("B", 1, 4, 1),
            Row("B", 2, 2, 2),
            Row("B", 3, 1, 3),
            Row("B", 3, 3, 3),
            Row("B", 4, 5, 5),
        ],
    )

    # Test descending single column
    window_spec_desc = Window.partition_by(col("cat")).order_by(col("val").desc())
    df_ranked_desc = df_to_test.with_column("rank", rank().over(window_spec_desc))
    Utils.check_answer(
        df_ranked_desc,
        [
            Row("A", 3, 4, 1),
            Row("A", 2, 4, 2),
            Row("A", 1, 2, 3),
            Row("A", 1, 2, 3),
            Row("B", 4, 5, 1),
            Row("B", 3, 1, 2),
            Row("B", 3, 3, 2),
            Row("B", 2, 2, 4),
            Row("B", 1, 4, 5),
        ],
    )

    # Test ascending double column
    window_spec_asc = Window.partition_by(col("cat")).order_by(
        col("val").asc(), col("val2").asc()
    )
    df_ranked_asc = df_to_test.with_column("rank", rank().over(window_spec_asc))
    Utils.check_answer(
        df_ranked_asc,
        [
            Row("A", 1, 2, 1),
            Row("A", 1, 2, 1),
            Row("A", 2, 4, 3),
            Row("A", 3, 4, 4),
            Row("B", 1, 4, 1),
            Row("B", 2, 2, 2),
            Row("B", 3, 1, 3),
            Row("B", 3, 3, 4),
            Row("B", 4, 5, 5),
        ],
    )

    # Test descending double column
    window_spec_desc = Window.partition_by(col("cat")).order_by(
        col("val").desc(), col("val2").desc()
    )
    df_ranked_desc = df_to_test.with_column("rank", rank().over(window_spec_desc))
    Utils.check_answer(
        df_ranked_desc,
        [
            Row("A", 3, 4, 1),
            Row("A", 2, 4, 2),
            Row("A", 1, 2, 3),
            Row("A", 1, 2, 3),
            Row("B", 4, 5, 1),
            Row("B", 3, 3, 2),
            Row("B", 3, 1, 3),
            Row("B", 2, 2, 4),
            Row("B", 1, 4, 5),
        ],
    )

    # Test asc and desc
    window_spec_asc_desc = Window.partition_by(col("cat")).order_by(
        col("val").asc(), col("val2").desc()
    )
    df_ranked_asc_desc = df_to_test.with_column(
        "rank", rank().over(window_spec_asc_desc)
    )
    Utils.check_answer(
        df_ranked_asc_desc,
        [
            Row("A", 1, 2, 1),
            Row("A", 1, 2, 1),
            Row("A", 2, 4, 3),
            Row("A", 3, 4, 4),
            Row("B", 1, 4, 1),
            Row("B", 2, 2, 2),
            Row("B", 3, 3, 3),
            Row("B", 3, 1, 4),
            Row("B", 4, 5, 5),
        ],
    )


def test_get(session):
    data = [
        Row(101, 1, "cat"),
        Row(101, 2, "dog"),
        Row(101, 3, "dog"),
        Row(102, 4, "cat"),
    ]
    df = session.create_dataframe(data, schema=["ID", "TS", "VALUE"])

    agged = df.groupBy("ID").agg(
        array_agg(col("VALUE")).within_group(col("TS")).alias("VALUES")
    )
    get_df = agged.select("ID", get(col("VALUES"), 1).alias("ELEMENT"))
    Utils.check_answer(get_df, [Row(102, None), Row(101, '"dog"')])


def test_array_construct_indexing(session):
    data = [
        Row(a=1, b="name1", c=5.2),
        Row(a=2, b="name2", c=3.9),
        Row(a=3, b="name3", c=10.8),
    ]

    df = session.create_dataframe(data=data)

    df = df.with_column("d", array_construct(*["a", "b", "c"]))

    for n, result in [
        (
            1,
            [
                Row(
                    1,
                    "name1",
                    5.2,
                    '[\n  1,\n  "name1",\n  5.2\n]',
                    '[\n  1,\n  "name1",\n  5.2\n]',
                )
            ],
        ),
        (
            2,
            [
                Row(
                    2,
                    "name2",
                    3.9,
                    '[\n  2,\n  "name2",\n  3.9\n]',
                    '[\n  2,\n  "name2",\n  3.9\n]',
                )
            ],
        ),
        (
            3,
            [
                Row(
                    3,
                    "name3",
                    10.8,
                    '[\n  3,\n  "name3",\n  10.8\n]',
                    '[\n  3,\n  "name3",\n  10.8\n]',
                )
            ],
        ),
    ]:
        # Each filter changes the index for the result. Test taht array construct maintains the index correctly
        filtered = df.filter(col("a") == n)
        filtered = filtered.with_column("arr", array_construct(*["a", "b", "c"]))
        Utils.check_answer(filtered, result)


def test_concat_ws_indexing(session):
    df = session.create_dataframe([(1, "A"), (2, "B"), (3, "C")], schema=["A", "B"])
    filtered = df.where(df.A > 1)
    final = filtered.with_column("concat", concat_ws(lit("-"), "A", "B"))
    Utils.check_answer(final, [Row(2, "B", "2-B"), Row(3, "C", "3-C")])
