#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import math

from snowflake.snowpark import DataFrame, Row, Session
from snowflake.snowpark.functions import (  # abs,; asc,; contains,; count,; date_format,; desc,; is_null,; min,
    col,
    max,
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
