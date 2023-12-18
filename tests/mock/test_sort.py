#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import DataFrame, Row, Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.mock._connection import MockServerConnection
from tests.utils import Utils

session = Session(MockServerConnection())


@pytest.mark.localtest
def test_sort_single_column():
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
    assert origin_df.sort(col("v")).collect() == expected
    expected.reverse()
    assert origin_df.sort(col("v").desc()).collect() == expected

    origin_df: DataFrame = session.create_dataframe(
        [[1.0], [8.0], [6.0], [None], [3.0], [100.0], [400.0], [float("nan")]],
        schema=["v"],
    )
    expected_null_first = [
        Row(None),
        Row(1.0),
        Row(3.0),
        Row(6.0),
        Row(8.0),
        Row(100.0),
        Row(400.0),
        Row(float("nan")),
    ]
    expected_null_last = [
        Row(1),
        Row(3),
        Row(6),
        Row(8),
        Row(100),
        Row(400),
        Row(float("nan")),
        Row(None),
    ]
    Utils.check_answer(
        origin_df.sort(col("v").asc_nulls_first()).collect(),
        expected_null_first,
        sort=False,
    )
    Utils.check_answer(
        origin_df.sort(col("v").asc_nulls_last()).collect(),
        expected_null_last,
        sort=False,
    )


@pytest.mark.localtest
def test_sort_multiple_column():
    origin_df: DataFrame = session.create_dataframe(
        [
            [1.0, 2.3],
            [8.0, 1.9],
            [None, 7.8],
            [3.0, 5.6],
            [3.0, 4.7],
            [3.0, None],
            [float("nan"), 0.9],
        ],
        schema=["m", "n"],
    )
    Utils.check_answer(
        origin_df.sort(col("m")).collect(),
        [
            Row(None, 7.8),
            Row(1.0, 2.3),
            Row(3.0, 5.6),
            Row(3.0, 4.7),
            Row(3.0, None),
            Row(8.0, 1.9),
            Row(float("nan"), 0.9),
        ],
        sort=False,
    )
    Utils.check_answer(
        origin_df.sort(col("m").asc_nulls_last()).collect(),
        [
            Row(1.0, 2.3),
            Row(3.0, 5.6),
            Row(3.0, 4.7),
            Row(3.0, None),
            Row(8.0, 1.9),
            Row(float("nan"), 0.9),
            Row(None, 7.8),
        ],
        sort=False,
    )
    Utils.check_answer(
        origin_df.sort(col("m").desc_nulls_first()).collect(),
        [
            Row(None, 7.8),
            Row(float("nan"), 0.9),
            Row(8.0, 1.9),
            Row(3.0, 5.6),
            Row(3.0, 4.7),
            Row(3.0, None),
            Row(1.0, 2.3),
        ],
        sort=False,
    )
    Utils.check_answer(
        origin_df.sort(col("m").desc_nulls_last()).collect(),
        [
            Row(float("nan"), 0.9),
            Row(8.0, 1.9),
            Row(3.0, 5.6),
            Row(3.0, 4.7),
            Row(3.0, None),
            Row(1.0, 2.3),
            Row(None, 7.8),
        ],
        sort=False,
    )

    Utils.check_answer(
        origin_df.sort([col("m"), col("n")], ascending=[1, 1]).collect(),
        [
            Row(None, 7.8),
            Row(1.0, 2.3),
            Row(3.0, None),
            Row(3.0, 4.7),
            Row(3.0, 5.6),
            Row(8.0, 1.9),
            Row(float("nan"), 0.9),
        ],
        sort=False,
    )

    Utils.check_answer(
        origin_df.sort([col("m"), col("n")], ascending=[True, False]).collect(),
        [
            Row(None, 7.8),
            Row(1.0, 2.3),
            Row(3.0, 5.6),
            Row(3.0, 4.7),
            Row(3.0, None),
            Row(8.0, 1.9),
            Row(float("nan"), 0.9),
        ],
        sort=False,
    )

    Utils.check_answer(
        origin_df.sort([col("m"), col("n").desc_nulls_first()]).collect(),
        [
            Row(None, 7.8),
            Row(1.0, 2.3),
            Row(3.0, None),
            Row(3.0, 5.6),
            Row(3.0, 4.7),
            Row(8.0, 1.9),
            Row(float("nan"), 0.9),
        ],
        sort=False,
    )
