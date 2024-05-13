#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import DataFrame, Row
from tests.utils import Utils


@pytest.mark.localtest
def test_union_basic(session):
    df1: DataFrame = session.create_dataframe(
        [
            [1, 2],
            [3, 4],
        ],
        schema=["a", "b"],
    )

    df2: DataFrame = session.create_dataframe(
        [
            [1, 2],
            [5, 6],
        ],
        schema=["a", "b"],
    )

    df3: DataFrame = session.create_dataframe(
        [
            [5, 6],
            [9, 10],
            [11, 12],
        ],
        schema=["a", "b"],
    )

    Utils.check_answer(
        df1.union(df2).collect(),
        [
            Row(1, 2),
            Row(3, 4),
            Row(5, 6),
        ],
    )

    Utils.check_answer(
        df1.union_all(df2).collect(),
        [
            Row(1, 2),
            Row(3, 4),
            Row(1, 2),
            Row(5, 6),
        ],
    )

    Utils.check_answer(
        df1.union(df2).union(df3).collect(),
        [
            Row(1, 2),
            Row(3, 4),
            Row(5, 6),
            Row(9, 10),
            Row(11, 12),
        ],
    )

    Utils.check_answer(
        df1.union_all(df2).union_all(df3).collect(),
        [
            Row(1, 2),
            Row(3, 4),
            Row(1, 2),
            Row(5, 6),
            Row(5, 6),
            Row(9, 10),
            Row(11, 12),
        ],
    )


@pytest.mark.localtest
def test_union_by_name(session):
    df1: DataFrame = session.create_dataframe(
        [
            [1, 2],
            [3, 4],
        ],
        schema=["a", "b"],
    )

    df2: DataFrame = session.create_dataframe(
        [
            [1, 2],
            [2, 1],
            [5, 6],
        ],
        schema=["b", "a"],
    )
    Utils.check_answer(
        df1.union_by_name(df2).collect(),
        [
            Row(1, 2),
            Row(3, 4),
            Row(2, 1),
            Row(6, 5),
        ],
    )

    Utils.check_answer(
        df1.union_all_by_name(df2).collect(),
        [
            Row(1, 2),
            Row(3, 4),
            Row(2, 1),
            Row(1, 2),
            Row(6, 5),
        ],
    )
