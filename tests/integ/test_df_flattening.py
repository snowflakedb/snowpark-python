#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.functions import col, pow, rank
from snowflake.snowpark.window import Window


def test_order_by_with_mixed_columns(session):
    df = session.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
    result = df.select(pow(2, "c").alias("d")).orderBy(col("c"), col("d")).collect()
    assert result == [(8,), (64,)]


def test_order_by_with_mixed_columns_and_window_function(session):
    df = session.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
    result = (
        df.select(rank().over(Window.order_by(col("c"))).alias("rank"))
        .orderBy(col("c"), col("rank"))
        .collect()
    )
    assert result == [(1,), (2,)]


def test_filter_with_mixed_columns(session):
    df = session.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
    result = df.select(pow(2, "c").alias("d")).filter(col("c") > col("d")).collect()
    assert result == []


def test_filter_and_order_by_with_mixed_columns(session):
    df = session.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
    filter_first = (
        df.select(pow(2, "c").alias("d"))
        .filter(col("c") < col("d"))
        .orderBy(col("c"), col("d"))
        .collect()
    )
    assert filter_first == [(8,), (64,)]

    order_first = (
        df.select(pow(2, "c").alias("d"))
        .orderBy(col("c"), col("d"))
        .filter(col("c") < col("d"))
        .collect()
    )
    assert order_first == [(8,), (64,)]
