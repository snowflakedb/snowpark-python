#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.functions import col, when
from snowflake.snowpark.row import Row


def test_casewhen_with_non_zero_row_index(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    assert df.filter(col("a") > 1).select(
        when(col("a").is_null(), 5).when(col("a") == 1, 6).otherwise(7).as_("a")
    ).collect() == [Row(A=7)]


def test_like_with_non_zero_row_index(session):
    df = session.create_dataframe([["1", 2], ["3", 4]], schema=["a", "b"])
    assert df.filter(col("b") > 2).select(
        col("a").like("1").alias("res")
    ).collect() == [Row(RES=False)]
