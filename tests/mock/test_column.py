#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.functions import col, when, array_agg
from snowflake.snowpark.row import Row
from tests.utils import Utils


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


def test_like_with_several_rows(session):
    df = session.create_dataframe(
        [
            [
                "This is an apple",
                "This is an apple",
            ],
            [
                "This is an apple",
                "This is an orange",
            ],
            [
                "This is an apple",
                "This is ",
            ],
            [
                "This is an apple",
                "Station is",
            ],
        ],
        ["A", "B"],
    )

    res = df.with_column("RESULT", col("A").like(col("B")))
    Utils.check_answer(
        res,
        [
            Row("This is an apple", "This is an apple", True),
            Row("This is an apple", "This is an orange", False),
            Row("This is an apple", "This is ", False),
            Row("This is an apple", "Station is", False),
        ],
    )


def test_get_item(session):
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
    get_df = agged.select("ID", col("VALUES").getItem(1).alias("ELEMENT"))
    Utils.check_answer(get_df, [Row(102, None), Row(101, '"dog"')])
