#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# pandas 3 raises TypeError for a value that does not fit a column's dtype; pandas 2 widens it.

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, lit, sqrt, to_variant, when, when_matched


def test_update_sets_null_on_expression_column(session):
    session.create_dataframe([[1], [4]], schema=["a"]).select(
        col("a"), (col("a") > 0).alias("flag")
    ).write.save_as_table("t", mode="overwrite")
    table = session.table("t")
    table.update({"flag": lit(None)}, col("a") == 4)
    assert table.sort("a").collect() == [Row(1, True), Row(4, None)]


def test_merge_update_sets_null_on_expression_column(session):
    session.create_dataframe([[1], [4]], schema=["a"]).select(
        col("a"), (col("a") > 0).alias("flag")
    ).write.save_as_table("t", mode="overwrite")
    target = session.table("t")
    source = session.create_dataframe([[4]], schema=["a"])
    target.merge(
        source,
        target["a"] == source["a"],
        [when_matched().update({"flag": lit(None)})],
    )
    assert target.sort("a").collect() == [Row(1, True), Row(4, None)]


def test_case_when_mixes_integer_float_and_null(session):
    df = session.create_dataframe([[1], [2], [3]], schema=["a"])
    result = df.select(
        when(col("a") == 1, lit(1))
        .when(col("a") == 2, lit(2.5))
        .otherwise(lit(None))
        .alias("c")
    ).collect()
    assert result == [Row(1.0), Row(2.5), Row(None)]


def test_to_variant_of_float_expression(session):
    df = session.create_dataframe([[4], [9]], schema=["a"])
    assert df.select(to_variant(sqrt(col("a")))).collect() == [Row("2.0"), Row("3.0")]
