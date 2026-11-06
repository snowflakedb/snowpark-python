#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# pandas 3 infers the str dtype for string columns; NULL must still come back as None.

import pandas as pd

from snowflake.snowpark import Row
from snowflake.snowpark.functions import (
    col,
    sum_distinct,
    to_char,
    udf,
    when_not_matched,
)
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def test_udf_receives_none_for_null(session):
    concat = udf(
        lambda x, y: f"{x}-{y}",
        return_type=StringType(),
        input_types=[StringType(), StringType()],
    )
    df = session.create_dataframe([["a", None], [None, "c"]], schema=["x", "y"])
    assert df.select(concat("x", "y")).collect() == [Row("a-None"), Row("None-c")]


def test_pivot_fills_missing_cells_with_none(session):
    df = session.create_dataframe(
        [["k1", "a", "x"], ["k2", "b", "y"]], schema=["key", "pivot", "value"]
    )
    result = (
        df.group_by("key").pivot("pivot", ["a", "b"]).min("value").sort("key").collect()
    )
    assert result == [Row("k1", "x", None), Row("k2", None, "y")]


def test_sum_distinct_skips_null_strings(session):
    df = session.create_dataframe([["1"], ["2"], ["2"], [None]], schema=["a"])
    assert df.select(sum_distinct(col("a"))).collect() == [Row(3)]


def test_apply_in_pandas_output_keeps_null(session):
    def handler(pdf):
        return pd.DataFrame({"O": ["x", None]})

    out = (
        session.create_dataframe([[1]], schema=["k"])
        .group_by("k")
        .apply_in_pandas(
            handler, output_schema=StructType([StructField("O", StringType())])
        )
    )
    assert out.filter(col("O").is_null()).count() == 1


def test_merge_insert_sets_unspecified_column_to_null(session):
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=True),
            StructField("name", StringType(), nullable=True),
        ]
    )
    session.create_dataframe([[1, "a"]], schema=schema).write.save_as_table(
        "target", mode="overwrite"
    )
    target = session.table("target")
    source = session.create_dataframe([[7]], schema=["id"])
    target.merge(
        source,
        target["id"] == source["id"],
        [when_not_matched().insert({"id": source["id"]})],
    )
    assert target.sort("id").collect() == [Row(1, "a"), Row(7, None)]
    assert target.filter(col("name").is_null()).count() == 1


def test_sort_strings_with_null(session):
    df = session.create_dataframe([["b"], [None], ["a"], ["c"]], schema=["s"])
    assert df.sort(col("s").asc_nulls_first()).collect() == [
        Row(None),
        Row("a"),
        Row("b"),
        Row("c"),
    ]
    assert df.sort(col("s").desc_nulls_last()).collect() == [
        Row("c"),
        Row("b"),
        Row("a"),
        Row(None),
    ]


def test_function_result_built_by_pandas_keeps_null(session):
    df = session.create_dataframe([[True], [None]], schema=["b"])
    assert df.select(to_char(col("b"))).collect() == [Row("true"), Row(None)]
