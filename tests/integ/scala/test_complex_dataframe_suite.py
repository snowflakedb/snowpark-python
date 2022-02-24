#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import TestFiles, Utils


def test_combination_of_multiple_operators(session):
    df1 = session.create_dataframe([1, 2]).to_df("a")
    df2 = session.create_dataframe([[i, f"test{i}"] for i in [1, 2]]).to_df("a", "b")

    assert df1.join(df2, "a").except_(df2).collect() == []

    res = df1.join(df2, "a").intersect(df2).collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "test1"), Row(2, "test2")]

    res1 = df1.join(df2, "a").collect()
    res1.sort(key=lambda x: x[0])
    res2 = df2.filter(col("a") < 2).union(df2.filter(col("a") >= 2)).collect()
    res2.sort(key=lambda x: x[0])
    assert res1 == res2

    res = (
        df1.join(df2, "a")
        .union_all(df2.filter(col("a") < 2).union_all(df2.filter(col("a") >= 2)))
        .collect()
    )
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "test1"),
        Row(1, "test1"),
        Row(2, "test2"),
        Row(2, "test2"),
    ]


def test_combination_of_multiple_operators_with_filters(session):
    df1 = session.create_dataframe([i for i in range(1, 11)]).to_df("a")
    df2 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 11)]).to_df(
        "a", "b"
    )

    assert (
        df1.filter(col("a") < 6)
        .join(df2, "a")
        .intersect(df2.filter(col("a") > 5))
        .collect()
        == []
    )

    res1 = df1.filter(col("a") < 6).join(df2, "a", "left_semi").collect()
    res1.sort(key=lambda x: x[0])
    res2 = df1.filter(col("a") < 6).collect()
    res2.sort(key=lambda x: x[0])
    assert res1 == res2

    assert df1.filter(col("a") < 6).join(df2, ["a"], "left_anti").collect() == []

    df = df1.filter(col("a") < 6).join(df2, "a").union(df2.filter(col("a") > 5))
    # don't sort
    assert df.collect() == [Row(i, f"test{i}") for i in range(1, 11)]


def test_join_on_top_of_unions(session):
    df1 = session.create_dataframe([i for i in range(1, 6)]).to_df("a")
    df2 = session.create_dataframe([i for i in range(6, 11)]).to_df("a")
    df3 = session.create_dataframe([[i, f"test{i}"] for i in range(1, 6)]).to_df(
        "a", "b"
    )
    df4 = session.create_dataframe([[i, f"test{i}"] for i in range(6, 11)]).to_df(
        "a", "b"
    )

    res = df1.union(df2).join(df3.union(df4), "a").sort(col("a")).collect()
    assert res == [Row(i, f"test{i}") for i in range(1, 11)]


def test_combination_of_multiple_data_sources(session, resources_path):
    test_files = TestFiles(resources_path)
    test_file_csv = "testCSV.csv"
    tmp_stage_name = Utils.random_stage_name()
    tmp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    user_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
        ]
    )

    try:
        Utils.create_table(session, tmp_table_name, "num int")
        session.sql(f"insert into {tmp_table_name} values(1),(2),(3)").collect()
        session.sql(f"CREATE TEMPORARY STAGE {tmp_stage_name}").collect()
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name, test_files.test_file_csv, compress=False
        )

        test_file_on_stage = f"@{tmp_stage_name}/{test_file_csv}"
        df1 = session.read.schema(user_schema).csv(test_file_on_stage)
        df2 = session.table(tmp_table_name)

        Utils.check_answer(
            df2.join(df1, df1["a"] == df2["num"]),
            [Row(1, 1, "one", 1.2), Row(2, 2, "two", 2.2)],
        )

        Utils.check_answer(
            df2.filter(col("num") == 1).join(
                df1.select("a", "b"), df1["a"] == df2["num"]
            ),
            [Row(1, 1, "one")],
        )

    finally:
        Utils.drop_table(session, tmp_table_name)
        Utils.drop_stage(session, tmp_stage_name)
