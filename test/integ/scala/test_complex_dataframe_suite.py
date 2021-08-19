#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from snowflake.snowpark.functions import col
from snowflake.snowpark.row import Row


def test_combination_of_multiple_operators(session_cnx):
    with session_cnx() as session:
        df1 = session.createDataFrame([1, 2]).toDF("a")
        df2 = session.createDataFrame([[i, f"test{i}"] for i in [1, 2]]).toDF("a", "b")

        assert df1.join(df2, "a").except_(df2).collect() == []

        res = df1.join(df2, "a").intersect(df2).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "test1"]), Row([2, "test2"])]

        res1 = df1.join(df2, "a").collect()
        res1.sort(key=lambda x: x[0])
        res2 = df2.filter(col("a") < 2).union(df2.filter(col("a") >= 2)).collect()
        res2.sort(key=lambda x: x[0])
        assert res1 == res2

        res = (
            df1.join(df2, "a")
            .union(df2.filter(col("a") < 2).union(df2.filter(col("a") >= 2)))
            .collect()
        )
        res.sort(key=lambda x: x[0])
        assert res == [
            Row([1, "test1"]),
            Row([1, "test1"]),
            Row([2, "test2"]),
            Row([2, "test2"]),
        ]


def test_combination_of_multiple_operators_with_filters(session_cnx):
    with session_cnx() as session:
        df1 = session.createDataFrame([i for i in range(1, 11)]).toDF("a")
        df2 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 11)]).toDF(
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
        assert df.collect() == [Row([i, f"test{i}"]) for i in range(1, 11)]


def test_join_on_top_of_unions(session_cnx):
    with session_cnx() as session:
        df1 = session.createDataFrame([i for i in range(1, 6)]).toDF("a")
        df2 = session.createDataFrame([i for i in range(6, 11)]).toDF("a")
        df3 = session.createDataFrame([[i, f"test{i}"] for i in range(1, 6)]).toDF(
            "a", "b"
        )
        df4 = session.createDataFrame([[i, f"test{i}"] for i in range(6, 11)]).toDF(
            "a", "b"
        )

        res = df1.union(df2).join(df3.union(df4), "a").collect()
        # don't sort
        assert res == [Row([i, f"test{i}"]) for i in range(1, 11)]
