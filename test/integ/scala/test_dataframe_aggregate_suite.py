#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from decimal import Decimal
from math import sqrt
from test.utils import TestData

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    count_distinct,
    lit,
    max,
    mean,
    median,
    min,
    stddev,
    stddev_pop,
    stddev_samp,
    sum,
    sum_distinct,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException


def test_limit_plus_aggregates(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([["a", 1], ["b", 2], ["c", 1], ["d", 5]]).toDF(
            ["id", "value"]
        )
        limit2df = df.limit(2)
        res1 = limit2df.groupBy("id").count().select(col("id")).collect()
        res1.sort(key=lambda x: x[0])

        res2 = limit2df.select(col("id")).collect()
        res2.sort(key=lambda x: x[0])

        assert res1 == res2


def test_rel_grouped_dataframe_agg(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = (
            session.createDataFrame([[1, "One"], [2, "Two"], [3, "Three"]])
            .toDF(["empid", "name"])
            .groupBy()
        )

        # Agg() on 1 column
        assert df.agg(max(col("empid"))).collect() == [Row([3])]
        assert df.agg([min(col("empid"))]).collect() == [Row([1])]
        assert df.agg([(col("empid"), "max")]).collect() == [Row([3])]
        assert df.agg([(col("empid"), "avg")]).collect() == [Row([2.0])]

        # Agg() on 2 columns
        assert df.agg([max(col("empid")), max(col("name"))]).collect() == [
            Row([3, "Two"])
        ]
        assert df.agg([min(col("empid")), min(col("name"))]).collect() == [
            Row([1, "One"])
        ]
        assert df.agg([(col("empid"), "max"), (col("name"), "max")]).collect() == [
            Row([3, "Two"])
        ]
        assert df.agg([(col("empid"), "min"), (col("name"), "min")]).collect() == [
            Row([1, "One"])
        ]


def test_rel_grouped_dataframe_max(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame(
            [("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e")]
        ).toDF(["key", "value1", "value2", "rest"])

        # below 2 ways to call max() must return the same result.
        expected = [Row(["a", 3, 33]), Row(["b", 4, 44])]
        assert (
            df1.groupBy("key").max(col("value1"), col("value2")).collect() == expected
        )
        assert (
            df1.groupBy("key").agg([max(col("value1")), max(col("value2"))]).collect()
            == expected
        )


def test_rel_grouped_dataframe_avg_mean(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame(
            [("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e")]
        ).toDF(["key", "value1", "value2", "rest"])

        expected = [Row(["a", 2.0, 22.0]), Row(["b", 3, 33.0])]
        assert (
            df1.groupBy("key").avg(col("value1"), col("value2")).collect() == expected
        )
        assert (
            df1.groupBy("key").agg([avg(col("value1")), avg(col("value2"))]).collect()
            == expected
        )
        # Same results for mean()
        assert (
            df1.groupBy("key").mean(col("value1"), col("value2")).collect() == expected
        )
        assert (
            df1.groupBy("key").agg([mean(col("value1")), mean(col("value2"))]).collect()
            == expected
        )


def test_rel_grouped_dataframe_median(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame(
            [
                ("a", 1, 11, "b"),
                ("b", 2, 22, "c"),
                ("a", 3, 33, "d"),
                ("b", 4, 44, "e"),
                ("b", 4, 44, "f"),
            ]
        ).toDF(["key", "value1", "value2", "rest"])

        expected = [Row(["a", 2.0, 22.0]), Row(["b", 4, 44.0])]
        assert (
            df1.groupBy("key").median(col("value1"), col("value2")).collect()
            == expected
        )
        assert (
            df1.groupBy("key")
            .agg([median(col("value1")), median(col("value2"))])
            .collect()
            == expected
        )


def test_builtin_functions(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([(1, 11), (2, 12), (1, 13)]).toDF(["a", "b"])

        assert df.groupBy("a").builtin("max")(col("a"), col("b")).collect() == [
            Row([1, 1, 13]),
            Row([2, 2, 12]),
        ]
        assert df.groupBy("a").builtin("max")(col("b")).collect() == [
            Row([1, 13]),
            Row([2, 12]),
        ]


def test_non_empty_arg_functions(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        with pytest.raises(SnowparkClientException) as ex_info:
            TestData.integer1(session).groupBy("a").avg()
        assert "the argument of avg function can't be empty" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            TestData.integer1(session).groupBy("a").sum()
        assert "the argument of sum function can't be empty" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            TestData.integer1(session).groupBy("a").median()
        assert "the argument of median function can't be empty" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            TestData.integer1(session).groupBy("a").min()
        assert "the argument of min function can't be empty" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            TestData.integer1(session).groupBy("a").max()
        assert "the argument of max function can't be empty" in str(ex_info)


def test_null_count(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        assert TestData.test_data3(session).groupBy("a").agg(
            count(col("b"))
        ).collect() == [Row([1, 0]), Row([2, 1])]

        assert TestData.test_data3(session).groupBy("a").agg(
            count(col("a") + col("b"))
        ).collect() == [Row([1, 0]), Row([2, 1])]

        assert (
            TestData.test_data3(session)
            .agg(
                [
                    count(col("a")),
                    count(col("b")),
                    count(lit(1)),
                    count_distinct(col("a")),
                    count_distinct(col("b")),
                ]
            )
            .collect()
            == [Row([2, 1, 2, 2, 1])]
        )

        assert TestData.test_data3(session).agg(
            [count(col("b")), count_distinct(col("b")), sum_distinct(col("b"))]
        ).collect() == [Row([1, 1, 2])]


def test_distinct(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame(
            [(1, "one", 1.0), (2, "one", 2.0), (2, "two", 1.0)]
        ).toDF("i", "s", '"i"')

        assert df.distinct().collect() == [
            Row([1, "one", 1.0]),
            Row([2, "one", 2.0]),
            Row([2, "two", 1.0]),
        ]
        assert df.select("i").distinct().collect() == [Row(1), Row(2)]
        assert df.select('"i"').distinct().collect() == [Row(1), Row(2)]
        assert df.select("s").distinct().collect() == [Row(["one"]), Row(["two"])]

        res = df.select(["i", '"i"']).distinct().collect()
        res.sort(key=lambda x: (x[0], x[1]))
        assert res == [Row([1, 1.0]), Row([2, 1.0]), Row([2, 2.0])]

        res = df.select(["s", '"i"']).distinct().collect()
        res.sort(key=lambda x: (x[1], x[0]))
        assert res == [Row(["one", 1.0]), Row(["two", 1.0]), Row(["one", 2.0])]
        assert df.filter(col("i") < 0).distinct().collect() == []


def test_distinct_and_joins(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        lhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
            "i", "s", '"i"'
        )
        rhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
            "i", "s", '"i"'
        )

        res = lhs.join(rhs, lhs["i"] == rhs["i"]).distinct().collect()
        res.sort(key=lambda x: x[0])
        assert res == [
            Row([1, "one", 1.0, 1, "one", 1.0]),
            Row([2, "one", 2.0, 2, "one", 2.0]),
        ]

        lhsD = lhs.select(col("s")).distinct()
        res = lhsD.join(rhs, lhsD["s"] == rhs["s"]).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(["one", 1, "one", 1.0]), Row(["one", 2, "one", 2.0])]

        rhsD = rhs.select(col("s"))
        res = lhsD.join(rhsD, lhsD["s"] == rhsD["s"]).collect()
        assert res == [Row(["one", "one"]), Row(["one", "one"])]

        rhsD = rhs.select(col("s")).distinct()
        res = lhsD.join(rhsD, lhsD["s"] == rhsD["s"]).collect()
        assert res == [Row(["one", "one"])]


def test_groupBy(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        assert TestData.test_data2(session).groupBy("a").agg(
            sum(col("b"))
        ).collect() == [Row([1, 3]), Row([2, 3]), Row([3, 3])]

        assert TestData.test_data2(session).groupBy("a").agg(
            sum(col("b")).as_("totB")
        ).agg(sum(col("totB"))).collect() == [Row([9])]

        assert TestData.test_data2(session).groupBy("a").agg(
            count(col("*"))
        ).collect() == [Row([1, 2]), Row([2, 2]), Row([3, 2])]

        assert TestData.test_data2(session).groupBy("a").agg(
            [(col("*"), "count")]
        ).collect() == [Row([1, 2]), Row([2, 2]), Row([3, 2])]

        assert TestData.test_data2(session).groupBy("a").agg(
            [(col("b"), "sum")]
        ).collect() == [Row([1, 3]), Row([2, 3]), Row([3, 3])]

        df1 = session.createDataFrame(
            [("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d")]
        ).toDF(["key", "value1", "value2", "rest"])

        assert df1.groupBy("key").min(col("value2")).collect() == [
            Row(["a", 0]),
            Row(["b", 4]),
        ]

        assert TestData.decimal_data(session).groupBy("a").agg(
            sum(col("b"))
        ).collect() == [
            Row([Decimal(1), Decimal(3)]),
            Row([Decimal(2), Decimal(3)]),
            Row([Decimal(3), Decimal(3)]),
        ]


def test_agg_should_be_order_preserving(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = (
            session.range(2)
            .groupBy("id")
            .agg([(col("id"), "sum"), (col("id"), "count"), (col("id"), "min")])
        )

        assert [f.name for f in df.schema.fields] == [
            "ID",
            '"SUM(ID)"',
            '"COUNT(ID)"',
            '"MIN(ID)"',
        ]
        assert df.collect() == [Row([0, 0, 1, 0]), Row([1, 1, 1, 1])]


def test_count(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        assert TestData.test_data2(session).agg(
            [count(col("a")), sum_distinct(col("a"))]
        ).collect() == [Row([6, 6.0])]


def test_stddev(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        test_data_dev = sqrt(4 / 5)

        assert TestData.test_data2(session).agg(
            [stddev(col("a")), stddev_pop(col("a")), stddev_samp(col("a"))]
        ).collect() == [Row([test_data_dev, 0.8164967850518458, test_data_dev])]


def test_spark14664_decimal_sum_over_window_should_work(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        assert session.sql(
            "select sum(a) over () from values (1.0), (2.0), (3.0) T(a)"
        ).collect() == [Row([6.0]), Row([6.0]), Row([6.0])]
        assert session.sql(
            "select avg(a) over () from values (1.0), (2.0), (3.0) T(a)"
        ).collect() == [Row([2.0]), Row([2.0]), Row([2.0])]


def test_aggregate_function_in_groupby(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        with pytest.raises(ProgrammingError) as ex_info:
            TestData.test_data4(session).groupBy(sum(col('"KEY"'))).count().collect()
        assert "is not a valid group by expression" in str(ex_info)


def test_spark21580_ints_in_agg_exprs_are_taken_as_groupby_ordinal(
    session_cnx, db_parameters
):
    with session_cnx(db_parameters) as session:
        assert TestData.test_data2(session).groupBy(lit(3), lit(4)).agg(
            [lit(6), lit(7), sum(col("b"))]
        ).collect() == [Row([3, 4, 6, 7, 9])]

        assert TestData.test_data2(session).groupBy([lit(3), lit(4)]).agg(
            [lit(6), col("b"), sum(col("b"))]
        ).collect() == [Row([3, 4, 6, 1, 3]), Row([3, 4, 6, 2, 6])]

        testdata2str = (
            "(SELECT * FROM VALUES (1,1),(1,2),(2,1),(2,2),(3,1),(3,2) T(a, b) )"
        )
        assert session.sql(
            f"SELECT 3, 4, SUM(b) FROM {testdata2str} GROUP BY 1, 2"
        ).collect() == [Row([3, 4, 9])]

        assert session.sql(
            f"SELECT 3 AS c, 4 AS d, SUM(b) FROM {testdata2str} GROUP BY c, d"
        ).collect() == [Row([3, 4, 9])]


def test_distinct_and_unions(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        lhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
            "i", "s", '"i"'
        )
        rhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
            "i", "s", '"i"'
        )

        res = lhs.union(rhs).distinct().collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "one", 1.0]), Row([2, "one", 2.0])]

        lhsD = lhs.select(col("s")).distinct()
        rhs = rhs.select(col("s"))
        res = lhsD.union(rhs).collect()
        assert res == [Row("one"), Row("one"), Row("one")]

        lhs = lhs.select(col("s"))
        rhsD = rhs.select("s").distinct()

        res = lhs.union(rhsD).collect()
        assert res == [Row("one"), Row("one"), Row("one")]

        res = lhsD.union(rhsD).collect()
        assert res == [Row("one"), Row("one")]


def test_count_if(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        session.createDataFrame(
            [["a", None], ["a", 1], ["a", 2], ["a", 3],
             ["b", None], ["b", 4], ["b", 5], ["b", 6]]
        ).toDF("x", "y").createOrReplaceTempView("tempView")

        res = session.sql("SELECT COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), COUNT_IF(y IS NULL) FROM tempView").collect()
        assert res == [Row([0, 3, 3, 2])]

        res = session.sql("SELECT x, COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), COUNT_IF(y IS NULL) FROM tempView GROUP BY x").collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(["a", 0, 1, 2, 1]), Row(["b", 0, 2, 1, 1])]

        res = session.sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 1").collect()
        assert res == [Row(["a"])]

        res = session.sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 2").collect()
        assert res == [Row(["b"])]

        res = session.sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y IS NULL) > 0").collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(["a"]), Row(["b"])]

        res = session.sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(NULL) > 0").collect()
        assert res == []

        with pytest.raises(ProgrammingError) as ex_info:
            session.sql("SELECT COUNT_IF(x) FROM tempView").collect()


def test_agg_without_groups(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        assert TestData.test_data2(session).agg(sum(col("b"))).collect() == [Row([9])]


def test_agg_without_groups_and_functions(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        assert TestData.test_data2(session).agg(lit(1)).collect() == [Row([1])]


def test_null_average(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        assert TestData.test_data3(session).agg(avg(col("b"))).collect() == [Row([2.0])]

        assert TestData.test_data3(session).agg(
            [avg(col("b")), count_distinct(col("b"))]
        ).collect() == [Row([2.0, 1])]

        assert TestData.test_data3(session).agg(
            [avg(col("b")), sum_distinct(col("b"))]
        ).collect() == [Row([2.0, 2.0])]


def test_zero_average(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[]]).toDF(["a"])
        assert df.agg(avg(col("a"))).collect() == [Row([None])]

        assert df.agg([avg(col("a")), sum_distinct(col("a"))]).collect() == [
            Row([None, None])
        ]


def test_multiple_column_distinct_count(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df1 = session.createDataFrame(
            [
                ("a", "b", "c"),
                ("a", "b", "c"),
                ("a", "b", "d"),
                ("x", "y", "z"),
                ("x", "q", None),
            ]
        ).toDF("key1", "key2", "key3")

        res = df1.agg(count_distinct(col("key1"), col("key2"))).collect()
        assert res == [Row(3)]

        res = df1.agg(count_distinct(col("key1"), col("key2"), col("key3"))).collect()
        assert res == [Row(3)]

        res = (
            df1.groupBy(col("key1"))
            .agg(count_distinct(col("key2"), col("key3")))
            .collect()
        )
        res.sort(key=lambda x: x[0])
        assert res == [Row(["a", 2]), Row(["x", 1])]


def test_zero_count(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        empty_table = session.createDataFrame([[]]).toDF(["a"])
        assert empty_table.agg([count(col("a")), sum_distinct(col("a"))]).collect() == [
            Row([0, None])
        ]


def test_zero_stddev(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[]]).toDF(["a"])
        assert df.agg(
            [stddev(col("a")), stddev_pop(col("a")), stddev_samp(col("a"))]
        ).collect() == [Row([None, None, None])]


def test_zero_sum(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[]]).toDF(["a"])
        assert df.agg([sum(col("a"))]).collect() == [Row([None])]


def test_zero_sum_distinct(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[]]).toDF(["a"])
        assert df.agg([sum_distinct(col("a"))]).collect() == [Row([None])]
