#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from decimal import Decimal
from math import sqrt
from test.utils import TestData, Utils

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    count_distinct,
    kurtosis,
    lit,
    max,
    mean,
    median,
    min,
    skew,
    sql_expr,
    stddev,
    stddev_pop,
    stddev_samp,
    sum,
    sum_distinct,
    var_pop,
    var_samp,
    variance,
)


def test_limit_plus_aggregates(session):
    df = session.createDataFrame([["a", 1], ["b", 2], ["c", 1], ["d", 5]]).toDF(
        ["id", "value"]
    )
    limit2df = df.limit(2)
    res1 = limit2df.groupBy("id").count().select(col("id")).collect()
    res1.sort(key=lambda x: x[0])

    res2 = limit2df.select(col("id")).collect()
    res2.sort(key=lambda x: x[0])

    assert res1 == res2


def test_rel_grouped_dataframe_agg(session):
    df = (
        session.createDataFrame([[1, "One"], [2, "Two"], [3, "Three"]])
        .toDF(["empid", "name"])
        .groupBy()
    )

    # Agg() on 1 column
    assert df.agg(max(col("empid"))).collect() == [Row(3)]
    assert df.agg([min(col("empid"))]).collect() == [Row(1)]
    assert df.agg([(col("empid"), "max")]).collect() == [Row(3)]
    assert df.agg([(col("empid"), "avg")]).collect() == [Row(2.0)]

    # Agg() on 2 columns
    assert df.agg([max(col("empid")), max(col("name"))]).collect() == [Row(3, "Two")]
    assert df.agg([min(col("empid")), min(col("name"))]).collect() == [Row(1, "One")]
    assert df.agg([(col("empid"), "max"), (col("name"), "max")]).collect() == [
        Row(3, "Two")
    ]
    assert df.agg([(col("empid"), "min"), (col("name"), "min")]).collect() == [
        Row(1, "One")
    ]


def test_rel_grouped_dataframe_max(session):
    df1 = session.createDataFrame(
        [("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e")]
    ).toDF(["key", "value1", "value2", "rest"])

    # below 2 ways to call max() must return the same result.
    expected = [Row("a", 3, 33), Row("b", 4, 44)]
    assert df1.groupBy("key").max(col("value1"), col("value2")).collect() == expected
    assert (
        df1.groupBy("key").agg([max(col("value1")), max(col("value2"))]).collect()
        == expected
    )

    # same as above, but pass str instead of Column
    assert df1.groupBy("key").max("value1", "value2").collect() == expected
    assert df1.groupBy("key").agg([max("value1"), max("value2")]).collect() == expected


def test_rel_grouped_dataframe_avg_mean(session):
    df1 = session.createDataFrame(
        [("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e")]
    ).toDF(["key", "value1", "value2", "rest"])

    expected = [Row("a", 2.0, 22.0), Row("b", 3, 33.0)]
    assert df1.groupBy("key").avg(col("value1"), col("value2")).collect() == expected
    assert (
        df1.groupBy("key").agg([avg(col("value1")), avg(col("value2"))]).collect()
        == expected
    )
    # Same results for mean()
    assert df1.groupBy("key").mean(col("value1"), col("value2")).collect() == expected
    assert (
        df1.groupBy("key").agg([mean(col("value1")), mean(col("value2"))]).collect()
        == expected
    )

    # same as above, but pass str instead of Column
    assert df1.groupBy("key").avg("value1", "value2").collect() == expected
    assert df1.groupBy("key").agg([avg("value1"), avg("value2")]).collect() == expected
    # Same results for mean()
    assert df1.groupBy("key").mean("value1", "value2").collect() == expected
    assert (
        df1.groupBy("key").agg([mean("value1"), mean("value2")]).collect() == expected
    )


def test_rel_grouped_dataframe_median(session):
    df1 = session.createDataFrame(
        [
            ("a", 1, 11, "b"),
            ("b", 2, 22, "c"),
            ("a", 3, 33, "d"),
            ("b", 4, 44, "e"),
            ("b", 4, 44, "f"),
        ]
    ).toDF(["key", "value1", "value2", "rest"])

    # call median without groupb-y
    Utils.check_answer(
        df1.select(median(col("value1")), median(col("value2"))), [Row(3.0, 33.0)]
    )

    expected = [Row("a", 2.0, 22.0), Row("b", 4, 44.0)]
    assert df1.groupBy("key").median(col("value1"), col("value2")).collect() == expected
    assert (
        df1.groupBy("key").agg([median(col("value1")), median(col("value2"))]).collect()
        == expected
    )
    # same as above, but pass str instead of Column
    assert df1.groupBy("key").median("value1", "value2").collect() == expected
    assert (
        df1.groupBy("key").agg([median("value1"), median("value2")]).collect()
        == expected
    )


def test_builtin_functions(session):
    df = session.createDataFrame([(1, 11), (2, 12), (1, 13)]).toDF(["a", "b"])

    assert df.groupBy("a").builtin("max")(col("a"), col("b")).collect() == [
        Row(1, 1, 13),
        Row(2, 2, 12),
    ]
    assert df.groupBy("a").builtin("max")(col("b")).collect() == [
        Row(1, 13),
        Row(2, 12),
    ]


def test_non_empty_arg_functions(session):
    func_name = "avg"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).groupBy("a").avg()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )

    func_name = "sum"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).groupBy("a").sum()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )

    func_name = "median"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).groupBy("a").median()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )

    func_name = "min"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).groupBy("a").min()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )

    func_name = "max"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).groupBy("a").max()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )


def test_null_count(session):
    assert TestData.test_data3(session).groupBy("a").agg(count(col("b"))).collect() == [
        Row(1, 0),
        Row(2, 1),
    ]

    assert TestData.test_data3(session).groupBy("a").agg(
        count(col("a") + col("b"))
    ).collect() == [Row(1, 0), Row(2, 1)]

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
        == [Row(2, 1, 2, 2, 1)]
    )

    assert TestData.test_data3(session).agg(
        [count(col("b")), count_distinct(col("b")), sum_distinct(col("b"))]
    ).collect() == [Row(1, 1, 2)]


def test_distinct(session):
    df = session.createDataFrame(
        [(1, "one", 1.0), (2, "one", 2.0), (2, "two", 1.0)]
    ).toDF("i", "s", '"i"')

    assert df.distinct().collect() == [
        Row(1, "one", 1.0),
        Row(2, "one", 2.0),
        Row(2, "two", 1.0),
    ]
    assert df.select("i").distinct().collect() == [Row(1), Row(2)]
    assert df.select('"i"').distinct().collect() == [Row(1), Row(2)]
    assert df.select("s").distinct().collect() == [Row("one"), Row("two")]

    res = df.select(["i", '"i"']).distinct().collect()
    res.sort(key=lambda x: (x[0], x[1]))
    assert res == [Row(1, 1.0), Row(2, 1.0), Row(2, 2.0)]

    res = df.select(["s", '"i"']).distinct().collect()
    res.sort(key=lambda x: (x[1], x[0]))
    assert res == [Row("one", 1.0), Row("two", 1.0), Row("one", 2.0)]
    assert df.filter(col("i") < 0).distinct().collect() == []


def test_distinct_and_joins(session):
    lhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
        "i", "s", '"i"'
    )
    rhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
        "i", "s", '"i"'
    )

    res = lhs.join(rhs, lhs["i"] == rhs["i"]).distinct().collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "one", 1.0, 1, "one", 1.0),
        Row(2, "one", 2.0, 2, "one", 2.0),
    ]

    lhsD = lhs.select(col("s")).distinct()
    res = lhsD.join(rhs, lhsD["s"] == rhs["s"]).collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row("one", 1, "one", 1.0), Row("one", 2, "one", 2.0)]

    rhsD = rhs.select(col("s"))
    res = lhsD.join(rhsD, lhsD["s"] == rhsD["s"]).collect()
    assert res == [Row("one", "one"), Row("one", "one")]

    rhsD = rhs.select(col("s")).distinct()
    res = lhsD.join(rhsD, lhsD["s"] == rhsD["s"]).collect()
    assert res == [Row("one", "one")]


def test_groupBy(session):
    assert TestData.test_data2(session).groupBy("a").agg(sum(col("b"))).collect() == [
        Row(1, 3),
        Row(2, 3),
        Row(3, 3),
    ]

    assert TestData.test_data2(session).groupBy("a").agg(sum(col("b")).as_("totB")).agg(
        sum(col("totB"))
    ).collect() == [Row(9)]

    assert TestData.test_data2(session).groupBy("a").agg(count(col("*"))).collect() == [
        Row(1, 2),
        Row(2, 2),
        Row(3, 2),
    ]

    assert TestData.test_data2(session).groupBy("a").agg(
        [(col("*"), "count")]
    ).collect() == [Row(1, 2), Row(2, 2), Row(3, 2)]

    assert TestData.test_data2(session).groupBy("a").agg(
        [(col("b"), "sum")]
    ).collect() == [Row(1, 3), Row(2, 3), Row(3, 3)]

    df1 = session.createDataFrame(
        [("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d")]
    ).toDF(["key", "value1", "value2", "rest"])

    assert df1.groupBy("key").min(col("value2")).collect() == [
        Row("a", 0),
        Row("b", 4),
    ]

    # same as above, but pass str instead of Column to min()
    assert df1.groupBy("key").min("value2").collect() == [
        Row("a", 0),
        Row("b", 4),
    ]

    assert TestData.decimal_data(session).groupBy("a").agg(sum(col("b"))).collect() == [
        Row(Decimal(1), Decimal(3)),
        Row(Decimal(2), Decimal(3)),
        Row(Decimal(3), Decimal(3)),
    ]


def test_agg_should_be_order_preserving(session):
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
    assert df.collect() == [Row(0, 0, 1, 0), Row(1, 1, 1, 1)]


def test_count(session):
    assert TestData.test_data2(session).agg(
        [count(col("a")), sum_distinct(col("a"))]
    ).collect() == [Row(6, 6.0)]


def test_stddev(session):
    test_data_dev = sqrt(4 / 5)

    assert TestData.test_data2(session).agg(
        [stddev(col("a")), stddev_pop(col("a")), stddev_samp(col("a"))]
    ).collect() == [Row(test_data_dev, 0.8164967850518458, test_data_dev)]

    # same as above, but pass str instead of Column
    assert TestData.test_data2(session).agg(
        [stddev("a"), stddev_pop("a"), stddev_samp("a")]
    ).collect() == [Row(test_data_dev, 0.8164967850518458, test_data_dev)]


def test_sn_moments(session):
    test_data2 = TestData.test_data2(session)
    spark_variance = test_data2.agg(variance(col("a")))
    Utils.check_answer(spark_variance, [Row(Decimal("0.8"))])

    Utils.check_answer(
        test_data2.groupBy(col("a")).agg(variance(col("b"))),
        [Row(1, 0.50000), Row(2, 0.50000), Row(3, 0.500000)],
    )

    variance_result = session.sql(
        "select variance(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b);"
    ).collect()

    Utils.check_answer(spark_variance, variance_result[0])

    spark_variance_pop = test_data2.agg(var_pop(col("a")))
    Utils.check_answer(spark_variance_pop, [Row(Decimal("0.666667"))])

    spark_variance_samp = test_data2.agg(var_samp(col("a")))
    Utils.check_answer(spark_variance_samp, [Row(Decimal("0.8"))])

    spark_kurtosis = test_data2.agg(kurtosis(col("a")))
    Utils.check_answer(spark_kurtosis, [Row(Decimal("-1.8750"))])

    # add SQL test
    agg_kurtosis_result = session.sql(
        "select kurtosis(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b);"
    ).collect()
    Utils.check_answer(spark_kurtosis, agg_kurtosis_result[0])


def test_sn_zero_moments(session):
    input = session.createDataFrame([[1, 2]]).toDF("a", "b")
    Utils.check_answer(
        input.agg(
            [
                stddev(col("a")),
                stddev_samp(col("a")),
                stddev_pop(col("a")),
                variance(col("a")),
                var_samp(col("a")),
                var_pop(col("a")),
                skew(col("a")),
                kurtosis(col("a")),
            ]
        ),
        [Row(None, None, 0.0, None, None, 0.0000, None, None)],
    )

    Utils.check_answer(
        input.agg(
            [
                sql_expr("stddev(a)"),
                sql_expr("stddev_samp(a)"),
                sql_expr("stddev_pop(a)"),
                sql_expr("variance(a)"),
                sql_expr("var_samp(a)"),
                sql_expr("var_pop(a)"),
                sql_expr("skew(a)"),
                sql_expr("kurtosis(a)"),
            ]
        ),
        [Row(None, None, 0.0, None, None, 0.0000, None, None)],
    )


def test_sn_null_moments(session):
    empty_table_data = session.createDataFrame([[]]).toDF("a")

    Utils.check_answer(
        empty_table_data.agg(
            [
                variance(col("a")),
                var_samp(col("a")),
                var_pop(col("a")),
                skew(col("a")),
                kurtosis(col("a")),
            ]
        ),
        [Row(None, None, None, None, None)],
    )

    Utils.check_answer(
        empty_table_data.agg(
            [
                sql_expr("variance(a)"),
                sql_expr("var_samp(a)"),
                sql_expr("var_pop(a)"),
                sql_expr("skew(a)"),
                sql_expr("kurtosis(a)"),
            ]
        ),
        [Row(None, None, None, None, None)],
    )


def test_spark14664_decimal_sum_over_window_should_work(session):
    assert session.sql(
        "select sum(a) over () from values (1.0), (2.0), (3.0) T(a)"
    ).collect() == [Row(6.0), Row(6.0), Row(6.0)]
    assert session.sql(
        "select avg(a) over () from values (1.0), (2.0), (3.0) T(a)"
    ).collect() == [Row(2.0), Row(2.0), Row(2.0)]


def test_aggregate_function_in_groupby(session):
    with pytest.raises(ProgrammingError) as ex_info:
        TestData.test_data4(session).groupBy(sum(col('"KEY"'))).count().collect()
    assert "is not a valid group by expression" in str(ex_info)


def test_spark21580_ints_in_agg_exprs_are_taken_as_groupby_ordinal(
    session, db_parameters
):
    assert TestData.test_data2(session).groupBy(lit(3), lit(4)).agg(
        [lit(6), lit(7), sum(col("b"))]
    ).collect() == [Row(3, 4, 6, 7, 9)]

    assert TestData.test_data2(session).groupBy([lit(3), lit(4)]).agg(
        [lit(6), col("b"), sum(col("b"))]
    ).collect() == [Row(3, 4, 6, 1, 3), Row(3, 4, 6, 2, 6)]

    testdata2str = "(SELECT * FROM VALUES (1,1),(1,2),(2,1),(2,2),(3,1),(3,2) T(a, b) )"
    assert session.sql(
        f"SELECT 3, 4, SUM(b) FROM {testdata2str} GROUP BY 1, 2"
    ).collect() == [Row(3, 4, 9)]

    assert session.sql(
        f"SELECT 3 AS c, 4 AS d, SUM(b) FROM {testdata2str} GROUP BY c, d"
    ).collect() == [Row(3, 4, 9)]


def test_distinct_and_unions(session):
    lhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
        "i", "s", '"i"'
    )
    rhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
        "i", "s", '"i"'
    )

    res = lhs.union(rhs).distinct().collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.0), Row(2, "one", 2.0)]

    lhsD = lhs.select(col("s")).distinct()
    rhs = rhs.select(col("s"))
    res = lhsD.union(rhs).collect()
    assert res == [Row("one")]

    lhs = lhs.select(col("s"))
    rhsD = rhs.select("s").distinct()

    res = lhs.union(rhsD).collect()
    assert res == [Row("one")]

    res = lhsD.union(rhsD).collect()
    assert res == [Row("one")]


def test_distinct_and_unionall(session):
    lhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
        "i", "s", '"i"'
    )
    rhs = session.createDataFrame([(1, "one", 1.0), (2, "one", 2.0)]).toDF(
        "i", "s", '"i"'
    )

    res = lhs.unionAll(rhs).distinct().collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.0), Row(2, "one", 2.0)]

    lhsD = lhs.select(col("s")).distinct()
    rhs = rhs.select(col("s"))
    res = lhsD.unionAll(rhs).collect()
    assert res == [Row("one"), Row("one"), Row("one")]

    lhs = lhs.select(col("s"))
    rhsD = rhs.select("s").distinct()

    res = lhs.unionAll(rhsD).collect()
    assert res == [Row("one"), Row("one"), Row("one")]

    res = lhsD.unionAll(rhsD).collect()
    assert res == [Row("one"), Row("one")]


def test_count_if(session):
    session.createDataFrame(
        [
            ["a", None],
            ["a", 1],
            ["a", 2],
            ["a", 3],
            ["b", None],
            ["b", 4],
            ["b", 5],
            ["b", 6],
        ]
    ).toDF("x", "y").createOrReplaceTempView("tempView")

    res = session.sql(
        "SELECT COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), COUNT_IF(y IS NULL) FROM tempView"
    ).collect()
    assert res == [Row(0, 3, 3, 2)]

    res = session.sql(
        "SELECT x, COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), COUNT_IF(y IS NULL) FROM tempView GROUP BY x"
    ).collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row("a", 0, 1, 2, 1), Row("b", 0, 2, 1, 1)]

    res = session.sql(
        "SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 1"
    ).collect()
    assert res == [Row("a")]

    res = session.sql(
        "SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 2"
    ).collect()
    assert res == [Row("b")]

    res = session.sql(
        "SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y IS NULL) > 0"
    ).collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row("a"), Row("b")]

    res = session.sql(
        "SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(NULL) > 0"
    ).collect()
    assert res == []

    with pytest.raises(ProgrammingError) as ex_info:
        session.sql("SELECT COUNT_IF(x) FROM tempView").collect()


def test_agg_without_groups(session):
    assert TestData.test_data2(session).agg(sum(col("b"))).collect() == [Row(9)]


def test_agg_without_groups_and_functions(session):
    assert TestData.test_data2(session).agg(lit(1)).collect() == [Row(1)]


def test_null_average(session):
    assert TestData.test_data3(session).agg(avg(col("b"))).collect() == [Row(2.0)]

    assert TestData.test_data3(session).agg(
        [avg(col("b")), count_distinct(col("b"))]
    ).collect() == [Row(2.0, 1)]

    assert TestData.test_data3(session).agg(
        [avg(col("b")), sum_distinct(col("b"))]
    ).collect() == [Row(2.0, 2.0)]


def test_zero_average(session):
    df = session.createDataFrame([[]]).toDF(["a"])
    assert df.agg(avg(col("a"))).collect() == [Row(None)]

    assert df.agg([avg(col("a")), sum_distinct(col("a"))]).collect() == [
        Row(None, None)
    ]


def test_multiple_column_distinct_count(session):
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
        df1.groupBy(col("key1")).agg(count_distinct(col("key2"), col("key3"))).collect()
    )
    res.sort(key=lambda x: x[0])
    assert res == [Row("a", 2), Row("x", 1)]


def test_zero_count(session):
    empty_table = session.createDataFrame([[]]).toDF(["a"])
    assert empty_table.agg([count(col("a")), sum_distinct(col("a"))]).collect() == [
        Row(0, None)
    ]


def test_zero_stddev(session):
    df = session.createDataFrame([[]]).toDF(["a"])
    assert df.agg(
        [stddev(col("a")), stddev_pop(col("a")), stddev_samp(col("a"))]
    ).collect() == [Row(None, None, None)]


def test_zero_sum(session):
    df = session.createDataFrame([[]]).toDF(["a"])
    assert df.agg([sum(col("a"))]).collect() == [Row(None)]


def test_zero_sum_distinct(session):
    df = session.createDataFrame([[]]).toDF(["a"])
    assert df.agg([sum_distinct(col("a"))]).collect() == [Row(None)]


def test_limit_and_aggregates(session):
    df = session.createDataFrame([("a", 1), ("b", 2), ("c", 1), ("d", 5)]).toDF(
        "id", "value"
    )
    limit_2df = df.limit(2)
    Utils.check_answer(
        limit_2df.groupBy("id").count().select(col("id")), limit_2df.select("id"), True
    )
