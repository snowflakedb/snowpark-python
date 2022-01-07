#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from decimal import Decimal

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Window
from snowflake.snowpark.functions import (
    avg,
    col,
    corr,
    count,
    covar_pop,
    covar_samp,
    cume_dist,
    dense_rank,
    kurtosis,
    lead,
    lit,
    max as max_,
    mean,
    min as min_,
    ntile,
    percent_rank,
    rank,
    row_number,
    skew,
    stddev,
    stddev_pop,
    stddev_samp,
    sum as sum_,
    var_pop,
    var_samp,
    variance,
)
from tests.utils import TestData, Utils


def test_partition_by_order_by_rows_between(session):
    df = session.createDataFrame(
        [(1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")]
    ).toDF("key", "value")
    window = Window.partitionBy("value").orderBy("key").rowsBetween(-1, 2)
    Utils.check_answer(
        df.select("key", avg("key").over(window)),
        [
            Row(1, Decimal("1.333")),
            Row(1, Decimal("1.333")),
            Row(2, Decimal("1.500")),
            Row(2, Decimal("2.000")),
            Row(2, Decimal("2.000")),
        ],
    )

    window2 = Window.rowsBetween(Window.currentRow, 2).orderBy("key")
    Utils.check_answer(
        df.select("key", avg("key").over(window2)),
        [
            Row(2, Decimal("2.000")),
            Row(2, Decimal("2.000")),
            Row(2, Decimal("2.000")),
            Row(1, Decimal("1.666")),
            Row(1, Decimal("1.333")),
        ],
        sort=False,
    )


def test_range_between(session):
    df = session.createDataFrame(["non_numeric"]).toDF("value")
    window = Window.orderBy("value")
    Utils.check_answer(
        df.select(
            "value",
            min_("value").over(
                window.rangeBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        ),
        [Row("non_numeric", "non_numeric")],
    )
    window2 = Window.rangeBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    ).orderBy("value")
    Utils.check_answer(
        df.select("value", min_("value").over(window2)),
        [Row("non_numeric", "non_numeric")],
    )


def test_window_function_with_aggregates(session):
    df = session.createDataFrame(
        [("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2)]
    ).toDF("key", "value")
    window = Window.orderBy()
    Utils.check_answer(
        df.groupBy("key").agg(
            [sum_("value"), sum_(sum_("value")).over(window) - sum_("value")]
        ),
        [Row("a", 6, 9), Row("b", 9, 6)],
    )


def test_window_function_inside_where_and_having_clauses(session):
    with pytest.raises(ProgrammingError) as ex_info:
        TestData.test_data2(session).select("a").where(
            rank().over(Window.orderBy("b")) == 1
        ).collect()
    assert "invalid identifier" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.test_data2(session).where(
            (col("b") == 2) & rank().over(Window.orderBy("b")) == 1
        ).collect()
    assert "outside of SELECT, QUALIFY, and ORDER BY clauses" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.test_data2(session).groupBy("a").agg(avg("b").as_("avgb")).where(
            (col("a") > col("avgb")) & rank().over(Window.orderBy("a")) == 1
        ).collect()
    assert "outside of SELECT, QUALIFY, and ORDER BY clauses" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.test_data2(session).groupBy("a").agg(
            [max_("b").as_("avgb"), sum_("b").as_("sumb")]
        ).where(rank().over(Window.orderBy("a")) == 1).collect()
    assert "outside of SELECT, QUALIFY, and ORDER BY clauses" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.test_data2(session).groupBy("a").agg(
            [max_("b").as_("avgb"), sum_("b").as_("sumb")]
        ).where((col("sumb") == 5) & rank().over(Window.orderBy("a")) == 1).collect()
    assert "outside of SELECT, QUALIFY, and ORDER BY clauses" in str(ex_info)


def test_reuse_window_partition_by(session):
    df = session.createDataFrame([(1, "1"), (2, "2"), (1, "1"), (2, "2")]).toDF(
        "key", "value"
    )
    w = Window.partitionBy("key").orderBy("value")

    Utils.check_answer(
        df.select(lead("key", 1).over(w), lead("value", 1).over(w)),
        [Row(1, "1"), Row(2, "2"), Row(None, None), Row(None, None)],
    )


def test_reuse_window_order_by(session):
    df = session.createDataFrame([(1, "1"), (2, "2"), (1, "1"), (2, "2")]).toDF(
        "key", "value"
    )
    w = Window.orderBy("value").partitionBy("key")

    Utils.check_answer(
        df.select(lead("key", 1).over(w), lead("value", 1).over(w)),
        [Row(1, "1"), Row(2, "2"), Row(None, None), Row(None, None)],
    )


def test_rank_functions_in_unspecific_window(session):
    df = session.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (2, "2")]).toDF(
        "key", "value"
    )
    Utils.check_answer(
        df.select(
            "key",
            max_("key").over(Window.partitionBy("value").orderBy("key")),
            min_("key").over(Window.partitionBy("value").orderBy("key")),
            mean("key").over(Window.partitionBy("value").orderBy("key")),
            count("key").over(Window.partitionBy("value").orderBy("key")),
            sum_("key").over(Window.partitionBy("value").orderBy("key")),
            ntile(lit(2)).over(Window.partitionBy("value").orderBy("key")),
            row_number().over(Window.partitionBy("value").orderBy("key")),
            dense_rank().over(Window.partitionBy("value").orderBy("key")),
            rank().over(Window.partitionBy("value").orderBy("key")),
            cume_dist().over(Window.partitionBy("value").orderBy("key")),
            percent_rank().over(Window.partitionBy("value").orderBy("key")),
        ).collect(),
        [
            Row(1, 1, 1, 1.0, 1, 1, 1, 1, 1, 1, 0.3333333333333333, 0.0),
            Row(1, 1, 1, 1.0, 1, 1, 1, 1, 1, 1, 1.0, 0.0),
            Row(2, 2, 1, Decimal("1.666667"), 3, 5, 1, 2, 2, 2, 1.0, 0.5),
            Row(2, 2, 1, Decimal("1.666667"), 3, 5, 2, 3, 2, 2, 1.0, 0.5),
        ],
    )


def test_empty_over_spec(session):
    df = session.createDataFrame([("a", 1), ("a", 1), ("a", 2), ("b", 2)]).toDF(
        "key", "value"
    )
    df.createOrReplaceTempView("window_table")
    Utils.check_answer(
        df.select("key", "value", sum_("value").over(), avg("value").over()),
        [
            Row("a", 1, 6, 1.5),
            Row("a", 1, 6, 1.5),
            Row("a", 2, 6, 1.5),
            Row("b", 2, 6, 1.5),
        ],
    )
    Utils.check_answer(
        session.sql(
            "select key, value, sum(value) over(), avg(value) over() from window_table"
        ),
        [
            Row("a", 1, 6, 1.5),
            Row("a", 1, 6, 1.5),
            Row("a", 2, 6, 1.5),
            Row("b", 2, 6, 1.5),
        ],
    )


def test_null_inputs(session):
    df = session.createDataFrame(
        [("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2)]
    ).toDF("key", "value")
    window = Window.orderBy()
    Utils.check_answer(
        df.select(
            "key", "value", avg(lit(None)).over(window), sum_(lit(None)).over(window)
        ),
        [
            Row("a", 1, None, None),
            Row("a", 1, None, None),
            Row("a", 2, None, None),
            Row("a", 2, None, None),
            Row("b", 4, None, None),
            Row("b", 3, None, None),
            Row("b", 2, None, None),
        ],
        sort=False,
    )


def test_window_function_should_fail_if_order_by_clause_is_not_specified(session):
    df = session.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (2, "2")]).toDF(
        "key", "value"
    )
    # Here we missed .orderBy("key")!
    with pytest.raises(ProgrammingError) as ex_info:
        df.select(row_number().over(Window.partitionBy("value"))).collect()
    assert "requires ORDER BY in window specification" in str(ex_info)


def test_corr_covar_pop_stddev_pop_functions_in_specific_window(session):
    df = session.createDataFrame(
        [
            ("a", "p1", 10.0, 20.0),
            ("b", "p1", 20.0, 10.0),
            ("c", "p2", 20.0, 20.0),
            ("d", "p2", 20.0, 20.0),
            ("e", "p3", 0.0, 0.0),
            ("f", "p3", 6.0, 12.0),
            ("g", "p3", 6.0, 12.0),
            ("h", "p3", 8.0, 16.0),
            ("i", "p4", 5.0, 5.0),
        ]
    ).toDF("key", "partitionId", "value1", "value2")
    Utils.check_answer(
        df.select(
            "key",
            corr("value1", "value2").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            covar_pop("value1", "value2").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            var_pop("value1").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            stddev_pop("value1").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            var_pop("value2").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            stddev_pop("value2").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        ),
        [
            Row("a", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
            Row("b", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
            Row("c", None, 0.0, 0.0, 0.0, 0.0, 0.0),
            Row("d", None, 0.0, 0.0, 0.0, 0.0, 0.0),
            Row("e", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
            Row("f", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
            Row("g", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
            Row("h", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
            Row("i", None, 0.0, 0.0, 0.0, 0.0, 0.0),
        ],
    )


def test_covar_samp_var_samp_stddev_samp_functions_in_specific_window(session):
    df = session.createDataFrame(
        [
            ("a", "p1", 10.0, 20.0),
            ("b", "p1", 20.0, 10.0),
            ("c", "p2", 20.0, 20.0),
            ("d", "p2", 20.0, 20.0),
            ("e", "p3", 0.0, 0.0),
            ("f", "p3", 6.0, 12.0),
            ("g", "p3", 6.0, 12.0),
            ("h", "p3", 8.0, 16.0),
            ("i", "p4", 5.0, 5.0),
        ]
    ).toDF("key", "partitionId", "value1", "value2")
    Utils.check_answer(
        df.select(
            "key",
            covar_samp("value1", "value2").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            var_samp("value1").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            variance("value1").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            stddev_samp("value1").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            stddev("value1").over(
                Window.partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        ),
        [
            Row("a", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
            Row("b", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
            Row("c", 0.0, 0.0, 0.0, 0.0, 0.0),
            Row("d", 0.0, 0.0, 0.0, 0.0, 0.0),
            Row("e", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
            Row("f", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
            Row("g", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
            Row("h", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
            Row("i", None, None, None, None, None),
        ],
    )


def test_aggregation_function_on_invalid_column(session):
    df = session.createDataFrame([(1, "1")]).toDF("key", "value")
    with pytest.raises(ProgrammingError) as ex_info:
        df.select("key", count("invalid").over()).collect()
    assert "invalid identifier" in str(ex_info)


def test_skewness_and_kurtosis_functions_in_window(session):
    df = session.createDataFrame(
        [
            ("a", "p1", 1.0),
            ("b", "p1", 1.0),
            ("c", "p1", 2.0),
            ("d", "p1", 2.0),
            ("e", "p1", 3.0),
            ("f", "p1", 3.0),
            ("g", "p1", 3.0),
            ("h", "p2", 1.0),
            ("i", "p2", 2.0),
            ("j", "p2", 5.0),
        ]
    ).toDF("key", "partition", "value")
    Utils.check_answer(
        df.select(
            "key",
            skew("value").over(
                Window.partitionBy("partition")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            kurtosis("value").over(
                Window.partitionBy("partition")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        ),
        # results are checked by scipy.stats.skew() and scipy.stats.kurtosis()
        [
            Row("a", -0.353044463087872, -1.8166064369747783),
            Row("b", -0.353044463087872, -1.8166064369747783),
            Row("c", -0.353044463087872, -1.8166064369747783),
            Row("d", -0.353044463087872, -1.8166064369747783),
            Row("e", -0.353044463087872, -1.8166064369747783),
            Row("f", -0.353044463087872, -1.8166064369747783),
            Row("g", -0.353044463087872, -1.8166064369747783),
            Row("h", 1.293342780733395, None),
            Row("i", 1.293342780733395, None),
            Row("j", 1.293342780733395, None),
        ],
    )


def test_window_functions_in_multiple_selects(session):
    df = session.createDataFrame(
        [("S1", "P1", 100), ("S1", "P1", 700), ("S2", "P1", 200), ("S2", "P2", 300)]
    ).toDF("sno", "pno", "qty")
    w1 = Window.partitionBy("sno")
    w2 = Window.partitionBy("sno", "pno")
    select = df.select(
        "sno", "pno", "qty", sum_("qty").over(w2).alias("sum_qty_2")
    ).select(
        "sno", "pno", "qty", col("sum_qty_2"), sum_("qty").over(w1).alias("sum_qty_1")
    )
    Utils.check_answer(
        select,
        [
            Row("S1", "P1", 100, 800, 800),
            Row("S1", "P1", 700, 800, 800),
            Row("S2", "P1", 200, 200, 500),
            Row("S2", "P2", 300, 300, 500),
        ],
    )
