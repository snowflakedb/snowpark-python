#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from decimal import Decimal
from test.utils import Utils

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Window
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    kurtosis,
    lit,
    min as min_,
    skew,
    sum as sum_,
)


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
