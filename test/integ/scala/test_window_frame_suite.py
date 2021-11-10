#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import sys
from decimal import Decimal
from test.utils import Utils

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Window
from snowflake.snowpark.functions import avg, col, count, min as min_, sum as sum_


def test_unbounded_rows_range_between_with_aggregation(session):
    df = session.createDataFrame([("one", 1), ("two", 2), ("one", 3), ("two", 4)]).toDF(
        "key", "value"
    )
    window = Window.partitionBy("key").orderBy("value")
    Utils.check_answer(
        df.select(
            "key",
            sum_("value").over(
                window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
            sum_("value").over(
                window.rangeBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        ),
        [Row("one", 4, 4), Row("one", 4, 4), Row("two", 6, 6), Row("two", 6, 6)],
    )


def test_rows_between_boundary(session):
    # This test is different from scala as `int` in Python is unbounded
    df = session.createDataFrame(
        [(1, "1"), (1, "1"), (sys.maxsize, "1"), (3, "2"), (2, "1"), (sys.maxsize, "2")]
    ).toDF("key", "value")
    Utils.check_answer(
        df.select(
            "key",
            count("key").over(
                Window.partitionBy("value").orderBy("key").rowsBetween(0, 100)
            ),
        ),
        [
            Row(1, 3),
            Row(1, 4),
            Row(2, 2),
            Row(sys.maxsize, 1),
            Row(sys.maxsize, 1),
            Row(3, 2),
        ],
    )
    Utils.check_answer(
        df.select(
            "key",
            count("key").over(
                Window.partitionBy("value").orderBy("key").rowsBetween(0, sys.maxsize)
            ),
        ),
        [
            Row(1, 3),
            Row(1, 4),
            Row(2, 2),
            Row(sys.maxsize, 1),
            Row(sys.maxsize, 1),
            Row(3, 2),
        ],
    )
    Utils.check_answer(
        df.select(
            "key",
            count("key").over(
                Window.partitionBy("value").orderBy("key").rowsBetween(-sys.maxsize, 0)
            ),
        ),
        [
            Row(1, 1),
            Row(1, 2),
            Row(2, 3),
            Row(3, 1),
            Row(sys.maxsize, 2),
            Row(sys.maxsize, 4),
        ],
    )


def test_range_between_should_accep_at_most_one_order_by_expression_when_unbounded(
    session,
):
    df = session.createDataFrame([(1, 1)]).toDF("key", "value")
    window = Window.orderBy("key", "value")
    Utils.check_answer(
        df.select(
            "key",
            min_("key").over(
                window.rangeBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        ),
        [Row(1, 1)],
    )

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(
            min_("key").over(window.rangeBetween(Window.unboundedPreceding, 1))
        ).collect()
    assert "Cumulative window frame unsupported for function MIN" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(
            min_("key").over(window.rangeBetween(-1, Window.unboundedFollowing))
        ).collect()
    assert "Cumulative window frame unsupported for function MIN" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(min_("key").over(window.rangeBetween(-1, 1))).collect()
    assert "Sliding window frame unsupported for function MIN" in str(ex_info)


def test_range_between_should_accept_numeric_values_only_when_bounded(session):
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

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(
            min_("value").over(window.rangeBetween(Window.unboundedPreceding, 1))
        ).collect()
    assert "Cumulative window frame unsupported for function MIN" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(
            min_("value").over(window.rangeBetween(-1, Window.unboundedFollowing))
        ).collect()
    assert "Cumulative window frame unsupported for function MIN" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(min_("value").over(window.rangeBetween(-1, 1))).collect()
    assert "Sliding window frame unsupported for function MIN" in str(ex_info)


def test_sliding_rows_between_with_aggregation(session):
    df = session.createDataFrame(
        [(1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")]
    ).toDF("key", "value")
    window = Window.partitionBy("value").orderBy(col("key")).rowsBetween(-1, 2)
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


def test_reverse_sliding_rows_between_with_aggregation(session):
    df = session.createDataFrame(
        [(1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")]
    ).toDF("key", "value")
    window = Window.partitionBy("value").orderBy(col("key").desc()).rowsBetween(-1, 2)
    Utils.check_answer(
        df.select("key", avg("key").over(window)),
        [
            Row(1, Decimal("1.000")),
            Row(1, Decimal("1.333")),
            Row(2, Decimal("1.333")),
            Row(2, Decimal("2.000")),
            Row(2, Decimal("2.000")),
        ],
    )
