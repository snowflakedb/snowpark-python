#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import sys
from decimal import Decimal

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Window
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    lag,
    lead,
    min as min_,
    sum as sum_,
)
from tests.utils import Utils


def test_lead_lag_with_positive_offset(session):
    df = session.createDataFrame(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    window = Window.partitionBy("key").orderBy("value")
    Utils.check_answer(
        df.select("key", lead("value", 1).over(window), lag("value", 1).over(window)),
        [Row(1, "3", None), Row(1, None, "1"), Row(2, "4", None), Row(2, None, "2")],
    )


def test_reverse_lead_lag_with_positive_offset(session):
    df = session.createDataFrame(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    window = Window.partitionBy("key").orderBy(col("value").desc())
    Utils.check_answer(
        df.select("key", lead("value", 1).over(window), lag("value", 1).over(window)),
        [Row(1, "1", None), Row(1, None, "3"), Row(2, "2", None), Row(2, None, "4")],
    )


def test_lead_lag_with_negative_offset(session):
    df = session.createDataFrame(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    window = Window.partitionBy("key").orderBy("value")
    Utils.check_answer(
        df.select("key", lead("value", -1).over(window), lag("value", -1).over(window)),
        [Row(1, "1", None), Row(1, None, "3"), Row(2, "2", None), Row(2, None, "4")],
    )


def test_reverse_lead_lag_with_negative_offset(session):
    df = session.createDataFrame(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    window = Window.partitionBy("key").orderBy(col("value").desc())
    Utils.check_answer(
        df.select("key", lead("value", -1).over(window), lag("value", -1).over(window)),
        [Row(1, "3", None), Row(1, None, "1"), Row(2, "4", None), Row(2, None, "2")],
    )


def test_lead_lag_with_default_value(session):
    default = None
    df = session.createDataFrame(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4"), (2, "5")], schema=["key", "value"]
    )
    window = Window.partitionBy("key").orderBy("value")
    Utils.check_answer(
        df.select(
            "key",
            lead("value", 2).over(window),
            lag("value", 2).over(window),
            lead("value", -2).over(window),
            lag("value", -2).over(window),
        ),
        [
            Row(1, default, default, default, default),
            Row(1, default, default, default, default),
            Row(2, "5", default, default, "5"),
            Row(2, default, "2", "2", default),
            Row(2, default, default, default, default),
        ],
    )


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


def test_range_between_should_accept_at_most_one_order_by_expression_when_unbounded(
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
