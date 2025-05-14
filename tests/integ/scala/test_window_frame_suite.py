#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import sys
from decimal import Decimal

import pytest

from snowflake.snowpark import Row, Window
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    first_value,
    lag,
    last_value,
    lead,
    lit,
    make_interval,
    min as min_,
    sum as sum_,
)
from snowflake.snowpark.types import (
    DateType,
    DecimalType,
    LongType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
)
from tests.utils import Utils, multithreaded_run


def test_lead_lag_with_positive_offset(session):
    df = session.create_dataframe(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    window = Window.partition_by("key").order_by("value")
    Utils.check_answer(
        df.select("key", lead("value", 1).over(window), lag("value", 1).over(window)),
        [Row(1, "3", None), Row(1, None, "1"), Row(2, "4", None), Row(2, None, "2")],
    )


@multithreaded_run()
def test_reverse_lead_lag_with_positive_offset(session):
    df = session.create_dataframe(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    window = Window.partition_by("key").order_by(col("value").desc())
    Utils.check_answer(
        df.select("key", lead("value", 1).over(window), lag("value", 1).over(window)),
        [Row(1, "1", None), Row(1, None, "3"), Row(2, "2", None), Row(2, None, "4")],
    )


def test_lead_lag_with_negative_offset(session):
    df = session.create_dataframe(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    window = Window.partition_by("key").order_by("value")
    Utils.check_answer(
        df.select("key", lead("value", -1).over(window), lag("value", -1).over(window)),
        [Row(1, "1", None), Row(1, None, "3"), Row(2, "2", None), Row(2, None, "4")],
    )


def test_reverse_lead_lag_with_negative_offset(session):
    df = session.create_dataframe(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    window = Window.partition_by("key").order_by(col("value").desc())
    Utils.check_answer(
        df.select("key", lead("value", -1).over(window), lag("value", -1).over(window)),
        [Row(1, "3", None), Row(1, None, "1"), Row(2, "4", None), Row(2, None, "2")],
    )


@pytest.mark.parametrize("default", [None, "10"])
def test_lead_lag_with_default_value(session, default):
    df = session.create_dataframe(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4"), (2, "5")], schema=["key", "value"]
    )
    window = Window.partition_by("key").order_by("value")
    Utils.check_answer(
        df.select(
            "key",
            lead("value", 2, default).over(window),
            lag("value", 2, default).over(window),
            lead("value", -2, default).over(window),
            lag("value", -2, default).over(window),
        ),
        [
            Row(1, default, default, default, default),
            Row(1, default, default, default, default),
            Row(2, "5", default, default, "5"),
            Row(2, default, "2", "2", default),
            Row(2, default, default, default, default),
        ],
    )


@multithreaded_run()
def test_lead_lag_with_ignore_or_respect_nulls(session):
    df = session.create_dataframe(
        [(1, 5), (2, 4), (3, None), (4, 2), (5, None), (6, None), (7, 6)],
        schema=["key", "value"],
    )
    window = Window.order_by("key")
    Utils.check_answer(
        df.select(
            "key",
            lead("value").over(window),
            lag("value").over(window),
            lead("value", ignore_nulls=True).over(window),
            lag("value", ignore_nulls=True).over(window),
        ).sort("key"),
        [
            Row(1, 4, None, 4, None),
            Row(2, None, 5, 2, 5),
            Row(3, 2, 4, 2, 4),
            Row(4, None, None, 6, 4),
            Row(5, None, 2, 6, 2),
            Row(6, 6, None, 6, 2),
            Row(7, None, None, None, 2),
        ],
    )


def test_first_last_value_with_ignore_or_respect_nulls(session):
    df = session.create_dataframe(
        [(1, None), (2, 4), (3, None), (4, 2), (5, None), (6, 6), (7, None)],
        schema=["key", "value"],
    )
    window = Window.order_by("key")
    Utils.check_answer(
        df.select(
            "key",
            first_value("value").over(window),
            last_value("value").over(window),
            first_value("value", ignore_nulls=True).over(window),
            last_value("value", ignore_nulls=True).over(window),
        ).sort("key"),
        [
            Row(1, None, None, 4, 6),
            Row(2, None, None, 4, 6),
            Row(3, None, None, 4, 6),
            Row(4, None, None, 4, 6),
            Row(5, None, None, 4, 6),
            Row(6, None, None, 4, 6),
            Row(7, None, None, 4, 6),
        ],
    )


def test_unbounded_rows_range_between_with_aggregation(session):
    df = session.create_dataframe(
        [("one", 1), ("two", 2), ("one", 3), ("two", 4)]
    ).to_df("key", "value")
    window = Window.partition_by("key").order_by("value")
    Utils.check_answer(
        df.select(
            "key",
            sum_("value").over(
                window.rows_between(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
            sum_("value").over(
                window.range_between(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        ),
        [Row("one", 4, 4), Row("one", 4, 4), Row("two", 6, 6), Row("two", 6, 6)],
    )


def test_rows_between_boundary(session):
    # This test is different from scala as `int` in Python is unbounded
    df = session.create_dataframe(
        [
            (1, "1"),
            (1, "1"),
            (sys.maxsize, "1"),
            (3, "2"),
            (2, "1"),
            (sys.maxsize, "2"),
        ]
    ).to_df("key", "value")

    Utils.check_answer(
        df.select(
            "key",
            count("key").over(
                Window.partition_by("value").order_by("key").rows_between(0, 100)
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
                Window.partition_by("value")
                .order_by("key")
                .rows_between(0, sys.maxsize)
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
                Window.partition_by("value")
                .order_by("key")
                .rows_between(-sys.maxsize, 0)
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


def test_range_between_should_accept_at_most_one_order_by_expression_when_bounded(
    session, local_testing_mode
):
    df = session.create_dataframe([(1, 1)]).to_df("key", "value")
    window = Window.order_by("key", "value")
    Utils.check_answer(
        df.select(
            "key",
            min_("key").over(
                window.range_between(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        ),
        [Row(1, 1)],
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(
            min_("key").over(window.range_between(Window.unboundedPreceding, 1))
        ).collect()
        if not local_testing_mode:
            assert "Cumulative window frame unsupported for function MIN" in str(
                ex_info.value
            )

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(
            min_("key").over(window.range_between(-1, Window.unboundedFollowing))
        ).collect()
        if not local_testing_mode:
            assert "Cumulative window frame unsupported for function MIN" in str(
                ex_info.value
            )

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(min_("key").over(window.range_between(-1, 1))).collect()
        if not local_testing_mode:
            assert "Sliding window frame unsupported for function MIN" in str(
                ex_info.value
            )


def test_range_between_should_accept_non_numeric_values_only_when_unbounded(
    session, local_testing_mode
):
    df = session.create_dataframe(["non_numeric"]).to_df("value")
    window = Window.order_by("value")
    Utils.check_answer(
        df.select(
            "value",
            min_("value").over(
                window.range_between(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        ),
        [Row("non_numeric", "non_numeric")],
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(
            min_("value").over(window.range_between(Window.unboundedPreceding, 1))
        ).collect()
        if not local_testing_mode:
            assert "Cumulative window frame unsupported for function MIN" in str(
                ex_info.value
            )

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(
            min_("value").over(window.range_between(-1, Window.unboundedFollowing))
        ).collect()
        if not local_testing_mode:
            assert "Cumulative window frame unsupported for function MIN" in str(
                ex_info.value
            )

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(min_("value").over(window.range_between(-1, 1))).collect()
        if not local_testing_mode:
            assert "Sliding window frame unsupported for function MIN" in str(
                ex_info.value
            )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435114: windowed aggregations do not have a consistent precision in live.",
)
def test_sliding_rows_between_with_aggregation(session):
    df = session.create_dataframe(
        [(1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")]
    ).to_df("key", "value")
    window = Window.partition_by("value").order_by(col("key")).rows_between(-1, 2)
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435114: windowed aggregations do not have a consistent precision in live.",
)
def test_reverse_sliding_rows_between_with_aggregation(session):
    df = session.create_dataframe(
        [(1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")]
    ).to_df("key", "value")

    window = (
        Window.partition_by("value").order_by(col("key").desc()).rows_between(-1, 2)
    )

    result = df.select("key", avg("key").over(window).alias("avg"))
    assert result.schema == StructType(
        [
            StructField("KEY", LongType(), nullable=False),
            StructField("AVG", DecimalType(38, 3), nullable=True),
        ]
    )
    Utils.check_answer(
        result,
        [
            Row(1, Decimal("1.000")),
            Row(1, Decimal("1.333")),
            Row(2, Decimal("1.333")),
            Row(2, Decimal("2.000")),
            Row(2, Decimal("2.000")),
        ],
    )


def test_range_between_should_include_rows_equal_to_current_row(session):
    df1 = session.create_dataframe(
        [("b", 10), ("a", 10), ("a", 10), ("d", 15), ("e", 20), ("f", 20)],
        schema=["c1", "c2"],
    )
    win = Window.order_by(col("c2").asc(), col("c1").desc()).range_between(
        -sys.maxsize, 0
    )
    Utils.check_answer(
        df1.select(col("c1"), col("c2"), (sum_(col("c2")).over(win)).alias("win_sum")),
        [
            Row(C1="b", C2=10, WIN_SUM=10),
            Row(C1="a", C2=10, WIN_SUM=30),
            Row(C1="a", C2=10, WIN_SUM=30),
            Row(C1="d", C2=15, WIN_SUM=45),
            Row(C1="e", C2=20, WIN_SUM=85),
            Row(C1="f", C2=20, WIN_SUM=65),
        ],
    )


def test_range_between_timestamp(session, local_testing_mode):
    df = session.create_dataframe(
        [
            datetime.datetime(2021, 12, 21, 9, 12, 56),
            datetime.datetime(2021, 12, 21, 8, 12, 56),
            datetime.datetime(2021, 12, 21, 7, 12, 56),
            datetime.datetime(2021, 12, 21, 6, 12, 56),
        ],
        schema=StructType([StructField("a", TimestampType(TimestampTimeZone.NTZ))]),
    )

    window = Window.order_by(col("a")).range_between(-make_interval(hours=2), 0)
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(2), Row(3), Row(3)])

    window = Window.order_by(col("a").desc()).range_between(-make_interval(hours=2), 0)
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(2), Row(3), Row(3)])

    window = Window.order_by(col("a")).range_between(0, make_interval(minutes=60))
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(2), Row(2), Row(2)])

    window = Window.order_by(col("a").desc()).range_between(
        0, make_interval(minutes=60)
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(2), Row(2), Row(2)])

    window = Window.order_by(col("a")).range_between(make_interval(hours=1), 0)
    with pytest.raises(SnowparkSQLException, match="Invalid window frame"):
        df.select(count("a").over(window)).collect()

    window = Window.order_by(col("a")).range_between(0, -make_interval(hours=1))
    with pytest.raises(SnowparkSQLException, match="Invalid window frame"):
        df.select(count("a").over(window)).collect()

    window = Window.order_by(col("a")).range_between(
        -make_interval(secs=0), Window.currentRow
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(1), Row(1), Row(1)])

    window = Window.order_by(col("a")).range_between(
        -make_interval(hours=1), Window.unboundedFollowing
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(4), Row(4), Row(3), Row(2)])

    window = Window.order_by(col("a").desc()).range_between(
        -make_interval(hours=1), Window.unboundedFollowing
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(4), Row(4), Row(3), Row(2)])

    window = Window.order_by(col("a")).range_between(
        make_interval(hours=1), Window.unboundedFollowing
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(3), Row(2), Row(1), Row(0)])

    window = Window.order_by(col("a").desc()).range_between(
        make_interval(hours=1), Window.unboundedFollowing
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(3), Row(2), Row(1), Row(0)])

    window = Window.order_by(col("a")).range_between(
        -make_interval(hours=1), make_interval(hours=1)
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(2), Row(3), Row(3), Row(2)])

    window = Window.order_by(col("a")).range_between(
        make_interval(hours=1), make_interval(hours=1)
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(0), Row(1), Row(1), Row(1)])

    window = Window.order_by(col("a")).range_between(make_interval(hours=1), 3)
    with pytest.raises(
        SnowparkSQLException,
        match="Mixing numeric and interval frames is not supported",
    ):
        df.select(count("a").over(window)).collect()

    if not local_testing_mode:
        # Local testing mode can't tell the difference between preceding and following
        window = Window.order_by(col("a")).range_between(
            make_interval(secs=0), Window.currentRow
        )
        with pytest.raises(SnowparkSQLException, match="Invalid window frame"):
            df.select(count("a").over(window)).collect()


def test_range_between_date(session, local_testing_mode):
    df = session.create_dataframe(
        [
            datetime.date(2021, 12, 21),
            datetime.date(2021, 12, 22),
            datetime.date(2021, 12, 23),
            datetime.date(2021, 12, 24),
        ],
        schema=StructType([StructField("a", DateType())]),
    )

    window = Window.order_by(col("a")).range_between(-make_interval(days=2), 0)
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(2), Row(3), Row(3)])

    window = Window.order_by(col("a").desc()).range_between(-make_interval(days=2), 0)
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(2), Row(3), Row(3)])

    window = Window.order_by(col("a")).range_between(-1, 1)
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(2), Row(3), Row(3), Row(2)])

    window = Window.order_by(col("a").desc()).range_between(0, make_interval(days=1))
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(2), Row(2), Row(2)])

    window = Window.order_by(col("a")).range_between(make_interval(days=1), 0)
    with pytest.raises(SnowparkSQLException, match="Invalid window frame"):
        df.select(count("a").over(window)).collect()

    window = Window.order_by(col("a")).range_between(0, -make_interval(days=1))
    with pytest.raises(SnowparkSQLException, match="Invalid window frame"):
        df.select(count("a").over(window)).collect()

    window = Window.order_by(col("a")).range_between(
        -make_interval(days=0), Window.currentRow
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(1), Row(1), Row(1), Row(1)])

    window = Window.order_by(col("a")).range_between(-1, Window.unboundedFollowing)
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(4), Row(4), Row(3), Row(2)])

    window = Window.order_by(col("a")).range_between(
        -make_interval(days=1), Window.unboundedFollowing
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(4), Row(4), Row(3), Row(2)])

    window = Window.order_by(col("a").desc()).range_between(
        -make_interval(days=1), Window.unboundedFollowing
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(4), Row(4), Row(3), Row(2)])

    window = Window.order_by(col("a")).range_between(
        make_interval(days=1), Window.unboundedFollowing
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(3), Row(2), Row(1), Row(0)])

    window = Window.order_by(col("a").desc()).range_between(
        make_interval(days=1), Window.unboundedFollowing
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(3), Row(2), Row(1), Row(0)])

    window = Window.order_by(col("a")).range_between(
        -make_interval(days=1), make_interval(days=1)
    )
    df_count = df.select(count("a").over(window))
    Utils.check_answer(df_count, [Row(2), Row(3), Row(3), Row(2)])

    window = Window.order_by(col("a")).range_between(make_interval(days=1), 3)
    with pytest.raises(
        SnowparkSQLException,
        match="Mixing numeric and interval frames is not supported",
    ):
        df.select(count("a").over(window)).collect()

    if not local_testing_mode:
        # Local testing mode can't tell the difference between preceding and following
        window = Window.order_by(col("a")).range_between(
            make_interval(days=0), Window.currentRow
        )
        with pytest.raises(SnowparkSQLException, match="Invalid window frame"):
            df.select(count("a").over(window)).collect()


def test_range_between_negative(session, local_testing_mode):
    df = session.range(10)
    window = Window.order_by("id").range_between(-make_interval(mins=1), 0)
    with pytest.raises(
        SnowparkSQLException,
        match="numeric ORDER BY clause only allows numeric window frame boundaries",
    ):
        df.select(count("id").over(window)).collect()


def test_range_between_negative_local_testing_compatible(session):
    df = session.range(10).with_column("str", lit("string"))

    with pytest.raises(
        ValueError,
        match="start must be an integer or a Column",
    ):
        Window.order_by("id").range_between("-1", 0)

    with pytest.raises(
        ValueError,
        match="end must be an integer or a Column",
    ):
        Window.order_by("id").range_between(0, 1.1)

    with pytest.raises(SnowparkSQLException):
        window = Window.order_by("str").range_between(-1, 1)
        df.select(count("id").over(window)).collect()


def test_rows_between_range_between_literal_offsets(session):
    data = [
        (1, 10),
        (1, 20),
        (1, 30),
        (1, 40),
        (1, 50),
        (2, 1),
        (2, 2),
        (2, 3),
        (2, 4),
        (2, 5),
    ]

    df = session.create_dataframe(data, ["id", "value"])
    window = Window.partition_by("id").order_by("value")

    # Rows between does not differ for id 1 and 2 because it looks row presence only.
    Utils.check_answer(
        df.select(
            "id",
            "value",
            count("value").over(window.rows_between(-1, 1)).alias("count"),
        ),
        [
            Row(2, 5, 2),
            Row(2, 4, 3),
            Row(2, 3, 3),
            Row(2, 2, 3),
            Row(2, 1, 2),
            Row(1, 50, 2),
            Row(1, 40, 3),
            Row(1, 30, 3),
            Row(1, 20, 3),
            Row(1, 10, 2),
        ],
    )
    # Range between differs for id 1 and 2 because it looks at actual value ranges
    Utils.check_answer(
        df.select(
            "id",
            "value",
            count("value").over(window.range_between(-1, 1)).alias("count"),
        ),
        [
            Row(2, 1, 2),
            Row(2, 2, 3),
            Row(2, 3, 3),
            Row(2, 4, 3),
            Row(2, 5, 2),
            Row(1, 10, 1),
            Row(1, 20, 1),
            Row(1, 30, 1),
            Row(1, 40, 1),
            Row(1, 50, 1),
        ],
    )

    Utils.check_answer(
        df.select(
            "id",
            "value",
            count("value")
            .over(window.range_between(Window.unboundedPreceding, 10))
            .alias("count"),
        ),
        [
            Row(2, 1, 5),
            Row(2, 2, 5),
            Row(2, 3, 5),
            Row(2, 4, 5),
            Row(2, 5, 5),
            Row(1, 10, 2),
            Row(1, 20, 3),
            Row(1, 30, 4),
            Row(1, 40, 5),
            Row(1, 50, 5),
        ],
    )

    Utils.check_answer(
        df.select(
            "id",
            "value",
            count("value")
            .over(window.range_between(-10, Window.unboundedFollowing))
            .alias("count"),
        ),
        [
            Row(2, 1, 5),
            Row(2, 2, 5),
            Row(2, 3, 5),
            Row(2, 4, 5),
            Row(2, 5, 5),
            Row(1, 10, 5),
            Row(1, 20, 5),
            Row(1, 30, 4),
            Row(1, 40, 3),
            Row(1, 50, 2),
        ],
    )

    Utils.check_answer(
        df.select(
            "id",
            "value",
            count("value")
            .over(window.range_between(Window.currentRow, 5))
            .alias("count"),
        ),
        [
            Row(2, 1, 5),
            Row(2, 2, 4),
            Row(2, 3, 3),
            Row(2, 4, 2),
            Row(2, 5, 1),
            Row(1, 10, 1),
            Row(1, 20, 1),
            Row(1, 30, 1),
            Row(1, 40, 1),
            Row(1, 50, 1),
        ],
    )
