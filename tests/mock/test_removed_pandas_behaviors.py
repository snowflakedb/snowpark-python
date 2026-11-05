#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# pandas 3 removed implicit behaviors that local testing relied on: positional fallback for
# Series[int], implicit string parsing in datetime isin, NaN (not pd.NA) in object
# arithmetic, ns-only datetime inference and the float64 fallback of to_numeric.

import datetime

import pandas as pd
import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, concat_ws, date_trunc, dateadd, lit
from snowflake.snowpark.types import DateType, LongType, StructField, StructType

# Computed here instead of imported from snowpark, in line with the other test files.
IS_PANDAS_3 = int(pd.__version__.split(".")[0]) >= 3
# Some fixes only apply on pandas 3, to keep pandas 2 results unchanged.
pandas_3_only = pytest.mark.skipif(
    not IS_PANDAS_3, reason="pandas 2 keeps its existing behavior here"
)


def test_concat_ws_with_column_separator(session):
    df = session.create_dataframe(
        [["-", "a", "b"], ["+", "c", None]], schema=["sep", "a", "b"]
    )
    assert df.select(concat_ws(col("sep"), col("a"), col("b"))).collect() == [
        Row("a-b"),
        Row(None),
    ]


@pandas_3_only
def test_arithmetic_after_filter_keeps_null_rows(session):
    df = session.create_dataframe(
        [[1, None], [2, 2], [3, None], [4, 4]], schema=["a", "b"]
    )
    result = df.filter(col("a") % 2 == 1).select(col("a") + col("b")).collect()
    assert result == [Row(None), Row(None)]


def test_isin_parses_strings_and_dates_for_timestamps(session):
    df = session.create_dataframe(
        [[datetime.datetime(2024, 1, 1, 10)], [datetime.datetime(2024, 1, 2, 10)]],
        schema=["ts"],
    )
    day = date_trunc("day", col("ts"))
    assert df.select(day.isin("2024-01-02"), ~day.isin("2024-01-02")).collect() == [
        Row(False, True),
        Row(True, False),
    ]
    assert df.select(day.isin(lit(datetime.date(2024, 1, 2)))).collect() == [
        Row(False),
        Row(True),
    ]
    candidates = session.create_dataframe([["2024-01-02"]], schema=["s"])
    assert df.select(day.isin(candidates)).collect() == [Row(False), Row(True)]


def test_arithmetic_with_null_operand(session):
    schema = StructType([StructField("b", LongType()), StructField("c", LongType())])
    df = session.create_dataframe([[10, 2], [None, 2], [6, 3]], schema=schema)
    division = [r[0] for r in df.select(col("b") / col("c")).collect()]
    assert division[0] == 5 and division[2] == 2
    modulo = [r[0] for r in df.select(col("b") % col("c")).collect()]
    assert modulo[0] == 0 and modulo[2] == 0


@pandas_3_only
def test_dateadd_on_null_date(session):
    df = session.create_dataframe(
        [[datetime.date(2024, 1, 31)], [None]],
        schema=StructType([StructField("d", DateType())]),
    )
    for unit, expected in (
        ("year", datetime.date(2025, 1, 31)),
        ("quarter", datetime.date(2024, 4, 30)),
        ("month", datetime.date(2024, 2, 29)),
        ("day", datetime.date(2024, 2, 1)),
    ):
        assert df.select(dateadd(unit, lit(1), col("d"))).collect() == [
            Row(expected),
            Row(None),
        ]


def test_create_dataframe_with_timestamp_outside_ns_range(session):
    pdf = pd.DataFrame({"A": [datetime.datetime(3000, 1, 1)]})
    assert session.create_dataframe(pdf).collect() == [
        Row(A=datetime.datetime(3000, 1, 1))
    ]


def test_to_pandas_integer_beyond_int64(session):
    schema = StructType([StructField("a", LongType())])
    df = session.create_dataframe([[2**70]], schema=schema)
    assert str(df.to_pandas()["A"].dtype) == "float64"
