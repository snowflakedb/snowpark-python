#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Integration tests for the polars interop surface.

Mirrors ``test_df_to_arrow.py`` and exercises the four-API symmetry promised
in the design doc: ``DataFrame.to_polars`` / ``DataFrame.to_polars_batches``,
``Session.write_polars``, and ``Session.create_dataframe(pl.DataFrame)``.
"""

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Iterator
from unittest import mock

import pytest

from snowflake.snowpark.functions import col
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import DecimalType

from tests.utils import TestData, Utils

try:
    import polars as pl
    import pyarrow as pa  # noqa: F401
    import pandas  # noqa: F401
except ImportError:
    pytest.skip("polars / pyarrow / pandas not available", allow_module_level=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def basic_polars_df():
    yield pl.DataFrame({"a": [1, 2, 3]})


@pytest.fixture(scope="module")
def polars_df():
    yield pl.DataFrame({"A": [1, 2, 3], "B": ["a", "b", "c"], "C": [1.0, 1.11, 0.0]})


# ---------------------------------------------------------------------------
# DataFrame.to_polars
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
@pytest.mark.parametrize(
    "example,expected",
    [
        (TestData.integer1, {"A": [1, 2, 3]}),
        (
            TestData.null_data1,
            {"A": [None, Decimal("2"), Decimal("1"), Decimal("3"), None]},
        ),
        (
            TestData.double1,
            {"A": [Decimal("1.111"), Decimal("2.222"), Decimal("3.333")]},
        ),
        (TestData.string1, {"A": ["test1", "test2", "test3"], "B": ["a", "b", "c"]}),
        (
            TestData.datetime_primitives2,
            {
                "TIMESTAMP": [
                    datetime(9999, 12, 31, 0, 0, 0, 123456),
                    datetime(1583, 1, 1, 23, 59, 59, 567890),
                ]
            },
        ),
        (
            TestData.date1,
            {
                "A": [date(2020, 8, 1), date(2010, 12, 1)],
                "B": [Decimal("1"), Decimal("2")],
            },
        ),
    ],
)
def test_to_polars(session, example, expected):
    df = example(session)
    pl_df = df.to_polars()
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.to_dict(as_series=False) == expected


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_decimal_precision(session):
    """Verify high-precision decimals survive the arrow -> polars hop."""
    data = [
        [1111111111111111111, 222222222222222222],
        [3333333333333333333, 444444444444444444],
        [9223372036854775807, 111111111111111111],
        [-9223372036854775808, 999999999999999999],
    ]
    df = session.create_dataframe(data, schema=["A", "B"]).select(
        col("A").cast(DecimalType(38, 0)).alias("A"),
        col("B").cast(DecimalType(18, 0)).alias("B"),
    )

    pl_df = df.to_polars()
    assert isinstance(pl_df, pl.DataFrame)
    # polars maps fixed-precision decimals to pl.Decimal and ints to pl.Int64.
    assert pl_df.schema["A"] == pl.Decimal(38, 0)
    assert pl_df.schema["B"] == pl.Int64
    rows = pl_df.to_dicts()
    assert [[int(r["A"]), int(r["B"])] for r in rows] == data


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_batches(session):
    df = session.range(100000).cache_result()
    iterator = df.to_polars_batches()
    assert isinstance(iterator, Iterator)

    pl_df = df.to_polars()
    pl_batches = list(iterator)
    assert len(pl_batches) > 1
    assert all(isinstance(b, pl.DataFrame) for b in pl_batches)
    # concat should reconstruct the eager result row-for-row
    pl_concat = pl.concat(pl_batches)
    assert pl_concat.to_dict(as_series=False) == pl_df.to_dict(as_series=False)


def test_to_polars_not_installed_raises(session):
    """When polars is missing, the error should be loud and actionable."""
    df = session.create_dataframe([(1,)], schema=["A"])
    with mock.patch("snowflake.snowpark.dataframe.installed_polars", False):
        with pytest.raises(ModuleNotFoundError, match=r"polars.*\[polars\]"):
            df.to_polars()
        with pytest.raises(ModuleNotFoundError, match=r"polars.*\[polars\]"):
            list(df.to_polars_batches())


def test_to_polars_does_not_expose_block_param(session):
    """block was dropped in v1 to avoid shipping a NotImplementedError-only kwarg."""
    df = session.create_dataframe([(1,)], schema=["A"])
    with pytest.raises(TypeError, match="block"):
        df.to_polars(block=False)
    with pytest.raises(TypeError, match="block"):
        list(df.to_polars_batches(block=False))


# ---------------------------------------------------------------------------
# Session.create_dataframe(pl.DataFrame)
# ---------------------------------------------------------------------------


def test_create_dataframe_from_polars_round_trip(session):
    """pl -> Snowpark -> pl should preserve content."""
    pl_df = pl.DataFrame({"A": [1, 2, 3]})
    sp_df = session.create_dataframe(pl_df)
    pl_round_trip = sp_df.to_polars()
    assert pl_round_trip.to_dict(as_series=False) == pl_df.to_dict(as_series=False)


def test_create_dataframe_from_polars_check_answer(session):
    """Compare against the Row-based equivalent to confirm the upload path."""
    pl_df = pl.DataFrame({"A": [1, 2, 3]})
    sp_df = session.create_dataframe(pl_df)
    expected = session.create_dataframe([(1,), (2,), (3,)], schema=["A"])
    Utils.check_answer(sp_df, expected)


def test_create_dataframe_from_polars_warns_with_polars_label(session):
    """The schema-ignored warning should name the original input format (polars)."""
    pl_df = pl.DataFrame({"A": [1, 2, 3]})
    schema = ["different_name"]
    with pytest.warns(UserWarning, match="polars DataFrame"):
        session.create_dataframe(pl_df, schema=schema)


def test_create_dataframe_polars_works_without_pandas(session):
    """Regression: the polars path must not hard-gate on installed_pandas.

    Earlier code routed pl.DataFrame through ``installed_pandas and isinstance(...)``
    which would TypeError in a polars-only env. Simulate that by patching the
    pandas guard off and confirming the upload path still succeeds.
    """
    pl_df = pl.DataFrame({"A": [1, 2, 3]})
    with mock.patch("snowflake.snowpark.session.installed_pandas", False):
        sp_df = session.create_dataframe(pl_df)
        Utils.check_answer(sp_df, [Row(1), Row(2), Row(3)])


# ---------------------------------------------------------------------------
# Session.write_polars
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_write_polars_overwrite_auto_create(session, basic_polars_df):
    table_name = Utils.random_table_name()
    try:
        # First call: auto-create + 3 rows
        session.write_polars(basic_polars_df, table_name, auto_create_table=True)
        Utils.check_answer(session.table(table_name), [Row(1), Row(2), Row(3)])

        # Second call: append
        session.write_polars(basic_polars_df, table_name, auto_create_table=True)
        Utils.check_answer(
            session.table(table_name),
            [Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)],
        )

        # Third call: truncate + replace
        session.write_polars(
            basic_polars_df, table_name, auto_create_table=True, overwrite=True
        )
        Utils.check_answer(session.table(table_name), [Row(1), Row(2), Row(3)])

        # Overwrite without auto_create still replaces
        session.write_polars(
            basic_polars_df, table_name, auto_create_table=False, overwrite=True
        )
        Utils.check_answer(session.table(table_name), [Row(1), Row(2), Row(3)])
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
@pytest.mark.parametrize("table_type", ["", "TEMPORARY", "TRANSIENT"])
def test_write_polars_table_type(session, polars_df, table_type):
    table_name = Utils.random_table_name()
    try:
        session.write_polars(
            polars_df, table_name, auto_create_table=True, table_type=table_type
        )
        expected_in_ddl = table_type or "replace TABLE"
        ddl = session._run_query(f"select get_ddl('table', '{table_name}')")
        assert expected_in_ddl in ddl[0][0]
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_write_polars_returns_snowpark_table(session, basic_polars_df):
    table_name = Utils.random_table_name()
    try:
        table = session.write_polars(
            basic_polars_df, table_name, auto_create_table=True
        )
        # write_polars returns a Snowpark Table pointing at the written rows.
        assert table.columns == ['"a"']
        Utils.check_answer(table, [Row(1), Row(2), Row(3)])
    finally:
        Utils.drop_table(session, table_name)


def test_write_polars_not_installed_raises(session, basic_polars_df):
    table_name = Utils.random_table_name()
    with mock.patch("snowflake.snowpark.session.installed_polars", False):
        with pytest.raises(ModuleNotFoundError, match="polars"):
            session.write_polars(basic_polars_df, table_name, auto_create_table=True)


# ---------------------------------------------------------------------------
# Round-trip sanity check across the whole surface
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_full_round_trip_via_write_polars(session):
    """polars -> write_polars -> session.table -> to_polars -> polars."""
    table_name = Utils.random_table_name()
    pl_df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["a", "b", "c", "d"],
            "score": [1.5, 2.5, 3.5, 4.5],
        }
    )
    try:
        session.write_polars(pl_df, table_name, auto_create_table=True)
        round_trip = session.table(table_name).to_polars()
        # write_arrow uppercases unquoted identifiers; rename back for comparison.
        round_trip = round_trip.rename(
            {c: c.lower() for c in round_trip.columns if c.isupper()}
        )
        # write_arrow may re-order rows; compare as sorted dicts.
        assert sorted(round_trip.to_dicts(), key=lambda r: r["id"]) == sorted(
            pl_df.to_dicts(), key=lambda r: r["id"]
        )
    finally:
        Utils.drop_table(session, table_name)


# avoid unused-import pruning
_ = math
