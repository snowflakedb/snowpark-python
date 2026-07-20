#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from datetime import date, datetime
from decimal import Decimal
from unittest import mock

import pytest

from snowflake.snowpark.functions import col
from snowflake.snowpark.types import DecimalType

from tests.utils import TestData

try:
    import polars as pl
except ImportError:
    pytest.skip("polars not available", allow_module_level=True)


def polars_to_pydict(df: "pl.DataFrame") -> dict:
    return df.to_arrow().to_pydict()


# ---------------------------------------------------------------------------
# Core type-family correctness
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
        pytest.param(
            TestData.array1,
            {
                "ARR1": ["[\n  1,\n  2,\n  3\n]", "[\n  6,\n  7,\n  8\n]"],
                "ARR2": ["[\n  3,\n  4,\n  5\n]", "[\n  9,\n  0,\n  1\n]"],
            },
            id="semi-structured array",
        ),
        pytest.param(
            TestData.object2,
            {
                "OBJ": [
                    '{\n  "age": 21,\n  "name": "Joe",\n  "zip": 21021\n}',
                    '{\n  "age": 26,\n  "name": "Jay",\n  "zip": 94021\n}',
                ],
                "K": ["age", "key"],
                "V": [Decimal("0"), Decimal("0")],
                "FLAG": [True, False],
            },
            id="semi-structured object",
        ),
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
def test_to_polars_type_correctness(session, example, expected):
    df = example(session)
    assert polars_to_pydict(df.to_polars()) == expected


# ---------------------------------------------------------------------------
# Decimal / NUMBER precision
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_decimal_precision(session):
    data = [
        [1111111111111111111, 222222222222222222],
        [3333333333333333333, 444444444444444444],
        [5555555555555555555, 666666666666666666],
        [7777777777777777777, 888888888888888888],
        [9223372036854775807, 111111111111111111],
        [2222222222222222222, 333333333333333333],
        [4444444444444444444, 555555555555555555],
        [6666666666666666666, 777777777777777777],
        [-9223372036854775808, 999999999999999999],
    ]
    df = session.create_dataframe(data, schema=["A", "B"]).select(
        col("A").cast(DecimalType(38, 0)).alias("A"),
        col("B").cast(DecimalType(18, 0)).alias("B"),
    )
    pl_df = df.to_polars()
    pa_df = pl_df.to_arrow()
    assert str(pa_df.schema[0].type) == "decimal128(38, 0)"
    assert str(pa_df.schema[1].type) == "int64"
    assert [[int(x) for x in row.values()] for row in pa_df.to_pylist()] == data


# ---------------------------------------------------------------------------
# NULL handling
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_null_integer_column(session):
    df = session.create_dataframe([[0], [1], [None]], schema=["A"])
    col_values = df.to_polars()["A"].to_list()
    assert col_values[0] == 0
    assert col_values[1] == 1
    assert col_values[2] is None


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_all_null_column(session):
    df = session.create_dataframe([[None], [None], [None]], schema=["A"])
    pl_df = df.to_polars()
    assert pl_df.height == 3
    assert all(v is None for v in pl_df["A"].to_list())


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_empty_dataframe(session):
    df = session.create_dataframe([[1, 2]], schema=["A", "B"]).filter(col("A") > 100)
    pl_df = df.to_polars()
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.height == 0
    assert set(pl_df.columns) == {"A", "B"}


# ---------------------------------------------------------------------------
# Eager path
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_eager_returns_dataframe(session):
    df = session.create_dataframe([[1, "a"], [2, "b"]], schema=["A", "B"])
    result = df.to_polars()
    assert isinstance(result, pl.DataFrame)
    assert result.shape == (2, 2)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_eager_multi_batch(session):
    """Exercises the pl.concat(parts) path when the result spans multiple Arrow batches."""
    n = 100_000
    pl_df = session.range(n).to_polars()
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.height == n
    assert pl_df.columns == ["ID"]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_timestamp_ltz_and_tz(session):
    """TIMESTAMP_LTZ and TIMESTAMP_TZ survive the Arrow path (returned as tz-aware datetimes)."""
    df = session.sql(
        "SELECT "
        "TO_TIMESTAMP_LTZ('2024-01-15 12:00:00 -0800') AS ts_ltz, "
        "TO_TIMESTAMP_TZ('2024-01-15 20:00:00 +0530') AS ts_tz"
    )
    pl_df = df.to_polars()
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.height == 1
    assert pl_df["TS_LTZ"][0] is not None
    assert pl_df["TS_TZ"][0] is not None
    assert pl_df["TS_LTZ"].dtype.time_zone is not None
    assert pl_df["TS_TZ"].dtype.time_zone is not None


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_eager_matches_to_arrow(session):
    df = session.sql(
        "SELECT 42::INT AS i, 3.14::FLOAT AS f, 'hello'::VARCHAR AS s, "
        "TRUE AS b, DATE '2024-01-01' AS d, "
        "TO_TIMESTAMP_NTZ('2024-01-01 12:00:00') AS t"
    )
    assert polars_to_pydict(df.to_polars()) == df.to_arrow().to_pydict()


# ---------------------------------------------------------------------------
# Lazy path
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_lazy_returns_lazyframe(session):
    df = session.create_dataframe([[1, "a"], [2, "b"]], schema=["A", "B"])
    lf = df.to_polars(lazy=True)
    assert isinstance(lf, pl.LazyFrame)
    assert set(lf.collect_schema().names()) == {"A", "B"}


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_lazy_collect_matches_eager(session):
    df = session.create_dataframe(
        [[1, "a", 1.5], [2, "b", 2.5], [3, "c", 3.5]], schema=["A", "B", "C"]
    )
    assert df.to_polars().sort("A").equals(df.to_polars(lazy=True).collect().sort("A"))


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_lazy_type_correctness(session):
    df = TestData.datetime_primitives2(session)
    expected = {
        "TIMESTAMP": [
            datetime(9999, 12, 31, 0, 0, 0, 123456),
            datetime(1583, 1, 1, 23, 59, 59, 567890),
        ]
    }
    assert polars_to_pydict(df.to_polars(lazy=True).collect()) == expected


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_lazy_projection_pushdown(session):
    df = session.create_dataframe(
        [[i, str(i), i * 1.5] for i in range(20)], schema=["A", "B", "C"]
    )
    with mock.patch.object(
        type(df), "select", autospec=True, side_effect=type(df).select
    ) as spy:
        # Single-column projection
        result = df.to_polars(lazy=True).select("A").collect()
        assert result.columns == ["A"]
        assert result.height == 20
        called_col_sets = [
            {str(a).strip('"') for a in call.args[1:]} for call in spy.mock_calls
        ]
        assert any({"A"} == s for s in called_col_sets)

    with mock.patch.object(
        type(df), "select", autospec=True, side_effect=type(df).select
    ) as spy:
        # Multi-column projection
        result = df.to_polars(lazy=True).select("A", "B").collect()
        assert set(result.columns) == {"A", "B"}
        assert result.height == 20
        called_col_sets = [
            {str(a).strip('"') for a in call.args[1:]} for call in spy.mock_calls
        ]
        assert any({"A", "B"} == s for s in called_col_sets)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_lazy_projection_pushdown_quoted_identifiers(session):
    """Mixed-case quoted Snowflake identifiers survive projection pushdown without being uppercased."""
    df = session.sql('SELECT 1 AS "myInt", \'hello\' AS "myStr"')
    result = df.to_polars(lazy=True).select("myInt").collect()
    assert result.columns == ["myInt"]
    assert result.height == 1
    assert result[0, 0] == 1


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_lazy_limit_pushdown(session):
    df = session.create_dataframe([[i, str(i)] for i in range(50)], schema=["A", "B"])
    assert df.to_polars(lazy=True).head(5).collect().height == 5


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_lazy_construction_does_not_fetch_batches(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
    with mock.patch.object(
        type(df),
        "to_arrow_batches",
        autospec=True,
        side_effect=type(df).to_arrow_batches,
    ) as batches_spy:
        lf = df.to_polars(lazy=True)
        assert isinstance(lf, pl.LazyFrame)
        assert batches_spy.call_count == 0
        lf.collect()
        assert batches_spy.call_count == 1


# ---------------------------------------------------------------------------
# statement_params
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)
def test_to_polars_statement_params(session):
    df = session.create_dataframe([[1]], schema=["A"])
    params = {"QUERY_TAG": "polars_integ_test"}
    assert isinstance(df.to_polars(statement_params=params), pl.DataFrame)
    assert isinstance(
        df.to_polars(lazy=True, statement_params=params).collect(), pl.DataFrame
    )


# ---------------------------------------------------------------------------
# Missing dependency
# ---------------------------------------------------------------------------


def test_to_polars_raises_when_polars_missing(session):
    df = session.create_dataframe([[1]], schema=["A"])
    with mock.patch.dict("sys.modules", {"polars": None}):
        with pytest.raises(ModuleNotFoundError, match="polars"):
            df.to_polars()
