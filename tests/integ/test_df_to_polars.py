#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from datetime import date, datetime
from decimal import Decimal
from unittest import mock

import pyarrow as pa
import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import DecimalType

from tests.utils import TestData

try:
    import polars as pl

    _polars_available = True
except ImportError:
    pl = None  # type: ignore[assignment]
    _polars_available = False

# Shorthand for tests that require a live Snowflake connection + Arrow support.
_skip_local = pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="arrow not fully supported by local testing.",
)


@pytest.fixture
def _no_polars_required():
    """Request this fixture to opt out of the polars-availability skip."""


@pytest.fixture(autouse=True)
def _require_polars(request):
    """Skip tests that need polars when it is not installed.

    Tests that mock polars or test the missing-dep path should request
    the ``_no_polars_required`` fixture to opt out.
    """
    if not _polars_available and "_no_polars_required" not in request.fixturenames:
        pytest.skip("polars not available")


def polars_to_pydict(df: "pl.DataFrame") -> dict:
    return df.to_arrow().to_pydict()


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def local_session():
    """Local-testing session — no Snowflake connection required."""
    with Session.builder.config("local_testing", True).create() as s:
        yield s


def _mock_pl():
    """Minimal polars mock for unit tests that run without polars installed."""
    pl_mock = mock.MagicMock(name="polars")
    mock_frame = mock.MagicMock(name="DataFrame")
    mock_frame.schema = {"A": mock.MagicMock(), "B": mock.MagicMock()}
    pl_mock.from_arrow.return_value = mock_frame
    pl_mock.concat.return_value = mock_frame
    mock_lf = mock.MagicMock(name="LazyFrame")
    pl_mock.io.plugins.register_io_source.return_value = mock_lf
    return pl_mock, mock_frame, mock_lf


_BATCH = pa.RecordBatch.from_pydict({"A": [1, 2], "B": ["a", "b"]})


# ---------------------------------------------------------------------------
# Type correctness (eager)
# ---------------------------------------------------------------------------


@_skip_local
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
    assert polars_to_pydict(example(session).to_polars()) == expected


@_skip_local
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
    pa_df = df.to_polars().to_arrow()
    assert str(pa_df.schema[0].type) == "decimal128(38, 0)"
    assert str(pa_df.schema[1].type) == "int64"
    assert [[int(x) for x in row.values()] for row in pa_df.to_pylist()] == data


# ---------------------------------------------------------------------------
# NULL handling
# ---------------------------------------------------------------------------


@_skip_local
def test_to_polars_null_handling(session):
    # Nullable integer column
    col_values = (
        session.create_dataframe([[0], [1], [None]], schema=["A"])
        .to_polars()["A"]
        .to_list()
    )
    assert col_values == [0, 1, None]

    # All-null column
    pl_df = session.create_dataframe([[None], [None], [None]], schema=["A"]).to_polars()
    assert pl_df.height == 3
    assert all(v is None for v in pl_df["A"].to_list())


# ---------------------------------------------------------------------------
# Eager path
# ---------------------------------------------------------------------------


@_skip_local
def test_to_polars_eager_multi_batch(session):
    """Exercises the pl.concat(parts) path when the result spans multiple Arrow batches."""
    pl_df = session.range(100_000).to_polars()
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.height == 100_000
    assert pl_df.columns == ["ID"]


@_skip_local
def test_to_polars_empty_dataframe(session):
    pl_df = (
        session.create_dataframe([[1, 2]], schema=["A", "B"])
        .filter(col("A") > 100)
        .to_polars()
    )
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.height == 0
    assert set(pl_df.columns) == {"A", "B"}


@_skip_local
def test_to_polars_timestamp_ltz_and_tz(session):
    """TIMESTAMP_LTZ and TIMESTAMP_TZ survive the Arrow path as tz-aware datetimes."""
    pl_df = session.sql(
        "SELECT TO_TIMESTAMP_LTZ('2024-01-15 12:00:00 -0800') AS ts_ltz,"
        "       TO_TIMESTAMP_TZ('2024-01-15 20:00:00 +0530')  AS ts_tz"
    ).to_polars()
    assert (
        pl_df["TS_LTZ"][0] is not None and pl_df["TS_LTZ"].dtype.time_zone is not None
    )
    assert pl_df["TS_TZ"][0] is not None and pl_df["TS_TZ"].dtype.time_zone is not None


@_skip_local
def test_to_polars_eager_matches_to_arrow(session):
    df = session.sql(
        "SELECT 42::INT AS i, 3.14::FLOAT AS f, 'hello'::VARCHAR AS s,"
        "       TRUE AS b, DATE '2024-01-01' AS d,"
        "       TO_TIMESTAMP_NTZ('2024-01-01 12:00:00') AS t"
    )
    assert polars_to_pydict(df.to_polars()) == df.to_arrow().to_pydict()


# ---------------------------------------------------------------------------
# Lazy path
# ---------------------------------------------------------------------------


@_skip_local
def test_to_polars_lazy_collect_matches_eager(session):
    df = session.create_dataframe(
        [[1, "a", 1.5], [2, "b", 2.5], [3, "c", 3.5]], schema=["A", "B", "C"]
    )
    lf = df.to_polars(lazy=True)
    assert isinstance(lf, pl.LazyFrame)
    assert df.to_polars().sort("A").equals(lf.collect().sort("A"))


@_skip_local
def test_to_polars_lazy_projection_pushdown(session):
    df = session.create_dataframe(
        [[i, str(i), i * 1.5] for i in range(20)], schema=["A", "B", "C"]
    )
    with mock.patch.object(
        type(df), "select", autospec=True, side_effect=type(df).select
    ) as spy:
        result = df.to_polars(lazy=True).select("A").collect()
        assert result.columns == ["A"] and result.height == 20
        assert any(
            {"A"} == {str(a).strip('"') for a in c.args[1:]} for c in spy.mock_calls
        )

    with mock.patch.object(
        type(df), "select", autospec=True, side_effect=type(df).select
    ) as spy:
        result = df.to_polars(lazy=True).select("A", "B").collect()
        assert set(result.columns) == {"A", "B"} and result.height == 20
        assert any(
            {"A", "B"} == {str(a).strip('"') for a in c.args[1:]}
            for c in spy.mock_calls
        )


@_skip_local
def test_to_polars_lazy_projection_pushdown_quoted_identifiers(session):
    """Mixed-case quoted identifiers survive projection pushdown without being uppercased."""
    result = (
        session.sql('SELECT 1 AS "myInt", \'hello\' AS "myStr"')
        .to_polars(lazy=True)
        .select("myInt")
        .collect()
    )
    assert result.columns == ["myInt"] and result.height == 1 and result[0, 0] == 1


@_skip_local
def test_to_polars_lazy_limit_pushdown(session):
    df = session.create_dataframe([[i, str(i)] for i in range(50)], schema=["A", "B"])
    assert df.to_polars(lazy=True).head(5).collect().height == 5


@_skip_local
def test_to_polars_lazy_construction_does_not_fetch_batches(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
    with mock.patch.object(
        type(df),
        "to_arrow_batches",
        autospec=True,
        side_effect=type(df).to_arrow_batches,
    ) as spy:
        lf = df.to_polars(lazy=True)
        assert isinstance(lf, pl.LazyFrame) and spy.call_count == 0
        lf.collect()
        assert spy.call_count == 1


@_skip_local
def test_to_polars_statement_params(session):
    df = session.create_dataframe([[1]], schema=["A"])
    params = {"QUERY_TAG": "polars_integ_test"}
    assert isinstance(df.to_polars(statement_params=params), pl.DataFrame)
    assert isinstance(
        df.to_polars(lazy=True, statement_params=params).collect(), pl.DataFrame
    )


# ---------------------------------------------------------------------------
# Unit coverage — no Snowflake, polars injected via mock
# These run regardless of whether polars is installed.
# ---------------------------------------------------------------------------


def test_to_polars_eager_unit_multi_batch_concat(local_session, _no_polars_required):
    """pl.concat() is called when to_arrow_batches yields more than one batch."""
    pl_mock, mock_frame, _ = _mock_pl()
    df = local_session.create_dataframe([[1, "a"], [2, "b"]], schema=["A", "B"])
    batch2 = pa.RecordBatch.from_pydict({"A": [3], "B": ["c"]})
    with mock.patch.dict("sys.modules", {"polars": pl_mock}):
        with mock.patch.object(
            type(df), "to_arrow_batches", return_value=[_BATCH, batch2]
        ):
            result = df.to_polars()
    pl_mock.concat.assert_called_once()
    assert result is mock_frame


def test_to_polars_eager_unit_no_batches_fallback(local_session, _no_polars_required):
    """When to_arrow_batches yields nothing, falls back to limit(0).to_arrow() for schema."""
    pl_mock, _, _ = _mock_pl()
    schema_table = pa.Table.from_pydict({"A": pa.array([], type=pa.int64())})
    df = local_session.create_dataframe([[1]], schema=["A"])
    with mock.patch.dict("sys.modules", {"polars": pl_mock}):
        with mock.patch.object(type(df), "to_arrow_batches", return_value=[]):
            with mock.patch.object(type(df), "to_arrow", return_value=schema_table):
                df.to_polars()
    pl_mock.from_arrow.assert_called_once_with(schema_table)


def test_to_polars_lazy_unit_scan_executes(local_session, _no_polars_required):
    """_scan body yields pl.from_arrow(batch) for each batch returned by to_arrow_batches."""
    pl_mock, _, mock_lf = _mock_pl()
    schema_table = pa.Table.from_pydict({"A": [1], "B": ["a"]})
    df = local_session.create_dataframe([[1, "a"]], schema=["A", "B"])

    captured = {}
    pl_mock.io.plugins.register_io_source.side_effect = (
        lambda fn, schema: captured.update(fn=fn) or mock_lf
    )
    with mock.patch.dict("sys.modules", {"polars": pl_mock}):
        with mock.patch.object(type(df), "to_arrow", return_value=schema_table):
            df.to_polars(lazy=True)

    with mock.patch.object(type(df), "to_arrow_batches", return_value=[_BATCH]):
        assert (
            len(
                list(
                    captured["fn"](
                        with_columns=None, predicate=None, n_rows=None, batch_size=None
                    )
                )
            )
            == 1
        )


def test_to_polars_lazy_unit_scan_pushdowns(local_session, _no_polars_required):
    """_scan calls select() for with_columns projection and limit() for n_rows."""
    pl_mock, _, mock_lf = _mock_pl()
    schema_table = pa.Table.from_pydict({"A": [1], "B": ["a"]})
    df = local_session.create_dataframe([[1, "a"]], schema=["A", "B"])

    captured = {}
    pl_mock.io.plugins.register_io_source.side_effect = (
        lambda fn, schema: captured.update(fn=fn) or mock_lf
    )
    with mock.patch.dict("sys.modules", {"polars": pl_mock}):
        with mock.patch.object(type(df), "to_arrow", return_value=schema_table):
            df.to_polars(lazy=True)

    col_batch = pa.RecordBatch.from_pydict({"A": [1]})
    with mock.patch.object(type(df), "select", wraps=df.select) as sel_spy:
        with mock.patch.object(type(df), "limit", wraps=df.limit) as lim_spy:
            with mock.patch.object(
                type(df), "to_arrow_batches", return_value=[col_batch]
            ):
                list(
                    captured["fn"](
                        with_columns=["A"], predicate=None, n_rows=5, batch_size=None
                    )
                )
    sel_spy.assert_called_once()
    lim_spy.assert_called_once_with(5)


# ---------------------------------------------------------------------------
# Predicate correctness — regression coverage
#
# Polars passes the predicate to _scan and REMOVES it from its own plan
# (parsed_predicate_success=True after deserialize). If _scan does not apply
# it, rows leak through unfiltered. _scan therefore MUST apply
# df.filter(predicate) per batch. These tests would fail if that ever regressed.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _predicate_df(session):
    """24-row dataset used across predicate/DML tests."""
    rows = [
        [i, i * 10, ["A", "B", "C", "D"][i % 4], round(i * 1.5, 1)] for i in range(24)
    ]
    return session.create_dataframe(rows, schema=["ID", "VAL", "CAT", "SCORE"])


@_skip_local
def test_to_polars_lazy_predicate_applied(_predicate_df):
    """Simple filter: lazy result must match eager result."""
    lf = _predicate_df.to_polars(lazy=True)
    eager = _predicate_df.to_polars()
    got = lf.filter(pl.col("VAL") > 100).collect().sort("ID")
    want = eager.filter(pl.col("VAL") > 100).sort("ID")
    assert got.equals(want)


@_skip_local
def test_to_polars_lazy_filter_then_limit_correctness(_predicate_df):
    """lf.filter().limit(n) — the bug case: without _scan applying the predicate,
    Polars removes the filter from its plan and returns unfiltered rows.

    Correctness rule: filter().limit(n) → up to n rows where predicate holds.
    """
    lf = _predicate_df.to_polars(lazy=True)
    filtered = lf.filter(pl.col("VAL") > 100).collect()
    limited = lf.filter(pl.col("VAL") > 100).head(3).collect()
    assert limited.height <= 3
    # Every returned row must satisfy the predicate.
    assert (limited["VAL"] > 100).all()
    # Every returned row must be a subset of the full filter result.
    assert set(limited["ID"].to_list()).issubset(set(filtered["ID"].to_list()))


@_skip_local
def test_to_polars_lazy_limit_then_filter_differs(_predicate_df):
    """lf.head(n).filter(pred) — the ONLY case where Polars pushes BOTH n_rows
    AND predicate to _scan simultaneously. _scan must apply LIMIT first (in SQL),
    then filter (in Python). If the order were swapped, this test would return
    10 rows instead of 0 on this dataset.

    Semantic contract:
      lf.head(10).filter(VAL>100)  → 0 rows  (first 10 rows all have VAL≤90)
      lf.filter(VAL>100).head(10)  → 10 rows (IDs 11-20)

    Verify both actual row content (not just count) matches native Polars.
    """
    lf = _predicate_df.to_polars(lazy=True)
    eager = _predicate_df.to_polars()

    # Case: head(n).filter(p) — BOTH pushed to _scan; order-sensitive path
    lazy_limit_first = lf.head(10).filter(pl.col("VAL") > 100).collect().sort("ID")
    eager_limit_first = eager.head(10).filter(pl.col("VAL") > 100).sort("ID")
    assert lazy_limit_first.equals(eager_limit_first)
    assert lazy_limit_first.height == 0  # ← wrong _scan order would give 10 here

    # Case: filter(p).head(n) — only predicate pushed; Polars slices post-scan
    lazy_filter_first = lf.filter(pl.col("VAL") > 100).head(10).collect().sort("ID")
    eager_filter_first = eager.filter(pl.col("VAL") > 100).head(10).sort("ID")
    assert lazy_filter_first.equals(eager_filter_first)
    assert lazy_filter_first.height == 10

    # Regression guard: the two orderings must not accidentally converge.
    assert lazy_limit_first.height != lazy_filter_first.height


@_skip_local
@pytest.mark.parametrize(
    "predicate_fn",
    [
        lambda: (pl.col("VAL") > 100) & (pl.col("CAT") == "A"),
        lambda: (pl.col("VAL") < 50) | (pl.col("VAL") > 180),
        lambda: pl.col("CAT").is_in(["B", "D"]),
        lambda: pl.col("SCORE").is_between(10.0, 25.0),
        lambda: pl.col("CAT") != "A",
    ],
    ids=["and", "or", "is_in", "between", "not_eq"],
)
def test_to_polars_lazy_predicate_variety(_predicate_df, predicate_fn):
    """Every predicate type must round-trip through _scan correctly."""
    predicate = predicate_fn()
    lazy = _predicate_df.to_polars(lazy=True).filter(predicate).collect().sort("ID")
    eager = _predicate_df.to_polars().filter(predicate).sort("ID")
    assert lazy.equals(eager)


# ---------------------------------------------------------------------------
# DML operations Polars applies AFTER _scan yields batches
#
# _scan only knows about (with_columns, predicate, n_rows, batch_size).
# Everything else — group_by, agg, sort, join, distinct, window — is applied
# by the Polars engine on the yielded batches. These tests verify Polars can
# correctly consume our streamed batches for those operations.
# ---------------------------------------------------------------------------


@_skip_local
def test_to_polars_lazy_group_by_agg(_predicate_df):
    """group_by + agg is executed by Polars on batches yielded by _scan."""
    lazy = (
        _predicate_df.to_polars(lazy=True)
        .group_by("CAT")
        .agg(
            pl.col("VAL").sum().alias("TOTAL"),
            pl.col("SCORE").mean().alias("AVG"),
            pl.len().alias("CNT"),
        )
        .collect()
        .sort("CAT")
    )
    eager = (
        _predicate_df.to_polars()
        .group_by("CAT")
        .agg(
            pl.col("VAL").sum().alias("TOTAL"),
            pl.col("SCORE").mean().alias("AVG"),
            pl.len().alias("CNT"),
        )
        .sort("CAT")
    )
    assert lazy.equals(eager)


@_skip_local
def test_to_polars_lazy_filter_then_group_by(_predicate_df):
    """filter (pushed to _scan) + group_by (executed post-scan by Polars)."""
    lazy = (
        _predicate_df.to_polars(lazy=True)
        .filter(pl.col("VAL") > 50)
        .group_by("CAT")
        .agg(pl.len().alias("CNT"))
        .collect()
        .sort("CAT")
    )
    eager = (
        _predicate_df.to_polars()
        .filter(pl.col("VAL") > 50)
        .group_by("CAT")
        .agg(pl.len().alias("CNT"))
        .sort("CAT")
    )
    assert lazy.equals(eager)


@_skip_local
def test_to_polars_lazy_group_by_then_filter_on_agg(_predicate_df):
    """Filter on an aggregated column — Polars keeps this filter in its own plan
    because the column doesn't exist in the scan source."""
    lazy = (
        _predicate_df.to_polars(lazy=True)
        .group_by("CAT")
        .agg(pl.col("VAL").sum().alias("TOTAL"))
        .filter(pl.col("TOTAL") > 500)
        .collect()
        .sort("CAT")
    )
    eager = (
        _predicate_df.to_polars()
        .group_by("CAT")
        .agg(pl.col("VAL").sum().alias("TOTAL"))
        .filter(pl.col("TOTAL") > 500)
        .sort("CAT")
    )
    assert lazy.equals(eager)


@_skip_local
def test_to_polars_lazy_window_over(_predicate_df):
    """Window/partition_by via .over() — Polars applies these post-scan."""
    lazy = (
        _predicate_df.to_polars(lazy=True)
        .with_columns(
            pl.col("VAL").sum().over("CAT").alias("CAT_TOTAL"),
            pl.col("VAL").rank().over("CAT").alias("RANK_IN_CAT"),
        )
        .collect()
        .sort("ID")
    )
    eager = (
        _predicate_df.to_polars()
        .with_columns(
            pl.col("VAL").sum().over("CAT").alias("CAT_TOTAL"),
            pl.col("VAL").rank().over("CAT").alias("RANK_IN_CAT"),
        )
        .sort("ID")
    )
    assert lazy.equals(eager)


@_skip_local
def test_to_polars_lazy_unique_and_sort(_predicate_df):
    """unique + sort are executed by Polars on scan output."""
    lazy = (
        _predicate_df.to_polars(lazy=True).select("CAT").unique().sort("CAT").collect()
    )
    eager = _predicate_df.to_polars().select("CAT").unique().sort("CAT")
    assert lazy.equals(eager)


@_skip_local
def test_to_polars_lazy_join_between_lazyframes(session, _predicate_df):
    """Two LazyFrames from separate Snowpark scans, joined by Polars."""
    lookup = session.create_dataframe(
        [["A", "alpha"], ["B", "beta"], ["C", "gamma"], ["D", "delta"]],
        schema=["CAT", "LABEL"],
    )
    left = _predicate_df.to_polars(lazy=True)
    right = lookup.to_polars(lazy=True)
    lazy = left.join(right, on="CAT", how="inner").collect().sort("ID")

    left_e = _predicate_df.to_polars()
    right_e = lookup.to_polars()
    eager = left_e.join(right_e, on="CAT", how="inner").sort("ID")
    assert lazy.equals(eager)


@_skip_local
def test_to_polars_lazy_multi_step_chain(_predicate_df):
    """Chain touching every pushdown + post-scan operation together."""
    lazy = (
        _predicate_df.to_polars(lazy=True)
        .select("ID", "VAL", "CAT", "SCORE")  # projection → _scan
        .filter(pl.col("VAL") > 20)  # predicate → _scan
        .group_by("CAT")  # post-scan
        .agg(
            pl.col("SCORE").mean().alias("AVG_SCORE"),
            pl.col("VAL").max().alias("MAX_VAL"),
        )
        .filter(pl.col("AVG_SCORE") > 15.0)  # post-scan
        .sort("CAT")  # post-scan
        .collect()
    )
    eager = (
        _predicate_df.to_polars()
        .select("ID", "VAL", "CAT", "SCORE")
        .filter(pl.col("VAL") > 20)
        .group_by("CAT")
        .agg(
            pl.col("SCORE").mean().alias("AVG_SCORE"),
            pl.col("VAL").max().alias("MAX_VAL"),
        )
        .filter(pl.col("AVG_SCORE") > 15.0)
        .sort("CAT")
    )
    assert lazy.equals(eager)


# ---------------------------------------------------------------------------
# Column identifier casing (unquoted / quoted lowercase / quoted mixed-case)
#
# Snowflake folds unquoted identifiers to UPPERCASE, but preserves the exact
# case of quoted identifiers. Both Arrow output names and the with_columns
# pushdown path inside _scan must respect this. Polars is case-sensitive, so
# any mismatch would surface as ColumnNotFoundError on downstream ops.
# ---------------------------------------------------------------------------


@_skip_local
@pytest.mark.parametrize(
    "sql, expected_columns, pushdown_col",
    [
        # unquoted → Snowflake folds to uppercase
        ("SELECT 1 AS revenue, 'US' AS region", ["REVENUE", "REGION"], "REVENUE"),
        # quoted lowercase → preserved
        ('SELECT 1 AS "revenue", \'US\' AS "region"', ["revenue", "region"], "revenue"),
        # quoted mixed-case → preserved
        (
            'SELECT 1 AS "myRevenue", \'US\' AS "myRegion"',
            ["myRevenue", "myRegion"],
            "myRevenue",
        ),
    ],
    ids=["unquoted_folds_uppercase", "quoted_lowercase", "quoted_mixed_case"],
)
def test_to_polars_column_identifier_casing(
    session, sql, expected_columns, pushdown_col
):
    """Casing is preserved end-to-end through eager, lazy, and _scan projection pushdown."""
    df = session.sql(sql)

    eager = df.to_polars()
    assert eager.columns == expected_columns

    lf = df.to_polars(lazy=True)
    assert lf.collect_schema().names() == expected_columns

    # Round-trip via _scan's quote_name() path — user selects by the Polars column
    # name (which is what they see); _scan must translate that back to a Snowflake
    # identifier that resolves to the same column.
    projected = lf.select(pushdown_col).collect()
    assert projected.columns == [pushdown_col] and projected.height == 1


# ---------------------------------------------------------------------------
# Missing dependency
# ---------------------------------------------------------------------------


def test_to_polars_raises_when_polars_missing(local_session, _no_polars_required):
    df = local_session.create_dataframe([[1]], schema=["A"])
    with mock.patch.dict("sys.modules", {"polars": None}):
        with pytest.raises(ModuleNotFoundError, match="polars"):
            df.to_polars()
