#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import uuid
from datetime import date, datetime
from decimal import Decimal
from unittest import mock

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import DecimalType

from tests.utils import TestData, Utils

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
    """Projection on the returned LazyFrame yields correct columns/rows.

    Under the current parquet-lazy implementation, projection pushdown
    happens inside pl.scan_parquet at read time, not through a callback
    to the Snowpark DataFrame. We assert the resulting column set + row
    count rather than the specific plumbing.
    """
    df = session.create_dataframe(
        [[i, str(i), i * 1.5] for i in range(20)], schema=["A", "B", "C"]
    )
    result = df.to_polars(lazy=True).select("A").collect()
    assert result.columns == ["A"]
    assert result.height == 20

    result = df.to_polars(lazy=True).select("A", "B").collect()
    assert set(result.columns) == {"A", "B"}
    assert result.height == 20


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
def test_to_polars_lazy_returns_lazyframe(session):
    """Lazy mode returns a polars.LazyFrame; collect() materializes correct data."""
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
    lf = df.to_polars(lazy=True)
    assert isinstance(lf, pl.LazyFrame)
    collected = lf.collect().sort("A")
    assert collected.shape == (2, 2)
    assert collected["A"].to_list() == [1, 3]
    assert collected["B"].to_list() == [2, 4]


@_skip_local
def test_to_polars_statement_params(session):
    """statement_params must reach Snowflake on every to_polars path,
    including the raw-cursor Arrow path. Regression guard: earlier the Arrow
    path forwarded the params only on the (rare) empty-batch fallback and
    silently dropped them on the main query.

    The ``query_history()`` context manager doubles as a check that the
    raw-cursor Arrow path still triggers session-level query listeners.
    """
    df = session.create_dataframe([[1]], schema=["A"])

    tag = "polars_integ_test_" + uuid.uuid4().hex[:8]
    with session.query_history() as history:
        result = df.to_polars(statement_params={"QUERY_TAG": tag})
    assert isinstance(result, pl.DataFrame)
    assert history.queries, "no queries captured by the query listener"
    Utils.assert_executed_with_query_tag(session, tag)

    tag_lazy = "polars_integ_test_lazy_" + uuid.uuid4().hex[:8]
    with session.query_history() as history_lazy:
        result_lazy = df.to_polars(
            lazy=True, statement_params={"QUERY_TAG": tag_lazy}
        ).collect()
    assert isinstance(result_lazy, pl.DataFrame)
    assert history_lazy.queries, "no queries captured on the lazy path"
    Utils.assert_executed_with_query_tag(session, tag_lazy)


# ---------------------------------------------------------------------------
# use_parquet=True (eager parquet)
# ---------------------------------------------------------------------------


@_skip_local
def test_to_polars_use_parquet_basic_types(session):
    """Common primitive types round-trip through the eager Parquet path."""
    df = session.sql(
        "SELECT 42::INT AS i, 'hello'::VARCHAR AS s, TRUE AS b,"
        "       DATE '2024-01-01' AS d"
    )
    pl_df = df.to_polars(use_parquet=True)
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.height == 1
    row = pl_df.to_dicts()[0]
    assert row["I"] == 42
    assert row["S"] == "hello"
    assert row["B"] is True
    assert row["D"] == date(2024, 1, 1)


@_skip_local
def test_to_polars_use_parquet_matches_arrow_shape(session):
    """Eager Parquet and eager Arrow return the same shape on a non-trivial
    dataset. Values are compared in a downcast-aware way: FLOAT is compared
    at float32 precision on both sides."""
    df = session.create_dataframe(
        [[i, str(i), i * 1.5] for i in range(500)], schema=["ID", "S", "F"]
    )
    pq = df.to_polars(use_parquet=True)
    arrow = df.to_polars()
    assert pq.height == arrow.height == 500
    assert set(pq.columns) == set(arrow.columns)
    # FLOAT is downcast to float32 by the Parquet unload, so cast both sides
    # before comparing to avoid a precision-driven false negative.
    assert (
        pq.sort("ID")["F"]
        .cast(pl.Float32)
        .equals(arrow.sort("ID")["F"].cast(pl.Float32))
    )


@_skip_local
def test_to_polars_use_parquet_timestamp_ltz_raises(session):
    """TIMESTAMP_LTZ / TIMESTAMP_TZ can't be unloaded to Parquet — locks in
    the documented behavior so a future change to the doc or code is caught."""
    df = session.sql("SELECT TO_TIMESTAMP_LTZ('2024-01-15 12:00:00 -0800') AS ts_ltz")
    with pytest.raises(Exception, match="(?i)timestamp"):
        df.to_polars(use_parquet=True)


@_skip_local
def test_to_polars_use_parquet_empty(session):
    """Empty result from the Parquet path returns a schema-preserving DataFrame."""
    pl_df = (
        session.create_dataframe([[1, 2]], schema=["A", "B"])
        .filter(col("A") > 100)
        .to_polars(use_parquet=True)
    )
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.height == 0
    assert set(pl_df.columns) == {"A", "B"}


@_skip_local
def test_to_polars_use_parquet_ignored_when_lazy(session):
    """use_parquet=True with lazy=True returns a LazyFrame (parquet is always
    used for lazy; passing use_parquet is a no-op, not an error)."""
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
    lf = df.to_polars(lazy=True, use_parquet=True)
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().sort("A").shape == (2, 2)


# ---------------------------------------------------------------------------
# Predicate correctness — regression coverage
#
# Under the parquet-lazy implementation, predicates are applied by Polars on
# top of scan_parquet; row-group pruning may or may not kick in depending on
# clustering, but the collected result must always match the eager result.
# These tests catch any regression where predicate application drops or
# leaks rows.
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
    """lf.filter().limit(n) — must return up to n rows where the predicate holds.

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
    """lf.head(n).filter(pred) vs lf.filter(pred).head(n) — order matters.

    Semantic contract:
      lf.head(10).filter(VAL>100)  → 0 rows  (first 10 rows all have VAL≤90)
      lf.filter(VAL>100).head(10)  → 10 rows (IDs 11-20)

    Verify both actual row content (not just count) matches native Polars.
    """
    lf = _predicate_df.to_polars(lazy=True)
    eager = _predicate_df.to_polars()

    # Case: head(n).filter(p) — order-sensitive; limit must happen first.
    lazy_limit_first = lf.head(10).filter(pl.col("VAL") > 100).collect().sort("ID")
    eager_limit_first = eager.head(10).filter(pl.col("VAL") > 100).sort("ID")
    assert lazy_limit_first.equals(eager_limit_first)
    assert lazy_limit_first.height == 0  # swapped order would return 10

    # Case: filter(p).head(n) — filter first, then slice.
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
    """Every predicate type must round-trip through the lazy path correctly."""
    predicate = predicate_fn()
    lazy = _predicate_df.to_polars(lazy=True).filter(predicate).collect().sort("ID")
    eager = _predicate_df.to_polars().filter(predicate).sort("ID")
    assert lazy.equals(eager)


# ---------------------------------------------------------------------------
# DML operations Polars applies on top of the parquet scan
#
# group_by, agg, sort, join, distinct, window etc. are executed by the Polars
# engine on the rows the scan produces. These tests verify the lazy result
# matches the eager result for those operations.
# ---------------------------------------------------------------------------


@_skip_local
def test_to_polars_lazy_group_by_agg(_predicate_df):
    """group_by + agg on the lazy result matches the eager result."""
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
    """filter + group_by on the lazy result matches the eager result."""
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
    """Chain touching projection, filter, group_by, agg, filter, sort together."""
    lazy = (
        _predicate_df.to_polars(lazy=True)
        .select("ID", "VAL", "CAT", "SCORE")
        .filter(pl.col("VAL") > 20)
        .group_by("CAT")
        .agg(
            pl.col("SCORE").mean().alias("AVG_SCORE"),
            pl.col("VAL").max().alias("MAX_VAL"),
        )
        .filter(pl.col("AVG_SCORE") > 15.0)
        .sort("CAT")
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
# case of quoted identifiers. Both the Arrow output names and the Parquet
# column names emitted by COPY INTO must respect this — Polars is
# case-sensitive, so any mismatch would surface as ColumnNotFoundError on
# downstream ops.
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
    """Casing is preserved end-to-end through eager Arrow, lazy Parquet scan,
    and column-name-based projection on the LazyFrame."""
    df = session.sql(sql)

    eager = df.to_polars()
    assert eager.columns == expected_columns

    lf = df.to_polars(lazy=True)
    assert lf.collect_schema().names() == expected_columns

    # User selects by the Polars column name they see; that name must resolve
    # against the Parquet schema COPY INTO produced.
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


# ---------------------------------------------------------------------------
# Unit tests for polars_backend helpers
#
# These cover branches that either can't be triggered without a stored proc
# (SnowflakeFile-based opens), or that only fire on transient error paths
# (close failures, empty result edge cases). They run with polars installed
# but do not talk to Snowflake.
# ---------------------------------------------------------------------------


def test_open_stage_files_parallel_empty_paths_returns_empty():
    from snowflake.snowpark._internal.polars_backend import (
        _open_stage_files_parallel,
    )

    for is_sproc in (False, True):
        for is_lazy in (False, True):
            assert (
                _open_stage_files_parallel(
                    mock.MagicMock(), [], is_sproc=is_sproc, is_lazy=is_lazy
                )
                == []
            )


def test_open_stage_files_parallel_lazy_sproc_returns_handles():
    """is_lazy=True + is_sproc=True: returns SnowflakeFile handles without reading."""
    from snowflake.snowpark._internal.polars_backend import (
        _open_stage_files_parallel,
    )

    fake_file = mock.MagicMock()
    mock_module = mock.MagicMock()
    mock_module.SnowflakeFile.open.return_value = fake_file
    with mock.patch.dict("sys.modules", {"snowflake.snowpark.files": mock_module}):
        result = _open_stage_files_parallel(
            None,
            ["@stg/a.parquet", "@stg/b.parquet"],
            is_sproc=True,
            is_lazy=True,
        )

    assert result == [fake_file, fake_file]
    assert mock_module.SnowflakeFile.open.call_count == 2
    mock_module.SnowflakeFile.open.assert_any_call(
        "@stg/a.parquet", "rb", require_scoped_url=False
    )


def test_open_stage_files_parallel_eager_sproc_reads_and_closes():
    """is_lazy=False + is_sproc=True: reads via pl.read_parquet inside a
    ``with`` block so the file handle is closed after decode."""
    from snowflake.snowpark._internal.polars_backend import (
        _open_stage_files_parallel,
    )

    fake_file = mock.MagicMock()
    fake_file.__enter__.return_value = fake_file  # `with f as x: x is f`
    fake_frame = mock.MagicMock()
    mock_files_module = mock.MagicMock()
    mock_files_module.SnowflakeFile.open.return_value = fake_file
    with mock.patch.dict(
        "sys.modules", {"snowflake.snowpark.files": mock_files_module}
    ), mock.patch("polars.read_parquet", return_value=fake_frame) as mock_read:
        result = _open_stage_files_parallel(
            None, ["@stg/a.parquet"], is_sproc=True, is_lazy=False
        )

    assert result == [fake_frame]
    mock_read.assert_called_once_with(fake_file)
    fake_file.__enter__.assert_called_once()
    fake_file.__exit__.assert_called_once()


def test_arrow_eager_empty_result_returns_schema_frame():
    """When to_arrow_batches yields nothing, we fall back to a schema fetch."""
    from snowflake.snowpark._internal import polars_backend

    df = mock.MagicMock()
    df.to_arrow_batches.return_value = iter([])
    empty = mock.MagicMock(name="empty_pl_df")
    with mock.patch.object(
        polars_backend, "_empty_frame_from_schema", return_value=empty
    ) as m:
        result = polars_backend.arrow_eager(df, statement_params=None)
    assert result is empty
    m.assert_called_once()


def test_arrow_eager_uses_to_arrow_batches():
    """arrow_eager delegates entirely to to_arrow_batches (no raw cursor)."""
    import pyarrow as pa
    from snowflake.snowpark._internal import polars_backend

    batch = pa.RecordBatch.from_pydict({"A": [1, 2], "B": [3, 4]})
    df = mock.MagicMock()
    df.to_arrow_batches.return_value = iter([batch])
    result = polars_backend.arrow_eager(df, statement_params=None)
    df.to_arrow_batches.assert_called_once()
    assert result.shape == (2, 2)


def test_parquet_eager_no_paths_returns_schema_frame():
    """COPY INTO produced no files → schema-preserving empty frame."""
    from snowflake.snowpark._internal import polars_backend

    df = mock.MagicMock()
    empty = mock.MagicMock(name="empty_pl_df")
    with mock.patch.object(
        polars_backend, "_copy_df_to_stage", return_value=[]
    ), mock.patch.object(
        polars_backend, "_empty_frame_from_schema", return_value=empty
    ) as m:
        result = polars_backend.parquet_eager(df)
    assert result is empty
    m.assert_called_once()


def test_parquet_eager_no_frames_returns_schema_frame():
    """Paths exist but all reads produce empty list → schema frame."""
    from snowflake.snowpark._internal import polars_backend

    df = mock.MagicMock()
    empty = mock.MagicMock(name="empty_pl_df")
    with mock.patch.object(
        polars_backend, "_copy_df_to_stage", return_value=["@a.parquet"]
    ), mock.patch.object(
        polars_backend, "_open_stage_files_parallel", return_value=[]
    ), mock.patch.object(
        polars_backend, "_empty_frame_from_schema", return_value=empty
    ) as m:
        result = polars_backend.parquet_eager(df)
    assert result is empty
    m.assert_called_once()


def test_parquet_lazy_no_paths_returns_empty_lazyframe():
    """COPY INTO produced no files → empty LazyFrame with the DataFrame's schema."""
    from snowflake.snowpark._internal import polars_backend

    df = mock.MagicMock()
    empty_df = mock.MagicMock()
    empty_df.schema = {"A": pl.Int64}
    with mock.patch.object(
        polars_backend, "_copy_df_to_stage", return_value=[]
    ), mock.patch.object(
        polars_backend, "_empty_frame_from_schema", return_value=empty_df
    ):
        result = polars_backend.parquet_lazy(df)
    assert isinstance(result, pl.LazyFrame)
    assert result.collect_schema().names() == ["A"]


def test_max_workers_forwarded_to_open_helper():
    """max_workers passed to parquet_eager / parquet_lazy reaches _open_stage_files_parallel
    with the correct is_lazy flag."""
    from snowflake.snowpark._internal import polars_backend

    df = mock.MagicMock()
    fake_frame = mock.MagicMock()

    # parquet_eager: is_lazy=False, max_workers forwarded
    with mock.patch.object(
        polars_backend, "_copy_df_to_stage", return_value=["@a.parquet"]
    ), mock.patch.object(
        polars_backend,
        "_open_stage_files_parallel",
        return_value=[fake_frame],
    ) as mock_open:
        polars_backend.parquet_eager(df, max_workers=4)
    mock_open.assert_called_once()
    assert mock_open.call_args.kwargs.get("max_workers") == 4
    assert mock_open.call_args.kwargs.get("is_lazy") is False

    # parquet_lazy: is_lazy=True, max_workers forwarded
    with mock.patch.object(
        polars_backend, "_copy_df_to_stage", return_value=["@a.parquet"]
    ), mock.patch.object(
        polars_backend, "_open_stage_files_parallel", return_value=[mock.MagicMock()]
    ) as mock_open, mock.patch(
        "polars.scan_parquet", return_value=mock.MagicMock()
    ):
        polars_backend.parquet_lazy(df, max_workers=4)
    mock_open.assert_called_once()
    assert mock_open.call_args.kwargs.get("max_workers") == 4
    assert mock_open.call_args.kwargs.get("is_lazy") is True
    assert mock_open.call_args.kwargs.get("max_workers") == 4


@_skip_local
def test_to_polars_max_workers_param(session):
    """max_workers is accepted and produces correct results on the parquet paths."""
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
    pq = df.to_polars(use_parquet=True, max_workers=2)
    assert isinstance(pq, pl.DataFrame) and pq.height == 2
    lf = df.to_polars(lazy=True, max_workers=2)
    assert isinstance(lf, pl.LazyFrame) and lf.collect().height == 2
