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
    """Casing is preserved end-to-end through eager Arrow and eager Parquet,
    and column-name-based selection on the Parquet result."""
    df = session.sql(sql)

    eager = df.to_polars()
    assert eager.columns == expected_columns

    pq = df.to_polars(use_parquet=True)
    assert pq.columns == expected_columns

    # User selects by the Polars column name they see; that name must resolve
    # against the Parquet schema COPY INTO produced.
    projected = pq.select(pushdown_col)
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
        assert _open_stage_files_parallel(mock.MagicMock(), [], is_sproc=is_sproc) == []


def test_open_stage_files_parallel_eager_sproc_reads_and_closes():
    """is_sproc=True: reads via pl.read_parquet inside a ``with`` block so the
    file handle is closed after decode."""
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
        result = _open_stage_files_parallel(None, ["@stg/a.parquet"], is_sproc=True)

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


def test_max_workers_forwarded_to_open_helper():
    """max_workers passed to parquet_eager reaches _open_stage_files_parallel."""
    from snowflake.snowpark._internal import polars_backend

    df = mock.MagicMock()
    fake_frame = mock.MagicMock()

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


@_skip_local
def test_to_polars_max_workers_param(session):
    """max_workers is accepted and produces correct results on the parquet path."""
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
    pq = df.to_polars(use_parquet=True, max_workers=2)
    assert isinstance(pq, pl.DataFrame) and pq.height == 2
