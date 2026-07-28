#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    from snowflake.snowpark.dataframe import DataFrame

_logger = logging.getLogger(__name__)


# Maximum concurrent workers for parallel I/O (stage-file opens and Arrow
# chunk downloads).
_MAX_WORKERS = 16


def _empty_frame_from_schema(
    df: DataFrame, statement_params: dict[str, str] | None
) -> pl.DataFrame:
    """Return an empty polars DataFrame with the DataFrame's schema."""
    import polars as pl

    return pl.from_arrow(
        df.limit(0).to_arrow(
            statement_params=statement_params,
            _emit_ast=False,
        )
    )


def _open_stage_files_parallel(session, paths: list[str], is_sproc: bool) -> list:
    """Open stage files concurrently and return file-like objects.

    Uses ``SnowflakeFile.open`` inside a stored procedure (no local /tmp
    materialization) and ``session.file.get_stream`` on the client.
    """
    if is_sproc:
        from snowflake.snowpark.files import SnowflakeFile

        def _open(p: str):
            return SnowflakeFile.open(p, "rb", require_scoped_url=False)

    else:

        def _open(p: str):
            return session.file.get_stream(p)

    if not paths:
        return []
    workers = min(_MAX_WORKERS, max(2, len(paths)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(_open, paths))


def _open_and_read_parquet_parallel(
    session, paths: list[str], is_sproc: bool
) -> list[pl.DataFrame]:
    """Open and read each stage Parquet file in a thread pool.

    Opens and decodes run together per file so the read step is not serialized
    after a parallel open — the decode releases the GIL for I/O and native
    Arrow work.
    """
    import polars as pl

    if is_sproc:
        from snowflake.snowpark.files import SnowflakeFile

        def _open_read(p: str):
            f = SnowflakeFile.open(p, "rb", require_scoped_url=False)
            try:
                return pl.read_parquet(f)
            finally:
                try:
                    f.close()
                except Exception as e:
                    _logger.warning("failed to close stage file %s: %s", p, e)

    else:

        def _open_read(p: str):
            f = session.file.get_stream(p)
            try:
                return pl.read_parquet(f)
            finally:
                try:
                    f.close()
                except Exception as e:
                    _logger.warning("failed to close stage file %s: %s", p, e)

    if not paths:
        return []
    workers = min(_MAX_WORKERS, max(2, len(paths)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(_open_read, paths))


def _copy_df_to_stage(
    df: DataFrame,
    sub_prefix: str,
    statement_params: dict[str, str] | None = None,
) -> list[str]:
    """COPY INTO the session stage as Parquet and return the staged file paths.

    Uses ``DataFrame.write.copy_into_location`` so plan compilation and
    statement params go through the same path as other write actions. No
    pre-sort is applied — callers wanting row-group elimination should
    ``.sort(...)`` before ``to_polars``.
    """
    rid = uuid.uuid4().hex[:12]
    stage = df._session.get_session_stage().rstrip("/")
    sub = f"{stage}/{sub_prefix}/{rid}/"
    df.write.copy_into_location(
        sub,
        file_format_type="parquet",
        format_type_options={"COMPRESSION": "SNAPPY"},
        header=True,
        overwrite=True,
        statement_params=statement_params,
    )
    rows = df._session.sql(f"LIST '{sub}'").collect(statement_params=statement_params)
    return [r["name"] if r["name"].startswith("@") else "@" + r["name"] for r in rows]


def arrow_eager(
    df: DataFrame,
    statement_params: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Fetch the result as Arrow batches in parallel and return a polars DataFrame.

    Backs the default ``to_polars()`` path (``lazy=False, use_parquet=False``).
    Preserves full Snowflake type fidelity.

    Fast path (single-query plan): drives the raw cursor and uses
    ``cursor.get_result_batches()`` to download result chunks in parallel.
    Fallback path (multi-query plan — e.g. ``create_dataframe`` requires
    ``CREATE TEMP TABLE`` + ``BatchInsert`` + ``SELECT``): delegates to
    ``DataFrame.to_arrow_batches`` so placeholder substitution and batch
    inserts are handled by Snowpark's execution machinery.
    """
    import polars as pl

    if len(df._plan.queries) > 1:
        return _arrow_eager_fallback(df, statement_params)

    # Execute via Snowpark's server_connection so query listeners fire, but
    # keep the returned cursor so we can call get_result_batches() for the
    # parallel fetch. statement_params flows through the same `_statement_params`
    # kwarg run_query uses.
    server_conn = df._session._conn
    main_query = df._plan.queries[-1]
    cur = server_conn.execute_and_notify_query_listener(
        main_query.sql,
        params=(main_query.params if getattr(main_query, "params", None) else None),
        _statement_params=statement_params,
    )

    batches = cur.get_result_batches() or []
    if not batches:
        return _empty_frame_from_schema(df, statement_params)

    workers = min(_MAX_WORKERS, max(2, len(batches)))
    conn = server_conn._conn

    def _fetch(b):
        return b.to_arrow(connection=conn)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        tables = list(ex.map(_fetch, batches))

    parts = [pl.from_arrow(t) for t in tables if t is not None and t.num_rows > 0]
    if not parts:
        return _empty_frame_from_schema(df, statement_params)
    return pl.concat(parts) if len(parts) > 1 else parts[0]


def _arrow_eager_fallback(
    df: DataFrame, statement_params: dict[str, str] | None
) -> pl.DataFrame:
    """Serial-iterator fallback used when the plan has multiple queries."""
    import polars as pl

    parts = [
        pl.from_arrow(batch)
        for batch in df.to_arrow_batches(
            statement_params=statement_params, _emit_ast=False
        )
    ]
    if not parts:
        return _empty_frame_from_schema(df, statement_params)
    return pl.concat(parts) if len(parts) > 1 else parts[0]


def parquet_eager(
    df: DataFrame,
    is_sproc: bool = False,
    statement_params: dict[str, str] | None = None,
) -> pl.DataFrame:
    """COPY INTO Parquet, read the staged files in parallel, return a polars DataFrame.

    Backs ``to_polars(use_parquet=True)``. Subject to the Parquet type-fidelity
    caveats documented on ``DataFrame.to_polars``.
    """
    import polars as pl

    paths = _copy_df_to_stage(df, "to_polars_parquet_eager", statement_params)
    if not paths:
        return _empty_frame_from_schema(df, statement_params)
    frames = _open_and_read_parquet_parallel(df._session, paths, is_sproc)
    if not frames:
        return _empty_frame_from_schema(df, statement_params)
    return pl.concat(frames) if len(frames) > 1 else frames[0]


def parquet_lazy(
    df: DataFrame,
    is_sproc: bool = False,
    statement_params: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """COPY INTO Parquet, open the staged files, return a ``pl.scan_parquet`` LazyFrame.

    Backs ``to_polars(lazy=True)``. COPY INTO runs synchronously; only the
    scan/decode is deferred. Subject to the Parquet type-fidelity caveats
    documented on ``DataFrame.to_polars``.
    """
    import polars as pl

    paths = _copy_df_to_stage(df, "to_polars_parquet_lazy", statement_params)
    if not paths:
        return pl.LazyFrame(
            schema=_empty_frame_from_schema(df, statement_params).schema
        )
    # The stream objects are owned by the returned LazyFrame; Polars closes them
    # when the scan is materialized (or if the LazyFrame is discarded).
    streams = _open_stage_files_parallel(df._session, paths, is_sproc)
    return pl.scan_parquet(streams)
