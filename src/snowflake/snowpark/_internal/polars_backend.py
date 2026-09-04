#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    from snowflake.snowpark.dataframe import DataFrame


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


def _open_stage_files_parallel(
    session,
    paths: list[str],
    is_sproc: bool,
    max_workers: int | None = None,
) -> list:
    """Open stage files concurrently, reading each with ``pl.read_parquet``
    and returning the resulting ``pl.DataFrame`` (``with`` closes the handle
    after read).

    Uses ``SnowflakeFile.open`` inside a stored procedure and
    ``session.file.get_stream`` on the client.
    """
    if not paths:
        return []

    import polars as pl

    if is_sproc:
        from snowflake.snowpark.files import SnowflakeFile

        def opener(p: str):
            return SnowflakeFile.open(p, "rb", require_scoped_url=False)

    else:
        opener = session.file.get_stream

    def _work(p: str):
        with opener(p) as f:
            return pl.read_parquet(f)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        return list(ex.map(_work, paths))


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
    # `sub` is fully qualified; LIST returns stage-relative names. Construct fully-qualified paths.
    return [sub + row["name"].rsplit("/", 1)[-1] for row in rows]


def arrow_eager(
    df: DataFrame,
    statement_params: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Fetch the result as Arrow batches and return a polars DataFrame.

    Backs the default ``to_polars()`` path (``use_parquet=False``).
    Preserves full Snowflake type fidelity.
    """
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
    max_workers: int | None = None,
) -> pl.DataFrame:
    """COPY INTO Parquet, read the staged files in parallel, return a polars DataFrame.

    Backs ``to_polars(use_parquet=True)``. Subject to the Parquet type-fidelity
    caveats documented on ``DataFrame.to_polars``.
    """
    import polars as pl

    paths = _copy_df_to_stage(df, "to_polars_parquet_eager", statement_params)
    if not paths:
        return _empty_frame_from_schema(df, statement_params)
    frames = _open_stage_files_parallel(
        df._session, paths, is_sproc, max_workers=max_workers
    )
    if not frames:
        return _empty_frame_from_schema(df, statement_params)
    return pl.concat(frames) if len(frames) > 1 else frames[0]
