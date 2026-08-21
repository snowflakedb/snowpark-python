"""
Benchmark: enable_trace_sql_errors_to_dataframe AST overhead

Full cross-product: feature (OFF/ON) × ops (2/20/200) × query weight (~1s/~10s/~20s)
= 18 runs total.

Measures:
  - Client time: DataFrame plan build (where UUID comment injection lives)
  - Server time: .collect() execution
  - Overhead: % increase in client time with feature ON vs OFF

Usage:
    python perf_ast_overhead.py
"""

import time

from snowflake.snowpark import Session
from snowflake.snowpark.context import configure_development_features
from snowflake.snowpark.functions import avg, col, count, sum as sum_


# ============================================================
# Base query builders (produce server-time weight)
# Rowcounts calibrated on ZYAO_WH (XS warehouse).
# ============================================================


def base_query_1s(session):
    """~1s: 5M rows, group-by aggregate."""
    df = session.sql(
        """
        SELECT
            SEQ4() AS id,
            UNIFORM(1, 100, RANDOM()) AS grp,
            UNIFORM(1, 10000, RANDOM()) AS val
        FROM TABLE(GENERATOR(rowcount => 5000000))
    """
    )
    df = df.group_by(col("grp")).agg(
        sum_("val").alias("total"),
        avg("val").alias("average"),
        count("*").alias("cnt"),
    )
    return df


def base_query_10s(session):
    """~10s: 750M rows, filter + group-by + sort."""
    df = session.sql(
        """
        SELECT
            SEQ4() AS id,
            UNIFORM(1, 2000, RANDOM()) AS grp,
            UNIFORM(1, 500000, RANDOM()) AS val,
            UNIFORM(1, 100, RANDOM()) AS category,
            UNIFORM(1, 10, RANDOM()) AS region
        FROM TABLE(GENERATOR(rowcount => 750000000))
    """
    )
    df = df.filter(col("val") > 500)
    df = df.group_by(col("grp"), col("category"), col("region")).agg(
        sum_("val").alias("total"),
        avg("val").alias("average"),
        count("*").alias("cnt"),
    )
    df = df.filter(col("cnt") > 5)
    df = df.with_column("normalized", col("total") / col("cnt"))
    df = df.sort(col("normalized").desc())
    df = df.limit(10000)
    return df


def base_query_20s(session):
    """~20s: 1.5B rows, filter + group-by + sort."""
    df = session.sql(
        """
        SELECT
            SEQ4() AS id,
            UNIFORM(1, 2000, RANDOM()) AS grp,
            UNIFORM(1, 500000, RANDOM()) AS val,
            UNIFORM(1, 100, RANDOM()) AS category,
            UNIFORM(1, 10, RANDOM()) AS region
        FROM TABLE(GENERATOR(rowcount => 1500000000))
    """
    )
    df = df.filter(col("val") > 500)
    df = df.group_by(col("grp"), col("category"), col("region")).agg(
        sum_("val").alias("total"),
        avg("val").alias("average"),
        count("*").alias("cnt"),
    )
    df = df.filter(col("cnt") > 5)
    df = df.with_column("normalized", col("total") / col("cnt"))
    df = df.sort(col("normalized").desc())
    df = df.limit(10000)
    return df


# ============================================================
# Op chain stacker
# ============================================================


def add_ops(df, n_ops):
    """Stack n lightweight ops on top of a DataFrame."""
    for i in range(n_ops):
        if i % 3 == 0:
            df = df.with_column(f"x_{i}", col("total") + i)
        elif i % 3 == 1:
            df = df.filter(col("cnt") >= 0)
        else:
            df = df.select("*")
    return df


# ============================================================
# Runner
# ============================================================


def set_feature(enabled):
    configure_development_features(
        enable_trace_sql_errors_to_dataframe=enabled,
        enable_dataframe_trace_on_error=False,
    )


if __name__ == "__main__":
    session = Session.builder.config("connection_name", "zyao_ent_preprod9").create()
    print(f"Connected. Warehouse: {session.get_current_warehouse()}")  # noqa: T201

    # Warmup
    session.sql("SELECT 1").collect()

    bases = [
        ("~1s", base_query_1s),
        ("~10s", base_query_10s),
        ("~20s", base_query_20s),
    ]
    ops_levels = [2, 20, 200]

    # Print header
    print(f"\n{'=' * 80}")  # noqa: T201
    print(  # noqa: T201
        "Feature (OFF/ON) × Ops (2/20/200) × Query Weight (~1s/~10s/~20s)"
    )
    print(f"{'=' * 80}")  # noqa: T201
    print(  # noqa: T201
        f"| {'Query':<6} | {'Ops':<4} | {'Feature':<7} | {'Client(s)':>9} "
        f"| {'Server(s)':>9} | {'Total(s)':>9} | {'Client OH':>10} |"
    )
    print(  # noqa: T201
        f"|{'-' * 8}|{'-' * 6}|{'-' * 9}|{'-' * 11}|{'-' * 11}|{'-' * 11}|{'-' * 12}|"
    )

    for label, base_fn in bases:
        for n_ops in ops_levels:
            results = {}
            for enabled in [False, True]:
                set_feature(enabled)
                t0 = time.perf_counter()
                df = base_fn(session)
                df = add_ops(df, n_ops)
                t1 = time.perf_counter()
                df.collect()
                t2 = time.perf_counter()
                tag = "ON" if enabled else "OFF"
                results[tag] = {"client": t1 - t0, "server": t2 - t1, "total": t2 - t0}

            # Print OFF row
            r_off = results["OFF"]
            print(  # noqa: T201
                f"| {label:<6} | {n_ops:<4} | {'OFF':<7} | {r_off['client']:>9.4f} "
                f"| {r_off['server']:>9.3f} | {r_off['total']:>9.3f} | {'':>10} |"
            )
            # Print ON row with overhead
            r_on = results["ON"]
            if r_off["client"] > 0:
                pct = (r_on["client"] - r_off["client"]) / r_off["client"] * 100
                overhead_str = f"+{pct:.1f}%"
            else:
                overhead_str = "N/A"
            print(  # noqa: T201
                f"| {label:<6} | {n_ops:<4} | {'ON':<7} | {r_on['client']:>9.4f} "
                f"| {r_on['server']:>9.3f} | {r_on['total']:>9.3f} | {overhead_str:>10} |"
            )

    session.close()
    print("\nDone.")  # noqa: T201
