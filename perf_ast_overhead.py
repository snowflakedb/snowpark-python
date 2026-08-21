"""
Benchmark: enable_trace_sql_errors_to_dataframe AST overhead

Measures the client-side overhead of the AST tracing feature (UUID comment
injection + protobuf Stmt recording) across two dimensions:

  A) Op-count scaling: 2, 20, 200 DataFrame operations (lightweight query)
  B) Execution-time scaling: queries that run ~1s, ~10s, ~20s on server

Each scenario runs twice: feature OFF, then ON.

Usage:
    python perf_ast_overhead.py
"""

import time

from snowflake.snowpark import Session
from snowflake.snowpark.context import configure_development_features
from snowflake.snowpark.functions import avg, col, count, sum as sum_


# ============================================================
# DIMENSION A: Op-count scaling (lightweight base query)
# ============================================================


def build_ops_chain(session, n_ops):
    """Chain n operations: mix of with_column, filter, select."""
    df = session.sql("SELECT 1 AS a, 2 AS b, 3 AS c")
    for i in range(n_ops):
        if i % 3 == 0:
            df = df.with_column(f"x_{i}", col("a") + i)
        elif i % 3 == 1:
            df = df.filter(col("a") >= 0)
        else:
            df = df.select("*")
    return df


# ============================================================
# DIMENSION B: Heavy server execution (~1s / ~10s / ~20s)
# Rowcounts calibrated on ZYAO_WH (XS warehouse).
# Adjust if using a different warehouse size.
# ============================================================


def build_heavy_1s(session):
    """~1s: 5M rows, group-by aggregate."""
    df = session.sql("""
        SELECT
            SEQ4() AS id,
            UNIFORM(1, 100, RANDOM()) AS grp,
            UNIFORM(1, 10000, RANDOM()) AS val
        FROM TABLE(GENERATOR(rowcount => 5000000))
    """)
    df = df.group_by(col("grp")).agg(
        sum_("val").alias("total"),
        avg("val").alias("average"),
        count("*").alias("cnt"),
    )
    return df


def build_heavy_10s(session):
    """~10s: 750M rows, filter + group-by + sort."""
    df = session.sql("""
        SELECT
            SEQ4() AS id,
            UNIFORM(1, 2000, RANDOM()) AS grp,
            UNIFORM(1, 500000, RANDOM()) AS val,
            UNIFORM(1, 100, RANDOM()) AS category,
            UNIFORM(1, 10, RANDOM()) AS region
        FROM TABLE(GENERATOR(rowcount => 750000000))
    """)
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


def build_heavy_20s(session):
    """~20s: 1.5B rows, filter + group-by + sort."""
    df = session.sql("""
        SELECT
            SEQ4() AS id,
            UNIFORM(1, 2000, RANDOM()) AS grp,
            UNIFORM(1, 500000, RANDOM()) AS val,
            UNIFORM(1, 100, RANDOM()) AS category,
            UNIFORM(1, 10, RANDOM()) AS region
        FROM TABLE(GENERATOR(rowcount => 1500000000))
    """)
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
# Runner
# ============================================================


def set_feature(enabled):
    configure_development_features(
        enable_trace_sql_errors_to_dataframe=enabled,
        enable_dataframe_trace_on_error=False,
    )


def run_scenario(label, build_fn, *build_args):
    results = {}
    for enabled in [False, True]:
        set_feature(enabled)
        t0 = time.perf_counter()
        df = build_fn(*build_args)
        t1 = time.perf_counter()
        df.collect()
        t2 = time.perf_counter()
        tag = "ON" if enabled else "OFF"
        results[tag] = {"client": t1 - t0, "server": t2 - t1, "total": t2 - t0}
    return results


def print_row(label, tag, r, overhead_base=None):
    client = r["client"]
    server = r["server"]
    total = r["total"]
    if overhead_base is not None:
        pct = ((client - overhead_base) / overhead_base * 100) if overhead_base > 0 else 0
        overhead_str = f"+{pct:.1f}%"
    else:
        overhead_str = ""
    print(
        f"| {label:<6} | {tag:<7} | {client:>9.4f} | {server:>9.3f} | {total:>9.3f} | {overhead_str:>10} |"
    )


if __name__ == "__main__":
    session = Session.builder.config("connection_name", "zyao_ent_preprod9").create()
    print(f"Connected. Warehouse: {session.get_current_warehouse()}")

    # --- Dimension A ---
    print("\n" + "=" * 72)
    print("DIMENSION A: Op-count scaling (client-side overhead)")
    print("=" * 72)
    print(
        f"| {'Ops':<6} | {'Feature':<7} | {'Client(s)':>9} | {'Server(s)':>9} | {'Total(s)':>9} | {'Overhead':>10} |"
    )
    print(f"|{'-'*8}|{'-'*9}|{'-'*11}|{'-'*11}|{'-'*11}|{'-'*12}|")

    for n_ops in [2, 20, 200]:
        r = run_scenario(f"{n_ops} ops", build_ops_chain, session, n_ops)
        print_row(str(n_ops), "OFF", r["OFF"])
        print_row(str(n_ops), "ON", r["ON"], overhead_base=r["OFF"]["client"])

    # --- Dimension B ---
    print("\n" + "=" * 72)
    print("DIMENSION B: Execution-time scaling (overhead in context of real work)")
    print("=" * 72)
    print(
        f"| {'Target':<6} | {'Feature':<7} | {'Client(s)':>9} | {'Server(s)':>9} | {'Total(s)':>9} | {'Overhead':>10} |"
    )
    print(f"|{'-'*8}|{'-'*9}|{'-'*11}|{'-'*11}|{'-'*11}|{'-'*12}|")

    for label, fn in [("~1s", build_heavy_1s), ("~10s", build_heavy_10s), ("~20s", build_heavy_20s)]:
        r = run_scenario(label, fn, session)
        print_row(label, "OFF", r["OFF"])
        print_row(label, "ON", r["ON"], overhead_base=r["OFF"]["client"])

    session.close()
    print("\nDone.")
