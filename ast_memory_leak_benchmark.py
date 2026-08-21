#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""
Benchmark: memory leak from enable_trace_sql_errors_to_dataframe.

Enabling the feature forces AST collection on (session.py:767-768), which makes AstBatch
retain two dicts that are never pruned during a session:

  _bind_stmt_cache: dict[int, proto.Stmt]   one protobuf Stmt per bind(), i.e. per DataFrame op
                                            (batch.py:151)
  _dependency_cache: dict[int, set[int]]    direct dependency ids per bind, filled in
                                            cur_stmts_closure() (batch.py:198-204), which only
                                            runs via to_request() <- flush(), i.e. only on actions

The only caller of AstBatch.clear() is Session.close() (session.py:995), so entries survive the
whole session even after every user reference to the producing DataFrame is dropped.

WORKLOAD
    Each iteration builds a fresh DataFrame from a 5-row VALUES literal, stacks N ops on it (default
    100), calls .collect(), then drops every local reference. Anything still resident is therefore
    held by the caches, not by live objects -- that is what makes this a leak and not retention.

    Iterations are paced to a fixed cadence (default one collect every 3s) against an absolute
    schedule, so the measured bytes/second is a real rate rather than the benchmark's saturation
    ceiling. Iterations that cannot keep up are counted as overruns and the achieved cadence is
    reported instead of the requested one. Pass --collect-interval 0 to run unthrottled.

METRIC
    protobuf here is the upb C implementation, so Stmt payloads live in C arenas that tracemalloc
    and sys.getsizeof cannot see. VmRSS from /proc/self/status is the only ground truth.

    Because both phases run in one process and glibc does not return freed pages, the ON phase
    starts on a heap already grown by the OFF phase. So the metric is a least-squares fit of RSS vs
    cumulative ops *within* each phase, which is immune to a constant offset:

        leak_rate = slope_ON - slope_OFF     (bytes per DataFrame op)

    R2 is reported per phase so a noisy or plateauing fit is visible rather than laundered into a
    rate.

    Because the cadence is fixed, RSS is also fitted against elapsed time, giving MB/hour directly
    instead of multiplying bytes/op by an assumed op rate.

DURATION
    Both phases run for --phase-duration seconds (default 600). A pilot first times a few
    unthrottled iterations and warns if the requested cadence is not achievable at this op count.

Usage:
    python ast_memory_leak_benchmark.py                 # 100 ops, 1 collect/3s, ~21 min
    python ast_memory_leak_benchmark.py --smoke         # 20 ops, 1 collect/1s, ~2.5 min
    python ast_memory_leak_benchmark.py --collect-interval 0    # unthrottled (old behaviour)
"""

import argparse
import gc
import json
import os
import resource
import statistics
import sys
import time

from snowflake.snowpark import Session
from snowflake.snowpark.context import configure_development_features
from snowflake.snowpark.functions import col, lit, when

# Per-bind cost is NOT a single constant -- it tracks the op mix, because build_pipeline cycles
# i % 3 and with_column (a when/otherwise expression tree) is roughly 3-8x larger than select("*")
# or filter. Measured live: 4201 B/bind at 5 ops/iter (20% with_column) and 5725 B/bind at 20
# ops/iter (30%). The share converges (33% at 100 ops), so the 20-op figure is the better prior for
# any n_ops >= 20. Used only for the up-front "Expect:" line; every reported figure comes from the
# run itself.
#
# Independently corroborated offline: 62,760 real bind Stmts from tests/ast/data/*.test loaded into
# a dict cost 2184 B/entry -- a 13.5x amplification over their 161 B mean serialized size, from the
# upb arena + Python wrapper + dict slot -- plus 345-416 B/entry for _dependency_cache. That corpus
# has its own (lighter) op mix, which is why it lands below the figures above.
PREDICTED_BYTES_PER_BIND = 5725

# Minimum checkpoints needed before a least-squares fit is meaningful.
MIN_CHECKPOINTS_FOR_FIT = 8

BASE_VALUES_SQL = """
    SELECT column1 AS NAME, column2 AS AGE, column3 AS DEPT FROM VALUES
    ('Alice', 30, 'Engineering'),
    ('Bob', 25, 'Marketing'),
    ('Carol', 35, 'Engineering'),
    ('Dave', 28, 'Marketing'),
    ('Eve', 32, 'Engineering')
"""


def emit(*parts: object) -> None:
    """Single funnel for all output, so flake8's T201 is suppressed in exactly one place."""
    print(*parts)  # noqa: T201


# ============================================================
# RSS sampling
# ============================================================

_rss_fallback_warned = False


def read_rss_bytes() -> int:
    """Current resident set size in bytes.

    Prefers VmRSS from /proc/self/status (current RSS). Falls back to ru_maxrss, which is a *peak*
    and so cannot show memory being released -- warned about once if it is ever used.
    """
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    _, value, unit = line.split()
                    assert unit == "kB", f"expected VmRSS in kB, got {unit!r}"
                    return int(value) * 1024
    except OSError:
        pass

    global _rss_fallback_warned
    if not _rss_fallback_warned:
        _rss_fallback_warned = True
        emit(
            "WARNING: /proc/self/status unavailable; falling back to ru_maxrss, which reports PEAK "
            "RSS only. Growth will still be visible but any release will not be."
        )
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def read_peak_rss_bytes() -> int:
    """Peak RSS in bytes from VmHWM, for cross-checking read_rss_bytes(). 0 if unavailable."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def sample_rss() -> int:
    """RSS after a full collection, so checkpoints are deterministic."""
    gc.collect()
    return read_rss_bytes()


# ============================================================
# Least-squares fit
# ============================================================


def linear_fit(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """Ordinary least squares. Returns (slope, intercept, r_squared)."""
    n = len(xs)
    if n < 2:
        return 0.0, 0.0, 0.0

    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    if sxx == 0:
        return 0.0, mean_y, 0.0
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))

    slope = sxy / sxx
    intercept = mean_y - slope * mean_x

    syy = sum((y - mean_y) ** 2 for y in ys)
    if syy == 0:
        # A perfectly flat series is a perfect fit of a zero-slope line.
        r_squared = 1.0
    else:
        residual = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
        r_squared = 1.0 - residual / syy

    return slope, intercept, r_squared


# ============================================================
# Workload
# ============================================================


def build_pipeline(df, n_ops: int):
    """Stack n_ops transformations on df. Each op produces one AstBatch bind."""
    for i in range(n_ops):
        if i % 3 == 0:
            df = df.select("*")
        elif i % 3 == 1:
            df = df.filter(col("AGE") >= 0)
        else:
            df = df.with_column(
                f"x_{i}", when(col("AGE") > lit(i), col("AGE") + i).otherwise(lit(0))
            )
    return df


def one_iteration(session: Session, n_ops: int) -> None:
    """Fresh DataFrame, n_ops ops, one action, then every local reference is dropped."""
    df = session.sql(BASE_VALUES_SQL)
    df = build_pipeline(df, n_ops)
    df.collect()
    # df falls out of scope here. Whatever stays resident is held by AstBatch, not by the caller.


def cache_sizes(session: Session) -> tuple[int, int]:
    batch = session._ast_batch
    return len(batch._bind_stmt_cache), len(batch._dependency_cache)


def set_feature(enabled: bool) -> None:
    configure_development_features(
        enable_trace_sql_errors_to_dataframe=enabled,
    )


# ============================================================
# Phase runner
# ============================================================

HEADER = (
    f"| {'Elapsed':>8} | {'Ops':>7} | {'RSS (MB)':>9} | {'dRSS (MB)':>10} "
    f"| {'bind$':>8} | {'dep$':>8} | {'ops/s':>6} |"
)
RULE = f"|{'-' * 10}|{'-' * 9}|{'-' * 11}|{'-' * 12}|{'-' * 10}|{'-' * 10}|{'-' * 8}|"


def run_phase(
    session: Session,
    label: str,
    feature_on: bool,
    n_ops: int,
    checkpoint_every: int,
    phase_duration: float,
    collect_interval: float,
) -> dict:
    """Run one phase for phase_duration seconds and return its checkpoints plus fitted slopes.

    If collect_interval > 0, iterations are paced against an absolute schedule so the i-th collect
    starts at phase_start + i * collect_interval. Iterations that cannot keep up are counted as
    overruns rather than silently stretching the cadence.
    """
    set_feature(feature_on)

    ast_on = session.ast_enabled
    binds, deps = cache_sizes(session)

    emit("")
    emit("=" * 84)
    emit(f"PHASE {label}  (enable_trace_sql_errors_to_dataframe={feature_on})")
    emit("=" * 84)
    emit(f"  session.ast_enabled = {ast_on}")
    emit(f"  entering with bind_stmt_cache={binds} dependency_cache={deps}")

    if not feature_on and ast_on:
        emit("")
        emit("  " + "!" * 76)
        emit(
            "  ! WARNING: AST is enabled during the OFF phase. The server can turn it on\n"
            "  ! independently via set_ast_state(AstFlagSource.SERVER, ...) (session.py:759),\n"
            "  ! so this baseline is NOT AST-free and the OFF/ON contrast is not meaningful."
        )
        emit("  " + "!" * 76)
    if feature_on and not ast_on:
        emit("")
        emit(
            "  ! WARNING: the feature flag did not enable AST collection. The ON phase will not\n"
            "  ! exercise the caches and the measured leak rate will be ~0."
        )

    emit("")
    emit(HEADER)
    emit(RULE)

    checkpoints: list[dict] = []
    baseline_rss = sample_rss()
    phase_start = time.perf_counter()
    last_time = phase_start
    last_ops = 0
    total_ops = 0
    iterations = 0
    overruns = 0
    total_lateness = 0.0
    first_iter_start = 0.0
    last_iter_start = 0.0

    while True:
        # Pace against an absolute schedule, NOT sleep(interval) after the work. A fixed delay
        # would make the period interval + build_time (e.g. 3 + 2.3 = 5.3s), which would corrupt
        # every per-second figure derived from this run. Absolute deadlines also stop drift from
        # accumulating across hundreds of iterations.
        if collect_interval > 0:
            deadline = phase_start + iterations * collect_interval
            now = time.perf_counter()
            if now < deadline:
                time.sleep(deadline - now)
            elif iterations:
                overruns += 1
                total_lateness += now - deadline

        last_iter_start = time.perf_counter()
        if not iterations:
            first_iter_start = last_iter_start

        one_iteration(session, n_ops)
        iterations += 1
        total_ops += n_ops

        if iterations % checkpoint_every:
            if time.perf_counter() - phase_start >= phase_duration:
                break
            continue

        now = time.perf_counter()
        rss = sample_rss()
        binds, deps = cache_sizes(session)
        elapsed = now - phase_start
        window = now - last_time
        ops_per_s = (total_ops - last_ops) / window if window > 0 else 0.0
        last_time, last_ops = now, total_ops

        checkpoints.append(
            {
                "elapsed_s": elapsed,
                "ops": total_ops,
                "rss_bytes": rss,
                "bind_stmt_cache": binds,
                "dependency_cache": deps,
                "ops_per_s": ops_per_s,
                "overruns": overruns,
            }
        )

        emit(
            f"| {elapsed:>7.1f}s | {total_ops:>7} | {rss / 1e6:>9.1f} "
            f"| {(rss - baseline_rss) / 1e6:>+10.1f} | {binds:>8} | {deps:>8} "
            f"| {ops_per_s:>6.1f} |"
        )

        if elapsed >= phase_duration:
            break

    xs_ops = [float(c["ops"]) for c in checkpoints]
    xs_time = [float(c["elapsed_s"]) for c in checkpoints]
    ys = [float(c["rss_bytes"]) for c in checkpoints]
    slope, _, r2 = linear_fit(xs_ops, ys)
    slope_time, _, r2_time = linear_fit(xs_time, ys)

    binds, deps = cache_sizes(session)
    duration = checkpoints[-1]["elapsed_s"] if checkpoints else 0.0
    final_rss = ys[-1] if ys else baseline_rss
    # Cadence is a start-to-start period, so measure it between the first and last iteration
    # starts. duration / iterations would include the trailing work but not the final gap, so a
    # perfectly held 1.00s cadence would report as 0.90s and trip the skew warning.
    achieved_interval = (
        (last_iter_start - first_iter_start) / (iterations - 1)
        if iterations > 1
        else 0.0
    )

    emit("")
    emit(f"  {label}: {iterations} iterations, {total_ops} ops in {duration:.1f}s")
    emit(f"  {label}: RSS {baseline_rss / 1e6:.1f} -> {final_rss / 1e6:.1f} MB")
    emit(
        f"  {label}: slope = {slope:.0f} B/op (R2 {r2:.4f}) = {slope_time / 1e3:.1f} KB/s (R2 {r2_time:.4f})"
    )
    if collect_interval > 0:
        note = (
            f", {overruns} overrun(s) late by {total_lateness / overruns:.2f}s on average"
            if overruns
            else ""
        )
        emit(
            f"  {label}: cadence target {collect_interval:.2f}s -> achieved "
            f"{achieved_interval:.2f}s{note}"
        )
    emit(f"  {label}: exiting with bind_stmt_cache={binds} dependency_cache={deps}")
    if len(checkpoints) < MIN_CHECKPOINTS_FOR_FIT:
        emit(
            f"  {label}: WARNING only {len(checkpoints)} checkpoints (< {MIN_CHECKPOINTS_FOR_FIT}); "
            "the fit is underpowered -- lower --checkpoint-every or raise --phase-duration"
        )

    return {
        "label": label,
        "feature_on": feature_on,
        "ast_enabled": ast_on,
        "n_ops_per_iter": n_ops,
        "iterations": iterations,
        "total_ops": total_ops,
        "duration_s": duration,
        "baseline_rss_bytes": baseline_rss,
        "final_rss_bytes": final_rss,
        "bind_stmt_cache_final": binds,
        "dependency_cache_final": deps,
        "slope_bytes_per_op": slope,
        "r_squared": r2,
        "slope_bytes_per_s": slope_time,
        "r_squared_time": r2_time,
        "collect_interval_target_s": collect_interval,
        "collect_interval_achieved_s": achieved_interval,
        "overruns": overruns,
        "total_lateness_s": total_lateness,
        "checkpoints": checkpoints,
    }


def pilot(
    session: Session, n_ops: int, iterations: int, collect_interval: float
) -> float:
    """Time unthrottled iterations to check whether the requested cadence is even achievable.

    Returns the median seconds per iteration. Knowing this up front beats inferring it from overrun
    counts after a 10-minute phase has already run.
    """
    emit("")
    emit(f"Pilot: timing {iterations} unthrottled iterations at {n_ops} ops ...")
    costs = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        one_iteration(session, n_ops)
        costs.append(time.perf_counter() - t0)

    median = statistics.median(costs)
    emit(
        f"Pilot: {median:.2f}s per iteration "
        f"(min {min(costs):.2f}, max {max(costs):.2f}) at {n_ops / median:.1f} ops/s"
    )

    if collect_interval > 0:
        if median > collect_interval:
            emit(
                f"Pilot: ! the {collect_interval:.2f}s cadence is NOT achievable -- one iteration\n"
                f"       ! costs {median:.2f}s, so every iteration will overrun and the achieved\n"
                f"       ! cadence will be ~{median:.2f}s. Continuing anyway; the leak rate per\n"
                f"       ! collect is pace-independent, and the report states the achieved cadence."
            )
        else:
            headroom = (collect_interval - median) / collect_interval * 100
            emit(
                f"Pilot: {collect_interval:.2f}s cadence is achievable "
                f"({headroom:.0f}% headroom, {median:.2f}s of work per {collect_interval:.2f}s slot)"
            )
    return median


# ============================================================
# Main
# ============================================================


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument(
        "--connection",
        default="zyao_ent_preprod9",
        help="connections.toml section name (default: %(default)s)",
    )
    p.add_argument(
        "--ops-per-iter",
        type=int,
        default=100,
        help="transformations chained per DataFrame (default: %(default)s)",
    )
    p.add_argument(
        "--collect-interval",
        type=float,
        default=3.0,
        help="seconds between collect() starts; 0 = unthrottled (default: %(default)s)",
    )
    p.add_argument(
        "--phase-duration",
        type=float,
        default=600.0,
        help="seconds per phase (default: %(default)s)",
    )
    p.add_argument(
        "--checkpoint-every", type=int, default=5, help="iterations per checkpoint"
    )
    p.add_argument(
        "--pilot-iterations",
        type=int,
        default=5,
        help="unthrottled iterations timed to check cadence feasibility (default: %(default)s)",
    )
    p.add_argument("--warmup-iterations", type=int, default=5)
    p.add_argument(
        "--out",
        default=os.path.join(
            "bench_trace", "results", "ast_memory_leak_100ops_3s.json"
        ),
    )
    p.add_argument(
        "--smoke",
        action="store_true",
        help="short validation run: 20 ops/iter, 1s cadence, 60s per phase",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.smoke:
        args.ops_per_iter = 20
        args.collect_interval = 1.0
        args.phase_duration = 60.0
        args.checkpoint_every = 2
        args.pilot_iterations = 3

    pace = (
        f"1 collect per {args.collect_interval:g}s"
        if args.collect_interval > 0
        else "unthrottled"
    )

    emit("=" * 84)
    emit("AST memory leak benchmark: enable_trace_sql_errors_to_dataframe")
    emit("=" * 84)
    emit(f"  connection        {args.connection}")
    emit(f"  ops per iteration {args.ops_per_iter}")
    emit(f"  pace              {pace}")
    emit(f"  duration          {args.phase_duration:.0f}s per phase (OFF then ON)")
    if args.smoke:
        emit("  MODE              SMOKE (expect an underpowered result)")

    # OFF must run first: configure_development_features(False) does not undo
    # set_ast_state(AstFlagSource.USER, True), so the flag is sticky once enabled.
    set_feature(False)

    session = Session.builder.config("connection_name", args.connection).create()
    try:
        emit(f"  warehouse         {session.get_current_warehouse()}")

        emit("")
        emit(f"Warmup: {args.warmup_iterations} iterations (discarded) ...")
        for _ in range(args.warmup_iterations):
            one_iteration(session, args.ops_per_iter)

        iter_cost = pilot(
            session, args.ops_per_iter, args.pilot_iterations, args.collect_interval
        )

        # With a fixed cadence the run length is arithmetic, not a projection. State the expected
        # growth so an underpowered configuration is obvious before 20 minutes are spent on it.
        if args.collect_interval > 0:
            collects = args.phase_duration / max(args.collect_interval, iter_cost)
            predicted_growth = (
                collects * (args.ops_per_iter + 2) * PREDICTED_BYTES_PER_BIND
            )
            emit(
                f"Expect: ~{collects:.0f} collects per phase x "
                f"{(args.ops_per_iter + 2) * PREDICTED_BYTES_PER_BIND / 1e3:.0f} KB = "
                f"~{predicted_growth / 1e6:.0f} MB of growth in the ON phase"
            )

        off = run_phase(
            session,
            "OFF",
            feature_on=False,
            n_ops=args.ops_per_iter,
            checkpoint_every=args.checkpoint_every,
            phase_duration=args.phase_duration,
            collect_interval=args.collect_interval,
        )

        on = run_phase(
            session,
            "ON",
            feature_on=True,
            n_ops=args.ops_per_iter,
            checkpoint_every=args.checkpoint_every,
            phase_duration=args.phase_duration,
            collect_interval=args.collect_interval,
        )

        report(off, on, args)

        payload = {
            "config": vars(args),
            "predicted_bytes_per_bind": PREDICTED_BYTES_PER_BIND,
            "pilot_seconds_per_iteration": iter_cost,
            "phases": [off, on],
        }
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(payload, f, indent=2)
        emit("")

        # Independent cross-check of the VmRSS parsing: ru_maxrss comes from the kernel via
        # getrusage and VmHWM from a different /proc field, both peaks. Since RSS only climbs during
        # the ON phase, all three should agree closely. A large gap means read_rss_bytes is wrong.
        emit(
            f"RSS cross-check: VmRSS {read_rss_bytes() / 1e6:.1f} MB | "
            f"VmHWM {read_peak_rss_bytes() / 1e6:.1f} MB (peak) | "
            f"ru_maxrss {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 / 1e6:.1f} MB (peak)"
        )
        emit(f"Raw checkpoint series written to {args.out}")
    finally:
        session.close()

    return 0


def report(off: dict, on: dict, args: argparse.Namespace) -> None:
    leak = on["slope_bytes_per_op"] - off["slope_bytes_per_op"]

    emit("")
    emit("=" * 84)
    emit("RESULT")
    emit("=" * 84)
    emit(
        f"| {'Phase':<6} | {'Collects':>8} | {'Ops':>8} | {'dRSS (MB)':>10} "
        f"| {'B/op':>8} | {'KB/s':>7} | {'R2':>7} |"
    )
    emit(f"|{'-' * 8}|{'-' * 10}|{'-' * 10}|{'-' * 12}|{'-' * 10}|{'-' * 9}|{'-' * 9}|")
    for ph in (off, on):
        grown = (ph["final_rss_bytes"] - ph["baseline_rss_bytes"]) / 1e6
        emit(
            f"| {ph['label']:<6} | {ph['iterations']:>8} | {ph['total_ops']:>8} "
            f"| {grown:>+10.1f} | {ph['slope_bytes_per_op']:>8.0f} "
            f"| {ph['slope_bytes_per_s'] / 1e3:>7.1f} | {ph['r_squared']:>7.4f} |"
        )

    leak_per_s = on["slope_bytes_per_s"] - off["slope_bytes_per_s"]
    binds_per_iter = (
        on["bind_stmt_cache_final"] / on["iterations"] if on["iterations"] else 0.0
    )

    emit("")
    if on["r_squared"] < 0.9:
        emit(
            f"INSUFFICIENT SIGNAL: ON-phase R2 = {on['r_squared']:.4f} (< 0.9), so RSS growth is\n"
            f"not cleanly linear in ops and no reliable rate can be reported. Raise\n"
            f"--phase-duration (currently {args.phase_duration:.0f}s) and re-run."
        )
    else:
        emit(f"LEAK RATE  = slope_ON - slope_OFF = {leak:.0f} bytes per DataFrame op")
        emit(
            f"           = {leak * on['n_ops_per_iter'] / 1e3:.0f} KB per collect() "
            f"({on['n_ops_per_iter']} ops = {binds_per_iter:.0f} binds each)"
        )
        emit(f"           = {leak * 10000 / 1e6:.1f} MB per 10,000 ops")

        # With a fixed cadence, MB/hour is measured directly off the time-axis fit rather than
        # derived from an assumed op rate.
        if on["collect_interval_target_s"] > 0:
            emit("")
            emit(
                f"MEASURED at {on['collect_interval_achieved_s']:.2f}s per collect "
                f"(R2 {on['r_squared_time']:.4f} against elapsed time):"
            )
            emit(f"           = {leak_per_s / 1e3:.0f} KB/s")
            emit(f"           = {leak_per_s * 3600 / 1e9:.2f} GB/hour")
            for hours in (8, 24):
                emit(
                    f"           = {leak_per_s * 3600 * hours / 1e9:.1f} GB per {hours}h session"
                )
        else:
            rates = [c["ops_per_s"] for c in on["checkpoints"]]
            if rates:
                sustained = sum(rates) / len(rates)
                per_hour = leak * sustained * 3600 / 1e6
                emit(
                    f"           = {per_hour:.0f} MB/hour at the saturated {sustained:.1f} ops/s"
                )
                emit(
                    "           (unthrottled run: this rate is the benchmark's ceiling, not a workload)"
                )

    # Invariants that make the attribution defensible.
    emit("")
    emit("Invariant checks:")

    if off["ast_enabled"]:
        emit(
            "  [WARN] OFF phase ran with AST enabled by the server, so its slope is not an\n"
            "         AST-free baseline (see the phase warning above)."
        )
    elif off["bind_stmt_cache_final"] == 0:
        emit("  [ok]   OFF phase left bind_stmt_cache empty (0 entries)")
    else:
        emit(
            f"  [WARN] OFF phase left {off['bind_stmt_cache_final']} bind_stmt_cache entries; "
            "expected 0"
        )

    binds = on["bind_stmt_cache_final"]
    ops = on["total_ops"]
    if ops and binds >= ops:
        emit(
            f"  [ok]   ON phase retained {binds} bind_stmt_cache entries for {ops} ops "
            f"({binds / ops:.2f} binds/op, >=1 as expected: each op plus the per-iteration base df)"
        )
    else:
        emit(
            f"  [WARN] ON phase retained {binds} bind_stmt_cache entries for {ops} ops"
        )

    if on["dependency_cache_final"] > 0:
        emit(
            f"  [ok]   ON phase filled dependency_cache with "
            f"{on['dependency_cache_final']} entries (the action path populates it)"
        )
    else:
        emit("  [WARN] ON phase left dependency_cache empty; flush() may not have run")

    target = on["collect_interval_target_s"]
    if target > 0:
        achieved = on["collect_interval_achieved_s"]
        over = on["overruns"]
        skew = abs(achieved - target) / target
        if skew <= 0.1:
            emit(
                f"  [ok]   cadence held: {achieved:.2f}s achieved vs {target:.2f}s target "
                f"({over} overrun(s) of {on['iterations']} iterations)"
            )
        else:
            mean_late = on["total_lateness_s"] / over if over else 0.0
            emit(
                f"  [WARN] cadence NOT held: {achieved:.2f}s achieved vs {target:.2f}s target "
                f"({skew * 100:.0f}% off; {over} of {on['iterations']} iterations overran, "
                f"late by {mean_late:.2f}s on average).\n"
                f"         Per-collect and per-op figures remain valid; the GB/hour figures apply\n"
                f"         to the achieved {achieved:.2f}s cadence, not the requested {target:.2f}s."
            )

    # Drop the first checkpoint: it absorbs phase-start overhead (flag flip, first compile after
    # the switch) and is not representative of steady-state throughput. Compare halves by median
    # rather than endpoints so one slow window does not dominate.
    rates = [c["ops_per_s"] for c in on["checkpoints"]][1:]
    if len(rates) >= 4:
        mid = len(rates) // 2
        early = statistics.median(rates[:mid])
        late = statistics.median(rates[mid:])
        drift = abs(late - early) / early if early > 0 else 0.0
        if drift <= 0.2:
            emit(
                f"  [ok]   ON throughput flat ({early:.1f} -> {late:.1f} ops/s median, "
                f"{drift * 100:.0f}% drift) while RSS climbed: a pure memory leak, "
                "not a slowdown"
            )
        else:
            emit(
                f"  [note] ON throughput drifted {drift * 100:.0f}% "
                f"({early:.1f} -> {late:.1f} ops/s median); growth may not be purely memory"
            )

    # Convert to per *bind* -- the quantity comparable across configs. Ops and binds are not 1:1:
    # each iteration also binds its base DataFrame and its collect(), so binds/op exceeds 1 and
    # falls toward 1 as --ops-per-iter grows.
    #
    # Per-bind cost is only constant for a fixed op mix. build_pipeline cycles i % 3, so the share
    # of the expensive with_column op shifts with n_ops (20% at 5 ops, 30% at 20, 33% at 100),
    # which is why the band below is wide and the op count is printed alongside.
    binds_per_op = binds / ops if ops else 0.0
    per_bind_kb = (leak / binds_per_op / 1000.0) if binds_per_op else 0.0
    n_ops = on["n_ops_per_iter"]
    wc_share = 100 * (n_ops // 3) / n_ops if n_ops else 0.0  # i % 3 == 2 branch count
    in_range = 2.2 <= per_bind_kb <= 7.0
    tag = f"[{'ok' if in_range else 'note'}]".ljust(
        7
    )  # match the [ok]/[WARN] column width
    emit(
        f"  {tag}{leak / 1000.0:.1f} KB/op at "
        f"{binds_per_op:.2f} binds/op = {per_bind_kb:.1f} KB/bind "
        f"(at {n_ops} ops/iter, {wc_share:.0f}% with_column)"
    )
    if not in_range:
        emit(
            "         outside the 2.2-7.0 KB/bind range seen so far; check whether the op mix or "
            "expression size changed"
        )


if __name__ == "__main__":
    sys.exit(main())
