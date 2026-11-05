#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""SQL compilation errors are mapped to the right Python lines when the plan compiler rewrites the
query, e.g. with a WITH clause for CTE optimization or with large query breakdown.

Each workload marks the line of the offending operation with ``# MARK`` and the lines of all its
dataframe operations with ``# OP``. The mapped lines must be lines of operations of the workload
and, with multiline queries, contain the offending line. Where the mapping cannot be computed it
must be absent, never point to an unrelated line.
"""

import inspect
import re

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, lit

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SQL error tracing is not supported in local testing mode",
        run=False,
    ),
]

_MAPPED_LINE = re.compile(r"line[s]? (\d+)")


def _small(s):
    return s.create_dataframe([(1, "a"), (2, "b")], schema=["ID", "V"])  # OP


def error_in_reused_part(s):
    b = _small(s).with_column("X", lit(1))  # OP
    bad = b.filter(col("NOPE") > 0).select("ID", "X")  # OP MARK
    return bad.union_all(bad)  # OP


def error_outside_reused_part(s):
    b = _small(s).with_column("X", lit(1))  # OP
    left = b.filter(col("ID") > 0)  # OP
    right = b.filter(col("NOPE") < 5)  # OP MARK
    return left.union_all(right)  # OP


def diamond(s):
    base = _small(s).with_column("X", lit(1))  # OP
    left = base.filter(col("ID") > 0)  # OP
    right = base.select("ID", col("V")).filter(col("NOPE") > 1)  # OP MARK
    return left.join(right, "ID")  # OP


def self_union_all(s):
    a = _small(s).filter(col("NOPE") > 0)  # OP MARK
    return a.union_all(a)  # OP


def three_way_reuse(s):
    b = _small(s).with_column("X", lit(1))  # OP
    bad = b.filter(col("NOPE") * 2 > 0).sort("ID")  # OP MARK
    return bad.union_all(bad).union_all(bad)  # OP


def large_create_dataframe_reused(s):
    b = s.create_dataframe([(i, str(i)) for i in range(600)], schema=["ID", "V"])  # OP
    bad = b.filter(col("NOPE") > 0)  # OP MARK
    return bad.union_all(bad)  # OP


def error_after_reuse(s):
    b = _small(s).with_column("X", lit(1))  # OP
    u = b.union_all(b)  # OP
    return u.filter(col("NOPE") > 1)  # OP MARK


def nested_reuse(s):
    b = _small(s)  # OP
    b2 = b.union_all(b).with_column("Y", lit(2))  # OP
    bad = b2.filter(col("NOPE") > 1)  # OP MARK
    return bad.join(bad.select(col("ID").alias("I2")), col("ID") == col("I2"))  # OP


WORKLOADS = [
    error_in_reused_part,
    error_outside_reused_part,
    diamond,
    self_union_all,
    three_way_reuse,
    large_create_dataframe_reused,
    error_after_reuse,
    nested_reuse,
]


def _marked_lines(fn, marker):
    lines, start = inspect.getsourcelines(fn)
    helper_lines, helper_start = inspect.getsourcelines(_small)
    found = {
        start + i
        for i, line in enumerate(lines)
        if re.search(rf"#.*\b{marker}\b", line)
    }
    if marker == "OP":
        found |= {
            helper_start + i
            for i, line in enumerate(helper_lines)
            if re.search(r"#.*\bOP\b", line)
        }
    return found


@pytest.fixture(autouse=True)
def setup(session):
    old = (
        session.cte_optimization_enabled,
        session._generate_multiline_queries,
        session.large_query_breakdown_enabled,
    )
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    yield
    context.configure_development_features()
    session.cte_optimization_enabled = old[0]
    (
        session._enable_multiline_queries
        if old[1]
        else session._disable_multiline_queries
    )()
    session.large_query_breakdown_enabled = old[2]


def _mapped_lines(session, workload):
    with pytest.raises(SnowparkSQLException) as ex:
        workload(session).collect()
    message = str(ex.value)
    assert "invalid identifier 'NOPE'" in message
    if "corresponds to Python source" not in message:
        return None
    context_text = message.split("--- Additional Debug Information ---")[-1]
    mapping = context_text.split("corresponds to Python source")[-1].split("\n\n")[0]
    return {int(n) for n in _MAPPED_LINE.findall(mapping)}


@pytest.mark.parametrize("multiline", [True, False], ids=["multiline", "singleline"])
@pytest.mark.parametrize("cte", [False, True], ids=["nocte", "cte"])
@pytest.mark.parametrize("workload", WORKLOADS, ids=lambda w: w.__name__)
def test_sql_error_maps_to_offending_operation(session, workload, cte, multiline):
    session.cte_optimization_enabled = cte
    (
        session._enable_multiline_queries
        if multiline
        else session._disable_multiline_queries
    )()
    if cte:
        # make sure the plan compiler actually rewrote the query
        assert workload(session).queries["queries"][-1].startswith("WITH")

    lines = _mapped_lines(session, workload)
    assert lines is not None, "the error was not mapped to the Python source"
    assert lines <= _marked_lines(workload, "OP")
    if multiline:
        # In a single-line query every error is on line 1, which maps to the outer operations.
        (mark,) = _marked_lines(workload, "MARK")
        assert mark in lines


def test_same_lines_with_and_without_cte(session):
    """With multiline queries the mapping does not depend on the CTE optimization."""
    session._enable_multiline_queries()
    results = {}
    for enabled in (False, True):
        session.cte_optimization_enabled = enabled
        results[enabled] = {w.__name__: _mapped_lines(session, w) for w in WORKLOADS}
    assert results[False] == results[True]


def test_large_query_breakdown_is_never_mapped_to_a_wrong_line(session):
    session._enable_multiline_queries()
    session.cte_optimization_enabled = False
    session.large_query_breakdown_enabled = True
    bounds = session.large_query_breakdown_complexity_bounds
    session.large_query_breakdown_complexity_bounds = (1, 2)
    try:
        for workload in WORKLOADS:
            lines = _mapped_lines(session, workload)
            if lines is not None:
                assert lines <= _marked_lines(workload, "OP"), workload.__name__
    finally:
        session.large_query_breakdown_complexity_bounds = bounds


def test_a_failure_in_the_cte_segments_does_not_fail_the_query(session, monkeypatch):
    from snowflake.snowpark._internal.compiler import utils as compiler_utils

    def broken(*args, **kwargs):
        raise TypeError("broken segments")

    monkeypatch.setattr(compiler_utils, "CteSegment", broken)
    session.cte_optimization_enabled = True
    b = _small(session).with_column("X", lit(1))
    df = b.filter(col("ID") > 0).union_all(b.filter(col("ID") > 1))
    assert df.queries["queries"][-1].startswith("WITH")
    assert len(df.collect()) == 3
    # the error is still raised, only without the mapped source lines
    assert _mapped_lines(session, error_in_reused_part) is None
