#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Test matrix for the debug features (``session.debug`` and
``context.configure_development_features``).

Every workload runs with the features off and then on, under the session settings
``sql_simplifier_enabled``, ``cte_optimization_enabled`` and multiline queries, and asserts:

- A, no new errors: a workload that succeeds with the features off succeeds with them on, and
  the features log no swallowed exception.
- B, no behavior change: the same rows, schema, count and ``df.queries``; for an injected
  error, the same exception class, error code and message (without the debug information).
- With ``trace_sql_errors_to_dataframe`` on, an injected SQL compilation error is mapped to the
  line marked ``# ERR`` (alone or among the candidates), never to another line only.

By default the cases cover every pair of parameter values; set
``SNOWPARK_DEVFEATURES_FULL_MATRIX=1`` to run the full cross product.
"""

import inspect
import itertools
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Callable, Optional

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark import Row, Window
from snowflake.snowpark._internal.utils import TempObjectType, remove_comments
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    lit,
    row_number,
    sql_expr,
    sum as sum_,
    udf,
)
from snowflake.snowpark.types import IntegerType, StructField, StructType
from tests.utils import Utils

try:
    import pandas  # noqa: F401

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="needs a live connection",
        run=False,
    ),
]

FEATURES = (
    "eager_schema_validation",
    "trace_sql_errors_to_dataframe",
    "dataframe_trace_on_error",
)
# The 8 combinations set with session.debug, and every feature on set with
# configure_development_features.
FEATURE_CASES = [
    dict(zip(FEATURES, on)) for on in itertools.product([False, True], repeat=3)
] + ["configure_all"]
FEATURE_IDS = [
    "debug-" + "".join("1" if on else "0" for on in case.values())
    for case in FEATURE_CASES[:-1]
] + ["configure_all"]

SETTINGS = list(itertools.product([True, False], repeat=3))
FULL_MATRIX = os.environ.get("SNOWPARK_DEVFEATURES_FULL_MATRIX") == "1"

TABLE = Utils.random_name_for_temp_object(TempObjectType.TABLE)
_UUID_MARKER = re.compile(r"^-- ([0-9a-f-]{36})$", re.M)
_TEMP_NAME = re.compile(r"SNOWPARK_TEMP_\w+")
# Aliases generated per DataFrame: semi/anti join columns and the drop_duplicates row number.
_GENERATED_ALIAS = re.compile(r'"([lr]_\d{4}_\w+|[A-Za-z0-9]{10})"')
_WHITESPACE = re.compile(r"\s+")
_QUERY_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_DEBUG_HEADER = "--- Additional Debug Information ---"
_SWALLOWED = (
    "Error when getting the df_transform_debug_trace",
    "Failed to record the dataframe trace",
)


@dataclass
class Workload:
    name: str
    build: Callable  # build(session) -> DataFrame
    action: str = "collect"  # collect, count, first, to_pandas or save_as_table
    sorts: bool = False  # the workload sorts its rows itself
    inject_error: Optional[Callable] = None  # inject_error(session) -> DataFrame
    # The error is a SQL compilation error that the sql error tracing maps to the line marked
    # "# ERR" in inject_error.
    maps_line: bool = True


def _a(s):
    return s.create_dataframe(
        [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0), (4, "a", None)],
        schema=["ID", "NAME", "V"],
    )


def _b(s):
    return s.create_dataframe([(1, "x"), (2, "y"), (5, "z")], schema=["ID", "TAG"])


def _err_filter(s):
    df = _a(s).select("ID", "V")
    df = df.filter(sql_expr("TO_DATE(ID, V, ID, V)") > 0)  # ERR
    return df


def _err_with_column(s):
    df = _a(s).with_column("X", col("ID") + 1)
    df = df.with_column("Y", sql_expr("TO_DATE(ID, V, ID, V)"))  # ERR
    df = df.with_column("Z", col("X") * 2)
    return df


def _err_invalid_identifier(s):
    df = _a(s).select("ID", "NAME")
    df = df.select(col("ID") + col("DOES_NOT_EXIST"))  # ERR
    return df


def _err_agg(s):
    df = _a(s).group_by("NAME")
    df = df.agg(sum_(sql_expr("TO_DATE(ID, V, ID, V)")).alias("S"))  # ERR
    return df


def _err_window(s):
    w = Window.partition_by("NAME").order_by("ID")
    df = _a(s).with_column("R", sum_("V").over(w))
    df = df.with_column("E", sql_expr("TO_DATE(ID, V, ID, V)"))  # ERR
    return df


def _err_join(s):
    left = _a(s).filter(col("ID") > 0)
    right = _b(s).with_column("E", sql_expr("TO_DATE(ID, TAG, ID, TAG)"))  # ERR
    return left.join(right, on="ID")


def _err_union(s):
    top = _b(s).select("ID", "TAG")
    bottom = _b(s).select("ID", sql_expr("TO_DATE(ID, TAG, ID, TAG)"))  # ERR
    return top.union_all(bottom)


def _err_sql_source(s):
    df = s.sql("select 1 as A, 'x' as B")
    df = df.select(sql_expr("TO_DATE(A, B, A, B)").alias("C"))  # ERR
    return df


def _err_chain(s):
    df = _a(s)
    for i in range(10):
        df = df.with_column(f"C{i}", col("ID") + i)
    df = df.with_column("BAD", sql_expr("TO_DATE(ID, V, ID, V)"))  # ERR
    return df


def _err_runtime_cast(s):
    # A runtime (not compilation) error: not mapped to a line.
    return _b(s).select(col("TAG").cast(IntegerType()))


def _err_missing_table(s):
    return s.table("SNOWPARK_DEVFEATURES_DOES_NOT_EXIST").select("ID")


def _chain(s, steps):
    df = s.range(3)
    for i in range(steps):
        df = df.with_column(f"C{i}", col("ID") + i)
    return df


def _nested(s):
    df = _a(s)
    for i in range(5):
        df = df.select("ID", "V", (col("ID") + i).alias(f"N{i}")).filter(
            col("ID") > i - 5
        )
        df = df.select("ID", "V")
    return df


def _diamond(s):
    base = _a(s).filter(col("ID") > 1)
    left = base.select("ID", col("V").alias("L"))
    right = base.select(col("ID").alias("RID"), col("V").alias("R"))
    return left.join(right, col("ID") == col("RID"))


def _self_join(s):
    df = _a(s)
    return (
        df.alias("x")
        .join(df.alias("y"), col("x", "ID") == col("y", "ID"))
        .select(col("x", "ID").alias("I"), col("y", "NAME").alias("N"))
    )


def _udf(s):
    plus_one = udf(
        lambda x: x + 1,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        session=s,
    )
    return _a(s).select("ID", plus_one(col("ID")).alias("P"))


def _udtf(s):
    class Repeat:
        def process(self, n: int):
            for i in range(n):
                yield (i,)

    repeat = s.udtf.register(
        Repeat,
        output_schema=StructType([StructField("I", IntegerType())]),
        input_types=[IntegerType()],
    )
    return s.create_dataframe([(2,), (3,)], schema=["N"]).join_table_function(
        repeat(col("N"))
    )


def _unpivot(s):
    return s.create_dataframe(
        [(1, 10, 20), (2, 30, 40)], schema=["ID", "JAN", "FEB"]
    ).unpivot("SALES", "MONTH", ["JAN", "FEB"])


WORKLOADS = [
    # sources
    Workload("table", lambda s: s.table(TABLE)),
    Workload("sql_select", lambda s: s.sql("select 1 as A union all select 2")),
    Workload("sql_non_select", lambda s: s.sql(f"describe table {TABLE}")),
    Workload(
        "sql_literal_with_empty_line",
        lambda s: s.sql("select 'a\n\nb' as S"),
    ),
    Workload("create_dataframe", _a, inject_error=_err_runtime_cast, maps_line=False),
    Workload("range", lambda s: s.range(1, 10, 2)),
    # projections
    Workload("select", lambda s: _a(s).select("ID", "V"), inject_error=_err_filter),
    Workload(
        "with_column",
        lambda s: _a(s).with_column("X", col("ID") * 2),
        inject_error=_err_with_column,
    ),
    Workload("rename", lambda s: _a(s).rename(col("ID"), "NEW_ID")),
    Workload("drop", lambda s: _a(s).drop("V")),
    Workload(
        "alias", lambda s: _a(s).select(col("ID").alias("I"), (col("V") + 1).alias("W"))
    ),
    Workload(
        "sql_expr",
        lambda s: _a(s).select(sql_expr("ID * 10").alias("T")),
        inject_error=_err_invalid_identifier,
    ),
    Workload(
        "quoted_mixed_case",
        lambda s: s.create_dataframe(
            [(1, 2)], schema=['"MixedCase"', '"lower"']
        ).select(col('"MixedCase"'), (col('"lower"') + 1).alias('"Out Col"')),
    ),
    # row operations
    Workload(
        "filter", lambda s: _a(s).filter((col("ID") > 1) & col("V").is_not_null())
    ),
    Workload("sort", lambda s: _a(s).sort(col("ID").desc()), sorts=True),
    Workload("limit", lambda s: _a(s).sort("ID").limit(2, offset=1), sorts=True),
    Workload("distinct", lambda s: _a(s).select("NAME").distinct()),
    Workload("drop_duplicates", lambda s: _a(s).drop_duplicates("NAME").select("NAME")),
    Workload("sample", lambda s: _a(s).sample(n=2), action="count"),
    Workload("na_fill_drop", lambda s: _a(s).na.fill({"V": 0.0}).na.drop()),
    # aggregation
    Workload(
        "group_by_agg",
        lambda s: _a(s)
        .group_by("NAME")
        .agg(sum_("V").alias("S"), count("ID").alias("C")),
        inject_error=_err_agg,
    ),
    Workload("rollup", lambda s: _a(s).rollup("NAME").agg(avg("V").alias("A"))),
    Workload("cube", lambda s: _a(s).cube("NAME", "ID").agg(sum_("V").alias("S"))),
    Workload(
        "pivot",
        lambda s: _a(s).select("ID", "NAME", "V").pivot("NAME", ["a", "b"]).sum("V"),
    ),
    Workload("unpivot", _unpivot),
    # windows
    Workload(
        "window",
        lambda s: _a(s).with_column(
            "RN", row_number().over(Window.partition_by("NAME").order_by("ID"))
        ),
        inject_error=_err_window,
    ),
    # joins
    Workload(
        "join_inner",
        lambda s: (
            lambda a, b: a.join(b, a["ID"] == b["ID"]).select(
                a["ID"].alias("ID"), "TAG"
            )
        )(_a(s), _b(s)),
        inject_error=_err_join,
    ),
    Workload("join_left", lambda s: _a(s).join(_b(s), on="ID", how="left")),
    Workload("join_full", lambda s: _a(s).join(_b(s), on="ID", how="full")),
    Workload("join_semi", lambda s: _a(s).join(_b(s), on="ID", how="semi")),
    Workload("join_anti", lambda s: _a(s).join(_b(s), on="ID", how="anti")),
    Workload(
        "join_cross", lambda s: _a(s).select("ID").cross_join(_b(s).select("TAG"))
    ),
    Workload("join_natural", lambda s: _a(s).natural_join(_b(s))),
    Workload("join_using", lambda s: _a(s).join(_b(s), using_columns=["ID"])),
    Workload("self_join", _self_join),
    # set operations
    Workload("union", lambda s: _b(s).union(_b(s)), inject_error=_err_union),
    Workload("union_all", lambda s: _b(s).union_all(_b(s))),
    Workload(
        "union_by_name",
        lambda s: _b(s).union_by_name(_b(s).select("TAG", "ID")),
    ),
    Workload("intersect", lambda s: _b(s).intersect(_b(s).filter(col("ID") > 1))),
    Workload("except", lambda s: _b(s).except_(_b(s).filter(col("ID") > 1))),
    # shapes
    Workload("diamond", _diamond),
    Workload(
        "self_union", lambda s: (lambda d: d.union_all(d))(_b(s).filter(col("ID") > 0))
    ),
    Workload("chain_50", lambda s: _chain(s, 50), inject_error=_err_chain),
    Workload("nested_subqueries", _nested),
    Workload(
        "cache_result_mid_chain",
        lambda s: _a(s).filter(col("ID") > 1).cache_result().with_column("X", lit(1)),
    ),
    # functions
    Workload("udf", _udf),
    Workload("udtf", _udtf),
    # actions
    Workload("action_count", lambda s: _a(s).filter(col("ID") > 1), action="count"),
    Workload(
        "action_first",
        lambda s: _a(s).sort("ID"),
        action="first",
        sorts=True,
        inject_error=_err_sql_source,
    ),
    Workload(
        "action_to_pandas",
        lambda s: _a(s).select("ID", "NAME"),
        action="to_pandas",
        inject_error=_err_missing_table,
        maps_line=False,
    ),
    Workload(
        "action_save_as_table",
        lambda s: _a(s).select("ID", "V"),
        action="save_as_table",
    ),
]
WORKLOAD_BY_NAME = {w.name: w for w in WORKLOADS}


def _xfail(reason):
    return [pytest.mark.xfail(strict=True, reason=reason)]


def _clean_known_bugs(workload, on, simplifier, multiline):
    if (
        workload.name == "sql_literal_with_empty_line"
        and on["trace_sql_errors_to_dataframe"]
    ):
        return _xfail(
            "SNOW-4412751: empty lines inside literals of raw SQL are stripped"
        )
    return []


def _error_known_bugs(workload, on, simplifier, multiline):
    eager, trace_sql = (
        on["eager_schema_validation"],
        on["trace_sql_errors_to_dataframe"],
    )
    if workload.name == "select" and eager and trace_sql and simplifier and multiline:
        return _xfail(
            "untracked: with eager schema validation the SQL error is mapped to the line "
            "of the previous operation, not the offending filter"
        )
    if (
        trace_sql
        and not multiline
        and workload.name in ("select", "with_column")
        and (simplifier or workload.name == "select")
    ):
        return _xfail(
            "untracked: trace_sql_errors_to_dataframe adds line breaks to single-line SQL, "
            "so 'error line N at position M' in the error message changes"
        )
    if (
        eager
        and simplifier
        and (
            workload.name == "with_column"
            or multiline
            and workload.name in ("sql_expr", "window", "chain_50", "action_first")
        )
    ):
        return _xfail(
            "untracked: eager schema validation raises the SQL error from its schema query, "
            "so 'error line N at position M' in the error message changes"
        )
    return []


def _cases(workloads, known_bugs):
    """(workload, feature case, settings): every feature case runs every workload; the settings
    rotate so that each pair of parameter values is covered, unless the full matrix is on."""
    for i, workload in enumerate(workloads):
        for j, (features, feature_id) in enumerate(zip(FEATURE_CASES, FEATURE_IDS)):
            settings = SETTINGS if FULL_MATRIX else [SETTINGS[(i + j) % len(SETTINGS)]]
            for simplifier, cte, multiline in settings:
                yield pytest.param(
                    workload.name,
                    features,
                    (simplifier, cte, multiline),
                    id="-".join(
                        [
                            workload.name,
                            feature_id,
                            "simplifier" if simplifier else "nosimplifier",
                            "cte" if cte else "nocte",
                            "multiline" if multiline else "singleline",
                        ]
                    ),
                    marks=known_bugs(
                        workload,
                        dict.fromkeys(FEATURES, True)
                        if features == "configure_all"
                        else features,
                        simplifier,
                        multiline,
                    ),
                )


@pytest.fixture(scope="module", autouse=True)
def table(session):
    session.sql(f"create temp table {TABLE} (ID int, NAME string)").collect()
    session.sql(f"insert into {TABLE} values (1, 'a'), (2, 'b')").collect()
    yield TABLE
    session.sql(f"drop table if exists {TABLE}").collect()


@pytest.fixture
def apply_settings(session):
    """Sets the session settings of a case; restores them and turns every feature off after."""
    simplifier = session.sql_simplifier_enabled
    cte = session.cte_optimization_enabled
    multiline = session._generate_multiline_queries

    def apply(settings):
        session.sql_simplifier_enabled, session.cte_optimization_enabled, on = settings
        if on:
            session._enable_multiline_queries()
        else:
            session._disable_multiline_queries()

    yield apply
    session.debug.set()
    context.configure_development_features()
    apply((simplifier, cte, multiline))


def _set_features(session, features):
    if features == "configure_all":
        context.configure_development_features(
            enable_eager_schema_validation=True,
            enable_dataframe_trace_on_error=True,
            enable_trace_sql_errors_to_dataframe=True,
        )
    else:
        session.debug.set(**features)


def _features_off(session):
    session.debug.set()
    context.configure_development_features()


def _normalize_query(query):
    """Removes the uuid markers that the sql error tracing adds and the random names; the
    markers leave line breaks, so whitespace outside string literals is collapsed too."""
    query = remove_comments(query, _UUID_MARKER.findall(query))
    query = _GENERATED_ALIAS.sub('"ALIAS"', _TEMP_NAME.sub("SNOWPARK_TEMP_X", query))
    parts = query.split("'")
    parts[::2] = [_WHITESPACE.sub("", p) for p in parts[::2]]
    return "'".join(parts)


def _run(session, workload):
    df = workload.build(session)
    queries = [_normalize_query(q) for q in df.queries["queries"]]
    schema = [(f.name, str(f.datatype)) for f in df.schema.fields]
    if workload.action == "collect":
        result = df.collect()
    elif workload.action == "count":
        result = df.count()
    elif workload.action == "first":
        result = df.first()
    elif workload.action == "to_pandas":
        if not is_pandas_available:
            pytest.skip("pandas is not available")
        result = [Row(*r) for r in df.to_pandas().itertuples(index=False)]
    else:
        target = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        df.write.save_as_table(target, table_type="temp")
        result = session.table(target).collect()
        Utils.drop_table(session, target)
    if isinstance(result, list) and not workload.sorts:
        result = sorted(result, key=str)
    return result, schema, queries


def _error(session, workload):
    with pytest.raises(Exception) as ex:
        df = workload.inject_error(session)
        df.collect()
    e = ex.value
    message = str(e).split(_DEBUG_HEADER)[0]
    message = _TEMP_NAME.sub("SNOWPARK_TEMP_X", _QUERY_ID.sub("<id>", message))
    return type(e), getattr(e, "error_code", None), message.rstrip(), e


def _swallowed(caplog):
    return [m for m in _SWALLOWED if m in caplog.text]


@pytest.mark.parametrize(
    "name, features, settings", list(_cases(WORKLOADS, _clean_known_bugs))
)
def test_clean_workload(session, apply_settings, caplog, name, features, settings):
    """A (no new errors) and B (same rows, schema, count and queries)."""
    workload = WORKLOAD_BY_NAME[name]
    apply_settings(settings)
    _features_off(session)
    before = _run(session, workload)
    _set_features(session, features)
    with caplog.at_level(logging.DEBUG, logger="snowflake.snowpark"):
        after = _run(session, workload)
    assert _swallowed(caplog) == []
    assert after[0] == before[0], "rows changed"
    assert after[1] == before[1], "schema changed"
    assert after[2] == before[2], "queries changed"


def _err_line(inject_error):
    lines, start = inspect.getsourcelines(inject_error)
    return next(
        start + i for i, line in enumerate(lines) if line.rstrip().endswith("# ERR")
    )


def _mapped_lines(debug_context):
    """The lines of this file, or ranges of them, that the debug context maps the error to."""
    pattern = re.escape(os.path.basename(__file__)) + r": lines? (\d+)(?:-(\d+))?"
    return [
        range(int(first), int(last or first) + 1)
        for first, last in re.findall(pattern, debug_context or "")
    ]


@pytest.mark.parametrize(
    "name, features, settings",
    list(
        _cases([w for w in WORKLOADS if w.inject_error is not None], _error_known_bugs)
    ),
)
def test_injected_error(session, apply_settings, caplog, name, features, settings):
    """B for errors: the same exception class, error code and message; and the sql error
    tracing maps a compilation error to the offending line."""
    workload = WORKLOAD_BY_NAME[name]
    apply_settings(settings)
    _features_off(session)
    before = _error(session, workload)
    _set_features(session, features)
    with caplog.at_level(logging.DEBUG, logger="snowflake.snowpark"):
        after = _error(session, workload)
    assert _swallowed(caplog) == []
    assert after[:3] == before[:3]

    trace_sql_errors = session.debug.trace_sql_errors_to_dataframe
    # Line numbers are flaky before Python 3.11 and in FIPS mode (SNOW-2204213).
    if (
        trace_sql_errors
        and workload.maps_line
        and sys.version_info >= (3, 11)
        and "FIPS_TEST" not in os.environ
    ):
        line = _err_line(workload.inject_error)
        mapped = _mapped_lines(getattr(after[3], "debug_context", None))
        assert mapped, f"no line mapped, expected line {line}"
        assert any(line in r for r in mapped), f"mapped {mapped}, expected line {line}"
