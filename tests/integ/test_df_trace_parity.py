#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Differential parity of the lightweight DataFrame trace and the AST backend.

For every workload of the corpus the AST backend (a frozen copy of the original debug_utils, the
oracle) and the lightweight backend compute the debug output of the same error on the same plan.
They must be equal, or the difference must be a documented gap (``KNOWN_GAPS``).
"""

import re

import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from tests.integ import df_trace_corpus
from tests.integ.df_trace_corpus import WORKLOADS, Ctx
from tests.integ.df_trace_harness import DebugFeatures, observe
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SnowparkSQLException is not raised in local testing mode",
        run=False,
    ),
]

# Workloads where the AST lineage contains entries that the lightweight lineage lacks (strict xfail).
KNOWN_GAPS = {
    "shape_same_df_twice_in_expression": (
        "GAP-1: a dataframe used inside a column expression (isin/subquery) is not an input of the "
        "operation in the lightweight trace; the AST records it as a reference"
    ),
    "shape_udf_in_pipeline": (
        "GAP-2: the registration of a UDF used by the operation is a dependency in the AST lineage; "
        "the lightweight trace does not track UDF registration"
    ),
}

# (workload, setting id) -> reason, for differences that only show with one setting.
KNOWN_SETTING_GAPS = {
    ("op_describe", "simplifier-cte-multiline"): "GAP-3",
    ("op_to_df_then_union_with_self", "simplifier-cte-multiline"): "GAP-3",
}
GAP_3 = (
    "GAP-3: with CTE optimization the SQL line maps to a leaf plan that the AST list carries the "
    "operations of the dataframe that consumed it, the lightweight list carries only the leaf's own"
)

# Workloads where the lightweight output is a superset of the AST output (listed behavior changes).
_ACTION_SUPERSET = (
    "an action that runs its own internal dataframe (count, show, first, explain, stat functions, "
    "writers, copy_into_table) is recorded as an operation in the lightweight trace; the AST "
    "records nothing for the internal dataframe, so its trace is empty"
)
LITE_SUPERSET = {
    "op_random_split_first": (
        "the AST does not link the dataframes returned by random_split to their input, the "
        "lightweight trace does"
    ),
    **{
        name: _ACTION_SUPERSET
        for name in (
            "act_count",
            "act_show",
            "act_first",
            "act_stat_corr_cov",
            "act_stat_approx_quantile",
            "act_write_copy_into_location",
            "act_write_csv_json_parquet",
            "act_write_json",
            "act_write_parquet",
            "act_write_partition_by_format_save",
            "act_write_insert_into",
            "act_write_options",
        )
    },
}

_LINES = re.compile(r"line[s]? (\d+)(?:-(\d+))?")


def _mapped_lines(text):
    return set(_LINES.findall(text))


_ENTRY_START = re.compile(r"^(/|[A-Za-z]:\\).*\|\d+")


def _parse_lineage(text):
    """Returns (header, entries). An entry can span several lines (multi-line calls)."""
    if not text:
        return "", []
    lines = text.split("\n")
    header, entries = lines[0], []
    for line in lines[1:]:
        if _ENTRY_START.match(line) or not entries:
            entries.append(line)
        else:
            entries[-1] += "\n" + line
    return header, entries


def _layers_are_single(trace_node) -> bool:
    """True if every level of the lineage graph has one node, so the order is fully determined."""
    layer, seen = [trace_node], {id(trace_node)}
    while layer:
        if len(layer) > 1:
            return False
        nxt = []
        for n in layer:
            for p in n.parents:
                if id(p) not in seen:
                    seen.add(id(p))
                    nxt.append(p)
        layer = nxt
    return True


SETTINGS = [
    pytest.param(
        dict(simplifier=True, cte=False, multiline=True),
        id="simplifier-nocte-multiline",
    ),
    pytest.param(
        dict(simplifier=True, cte=True, multiline=True), id="simplifier-cte-multiline"
    ),
    pytest.param(
        dict(simplifier=False, cte=False, multiline=True),
        id="nosimplifier-nocte-multiline",
    ),
    pytest.param(
        dict(simplifier=True, cte=False, multiline=False),
        id="simplifier-nocte-singleline",
    ),
]


@pytest.fixture(scope="module")
def ctx(session):
    table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    session.sql(f"create temp table {table} (ID int, NAME string, V float)").collect()
    session.sql(f"create temp stage {stage}").collect()
    df_trace_corpus.CURRENT_CTX[0] = Ctx(table=table, stage=stage)
    yield df_trace_corpus.CURRENT_CTX[0]
    session.sql(f"drop table if exists {table}").collect()
    session.sql(f"drop stage if exists {stage}").collect()


@pytest.fixture()
def configured(session, request):
    settings = request.getfixturevalue("settings")
    old = (
        session.sql_simplifier_enabled,
        session.cte_optimization_enabled,
        session._generate_multiline_queries,
    )
    session.sql_simplifier_enabled = settings["simplifier"]
    session.cte_optimization_enabled = settings["cte"]
    (
        session._enable_multiline_queries
        if settings["multiline"]
        else session._disable_multiline_queries
    )()
    yield
    session.sql_simplifier_enabled = old[0]
    session.cte_optimization_enabled = old[1]
    (
        session._enable_multiline_queries
        if old[2]
        else session._disable_multiline_queries
    )()


@pytest.mark.parametrize("settings", SETTINGS)
@pytest.mark.parametrize("workload", WORKLOADS, ids=lambda w: w.name)
def test_lightweight_trace_matches_ast_backend(
    session, ctx, configured, settings, workload, request
):
    if workload.needs_simplifier and not settings["simplifier"]:
        pytest.skip("the API requires the SQL simplifier")
    if workload.name in KNOWN_GAPS:
        pytest.xfail(KNOWN_GAPS[workload.name])
    setting_id = request.node.callspec.id.split("-", 1)[1]
    if (workload.name, setting_id) in KNOWN_SETTING_GAPS:
        pytest.xfail(GAP_3)
    with DebugFeatures(ast=True):
        observed = observe(workload.build, session, ctx, workload.action)

    ast_out, lite_out = observed.oracle.as_dict(), observed.lite.as_dict()
    superset = workload.name in LITE_SUPERSET
    if superset:
        assert _mapped_lines(lite_out["mapped"]) >= _mapped_lines(ast_out["mapped"])
    for part in (
        ("missing_object", "existing_object")
        if superset
        else (
            "mapped",
            "missing_object",
            "existing_object",
        )
    ):
        assert (
            lite_out[part] == ast_out[part]
        ), f"{part} differs for {workload.name}\n--- AST ---\n{ast_out[part]}\n--- lite ---\n{lite_out[part]}"

    ast_header, ast_entries = _parse_lineage(ast_out["lineage"])
    lite_header, lite_entries = _parse_lineage(lite_out["lineage"])
    message = (
        f"lineage differs for {workload.name}\n--- AST ---\n{ast_out['lineage']}"
        f"\n--- lite ---\n{lite_out['lineage']}"
    )
    if superset:
        assert set(lite_entries) >= set(ast_entries), message
    else:
        # Same operations, same count, same most recent operation.
        assert sorted(lite_entries) == sorted(ast_entries), message
        assert lite_header == ast_header, message
        assert lite_entries[:1] == ast_entries[:1], message
        # The AST visits the dataframes of one level in the iteration order of a set of ids, which is
        # arbitrary. Where the order is determined (one node per level) it must be the same.
        if observed.top_plan.df_trace_nodes and _layers_are_single(
            observed.top_plan.df_trace_nodes[-1]
        ):
            assert lite_entries == ast_entries, message

    # The message of the raised exception carries the lightweight output.
    context_text = observed.exception.debug_context or ""
    if lite_out["lineage"]:
        assert lite_out["lineage"] in context_text
