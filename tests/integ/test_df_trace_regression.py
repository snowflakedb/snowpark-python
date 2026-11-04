#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Regression tests of the debug output of the error experience.

For every workload of the corpus and every setting, the debug output (lineage trace, SQL error to
Python source mapping, missing and existing object context) must match ``df_trace_golden.json``.
The snapshot was generated from the AST backend before the debug features moved to the lightweight
DataFrame trace, so it also keeps the output equal to the one of the AST backend, except for the
documented supersets (``LITE_SUPERSET``). After an intended change of the output, regenerate it
with ``SNOWPARK_DF_TRACE_UPDATE_GOLDEN=1`` (run without xdist) and review the diff.
"""

import json
import os
import re
import sys

import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from tests.integ import df_trace_corpus
from tests.integ.df_trace_corpus import WORKLOADS, Ctx
from tests.integ.df_trace_harness import DebugFeatures, normalize_output, observe
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SnowparkSQLException is not raised in local testing mode",
        run=False,
    ),
]

GOLDEN_PATH = os.path.join(os.path.dirname(__file__), "df_trace_golden.json")
UPDATE_GOLDEN = os.environ.get("SNOWPARK_DF_TRACE_UPDATE_GOLDEN") == "1"

with open(GOLDEN_PATH) as _f:
    _GOLDEN_DOC = json.load(_f)
# Before Python 3.11 frames have no column positions, so the source snippets differ.
GOLDEN_KEY = "cases" if sys.version_info >= (3, 11) else "cases_before_py311"
GOLDEN = _GOLDEN_DOC[GOLDEN_KEY]

# Workloads where the output is a superset of the AST output (listed behavior changes).
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


_ENTRY_START = re.compile(r"^[^\s|]+\|\d+:")


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


def _settings_id(settings) -> str:
    return "-".join(
        (
            "simplifier" if settings["simplifier"] else "nosimplifier",
            "cte" if settings["cte"] else "nocte",
            "multiline" if settings["multiline"] else "singleline",
        )
    )


@pytest.fixture(scope="module")
def golden_updates():
    updates = {}
    yield updates
    if UPDATE_GOLDEN and updates:
        _GOLDEN_DOC[GOLDEN_KEY] = dict(sorted({**GOLDEN, **updates}.items()))
        with open(GOLDEN_PATH, "w") as f:
            json.dump(_GOLDEN_DOC, f, indent=1, sort_keys=True)
            f.write("\n")


@pytest.mark.parametrize("settings", SETTINGS)
@pytest.mark.parametrize("workload", WORKLOADS, ids=lambda w: w.name)
def test_debug_output_matches_snapshot(
    session, ctx, configured, golden_updates, settings, workload
):
    if workload.needs_simplifier and not settings["simplifier"]:
        pytest.skip("the API requires the SQL simplifier")
    with DebugFeatures():
        observed = observe(workload.build, session, ctx, workload.action)

    key = f"{workload.name}--{_settings_id(settings)}"
    out = {k: normalize_output(v, ctx) for k, v in observed.output.as_dict().items()}
    if UPDATE_GOLDEN:
        golden_updates[key] = out
        return
    assert key in GOLDEN, f"no snapshot for {key}; regenerate df_trace_golden.json"
    expected = GOLDEN[key]

    superset = workload.name in LITE_SUPERSET
    if superset:
        assert _mapped_lines(out["mapped"]) >= _mapped_lines(expected["mapped"])
    for part in (
        ("missing_object", "existing_object")
        if superset
        else ("mapped", "missing_object", "existing_object")
    ):
        assert (
            out[part] == expected[part]
        ), f"{part} differs for {key}\n--- expected ---\n{expected[part]}\n--- actual ---\n{out[part]}"

    expected_header, expected_entries = _parse_lineage(expected["lineage"])
    header, entries = _parse_lineage(out["lineage"])
    message = (
        f"lineage differs for {key}\n--- expected ---\n{expected['lineage']}"
        f"\n--- actual ---\n{out['lineage']}"
    )
    if superset:
        assert set(entries) >= set(expected_entries), message
    else:
        # Same operations, same count, same most recent operation.
        assert sorted(entries) == sorted(expected_entries), message
        assert header == expected_header, message
        assert entries[:1] == expected_entries[:1], message
        # The AST visited the dataframes of one level in the iteration order of a set of ids, which
        # is arbitrary. Where the order is determined (one node per level) it must be the same.
        if observed.top_plan.df_trace_nodes and _layers_are_single(
            observed.top_plan.df_trace_nodes[-1]
        ):
            assert entries == expected_entries, message

    # The message of the raised exception carries the output.
    context_text = normalize_output(observed.exception.debug_context or "", ctx)

    if out["lineage"]:
        assert out["lineage"] in context_text
