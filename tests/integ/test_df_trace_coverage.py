#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Enforces that the workload corpus exercises the public DataFrame API.

Every public method of the classes below is either called by a workload (measured with a profiler
while the corpus runs), or listed in ``EXCLUDED`` with the reason why it cannot affect the
lineage of an error. A new public method fails this test until it gets a workload or an exclusion.
"""

import inspect
import sys

import pytest

from snowflake.snowpark import (
    DataFrame,
    DataFrameNaFunctions,
    DataFrameReader,
    DataFrameStatFunctions,
    DataFrameWriter,
    RelationalGroupedDataFrame,
    Session,
    Table,
)
import snowflake.snowpark.context as context
from snowflake.snowpark._internal.utils import (
    AstFlagSource,
    TempObjectType,
    is_ast_enabled,
    set_ast_state,
)
from snowflake.snowpark.udaf import UDAFRegistration
from snowflake.snowpark.udf import UDFRegistration
from snowflake.snowpark.udtf import UDTFRegistration
from tests.integ import df_trace_corpus
from tests.integ.df_trace_corpus import WORKLOADS, Ctx
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SnowparkSQLException is not raised in local testing mode",
        run=False,
    ),
]

SESSION_FACTORIES = {
    "create_dataframe",
    "sql",
    "table",
    "range",
    "generator",
    "table_function",
    "flatten",
}

CLASSES = [
    UDFRegistration,
    UDTFRegistration,
    UDAFRegistration,
    DataFrame,
    Table,
    RelationalGroupedDataFrame,
    DataFrameNaFunctions,
    DataFrameStatFunctions,
    DataFrameReader,
    DataFrameWriter,
]

_NO_CHAIN = "does not produce a dataframe or a query that can fail with a lineage"
_ASYNC = "async job: the error is raised by the connector from AsyncJob.result(), not by a plan"
_EXTERNAL = "needs an external system or Cortex that the test account does not provide"
_DML = "DML on a table, not a derivation of a dataframe"

# function name -> reason
EXCLUDED = {
    # DataFrame / Table
    "collect_nowait": _ASYNC,
    "explain": "does not fail for an invalid plan",
    "get_execution_profile": "profiling output only",
    "to_arrow": "needs pyarrow output; same plan execution path as to_pandas",
    "to_arrow_batches": "needs pyarrow output; same plan execution path as to_pandas_batches",
    "to_polars": "needs polars; same plan execution path as to_pandas",
    "to_snowpark_pandas": "needs modin; creates a Snowpark pandas dataframe",
    "create_or_replace_dynamic_table": "needs a target lag and warehouse; same view-style plan",
    "copy_into_table": "requires a reader-created dataframe and staged files",
    "cov": "stat function, same execution path as corr workload",
    "delete": _DML,
    "update": _DML,
    "merge": _DML,
    "drop_table": "drops a table",
    # grouped
    "ai_agg": _EXTERNAL,
    "apply_in_pandas": "needs a UDTF registration and pandas execution",
    # reader
    "avro": "same reader plan path as csv/parquet (file format option only)",
    "orc": "same reader plan path as csv/parquet (file format option only)",
    "xml": "same reader plan path as csv/parquet (file format option only)",
    "file": "returns a file object; not a dataframe",
    "dbapi": _EXTERNAL,
    "jdbc": _EXTERNAL,
    "directory": "needs a directory-enabled stage",
    "documents": _EXTERNAL,
    "with_metadata": "reader option, same reader plan path",
    # writer
}


def _public_functions():
    """code object -> sorted public names, for every public function of the API classes."""
    universe = {}
    for cls in CLASSES:
        for name, member in inspect.getmembers(cls):
            if name.startswith("_") or not callable(member):
                continue
            target = inspect.unwrap(member)
            code = getattr(target, "__code__", None)
            if code is None:
                continue
            universe.setdefault(code, set()).add(name)
    for name in SESSION_FACTORIES:
        member = getattr(Session, name)
        code = inspect.unwrap(member).__code__
        universe.setdefault(code, set()).add(name)
    return universe


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


def test_corpus_covers_public_api(session, ctx):
    universe = _public_functions()
    called = set()

    def profiler(frame, event, arg):
        if event == "call" and frame.f_code in universe:
            called.add(frame.f_code)

    old_ast = is_ast_enabled()
    context.configure_development_features(
        enable_dataframe_trace_on_error=True,
        enable_trace_sql_errors_to_dataframe=True,
    )
    set_ast_state(AstFlagSource.TEST, False)
    sys.setprofile(profiler)
    try:
        for workload in WORKLOADS:
            try:
                workload.action(workload.build(session, ctx))
            except Exception:  # noqa: BLE001 - the workloads are meant to fail
                pass
    finally:
        sys.setprofile(None)
        context.configure_development_features()
        set_ast_state(AstFlagSource.TEST, old_ast)

    uncovered = {
        sorted(names)[0]
        for code, names in universe.items()
        if code not in called and not (names & EXCLUDED.keys())
    }
    assert not uncovered, (
        "public API without a workload (add one to df_trace_corpus.py or exclude it with a reason): "
        f"{sorted(uncovered)}"
    )

    known = {n for names in universe.values() for n in names}
    stale = {n for n in EXCLUDED if n not in known}
    assert not stale, f"exclusions that name no public API: {sorted(stale)}"
    covered_names = {
        n for code, names in universe.items() if code in called for n in names
    }
    redundant = {n for n in EXCLUDED if n in covered_names}
    assert (
        not redundant
    ), f"excluded but covered, remove from EXCLUDED: {sorted(redundant)}"
