"""Test the WIP "pinpoint to one line" tracing inside a stored procedure.

Background: a sproc's handler runs server-side against the *published*
snowflake-snowpark-python from the Anaconda channel, not this working tree. That
release reports the same version string (1.54.0) as local HEAD but lacks the three
WIP commits, so `_narrow_source_locations_by_position` is simply not there and the
error falls back to listing every dataframe op merged onto the SQL line.

Rather than stage a build of this tree, the handler below monkeypatches the WIP
implementation onto the released module in-process. Two details make this work:

  * `snowflake_plan.py` does a module-level `from ..debug_utils import
    get_python_source_from_sql_error`, so patching `debug_utils` alone is a no-op.
    Both namespaces have to be rebound.
  * The released `_extract_source_locations_from_plan` takes no `ast_ids`
    argument (the WIP added it), so we ship our own copy rather than call it with
    the new signature and get a TypeError swallowed by the diagnostics try/except.

CREATE_FOO_SQL is a raw string on purpose: the handler needs the two characters
`\\` `n` to reach the server so the inner Python parses them as an escape. A
non-raw wrapper would embed real newlines inside single-quoted literals and the
handler would fail with SyntaxError.

Not covered here: the WIP also widens the gate in `SnowflakePlan.Decorator.wrap_exception`
from `"SQL compilation error:" in e.msg` to `"error line" in e.msg.lower()`, which is what
lets execution-time errors ("Error line 13 position 10: Division by zero") be traced. That
decorator is applied at class-definition time, so it cannot be patched after import. This
demo therefore only exercises the compilation-error path, which passes the released gate
unchanged.
"""

from snowflake.snowpark import Session

session = Session.builder.config("connection_name", "zyao_ent_preprod9").create()

CREATE_FOO_SQL = r'''
CREATE OR REPLACE PROCEDURE FOO()
RETURNS STRING
LANGUAGE PYTHON
packages=('snowflake-snowpark-python')
runtime_version=3.13
handler='run'
AS $$
from typing import Any, List, Optional
import re

from snowflake.snowpark.context import configure_development_features
from snowflake.snowpark.functions import col, avg, count
from snowflake.snowpark.exceptions import SnowparkSQLException

from snowflake.snowpark._internal import debug_utils
from snowflake.snowpark._internal.analyzer import snowflake_plan as snowflake_plan_mod


# --- WIP implementation, lifted from debug_utils.py -------------------------------

SQL_ERROR_LINE_REGEX = re.compile(
    r"error line (\d+)\s*(?:at\s+)?position (\d+)", re.IGNORECASE
)


def _extract_source_locations_from_plan(plan, ast_ids=None) -> List[str]:
    """Released copy takes no ast_ids, so we carry the WIP signature ourselves."""
    source_locations = []
    found_locations = set()

    if ast_ids is None:
        ast_ids = plan.df_ast_ids

    if ast_ids is not None:
        for ast_id in ast_ids:
            bind_stmt = plan.session._ast_batch._bind_stmt_cache.get(ast_id)
            if bind_stmt is not None:
                src = debug_utils.extract_src_from_expr(bind_stmt.bind.expr)
                location = debug_utils._format_source_location(src)
                if location and location not in found_locations:
                    found_locations.add(location)
                    source_locations.append(location)

    return source_locations


def _flatten_and_conjuncts(expression: Any) -> List[Any]:
    from snowflake.snowpark._internal.analyzer.binary_expression import And

    if isinstance(expression, And):
        return _flatten_and_conjuncts(expression.left) + _flatten_and_conjuncts(
            expression.right
        )
    return [expression]


def _ast_ids_for_variant(plan, variant: str) -> List[int]:
    ast_ids = []
    for ast_id in plan.df_ast_ids or []:
        bind_stmt = plan.session._ast_batch._bind_stmt_cache.get(ast_id)
        if bind_stmt is not None and bind_stmt.bind.expr.WhichOneof("variant") == variant:
            ast_ids.append(ast_id)
    return ast_ids


def _narrow_source_locations_by_position(
    top_plan, plan, sql_line_number: int, sql_position: int
) -> Optional[List[str]]:
    source_plan = getattr(plan, "source_plan", None)
    where = getattr(source_plan, "where", None)
    if where is None:
        return None

    conjuncts = _flatten_and_conjuncts(where)
    if len(conjuncts) < 2:
        return None

    filter_ast_ids = _ast_ids_for_variant(plan, "dataframe_filter")
    if len(filter_ast_ids) != len(conjuncts):
        return None

    try:
        sql_line = top_plan.queries[-1].sql.split("\n")[sql_line_number]
    except IndexError:
        return None

    analyzer = source_plan.analyzer
    alias_map = source_plan.df_aliased_col_name_to_real_col_name
    offset = sql_position - 1

    for conjunct, ast_id in zip(conjuncts, filter_ast_ids):
        rendered = analyzer.analyze(conjunct, alias_map)
        if not rendered:
            continue
        start = sql_line.find(rendered)
        while start >= 0:
            if start <= offset < start + len(rendered):
                return _extract_source_locations_from_plan(plan, [ast_id])
            start = sql_line.find(rendered, start + 1)

    return None


def get_python_source_from_sql_error(top_plan, error_msg: str) -> str:
    match = SQL_ERROR_LINE_REGEX.search(error_msg)
    if not match:
        return ""

    sql_line_number = int(match.group(1)) - 1
    sql_position = int(match.group(2))

    from snowflake.snowpark._internal.utils import get_plan_from_line_numbers

    plan = get_plan_from_line_numbers(top_plan, sql_line_number)

    source_locations = None
    try:
        source_locations = _narrow_source_locations_by_position(
            top_plan, plan, sql_line_number, sql_position
        )
    except Exception as narrow_error:
        # Surface this instead of debug-logging it: sproc logs are not readable here,
        # and a silent narrowing failure is indistinguishable from "did not apply".
        return (
            "\nNARROWING RAISED: "
            + type(narrow_error).__name__
            + ": "
            + str(narrow_error)
            + "\n"
        )
    if not source_locations:
        source_locations = _extract_source_locations_from_plan(plan)

    if source_locations:
        error_kind = (
            "SQL compilation error"
            if "SQL compilation error" in error_msg
            else "SQL error"
        )
        if len(source_locations) == 1:
            return f"\n{error_kind} corresponds to Python source at {source_locations[0]}.\n"
        else:
            locations_str = "\n  - ".join(source_locations)
            return f"\n{error_kind} corresponds to Python sources at:\n  - {locations_str}\n"
    return ""


# --- apply the patch --------------------------------------------------------------

_PATCH_REPORT = []

_PATCH_REPORT.append(
    "released had _narrow_source_locations_by_position="
    + str(hasattr(debug_utils, "_narrow_source_locations_by_position"))
)

debug_utils.SQL_ERROR_LINE_REGEX = SQL_ERROR_LINE_REGEX
debug_utils._flatten_and_conjuncts = _flatten_and_conjuncts
debug_utils._ast_ids_for_variant = _ast_ids_for_variant
debug_utils._narrow_source_locations_by_position = _narrow_source_locations_by_position
debug_utils.get_python_source_from_sql_error = get_python_source_from_sql_error

# The one that actually matters: snowflake_plan holds its own reference from import time.
assert hasattr(snowflake_plan_mod, "get_python_source_from_sql_error"), (
    "snowflake_plan no longer imports get_python_source_from_sql_error by name; "
    "find where wrap_exception resolves it before trusting this run"
)
snowflake_plan_mod.get_python_source_from_sql_error = get_python_source_from_sql_error

_PATCH_REPORT.append(
    "snowflake_plan rebound="
    + str(
        snowflake_plan_mod.get_python_source_from_sql_error
        is get_python_source_from_sql_error
    )
)

configure_development_features(
    enable_dataframe_trace_on_error=False,
    enable_trace_sql_errors_to_dataframe=True,
)


def run(session):
    df = session.sql(
        """
        SELECT column1 AS NAME, column2 AS AGE, column3 AS DEPT FROM VALUES
        ('Alice', 30, 'Engineering'),
        ('Bob', 25, 'Marketing'),
        ('Carol', 35, 'Engineering'),
        ('Dave', 28, 'Marketing'),
        ('Eve', 32, 'Engineering')
    """
    )

    df_valid = df.select(col("NAME"), col("AGE"), col("DEPT"))
    df_valid.show()

    # Pipeline 2: independent age statistics (not chained from df2/df3/df4)
    df_stats = df.group_by(col("DEPT")).agg(
        avg("AGE").alias("AVG_AGE"), count("*").alias("CNT")
    )
    df_senior = df_stats.filter(col("AVG_AGE") > 30)

    # Pipeline 1: department-level analysis
    df2_eng = df.select(col("NAME"), col("DEPT"))
    df2_extra = df.filter(col("AGE") > 30).select(col("NAME"), col("DEPT"))
    df2 = df2_eng.union(df2_extra)

    # Pipeline 3: lookup-style independent DataFrame
    df_threshold = session.sql("SELECT 28 AS MIN_AGE, 'Marketing' AS TARGET_DEPT")

    # Continue on Pipeline 1. This line is the only real culprit; the released code
    # blames this line plus the union above and the filter below.
    df3 = df2.filter(10 / (col("AGE_NON_EXISTENT_COLUMN") % 5) >= 1)
    df4 = df3.filter(col("DEPT") == "Engineering")

    df_filtered = df.join(
        df_threshold, (col("AGE") >= col("MIN_AGE")) & (col("DEPT") == col("TARGET_DEPT"))
    )

    # Return the enhanced message as a value rather than letting it propagate, so the
    # debug section is readable instead of buried in a nested server traceback.
    try:
        df4.show()
    except SnowparkSQLException as e:
        # str(e), not e.message: debug_context is a separate attribute that only
        # __str__ concatenates, so e.message drops the whole debug section.
        return "\n".join(_PATCH_REPORT) + "\n\n" + str(e)

    df_senior.show()
    df_filtered.show()
    return "\n".join(_PATCH_REPORT) + "\n\nUNEXPECTED: df4.show() did not raise"
$$
'''

session.sql(CREATE_FOO_SQL).collect()

print(session.sql("CALL FOO()").collect()[0][0])
