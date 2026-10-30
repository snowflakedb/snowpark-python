#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from types import SimpleNamespace
from unittest.mock import patch

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark import Row, Window
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark.exceptions import SnowparkPlanException, SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    count,
    lit,
    parse_json,
    row_number,
    sql_expr,
    sum as sum_,
)
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="Metadata validation is not used in local testing mode",
    ),
]


@pytest.fixture(autouse=True)
def metadata_validation_on():
    context.configure_development_features(enable_metadata_validation=True)
    yield
    context.configure_development_features(enable_metadata_validation=False)


@pytest.fixture
def base(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    return session.create_dataframe([(1, "x"), (2, "y")], schema=["a", "b"])


MISSING_COLUMN_OPS = [
    pytest.param(lambda df: df.select("NOPE"), id="select_str"),
    pytest.param(lambda df: df.select(col("NOPE")), id="select_col"),
    pytest.param(lambda df: df.select("a", "NOPE"), id="select_valid_and_missing"),
    pytest.param(lambda df: df.select(col("NOPE") + 1), id="select_expr"),
    pytest.param(lambda df: df.with_column("n", col("NOPE")), id="with_column"),
    pytest.param(lambda df: df.with_columns(["n"], [col("NOPE")]), id="with_columns"),
    pytest.param(lambda df: df.filter(col("NOPE") > 1), id="filter"),
    pytest.param(
        lambda df: df.where(col("a") > 0).filter(col("NOPE") > 1), id="filter_chain"
    ),
    pytest.param(lambda df: df.sort("NOPE"), id="sort_str"),
    pytest.param(lambda df: df.sort(col("NOPE").desc()), id="sort_col"),
]


@pytest.mark.parametrize("op", MISSING_COLUMN_OPS)
@pytest.mark.parametrize("derived", [False, True], ids=["fresh", "after_select"])
def test_missing_column_raises_at_construction(base, op, derived):
    df = base.select("a", "b") if derived else base
    with pytest.raises(SnowparkPlanException) as ex_info:
        op(df)
    assert ex_info.value.error_code == "1200"
    assert 'Invalid identifier "NOPE"' in str(ex_info.value)
    assert "There are existing quoted column identifiers: ['\"A\"', '\"B\"']" in str(
        ex_info.value
    )


def test_error_suggests_case_sensitive_match(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.sql('select 1 as "abc", 2 as "Mixed"')
    with pytest.raises(SnowparkPlanException) as ex_info:
        df.select("ABC")
    assert "Do you mean '\"abc\"'?" in str(ex_info.value)
    # quoted names are case sensitive
    with pytest.raises(SnowparkPlanException):
        df.select('"mixed"')


# one operation per kind: select, with_column, filter, sort
@pytest.mark.parametrize("op", [MISSING_COLUMN_OPS[i] for i in (0, 4, 6, 8)])
def test_disabled_flag_keeps_server_error(base, op):
    context.configure_development_features(enable_metadata_validation=False)
    df = op(base)
    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        df.collect()


@pytest.fixture
def frames(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    d1 = session.create_dataframe([(1, "x", 10), (2, "y", 20)], schema=["id", "v", "n"])
    d2 = session.create_dataframe([(1, "p", 30), (2, "q", 40)], schema=["id", "v", "n"])
    d3 = session.create_dataframe([(1, "z", 50)], schema=["id", "v", "n"])
    return SimpleNamespace(
        session=session,
        d1=d1,
        d2=d2,
        d3=d3,
        j=d1.join(d2, d1["id"] == d2["id"]),
        mixed=session.sql('select 1 as "Mixed", 2 as x'),
    )


# Operations that are valid or fail on the server. The flag must not change their outcome, so
# each is run with it off and on. A name only the join renamed stays a server-side ambiguous-join
# error. Missing columns are the only intended difference and are covered separately.
UNCHANGED_OPS = {
    "select": lambda f: f.d1.select("id", "v"),
    "select_star": lambda f: f.d1.select("*").filter(col("n") > 10),
    "select_expr": lambda f: f.d1.select_expr("n + 1 as c").filter(sql_expr("c > 11")),
    "select_dollar": lambda f: f.d1.select(col("$1")),
    "lateral_alias": lambda f: f.d1.select((col("n") + 1).alias("x"), col("x") + 1),
    "with_column_replace": lambda f: f.d1.with_column("n", col("n") + 1).select("n"),
    "with_columns_chain": lambda f: f.d1.with_column("c", col("n") + 1)
    .with_column("d", col("c") + 1)
    .filter(col("d") > 0)
    .sort("d"),
    "filter_dropped_column": lambda f: f.d1.select("id").filter(col("v") == "y"),
    "sort_alias": lambda f: f.d1.select(col("n").alias("z")).sort("z"),
    "sort_dropped_column": lambda f: f.d1.select("id").sort("n"),
    "rename_then_filter": lambda f: f.d1.with_column_renamed("n", "z").filter(
        col("z") > 0
    ),
    "drop_then_filter": lambda f: f.d1.drop("v").filter(col("n") > 0).sort("id"),
    "to_df": lambda f: f.d1.to_df("a", "b", "c").select("a", "c").filter(col("c") > 0),
    "lower_case_names": lambda f: f.d1.select("id", "v")
    .filter(col("id") > 0)
    .sort("v"),
    "quoted_mixed_case": lambda f: f.mixed.group_by('"Mixed"')
    .count()
    .filter(col('"Mixed"') > 0)
    .select('"Mixed"', "COUNT"),
    "variant_path": lambda f: f.session.sql("select parse_json('{\"k\":1}') as V")
    .select(col("V")["k"])
    .filter(col("V")["k"] > 0),
    "group_by_agg": lambda f: f.d1.group_by("id")
    .agg(sum_("n").alias("s"))
    .select("s")
    .filter(col("s") > 0),
    "window": lambda f: f.d1.select(
        "id", row_number().over(Window.order_by("n")).alias("r")
    ).filter(col("r") == 1),
    "union": lambda f: f.d1.union(f.d1).select("id").filter(col("id") > 0),
    "pivot": lambda f: f.d1.pivot("v", ["x", "y"]).sum("n").select("id").sort("id"),
    "table_function": lambda f: f.d1.join_table_function(
        "flatten", parse_json(lit("[1,2]"))
    )
    .select("id", "VALUE")
    .filter(col("VALUE") > 0),
    "join_using": lambda f: f.d1.join(f.d2.select("id", "n"), "id").select("id", "v"),
    "join_lsuffix": lambda f: f.d1.join(
        f.d2, f.d1["id"] == f.d2["id"], lsuffix="_l", rsuffix="_r"
    )
    .select("V_L", "V_R")
    .filter(col("V_L") == "x"),
    "natural_join": lambda f: f.d1.natural_join(f.d3).filter(col("id") == 1),
    "cross_join": lambda f: f.d1.cross_join(f.d3.select(col("n").alias("m")))
    .select("v", "m")
    .filter(col("m") > 0),
    "join_then_agg_then_join": lambda f: f.d1.group_by("id")
    .agg(count("*").alias("c"))
    .join(f.d2.select("id", "n"), "id")
    .select("id", "c", "n")
    .filter(col("c") > 0),
    "same_name_join_select": lambda f: f.j.select(f.d1["v"], f.d2["v"]).filter(
        f.d1["v"] == "x"
    ),
    "same_name_join_clauses": lambda f: f.j.filter(f.d1["v"] == "x").sort(f.d2["v"]),
    "same_name_join_with_column": lambda f: f.j.with_column(
        "z", f.d1["n"] + f.d2["n"]
    ).filter(f.d2["n"] > 0),
    "same_name_join_alias": lambda f: f.j.select(f.d1["v"].alias("a")).filter(
        col("a") == "x"
    ),
    "same_name_join_group_by": lambda f: f.j.group_by(f.d1["v"])
    .agg(sum_(f.d2["n"]).alias("s"))
    .filter(col("s") > 0),
    "same_name_join_window": lambda f: f.j.select(
        f.d1["v"], row_number().over(Window.order_by(f.d2["n"])).alias("r")
    ).filter(col("r") == 1),
    "same_name_join_drop": lambda f: f.j.drop(f.d1["v"]).select(f.d2["v"]),
    "same_name_join_renamed_column": lambda f: f.j.with_column_renamed(
        f.d1["v"], "VV"
    ).select("VV"),
    "three_way_join": lambda f: f.j.join(f.d3, f.d1["id"] == f.d3["id"])
    .select(f.d1["v"], f.d2["v"], f.d3["v"])
    .filter(f.d3["v"] == "z"),
    "union_of_joins": lambda f: f.j.select(f.d1["v"].alias("V"))
    .union_all(f.j.select(f.d2["v"].alias("V")))
    .filter(col("V") == "x"),
    "parent_column_after_select": lambda f: f.d1.select(
        (col("n") + 1).alias("m")
    ).filter(f.d1["n"] > 0),
    "aliased_dataframes": lambda f: f.d1.alias("l")
    .join(f.d2.alias("r"), col("l", "id") == col("r", "id"))
    .select(col("l", "v"), col("r", "v")),
    "ambiguous_join_select": lambda f: f.j.select("v"),
    "ambiguous_join_filter": lambda f: f.j.filter(col("id") == 1),
    "ambiguous_join_sort": lambda f: f.j.sort("id"),
}


def _outcome(build, frames):
    try:
        return "ok", sorted(str(row) for row in build(frames).collect())
    except Exception as ex:
        return "error", type(ex).__name__


@pytest.mark.parametrize("name", UNCHANGED_OPS)
def test_flag_does_not_change_outcome(frames, name):
    build = UNCHANGED_OPS[name]
    context.configure_development_features(enable_metadata_validation=False)
    expected = _outcome(build, frames)
    context.configure_development_features(enable_metadata_validation=True)
    assert _outcome(build, frames) == expected
    if name.startswith("ambiguous_join"):
        assert expected == ("error", "SnowparkSQLAmbiguousJoinException")
    else:
        assert expected[0] == "ok"


def test_simplifier_disabled_is_noop(session):
    original = session.sql_simplifier_enabled
    session.sql_simplifier_enabled = False
    try:
        df = session.create_dataframe([(1,)], schema=["a"])
        bad = df.select("NOPE")  # no early error without the simplifier
        with pytest.raises(SnowparkSQLException):
            bad.collect()
    finally:
        session.sql_simplifier_enabled = original


def _count_describes(fn):
    calls = []
    original = SnowflakePlan._analyze_attributes

    def counting(self):
        calls.append(1)
        return original(self)

    with patch.object(SnowflakePlan, "_analyze_attributes", counting):
        try:
            fn()
        except Exception:
            pass
    return len(calls)


@pytest.mark.parametrize("reduce_describe", [True, False])
def test_no_extra_describe_queries(session, reduce_describe):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    original = session.reduce_describe_query_enabled
    session.reduce_describe_query_enabled = reduce_describe
    try:

        def chain():
            df = session.create_dataframe([(1, "x")], schema=["a", "b"])
            return (
                df.select("a", "b")
                .filter(col("a") > 0)
                .with_column("c", col("a") + 1)
                .sort("c")
                .select("c", "b")
            )

        def failing():
            session.create_dataframe([(1, "x")], schema=["a", "b"]).filter(
                col("NOPE") > 0
            )

        context.configure_development_features(enable_metadata_validation=False)
        describes_off = _count_describes(chain)
        failing_off = _count_describes(failing)
        context.configure_development_features(enable_metadata_validation=True)
        describes_on = _count_describes(chain)
        failing_on = _count_describes(failing)
        assert describes_on == describes_off
        assert failing_on == failing_off
        if reduce_describe:
            assert describes_on == 0
    finally:
        session.reduce_describe_query_enabled = original


def test_table_frame_validates_without_extra_describe(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    table_name = Utils.random_table_name()
    try:
        session.create_dataframe([(1, "x")], schema=["a", "b"]).write.save_as_table(
            table_name, table_type="temp"
        )

        def build():
            return session.table(table_name).filter(col("NOPE") > 0)

        context.configure_development_features(enable_metadata_validation=False)
        describes_off = _count_describes(build)
        context.configure_development_features(enable_metadata_validation=True)
        describes_on = _count_describes(build)
        with pytest.raises(SnowparkPlanException):
            build()
        assert describes_on == describes_off
    finally:
        Utils.drop_table(session, table_name)


def test_combined_with_eager_schema_validation(base):
    context.configure_development_features(enable_eager_schema_validation=True)
    try:
        with pytest.raises(SnowparkPlanException):
            base.select("NOPE")
        Utils.check_answer(base.select("a"), [Row(1), Row(2)])
    finally:
        context.configure_development_features(enable_eager_schema_validation=False)
