#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""CTE deduplication tests for _is_snowpark_connect_compatible_mode.

When connect-compatible mode is enabled, two different DataFrame objects that
produce the same SQL should NOT be merged into a single CTE. Only the same
Python object referenced multiple times (e.g. df.union_all(df)) should be
deduplicated.

This prevents incorrect results when non-deterministic functions like
uuid_string() are used: df1.union_all(df2) should produce two independent
evaluations, not a single CTE referenced twice.

Tests cover:
1. Union with two distinct DFs (no CTE in connect mode)
2. Union with same DF ref (CTE still applies in connect mode)
3. Join-based triggers with distinct DFs
4. Chained operations producing imbalanced subtrees
"""

import copy
from unittest import mock

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark.functions import col, lit, random, uuid_string
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="CTE is a SQL feature",
        run=False,
    ),
]


@pytest.fixture(scope="module")
def test_table_name(session):
    """Create a shared test table with columns a (INT) and b (INT)."""
    name = random_name_for_temp_object(TempObjectType.TABLE)
    session.sql(
        f"""
        CREATE OR REPLACE TEMP TABLE {name} (a INT, b INT)
        """
    ).collect()
    session.sql(
        f"""
        INSERT INTO {name} VALUES (1, 2), (3, 4), (5, 6), (7, 8), (9, 10)
        """
    ).collect()
    yield name
    session.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.fixture(autouse=True)
def enable_connect_compatible_mode():
    """Patch _is_snowpark_connect_compatible_mode to True for all tests in this module."""
    with mock.patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        yield


def _get_query_cte_off_and_on(session, df):
    """Get the last generated query with CTE off and CTE on using mock.patch.object."""
    with mock.patch.object(session, "_cte_optimization_enabled", False):
        query_off = df.queries["queries"][-1]
    with mock.patch.object(session, "_cte_optimization_enabled", True):
        query_on = df.queries["queries"][-1]
    return query_off, query_on


def _collect_uuid_halves(df):
    """Collect a union DataFrame and return (top_half_uuids, bottom_half_uuids)."""
    result = df.collect()
    half = len(result) // 2
    assert half > 0, "Need at least 2 rows to compare halves"
    top = [row["U"] for row in result[:half]]
    bot = [row["U"] for row in result[half:]]
    return top, bot


def assert_cte_sql_shape(
    query_off: str, query_on: str, expect_cte: bool = True
) -> None:
    """Assert that generated SQL has the expected CTE shape (no result comparison).

    Args:
        query_off: The last generated query with CTE optimization disabled.
        query_on: The last generated query with CTE optimization enabled.
        expect_cte: If True, assert query_on starts with WITH. If False, assert it does not.
    """
    assert (
        not query_off.strip().upper().startswith("WITH")
    ), f"CTE OFF should not produce CTE SQL, got: {query_off[:120]}"
    if expect_cte:
        assert (
            query_on.strip().upper().startswith("WITH")
        ), f"CTE ON should produce CTE SQL, got: {query_on[:120]}"
    else:
        assert (
            not query_on.strip().upper().startswith("WITH")
        ), f"Expected no CTE, but got CTE SQL: {query_on[:120]}"


# ---------------------------------------------------------------------------
# Union: two distinct DFs vs same DF ref
# ---------------------------------------------------------------------------


def test_two_dfs_same_sql_no_cte_in_connect_mode(session, test_table_name):
    """Two independently constructed DataFrames with uuid_string() should NOT
    be merged into a CTE in connect-compatible mode.
    Values in each half should differ (independent evaluations)."""
    base = session.table(test_table_name).select("a", "b")
    df1 = base.select("a", uuid_string().alias("u"))
    df2 = base.select("a", uuid_string().alias("u"))
    df_union = df1.union_all(df2)

    query_off, query_on = _get_query_cte_off_and_on(session, df_union)
    assert_cte_sql_shape(query_off, query_on, expect_cte=False)

    top, bot = _collect_uuid_halves(df_union)
    assert (
        top != bot
    ), "Two distinct DFs should produce different uuid values in each half"


def test_same_df_ref_still_uses_cte_in_connect_mode(session, test_table_name):
    """A single DataFrame referenced twice (df.union_all(df)) should still be
    deduplicated in connect-compatible mode.
    Values in each half should be identical (same CTE evaluation reused)."""
    base = session.table(test_table_name).select("a", "b")
    df = base.select("a", uuid_string().alias("u"))
    df_union = df.union_all(df)

    query_off, query_on = _get_query_cte_off_and_on(session, df_union)
    assert_cte_sql_shape(query_off, query_on, expect_cte=True)

    top, bot = _collect_uuid_halves(df_union)
    assert (
        top == bot
    ), "Same DF ref should produce identical uuid values in each half (CTE reuse)"


def test_connect_mode_with_random(session, test_table_name):
    """random() with two separate DataFrames should not be CTE-merged in connect mode.
    The random values in each half should differ (independent evaluations)."""
    base = session.table(test_table_name).select("a")
    df1 = base.select("a", random().alias("r"))
    df2 = base.select("a", random().alias("r"))
    df_union = df1.union_all(df2)

    query_off, query_on = _get_query_cte_off_and_on(session, df_union)
    assert_cte_sql_shape(query_off, query_on, expect_cte=False)

    result = df_union.collect()
    half = len(result) // 2
    top_vals = [row["R"] for row in result[:half]]
    bot_vals = [row["R"] for row in result[half:]]
    assert (
        top_vals != bot_vals
    ), "Two distinct DFs with random() should produce different values"


# ---------------------------------------------------------------------------
# Join: two distinct DFs with same SQL joined together
# ---------------------------------------------------------------------------


def test_join_two_distinct_dfs_no_cte_in_connect_mode(session, test_table_name):
    """Two independently constructed DataFrames joined together should not
    be CTE-merged when they are different objects in connect mode.
    The uuid columns from each side should contain different values."""
    base = session.table(test_table_name).select("a", "b")
    df1 = base.select("a", uuid_string().alias("u"))
    df2 = base.select("a", uuid_string().alias("u"))
    df_joined = df1.join(df2, df1["a"] == df2["a"])

    query_off, query_on = _get_query_cte_off_and_on(session, df_joined)
    assert_cte_sql_shape(query_off, query_on, expect_cte=False)

    result = df_joined.collect()
    assert len(result) > 0
    lhs_uuids = {row[1] for row in result}
    rhs_uuids = {row[3] for row in result}
    assert (
        lhs_uuids != rhs_uuids
    ), "Joined distinct DFs should have different uuid columns"


def test_join_same_df_ref_uses_cte_in_connect_mode(session, test_table_name):
    """A single DataFrame self-joined (via copy.copy) should still trigger CTE
    in connect-compatible mode because the underlying from_ is the same object."""
    base = session.table(test_table_name).select(
        col("a").alias("a"), col("b").alias("b")
    )
    df = base.select(col("a").alias("a"), lit(1).alias("v"))
    df_joined = df.natural_join(copy.copy(df))

    query_off, query_on = _get_query_cte_off_and_on(session, df_joined)
    assert_cte_sql_shape(query_off, query_on, expect_cte=True)


# ---------------------------------------------------------------------------
# Chained operations producing imbalanced subtrees
# ---------------------------------------------------------------------------


def test_chained_filter_union_imbalanced_no_cte_connect_mode(session, test_table_name):
    """Chained operations that produce structurally different trees but
    identical SQL at the leaf level. The two branches have different depths.

    Tree:     union_all
             /         \\
          filter        select
            |              |
         select(uuid)   select(uuid)   ← different objects, same SQL
            |              |
          base           base           ← same object (shared); skipped
                                          by is_simple_select_entity

    In connect mode, the two select(uuid) nodes are different objects and
    should not be CTE-merged, even though they have the same encoded id.
    """
    base = session.table(test_table_name).select("a", "b")
    df1 = base.select("a", uuid_string().alias("u"))
    df2 = base.select("a", uuid_string().alias("u"))
    df1_filtered = df1.filter(col("a") > 1)
    df_union = df1_filtered.union_all(df2)

    query_off, query_on = _get_query_cte_off_and_on(session, df_union)
    assert_cte_sql_shape(query_off, query_on, expect_cte=False)

    result = df_union.collect()
    uuids = {row["U"] for row in result}
    assert len(uuids) == len(
        result
    ), "All uuids should be unique across distinct DF branches"


def test_chained_agg_union_imbalanced_no_cte_connect_mode(session, test_table_name):
    """Imbalanced tree where both branches aggregate independently.

    Tree:     union_all
             /         \\
        group_by       group_by
            |              |
        select(uuid)   select(uuid)   ← different objects, same SQL
            |              |
          base           base          ← same object (shared); skipped
                                         by is_simple_select_entity

    The two select(uuid) nodes produce the same SQL but are different objects.
    """
    base = session.table(test_table_name).select("a", "b")
    df1 = base.select("a", uuid_string().alias("u"))
    df2 = base.select("a", uuid_string().alias("u"))
    df1_agg = df1.group_by("a").count()
    df2_agg = df2.group_by("a").count()
    df_union = df1_agg.union_all(df2_agg)

    query_off, query_on = _get_query_cte_off_and_on(session, df_union)
    assert_cte_sql_shape(query_off, query_on, expect_cte=False)

    result = df_union.collect()
    assert len(result) > 0, "Aggregated union should produce rows"


def test_chained_join_then_union_imbalanced_connect_mode(session, test_table_name):
    """Three distinct DFs combined: two joined, then unioned with a third.

    Tree:        union_all
                /         \\
             join         select(uuid)  ← df3: independent
            /    \\
    select(uuid)  select(uuid)          ← df1, df2: independent
        |            |
      base         base                 ← same object (shared); skipped
                                          by is_simple_select_entity

    All three select(uuid) nodes have the same SQL but are different objects.
    None should be CTE-merged in connect mode.
    """
    base = session.table(test_table_name).select("a", "b")
    df1 = base.select("a", uuid_string().alias("u"))
    df2 = base.select("a", uuid_string().alias("u"))
    df3 = base.select("a", uuid_string().alias("u"))
    df_joined = df1.join(df2, df1["a"] == df2["a"]).select(
        df1["a"].alias("a"), df1["u"].alias("u")
    )
    df_union = df_joined.union_all(df3)

    query_off, query_on = _get_query_cte_off_and_on(session, df_union)
    assert_cte_sql_shape(query_off, query_on, expect_cte=False)

    result = df_union.collect()
    assert len(result) > 0, "Join-then-union should produce rows"
    uuids = [row["U"] for row in result]
    assert len(set(uuids)) == len(
        uuids
    ), "All uuids should be unique across the three distinct DFs"


def test_chained_operations_same_ref_shared_subtree_cte_connect_mode(
    session, test_table_name
):
    """Chained operations where the same object is used in both branches.
    CTE should still apply in connect mode because it's the same Python object.

    Tree:     union_all
             /         \\
          filter       filter
            |            |
           df           df     ← same object referenced twice

    """
    base = session.table(test_table_name).select(
        col("a").alias("a"), col("b").alias("b")
    )
    df = base.select(col("a").alias("a"), uuid_string().alias("u"))
    left = df.filter(col("a") > 1)
    right = df.filter(col("a") <= 9)
    df_union = left.union_all(right)

    query_off, query_on = _get_query_cte_off_and_on(session, df_union)
    assert_cte_sql_shape(query_off, query_on, expect_cte=True)

    with mock.patch.object(session, "_cte_optimization_enabled", True):
        result = df_union.collect()
    left_uuids = {row["U"] for row in result if row["A"] > 1}
    right_uuids = {row["U"] for row in result if row["A"] <= 9}
    shared = left_uuids & right_uuids
    assert len(shared) > 0, "Same DF ref with CTE should reuse uuids across branches"


def test_imbalanced_tree_non_simple_base_cte_connect_mode(session, test_table_name):
    """Imbalanced tree where the shared base is NOT a simple select entity
    (it has a filter), so it is eligible for CTE dedup. The two leaf branches
    are different objects (different select(uuid) calls) so they should NOT be
    CTE-merged, but the shared filtered base should be.

    Tree:        union_all
                /         \\
             filter       select(uuid)   ← df2: distinct object, no CTE
               |              |
           select(uuid)     base         ← same object in both branches;
               |                           has filter → not simple select
             base                          → eligible for CTE

    base is the same Python object shared by df1 and df2. Since base
    is not a simple select entity (it has a filter), it qualifies for
    CTE dedup even in connect mode.
    """
    base = (
        session.table(test_table_name)
        .select(col("a").alias("a"), col("b").alias("b"))
        .filter(col("a") > 0)
    )
    df1 = base.select(col("a").alias("a"), uuid_string().alias("u"))
    df2 = base.select(col("a").alias("a"), uuid_string().alias("u"))
    df_union = df1.filter(col("a") > 3).union_all(df2)

    query_off, query_on = _get_query_cte_off_and_on(session, df_union)
    assert_cte_sql_shape(query_off, query_on, expect_cte=True)

    result = df_union.collect()
    uuids = {row["U"] for row in result}
    assert len(uuids) == len(
        result
    ), "All uuids should be unique — the two select(uuid) are distinct objects"


def test_imbalanced_join_shared_base_cte_connect_mode(session, test_table_name):
    """Imbalanced join tree where the shared base (same object, not simple
    select) appears in both branches. CTE should be applied for the shared
    base even though the outer branches are different objects.

    Tree:          join
                  /    \\
             filter    select(uuid)   ← df2: distinct object, no CTE
               |           |
          select(uuid)   base         ← same object (has filter,
               |                        not simple select → CTE)
             base

    base is the same Python object in both branches, with a filter that
    prevents is_simple_select_entity from excluding it. The two
    select(uuid) nodes are different objects so they won't be merged.
    """
    base = (
        session.table(test_table_name)
        .select(col("a").alias("a"), col("b").alias("b"))
        .filter(col("a") > 0)
    )
    df1 = base.select(col("a").alias("a_l"), uuid_string().alias("u_l"))
    df2 = base.select(col("a").alias("a_r"), uuid_string().alias("u_r"))
    df_joined = df1.filter(col("a_l") > 3).join(df2, df1["a_l"] == df2["a_r"])

    query_off, query_on = _get_query_cte_off_and_on(session, df_joined)
    assert_cte_sql_shape(query_off, query_on, expect_cte=True)

    result = df_joined.collect()
    assert len(result) > 0
    lhs_uuids = {row["U_L"] for row in result}
    rhs_uuids = {row["U_R"] for row in result}
    assert (
        lhs_uuids != rhs_uuids
    ), "Joined distinct DFs should have different uuid columns even with shared CTE base"
