#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
TODO:
* test sort break points are correct
"""

import logging

import pytest

from snowflake.snowpark._internal.compiler import large_query_breakdown
from snowflake.snowpark.functions import col, lit, sum_distinct
from tests.utils import Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="Large Query Breakdown runs in non-local testing mode",
        run=False,
    )
]

DEFAULT_LOWER_BOUND = large_query_breakdown.COMPLEXITY_SCORE_LOWER_BOUND
DEFAULT_UPPER_BOUND = large_query_breakdown.COMPLEXITY_SCORE_UPPER_BOUND


@pytest.fixture(autouse=True)
def large_query_df(session):
    base_df = session.sql("select 1 as A, 2 as B")
    df1 = base_df.with_column("A", col("A") + lit(1))
    df2 = base_df.with_column("B", col("B") + lit(1))

    for i in range(100):
        df1 = df1.with_column("A", col("A") + lit(i))
        df2 = df2.with_column("B", col("B") + lit(i))
    df1 = df1.group_by(col("A")).agg(sum_distinct(col("B")).alias("B"))
    df2 = df2.group_by(col("B")).agg(sum_distinct(col("A")).alias("A"))

    union_df = df1.union_all(df2)
    final_df = union_df.with_column("A", col("A") + lit(1))
    yield final_df


@pytest.fixture(autouse=True)
def setup(session):
    is_query_compilation_stage_enabled = session._query_compilation_stage_enabled
    session._query_compilation_stage_enabled = True
    yield
    session._query_compilation_stage_enabled = is_query_compilation_stage_enabled


def set_bounds(lower_bound: int, upper_bound: int):
    large_query_breakdown.COMPLEXITY_SCORE_LOWER_BOUND = lower_bound
    large_query_breakdown.COMPLEXITY_SCORE_UPPER_BOUND = upper_bound


def reset_bounds():
    set_bounds(DEFAULT_LOWER_BOUND, DEFAULT_UPPER_BOUND)


def check_result_with_and_without_breakdown(session, df):
    large_query_enabled = session.large_query_breakdown_enabled
    try:
        session._large_query_breakdown_enabled = True
        enabled_result = df.collect()

        session._large_query_breakdown_enabled = False
        disabled_result = df.collect()

        Utils.check_answer(enabled_result, disabled_result)
    finally:
        session._large_query_breakdown_enabled = large_query_enabled


def test_large_query_breakdown_with_cte_optimization(session):
    """Test large query breakdown works with cte optimized plan"""
    set_bounds(300, 600)
    cte_optimization_enabled = session._cte_optimization_enabled
    large_query_breakdown_enabled = session._large_query_breakdown_enabled
    session._cte_optimization_enabled = True
    try:
        df0 = session.sql("select 2 as b, 32 as c")
        df1 = session.sql("select 1 as a, 2 as b").filter(col("a") == 1)
        df1 = df1.join(df0, on=["b"], how="inner")

        df2 = df1.filter(col("b") == 2).union_all(df1)
        df3 = df1.with_column("a", col("a") + 1)
        for i in range(100):
            df2 = df2.with_column("a", col("a") + i)
            df3 = df3.with_column("b", col("b") + i)

        df2 = df2.group_by("a").agg(sum_distinct(col("b")).alias("b"))
        df3 = df3.group_by("b").agg(sum_distinct(col("a")).alias("a"))

        df4 = df2.union_all(df3).filter(col("a") > 2).with_column("a", col("a") + 1)
        check_result_with_and_without_breakdown(session, df4)

        session._large_query_breakdown_enabled = True
        assert len(df4.queries["queries"]) == 2
        assert len(df4.queries["post_actions"]) == 1
    finally:
        session._cte_optimization_enabled = cte_optimization_enabled
        session._large_query_breakdown_enabled = large_query_breakdown_enabled
        reset_bounds()


def test_optimization_skipped_with_transaction(session, large_query_df, caplog):
    """Test large query breakdown is skipped when transaction is enabled"""
    set_bounds(300, 600)
    large_query_breakdown_enabled = session.large_query_breakdown_enabled
    try:
        session._large_query_breakdown_enabled = True
        session.sql("begin").collect()
        assert Utils.is_active_transaction(session)
        with caplog.at_level(logging.DEBUG):
            large_query_df.collect()
        assert "Skipping large query breakdown" in caplog.text
        assert Utils.is_active_transaction(session)
        assert session.sql("commit").collect()
        assert not Utils.is_active_transaction(session)
    finally:
        session._large_query_breakdown_enabled = large_query_breakdown_enabled
        reset_bounds()


def test_optimization_skipped_with_views_and_dynamic_tables(session, caplog):
    """Test large query breakdown is skipped plan is a view or dynamic table"""
    set_bounds(300, 600)
    large_query_breakdown_enabled = session.large_query_breakdown_enabled
    source_table = Utils.random_table_name()
    table_name = Utils.random_table_name()
    view_name = Utils.random_view_name()
    try:
        session._large_query_breakdown_enabled = True
        session.sql("select 1 as a, 2 as b").write.save_as_table(source_table)
        df = session.table(source_table)
        with caplog.at_level(logging.DEBUG):
            df.create_or_replace_dynamic_table(
                table_name, warehouse=session.get_current_warehouse(), lag="20 minutes"
            )
        assert (
            "Skipping large query breakdown optimization for view/dynamic table plan"
            in caplog.text
        )

        with caplog.at_level(logging.DEBUG):
            df.create_or_replace_view(view_name)
        assert (
            "Skipping large query breakdown optimization for view/dynamic table plan"
            in caplog.text
        )
    finally:
        Utils.drop_dynamic_table(session, table_name)
        Utils.drop_view(session, view_name)
        Utils.drop_table(session, source_table)
        reset_bounds()
        session._large_query_breakdown_enabled = large_query_breakdown_enabled
    pass


def test_async_job_with_large_query_breakdown(session, large_query_df):
    """Test large query breakdown gives same result for async and non-async jobs"""
    set_bounds(300, 600)
    large_query_breakdown_enabled = session.large_query_breakdown_enabled
    try:
        session._large_query_breakdown_enabled = True
        job = large_query_df.collect(block=False)
        result = job.result()
        assert result == large_query_df.collect()
        assert len(large_query_df.queries["queries"]) == 2
        assert len(large_query_df.queries["post_actions"]) == 1
    finally:
        session._large_query_breakdown_enabled = large_query_breakdown_enabled
        reset_bounds()


def test_complexity_bounds_affect_num_partitions(session, large_query_df):
    """Test complexity bounds affect number of partitions.
    Also test that when partitions are added, drop table queries are added.
    """
    large_query_breakdown_enabled = session.large_query_breakdown_enabled
    try:
        set_bounds(300, 600)
        session._large_query_breakdown_enabled = True
        assert len(large_query_df.queries["queries"]) == 2
        assert len(large_query_df.queries["post_actions"]) == 1
        assert large_query_df.queries["queries"][0].startswith("CREATE  TEMP  TABLE")
        assert large_query_df.queries["post_actions"][0].startswith(
            "DROP  TABLE  If  EXISTS"
        )

        set_bounds(300, 412)
        session._large_query_breakdown_enabled = True
        assert len(large_query_df.queries["queries"]) == 3
        assert len(large_query_df.queries["post_actions"]) == 2
        assert large_query_df.queries["queries"][0].startswith("CREATE  TEMP  TABLE")
        assert large_query_df.queries["queries"][1].startswith("CREATE  TEMP  TABLE")
        assert large_query_df.queries["post_actions"][0].startswith(
            "DROP  TABLE  If  EXISTS"
        )
        assert large_query_df.queries["post_actions"][1].startswith(
            "DROP  TABLE  If  EXISTS"
        )

        set_bounds(0, 300)
        assert len(large_query_df.queries["queries"]) == 1
        assert len(large_query_df.queries["post_actions"]) == 0

        reset_bounds()
        assert len(large_query_df.queries["queries"]) == 1
        assert len(large_query_df.queries["post_actions"]) == 0
    finally:
        reset_bounds()
        session._large_query_breakdown_enabled = large_query_breakdown_enabled


def test_large_query_breakdown_enabled_parameter(session, caplog):
    with caplog.at_level(logging.WARNING):
        session.large_query_breakdown_enabled = True
    assert "large_query_breakdown_enabled is experimental" in caplog.text
