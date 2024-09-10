#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import logging
from unittest.mock import patch

import pytest

from snowflake.snowpark._internal.analyzer import analyzer
from snowflake.snowpark._internal.compiler import large_query_breakdown
from snowflake.snowpark.functions import col, lit, sum_distinct, when_matched
from snowflake.snowpark.row import Row
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
    large_query_breakdown_enabled = session.large_query_breakdown_enabled
    cte_optimization_enabled = session._cte_optimization_enabled
    is_query_compilation_stage_enabled = session._query_compilation_stage_enabled
    session._query_compilation_stage_enabled = True
    session._large_query_breakdown_enabled = True
    yield
    session._query_compilation_stage_enabled = is_query_compilation_stage_enabled
    session._cte_optimization_enabled = cte_optimization_enabled
    session._large_query_breakdown_enabled = large_query_breakdown_enabled
    reset_bounds()


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


def test_no_valid_nodes_found(session, large_query_df, caplog):
    """Test large query breakdown works with default bounds"""
    set_bounds(300, 600)

    base_df = session.sql("select 1 as A, 2 as B")
    df1 = base_df.with_column("A", col("A") + lit(1))
    df2 = base_df.with_column("B", col("B") + lit(1))

    for i in range(102):
        df1 = df1.with_column("A", col("A") + lit(i))
        df2 = df2.with_column("B", col("B") + lit(i))

    union_df = df1.union_all(df2)
    final_df = union_df.with_column("A", col("A") + lit(1))

    with caplog.at_level(logging.DEBUG):
        queries = final_df.queries
    assert len(queries["queries"]) == 1, queries["queries"]
    assert len(queries["post_actions"]) == 0, queries["post_actions"]
    assert "Could not find a valid node for partitioning" in caplog.text


def test_large_query_breakdown_with_cte_optimization(session):
    """Test large query breakdown works with cte optimized plan"""
    set_bounds(300, 600)
    session._cte_optimization_enabled = True
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

    assert len(df4.queries["queries"]) == 2
    assert df4.queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert df4.queries["queries"][1].startswith("WITH SNOWPARK_TEMP_CTE_")

    assert len(df4.queries["post_actions"]) == 1
    assert df4.queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")


def test_save_as_table(session, large_query_df):
    set_bounds(300, 600)
    table_name = Utils.random_table_name()
    with session.query_history() as history:
        large_query_df.write.save_as_table(table_name, mode="overwrite")

    assert len(history.queries) == 4
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert history.queries[1].sql_text.startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert history.queries[2].sql_text.startswith(
        f"CREATE  OR  REPLACE    TABLE  {table_name}"
    )
    assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")


def test_update_delete_merge(session, large_query_df):
    set_bounds(300, 600)
    session._large_query_breakdown_enabled = True
    table_name = Utils.random_table_name()
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
    df.write.save_as_table(table_name, mode="overwrite", table_type="temp")
    t = session.table(table_name)

    # update
    with session.query_history() as history:
        t.update({"B": 0}, t.a == large_query_df.a, large_query_df)
    assert len(history.queries) == 4
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert history.queries[1].sql_text.startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert history.queries[2].sql_text.startswith(f"UPDATE {table_name}")
    assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")

    # delete
    with session.query_history() as history:
        t.delete(t.a == large_query_df.a, large_query_df)
    assert len(history.queries) == 4
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert history.queries[1].sql_text.startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert history.queries[2].sql_text.startswith(f"DELETE  FROM {table_name} USING")
    assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")

    # merge
    with session.query_history() as history:
        t.merge(
            large_query_df,
            t.a == large_query_df.a,
            [when_matched().update({"b": large_query_df.b})],
        )
    assert len(history.queries) == 4
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert history.queries[1].sql_text.startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert history.queries[2].sql_text.startswith(f"MERGE  INTO {table_name} USING")
    assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")


def test_copy_into_location(session, large_query_df):
    set_bounds(300, 600)
    remote_file_path = f"{session.get_session_stage()}/df.parquet"
    with session.query_history() as history:
        large_query_df.write.copy_into_location(
            remote_file_path,
            file_format_type="parquet",
            header=True,
            overwrite=True,
            single=True,
        )
    assert len(history.queries) == 4, history.queries
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert history.queries[1].sql_text.startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert history.queries[2].sql_text.startswith(f"COPY  INTO '{remote_file_path}'")
    assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")


def test_pivot_unpivot(session):
    set_bounds(300, 600)
    session.sql(
        """create or replace temp table monthly_sales(A int, B int, month text)
                as select * from values
                (1, 10000, 'JAN'),
                (1, 400, 'JAN'),
                (2, 4500, 'JAN'),
                (2, 35000, 'JAN'),
                (1, 5000, 'FEB'),
                (1, 3000, 'FEB'),
                (2, 200, 'FEB')"""
    ).collect()
    df_pivot = session.table("monthly_sales").with_column("A", col("A") + lit(1))
    df_unpivot = session.create_dataframe(
        [(1, "electronics", 100, 200), (2, "clothes", 100, 300)],
        schema=["A", "dept", "jan", "feb"],
    )

    for i in range(100):
        df_pivot = df_pivot.with_column("A", col("A") + lit(i))
        df_unpivot = df_unpivot.with_column("A", col("A") + lit(i))

    df_pivot = df_pivot.pivot("month", ["JAN", "FEB"]).sum("B")
    df_unpivot = df_unpivot.unpivot("sales", "month", ["jan", "feb"])

    join_df = df_pivot.join(df_unpivot, "A")
    final_df = join_df.with_column("A", col("A") + lit(1))

    check_result_with_and_without_breakdown(session, final_df)

    plan_queries = final_df.queries
    assert len(plan_queries["queries"]) == 2
    assert plan_queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")

    assert len(plan_queries["post_actions"]) == 1
    assert plan_queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")


def test_sort(session):
    set_bounds(300, 600)
    base_df = session.sql("select 1 as A, 2 as B")
    df1 = base_df.with_column("A", col("A") + lit(1))
    df2 = base_df.with_column("B", col("B") + lit(1))

    for i in range(100):
        df1 = df1.with_column("A", col("A") + lit(i))
        df2 = df2.with_column("B", col("B") + lit(i))
    df1_with_sort = df1.order_by("A")

    union_df = df1_with_sort.union_all(df2)
    final_df = union_df.with_column("A", col("A") + lit(1))
    check_result_with_and_without_breakdown(session, final_df)

    plan_queries = final_df.queries
    assert len(plan_queries["queries"]) == 2
    assert plan_queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")

    assert len(plan_queries["post_actions"]) == 1
    assert plan_queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")

    # when sort is applied on the final dataframe, it should not be broken down
    union_df = df1.union_all(df2)
    final_df = union_df.with_column("A", col("A") + lit(1)).order_by("A")

    check_result_with_and_without_breakdown(session, final_df)

    plan_queries = final_df.queries
    assert len(plan_queries["queries"]) == 1
    assert len(plan_queries["post_actions"]) == 0


def test_multiple_query_plan(session, large_query_df):
    set_bounds(300, 600)
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        base_df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])

        df1 = base_df.with_column("A", col("A") + lit(1))
        df2 = base_df.with_column("B", col("B") + lit(1))

        for i in range(100):
            df1 = df1.with_column("A", col("A") + lit(i))
            df2 = df2.with_column("B", col("B") + lit(i))
        df1 = df1.group_by(col("A")).agg(sum_distinct(col("B")).alias("B"))
        df2 = df2.group_by(col("B")).agg(sum_distinct(col("A")).alias("A"))

        union_df = df1.union_all(df2)
        final_df = union_df.with_column("A", col("A") + lit(1))

        check_result_with_and_without_breakdown(session, final_df)

        plan_queries = final_df.queries
        assert len(plan_queries["queries"]) == 4
        assert plan_queries["queries"][0].startswith(
            "CREATE  OR  REPLACE  SCOPED TEMPORARY  TABLE"
        )
        assert plan_queries["queries"][1].startswith("INSERT  INTO")
        assert plan_queries["queries"][2].startswith("CREATE  SCOPED TEMPORARY  TABLE")

        assert len(plan_queries["post_actions"]) == 2
        for query in plan_queries["post_actions"]:
            assert query.startswith("DROP  TABLE  If  EXISTS")

    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold


def test_optimization_skipped_with_transaction(session, large_query_df, caplog):
    """Test large query breakdown is skipped when transaction is enabled"""
    set_bounds(300, 600)
    session.sql("begin").collect()
    assert Utils.is_active_transaction(session)
    with caplog.at_level(logging.DEBUG):
        with session.query_history() as history:
            large_query_df.collect()
    assert len(history.queries) == 2, history.queries
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert "Skipping large query breakdown" in caplog.text
    assert Utils.is_active_transaction(session)
    assert session.sql("commit").collect()
    assert not Utils.is_active_transaction(session)


def test_optimization_skipped_with_views_and_dynamic_tables(session, caplog):
    """Test large query breakdown is skipped plan is a view or dynamic table"""
    set_bounds(300, 600)
    source_table = Utils.random_table_name()
    table_name = Utils.random_table_name()
    view_name = Utils.random_view_name()
    try:
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


def test_async_job_with_large_query_breakdown(session, large_query_df):
    """Test large query breakdown gives same result for async and non-async jobs"""
    set_bounds(300, 600)
    job = large_query_df.collect(block=False)
    result = job.result()
    assert result == large_query_df.collect()
    assert len(large_query_df.queries["queries"]) == 2
    assert large_query_df.queries["queries"][0].startswith(
        "CREATE  SCOPED TEMPORARY  TABLE"
    )

    assert len(large_query_df.queries["post_actions"]) == 1
    assert large_query_df.queries["post_actions"][0].startswith(
        "DROP  TABLE  If  EXISTS"
    )


def test_add_parent_plan_uuid_to_statement_params(session, large_query_df):
    set_bounds(300, 600)

    with patch.object(
        session._conn, "run_query", wraps=session._conn.run_query
    ) as patched_run_query:
        result = large_query_df.collect()
        Utils.check_answer(result, [Row(1, 4954), Row(2, 4953)])

        plan = large_query_df._plan
        # 1 for current transaction, 1 for partition, 1 for main query, 1 for post action
        assert patched_run_query.call_count == 4

        for i, call in enumerate(patched_run_query.call_args_list):
            if i == 0:
                assert call.args[0] == "SELECT CURRENT_TRANSACTION()"
            else:
                assert "_statement_params" in call.kwargs
                assert call.kwargs["_statement_params"]["_PLAN_UUID"] == plan.uuid


def test_complexity_bounds_affect_num_partitions(session, large_query_df):
    """Test complexity bounds affect number of partitions.
    Also test that when partitions are added, drop table queries are added.
    """
    set_bounds(300, 600)
    assert len(large_query_df.queries["queries"]) == 2
    assert len(large_query_df.queries["post_actions"]) == 1
    assert large_query_df.queries["queries"][0].startswith(
        "CREATE  SCOPED TEMPORARY  TABLE"
    )
    assert large_query_df.queries["post_actions"][0].startswith(
        "DROP  TABLE  If  EXISTS"
    )

    set_bounds(300, 412)
    assert len(large_query_df.queries["queries"]) == 3
    assert len(large_query_df.queries["post_actions"]) == 2
    assert large_query_df.queries["queries"][0].startswith(
        "CREATE  SCOPED TEMPORARY  TABLE"
    )
    assert large_query_df.queries["queries"][1].startswith(
        "CREATE  SCOPED TEMPORARY  TABLE"
    )
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


def test_large_query_breakdown_enabled_parameter(session, caplog):
    with caplog.at_level(logging.WARNING):
        session.large_query_breakdown_enabled = True
    assert "large_query_breakdown_enabled is experimental" in caplog.text
