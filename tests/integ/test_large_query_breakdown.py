#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


import logging
import os
import tempfile
from unittest.mock import patch

import pytest

from snowflake.snowpark._internal.analyzer import analyzer
from snowflake.snowpark.functions import col, lit, sum_distinct, when_matched
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import (
    DEFAULT_COMPLEXITY_SCORE_LOWER_BOUND,
    DEFAULT_COMPLEXITY_SCORE_UPPER_BOUND,
    Session,
)
from tests.integ.test_deepcopy import (
    create_df_with_deep_nested_with_column_dependencies,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import IS_IN_STORED_PROC, Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="Large Query Breakdown runs in non-local testing mode",
        run=False,
    )
]


@pytest.fixture(autouse=True)
def large_query_df(session):
    base_df = session.sql("select 1 as A, 2 as B")
    df1 = base_df.with_column("A", col("A") + lit(1))
    df2 = base_df.with_column("B", col("B") + lit(1))

    for i in range(110):
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
    session._cte_optimization_enabled = False
    set_bounds(session, 300, 600)
    yield
    session._query_compilation_stage_enabled = is_query_compilation_stage_enabled
    session._cte_optimization_enabled = cte_optimization_enabled
    session._large_query_breakdown_enabled = large_query_breakdown_enabled
    reset_bounds(session)


def set_bounds(session: Session, lower_bound: int, upper_bound: int):
    session._large_query_breakdown_complexity_bounds = (lower_bound, upper_bound)


def reset_bounds(session: Session):
    set_bounds(
        session,
        DEFAULT_COMPLEXITY_SCORE_LOWER_BOUND,
        DEFAULT_COMPLEXITY_SCORE_UPPER_BOUND,
    )


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


def check_summary_breakdown_value(patch_send, expected_summary):
    _, kwargs = patch_send.call_args
    summary_value = kwargs["compilation_stage_summary"]
    assert summary_value["breakdown_summary"] == expected_summary


def check_optimization_skipped_reason(patch_send, expected_reason):
    summary_value = patch_send.call_args[1]["compilation_stage_summary"]
    assert (
        summary_value["query_breakdown_optimization_skipped_reason"] == expected_reason
    )


def test_no_pipeline_breaker_nodes(session):
    """Test large query breakdown breaks select statement when no pipeline breaker nodes found"""
    if not session.sql_simplifier_enabled:
        pytest.skip(
            "without sql simplifier, the plan is too large and hits max recursion depth"
        )
    base_df = session.sql("select 1 as A, 2 as B")
    df1 = base_df.with_column("A", col("A") + lit(1))
    df2 = base_df.with_column("B", col("B") + lit(1))

    for i in range(160):
        df1 = df1.with_column("A", col("A") + lit(i))
        df2 = df2.with_column("B", col("B") + lit(i))

    union_df = df1.union_all(df2)
    final_df = union_df.with_column("A", col("A") + lit(1))

    with patch.object(
        session._conn._telemetry_client, "send_query_compilation_summary_telemetry"
    ) as patch_send:
        # there is one query count in large query breakdown to check there
        # is active transaction
        with SqlCounter(query_count=1, describe_count=0):
            queries = final_df.queries

    assert len(queries["queries"]) == 2
    assert queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")

    assert len(queries["post_actions"]) == 1
    assert queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")

    patch_send.assert_called_once()
    expected_summary = [
        {
            "num_partitions_made": 1,
            "num_pipeline_breaker_used": 0,
            "num_relaxed_breaker_used": 1,
        }
    ]
    check_summary_breakdown_value(patch_send, expected_summary)


def test_large_query_breakdown_external_cte_ref(session):
    session._cte_optimization_enabled = True
    sql_simplifier_enabled = session.sql_simplifier_enabled
    if not sql_simplifier_enabled:
        set_bounds(session, 50, 90)

    base_select = session.sql("select 1 as A, 2 as B")
    df1 = base_select.with_column("A", col("A") + lit(1))
    df2 = base_select.with_column("B", col("B") + lit(1))
    base_df = df1.union_all(df2)

    df1 = base_df.with_column("A", col("A") + 1)
    df2 = base_df.with_column("B", col("B") + 1)
    for i in range(6):
        df1 = df1.with_column("A", col("A") + i + col("A"))
        df2 = df2.with_column("B", col("B") + i + col("B"))

    df1 = df1.group_by("A").agg(sum_distinct(col("B")).alias("B"))
    df2 = df2.group_by("B").agg(sum_distinct(col("A")).alias("A"))
    final_df = df1.union_all(df2)

    with SqlCounter(query_count=3, describe_count=0):
        check_result_with_and_without_breakdown(session, final_df)

    with patch.object(
        session._conn._telemetry_client, "send_query_compilation_summary_telemetry"
    ) as patch_send:
        queries = final_df.queries

    # assert that we did not break the plan
    assert len(queries["queries"]) == 1

    patch_send.assert_called_once()
    expected_summary = [
        {
            "failed_partition_summary": {
                "num_external_cte_ref_nodes": 6 if sql_simplifier_enabled else 2,
                "num_non_pipeline_breaker_nodes": 0 if sql_simplifier_enabled else 2,
                "num_nodes_below_lower_bound": 28,
                "num_nodes_above_upper_bound": 1 if sql_simplifier_enabled else 0,
                "num_valid_nodes": 0,
                "num_valid_nodes_relaxed": 0,
            },
            "num_partitions_made": 0,
            "num_pipeline_breaker_used": 0,
            "num_relaxed_breaker_used": 0,
        }
    ]
    check_summary_breakdown_value(patch_send, expected_summary)


def test_breakdown_at_with_query_node(session):
    session._cte_optimization_enabled = True
    if not session.sql_simplifier_enabled:
        set_bounds(session, 40, 80)

    df0 = session.sql("select 1 as A, 2 as B")
    for i in range(7):
        df0 = df0.with_column("A", col("A") + i + col("A"))

    union_df = df0.union_all(df0)
    final_df = union_df.with_column("A", col("A") + 1)
    for i in range(5):
        final_df = final_df.with_column("A", col("A") + i + col("A"))

    with SqlCounter(query_count=5, describe_count=0):
        check_result_with_and_without_breakdown(session, final_df)

    queries = final_df.queries
    assert len(queries["queries"]) == 2
    assert queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert "WITH SNOWPARK_TEMP_CTE_" not in queries["queries"][0]
    assert len(queries["post_actions"]) == 1


def test_large_query_breakdown_with_cte_optimization(session):
    """Test large query breakdown works with cte optimized plan"""
    session._cte_optimization_enabled = True

    if not session.sql_simplifier_enabled:
        # the complexity bounds are updated since nested selected calculation is not supported
        # when sql simplifier disabled
        set_bounds(session, 50, 80)
    df0 = session.sql("select 2 as b, 32 as c")
    df1 = session.sql("select 1 as a, 2 as b").filter(col("a") == 1)
    df1 = df1.join(df0, on=["b"], how="inner")

    df2 = df1.filter(col("b") == 2).union_all(df1)
    df3 = session.sql("select 3 as b, 4 as c").with_column("a", col("b") + 1)
    for i in range(7):
        df2 = df2.with_column("a", col("a") + i + col("a"))
        df3 = df3.with_column("b", col("b") + i + col("b"))

    df2 = df2.select("b", "a")
    df3 = df3.group_by("b").agg(sum_distinct(col("a")).alias("a"))

    df4 = df2.union_all(df3).filter(col("a") > 2).with_column("a", col("a") + 1)
    # without large query breakdown, there is only 1 query
    # with large query breakdown, there are 4 queries:
    #   1 SELECT CURRENT_TRANSACTION()
    #   1 CREATE TEMP TABLE QUERY
    #   the actual query
    #   1 drop temp table query
    with SqlCounter(query_count=5, describe_count=0):
        check_result_with_and_without_breakdown(session, df4)

    with patch.object(
        session._conn._telemetry_client, "send_query_compilation_summary_telemetry"
    ) as patch_send:
        queries = df4.queries

    assert len(queries["queries"]) == 2
    assert queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert queries["queries"][1].startswith("WITH SNOWPARK_TEMP_CTE_")

    assert len(queries["post_actions"]) == 1
    assert queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")

    expected_summary = [
        {
            "num_partitions_made": 1,
            "num_pipeline_breaker_used": 1,
            "num_relaxed_breaker_used": 0,
        }
    ]
    check_summary_breakdown_value(patch_send, expected_summary)
    patch_send.assert_called_once()


def test_save_as_table(session, large_query_df):
    table_name = Utils.random_table_name()
    with session.query_history() as history:
        # one describe call coming from the save_as_table resolve at frontend dataframe layer
        with SqlCounter(query_count=4, describe_count=1):
            large_query_df.write.save_as_table(table_name, mode="overwrite")

    assert len(history.queries) == 4
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert history.queries[1].sql_text.startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert history.queries[2].sql_text.startswith(
        f"CREATE  OR  REPLACE    TABLE  {table_name}"
    )
    assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")


def test_variable_binding(session):
    if not session.sql_simplifier_enabled:
        set_bounds(session, 40, 80)

    df1 = session.sql(
        "select $1 as A, $2 as B from values (?,?), (?,?)", params=[1, "a", 2, "b"]
    )
    df2 = session.sql(
        "select $1 as A, $2 as B from values (?,?), (?,?)", params=[3, "c", 4, "d"]
    )

    for i in range(7):
        df1 = df1.with_column("A", col("A") + i + col("A"))
        df2 = df2.with_column("A", col("A") + i + col("A"))
    df1 = df1.group_by("B").agg(sum_distinct(col("A")).alias("A"))
    df2 = df2.group_by("B").agg(sum_distinct(col("A")).alias("A"))
    final_df = df1.union_all(df2)

    check_result_with_and_without_breakdown(session, final_df)
    with SqlCounter(query_count=1, describe_count=0):
        queries = final_df.queries
    assert len(queries["queries"]) == 2
    assert len(queries["post_actions"]) == 1


def test_update_delete_merge(session, large_query_df):
    if not session.sql_simplifier_enabled:
        pytest.skip(
            "without sql simplifier, the plan is too large and hits max recursion depth"
        )
    session._large_query_breakdown_enabled = True
    table_name = Utils.random_table_name()
    # There is one SELECT CURRENT_TRANSACTION() query and one save_as_table query since large
    # query breakdown is not triggered.
    # There are two describe queries triggered, one from save_as_table, one from session.table
    expected_describe_count = 1 if session.reduce_describe_query_enabled else 2
    with SqlCounter(query_count=2, describe_count=expected_describe_count):
        df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
        df.write.save_as_table(table_name, mode="overwrite", table_type="temp")
        t = session.table(table_name)

    # update
    with session.query_history() as history:
        # 3 describe call triggered due to column state extraction
        with SqlCounter(query_count=4, describe_count=2):
            t.update({"B": 0}, t.a == large_query_df.a, large_query_df)
    assert len(history.queries) == 4
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert history.queries[1].sql_text.startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert history.queries[2].sql_text.startswith(f"UPDATE {table_name}")
    assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")

    # delete
    with session.query_history() as history:
        with SqlCounter(query_count=4, describe_count=0):
            t.delete(t.a == large_query_df.a, large_query_df)
    assert len(history.queries) == 4
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert history.queries[1].sql_text.startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert history.queries[2].sql_text.startswith(f"DELETE  FROM {table_name} USING")
    assert history.queries[3].sql_text.startswith("DROP  TABLE  If  EXISTS")

    # merge
    with session.query_history() as history:
        with SqlCounter(query_count=4, describe_count=0):
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
    remote_file_path = f"{session.get_session_stage()}/df.parquet"
    with session.query_history() as history:
        with SqlCounter(query_count=4, describe_count=0):
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


def test_in_with_subquery_multiple_query(session):
    if not session.sql_simplifier_enabled:
        set_bounds(session, 40, 80)

    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df0 = session.create_dataframe([[1], [2], [3], [4]], schema=["A"])
        df1 = session.create_dataframe([[1, 2, 33], [4, 5, 66]], schema=["A", "B", "C"])
        df_filter = df0.filter(df0.a < 3)
        df_in = df1.filter(~df1.a.in_(df_filter))
        df2 = session.create_dataframe(
            [[11, 12, 13], [21, 22, 23], [31, 32, 33]], schema=["A", "B", "C"]
        )

        for i in range(7):
            df_in = df_in.with_column("A", col("A") + i + col("A"))
            df2 = df2.with_column("A", col("A") + i + col("A"))

        df_in = df_in.group_by("A").agg(
            sum_distinct(col("B")).alias("B"), sum_distinct(col("C")).alias("C")
        )

        final_df = df_in.union_all(df2)
        check_result_with_and_without_breakdown(session, final_df)

        with SqlCounter(query_count=1, describe_count=0):
            queries = final_df.queries
        assert len(queries["queries"]) == 8
        assert len(queries["post_actions"]) == 4

    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold


def test_pivot_unpivot(session):
    if not session.sql_simplifier_enabled:
        # the complexity bounds are updated since nested selected calculation is not supported
        # when sql simplifier disabled
        set_bounds(session, 40, 60)
    table_name = Utils.random_table_name()
    session.create_dataframe(
        [
            (1, 10000, "JAN"),
            (1, 400, "JAN"),
            (2, 4500, "JAN"),
            (2, 35000, "JAN"),
            (1, 5000, "FEB"),
            (1, 3000, "FEB"),
            (2, 200, "FEB"),
        ],
        schema=["A", "B", "month"],
    ).write.save_as_table(table_name, table_type="temp")
    df_pivot = session.table(table_name).with_column("A", col("A") + lit(1))
    df_unpivot = session.create_dataframe(
        [(1, "electronics", 100, 200), (2, "clothes", 100, 300)],
        schema=["A", "dept", "jan", "feb"],
    )

    for i in range(6):
        df_pivot = df_pivot.with_column("A", col("A") + lit(i) + col("A"))
        df_unpivot = df_unpivot.with_column("A", col("A") + lit(i) + col("A"))

    df_pivot = df_pivot.pivot("month", ["JAN", "FEB"]).sum("B")
    df_unpivot = df_unpivot.unpivot("sales", "month", ["jan", "feb"])

    join_df = df_pivot.join(df_unpivot, "A")
    final_df = join_df.with_column("A", col("A") + lit(1))

    with SqlCounter(query_count=5, describe_count=0):
        check_result_with_and_without_breakdown(session, final_df)

    plan_queries = final_df.queries
    assert len(plan_queries["queries"]) == 2
    assert plan_queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")

    assert len(plan_queries["post_actions"]) == 1
    assert plan_queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")


def test_sort(session):
    if not session.sql_simplifier_enabled:
        pytest.skip(
            "without sql simplifier, the plan is too large and hits max recursion depth"
        )
    base_df = session.sql("select 1 as A, 2 as B")
    df1 = base_df.with_column("A", col("A") + lit(1))
    df2 = base_df.with_column("B", col("B") + lit(1))

    for i in range(160):
        df1 = df1.with_column("A", col("A") + lit(i))
        df2 = df2.with_column("B", col("B") + lit(i))

    # when sort is applied on the final dataframe, the final result should be the same as
    # when no optimization is applied.
    # A cut will be made at SelectStatement
    union_df = df1.union_all(df2)
    final_df = union_df.with_column("A", col("A") + lit(1)).order_by("A")

    with SqlCounter(query_count=5, describe_count=0):
        check_result_with_and_without_breakdown(session, final_df)

    with SqlCounter(query_count=1, describe_count=0):
        plan_queries = final_df.queries
    assert len(plan_queries["queries"]) == 2
    assert plan_queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")

    assert len(plan_queries["post_actions"]) == 1
    assert plan_queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")


def test_multiple_query_plan(session):
    if not session.sql_simplifier_enabled:
        pytest.skip(
            "without sql simplifier, the plan is too large and hits max recursion depth"
        )
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        base_df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])

        df1 = base_df.with_column("A", col("A") + lit(1))
        df2 = base_df.with_column("B", col("B") + lit(1))

        for i in range(160):
            df1 = df1.with_column("A", col("A") + lit(i))
            df2 = df2.with_column("B", col("B") + lit(i))
        df1 = df1.group_by(col("A")).agg(sum_distinct(col("B")).alias("B"))
        df2 = df2.group_by(col("B")).agg(sum_distinct(col("A")).alias("A"))

        union_df = df1.union_all(df2)
        final_df = union_df.with_column("A", col("A") + lit(1))

        with SqlCounter(
            query_count=11,
            describe_count=0,
            high_count_expected=True,
            high_count_reason="low array bind threshold",
        ):
            check_result_with_and_without_breakdown(session, final_df)

        with SqlCounter(query_count=1, describe_count=0):
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
    session.sql("begin").collect()
    assert Utils.is_active_transaction(session)
    with caplog.at_level(logging.DEBUG):
        with session.query_history() as history:
            with SqlCounter(query_count=2, describe_count=0):
                with patch.object(
                    session._conn._telemetry_client,
                    "send_query_compilation_summary_telemetry",
                ) as patch_send:
                    large_query_df.collect()

    check_optimization_skipped_reason(patch_send, {"active transaction": 1})

    assert len(history.queries) == 2, history.queries
    assert history.queries[0].sql_text == "SELECT CURRENT_TRANSACTION()"
    assert "Skipping large query breakdown" in caplog.text
    assert Utils.is_active_transaction(session)
    assert session.sql("commit").collect()
    assert not Utils.is_active_transaction(session)


def test_optimization_skipped_with_views_and_dynamic_tables(session, caplog):
    """Test large query breakdown is skipped plan is a view or dynamic table"""
    source_table = Utils.random_table_name()
    table_name = Utils.random_table_name()
    view_name = Utils.random_view_name()
    try:
        session.sql("select 1 as a, 2 as b").write.save_as_table(source_table)
        df = session.table(source_table)
        with caplog.at_level(logging.DEBUG):
            with patch.object(
                session._conn._telemetry_client,
                "send_query_compilation_summary_telemetry",
            ) as patch_send:
                df.create_or_replace_dynamic_table(
                    table_name,
                    warehouse=session.get_current_warehouse(),
                    lag="20 minutes",
                )
        assert (
            "Skipping large query breakdown optimization for view/dynamic table plan"
            in caplog.text
        )
        check_optimization_skipped_reason(
            patch_send, {"view or dynamic table command": 1}
        )

        with caplog.at_level(logging.DEBUG):
            with patch.object(
                session._conn._telemetry_client,
                "send_query_compilation_summary_telemetry",
            ) as patch_send:
                df.create_or_replace_view(view_name)
        assert (
            "Skipping large query breakdown optimization for view/dynamic table plan"
            in caplog.text
        )
        patch_send.assert_called_once()
        check_optimization_skipped_reason(
            patch_send, {"view or dynamic table command": 1}
        )
    finally:
        Utils.drop_dynamic_table(session, table_name)
        Utils.drop_view(session, view_name)
        Utils.drop_table(session, source_table)


def test_large_query_breakdown_with_nested_select(session):
    if not session.sql_simplifier_enabled:
        pytest.skip(
            "the nested select optimization is only enabled with sql simplifier"
        )

    temp_table_name = Utils.random_table_name()
    final_df = create_df_with_deep_nested_with_column_dependencies(
        session, temp_table_name, 8
    )

    with SqlCounter(query_count=1, describe_count=0):
        queries = final_df.queries
    assert len(queries["queries"]) == 3
    assert queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")
    assert queries["queries"][1].startswith("CREATE  SCOPED TEMPORARY  TABLE")

    assert len(queries["post_actions"]) == 2
    assert queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")
    assert queries["post_actions"][1].startswith("DROP  TABLE  If  EXISTS")

    with SqlCounter(query_count=7, describe_count=0):
        check_result_with_and_without_breakdown(session, final_df)


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="cannot create a new session in stored procedure"
)
@pytest.mark.parametrize("db_or_schema", ["database", "schema"])
def test_optimization_skipped_with_no_active_db_or_schema(
    session, db_or_schema, caplog
):
    df = session.sql("select 1 as a, 2 as b").select("a", "b")

    # no database check
    with patch.object(session, f"get_current_{db_or_schema}", return_value=None):
        with patch.object(
            session._conn._telemetry_client,
            "send_query_compilation_summary_telemetry",
        ) as patch_send:
            with caplog.at_level(logging.DEBUG):
                with SqlCounter(query_count=0, describe_count=0):
                    df.queries
    assert (
        f"Skipping large query breakdown optimization since there is no active {db_or_schema}"
        in caplog.text
    )
    patch_send.assert_called_once()
    check_optimization_skipped_reason(patch_send, {f"no active {db_or_schema}": 1})


def test_async_job_with_large_query_breakdown(large_query_df):
    """Test large query breakdown gives same result for async and non-async jobs"""
    with SqlCounter(query_count=3):
        # 1 for current transaction
        # 1 for created temp table; main query submitted as multi-statement query
        # 1 for post action
        job = large_query_df.collect(block=False)
        result = job.result()
    with SqlCounter(query_count=4):
        # 1 for current transaction
        # 1 for created temp table
        # 1 for main query
        # 1 for post action
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
    with patch.object(
        session._conn, "run_query", wraps=session._conn.run_query
    ) as patched_run_query:
        result = large_query_df.collect()
        Utils.check_answer(result, [Row(1, 5999), Row(2, 5998)])

        plan = large_query_df._plan
        # 1 for current transaction, 1 for partition, 1 for main query, 1 for post action
        assert patched_run_query.call_count == 4

        for i, call in enumerate(patched_run_query.call_args_list):
            if i == 0:
                assert call.args[0] == "SELECT CURRENT_TRANSACTION()"
            else:
                assert "_statement_params" in call.kwargs
                assert call.kwargs["_statement_params"]["_PLAN_UUID"] == plan.uuid


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-609328: support caplog in SP regression test"
)
@pytest.mark.parametrize("error_type", [AssertionError, ValueError, RuntimeError])
@patch("snowflake.snowpark._internal.compiler.plan_compiler.LargeQueryBreakdown.apply")
def test_optimization_skipped_with_exceptions(
    mock_lqb_apply, session, large_query_df, caplog, error_type
):
    """Test large query breakdown is skipped when there are exceptions"""
    caplog.clear()
    mock_lqb_apply.side_effect = error_type("test exception")
    with caplog.at_level(logging.DEBUG):
        with patch.object(
            session._conn._telemetry_client,
            "send_query_compilation_stage_failed_telemetry",
        ) as patch_send:
            queries = large_query_df.queries

    assert "Skipping optimization due to error:" in caplog.text
    assert len(queries["queries"]) == 1
    assert len(queries["post_actions"]) == 0

    patch_send.assert_called_once()
    _, kwargs = patch_send.call_args
    print(kwargs)
    assert kwargs["error_message"] == "test exception"
    assert kwargs["error_type"] == error_type.__name__


def test_large_query_breakdown_with_nested_cte(session):
    session.cte_optimization_enabled = True
    if session.sql_simplifier_enabled:
        set_bounds(session, 35, 45)
    else:
        set_bounds(session, 55, 65)

    temp_table = Utils.random_table_name()
    session.create_dataframe([(1, 2), (3, 4)], ["A", "B"]).write.save_as_table(
        temp_table, table_type="temp"
    )
    base_select = session.table(temp_table)
    for i in range(7):
        base_select = base_select.with_column("A", col("A") + lit(i))

    base_df = base_select.union_all(base_select)

    df1 = base_df.with_column("A", col("A") + 1)
    df2 = base_df.with_column("B", col("B") + 1)
    for i in range(2):
        df1 = df1.with_column("A", col("A") + i)

    df1 = df1.group_by("A").agg(sum_distinct(col("B")).alias("B"))
    df2 = df2.group_by("B").agg(sum_distinct(col("A")).alias("A"))
    mid_final_df = df1.union_all(df2)

    mid1 = mid_final_df.filter(col("A") > 10)
    mid2 = mid_final_df.filter(col("B") > 3)
    final_df = mid1.union_all(mid2)

    with SqlCounter(query_count=1, describe_count=0):
        queries = final_df.queries
        assert len(queries["queries"]) == 2
        assert len(queries["post_actions"]) == 1

        # assert that the first query contains the base temp table name
        assert temp_table in queries["queries"][0]

        # assert that query for upper cte node is re-written and does not
        # contain query for the base temp table
        assert temp_table not in queries["queries"][1]

    check_result_with_and_without_breakdown(session, final_df)


def test_complexity_bounds_affect_num_partitions(session, large_query_df):
    """Test complexity bounds affect number of partitions.
    Also test that when partitions are added, drop table queries are added.
    """
    sql_simplifier_enabled = session.sql_simplifier_enabled
    if sql_simplifier_enabled:
        set_bounds(session, 300, 600)
    else:
        set_bounds(session, 400, 600)

    with SqlCounter(query_count=1, describe_count=0):
        queries = large_query_df.queries

        assert len(queries["queries"]) == 2
        assert len(queries["post_actions"]) == 1
        assert queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")
        assert queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")

    if sql_simplifier_enabled:
        set_bounds(session, 300, 455)
    else:
        set_bounds(session, 400, 450)

    with SqlCounter(query_count=1, describe_count=0):
        queries = large_query_df.queries
        assert len(queries["queries"]) == 3
        assert len(queries["post_actions"]) == 2
        assert queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")
        assert queries["queries"][1].startswith("CREATE  SCOPED TEMPORARY  TABLE")
        assert queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")
        assert queries["post_actions"][1].startswith("DROP  TABLE  If  EXISTS")

    set_bounds(session, 0, 300)
    with SqlCounter(query_count=1, describe_count=0):
        queries = large_query_df.queries
        assert len(queries["queries"]) == (4 if sql_simplifier_enabled else 1)
        assert len(queries["post_actions"]) == (3 if sql_simplifier_enabled else 0)

    reset_bounds(session)
    with SqlCounter(query_count=1, describe_count=0):
        queries = large_query_df.queries
        assert len(queries["queries"]) == 1
        assert len(queries["post_actions"]) == 0


def test_to_selectable_memoization(session):
    session.cte_optimization_enabled = True
    sql_simplifier_enabled = session.sql_simplifier_enabled
    if sql_simplifier_enabled:
        set_bounds(session, 300, 520)
    else:
        set_bounds(session, 40, 55)
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).select("a", "b")
    for i in range(7):
        df = df.with_column("a", col("a") + i + col("a"))
    df1 = df.select("a", "b", (col("a") + col("b")).as_("b"))
    df2 = df.select("a", "b", (col("a") + col("b")).as_("c"))
    df3 = df.select("a", "b", (col("a") + col("b")).as_("d"))
    df5 = df1.union_all(df2).union_all(df3)
    with SqlCounter(query_count=1, describe_count=0):
        queries = df5.queries
        assert len(queries["queries"]) == 2
        assert len(queries["post_actions"]) == 1


@sql_count_checker(query_count=0)
def test_large_query_breakdown_enabled_parameter(session, caplog):
    with caplog.at_level(logging.WARNING):
        session.large_query_breakdown_enabled = True
    assert "large_query_breakdown_enabled is experimental" in caplog.text


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="requires graphviz")
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("plotting_score_threshold", [0, 10_000_000])
def test_plotter(large_query_df, enabled, plotting_score_threshold):
    original_plotter_enabled = os.environ.get("ENABLE_SNOWPARK_LOGICAL_PLAN_PLOTTING")
    original_score_threshold = os.environ.get(
        "SNOWPARK_LOGICAL_PLAN_PLOTTING_COMPLEXITY_THRESHOLD"
    )
    try:
        os.environ["ENABLE_SNOWPARK_LOGICAL_PLAN_PLOTTING"] = str(enabled)
        os.environ["SNOWPARK_LOGICAL_PLAN_PLOTTING_COMPLEXITY_THRESHOLD"] = str(
            plotting_score_threshold
        )
        tmp_dir = tempfile.gettempdir()

        with patch("graphviz.Graph.render") as mock_render:
            large_query_df.collect()
            should_plot = enabled and (plotting_score_threshold == 0)
            assert mock_render.called == should_plot
            if not should_plot:
                return

            assert mock_render.call_count == 5
            expected_files = [
                "original_plan",
                "deep_copied_plan",
                "cte_optimized_plan_0",
                "large_query_breakdown_plan_0",
                "large_query_breakdown_plan_1",
            ]
            for i, file in enumerate(expected_files):
                path = os.path.join(tmp_dir, "snowpark_query_plan_plots", file)
                assert mock_render.call_args_list[i][0][0] == path

    finally:
        if original_plotter_enabled is not None:
            os.environ[
                "ENABLE_SNOWPARK_LOGICAL_PLAN_PLOTTING"
            ] = original_plotter_enabled
        else:
            del os.environ["ENABLE_SNOWPARK_LOGICAL_PLAN_PLOTTING"]
        if original_score_threshold is not None:
            os.environ[
                "SNOWPARK_LOGICAL_PLAN_PLOTTING_COMPLEXITY_THRESHOLD"
            ] = original_score_threshold
        else:
            del os.environ["SNOWPARK_LOGICAL_PLAN_PLOTTING_COMPLEXITY_THRESHOLD"]
