#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import tempfile
import os
import logging

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col,
    sum as sum_,
    avg,
    count,
)
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    result = session.sql("SHOW PARAMETERS LIKE 'USE_CACHED_RESULT'").collect()
    original_use_cached_result = result[0]["value"] if result else "FALSE"
    # if we use cached result, some of our tests will be flaky b/c they may not rerun the expected query
    # and use the cached query result instead
    session.sql("ALTER SESSION SET USE_CACHED_RESULT = FALSE").collect()
    yield
    set_ast_state(AstFlagSource.TEST, original)
    if original_use_cached_result and original_use_cached_result.upper() == "TRUE":
        session.sql("ALTER SESSION SET USE_CACHED_RESULT = TRUE").collect()


def validate_execution_profile(df, expected_patterns=None):
    if not hasattr(df, "_ast_id") or df._ast_id is None:
        return False

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt"
    ) as temp_file:
        temp_filename = temp_file.name

    try:
        df.get_execution_profile(output_file=temp_filename)

        if os.path.exists(temp_filename):
            with open(temp_filename) as f:
                content = f.read()

            assert len(content) > 0, "Profile output should not be empty"
            assert "Analyzing Query" in content, "Should contain query analysis header"
            assert (
                "QUERY OPERATOR TREE" in content
            ), "Should contain operator tree section"
            assert (
                "DETAILED OPERATOR STATISTICS" in content
            ), "Should contain detailed stats section"

            if expected_patterns:
                for pattern in expected_patterns:
                    assert (
                        pattern in content
                    ), f"Expected pattern '{pattern}' not found in profile"

            return True
        return False
    finally:
        if os.path.exists(temp_filename):
            os.unlink(temp_filename)


def test_profiler_enable_disable(session):
    profiler = session.dataframe_profiler
    assert profiler._query_history is None
    profiler.enable()
    assert profiler._query_history is not None
    profiler.disable()
    assert profiler._query_history is None


def test_multiple_df(session):
    profiler = session.dataframe_profiler
    profiler.enable()

    try:
        test_data = [(i, f"value_{i}", i * 10) for i in range(1, 11)]
        df = session.create_dataframe(test_data, schema=["id", "name", "amount"])
        result1 = df.collect()
        assert len(result1) == 10
        filtered_df = df.filter(col("id") > 5)
        result2 = filtered_df.collect()
        assert len(result2) == 5
        agg_df = df.group_by(col("id") > 5).agg(
            count("*").alias("count"), sum_("amount").alias("total")
        )
        result3 = agg_df.collect()
        assert len(result3) == 2
        result4 = df.collect()
        assert len(result4) == 10
        assert result4 == result1
        validate_execution_profile(df, ["ValuesClause", "Input Rows", "Output Rows"])
        validate_execution_profile(filtered_df, ["Filter", "Input Rows", "Output Rows"])
        validate_execution_profile(agg_df, ["Aggregate", "Input Rows", "Output Rows"])

    finally:
        profiler.disable()


def test_multiple_collect_same_df(session):
    """Test collect() calls with various transformations applied to the same dataframe."""
    profiler = session.dataframe_profiler
    profiler.enable()

    try:
        df = session.create_dataframe(
            [(1, "A", 100), (2, "B", 200), (3, "A", 150), (4, "B", 250)],
            schema=["id", "category", "value"],
        )
        result1 = df.collect()
        assert len(result1) == 4
        validate_execution_profile(df, ["ValuesClause", "Input Rows", "Output Rows"])
        df = df.select("category", "value")
        result2 = df.collect()
        assert len(result2) == 4
        assert len(result2[0]) == 2
        validate_execution_profile(
            df,
            [
                "('A', 100), ('B', 200), ('A', 150), ('B', 250)",
                "Input Rows",
                "Output Rows",
            ],
        )
        df = df.filter(col("value") > 150)
        result3 = df.collect()
        assert len(result3) == 2
        validate_execution_profile(
            df,
            [
                "Filter",
                "('A', 100), ('B', 200), ('A', 150), ('B', 250)",
                "Input Rows",
                "Output Rows",
            ],
        )
        df = df.sort(col("value").desc())
        result4 = df.collect()
        assert len(result4) == 2
        assert result4[0]["VALUE"] == 250
        validate_execution_profile(
            df,
            [
                "Sort",
                "Filter",
                "('A', 100), ('B', 200), ('A', 150), ('B', 250)",
                "Input Rows",
                "Output Rows",
            ],
        )

    finally:
        profiler.disable()


def test_profiler_disabled_get_execution_profile(session, caplog):
    profiler = session.dataframe_profiler
    profiler.disable()
    df = session.create_dataframe([(1, "test")], schema=["id", "value"])
    df.collect()
    with caplog.at_level(logging.WARNING):
        df.get_execution_profile()
    assert (
        "No query history found. Enable dataframe profiler to get execution profile."
        in caplog.text
    )


def test_query_profiling_joins(session):
    profiler = session.dataframe_profiler
    profiler.enable()

    try:
        customers = session.create_dataframe(
            [
                (1, "Alice", "Premium"),
                (2, "Bob", "Standard"),
                (3, "Charlie", "Premium"),
            ],
            schema=["customer_id", "name", "tier"],
        )

        orders = session.create_dataframe(
            [(101, 1, 500), (102, 2, 300), (103, 1, 700), (104, 3, 400), (105, 2, 200)],
            schema=["order_id", "customer_id", "amount"],
        )

        customers.collect()
        orders.collect()

        joined = customers.join(
            orders, customers["customer_id"] == orders["customer_id"]
        )
        result1 = joined.collect()
        assert len(result1) == 5

        customer_totals = joined.group_by("tier").agg(
            sum_("amount").alias("total_amount"),
            count("*").alias("order_count"),
            avg("amount").alias("avg_amount"),
        )
        customer_totals.collect()
        validate_execution_profile(
            customers, ["ValuesClause", "Input Rows", "Output Rows"]
        )
        validate_execution_profile(
            orders, ["ValuesClause", "Input Rows", "Output Rows"]
        )
        validate_execution_profile(
            joined, ["Join", "equality_join_condition", "Input Rows", "Output Rows"]
        )
        validate_execution_profile(
            customer_totals, ["Aggregate", "Input Rows", "Output Rows"]
        )

    finally:
        profiler.disable()


def test_df_transformations_without_collect(session, caplog):
    profiler = session.dataframe_profiler
    profiler.enable()

    try:
        df = session.create_dataframe(
            [(1, "test"), (2, "data")], schema=["id", "value"]
        )
        transformed_df = df.filter(col("id") > 0).select("value").sort(col("value"))
        result = transformed_df.collect()
        with caplog.at_level(logging.WARNING):
            df.get_execution_profile()
        assert "No queries found for dataframe with ast_id" in caplog.text

        assert len(result) == 2
        orig_result = df.collect()
        assert len(orig_result) == 2
        validate_execution_profile(df, ["ValuesClause", "Input Rows", "Output Rows"])
        validate_execution_profile(
            transformed_df, ["Sort", "Filter", "Input Rows", "Output Rows"]
        )

    finally:
        profiler.disable()


def test_profiler_across_sessions(session, db_parameters):
    profiler1 = session.dataframe_profiler
    profiler1.enable()
    session2 = Session.builder.configs(db_parameters).create()
    session2.sql("ALTER SESSION SET USE_CACHED_RESULT = FALSE").collect()
    profiler2 = session2.dataframe_profiler

    try:
        assert profiler2._query_history is None
        profiler2.enable()
        df1 = session.create_dataframe(
            [(1, "session1"), (3, "more1")], schema=["id", "value"]
        )
        df2 = session2.create_dataframe(
            [(2, "session2"), (4, "more2")], schema=["id", "value"]
        )
        result1 = df1.collect()
        result2 = df2.collect()
        assert len(result1) == 2
        assert len(result2) == 2
        assert profiler1._query_history is not None
        assert profiler2._query_history is not None
        assert profiler1._query_history != profiler2._query_history
        validate_execution_profile(df1, ["ValuesClause", "Input Rows", "Output Rows"])
        validate_execution_profile(df2, ["ValuesClause", "Input Rows", "Output Rows"])

    finally:
        profiler1.disable()
        profiler2.disable()
        session2.close()
