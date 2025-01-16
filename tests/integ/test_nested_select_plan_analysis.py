#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, Optional

import pytest

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
)
from snowflake.snowpark._internal.analyzer.select_statement import SelectStatement
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import (
    add_months,
    avg,
    builtin,
    call_table_function,
    col,
    concat,
    initcap,
    lit,
    max as max_,
    min as min_,
)
from snowflake.snowpark.window import Window
from tests.integ.test_deepcopy import (
    create_df_with_deep_nested_with_column_dependencies,
)
from tests.integ.test_query_plan_analysis import assert_df_subtree_query_complexity
from tests.utils import Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="Breaking down queries is done for SQL translation",
        run=False,
    )
]


paramList = [False, True]


@pytest.fixture(params=paramList, autouse=True)
def setup(request, session):
    is_simplifier_enabled = session._sql_simplifier_enabled
    large_query_breakdown_enabled = session.large_query_breakdown_enabled
    session.large_query_breakdown_enabled = request.param
    session._sql_simplifier_enabled = True
    yield
    session._sql_simplifier_enabled = is_simplifier_enabled
    session.large_query_breakdown_enabled = large_query_breakdown_enabled


@pytest.fixture(scope="function")
def simple_dataframe(session) -> DataFrame:
    """
    The complexity of the simple_dataframe is {COLUMN: 6, LITERAL: 9}, and corresponds to the following query:

    SELECT "A", "B", "C" FROM (
        SELECT $1 AS "A", $2 AS "B", $3 AS "C" FROM  VALUES (
            1 :: INT, \'a\' :: STRING, 2 :: INT), (2 :: INT, \'b\' :: STRING, 3 :: INT), (3 :: INT, \'c\' :: STRING, 7 :: INT))
    """
    return session.create_dataframe(
        [[1, "a", 2], [2, "b", 3], [3, "c", 7]], schema=["a", "b", "c"]
    )


@pytest.fixture(scope="function")
def sample_table(session):
    table_name = Utils.random_table_name()
    Utils.create_table(
        session, table_name, "a int, b int, c int, d int", is_temporary=True
    )
    session._run_query(
        f"insert into {table_name}(a, b, c, d) values " "(1, 2, 3, 4), (5, 6, 7, 8)"
    )
    yield table_name
    Utils.drop_table(session, table_name)


def verify_dataframe_select_statement(
    df: DataFrame,
    can_be_merged_when_enabled: bool,
    complexity_before_merge: Dict[PlanNodeCategory, int],
    complexity_after_merge: Optional[Dict[PlanNodeCategory, int]] = None,
) -> None:
    assert isinstance(df._plan.source_plan, SelectStatement)

    if not df.session.large_query_breakdown_enabled:
        # if large query breakdown is disabled, _merge_projection_complexity_with_subquery will always be false
        assert df._plan.source_plan._merge_projection_complexity_with_subquery is False
        assert_df_subtree_query_complexity(df, complexity_before_merge)
    else:
        assert (
            df._plan.source_plan._merge_projection_complexity_with_subquery
            == can_be_merged_when_enabled
        )
        if can_be_merged_when_enabled:
            assert (
                complexity_after_merge
            ), "no complexity after merge is provided for validation"
            assert_df_subtree_query_complexity(df, complexity_after_merge)
        else:
            assert_df_subtree_query_complexity(df, complexity_before_merge)


def test_simple_valid_nested_select(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        (col("a") + 3).as_("a"), "c"
    )
    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.COLUMN: 8,
            PlanNodeCategory.LITERAL: 11,
        },
        complexity_after_merge={
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.LITERAL: 11,
            PlanNodeCategory.COLUMN: 5,
        },
    )
    # add one more select
    df_res = df_res.select(col("a") * 2, (col("c") + 2).as_("d"))
    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 4,
            PlanNodeCategory.COLUMN: 10,
            PlanNodeCategory.LITERAL: 13,
        },
        complexity_after_merge={
            PlanNodeCategory.LOW_IMPACT: 4,
            PlanNodeCategory.LITERAL: 13,
            PlanNodeCategory.COLUMN: 5,
        },
    )


def test_nested_select_with_star(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select("*")
    print(df_res._plan.cumulative_node_complexity)
    # star will be automatically flattened, the complexity won't be flattened
    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.COLUMN: 6,
            PlanNodeCategory.LITERAL: 10,
        },
    )
    df_res = df_res.select((col("a") + 3).as_("a"), "c")
    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.COLUMN: 8,
            PlanNodeCategory.LITERAL: 11,
        },
        complexity_after_merge={
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.LITERAL: 11,
            PlanNodeCategory.COLUMN: 5,
        },
    )


def test_nested_select_with_valid_function_expressions(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        concat("a", "b").as_("a"), initcap("c").as_("c"), "b"
    )
    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.FUNCTION: 2,
            PlanNodeCategory.COLUMN: 10,
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.LITERAL: 10,
        },
        complexity_after_merge={
            PlanNodeCategory.FUNCTION: 2,
            PlanNodeCategory.COLUMN: 7,
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.LITERAL: 10,
        },
    )

    df_res = df_res.select(concat("a", initcap(concat("b", "c"))), add_months("a", 5))
    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.FUNCTION: 6,
            PlanNodeCategory.COLUMN: 14,
            PlanNodeCategory.LITERAL: 11,
            PlanNodeCategory.LOW_IMPACT: 1,
        },
        complexity_after_merge={
            PlanNodeCategory.FUNCTION: 7,
            PlanNodeCategory.COLUMN: 9,
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.LITERAL: 12,
        },
    )


def test_nested_select_with_window_functions(simple_dataframe):
    window1 = Window.partition_by("a").order_by("b").rows_between(Window.CURRENT_ROW, 2)
    window2 = Window.order_by(col("c").desc()).range_between(
        Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING
    )
    df_res = simple_dataframe.select(
        avg("a").over(window1).as_("a"), avg("b").over(window2).as_("b")
    ).select((col("a") + 1).as_("a"), "b")

    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 6,
            PlanNodeCategory.COLUMN: 10,
            PlanNodeCategory.LITERAL: 11,
            PlanNodeCategory.WINDOW: 2,
            PlanNodeCategory.FUNCTION: 2,
            PlanNodeCategory.PARTITION_BY: 1,
            PlanNodeCategory.ORDER_BY: 2,
            PlanNodeCategory.OTHERS: 2,
        },
    )


def test_nested_select_with_table_functions(session):
    df = session.table_function(
        call_table_function(
            "split_to_table", lit("split words to table"), lit(" ")
        ).over()
    )
    df_res = df.select((col("a") + 1).as_("a"), "b", "c")

    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.COLUMN: 3,
            PlanNodeCategory.LITERAL: 1,
        },
    )


def test_nested_select_with_valid_builtin_function(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        builtin("nvl")(col("a"), col("b")).as_("a"),
        builtin("nvl2")(col("b"), col("c")).as_("c"),
    )

    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.FUNCTION: 2,
            PlanNodeCategory.COLUMN: 10,
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.LITERAL: 10,
        },
        complexity_after_merge={
            PlanNodeCategory.FUNCTION: 2,
            PlanNodeCategory.COLUMN: 7,
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.LITERAL: 10,
        },
    )


def test_nested_select_with_agg_functions(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        avg("a").as_("a"), min_("c").as_("c")
    )

    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.FUNCTION: 2,
            PlanNodeCategory.COLUMN: 8,
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.LITERAL: 10,
        },
    )

    df_res = simple_dataframe.select(
        max_("a").as_("a"), (min_("c") + 1).as_("c")
    ).select(col("a") + 1, "c")
    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.COLUMN: 7,
            PlanNodeCategory.LITERAL: 11,
            PlanNodeCategory.FUNCTION: 2,
        },
    )


def test_nested_select_with_limit_filter_order_by(simple_dataframe):
    def_order_by_filter = (
        simple_dataframe.select((col("a") + 1).as_("a"), "b", "c")
        .order_by(col("a"))
        .filter(col("a") == 1)
    )
    df_res = def_order_by_filter.select((col("a") + 2).as_("a"))

    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 3,
            PlanNodeCategory.COLUMN: 9,
            PlanNodeCategory.LITERAL: 12,
            PlanNodeCategory.FILTER: 1,
            PlanNodeCategory.OTHERS: 1,
            PlanNodeCategory.ORDER_BY: 1,
        },
    )


def test_select_with_dependency_within_same_level(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        (col("a") + 2).as_("d"), (col("d") + 1).as_("e")
    )

    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 3,
            PlanNodeCategory.COLUMN: 8,
            PlanNodeCategory.LITERAL: 12,
        },
    )


def test_select_with_duplicated_columns(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        (col("a") + 2).as_("b"), (col("b") + 1).as_("b")
    )

    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 3,
            PlanNodeCategory.COLUMN: 8,
            PlanNodeCategory.LITERAL: 12,
        },
        complexity_after_merge={
            PlanNodeCategory.LOW_IMPACT: 3,
            PlanNodeCategory.LITERAL: 12,
            PlanNodeCategory.COLUMN: 5,
        },
    )


def test_select_with_dollar_dependency(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1), "b", "c").select(
        (col("$1") + 2).as_("b"), col("$2").as_("c")
    )

    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.COLUMN: 8,
            PlanNodeCategory.LITERAL: 11,
        },
    )


def test_valid_after_invalid_nested_select(simple_dataframe):
    df_res_filtered = (
        simple_dataframe.filter(col("a") == 1)
        .select((col("a") + 1).as_("a"), "b", "c")
        .select((col("a") + 1).as_("a"), "b")
    )
    print(df_res_filtered._plan.cumulative_node_complexity)
    verify_dataframe_select_statement(
        df_res_filtered,
        can_be_merged_when_enabled=False,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 3,
            PlanNodeCategory.COLUMN: 9,
            PlanNodeCategory.LITERAL: 12,
            PlanNodeCategory.FILTER: 1,
        },
    )

    df_res = df_res_filtered.select((col("a") + 2).as_("a"), (col("b") + 2).as_("b"))
    print(df_res._plan.cumulative_node_complexity)
    verify_dataframe_select_statement(
        df_res,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 5,
            PlanNodeCategory.COLUMN: 11,
            PlanNodeCategory.LITERAL: 14,
            PlanNodeCategory.FILTER: 1,
        },
        complexity_after_merge={
            PlanNodeCategory.LOW_IMPACT: 5,
            PlanNodeCategory.LITERAL: 14,
            PlanNodeCategory.COLUMN: 9,
            PlanNodeCategory.FILTER: 1,
        },
    )


def test_simple_nested_select_with_repeated_column_dependency(session, sample_table):
    df = session.table(sample_table)
    df_select = df.select((col("a") + 1).as_("a"), "b", "c")
    assert_df_subtree_query_complexity(
        df_select,
        {
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.COLUMN: 4,
            PlanNodeCategory.LITERAL: 1,
        },
    )

    df_select = df_select.select((col("a") + 3).as_("a"), "c")
    # the two select complexity can be merged when large query breakdown enabled,
    # and will be equivalent to the complexity of
    # df.select((col("a") + 1 + 3).as_("a"), "c")
    verify_dataframe_select_statement(
        df_select,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.LITERAL: 2,
            PlanNodeCategory.COLUMN: 6,
            PlanNodeCategory.LOW_IMPACT: 2,
        },
        complexity_after_merge={
            PlanNodeCategory.LITERAL: 2,
            PlanNodeCategory.COLUMN: 3,
            PlanNodeCategory.LOW_IMPACT: 2,
        },
    )

    # add one more select with duplicated reference
    df_select = df_select.select(
        col("a") * 2 + col("a") + col("c"), (col("c") + 2).as_("d")
    )
    print(df_select._plan.cumulative_node_complexity)
    # the complexity can be continue merged with the previous select, and the whole tree complexity
    # will be equivalent to df.select((col("a") + 3 + 1) * 2 + (col("a") + 3 + 1)  + col("c"), (col("c") + 2).as_("d")
    verify_dataframe_select_statement(
        df_select,
        can_be_merged_when_enabled=True,
        complexity_before_merge={
            PlanNodeCategory.LOW_IMPACT: 6,
            PlanNodeCategory.COLUMN: 10,
            PlanNodeCategory.LITERAL: 4,
        },
        complexity_after_merge={
            PlanNodeCategory.LOW_IMPACT: 8,
            PlanNodeCategory.COLUMN: 5,
            PlanNodeCategory.LITERAL: 6,
        },
    )


def test_deep_nested_with_columns(session):
    temp_table_name = Utils.random_table_name()
    try:
        df = create_df_with_deep_nested_with_column_dependencies(
            session, temp_table_name, 5
        )
        verify_dataframe_select_statement(
            df,
            can_be_merged_when_enabled=True,
            complexity_before_merge={
                PlanNodeCategory.COLUMN: 97,
                PlanNodeCategory.CASE_WHEN: 4,
                PlanNodeCategory.LOW_IMPACT: 8,
                PlanNodeCategory.LITERAL: 20,
                PlanNodeCategory.FUNCTION: 60,
            },
            complexity_after_merge={
                PlanNodeCategory.COLUMN: 532,
                PlanNodeCategory.CASE_WHEN: 40,
                PlanNodeCategory.LOW_IMPACT: 80,
                PlanNodeCategory.LITERAL: 200,
                PlanNodeCategory.FUNCTION: 600,
            },
        )
        print(df._plan.cumulative_node_complexity)
    finally:
        Utils.drop_table(session, temp_table_name)
