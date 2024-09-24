#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

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
    return session.create_dataframe(
        [[1, "a", 2], [2, "b", 3], [3, "c", 7]], schema=["a", "b", "c"]
    )


def verify_dataframe_select_statement(
    df: DataFrame, can_be_merged_when_enabled: bool
) -> None:
    assert isinstance(df._plan.source_plan, SelectStatement)

    if not df.session.large_query_breakdown_enabled:
        # if large query breakdown is disabled, _merge_projection_complexity_with_subquery will always be false
        assert df._plan.source_plan._merge_projection_complexity_with_subquery is False
    else:
        assert (
            df._plan.source_plan._merge_projection_complexity_with_subquery
            == can_be_merged_when_enabled
        )


def test_simple_valid_nested_select(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        (col("a") + 3).as_("a"), "c"
    )
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=True)
    # add one more select
    df_res = df_res.select(col("a") * 2, (col("c") + 2).as_("d"))
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=True)


def test_nested_select_with_star(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select("*")
    # star will be automatically flattened, the complexity won't be flattened
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=False)
    df_res = df_res.select((col("a") + 3).as_("a"), "c")
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=True)


def test_nested_select_with_valid_function_expressions(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        concat("a", "b").as_("a"), initcap("c").as_("c"), "b"
    )
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=True)
    df_res = df_res.select(concat("a", initcap(concat("b", "c"))), add_months("a", 5))
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=True)


def test_nested_select_with_window_functions(simple_dataframe):
    window1 = Window.partition_by("a").order_by("b").rows_between(Window.CURRENT_ROW, 2)
    window2 = Window.order_by(col("c").desc()).range_between(
        Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING
    )
    df_res = simple_dataframe.select(
        avg("a").over(window1).as_("a"), avg("b").over(window2).as_("b")
    ).select((col("a") + 1).as_("a"), "b")
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=False)


def test_nested_select_with_table_functions(session):
    df = session.table_function(
        call_table_function(
            "split_to_table", lit("split words to table"), lit(" ")
        ).over()
    )
    df_res = df.select((col("a") + 1).as_("a"), "b", "c")

    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=False)


def test_nested_select_with_valid_builtin_function(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        builtin("nvl")(col("a"), col("b")).as_("a"),
        builtin("nvl2")(col("b"), col("c")).as_("c"),
    )
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=True)


def test_nested_select_with_agg_functions(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        avg("a").as_("a"), min_("c").as_("c")
    )
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=False)

    df_res = simple_dataframe.select(max_("a"))
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=False)


def test_nested_select_with_limit_filter_order_by(simple_dataframe):
    """
    df_res_filtered = (
        simple_dataframe.filter(col("a") == 1)
        .select((col("a") + 1).as_("a"), "b", "c")
        .select((col("a") + 1).as_("a"), "b")
    )
    verify_dataframe_select_statement(df_res_filtered, can_be_merged_when_enabled=False)

    df_res_limit = (
        simple_dataframe.select((col("a") + 1).as_("a"), "b", "c")
        .limit(10, 5)
        .select(concat("a", "b").as_("a"), initcap("c").as_("c"), "b")
    )
    verify_dataframe_select_statement(df_res_limit, can_be_merged_when_enabled=False)
    """

    def_order_by_filter = (
        simple_dataframe.select((col("a") + 1).as_("a"), "b", "c")
        .order_by(col("a"))
        .filter(col("a") == 1)
    )
    df_res = def_order_by_filter.select((col("a") + 2).as_("a"))
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=False)


def test_select_with_dependency_within_same_level(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        (col("a") + 2).as_("d"), (col("d") + 1).as_("e")
    )
    # star will be automatically flattened, the complexity won't be flattened
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=False)


def test_select_with_duplicated_columns(simple_dataframe):
    def_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(
        (col("a") + 2).as_("b"), (col("b") + 1).as_("b")
    )
    verify_dataframe_select_statement(def_res, can_be_merged_when_enabled=True)


def test_select_with_dollar_dependency(simple_dataframe):
    def_res = simple_dataframe.select((col("a") + 1), "b", "c").select(
        (col("$1") + 2).as_("b"), col("$2").as_("c")
    )
    verify_dataframe_select_statement(def_res, can_be_merged_when_enabled=False)


def test_valid_after_invalid_nested_select(simple_dataframe):
    df_res_filtered = (
        simple_dataframe.filter(col("a") == 1)
        .select((col("a") + 1).as_("a"), "b", "c")
        .select((col("a") + 1).as_("a"), "b")
    )
    verify_dataframe_select_statement(df_res_filtered, can_be_merged_when_enabled=False)

    df_res = df_res_filtered.select((col("a") + 2).as_("a"), (col("b") + 2).as_("b"))
    verify_dataframe_select_statement(df_res, can_be_merged_when_enabled=True)
