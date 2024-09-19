#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_EXCEPT,
    SET_INTERSECT,
    SET_UNION,
    SET_UNION_ALL,
    SelectStatement,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import (
    add_months,
    concat,
    initcap,
    avg,
    col,
    seq1,
    table_function,
    min as min_,
    max as max_,
)

from snowflake.snowpark.session import Session
from snowflake.snowpark.window import Window
from tests.utils import Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="Breaking down queries is done for SQL translation",
        run=False,
    )
]


@pytest.fixture(autouse=True)
def setup(session):
    is_simplifier_enabled = session._sql_simplifier_enabled
    session._sql_simplifier_enabled = True
    yield
    session._sql_simplifier_enabled = is_simplifier_enabled

@pytest.fixture(scope="function")
def simple_dataframe(session) -> DataFrame:
    return session.create_dataframe([[1, 'a', 2], [2, 'b', 3], [3, 'c', 7]], schema=["a", "b", "c"])

def verify_dataframe_select_statement(df: DataFrame, can_be_flattened: bool) -> None:
    assert isinstance(df._plan.source_plan, SelectStatement)
    assert df._plan.source_plan._try_flatten_projection_complexity == can_be_flattened


def test_simple_valid_nested_select(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select((col("a") + 3).as_("a"), "c")
    verify_dataframe_select_statement(df_res, can_be_flattened=True)
    # add one more select
    df_res = df_res.select(col("a") * 2, (col("c") + 2).as_("d"))
    verify_dataframe_select_statement(df_res, can_be_flattened=True)


def test_nested_select_with_star(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select('*')
    # star will be automatically flattened, the complexity won't be flattened
    verify_dataframe_select_statement(df_res, can_be_flattened=False)
    df_res = df_res.select((col("a") + 3).as_("a"), "c")
    verify_dataframe_select_statement(df_res, can_be_flattened=True)


def test_nested_select_with_valid_function_expressions(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(concat("a", "b").as_("a"), initcap("c").as_("c"), "b")
    verify_dataframe_select_statement(df_res, can_be_flattened=True)
    df_res = df_res.select(concat("a", initcap(concat("b", "c"))), add_months("a", 5))
    verify_dataframe_select_statement(df_res, can_be_flattened=True)


def test_nested_select_with_window_functions(simple_dataframe):
    window1 = (
        Window.partition_by("a").order_by("b").rows_between(Window.CURRENT_ROW, 2)
    )
    window2 = Window.order_by(col("c").desc()).range_between(
        Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING
    )
    df_res = simple_dataframe.select(avg("a").over(window1).as_("a"), avg("b").over(window2).as_("b")).select((col("a") + 1).as_("a"),"b")
    verify_dataframe_select_statement(df_res, can_be_flattened=True)


def test_nested_select_with_agg_functions(simple_dataframe):
    df_res = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").select(avg("a").as_("a"), min_("c").as_("c"))
    verify_dataframe_select_statement(df_res, can_be_flattened=False)

    df_res = simple_dataframe.select(max_("a"))
    verify_dataframe_select_statement(df_res, can_be_flattened=False)


def test_nested_select_with_limit_filter(simple_dataframe):
    df_res_filtered = simple_dataframe.filter(col("a") == 1).select((col("a") + 1).as_("a"), "b", "c").select((col("a") + 1).as_("a"), "b")
    verify_dataframe_select_statement(df_res_filtered, can_be_flattened=False)

    df_res_limit = simple_dataframe.select((col("a") + 1).as_("a"), "b", "c").limit(10, 5).select(concat("a", "b").as_("a"), initcap("c").as_("c"), "b")
    verify_dataframe_select_statement(df_res_limit, can_be_flattened=False)


def test_valid_after_invalid_nested_select(simple_dataframe):
    df_res_filtered = simple_dataframe.filter(col("a") == 1).select((col("a") + 1).as_("a"), "b", "c").select((col("a") + 1).as_("a"), "b")
    verify_dataframe_select_statement(df_res_filtered, can_be_flattened=False)

    df_res = df_res_filtered.select((col("a") + 2).as_("a"), (col("b") + 2).as_("b"))
    verify_dataframe_select_statement(df_res, can_be_flattened=True)
