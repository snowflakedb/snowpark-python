#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy

import pytest

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    ColumnStateDict,
    Selectable,
    SelectTableFunction,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SaveMode,
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    CreateViewCommand,
    LocalTempView,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import col, seq1, uniform

pytestmark = [
    pytest.mark.skip(
        "config.getoption('local_testing_mode', default=False)",
        reason="deepcopy is required by the new compilation with optimization, which is not supported by local testing",
        run=False,
    )
]


def verify_column_state(
    copied_state: ColumnStateDict, original_state: ColumnStateDict
) -> None:
    assert copied_state.has_changed_columns == original_state.has_changed_columns
    assert copied_state.has_new_columns == original_state.has_new_columns
    assert copied_state.has_dropped_columns == original_state.has_dropped_columns
    assert copied_state.dropped_columns == original_state.dropped_columns
    assert copied_state.active_columns == original_state.active_columns
    assert (
        copied_state.columns_referencing_all_columns
        == original_state.columns_referencing_all_columns
    )


def verify_logical_plan_node(
    copied_node: LogicalPlan, original_node: LogicalPlan
) -> None:
    if copied_node is None and original_node is None:
        return

    assert type(copied_node) == type(original_node)
    # verify the node complexity
    assert (
        copied_node.individual_node_complexity
        == original_node.individual_node_complexity
    )
    assert (
        copied_node.cumulative_node_complexity
        == original_node.cumulative_node_complexity
    )
    # verify update accumulative complexity of copied node doesn't impact original node
    original_complexity = copied_node.cumulative_node_complexity
    copied_node.cumulative_node_complexity = {PlanNodeCategory.OTHERS: 10000}
    assert copied_node.cumulative_node_complexity == {PlanNodeCategory.OTHERS: 10000}
    assert original_node.cumulative_node_complexity == original_complexity
    copied_node.cumulative_node_complexity = original_complexity

    if isinstance(copied_node, Selectable) and isinstance(original_node, Selectable):
        verify_column_state(copied_node.column_states, original_node.column_states)
        assert copied_node.flatten_disabled == original_node.flatten_disabled
        assert (
            copied_node.df_aliased_col_name_to_real_col_name
            == original_node.df_aliased_col_name_to_real_col_name
        )
    if isinstance(copied_node, SetStatement) and isinstance(
        original_node, SetStatement
    ):
        assert copied_node._sql_query == original_node._sql_query
    if isinstance(copied_node, SelectTableFunction) and isinstance(
        original_node, SelectTableFunction
    ):
        # check the source snowflake_plan
        assert (copied_node._snowflake_plan is not None) and (
            original_node._snowflake_plan is not None
        )
        check_copied_plan(copied_node._snowflake_plan, original_node._snowflake_plan)

    if isinstance(copied_node, Selectable) and isinstance(original_node, Selectable):
        copied_child_plan_nodes = copied_node.children_plan_nodes
        original_child_plan_nodes = original_node.children_plan_nodes
        for (copied_plan_node, original_plan_node) in zip(
            copied_child_plan_nodes, original_child_plan_nodes
        ):
            verify_logical_plan_node(copied_plan_node, original_plan_node)


def check_copied_plan(copied_plan: SnowflakePlan, original_plan: SnowflakePlan) -> None:
    # verify the instance type is the same
    assert type(copied_plan) == type(original_plan)
    assert copied_plan.queries == original_plan.queries
    assert copied_plan.post_actions == original_plan.post_actions
    assert (
        copied_plan.df_aliased_col_name_to_real_col_name
        == original_plan.df_aliased_col_name_to_real_col_name
    )
    assert (
        copied_plan.cumulative_node_complexity
        == original_plan.cumulative_node_complexity
    )
    assert (
        copied_plan.individual_node_complexity
        == original_plan.individual_node_complexity
    )
    assert copied_plan.is_ddl_on_temp_object == original_plan.is_ddl_on_temp_object
    assert copied_plan.api_calls == original_plan.api_calls
    assert copied_plan.expr_to_alias == original_plan.expr_to_alias

    # verify changes in the copied plan doesn't impact original plan
    original_sql = original_plan.queries[-1].sql
    copied_plan.queries[-1].sql = "NEW TEST SQL"
    assert original_plan.queries[-1].sql == original_sql
    # should reset the query back for later comparison
    copied_plan.queries[-1].sql = original_sql

    # verify the source plan root node
    copied_source_plan = copied_plan.source_plan
    original_source_plan = original_plan.source_plan
    verify_logical_plan_node(copied_source_plan, original_source_plan)


@pytest.mark.parametrize(
    "action",
    [
        lambda x: x.select("a", "b").select("b"),
        lambda x: x.filter(col("a") == 1).select("b"),
        lambda x: x.drop("b").sort("a", ascending=False),
        lambda x: x.to_df("a1", "b1").alias("L"),
    ],
)
def test_selectable_deepcopy(session, action):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df_res = action(df)
    # make a copy of the plan for df_res
    copied_plan = copy.deepcopy(df_res._plan)
    # verify copied plan
    check_copied_plan(copied_plan, df_res._plan)


@pytest.mark.parametrize(
    "action",
    [
        lambda x, y: x.union_all(y),
        lambda x, y: x.except_(y),
        lambda x, y: x.select("a").intersect(y.select("a")),
        lambda x, y: x.select("a").join(y, how="outer", rsuffix="_y"),
        lambda x, y: x.join(y.select("a"), how="left", rsuffix="_y"),
    ],
)
def test_setstatement_deepcopy(session, action):
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df2 = session.create_dataframe([[3, 4], [2, 1]], schema=["a", "b"])
    df_res = action(df1, df2)
    copied_plan = copy.deepcopy(df_res._plan)
    check_copied_plan(copied_plan, df_res._plan)


def test_df_alias_deepcopy(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df_res = df.to_df("a1", "b1").alias("L")
    copied_plan = copy.deepcopy(df_res._plan)
    check_copied_plan(copied_plan, df_res._plan)


def test_table_function(session):
    df = (
        session.generator(seq1(1), uniform(1, 10, 2), rowcount=150)
        .order_by(seq1(1))
        .limit(3, offset=20)
    )
    df_copied_plan = copy.deepcopy(df._plan)
    check_copied_plan(df_copied_plan, df._plan)
    df_res = df.union_all(df).select("*")
    df_res_copied = copy.deepcopy(df_res._plan)
    check_copied_plan(df_res_copied, df_res._plan)


"""
@pytest.mark.parametrize(
    "mode", [SaveMode.APPEND, SaveMode.TRUNCATE, SaveMode.ERROR_IF_EXISTS]
)
"""


@pytest.mark.parametrize("mode", [SaveMode.APPEND])
def test_table_creation(session, mode):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    create_table_logic_plan = SnowflakeCreateTable(
        [random_name_for_temp_object(TempObjectType.TABLE)],
        column_names=None,
        mode=mode,
        query=df._plan,
        table_type="temp",
        clustering_exprs=None,
        comment=None,
    )
    snowflake_plan = session._analyzer.resolve(create_table_logic_plan)
    copied_plan = copy.deepcopy(snowflake_plan)
    check_copied_plan(copied_plan, snowflake_plan)
    # The snowflake plan resolved for SnowflakeCreateTable doesn't have source plan attached today
    # make another copy to check for logical plan copy
    copied_logical_plan = copy.deepcopy(create_table_logic_plan)
    verify_logical_plan_node(copied_logical_plan, create_table_logic_plan)


def test_create_or_replace_view(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    create_view_logical_plan = CreateViewCommand(
        random_name_for_temp_object(TempObjectType.VIEW),
        LocalTempView(),
        None,
        df._plan,
    )

    snowflake_plan = session._analyzer.resolve(create_view_logical_plan)
    copied_plan = copy.deepcopy(snowflake_plan)
    check_copied_plan(copied_plan, snowflake_plan)

    # The snowflake plan resolved for CreateViewCommand doesn't have source plan attached today
    # make another copy to check for logical plan copy
    copied_logical_plan = copy.deepcopy(create_view_logical_plan)
    verify_logical_plan_node(copied_logical_plan, create_view_logical_plan)
