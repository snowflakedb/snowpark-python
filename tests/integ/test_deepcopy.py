#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
from typing import Callable, List, Optional

import pytest

from snowflake.snowpark import functions as F, types as T
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    attribute_to_schema_string,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    ColumnStateDict,
    Selectable,
    SelectableEntity,
    SelectSQL,
    SelectTableFunction,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SaveMode,
    SnowflakeCreateTable,
    TableCreationSource,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    CreateViewCommand,
    LocalTempView,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.column import CaseExpr, Column
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import col, lit, seq1, uniform
from tests.utils import Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="deepcopy is not supported and required by local testing",
        run=False,
    )
]


def create_df_with_deep_nested_with_column_dependencies(
    session, temp_table_name, nest_level: int
) -> DataFrame:
    """
    This creates a sample table with 1
    """
    # create a tabel with 11 columns (1 int columns and 10 string columns) for testing
    struct_fields = [T.StructField("intCol", T.IntegerType(), True)]
    for i in range(1, 11):
        struct_fields.append(T.StructField(f"col{i}", T.StringType(), True))
    schema = T.StructType(struct_fields)

    Utils.create_table(
        session, temp_table_name, attribute_to_schema_string(schema), is_temporary=True
    )

    df = session.table(temp_table_name)

    def get_col_ref_expression(iter_num: int, col_func: Callable) -> Column:
        ref_cols = [F.lit(str(iter_num))]
        for i in range(1, 5):
            col_name = f"col{i}"
            ref_col = col_func(df[col_name])
            ref_cols.append(ref_col)
        return F.concat(*ref_cols)

    for i in range(1, nest_level):
        int_col = df["intCol"]
        col1_base = get_col_ref_expression(i, F.initcap)
        case_expr: Optional[CaseExpr] = None
        # generate the condition expression based on the number of conditions
        for j in range(1, 3):
            if j == 1:
                cond_col = int_col < 100
                col_ref_expr = get_col_ref_expression(i, F.upper)
            else:
                cond_col = int_col < 300
                col_ref_expr = get_col_ref_expression(i, F.lower)
            case_expr = (
                F.when(cond_col, col_ref_expr)
                if case_expr is None
                else case_expr.when(cond_col, col_ref_expr)
            )

        col1 = case_expr.otherwise(col1_base)

        df = df.with_columns(["col1"], [col1])

    return df


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


def verify_snowflake_plan_attribute(
    copied_plan_attribute: List[Attribute], original_plan_attributes: List[Attribute]
) -> None:
    for copied_attribute, original_attribute in zip(
        copied_plan_attribute, original_plan_attributes
    ):
        assert copied_attribute.name == original_attribute.name
        assert copied_attribute.datatype == original_attribute.datatype
        assert copied_attribute.nullable == original_attribute.nullable


def check_copied_plan(
    copied_plan: SnowflakePlan,
    original_plan: SnowflakePlan,
    skip_attribute: bool = False,
) -> None:
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
    assert copied_plan.schema_query == original_plan.schema_query
    if not skip_attribute:
        verify_snowflake_plan_attribute(
            copied_plan.attributes, original_plan.attributes
        )

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


def test_selectsql(session):
    query = "show tables in schema limit 10"
    df = session.sql(query).filter(lit(True))

    def verify_selectsql(copied_node: SelectSQL, original_node: SelectSQL) -> None:
        assert copied_node.original_sql == original_node.original_sql
        assert copied_node.convert_to_select == original_node.convert_to_select
        assert copied_node.convert_to_select is True
        assert copied_node._sql_query == original_node._sql_query
        assert copied_node._schema_query == original_node._schema_query
        assert copied_node._query_param == original_node._query_param
        assert copied_node.pre_actions == original_node.pre_actions

    if session.sql_simplifier_enabled:
        assert len(df._plan.children_plan_nodes) == 1
        assert isinstance(df._plan.children_plan_nodes[0], SelectSQL)
        select_plan = df._plan.children_plan_nodes[0]
        copied_select = copy.deepcopy(select_plan)
        verify_logical_plan_node(copied_select, select_plan)
        verify_selectsql(copied_select, select_plan)
    else:
        copied_plan = copy.deepcopy(df._plan)
        check_copied_plan(copied_plan, df._plan)


def test_selectentity(session):
    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).write.save_as_table(
        temp_table_name, table_type="temp"
    )
    df = session.table(temp_table_name).filter(col("a") == 1)
    if session.sql_simplifier_enabled:
        assert len(df._plan.children_plan_nodes) == 1
        assert isinstance(df._plan.children_plan_nodes[0], SelectableEntity)

        select_plan = df._plan.children_plan_nodes[0]
        copied_select = copy.deepcopy(select_plan)
        verify_logical_plan_node(copied_select, select_plan)
        assert copied_select.entity.name == select_plan.entity.name
    else:
        copied_plan = copy.deepcopy(df._plan)
        check_copied_plan(copied_plan, df._plan)


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


@pytest.mark.parametrize(
    "mode", [SaveMode.APPEND, SaveMode.TRUNCATE, SaveMode.ERROR_IF_EXISTS]
)
def test_table_creation(session, mode):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    create_table_logic_plan = SnowflakeCreateTable(
        [random_name_for_temp_object(TempObjectType.TABLE)],
        column_names=None,
        mode=mode,
        query=df._plan,
        creation_source=TableCreationSource.OTHERS,
        table_type="temp",
        clustering_exprs=None,
        comment=None,
        table_exists=False,
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
        True,
        df._plan,
    )

    snowflake_plan = session._analyzer.resolve(create_view_logical_plan)
    copied_plan = copy.deepcopy(snowflake_plan)
    check_copied_plan(copied_plan, snowflake_plan)

    # The snowflake plan resolved for CreateViewCommand doesn't have source plan attached today
    # make another copy to check for logical plan copy
    copied_logical_plan = copy.deepcopy(create_view_logical_plan)
    verify_logical_plan_node(copied_logical_plan, create_view_logical_plan)


def test_deep_nested_select(session):
    temp_table_name = Utils.random_table_name()
    try:
        df = create_df_with_deep_nested_with_column_dependencies(
            session, temp_table_name, 20
        )
        # make a copy of the final df plan
        copied_plan = copy.deepcopy(df._plan)
        # skip the checking of plan attribute for this plan, because the plan is complicated for
        # compilation, and attribute issues describing call which will timeout during server compilation.
        check_copied_plan(copied_plan, df._plan, skip_attribute=True)
    finally:
        Utils.drop_table(session, temp_table_name)


@pytest.mark.parametrize(
    "generator",
    [
        lambda session_: session_.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]),
        lambda session_: session_.sql("select 1 as a, 2 as b"),
    ],
)
def test_deepcopy_no_duplicate(session, generator):
    base_df = generator(session)
    df1 = base_df.select(base_df.a, base_df.b.alias("c")).sort("a")
    df2 = base_df.filter(col("a") == 1).with_column("C", col("A") + col("B"))
    final_df = df1.union_all(df2.select("a", "c"))

    copied_plan = copy.deepcopy(final_df._plan)
    check_copied_plan(copied_plan, final_df._plan)

    def traverse_plan(plan, plan_id_map):
        plan_memo = id(plan)

        local_deepcopy_memo = {}
        first_deepcopy = copy.deepcopy(plan, local_deepcopy_memo)
        second_deepcopy = copy.deepcopy(plan, local_deepcopy_memo)
        assert plan_memo in local_deepcopy_memo
        assert first_deepcopy is second_deepcopy

        for child in plan.children_plan_nodes:
            traverse_plan(child, plan_id_map)

    traverse_plan(copied_plan, {})
