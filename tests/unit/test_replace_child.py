#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
from unittest import mock

import pytest

from snowflake.snowpark._internal.analyzer.binary_plan_node import Inner, Join, Union
from snowflake.snowpark._internal.analyzer.select_statement import (
    SelectableEntity,
    SelectSnowflakePlan,
    SelectSQL,
    SelectStatement,
    SelectTableFunction,
    SetOperand,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoLocationNode,
    Limit,
    LogicalPlan,
    SnowflakeCreateTable,
    SnowflakeTable,
)
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    TableDelete,
    TableMerge,
    TableUpdate,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import Project, Sort
from snowflake.snowpark._internal.compiler.utils import replace_child_and_update_parent

old_plan = LogicalPlan()
irrelevant_plan = LogicalPlan()


@pytest.fixture(scope="module")
def new_plan(mock_session):
    table_node = SnowflakeTable(name="table", session=mock_session)
    table_node._is_valid_for_replacement = True
    return table_node


def assert_precondition(plan, new_plan, analyzer, using_deep_copy=False):
    original_valid_for_replacement = plan._is_valid_for_replacement
    try:
        # verify when parent is not valid for replacement, an error is thrown
        plan._is_valid_for_replacement = False
        with pytest.raises(ValueError, match="is not valid for replacement."):
            replace_child_and_update_parent(plan, irrelevant_plan, new_plan, analyzer)

        valid_plan = plan
        if using_deep_copy:
            valid_plan = copy.deepcopy(plan)
        else:
            valid_plan._is_valid_for_replacement = True

        with pytest.raises(ValueError, match="is not a child of parent"):
            replace_child_and_update_parent(
                valid_plan, irrelevant_plan, new_plan, analyzer
            )
    finally:
        plan._is_valid_for_replacement = original_valid_for_replacement


def assert_plan_node_reset(node: LogicalPlan) -> None:
    if isinstance(node, SnowflakePlan):
        assert node.queries is None
        assert node.post_actions is None
    elif isinstance(node, (SelectStatement, SetStatement)):
        assert node._snowflake_plan is None
        assert node._sql_query is None
    elif isinstance(node, SelectSnowflakePlan):
        assert node._snowflake_plan is not None
        assert_plan_node_reset(node._snowflake_plan)


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_logical_plan(using_snowflake_plan, mock_query, new_plan, mock_analyzer):
    def get_children(plan):
        if isinstance(plan, SnowflakePlan):
            return plan.children_plan_nodes
        return plan.children

    project_plan = LogicalPlan()
    project_plan.children = [old_plan]
    src_join_plan = Join(
        left=old_plan,
        right=project_plan,
        join_type=Inner,
        join_condition=None,
        match_condition=None,
    )

    if using_snowflake_plan:
        join_plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=src_join_plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=None,
        )
    else:
        join_plan = src_join_plan

    assert_precondition(join_plan, new_plan, mock_analyzer)
    assert_precondition(project_plan, new_plan, mock_analyzer)

    if using_snowflake_plan:
        join_plan = copy.deepcopy(join_plan)
    else:
        join_plan._is_valid_for_replacement = True
    project_plan._is_valid_for_replacement = True

    assert len(get_children(join_plan)) == 2
    copied_old_plan, copied_project_plan = get_children(join_plan)
    assert isinstance(copied_old_plan, LogicalPlan)
    assert isinstance(copied_project_plan, LogicalPlan)

    replace_child_and_update_parent(join_plan, copied_old_plan, new_plan, mock_analyzer)
    assert get_children(join_plan) == [new_plan, copied_project_plan]

    assert project_plan.children == [old_plan]
    replace_child_and_update_parent(project_plan, old_plan, new_plan, mock_analyzer)
    assert project_plan.children == [new_plan]


@pytest.mark.parametrize(
    "plan_initializer",
    [
        lambda x: Sort([], x),
        lambda x: Limit(None, None, x),
        lambda x: CopyIntoLocationNode(x, "stage_location", copy_options={}),
    ],
)
def test_unary_plan(plan_initializer, new_plan, mock_analyzer):
    plan = plan_initializer(old_plan)

    assert plan.child == old_plan
    assert plan.children == [old_plan]

    assert_precondition(plan, new_plan, mock_analyzer)
    plan._is_valid_for_replacement = True

    replace_child_and_update_parent(plan, old_plan, new_plan, mock_analyzer)
    assert plan.child == new_plan
    assert plan.children == [new_plan]


def test_binary_plan(new_plan, mock_analyzer):
    left_plan = Project([], LogicalPlan())
    plan = Union(left=left_plan, right=old_plan, is_all=False)

    assert plan.left == left_plan
    assert plan.right == old_plan

    assert_precondition(plan, new_plan, mock_analyzer)
    plan._is_valid_for_replacement = True

    replace_child_and_update_parent(plan, old_plan, new_plan, mock_analyzer)
    assert plan.left == left_plan
    assert plan.right == new_plan
    assert plan.children == [left_plan, new_plan]


@pytest.mark.parametrize(
    "plan_initializer",
    [
        lambda x: TableDelete("table_name", None, x),
        lambda x: TableUpdate("table_name", {}, None, x),
        lambda x: TableMerge("table_name", x, None, []),
    ],
)
def test_table_delete_update_merge(
    plan_initializer, new_plan, mock_analyzer, mock_snowflake_plan
):
    def get_source(plan):
        if hasattr(plan, "source_data"):
            return plan.source_data
        return plan.source

    plan = plan_initializer(old_plan)

    assert get_source(plan) == old_plan
    assert_precondition(plan, new_plan, mock_analyzer)
    plan._is_valid_for_replacement = True

    replace_child_and_update_parent(plan, old_plan, new_plan, mock_analyzer)
    assert get_source(plan) == mock_snowflake_plan
    assert plan.children == [mock_snowflake_plan]
    assert mock_snowflake_plan.source_plan == new_plan


def test_snowflake_create_table(new_plan, mock_analyzer):
    plan = SnowflakeCreateTable(["temp_table"], None, "OVERWRITE", old_plan, "temp")

    assert plan.query == old_plan
    assert plan.children == [old_plan]

    assert_precondition(plan, new_plan, mock_analyzer)
    plan._is_valid_for_replacement = True

    replace_child_and_update_parent(plan, old_plan, new_plan, mock_analyzer)
    assert plan.query == new_plan
    assert plan.children == [new_plan]


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_selectable_entity(
    using_snowflake_plan, mock_session, mock_analyzer, mock_query, new_plan
):
    table = SnowflakeTable(name="table", session=mock_session)
    plan = SelectableEntity(entity=table, analyzer=mock_analyzer)
    if using_snowflake_plan:
        plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_analyzer, using_deep_copy=True)
    # SelectableEntity has no children
    assert plan.children_plan_nodes == []


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_sql(using_snowflake_plan, mock_session, mock_analyzer, new_plan):
    plan = SelectSQL("FAKE QUERY", analyzer=mock_analyzer)
    if using_snowflake_plan:
        plan = SnowflakePlan(
            queries=[Query("FAKE QUERY")],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_analyzer, using_deep_copy=True)
    # SelectSQL has no children
    assert plan.children_plan_nodes == []


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_snowflake_plan(
    using_snowflake_plan,
    mock_session,
    mock_analyzer,
    mock_query,
    new_plan,
):
    project_plan = Project([], old_plan)
    snowflake_plan = SnowflakePlan(
        queries=[mock_query],
        schema_query="",
        post_actions=[],
        expr_to_alias={},
        source_plan=project_plan,
        api_calls=None,
        df_aliased_col_name_to_real_col_name=None,
        placeholder_query=None,
        session=mock_session,
    )

    plan = SelectSnowflakePlan(snowflake_plan, analyzer=mock_analyzer)

    if using_snowflake_plan:
        plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_analyzer, using_deep_copy=True)
    plan = copy.deepcopy(plan)
    # deep copy created a copy of old_plan
    copied_old_plan = plan.children_plan_nodes[0]
    if using_snowflake_plan:
        copied_project = plan.source_plan._snowflake_plan.source_plan
    else:
        copied_project = plan._snowflake_plan.source_plan

    replace_child_and_update_parent(plan, copied_old_plan, new_plan, mock_analyzer)
    # verify the source plan is cleared
    # assert_plan_node_reset(plan)
    assert copied_project.children == [new_plan]


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_statement(
    using_snowflake_plan,
    mock_session,
    mock_analyzer,
    mock_query,
    new_plan,
    mock_snowflake_plan,
):
    from_ = SelectSnowflakePlan(
        SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=None,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        ),
        analyzer=mock_analyzer,
    )
    plan = SelectStatement(from_=from_, analyzer=mock_analyzer)

    if using_snowflake_plan:
        plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_analyzer, using_deep_copy=True)
    plan = copy.deepcopy(plan)
    replace_child_and_update_parent(plan, from_, new_plan, mock_analyzer)
    # assert_plan_node_reset(plan)
    assert len(plan.children_plan_nodes) == 1
    assert plan.children_plan_nodes[0].snowflake_plan == mock_snowflake_plan
    assert mock_snowflake_plan.source_plan == new_plan


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_table_function(
    using_snowflake_plan, mock_session, mock_analyzer, mock_query, new_plan
):
    project_plan = Project([], old_plan)
    snowflake_plan = SnowflakePlan(
        queries=[mock_query],
        schema_query="",
        post_actions=[],
        expr_to_alias={},
        source_plan=project_plan,
        api_calls=None,
        df_aliased_col_name_to_real_col_name=None,
        placeholder_query=None,
        session=mock_session,
    )
    plan = SelectTableFunction(
        TableFunctionExpression("table_function"),
        analyzer=mock_analyzer,
        snowflake_plan=snowflake_plan,
    )
    if using_snowflake_plan:
        plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_analyzer, using_deep_copy=True)
    plan = copy.deepcopy(plan)

    # deep copy created a copy of old_plan
    copied_old_plan = plan.children_plan_nodes[0]
    if using_snowflake_plan:
        copied_project = plan.source_plan._snowflake_plan.source_plan
    else:
        copied_project = plan._snowflake_plan.source_plan

    replace_child_and_update_parent(plan, copied_old_plan, new_plan, mock_analyzer)
    assert_plan_node_reset(plan)
    assert copied_project.children == [new_plan]


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_set_statement(
    using_snowflake_plan,
    mock_session,
    mock_analyzer,
    mock_query,
    new_plan,
    mock_snowflake_plan,
):
    selectable1 = SelectableEntity(
        SnowflakeTable(name="table1", session=mock_session), analyzer=mock_analyzer
    )
    selectable2 = SelectSQL("SELECT * FROM table2", analyzer=mock_analyzer)
    set_operand1 = SetOperand(selectable1, "UNION")
    set_operand2 = SetOperand(selectable2, "UNION")
    plan = SetStatement(set_operand1, set_operand2, analyzer=mock_analyzer)
    if using_snowflake_plan:
        plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_analyzer, using_deep_copy=True)
    plan = copy.deepcopy(plan)

    replace_child_and_update_parent(plan, selectable1, new_plan, mock_analyzer)
    # assert_plan_node_reset(plan)
    assert len(plan.children_plan_nodes) == 2
    assert plan.children_plan_nodes[0].snowflake_plan == mock_snowflake_plan
    assert plan.children_plan_nodes[1] == selectable2
    if not using_snowflake_plan:
        assert plan.set_operands[0].selectable.snowflake_plan == mock_snowflake_plan
    assert mock_snowflake_plan.source_plan == new_plan


def test_replace_child_negative(new_plan, mock_analyzer):
    mock_parent = mock.Mock()
    mock_parent._is_valid_for_replacement = True
    mock_child = LogicalPlan()
    mock_parent.children_plan_nodes = [mock_child]
    with pytest.raises(ValueError, match="not supported"):
        replace_child_and_update_parent(
            mock_parent, mock_child, new_plan, mock_analyzer
        )
