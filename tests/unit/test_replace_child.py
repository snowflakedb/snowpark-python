#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy

import pytest

from snowflake.snowpark._internal.analyzer.binary_plan_node import Inner, Join
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
    LogicalPlan,
    SnowflakeTable,
)
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.unary_plan_node import Project

old_plan = LogicalPlan()
new_plan = LogicalPlan()
irrelevant_plan = LogicalPlan()


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_logical_plan(using_snowflake_plan, mock_query):
    get_children = (
        lambda plan: plan.children_plan_nodes if using_snowflake_plan else plan.children
    )
    project_plan = Project([], old_plan)
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

    with pytest.raises(
        ValueError, match="replace child can only be called on a deep copied plan node."
    ):
        join_plan.replace_child(irrelevant_plan, new_plan)

    if using_snowflake_plan:
        join_plan = copy.deepcopy(join_plan)
    else:
        join_plan = copy.deepcopy(src_join_plan)
        join_plan._is_deep_copied = True

    with pytest.raises(
        ValueError, match="old_node to be replaced is not found in the children nodes."
    ):
        join_plan.replace_child(irrelevant_plan, new_plan)
    assert len(get_children(join_plan)) == 2
    copied_old_plan, copied_project_plan = get_children(join_plan)
    assert isinstance(copied_old_plan, LogicalPlan)
    assert isinstance(copied_project_plan, Project)

    join_plan.replace_child(copied_old_plan, new_plan)
    assert get_children(join_plan) == [new_plan, copied_project_plan]


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_selectable_entity(
    using_snowflake_plan, mock_session, mock_analyzer, mock_query
):
    table = SnowflakeTable(name="table", session=mock_session)
    selectable_entity = SelectableEntity(entity=table, analyzer=mock_analyzer)
    if using_snowflake_plan:
        selectable_entity = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=selectable_entity,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )
    with pytest.raises(
        ValueError, match="replace child can only be called on a deep copied plan node."
    ):
        selectable_entity.replace_child(irrelevant_plan, new_plan)
    selectable_entity = copy.deepcopy(selectable_entity)

    with pytest.raises(
        ValueError, match="old_node to be replaced is not found in the children nodes."
    ):
        selectable_entity.replace_child(irrelevant_plan, new_plan)

    # SelectableEntity has no children
    assert selectable_entity.children_plan_nodes == []


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_sql(using_snowflake_plan, mock_session, mock_analyzer):
    select_sql_plan = SelectSQL("FAKE QUERY", analyzer=mock_analyzer)
    with pytest.raises(
        ValueError, match="replace child can only be called on a deep copied plan node."
    ):
        select_sql_plan.replace_child(irrelevant_plan, new_plan)
    select_sql_plan = copy.deepcopy(select_sql_plan)

    with pytest.raises(
        ValueError, match="old_node to be replaced is not found in the children nodes."
    ):
        select_sql_plan.replace_child(irrelevant_plan, new_plan)

    if using_snowflake_plan:
        select_sql_plan = SnowflakePlan(
            queries=[Query("FAKE QUERY")],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=select_sql_plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    # SelectSQL has no children
    assert select_sql_plan.children_plan_nodes == []


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_snowflake_plan(
    using_snowflake_plan, mock_session, mock_analyzer, mock_query
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

    select_snowflake_plan = SelectSnowflakePlan(snowflake_plan, analyzer=mock_analyzer)

    if using_snowflake_plan:
        select_snowflake_plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=select_snowflake_plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    with pytest.raises(
        ValueError, match="replace child can only be called on a deep copied plan node."
    ):
        select_snowflake_plan.replace_child(irrelevant_plan, new_plan)
    select_snowflake_plan = copy.deepcopy(select_snowflake_plan)

    with pytest.raises(
        ValueError, match="old_node to be replaced is not found in the children nodes."
    ):
        select_snowflake_plan.replace_child(irrelevant_plan, new_plan)
    assert len(select_snowflake_plan.children_plan_nodes) == 1

    # deep copy created a copy of old_plan
    copied_old_plan = select_snowflake_plan.children_plan_nodes[0]

    select_snowflake_plan.replace_child(copied_old_plan, new_plan)
    assert select_snowflake_plan.children_plan_nodes == [new_plan]


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_statement(
    using_snowflake_plan, mock_session, mock_analyzer, mock_query
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
    select_statement_plan = SelectStatement(from_=from_, analyzer=mock_analyzer)

    if using_snowflake_plan:
        select_statement_plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=select_statement_plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    with pytest.raises(
        ValueError, match="replace child can only be called on a deep copied plan node."
    ):
        select_statement_plan.replace_child(irrelevant_plan, new_plan)
    select_statement_plan = copy.deepcopy(select_statement_plan)

    with pytest.raises(
        ValueError, match="old_node to be replaced is not found in the children nodes."
    ):
        select_statement_plan.replace_child(irrelevant_plan, new_plan)
    assert select_statement_plan.children_plan_nodes == [from_]

    select_statement_plan.replace_child(from_, new_plan)
    assert select_statement_plan.children_plan_nodes == [new_plan]


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_table_function(
    using_snowflake_plan, mock_session, mock_analyzer, mock_query
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
    table_function_plan = SelectTableFunction(
        TableFunctionExpression("table_function"),
        analyzer=mock_analyzer,
        snowflake_plan=snowflake_plan,
    )
    if using_snowflake_plan:
        table_function_plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=table_function_plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    with pytest.raises(
        ValueError, match="replace child can only be called on a deep copied plan node."
    ):
        table_function_plan.replace_child(irrelevant_plan, new_plan)
    table_function_plan = copy.deepcopy(table_function_plan)

    with pytest.raises(
        ValueError, match="old_node to be replaced is not found in the children nodes."
    ):
        table_function_plan.replace_child(irrelevant_plan, new_plan)
    assert len(table_function_plan.children_plan_nodes) == 1

    # deep copy created a copy of old_plan
    copied_old_plan = table_function_plan.children_plan_nodes[0]

    table_function_plan.replace_child(copied_old_plan, new_plan)
    assert table_function_plan.children_plan_nodes == [new_plan]


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_set_statement(using_snowflake_plan, mock_session, mock_analyzer, mock_query):
    selectable1 = SelectableEntity(
        SnowflakeTable(name="table1", session=mock_session), analyzer=mock_analyzer
    )
    selectable2 = SelectSQL("SELECT * FROM table2", analyzer=mock_analyzer)
    set_operand1 = SetOperand(selectable1, "UNION")
    set_operand2 = SetOperand(selectable2, "UNION")
    set_statement_plan = SetStatement(
        set_operand1, set_operand2, analyzer=mock_analyzer
    )
    if using_snowflake_plan:
        set_statement_plan = SnowflakePlan(
            queries=[mock_query],
            schema_query="",
            post_actions=[],
            expr_to_alias={},
            source_plan=set_statement_plan,
            api_calls=None,
            df_aliased_col_name_to_real_col_name=None,
            placeholder_query=None,
            session=mock_session,
        )

    with pytest.raises(
        ValueError, match="replace child can only be called on a deep copied plan node."
    ):
        set_statement_plan.replace_child(irrelevant_plan, new_plan)
    set_statement_plan = copy.deepcopy(set_statement_plan)

    with pytest.raises(
        ValueError, match="old_node to be replaced is not found in the children nodes."
    ):
        set_statement_plan.replace_child(irrelevant_plan, new_plan)
    assert set_statement_plan.children_plan_nodes == [selectable1, selectable2]

    set_statement_plan.replace_child(selectable1, new_plan)
    assert set_statement_plan.children_plan_nodes == [new_plan, selectable2]
