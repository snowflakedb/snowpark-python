#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
from functools import partial
from unittest import mock

import pytest
from snowflake.snowpark._internal.analyzer.metadata_utils import PlanMetadata

from snowflake.snowpark._internal.analyzer.binary_plan_node import Inner, Join, Union
from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
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
    WithQueryBlock,
)
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    TableDelete,
    TableMerge,
    TableUpdate,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import Project, Sort
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.compiler.utils import (
    replace_child,
    update_resolvable_node,
)

old_plan = LogicalPlan()
irrelevant_plan = LogicalPlan()


@pytest.fixture(scope="function")
def new_plan(mock_session):
    table_node = SnowflakeTable(name="table", session=mock_session)
    table_node._is_valid_for_replacement = True
    return table_node


def mock_snowflake_plan() -> SnowflakePlan:
    fake_snowflake_plan = mock.create_autospec(SnowflakePlan)
    fake_snowflake_plan._id = "dummy id"
    fake_snowflake_plan.expr_to_alias = {}
    fake_snowflake_plan.df_aliased_col_name_to_real_col_name = {}
    fake_snowflake_plan.queries = [
        Query("FAKE SQL", query_id_place_holder="query_id_place_holder_abc")
    ]
    fake_snowflake_plan.post_actions = []
    fake_snowflake_plan.api_calls = []
    fake_snowflake_plan.is_ddl_on_temp_object = False
    fake_snowflake_plan._output_dict = []
    fake_snowflake_plan.placeholder_query = None
    with_query_block = WithQueryBlock(name="TEST_CTE", child=LogicalPlan())
    fake_snowflake_plan.referenced_ctes = {with_query_block: 1}
    fake_snowflake_plan._cumulative_node_complexity = {}
    fake_snowflake_plan._is_valid_for_replacement = True
    fake_snowflake_plan._metadata = mock.create_autospec(PlanMetadata)
    fake_snowflake_plan._metadata.attributes = {}
    fake_snowflake_plan.query_line_intervals = []
    return fake_snowflake_plan


@pytest.fixture(scope="function")
def mock_query_generator(mock_session) -> QueryGenerator:
    def mock_resolve(x):
        snowflake_plan = mock_snowflake_plan()
        snowflake_plan.source_plan = x
        snowflake_plan.df_ast_id = None
        if hasattr(x, "post_actions"):
            snowflake_plan.post_actions = x.post_actions
        return snowflake_plan

    fake_query_generator = mock.create_autospec(QueryGenerator)
    fake_query_generator.resolve.side_effect = mock_resolve
    fake_query_generator.session = mock_session
    fake_query_generator.to_selectable = partial(
        QueryGenerator.to_selectable, fake_query_generator
    )
    fake_query_generator._to_selectable_memo_dict = {}
    return fake_query_generator


def assert_precondition(plan, new_plan, query_generator, using_deep_copy=False):
    original_valid_for_replacement = plan._is_valid_for_replacement
    try:
        # verify when parent is not valid for replacement, an error is thrown
        plan._is_valid_for_replacement = False
        with pytest.raises(ValueError, match="is not valid for replacement."):
            replace_child(plan, irrelevant_plan, new_plan, query_generator)
            update_resolvable_node(plan, query_generator)

        with pytest.raises(ValueError, match="is not valid for update."):
            update_resolvable_node(plan, query_generator)

        valid_plan = plan
        if using_deep_copy:
            valid_plan = copy.deepcopy(plan)
        else:
            valid_plan._is_valid_for_replacement = True

        with pytest.raises(ValueError, match="is not a child of parent"):
            replace_child(valid_plan, irrelevant_plan, new_plan, query_generator)

        if not isinstance(valid_plan, (SnowflakePlan, Selectable)):
            with pytest.raises(
                ValueError, match="It is not valid to update node with type"
            ):
                update_resolvable_node(valid_plan, query_generator)
    finally:
        plan._is_valid_for_replacement = original_valid_for_replacement


def verify_snowflake_plan(plan: SnowflakePlan, expected_plan: SnowflakePlan) -> None:
    assert plan.queries == expected_plan.queries
    assert plan.post_actions == expected_plan.post_actions
    assert plan.expr_to_alias == expected_plan.expr_to_alias
    assert plan.is_ddl_on_temp_object == expected_plan.is_ddl_on_temp_object
    assert plan._output_dict == expected_plan._output_dict
    assert (
        plan.df_aliased_col_name_to_real_col_name
        == expected_plan.df_aliased_col_name_to_real_col_name
    )
    current_referenced_ctes_name_map = {
        cte.name: count for cte, count in plan.referenced_ctes.items()
    }
    expected_referenced_ctes_name_map = {
        cte.name: count for cte, count in expected_plan.referenced_ctes.items()
    }
    assert current_referenced_ctes_name_map == expected_referenced_ctes_name_map
    assert plan._cumulative_node_complexity == expected_plan._cumulative_node_complexity
    assert plan.source_plan is not None


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_logical_plan(
    using_snowflake_plan, mock_query, mock_session, new_plan, mock_query_generator
):
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
            session=mock_session,
        )
    else:
        join_plan = src_join_plan

    assert_precondition(join_plan, new_plan, mock_query_generator)
    assert_precondition(project_plan, new_plan, mock_query_generator)

    if using_snowflake_plan:
        join_plan = copy.deepcopy(join_plan)
    else:
        join_plan._is_valid_for_replacement = True
    project_plan._is_valid_for_replacement = True

    assert len(get_children(join_plan)) == 2
    copied_old_plan, copied_project_plan = get_children(join_plan)
    assert isinstance(copied_old_plan, LogicalPlan)
    assert isinstance(copied_project_plan, LogicalPlan)

    replace_child(join_plan, copied_old_plan, new_plan, mock_query_generator)
    assert get_children(join_plan) == [new_plan, copied_project_plan]

    assert project_plan.children == [old_plan]
    replace_child(project_plan, old_plan, new_plan, mock_query_generator)
    assert project_plan.children == [new_plan]


@pytest.mark.parametrize(
    "plan_initializer",
    [
        lambda x: Sort([], x),
        lambda x: Limit(None, None, x),
        lambda x: CopyIntoLocationNode(x, "stage_location", copy_options={}),
    ],
)
def test_unary_plan(plan_initializer, new_plan, mock_query_generator):
    plan = plan_initializer(old_plan)

    assert plan.child == old_plan
    assert plan.children == [old_plan]

    assert_precondition(plan, new_plan, mock_query_generator)
    plan._is_valid_for_replacement = True

    replace_child(plan, old_plan, new_plan, mock_query_generator)
    assert plan.child == new_plan
    assert plan.children == [new_plan]


def test_binary_plan(new_plan, mock_query_generator):
    left_plan = Project([], LogicalPlan())
    plan = Union(left=left_plan, right=old_plan, is_all=False)

    assert plan.left == left_plan
    assert plan.right == old_plan

    assert_precondition(plan, new_plan, mock_query_generator)
    plan._is_valid_for_replacement = True

    replace_child(plan, old_plan, new_plan, mock_query_generator)
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
    plan_initializer, new_plan, mock_analyzer, mock_query_generator
):
    def get_source(plan):
        if hasattr(plan, "source_data"):
            return plan.source_data
        return plan.source

    plan = plan_initializer(old_plan)

    assert get_source(plan) == old_plan
    assert_precondition(plan, new_plan, mock_analyzer)
    plan._is_valid_for_replacement = True

    replace_child(plan, old_plan, new_plan, mock_query_generator)
    assert isinstance(get_source(plan), SnowflakePlan)
    assert plan.children == [get_source(plan)]
    assert plan.children[0].source_plan == new_plan
    verify_snowflake_plan(plan.children[0], mock_snowflake_plan())


def test_snowflake_create_table(new_plan, mock_query_generator):
    plan = SnowflakeCreateTable(["temp_table"], None, "OVERWRITE", old_plan, "temp")

    assert plan.query == old_plan
    assert plan.children == [old_plan]

    assert_precondition(plan, new_plan, mock_query_generator)
    plan._is_valid_for_replacement = True

    replace_child(plan, old_plan, new_plan, mock_query_generator)
    assert plan.query == new_plan
    assert plan.children == [new_plan]


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_selectable_entity(
    using_snowflake_plan,
    mock_session,
    mock_analyzer,
    mock_query_generator,
    mock_query,
    new_plan,
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
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_query_generator, using_deep_copy=True)
    # SelectableEntity has no children
    assert plan.children_plan_nodes == []
    # test update of SelectableEntity
    plan = copy.deepcopy(plan)
    update_resolvable_node(plan, mock_query_generator)
    if using_snowflake_plan:
        assert isinstance(plan.source_plan, SelectableEntity)
        assert plan.source_plan.analyzer == mock_query_generator
    else:
        assert plan._cumulative_node_complexity is None
        assert plan.analyzer == mock_query_generator


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_sql(
    using_snowflake_plan, mock_session, mock_analyzer, mock_query_generator, new_plan
):
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
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_query_generator, using_deep_copy=True)
    # SelectSQL has no children
    assert plan.children_plan_nodes == []

    # test update of SelectableEntity
    plan = copy.deepcopy(plan)
    update_resolvable_node(plan, mock_query_generator)
    if using_snowflake_plan:
        assert isinstance(plan.source_plan, SelectSQL)
        assert plan.source_plan.analyzer == mock_query_generator
    else:
        assert plan._cumulative_node_complexity is None
        assert plan.analyzer == mock_query_generator


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_snowflake_plan(
    using_snowflake_plan,
    mock_session,
    mock_analyzer,
    mock_query_generator,
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
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_query_generator, using_deep_copy=True)
    plan = copy.deepcopy(plan)
    # deep copy created a copy of old_plan
    copied_old_plan = plan.children_plan_nodes[0]
    if using_snowflake_plan:
        copied_project = plan.source_plan._snowflake_plan.source_plan
        copied_select_snowflake_plan = plan.source_plan
    else:
        copied_project = plan._snowflake_plan.source_plan
        copied_select_snowflake_plan = plan

    replace_child(plan, copied_old_plan, new_plan, mock_query_generator)
    assert copied_project.children == [new_plan]

    # verify node update
    update_resolvable_node(plan, mock_query_generator)
    expected_snowflake_plan_content = mock_snowflake_plan()
    verify_snowflake_plan(
        copied_select_snowflake_plan._snowflake_plan, expected_snowflake_plan_content
    )
    if using_snowflake_plan:
        verify_snowflake_plan(plan, expected_snowflake_plan_content)
    else:
        assert plan._cumulative_node_complexity is None

    # verify the analyzer of selectable is updated to query generator
    assert copied_select_snowflake_plan.analyzer == mock_query_generator


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_statement(
    using_snowflake_plan,
    mock_session,
    mock_query_generator,
    mock_analyzer,
    mock_query,
    new_plan,
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
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_query_generator, using_deep_copy=True)
    plan = copy.deepcopy(plan)
    replace_child(plan, plan.children_plan_nodes[0], new_plan, mock_query_generator)
    assert len(plan.children_plan_nodes) == 1

    new_replaced_plan = plan.children_plan_nodes[0]
    assert isinstance(new_replaced_plan, SelectSnowflakePlan)
    assert new_replaced_plan._snowflake_plan.source_plan == new_plan
    # new_replaced_plan is created with QueryGenerator.to_selectable
    assert new_replaced_plan.analyzer == mock_analyzer

    post_actions = [Query("drop table if exists table_name")]
    new_replaced_plan.post_actions = post_actions
    # verify node update
    update_resolvable_node(plan, mock_query_generator)
    assert plan.post_actions == post_actions

    expected_snowflake_plan_content = mock_snowflake_plan()
    verify_snowflake_plan(
        new_replaced_plan._snowflake_plan, expected_snowflake_plan_content
    )
    if using_snowflake_plan:
        expected_snowflake_plan_content.post_actions = post_actions
        verify_snowflake_plan(plan, expected_snowflake_plan_content)
    else:
        assert plan._cumulative_node_complexity is None


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_select_table_function(
    using_snowflake_plan,
    mock_session,
    mock_analyzer,
    mock_query_generator,
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
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_query_generator, using_deep_copy=True)
    plan = copy.deepcopy(plan)

    # deep copy created a copy of old_plan
    copied_old_plan = plan.children_plan_nodes[0]
    if using_snowflake_plan:
        copied_project = plan.source_plan._snowflake_plan.source_plan
    else:
        copied_project = plan._snowflake_plan.source_plan

    replace_child(plan, copied_old_plan, new_plan, mock_query_generator)
    assert copied_project.children == [new_plan]

    # verify node update
    update_resolvable_node(plan, mock_query_generator)
    expected_snowflake_plan_content = mock_snowflake_plan()
    if using_snowflake_plan:
        verify_snowflake_plan(plan, expected_snowflake_plan_content)
        assert isinstance(plan.source_plan, SelectTableFunction)
        assert plan.source_plan.analyzer == mock_query_generator
        verify_snowflake_plan(
            plan.source_plan.snowflake_plan, expected_snowflake_plan_content
        )
    else:
        assert plan._cumulative_node_complexity is None
        assert plan.analyzer == mock_query_generator
        verify_snowflake_plan(plan.snowflake_plan, expected_snowflake_plan_content)


@pytest.mark.parametrize("using_snowflake_plan", [True, False])
def test_set_statement(
    using_snowflake_plan,
    mock_session,
    mock_analyzer,
    mock_query_generator,
    mock_query,
    new_plan,
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
            session=mock_session,
        )

    assert_precondition(plan, new_plan, mock_analyzer, using_deep_copy=True)
    plan = copy.deepcopy(plan)

    replace_child(plan, plan.children_plan_nodes[0], new_plan, mock_query_generator)
    assert len(plan.children_plan_nodes) == 2
    new_replaced_plan = plan.children_plan_nodes[0]
    assert isinstance(plan.children_plan_nodes[0], SelectSnowflakePlan)
    assert isinstance(plan.children_plan_nodes[1], SelectSQL)

    mocked_snowflake_plan = mock_snowflake_plan()
    verify_snowflake_plan(new_replaced_plan.snowflake_plan, mocked_snowflake_plan)

    post_actions = [Query("drop table if exists table_name")]
    new_replaced_plan.post_actions = post_actions
    update_resolvable_node(plan, mock_query_generator)
    assert plan.post_actions == post_actions

    if using_snowflake_plan:
        copied_set_statement = plan.source_plan
    else:
        copied_set_statement = plan
        assert plan._cumulative_node_complexity is None

    assert copied_set_statement.analyzer == mock_query_generator
    assert copied_set_statement._sql_query is None
    assert copied_set_statement._snowflake_plan is None

    if using_snowflake_plan:
        mocked_snowflake_plan.post_actions = post_actions
        # verify the snowflake plan is also updated
        verify_snowflake_plan(plan, mocked_snowflake_plan)


def test_replace_child_negative(new_plan, mock_query_generator):
    mock_parent = mock.Mock()
    mock_parent._is_valid_for_replacement = True
    mock_child = LogicalPlan()
    mock_parent.children_plan_nodes = [mock_child]
    with pytest.raises(ValueError, match="not supported"):
        replace_child(mock_parent, mock_child, new_plan, mock_query_generator)
