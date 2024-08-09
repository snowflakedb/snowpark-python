#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import List

import pytest

from snowflake.snowpark import Window
from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectSQL,
    SelectStatement,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    PlanQueryType,
    SnowflakePlan,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SaveMode,
    SnowflakeCreateTable,
    TableCreationSource,
)
from snowflake.snowpark._internal.compiler.utils import create_query_generator
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import avg, col, lit
from tests.integ.scala.test_dataframe_reader_suite import get_reader
from tests.utils import TestFiles, Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="the new compilation step is not supported and required by local testing",
        run=False,
    )
]


def reset_node(node: LogicalPlan) -> None:
    def reset_selectable(selectable_node: Selectable) -> None:
        if not isinstance(node, (SelectSnowflakePlan, SelectSQL)):
            selectable_node._snowflake_plan = None
        if isinstance(node, (SelectStatement, SetStatement)):
            selectable_node._sql_query = None

    if isinstance(node, SnowflakePlan):
        # do not reset leaf snowflake plan
        if node.source_plan is not None:
            node.queries = None
            node.post_actions = None
            if isinstance(node.source_plan, Selectable):
                reset_selectable(node.source_plan)
    elif isinstance(node, Selectable):
        reset_selectable(node)


def reset_plan_tree(plan: SnowflakePlan) -> None:
    # traverse the tree to get all the children nodes for reset
    nodes = []
    current_level = [plan]
    while len(current_level) > 0:
        next_level = []
        for node in current_level:
            for child in node.children_plan_nodes:
                next_level.append(child)
            nodes.append(node)
        current_level = next_level

    for node in nodes:
        reset_node(node)


def check_generated_plan_queries(plan: SnowflakePlan) -> None:
    original_queries = [query.sql for query in plan.queries]
    original_post_actions = [query.sql for query in plan.post_actions]
    # init the query generator
    query_generator = create_query_generator(plan)
    source_plan = plan.source_plan
    # reset the whole plan
    reset_plan_tree(plan)
    assert plan.queries is None
    assert plan.post_actions is None
    # regenerate the queries
    plan_queries = query_generator.generate_queries([source_plan])
    queries = [query.sql for query in plan_queries[PlanQueryType.QUERIES]]
    post_actions = [query.sql for query in plan_queries[PlanQueryType.POST_ACTIONS]]
    assert queries == original_queries
    assert post_actions == original_post_actions


@pytest.mark.parametrize(
    "action",
    [
        lambda x: x.select("a", "b").select("b"),
        lambda x: x.filter(col("a") == 1).select("b"),
        lambda x: x.drop("b").sort("a", ascending=False),
        lambda x: x.to_df("a1", "b1").alias("L"),
        lambda x: x.select("a").union_all(x.select("b")),
        lambda x: x.select("a").intersect(x.select("b")),
        lambda x: x.join(x.select("a"), how="left", rsuffix="_y"),
    ],
)
def test_selectable_query_generation(session, action):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df_res = action(df)
    check_generated_plan_queries(df_res._plan)


@pytest.mark.parametrize("enable_sql_simplifier", [False])
@pytest.mark.parametrize(
    "query",
    [
        "select 1 as a, 2 as b",
        "show tables in schema limit 10",
    ],
)
def test_sql_select_with_sql_simplifier_configured(
    session, query, enable_sql_simplifier, sql_simplifier_enabled
):
    try:
        session.sql_simplifier_enabled = enable_sql_simplifier
        df = session.sql(query)
        # when sql simplifier is disabled, there is no source plan associated
        # with the df directly created from select.
        if enable_sql_simplifier:
            check_generated_plan_queries(df._plan)
        df_filtered = session.sql(query).filter(lit(True))
        check_generated_plan_queries(df_filtered._plan)
    finally:
        session.sql_simplifier_enabled = sql_simplifier_enabled


@pytest.mark.parametrize(
    "mode", [SaveMode.APPEND, SaveMode.TRUNCATE, SaveMode.ERROR_IF_EXISTS]
)
def test_table_create(session, mode):
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
    )
    snowflake_plan = session._analyzer.resolve(create_table_logic_plan)
    check_generated_plan_queries(snowflake_plan)


def test_pivot_unpivot(session):
    session.sql(
        """create or replace temp table monthly_sales(empid int, amount int, month text)
             as select * from values
             (1, 10000, 'JAN'),
             (1, 400, 'JAN'),
             (2, 4500, 'JAN'),
             (2, 35000, 'JAN'),
             (1, 5000, 'FEB'),
             (1, 3000, 'FEB'),
             (2, 200, 'FEB')"""
    ).collect()
    # static pivot
    df_pivot = (
        session.table("monthly_sales").pivot("month", ["JAN", "FEB"]).sum("amount")
    )
    check_generated_plan_queries(df_pivot._plan)
    # dynamic pivot
    # since we directly operate on the original plan tree, reconstruct a df plan
    df_dynamic_pivot = (
        session.table("monthly_sales")
        .pivot("month", default_on_null=True)
        .sum("amount")
    )
    check_generated_plan_queries(df_dynamic_pivot._plan)
    # unpivot
    df_unpivot = session.create_dataframe(
        [(1, "electronics", 100, 200), (2, "clothes", 100, 300)],
        schema=["empid", "dept", "jan", "feb"],
    ).unpivot("sales", "month", ["jan", "feb"])
    check_generated_plan_queries(df_unpivot._plan)


def test_groupby_agg(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df_groupby = df.group_by("a").avg("b").filter(col("a") == 1)
    check_generated_plan_queries(df_groupby._plan)


def test_window_function(session):
    window1 = (
        Window.partition_by("value").order_by("key").rows_between(Window.CURRENT_ROW, 2)
    )
    window2 = Window.order_by(col("key").desc()).range_between(
        Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING
    )
    df = (
        session.create_dataframe(
            [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
        )
        .select(
            avg("value").over(window1).as_("window1"),
            avg("value").over(window2).as_("window2"),
        )
        .sort("window1")
    )
    df_result = df.union_all(df).select("*")
    check_generated_plan_queries(df_result._plan)


@pytest.mark.skipif(
    "config.getoption('disable_sql_simplifier', default=False)",
    reason="no source plan is available for df reader",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_df_reader(session, mode, resources_path):
    reader = get_reader(session, mode)
    session_stage = session.get_session_stage()
    test_files = TestFiles(resources_path)
    test_file_on_stage = f"{session_stage}/testCSV.csv"
    Utils.upload_to_stage(
        session, session_stage, test_files.test_file_csv, compress=False
    )
    df = reader.option("INFER_SCHEMA", True).csv(test_file_on_stage)
    # no source plan is available when sql simplifier is disabled, skip the check
    check_generated_plan_queries(df._plan)


def test_dataframe_creation_with_multiple_queries(session):
    # multiple queries and
    df = session.create_dataframe([1] * 20000)
    queries, post_actions = df.queries["queries"], df.queries["post_actions"]

    def verify_multiple_create_queries(
        plan_queries: List[str], post_action_queries: List[str]
    ) -> None:
        assert len(plan_queries) == 3
        assert plan_queries[0].startswith("CREATE")
        assert plan_queries[1].startswith("INSERT")
        assert plan_queries[2].startswith("SELECT")
        assert len(post_action_queries) == 1
        assert post_action_queries[0].startswith("DROP")

    verify_multiple_create_queries(queries, post_actions)

    query_generator = create_query_generator(df._plan)
    # reset the whole plan
    reset_plan_tree(df._plan)
    # regenerate the queries
    plan_queries = query_generator.generate_queries([df._plan.source_plan])
    queries = [query.sql.lstrip() for query in plan_queries[PlanQueryType.QUERIES]]
    post_actions = [
        query.sql.lstrip() for query in plan_queries[PlanQueryType.POST_ACTIONS]
    ]
    verify_multiple_create_queries(queries, post_actions)


def test_multiple_plan_query_generation(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    with session.query_history() as query_history:
        df.write.save_as_table(
            table_name,
            table_type="temp",
            mode="overwrite",
        )
    # get the last query executed
    table_create_query = query_history.queries[-1].sql_text
    df2 = df.select((col("a") + 1).as_("a"), "b")
    df_res = df.union_all(df2)
    df_queries = [query.sql for query in df_res._plan.queries]
    df_post_actions = [query.sql for query in df_res._plan.post_actions]
    expected_queries = [table_create_query] + df_queries
    expected_post_actions = df_post_actions

    create_table_logic_plan = SnowflakeCreateTable(
        [table_name],
        column_names=None,
        mode=SaveMode.OVERWRITE,
        query=df._plan,
        creation_source=TableCreationSource.OTHERS,
        table_type="temp",
        clustering_exprs=None,
        comment=None,
    )
    snowflake_plan = session._analyzer.resolve(create_table_logic_plan)
    query_generator = create_query_generator(snowflake_plan)
    reset_plan_tree(snowflake_plan)
    reset_plan_tree(df_res._plan)
    logical_plans = [snowflake_plan.source_plan, df_res._plan.source_plan]
    generated_queries = query_generator.generate_queries(logical_plans)
    result_queries = [
        query.sql.lstrip() for query in generated_queries[PlanQueryType.QUERIES]
    ]
    result_post_actions = [
        query.sql.lstrip() for query in generated_queries[PlanQueryType.POST_ACTIONS]
    ]
    assert result_queries == expected_queries
    assert result_post_actions == expected_post_actions
