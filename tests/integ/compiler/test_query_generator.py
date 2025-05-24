#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
from typing import List

import pytest
from snowflake.connector import IntegrityError

from snowflake.snowpark import Window
from snowflake.snowpark._internal.analyzer import analyzer
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
    CopyIntoLocationNode,
    LogicalPlan,
    SaveMode,
    SnowflakeCreateTable,
    SnowflakeTable,
    TableCreationSource,
)
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    TableDelete,
    TableMerge,
    TableUpdate,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    CreateViewCommand,
    PersistedView,
    Project,
)
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.compiler.repeated_subquery_elimination import (
    RepeatedSubqueryElimination,
)
from snowflake.snowpark._internal.compiler.utils import (
    create_query_generator,
    resolve_and_update_snowflake_plan,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import avg, col, lit, when_matched
from snowflake.snowpark.types import StructType, StructField, LongType
from tests.integ.scala.test_dataframe_reader_suite import get_reader
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import TestFiles, Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="the new compilation step is not supported and required by local testing",
        run=False,
    )
]


def reset_node(node: LogicalPlan, query_generator: QueryGenerator) -> None:
    def reset_selectable(selectable_node: Selectable) -> None:
        # reset the analyzer to use the current query generator instance to
        # ensure the new query generator is used during the resolve process
        selectable_node._is_valid_for_replacement = True
        selectable_node.analyzer = query_generator
        if not isinstance(selectable_node, (SelectSnowflakePlan, SelectSQL)):
            selectable_node._snowflake_plan = None
        if isinstance(selectable_node, (SelectStatement, SetStatement)):
            selectable_node._sql_query = None
            selectable_node._projection_in_str = None
        if isinstance(selectable_node, SelectStatement):
            selectable_node.expr_to_alias = selectable_node.from_.expr_to_alias

    if isinstance(node, SnowflakePlan):
        # do not reset leaf snowflake plan
        if node.source_plan is not None:
            if isinstance(node.source_plan, Selectable):
                reset_selectable(node.source_plan)
    elif isinstance(node, Selectable):
        reset_selectable(node)


def re_resolve_and_compare_plan_queries(
    plan: SnowflakePlan, query_generator: QueryGenerator
) -> None:
    original_queries = [query.sql for query in plan.queries]
    original_post_actions = [query.sql for query in plan.post_actions]
    resolve_and_update_snowflake_plan(plan, query_generator)

    queries = [query.sql for query in plan.queries]
    post_actions = [query.sql for query in plan.post_actions]
    assert queries == original_queries
    assert post_actions == original_post_actions


def check_generated_plan_queries(
    plan: SnowflakePlan, expected_query_count: int = 0
) -> None:
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

    with SqlCounter(query_count=expected_query_count, describe_count=0):
        # init the query generator
        query_generator = create_query_generator(plan)

        nodes = nodes[::-1]  # reverse the list
        for node in nodes:
            reset_node(node, query_generator)
            if isinstance(node, SnowflakePlan):
                re_resolve_and_compare_plan_queries(node, query_generator)


def verify_multiple_create_queries(
    plan_queries: List[str], post_action_queries: List[str], num_queries: int
) -> None:
    assert len(plan_queries) == num_queries
    assert plan_queries[0].startswith("CREATE")
    assert plan_queries[1].startswith("INSERT")
    assert plan_queries[-1].startswith("SELECT")
    # Note that that is only true when the creat is followed with INSERT, there
    # could be cases that create with one single statement. So be careful when
    # using this utility.
    assert len(post_action_queries) == int(num_queries / 2)
    assert post_action_queries[0].startswith("DROP")


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
        table_exists=False,
    )
    snowflake_plan = session._analyzer.resolve(create_table_logic_plan)
    expected_query_count = 0
    check_generated_plan_queries(
        snowflake_plan, expected_query_count=expected_query_count
    )


@pytest.mark.parametrize(
    "plan_source_generator",
    [
        lambda x: SnowflakeCreateTable(
            ["random_temp_table_name"],
            column_names=None,
            mode=SaveMode.ERROR_IF_EXISTS,
            query=x.sql("select 1 as a, 2 as b, 3 as c")._plan,
            creation_source=TableCreationSource.OTHERS,
            table_type="temp",
            clustering_exprs=None,
            comment=None,
        ),
        lambda x: Project([], SnowflakeTable("random_temp_table_name", session=x)),
    ],
)
def test_table_create_from_large_query_breakdown(session, plan_source_generator):
    plan_source = plan_source_generator(session)
    snowflake_plan = session._analyzer.resolve(plan_source)
    generator = create_query_generator(snowflake_plan)

    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    child_df = session.sql("select 1 as a, 2 as b")
    create_table_source = SnowflakeCreateTable(
        [table_name],
        column_names=None,
        mode=SaveMode.ERROR_IF_EXISTS,
        query=child_df._plan,
        creation_source=TableCreationSource.LARGE_QUERY_BREAKDOWN,
        table_type="temp",
        clustering_exprs=None,
        comment=None,
    )

    with SqlCounter(query_count=0, describe_count=0):
        queries = generator.generate_queries([create_table_source])
    assert len(queries[PlanQueryType.QUERIES]) == 1
    assert len(queries[PlanQueryType.POST_ACTIONS]) == 0

    assert (
        queries[PlanQueryType.QUERIES][0].sql
        == f" CREATE  SCOPED TEMPORARY  TABLE  {table_name}    AS  SELECT  * \n FROM (\nselect 1 as a, 2 as b\n)"
    )


@sql_count_checker(query_count=0)
def test_create_query_generator_fails_with_large_query_breakdown(session):
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    child_df = session.sql("select 1 as a, 2 as b")
    create_table_source = SnowflakeCreateTable(
        [table_name],
        column_names=None,
        mode=SaveMode.ERROR_IF_EXISTS,
        query=child_df._plan,
        creation_source=TableCreationSource.LARGE_QUERY_BREAKDOWN,
        table_type="temp",
        clustering_exprs=None,
        comment=None,
    )

    with pytest.raises(
        AssertionError,
        match="query generator is not supported for large query breakdown as creation source",
    ):
        create_query_generator(session._analyzer.resolve(create_table_source))


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

    verify_multiple_create_queries(queries, post_actions, 3)

    with SqlCounter(query_count=0, describe_count=0):
        query_generator = create_query_generator(df._plan)
        # reset the whole plan
        re_resolve_and_compare_plan_queries(df._plan, query_generator)


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
    reset_node(snowflake_plan, query_generator)
    reset_node(df_res._plan, query_generator)
    logical_plans = [snowflake_plan.source_plan, df_res._plan.source_plan]
    with SqlCounter(query_count=0, describe_count=0):
        generated_queries = query_generator.generate_queries(logical_plans)
    result_queries = [
        query.sql.lstrip() for query in generated_queries[PlanQueryType.QUERIES]
    ]
    result_post_actions = [
        query.sql.lstrip() for query in generated_queries[PlanQueryType.POST_ACTIONS]
    ]
    assert result_queries == expected_queries
    assert result_post_actions == expected_post_actions


@pytest.mark.parametrize(
    "plan_lambda",
    [
        lambda df, name: SnowflakeCreateTable(
            [name],
            column_names=None,
            mode=SaveMode.OVERWRITE,
            query=df._plan,
            creation_source=TableCreationSource.OTHERS,
            table_type="temp",
            clustering_exprs=None,
            comment=None,
        ),
        lambda df, name: CreateViewCommand(name, PersistedView(), None, True, df._plan),
        lambda df, name: CopyIntoLocationNode(
            df._plan,
            name,
            partition_by=None,
            file_format_name=None,
            file_format_type=None,
            format_type_options=None,
            header=False,
            copy_options={},
        ),
        lambda df, name: TableMerge(
            name,
            df._plan,
            (df["a"] == 1)._expression,
            [when_matched().update({"value": df["a"]})._clause],
        ),
        lambda df, name: TableUpdate(name, {}, None, df._plan),
        lambda df, name: TableDelete(name, None, df._plan),
    ],
)
def test_referenced_cte_propagation(session, plan_lambda):
    cte_optimization_enabled = session._cte_optimization_enabled
    try:
        session._cte_optimization_enabled = True
        df0 = session.create_dataframe([[1], [2], [5]], schema=["a"])
        df = session.create_dataframe(
            [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]],
            schema=["a", "b", "c", "d"],
        )
        df_filter = df0.filter(col("a") < 3)
        # filter with NOT
        df_in = df.filter(~df["a"].in_(df_filter))
        df = df_in.union_all(df)
        table_name = random_name_for_temp_object(TempObjectType.TABLE)
        logical_plan = plan_lambda(df, table_name)
        snowflake_plan = session._analyzer.resolve(logical_plan)
        query_generator = create_query_generator(snowflake_plan)

        repeated_subquery_elimination = RepeatedSubqueryElimination(
            [copy.deepcopy(snowflake_plan)], query_generator
        )
        optimized_logical_plans = repeated_subquery_elimination.apply().logical_plans
        assert len(optimized_logical_plans) == 1
        assert len(optimized_logical_plans[0].referenced_ctes) == 1
    finally:
        session._cte_optimization_enabled = cte_optimization_enabled


def test_in_with_subquery(session):
    # This test checks the subquery usage of the code generator
    df0 = session.create_dataframe([[1], [2], [5]], schema=["a"])
    df = session.create_dataframe(
        [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33]], schema=["a", "b", "c", "d"]
    )
    df_filter = df0.filter(col("a") < 3)
    # filter with NOT
    df_in = df.filter(~df["a"].in_(df_filter))
    check_generated_plan_queries(df_in._plan)


def test_in_with_subquery_multiple_query(session):
    # multiple queries
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        if session.sql_simplifier_enabled:
            expected_describe_count = 1 if session.reduce_describe_query_enabled else 3
        else:
            expected_describe_count = 2
        with SqlCounter(query_count=0, describe_count=expected_describe_count):
            df0 = session.create_dataframe([[1], [2], [5], [7]], schema=["a"])
            df = session.create_dataframe(
                [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33], [5, "c", 21, 18]],
                schema=["a", "b", "c", "d"],
            )
            df_select = df.select(
                ~df["a"].in_(df0.filter(col("a") < 2)).as_("in_result")
            )
        with SqlCounter(query_count=0, describe_count=0):
            query_generator = create_query_generator(df_select._plan)
            re_resolve_and_compare_plan_queries(df_select._plan, query_generator)

        queries = [query.sql.lstrip() for query in df_select._plan.queries]
        post_actions = [query.sql.lstrip() for query in df_select._plan.post_actions]
        verify_multiple_create_queries(queries, post_actions, 5)
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold


def test_df_with_pre_actions(session):
    df = session.sql("show tables")
    if session.sql_simplifier_enabled:
        # when sql simplified is enabled, the df plan is associated with
        # a SelectStatement, we can check the query generation
        check_generated_plan_queries(df._plan)
    df = df.select("database_name", "schema_name", "is_external")
    check_generated_plan_queries(df._plan)


def test_dataframe_alas_join(session):
    df1 = session.create_dataframe([[1, 6], [3, 8], [7, 7]], schema=["col1", "col2"])
    df2 = session.create_dataframe([[1, 2], [3, 4], [5, 5]], schema=["col1", "col2"])
    df_res = (
        df1.alias("L")
        .join(df2.alias("R"), col("L", "col1") == col("R", "col1"))
        .select(col("L", "col1"), col("R", "col2"))
    )
    check_generated_plan_queries(df_res._plan)


def test_select_alias(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.select("a", "b", (col("a") + col("b")).as_("c"))
    # Add a new column d that doesn't use c after c was added previously. Flatten safely.
    df2 = df1.select("a", "b", "c", (col("a") + col("b") + 1).as_("d"))
    check_generated_plan_queries(df2._plan)


def test_nullable_is_false_dataframe(session):
    from snowflake.snowpark._internal.analyzer.analyzer import ARRAY_BIND_THRESHOLD

    schema = StructType([StructField("key", LongType(), nullable=True)])
    assert session.create_dataframe([None], schema=schema).collect()[0][0] is None

    assert (
        session.create_dataframe(
            [None for _ in range(ARRAY_BIND_THRESHOLD + 1)], schema=schema
        ).collect()[0][0]
        is None
    )

    schema = StructType([StructField("key", LongType(), nullable=False)])
    with pytest.raises(IntegrityError, match="NULL result in a non-nullable column"):
        session.create_dataframe([None for _ in range(10)], schema=schema).collect()

    with pytest.raises(IntegrityError, match="NULL result in a non-nullable column"):
        session.create_dataframe(
            [None for _ in range(ARRAY_BIND_THRESHOLD + 1)], schema=schema
        ).collect()
