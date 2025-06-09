#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
from typing import Dict, List

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.analyzer.analyzer_utils import schema_value_statement
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import PlanState
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    PlanQueryType,
    Query,
    SnowflakePlan,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    SaveMode,
    TableCreationSource,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.functions import col, lit, table_function
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType, LongType
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.utils import IS_IN_STORED_PROC, Utils

if sys.version_info <= (3, 9):
    from typing import Generator
else:
    from collections.abc import Generator

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    ),
]


@pytest.fixture(scope="module")
def temp_table(session: Session) -> Generator[str, None, None]:
    table_name = Utils.random_table_name()
    Utils.create_table(session, table_name, "a int, b int", True)
    session._run_query(f"insert into {table_name}(a, b) values (1, 2), (3, 4)")
    yield table_name
    Utils.drop_table(session, table_name)


def test_single_query(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name, "num int, str string")
        session.sql(
            f"insert into {table_name} values(1, 'a'),(2, 'b'),(3, 'c')"
        ).collect()

        df = session.table(table_name)
        assert df.count() == 3
        assert df.filter(col("num") < 3).count() == 2

        # build plan
        plans = session._plan_builder
        table_plan = plans.table(table_name, df._plan.source_plan)
        project = plans.project(["num"], table_plan, None)

        assert len(project.queries) == 1
    finally:
        Utils.drop_table(session, table_name)


def test_multiple_queries(session):
    # unary query
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    queries = [
        Query(
            f"create or replace temporary table {table_name1} as select * from values(1::INT, 'a'::STRING),(2::INT, 'b'::STRING) as T(A,B)"
        ),
        Query(f"select * from {table_name1}"),
    ]
    attrs = [Attribute("A", IntegerType()), Attribute("B", IntegerType())]

    try:
        plan = SnowflakePlan(
            queries, schema_value_statement(attrs), None, None, session=session
        )
        plan1 = session._plan_builder.project(["A"], plan, None)

        assert len(plan1.attributes) == 1
        assert plan1.attributes[0].name == '"A"'
        # is is always nullable
        assert plan1.attributes[0].nullable
        # SF always returns Long Type
        assert type(plan1.attributes[0].datatype) == LongType

        res = session._conn.execute(plan1)
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2)]
    finally:
        Utils.drop_table(session, table_name1)

    # binary query
    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    queries2 = [
        Query(
            f"create or replace temporary table {table_name2} as select * from values(3::INT),(4::INT) as T(A)"
        ),
        Query(f"select * from {table_name2}"),
    ]
    attrs2 = [Attribute("C", LongType())]

    try:
        plan2 = SnowflakePlan(
            queries2, schema_value_statement(attrs2), None, None, session=session
        )
        plan3 = session._plan_builder.set_operator(plan1, plan2, "UNION ALL", None)

        assert len(plan3.attributes) == 1
        assert plan3.attributes[0].name == '"A"'
        assert plan3.attributes[0].nullable
        assert type(plan3.attributes[0].datatype) == LongType

        res2 = session._conn.execute(plan3)
        res2.sort(key=lambda x: x[0])
        assert res2 == [Row(1), Row(2), Row(3), Row(4)]
    finally:
        Utils.drop_table(session, table_name2)


def test_execution_queries_and_post_actions(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.select("a", "b")
    # create a df where cte optimization can be applied
    df2 = df1.union(df1)
    original_cte_enabled_value = session.cte_optimization_enabled

    plan_queries = {
        PlanQueryType.QUERIES: df2._plan.queries,
        PlanQueryType.POST_ACTIONS: df2._plan.post_actions,
    }

    def check_plan_queries(
        cte_applied: bool, exec_queries: Dict[PlanQueryType, List["Query"]]
    ) -> None:
        assert (
            exec_queries[PlanQueryType.POST_ACTIONS]
            == plan_queries[PlanQueryType.POST_ACTIONS]
            == []
        )
        if cte_applied:
            assert (
                exec_queries[PlanQueryType.QUERIES][-1].sql
                != plan_queries[PlanQueryType.QUERIES][-1].sql
            )
            assert exec_queries[PlanQueryType.QUERIES][-1].sql.startswith("WITH")
            assert not plan_queries[PlanQueryType.QUERIES][-1].sql.startswith("WITH")
        else:
            assert (
                exec_queries[PlanQueryType.QUERIES]
                == plan_queries[PlanQueryType.QUERIES]
            )
            assert not (exec_queries[PlanQueryType.QUERIES][-1].sql.startswith("WITH"))

    try:
        # when cte is disabled, verify that the execution query got is the same as
        # the plan queries and post actions
        session.cte_optimization_enabled = False
        check_plan_queries(cte_applied=False, exec_queries=df2._plan.execution_queries)

        # when cte is enabled, verify that the execution query got is different
        # from the original plan queries
        session.cte_optimization_enabled = True
        check_plan_queries(
            # the cte optimization is not kicking in when sql simplifier disabled, because
            # the cte_optimization_enabled is set to False when constructing the plan for df2,
            # and place_holder is not propogated.
            cte_applied=session.sql_simplifier_enabled
            or session._query_compilation_stage_enabled,
            exec_queries=df2._plan.execution_queries,
        )

    finally:
        session.cte_optimization_enabled = original_cte_enabled_value


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Unable to detect sql_simplifier_enabled fixture in SP"
)
def test_plan_height(session, temp_table, sql_simplifier_enabled):
    df1 = session.table(temp_table)
    if sql_simplifier_enabled:
        assert df1._plan.plan_state[PlanState.PLAN_HEIGHT] == 2
    else:
        assert df1._plan.plan_state[PlanState.PLAN_HEIGHT] == 1

    df2 = session.create_dataframe([(1, 20), (3, 40)], schema=["a", "c"])
    df3 = session.create_dataframe(
        [(2, "twenty two"), (4, "forty four"), (4, "forty four")], schema=["b", "d"]
    )
    assert df2._plan.plan_state[PlanState.PLAN_HEIGHT] == 2
    assert df2._plan.plan_state[PlanState.PLAN_HEIGHT] == 2

    filter1 = df1.where(col("a") > 1)
    assert filter1._plan.plan_state[PlanState.PLAN_HEIGHT] == 2

    join1 = filter1.join(df2, on=["a"])
    assert join1._plan.plan_state[PlanState.PLAN_HEIGHT] == 4

    aggregate1 = df3.distinct()
    if sql_simplifier_enabled:
        assert aggregate1._plan.plan_state[PlanState.PLAN_HEIGHT] == 4
    else:
        assert aggregate1._plan.plan_state[PlanState.PLAN_HEIGHT] == 3

    join2 = join1.join(aggregate1, on=["b"])
    assert join2._plan.plan_state[PlanState.PLAN_HEIGHT] == 6

    split_to_table = table_function("split_to_table")
    table_function1 = join2.select("a", "b", split_to_table("d", lit(" ")))
    assert table_function1._plan.plan_state[PlanState.PLAN_HEIGHT] == 8

    filter3 = join2.where(col("a") > 1)
    filter4 = join2.where(col("a") < 1)
    if sql_simplifier_enabled:
        assert (
            filter3._plan.plan_state[PlanState.PLAN_HEIGHT]
            == filter4._plan.plan_state[PlanState.PLAN_HEIGHT]
            == 6
        )
    else:
        assert (
            filter3._plan.plan_state[PlanState.PLAN_HEIGHT]
            == filter4._plan.plan_state[PlanState.PLAN_HEIGHT]
            == 7
        )

    union1 = filter3.union_all_by_name(filter4)
    if sql_simplifier_enabled:
        assert union1._plan.plan_state[PlanState.PLAN_HEIGHT] == 8
    else:
        assert union1._plan.plan_state[PlanState.PLAN_HEIGHT] == 9


def test_plan_num_duplicate_nodes_describe_query(session, temp_table):
    df1 = session.sql(f"describe table {temp_table}")
    with session.query_history() as query_history:
        assert df1._plan.plan_state[PlanState.NUM_CTE_NODES] == 0
    assert len(query_history.queries) == 0
    with session.query_history() as query_history:
        df1.collect()
    assert len(query_history.queries) == 1


def test_create_scoped_temp_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name, "num int, str string(8)")
        session.sql(
            f"insert into {table_name} values(1, 'a'),(2, 'b'),(3, 'c')"
        ).collect()
        df = session.table(table_name)
        temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        assert (
            session._plan_builder.save_as_table(
                table_name=[temp_table_name],
                column_names=None,
                mode=SaveMode.ERROR_IF_EXISTS,
                table_type="temp",
                clustering_keys=None,
                comment=None,
                enable_schema_evolution=None,
                data_retention_time=None,
                max_data_extension_time=None,
                change_tracking=None,
                copy_grants=False,
                child=df._plan,
                source_plan=None,
                use_scoped_temp_objects=True,
                creation_source=TableCreationSource.CACHE_RESULT,
                child_attributes=df._plan.attributes,
            )
            .queries[0]
            .sql
            == f' CREATE  SCOPED TEMPORARY  TABLE {temp_table_name}("NUM" BIGINT, "STR" STRING(8))  '
        )
        assert (
            session._plan_builder.save_as_table(
                table_name=[temp_table_name],
                column_names=None,
                mode=SaveMode.ERROR_IF_EXISTS,
                table_type="temp",
                clustering_keys=None,
                comment=None,
                enable_schema_evolution=None,
                data_retention_time=None,
                max_data_extension_time=None,
                change_tracking=None,
                copy_grants=False,
                child=df._plan,
                source_plan=None,
                use_scoped_temp_objects=False,
                creation_source=TableCreationSource.CACHE_RESULT,
                child_attributes=df._plan.attributes,
            )
            .queries[0]
            .sql
            == f' CREATE  TEMPORARY  TABLE {temp_table_name}("NUM" BIGINT, "STR" STRING(8))  '
        )
        inner_select_sql = (
            f" SELECT * FROM {table_name}"
            if session._sql_simplifier_enabled
            else f" SELECT * FROM ({table_name})"
        )
        assert Utils.normalize_sql(
            session._plan_builder.save_as_table(
                table_name=[temp_table_name],
                column_names=None,
                mode=SaveMode.ERROR_IF_EXISTS,
                table_type="temp",
                clustering_keys=None,
                comment=None,
                enable_schema_evolution=None,
                data_retention_time=None,
                max_data_extension_time=None,
                change_tracking=None,
                copy_grants=False,
                child=df._plan,
                source_plan=None,
                use_scoped_temp_objects=False,
                creation_source=TableCreationSource.LARGE_QUERY_BREAKDOWN,
                child_attributes=None,
            )
            .queries[0]
            .sql
        ) == Utils.normalize_sql(
            f"CREATE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM ({inner_select_sql} )"
        )
        expected_sql = f' CREATE  TEMPORARY  TABLE  {temp_table_name}("NUM" BIGINT, "STR" STRING(8))'
        assert expected_sql in (
            session._plan_builder.save_as_table(
                table_name=[temp_table_name],
                column_names=None,
                mode=SaveMode.ERROR_IF_EXISTS,
                table_type="temporary",
                clustering_keys=None,
                comment=None,
                enable_schema_evolution=None,
                data_retention_time=None,
                max_data_extension_time=None,
                change_tracking=None,
                copy_grants=False,
                child=df._plan,
                source_plan=None,
                use_scoped_temp_objects=True,
                creation_source=TableCreationSource.OTHERS,
                child_attributes=df._plan.attributes,
            )
            .queries[0]
            .sql
        )
        expected_sql = (
            f" CREATE  SCOPED TEMPORARY  TABLE  {temp_table_name}    AS  SELECT"
        )
        assert expected_sql in (
            session._plan_builder.save_as_table(
                table_name=[temp_table_name],
                column_names=None,
                mode=SaveMode.ERROR_IF_EXISTS,
                table_type="temporary",
                clustering_keys=None,
                comment=None,
                enable_schema_evolution=None,
                data_retention_time=None,
                max_data_extension_time=None,
                change_tracking=None,
                copy_grants=False,
                child=df._plan,
                source_plan=None,
                use_scoped_temp_objects=True,
                creation_source=TableCreationSource.LARGE_QUERY_BREAKDOWN,
                child_attributes=[],
            )
            .queries[0]
            .sql
        )
        with pytest.raises(
            ValueError,
            match="Internally generated tables must be called with mode ERROR_IF_EXISTS",
        ):
            session._plan_builder.save_as_table(
                table_name=[temp_table_name],
                column_names=None,
                mode=SaveMode.APPEND,
                table_type="temporary",
                clustering_keys=None,
                comment=None,
                enable_schema_evolution=None,
                data_retention_time=None,
                max_data_extension_time=None,
                change_tracking=None,
                copy_grants=False,
                child=df._plan,
                source_plan=None,
                use_scoped_temp_objects=True,
                creation_source=TableCreationSource.CACHE_RESULT,
                child_attributes=df._plan.attributes,
            )

        with pytest.raises(
            ValueError,
            match="child attribute must be provided when table creation source is not large query breakdown",
        ):
            session._plan_builder.save_as_table(
                table_name=[temp_table_name],
                column_names=None,
                mode=SaveMode.ERROR_IF_EXISTS,
                table_type="temporary",
                clustering_keys=None,
                comment=None,
                enable_schema_evolution=None,
                data_retention_time=None,
                max_data_extension_time=None,
                change_tracking=None,
                copy_grants=False,
                child=df._plan,
                source_plan=None,
                use_scoped_temp_objects=True,
                creation_source=TableCreationSource.OTHERS,
                child_attributes=None,
            )

    finally:
        Utils.drop_table(session, table_name)


def test_invalid_identifier_error_message(session):
    df = session.create_dataframe([[1, 2, 3]], schema=['"abc"', '"abd"', '"def"'])
    with pytest.raises(SnowparkSQLException) as ex:
        df.select("abc").collect()
    assert ex.value.sql_error_code == 904
    assert "invalid identifier 'ABC'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abc\"'?" in str(ex.value)

    with pytest.raises(SnowparkSQLException) as ex:
        df.select("_ab").collect()
    assert "invalid identifier '_AB'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abd\"' or '\"abc\"'?" in str(ex.value)

    with pytest.raises(SnowparkSQLException) as ex:
        df.select('"abC"').collect()
    assert "invalid identifier '\"abC\"'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean" not in str(ex.value)

    df = session.create_dataframe([list(range(20))], schema=[str(i) for i in range(20)])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("20").collect()

    # Describing an invalid schema has correct context
    df = session.create_dataframe([1, 2, 3], schema=["A"])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("B").schema
    assert "There are existing quoted column identifiers: ['\"A\"']" in str(ex.value)

    # session.sql does not have schema query so no context is available
    with pytest.raises(SnowparkSQLException, match="invalid identifier 'B'") as ex:
        session.sql(
            """SELECT "B" FROM ( SELECT $1 AS "A" FROM  VALUES (1 :: INT))"""
        ).select("C")
    assert "There are existing quoted column identifiers" not in str(ex.value)
