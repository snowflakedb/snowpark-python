#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Row
from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark._internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import IntegerType, LongType
from tests.utils import Utils


def test_single_query(session_cnx):
    with session_cnx() as session:
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
            plans = session._Session__plan_builder
            table_plan = plans.table(table_name)
            project = plans.project(["num"], table_plan, None)

            assert len(project.queries) == 1
        finally:
            Utils.drop_table(session, table_name)


def test_multiple_queries(session_cnx):
    with session_cnx() as session:
        # unary query
        table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        queries = [
            Query(
                f"create or replace temporary table {table_name1} as select * from values(1::INT, 'a'::STRING),(2::INT, 'b'::STRING) as T(A,B)"
            ),
            Query(f"select * from {table_name1}"),
        ]
        attrs = [Attribute("A", IntegerType()), Attribute("B", IntegerType())]
        pkg = AnalyzerPackage()

        try:
            plan = SnowflakePlan(
                queries, pkg.schema_value_statement(attrs), None, None, session
            )
            plan1 = session._Session__plan_builder.project(["A"], plan, None)

            assert len(plan1.attributes()) == 1
            assert plan1.attributes()[0].name == '"A"'
            # is is always nullable
            assert plan1.attributes()[0].nullable
            # SF always returns Long Type
            assert type(plan1.attributes()[0].datatype) == LongType

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
                queries2, pkg.schema_value_statement(attrs2), None, None, session
            )
            plan3 = session._Session__plan_builder.set_operator(
                plan1, plan2, "UNION ALL", None
            )

            assert len(plan3.attributes()) == 1
            assert plan3.attributes()[0].name == '"A"'
            assert plan3.attributes()[0].nullable
            assert type(plan3.attributes()[0].datatype) == LongType

            res2 = session._conn.execute(plan3)
            res2.sort(key=lambda x: x[0])
            assert res2 == [Row(1), Row(2), Row(3), Row(4)]
        finally:
            Utils.drop_table(session, table_name2)
