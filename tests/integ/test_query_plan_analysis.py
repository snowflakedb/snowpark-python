#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


from typing import Dict

import pytest

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_EXCEPT,
    SET_INTERSECT,
    SET_UNION,
    SET_UNION_ALL,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import avg, col, lit, seq1, table_function, uniform
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


paramList = [False, True]


@pytest.fixture(params=paramList, autouse=True)
def setup(request, session):
    is_simplifier_enabled = session._sql_simplifier_enabled
    large_query_breakdown_enabled = session.large_query_breakdown_enabled
    session.large_query_breakdown_enabled = request.param
    session._sql_simplifier_enabled = True
    yield
    session._sql_simplifier_enabled = is_simplifier_enabled
    session.large_query_breakdown_enabled = large_query_breakdown_enabled


@pytest.fixture(scope="module")
def sample_table(session):
    table_name = Utils.random_table_name()
    Utils.create_table(
        session, table_name, "a int, b int, c int, d int", is_temporary=True
    )
    session._run_query(
        f"insert into {table_name}(a, b, c, d) values " "(1, 2, 3, 4), (5, 6, 7, 8)"
    )
    yield table_name
    Utils.drop_table(session, table_name)


def get_cumulative_node_complexity(df: DataFrame) -> Dict[str, int]:
    return df._plan.cumulative_node_complexity


def assert_df_subtree_query_complexity(
    df: DataFrame, estimate: Dict[PlanNodeCategory, int]
):
    assert (
        get_cumulative_node_complexity(df) == estimate
    ), f"query = {df.queries['queries'][-1]}"


def test_create_dataframe_from_values(session: Session):
    df1 = session.create_dataframe([[1], [2], [3]], schema=["a"])
    #  SELECT "A" FROM ( SELECT $1 AS "A" FROM  VALUES (1 :: INT), (2 :: INT), (3 :: INT))
    assert_df_subtree_query_complexity(
        df1, {PlanNodeCategory.LITERAL: 3, PlanNodeCategory.COLUMN: 2}
    )

    df2 = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
    #  SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM  VALUES (1 :: INT, 2 :: INT), (3 :: INT, 4 :: INT), (5 :: INT, 6 :: INT))
    assert_df_subtree_query_complexity(
        df2, {PlanNodeCategory.LITERAL: 6, PlanNodeCategory.COLUMN: 4}
    )


def test_session_table(session: Session, sample_table: str):
    # select * from sample_table
    df = session.table(sample_table)
    assert_df_subtree_query_complexity(df, {PlanNodeCategory.COLUMN: 1})


def test_range_statement(session: Session):
    df = session.range(1, 5, 2)
    # SELECT ( ROW_NUMBER()  OVER ( ORDER BY  SEQ8() ) -  1 ) * (2) + (1) AS id FROM ( TABLE (GENERATOR(ROWCOUNT => 2)))
    assert_df_subtree_query_complexity(
        df,
        {
            PlanNodeCategory.COLUMN: 1,
            PlanNodeCategory.LITERAL: 3,
            PlanNodeCategory.FUNCTION: 3,
            PlanNodeCategory.ORDER_BY: 1,
            PlanNodeCategory.WINDOW: 1,
        },
    )


def test_literal_complexity_for_snowflake_values(session: Session):
    from snowflake.snowpark._internal.analyzer import analyzer

    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    assert_df_subtree_query_complexity(
        df1, {PlanNodeCategory.COLUMN: 4, PlanNodeCategory.LITERAL: 4}
    )

    try:
        original_threshold = analyzer.ARRAY_BIND_THRESHOLD
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df2 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        # SELECT "A", "B" from (SELECT * FROM TEMP_TABLE)
        assert_df_subtree_query_complexity(df2, {PlanNodeCategory.COLUMN: 3})
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold


def test_generator_table_function(session: Session):
    df1 = session.generator(
        seq1(1).as_("seq"), uniform(1, 10, 2).as_("uniform"), rowcount=150
    )
    assert_df_subtree_query_complexity(
        df1,
        {
            PlanNodeCategory.COLUMN: 2,
            PlanNodeCategory.FUNCTION: 1,
            PlanNodeCategory.LITERAL: 1,
        },
    )

    df2 = df1.order_by("seq")
    # adds SELECT * from () ORDER BY seq ASC NULLS FIRST
    assert_df_subtree_query_complexity(
        df2,
        sum_node_complexities(
            get_cumulative_node_complexity(df1),
            {
                PlanNodeCategory.ORDER_BY: 1,
                PlanNodeCategory.COLUMN: 1,
                PlanNodeCategory.OTHERS: 1,
            },
        ),
    )


def test_join_table_function(session: Session):
    df1 = session.sql(
        "select 'James' as name, 'address1 address2 address3' as addresses"
    )
    # SelectSQL chooses num active columns as the best estimate
    assert_df_subtree_query_complexity(df1, {PlanNodeCategory.COLUMN: 1})

    split_to_table = table_function("split_to_table")

    # SELECT "SEQ", "INDEX", "VALUE" FROM (
    #  SELECT T_RIGHT."SEQ", T_RIGHT."INDEX", T_RIGHT."VALUE" FROM
    #      (select 'James' as name, 'address1 address2 address3' as addresses) AS T_LEFT
    #  JOIN  TABLE (split_to_table("ADDRESSES", ' ') ) AS T_RIGHT)
    df2 = df1.select(split_to_table(col("addresses"), lit(" ")))
    assert_df_subtree_query_complexity(
        df2,
        {
            PlanNodeCategory.COLUMN: 8,
            PlanNodeCategory.JOIN: 1,
            PlanNodeCategory.FUNCTION: 1,
            PlanNodeCategory.LITERAL: 1,
        },
    )

    #  SELECT T_LEFT.*, T_RIGHT.* FROM (select 'James' as name, 'address1 address2 address3' as addresses) AS T_LEFT
    #   JOIN  TABLE (split_to_table("ADDRESS", ' ')  OVER (PARTITION BY "LAST_NAME"  ORDER BY "FIRST_NAME" ASC NULLS FIRST)) AS T_RIGHT
    df3 = df1.join_table_function(
        split_to_table(col("address"), lit(" ")).over(
            partition_by="last_name", order_by="first_name"
        )
    )
    assert_df_subtree_query_complexity(
        df3,
        {
            PlanNodeCategory.COLUMN: 6,
            PlanNodeCategory.JOIN: 1,
            PlanNodeCategory.FUNCTION: 1,
            PlanNodeCategory.LITERAL: 1,
            PlanNodeCategory.PARTITION_BY: 1,
            PlanNodeCategory.ORDER_BY: 1,
            PlanNodeCategory.WINDOW: 1,
            PlanNodeCategory.OTHERS: 1,
        },
    )


@pytest.mark.parametrize(
    "set_operator", [SET_UNION, SET_UNION_ALL, SET_EXCEPT, SET_INTERSECT]
)
def test_set_operators(session: Session, sample_table: str, set_operator: str):
    df1 = session.table(sample_table)
    df2 = session.table(sample_table)
    if set_operator == SET_UNION:
        df = df1.union(df2)
    elif set_operator == SET_UNION_ALL:
        df = df1.union_all(df2)
    elif set_operator == SET_EXCEPT:
        df = df1.except_(df2)
    else:
        df = df1.intersect(df2)

    # ( SELECT  *  FROM SNOWPARK_TEMP_TABLE_9DJO2Y35IT) set_operator ( SELECT  *  FROM SNOWPARK_TEMP_TABLE_9DJO2Y35IT)
    assert_df_subtree_query_complexity(
        df, {PlanNodeCategory.COLUMN: 2, PlanNodeCategory.SET_OPERATION: 1}
    )


def test_agg(session: Session, sample_table: str):
    df = session.table(sample_table)
    df1 = df.agg(avg("a"))
    df2 = df.agg(avg("a") + 1)
    df3 = df.agg(avg("a"), avg(col("b") + lit(1)).as_("avg_b"))
    df4 = df.group_by(["a", "b"]).agg(avg("c"))

    #  SELECT avg("A") AS "AVG(A)" FROM ( SELECT  *  FROM sample_table) LIMIT 1
    assert_df_subtree_query_complexity(
        df1,
        {
            PlanNodeCategory.COLUMN: 2,
            PlanNodeCategory.LOW_IMPACT: 1,
            PlanNodeCategory.FUNCTION: 1,
        },
    )
    # SELECT (avg("A") + 1 :: INT) AS "ADD(AVG(A), LITERAL())" FROM ( SELECT  *  FROM sample_table) LIMIT 1
    assert_df_subtree_query_complexity(
        df2,
        {
            PlanNodeCategory.COLUMN: 2,
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.FUNCTION: 1,
            PlanNodeCategory.LITERAL: 1,
        },
    )
    # SELECT avg("A") AS "AVG(A)", avg(("B" + 1 :: INT)) AS "AVG_B" FROM ( SELECT  *  FROM sample_table) LIMIT 1
    assert_df_subtree_query_complexity(
        df3,
        {
            PlanNodeCategory.COLUMN: 3,
            PlanNodeCategory.LOW_IMPACT: 2,
            PlanNodeCategory.FUNCTION: 2,
            PlanNodeCategory.LITERAL: 1,
        },
    )
    # SELECT "A", "B", avg("C") AS "AVG(C)" FROM ( SELECT  *  FROM SNOWPARK_TEMP_TABLE_EV1NO4AID6) GROUP BY "A", "B"
    assert_df_subtree_query_complexity(
        df4,
        {
            PlanNodeCategory.COLUMN: 6,
            PlanNodeCategory.GROUP_BY: 1,
            PlanNodeCategory.FUNCTION: 1,
        },
    )


def test_window_function(session: Session):
    window1 = (
        Window.partition_by("value").order_by("key").rows_between(Window.CURRENT_ROW, 2)
    )
    window2 = Window.order_by(col("key").desc()).range_between(
        Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING
    )
    df = session.create_dataframe(
        [(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"]
    )
    table_name = Utils.random_table_name()
    try:
        df.write.save_as_table(table_name, table_type="temp", mode="overwrite")

        df1 = session.table(table_name).select(
            avg("value").over(window1).as_("window1")
        )
        # SELECT avg("VALUE") OVER (PARTITION BY "VALUE"  ORDER BY "KEY" ASC NULLS FIRST  ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING  ) AS "WINDOW1" FROM table_name
        assert_df_subtree_query_complexity(
            df1,
            {
                PlanNodeCategory.PARTITION_BY: 1,
                PlanNodeCategory.ORDER_BY: 1,
                PlanNodeCategory.WINDOW: 1,
                PlanNodeCategory.FUNCTION: 1,
                PlanNodeCategory.COLUMN: 4,
                PlanNodeCategory.LITERAL: 1,
                PlanNodeCategory.LOW_IMPACT: 2,
                PlanNodeCategory.OTHERS: 1,
            },
        )

        # SELECT avg("VALUE") OVER (  ORDER BY "KEY" DESC NULLS LAST  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS "WINDOW2" FROM (
        #   SELECT avg("VALUE") OVER (PARTITION BY "VALUE"  ORDER BY "KEY" ASC NULLS FIRST  ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING  ) AS "WINDOW1" FROM table_name)
        df2 = df1.select(avg("value").over(window2).as_("window2"))
        assert_df_subtree_query_complexity(
            df2,
            sum_node_complexities(
                get_cumulative_node_complexity(df1),
                {
                    PlanNodeCategory.ORDER_BY: 1,
                    PlanNodeCategory.WINDOW: 1,
                    PlanNodeCategory.FUNCTION: 1,
                    PlanNodeCategory.COLUMN: 2,
                    PlanNodeCategory.LOW_IMPACT: 3,
                    PlanNodeCategory.OTHERS: 1,
                },
            ),
        )
    finally:
        Utils.drop_table(session, table_name)


def test_join_statement(session: Session, sample_table: str):
    # SELECT * FROM table
    df1 = session.table(sample_table)
    assert_df_subtree_query_complexity(df1, {PlanNodeCategory.COLUMN: 1})
    # SELECT A, B, E FROM (SELECT $1 AS "A", $2 AS "B", $3 AS "E" FROM  VALUES (1 :: INT, 2 :: INT, 5 :: INT), (3 :: INT, 4 :: INT, 9 :: INT))
    df2 = session.create_dataframe([[1, 2, 5], [3, 4, 9]], schema=["a", "b", "e"])
    assert_df_subtree_query_complexity(
        df2, {PlanNodeCategory.COLUMN: 6, PlanNodeCategory.LITERAL: 6}
    )

    df3 = df1.join(df2)
    # SELECT  *  FROM (( SELECT "A" AS "l_fkl0_A", "B" AS "l_fkl0_B", "C" AS "C", "D" AS "D" FROM sample_table) AS SNOWPARK_LEFT
    # INNER JOIN (
    #   SELECT "A" AS "r_co85_A", "B" AS "r_co85_B", "E" AS "E" FROM (
    #       SELECT $1 AS "A", $2 AS "B", $3 AS "E" FROM  VALUES (1 :: INT, 2 :: INT, 5 :: INT), (3 :: INT, 4 :: INT, 9 :: INT))) AS SNOWPARK_RIGHT)
    assert_df_subtree_query_complexity(
        df3,
        {
            PlanNodeCategory.COLUMN: 11,
            PlanNodeCategory.LITERAL: 6,
            PlanNodeCategory.JOIN: 1,
        },
    )

    df4 = df1.join(df2, on=((df1["a"] == df2["a"]) & (df1["b"] == df2["b"])))
    # SELECT  *  FROM ((ch1) AS SNOWPARK_LEFT INNER JOIN ( ch2) AS SNOWPARK_RIGHT ON (("l_k7b8_A" = "r_e09m_A") AND ("l_k7b8_B" = "r_e09m_B")))
    assert_df_subtree_query_complexity(
        df4,
        sum_node_complexities(
            get_cumulative_node_complexity(df3),
            {PlanNodeCategory.COLUMN: 4, PlanNodeCategory.LOW_IMPACT: 3},
        ),
    )

    df5 = df1.join(df2, using_columns=["a", "b"])
    # SELECT  *  FROM ( (ch1) AS SNOWPARK_LEFT INNER JOIN (ch2) AS SNOWPARK_RIGHT USING (a, b))
    assert_df_subtree_query_complexity(
        df5,
        sum_node_complexities(
            get_cumulative_node_complexity(df3), {PlanNodeCategory.COLUMN: 2}
        ),
    )


def test_pivot(session: Session):
    try:
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [
                (1, 10000, "JAN"),
                (1, 400, "JAN"),
                (2, 4500, "JAN"),
                (2, 35000, "JAN"),
                (1, 5000, "FEB"),
                (1, 3000, "FEB"),
                (2, 200, "FEB"),
            ],
            schema=["empid", "amount", "month"],
        ).write.save_as_table(table_name, table_type="temp")

        df_pivot1 = (
            session.table(table_name).pivot("month", ["JAN", "FEB"]).sum("amount")
        )
        #  SELECT  *  FROM ( SELECT  *  FROM table_name) PIVOT (sum("AMOUNT") FOR "MONTH" IN ('JAN', 'FEB'))
        assert_df_subtree_query_complexity(
            df_pivot1,
            {
                PlanNodeCategory.PIVOT: 1,
                PlanNodeCategory.COLUMN: 4,
                PlanNodeCategory.LITERAL: 2,
                PlanNodeCategory.FUNCTION: 1,
            },
        )

        df_pivot2 = (
            session.table(table_name)
            .pivot("month", ["JAN", "FEB", "MARCH"])
            .sum("amount")
        )
        #  SELECT  *  FROM ( SELECT  *  FROM table_name) PIVOT (sum("AMOUNT") FOR "MONTH" IN ('JAN', 'FEB', 'MARCH'))
        assert_df_subtree_query_complexity(
            df_pivot2,
            {
                PlanNodeCategory.PIVOT: 1,
                PlanNodeCategory.COLUMN: 4,
                PlanNodeCategory.LITERAL: 3,
                PlanNodeCategory.FUNCTION: 1,
            },
        )
    finally:
        Utils.drop_table(session, table_name)


def test_unpivot(session: Session):
    try:
        sales_for_month = Utils.random_table_name()
        session.create_dataframe(
            [
                (1, "electronics", 100, 200),
                (2, "clothes", 100, 300),
            ],
            schema=["empid", "dept", "jan", "feb"],
        ).write.save_as_table(sales_for_month, table_type="temp")

        df_unpivot1 = session.table(sales_for_month).unpivot(
            "sales", "month", ["jan", "feb"]
        )
        #  SELECT  *  FROM ( SELECT  *  FROM (sales_for_month)) UNPIVOT (sales FOR month IN ("JAN", "FEB"))
        assert_df_subtree_query_complexity(
            df_unpivot1,
            {PlanNodeCategory.UNPIVOT: 1, PlanNodeCategory.COLUMN: 6},
        )
    finally:
        Utils.drop_table(session, sales_for_month)


def test_sample(session: Session, sample_table):
    df = session.table(sample_table).select("*")
    df_sample_frac = df.sample(0.5)
    # SELECT  *  FROM ( SELECT  *  FROM (sample_table)) SAMPLE (50.0)
    assert_df_subtree_query_complexity(
        df_sample_frac,
        {
            PlanNodeCategory.SAMPLE: 1,
            PlanNodeCategory.LITERAL: 1,
            PlanNodeCategory.COLUMN: 2,
        },
    )

    df_sample_rows = df.sample(n=1)
    # SELECT  *  FROM ( SELECT  *  FROM (sample_table)) SAMPLE (1 ROWS)
    assert_df_subtree_query_complexity(
        df_sample_rows,
        {
            PlanNodeCategory.SAMPLE: 1,
            PlanNodeCategory.LITERAL: 1,
            PlanNodeCategory.COLUMN: 2,
        },
    )


def test_select_statement_with_multiple_operations(session: Session, sample_table: str):
    df = session.table(sample_table)

    # add select
    # SELECT "A", "B", "C", "D" FROM sample_table
    # note that column stat is 5 even though selected columns is 4. This is because we count 1 column
    # from select * from sample_table which is flattened out. This is a known limitation but is okay
    # since we are not off my much
    df1 = df.select(df["*"])
    assert_df_subtree_query_complexity(df1, {PlanNodeCategory.COLUMN: 5})

    # SELECT "A", "B", "C" FROM sample_table
    df2 = df1.select("a", "b", "c")
    assert_df_subtree_query_complexity(df2, {PlanNodeCategory.COLUMN: 4})

    # 1 less active column
    df3 = df2.select("b", "c")
    assert_df_subtree_query_complexity(df3, {PlanNodeCategory.COLUMN: 3})

    # add sort
    # for additional ORDER BY "B" ASC NULLS FIRST
    df4 = df3.sort(col("b").asc())
    assert_df_subtree_query_complexity(
        df4,
        sum_node_complexities(
            get_cumulative_node_complexity(df3),
            {
                PlanNodeCategory.COLUMN: 1,
                PlanNodeCategory.ORDER_BY: 1,
                PlanNodeCategory.OTHERS: 1,
            },
        ),
    )

    # for additional ,"C" ASC NULLS FIRST
    df5 = df4.sort(col("c").desc())
    assert_df_subtree_query_complexity(
        df5,
        sum_node_complexities(
            get_cumulative_node_complexity(df4),
            {PlanNodeCategory.COLUMN: 1, PlanNodeCategory.OTHERS: 1},
        ),
    )

    # add filter
    # for WHERE ("B" > 2)
    df6 = df5.filter(col("b") > 2)
    assert_df_subtree_query_complexity(
        df6,
        sum_node_complexities(
            get_cumulative_node_complexity(df5),
            {
                PlanNodeCategory.FILTER: 1,
                PlanNodeCategory.COLUMN: 1,
                PlanNodeCategory.LITERAL: 1,
                PlanNodeCategory.LOW_IMPACT: 1,
            },
        ),
    )

    # for filter - AND ("C" > 3)
    df7 = df6.filter(col("c") > 3)
    assert_df_subtree_query_complexity(
        df7,
        sum_node_complexities(
            get_cumulative_node_complexity(df6),
            {
                PlanNodeCategory.COLUMN: 1,
                PlanNodeCategory.LITERAL: 1,
                PlanNodeCategory.LOW_IMPACT: 2,
            },
        ),
    )

    # add set operations
    df8 = df3.union_all(df4).union_all(df5)
    assert_df_subtree_query_complexity(
        df8,
        sum_node_complexities(
            *(get_cumulative_node_complexity(df) for df in [df3, df4, df5]),
            {PlanNodeCategory.SET_OPERATION: 2},
        ),
    )

    # + 2 for 2 unions, 30 for sum ob individual df complexity
    df9 = df8.union_all(df6).union_all(df7)
    assert_df_subtree_query_complexity(
        df9,
        sum_node_complexities(
            *(get_cumulative_node_complexity(df) for df in [df6, df7, df8]),
            {PlanNodeCategory.SET_OPERATION: 2},
        ),
    )

    # for limit
    df10 = df9.limit(2)
    assert_df_subtree_query_complexity(
        df10,
        sum_node_complexities(
            get_cumulative_node_complexity(df9), {PlanNodeCategory.LOW_IMPACT: 1}
        ),
    )

    # for offset
    df11 = df9.limit(3, offset=1)
    assert_df_subtree_query_complexity(
        df11,
        sum_node_complexities(
            get_cumulative_node_complexity(df9), {PlanNodeCategory.LOW_IMPACT: 2}
        ),
    )
