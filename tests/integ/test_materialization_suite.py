#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

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


@pytest.fixture(autouse=True)
def setup(session):
    is_simplifier_enabled = session._sql_simplifier_enabled
    session._sql_simplifier_enabled = True
    yield
    session._sql_simplifier_enabled = is_simplifier_enabled


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


def get_subtree_query_complexity(df: DataFrame) -> int:
    return df._plan.subtree_query_complexity


def assert_df_subtree_query_complexity(df: DataFrame, estimate: int):
    assert (
        get_subtree_query_complexity(df) == estimate
    ), f"query = {df.queries['queries'][-1]}"


def test_create_dataframe_from_values(session: Session):
    df1 = session.create_dataframe([[1], [2], [3]], schema=["a"])
    #  SELECT "A" FROM ( SELECT $1 AS "A" FROM  VALUES (1 :: INT), (2 :: INT), (3 :: INT))
    assert_df_subtree_query_complexity(df1, 5)

    df2 = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
    #  SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM  VALUES (1 :: INT, 2 :: INT), (3 :: INT, 4 :: INT), (5 :: INT, 6 :: INT))
    assert_df_subtree_query_complexity(df2, 10)


def test_session_table(session: Session, sample_table: str):
    df = session.table(sample_table)
    # select * from sample_table
    assert_df_subtree_query_complexity(df, 1)


def test_range_statement(session: Session):
    df = session.range(1, 5, 2)
    # SELECT ( ROW_NUMBER()  OVER ( ORDER BY  SEQ8() ) -  1 ) * (2) + (1) AS id FROM ( TABLE (GENERATOR(ROWCOUNT => 2)))
    assert_df_subtree_query_complexity(df, 6)


def test_generator_table_function(session: Session):
    df1 = session.generator(
        seq1(1).as_("seq"), uniform(1, 10, 2).as_("uniform"), rowcount=150
    )
    assert_df_subtree_query_complexity(df1, 5)

    df2 = df1.order_by("seq")
    # adds SELECT * from () ORDER BY seq ASC NULLS FIRST
    assert_df_subtree_query_complexity(
        df2, df1._select_statement.subtree_query_complexity + 3
    )


def test_join_table_function(session: Session):
    df1 = session.sql(
        "select 'James' as name, 'address1 address2 address3' as addresses"
    )
    # SelectSQL chooses num active columns as the best estimate
    # assert_df_subtree_query_complexity(df1, 2)

    split_to_table = table_function("split_to_table")
    df2 = df1.select(split_to_table(col("addresses"), lit(" ")))
    # +3 SELECT "SEQ", "INDEX", "VALUE" FROM (
    # +3  SELECT T_RIGHT."SEQ", T_RIGHT."INDEX", T_RIGHT."VALUE" FROM
    # +2      (select 'James' as name, 'address1 address2 address3' as addresses) AS T_LEFT
    # +3  JOIN  TABLE (split_to_table("ADDRESSES", ' ') ) AS T_RIGHT)
    assert_df_subtree_query_complexity(df2, 11)


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
    assert_df_subtree_query_complexity(df, 3)


def test_agg(session: Session, sample_table: str):
    df = session.table(sample_table)
    df1 = df.agg(avg("a"))
    df2 = df.agg(avg("a") + 1)
    df3 = df.agg(avg("a"), avg("b" + lit(1)).as_("avg_b"))

    #  SELECT avg("A") AS "AVG(A)" FROM ( SELECT  *  FROM sample_table) LIMIT 1
    assert_df_subtree_query_complexity(df1, 3)
    # SELECT (avg("A") + 1 :: INT) AS "ADD(AVG(A), LITERAL())" FROM ( SELECT  *  FROM sample_table) LIMIT 1
    assert_df_subtree_query_complexity(df2, 5)
    # SELECT avg("A") AS "AVG(A)", avg(('b' + 1 :: INT)) AS "AVG_B" FROM ( SELECT  *  FROM sample_table) LIMIT 1
    assert_df_subtree_query_complexity(df3, 6)


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

    df1 = df.select(avg("value").over(window1).as_("window1"))
    # SELECT avg("VALUE") OVER (PARTITION BY "VALUE"  ORDER BY "KEY" ASC NULLS FIRST  ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING  ) AS "WINDOW1" FROM ( base_df)
    assert_df_subtree_query_complexity(df1, get_subtree_query_complexity(df) + 9)

    # SELECT avg("VALUE") OVER (  ORDER BY "KEY" DESC NULLS LAST  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS "WINDOW2" FROM ( base df)
    df2 = df1.select(avg("value").over(window2).as_("window2"))
    assert_df_subtree_query_complexity(df2, get_subtree_query_complexity(df1) + 9)


def test_join_statement(session: Session, sample_table: str):
    # SELECT * FROM table
    df1 = session.table(sample_table)
    assert_df_subtree_query_complexity(df1, 1)
    # SELECT A, B, E FROM (SELECT $1 AS "A", $2 AS "B", $3 AS "E" FROM  VALUES (1 :: INT, 2 :: INT, 5 :: INT), (3 :: INT, 4 :: INT, 9 :: INT))
    df2 = session.create_dataframe([[1, 2, 5], [3, 4, 9]], schema=["a", "b", "e"])
    assert_df_subtree_query_complexity(df2, 12)

    df3 = df1.join(df2)
    # +3 SELECT  *  FROM ((ch1) AS SNOWPARK_LEFT INNER JOIN (ch2) AS SNOWPARK_RIGHT)
    # +4 ch1 = SELECT "A" AS "l_p8bm_A", "B" AS "l_p8bm_B", "C" AS "C", "D" AS "D" FROM (df1)
    # +0 ch2 = SELECT "A" AS "r_2og4_A", "B" AS "r_2og4_B", "E" AS "E" FROM (SELECT $1 AS "A", $2 AS "B", $3 AS "E" FROM  VALUES ())
    # ch2 is a re-write flattened version of df2 with aliases
    assert_df_subtree_query_complexity(df3, 13 + 7)

    df4 = df1.join(df2, on=((df1["a"] == df2["a"]) & (df1["b"] == df2["b"])))
    # SELECT  *  FROM ((ch1) AS SNOWPARK_LEFT INNER JOIN ( ch2) AS SNOWPARK_RIGHT ON (("l_k7b8_A" = "r_e09m_A") AND ("l_k7b8_B" = "r_e09m_B")))
    assert_df_subtree_query_complexity(df4, get_subtree_query_complexity(df3) + 7)

    df5 = df1.join(df2, using_columns=["a", "b"])
    # SELECT  *  FROM ( (ch1) AS SNOWPARK_LEFT INNER JOIN (ch2) AS SNOWPARK_RIGHT USING (a, b))
    assert_df_subtree_query_complexity(df5, get_subtree_query_complexity(df3) + 3)


def test_pivot_and_unpivot(session: Session):
    try:
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

        df_pivot1 = (
            session.table("monthly_sales").pivot("month", ["JAN", "FEB"]).sum("amount")
        )
        #  SELECT  *  FROM ( SELECT  *  FROM monthly_sales) PIVOT (sum("AMOUNT") FOR "MONTH" IN ('JAN', 'FEB'))
        assert_df_subtree_query_complexity(df_pivot1, 8)

        df_pivot2 = (
            session.table("monthly_sales")
            .pivot("month", ["JAN", "FEB", "MARCH"])
            .sum("amount")
        )
        #  SELECT  *  FROM ( SELECT  *  FROM monthly_sales) PIVOT (sum("AMOUNT") FOR "MONTH" IN ('JAN', 'FEB', 'MARCH'))
        assert_df_subtree_query_complexity(df_pivot2, 9)

        session.sql(
            """create or replace temp table sales_for_month(empid int, dept varchar, jan int, feb int)
            as select * from values
            (1, 'electronics', 100, 200),
            (2, 'clothes', 100, 300)"""
        ).collect()
        df_unpivot1 = session.table("sales_for_month").unpivot(
            "sales", "month", ["jan", "feb"]
        )
        #  SELECT  *  FROM ( SELECT  *  FROM (sales_for_month)) UNPIVOT (sales FOR month IN ("JAN", "FEB"))
        assert_df_subtree_query_complexity(df_unpivot1, 7)
    finally:
        Utils.drop_table(session, "monthly_sales")
        Utils.drop_table(session, "sales_for_month")


def test_sample(session: Session, sample_table):
    df = session.table(sample_table)
    df_sample_frac = df.sample(0.5)
    # SELECT  *  FROM ( SELECT  *  FROM (sample_table)) SAMPLE (50.0)
    assert_df_subtree_query_complexity(df_sample_frac, 3)

    df_sample_rows = df.sample(n=1)
    # SELECT  *  FROM ( SELECT  *  FROM (sample_table)) SAMPLE (1 ROWS)
    assert_df_subtree_query_complexity(df_sample_rows, 4)


@pytest.mark.parametrize("source_from_table", [True, False])
def test_select_statement_subtree_complexity_estimate(
    session: Session, sample_table: str, source_from_table: bool
):
    if source_from_table:
        df1 = session.table(sample_table)
    else:
        df1 = session.create_dataframe(
            [[1, 2, 3, 4], [5, 6, 7, 8]], schema=["a", "b", "c", "d"]
        )

    assert_df_subtree_query_complexity(df1, 1 if source_from_table else 16)

    # add select
    # +3 for column
    df2 = df1.select("a", "b", "c")
    assert_df_subtree_query_complexity(df2, 4 if source_from_table else 15)

    # +2 for column (1 less active column)
    df3 = df2.select("b", "c")
    assert_df_subtree_query_complexity(df3, 3 if source_from_table else 14)

    # add sort
    # +3 for additional ORDER BY "B" ASC NULLS FIRST
    df4 = df3.sort(col("b").asc())
    assert_df_subtree_query_complexity(df4, 3 + get_subtree_query_complexity(df3))

    # +3 for additional ,"C" ASC NULLS FIRST
    df5 = df4.sort(col("c").desc())
    assert_df_subtree_query_complexity(df5, 2 + get_subtree_query_complexity(df4))

    # add filter
    # +4 for WHERE ("B" > 2)
    df6 = df5.filter(col("b") > 2)
    assert_df_subtree_query_complexity(df6, 4 + get_subtree_query_complexity(df5))

    # +4 for filter - AND ("C" > 3)
    df7 = df6.filter(col("c") > 3)
    assert_df_subtree_query_complexity(df7, 4 + get_subtree_query_complexity(df6))

    # add set operations
    # +2 for 2 unions, 12 for sum of individual df complexity
    df8 = df3.union_all(df4).union_all(df5)
    assert_df_subtree_query_complexity(
        df8, 2 + sum(get_subtree_query_complexity(df) for df in [df3, df4, df5])
    )

    # + 2 for 2 unions, 30 for sum ob individual df complexity
    df9 = df8.union_all(df6).union_all(df7)
    assert_df_subtree_query_complexity(
        df9, 2 + sum(get_subtree_query_complexity(df) for df in [df6, df7, df8])
    )

    # +1 for limit
    df10 = df9.limit(2)
    assert_df_subtree_query_complexity(df10, 1 + get_subtree_query_complexity(df9))
