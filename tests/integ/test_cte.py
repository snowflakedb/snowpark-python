#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
import re
from functools import reduce
from typing import List

import pytest

import snowflake.snowpark.functions as F
from snowflake.connector.options import installed_pandas
from snowflake.snowpark import Window
from snowflake.snowpark._internal.analyzer import analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import PlanQueryType
from snowflake.snowpark._internal.utils import (
    TEMP_OBJECT_NAME_PREFIX,
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import avg, col, lit, seq1, uniform, when_matched
from tests.integ.scala.test_dataframe_reader_suite import get_reader
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import IS_IN_STORED_PROC, IS_IN_STORED_PROC_LOCALFS, TestFiles, Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="CTE is a SQL feature",
        run=False,
    ),
    pytest.mark.skipif(
        (not installed_pandas),
        reason="SQL Counter changes when pandas not installed",
        run=False,
    ),
]

binary_operations = [
    lambda x, y: x.union_all(y),
    lambda x, y: x.select("a").union(y.select("a")),
    lambda x, y: x.except_(y),
    lambda x, y: x.select("a").intersect(y.select("a")),
    lambda x, y: x.join(y.select("a", "b"), rsuffix="_y"),
    lambda x, y: x.select("a").join(y, how="outer", rsuffix="_y"),
    lambda x, y: x.join(y.select("a"), how="left", rsuffix="_y"),
]


WITH = "WITH"

paramList = [False, True]


@pytest.fixture(params=paramList, autouse=True)
def setup(request, session):
    is_cte_optimization_enabled = session._cte_optimization_enabled
    is_query_compilation_enabled = session._query_compilation_stage_enabled
    session._query_compilation_stage_enabled = request.param
    session._cte_optimization_enabled = True
    yield
    session._cte_optimization_enabled = is_cte_optimization_enabled
    session._query_compilation_stage_enabled = is_query_compilation_enabled


def check_result(session, df, expect_cte_optimized):
    df = df.sort(df.columns)
    session._cte_optimization_enabled = False
    result = df.collect()
    result_count = df.count()
    result_pandas = df.to_pandas() if installed_pandas else None

    session._cte_optimization_enabled = True
    cte_result = df.collect()
    cte_result_count = df.count()
    cte_result_pandas = df.to_pandas() if installed_pandas else None

    Utils.check_answer(cte_result, result)
    assert result_count == cte_result_count
    if installed_pandas:
        from pandas.testing import assert_frame_equal

        assert_frame_equal(result_pandas, cte_result_pandas)

    # verify no actual query or describe query is issued during that process
    with SqlCounter(query_count=0, describe_count=0):
        last_query = df.queries["queries"][-1]

        if expect_cte_optimized:
            assert last_query.startswith(WITH)
            assert last_query.count(WITH) == 1
        else:
            assert last_query.count(WITH) == 0


def count_number_of_ctes(query):
    # a CTE is represented with a pattern `SNOWPARK_TEMP_xxx AS`
    pattern = re.compile(rf"{TEMP_OBJECT_NAME_PREFIX}CTE_[0-9A-Z]+\sAS")
    return len(pattern.findall(query))


@pytest.mark.parametrize(
    "action",
    [
        lambda x: x.select("a", "b").select("b"),
        lambda x: x.filter(col("a") == 1).select("b"),
        lambda x: x.select("a").filter(col("a") == 1),
        lambda x: x.select_expr("sum(a) as a").with_column("b", seq1()),
        lambda x: x.drop("b").sort("a", ascending=False),
        lambda x: x.rename(col("a"), "new_a").limit(1),
        lambda x: x.to_df("a1", "b1").alias("L"),
    ],
)
@sql_count_checker(
    query_count=12,
    union_count=6,
    high_count_expected=True,
    high_count_reason="multiple evaluations",
)
def test_unary(session, action):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df_action = action(df)
    check_result(session, df_action, expect_cte_optimized=False)
    check_result(session, df_action.union_all(df_action), expect_cte_optimized=True)


@pytest.mark.parametrize("action", binary_operations)
def test_binary(session, action):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    with SqlCounter(query_count=6):
        check_result(session, action(df, df), expect_cte_optimized=True)

    df1 = session.create_dataframe([[3, 4], [2, 1]], schema=["a", "b"])
    with SqlCounter(query_count=6):
        check_result(session, action(df, df1), expect_cte_optimized=False)

    # multiple queries
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df2 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold
    with SqlCounter(
        query_count=36,
        high_count_expected=True,
        high_count_reason="small array bind threshold",
    ):
        df3 = action(df2, df2)
        check_result(session, df3, expect_cte_optimized=True)
        plan_queries = df3.queries
        # check the number of queries
        assert len(plan_queries["queries"]) == 3
        assert len(plan_queries["post_actions"]) == 1


@sql_count_checker(query_count=2, join_count=2)
def test_join_with_alias_dataframe(session):
    df1 = session.create_dataframe([[1, 6]], schema=["col1", "col2"])
    df_res = (
        df1.alias("L")
        .join(df1.alias("R"), col("L", "col1") == col("R", "col1"))
        .select(col("L", "col1"), col("R", "col2"))
    )

    session._cte_optimization_enabled = False
    result = df_res.collect()

    session._cte_optimization_enabled = True
    cte_result = df_res.collect()

    Utils.check_answer(cte_result, result)

    with SqlCounter(query_count=0, describe_count=0):
        last_query = df_res.queries["queries"][-1]
        assert last_query.startswith(WITH)
        assert last_query.count(WITH) == 1


@pytest.mark.parametrize("action", binary_operations)
def test_variable_binding_binary(session, action):
    df1 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]
    )
    df2 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "c", 3, "d"]
    )
    df3 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]
    )

    with SqlCounter(query_count=6):
        check_result(session, action(df1, df3), expect_cte_optimized=True)
    with SqlCounter(query_count=6):
        check_result(session, action(df1, df2), expect_cte_optimized=False)


def test_variable_binding_multiple(session):
    if not session._query_compilation_stage_enabled:
        pytest.skip(
            "CTE query generation without the new query generation doesn't work correctly"
        )

    df1 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]
    )
    df2 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "c", 3, "d"]
    )

    df_res = df1.union(df1).union(df2)
    with SqlCounter(query_count=6, union_count=12):
        check_result(session, df_res, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        plan_queries = df_res._plan.execution_queries

        assert plan_queries[PlanQueryType.QUERIES][-1].params == [
            1,
            "a",
            2,
            "b",
            1,
            "c",
            3,
            "d",
        ]

    df_res = df2.union(df1).union(df2).union(df1)
    with SqlCounter(query_count=6, union_count=18):
        check_result(session, df_res, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        plan_queries = df_res._plan.execution_queries

        assert plan_queries[PlanQueryType.QUERIES][-1].params == [
            1,
            "a",
            2,
            "b",
            1,
            "c",
            3,
            "d",
        ]
        assert plan_queries[PlanQueryType.QUERIES][-1].sql.count(WITH) == 1


@pytest.mark.parametrize(
    "action",
    [
        lambda x, y: x.union_all(y),
        lambda x, y: x.join(y.select((col("a") + 1).as_("a"))),
    ],
)
def test_number_of_ctes(session, action):
    df3 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df2 = df3.filter(col("a") == 1)
    df1 = df2.select((col("a") + 1).as_("a"), "b")

    # only df1 will be converted to a CTE
    root = action(df1, df1)
    with SqlCounter(query_count=6):
        check_result(session, root, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(root.queries["queries"][-1]) == 1

    # df1 and df3 will be converted to CTEs
    root = action(root, df3)
    with SqlCounter(query_count=6):
        check_result(session, root, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(root.queries["queries"][-1]) == 2

    # df1, df2 and df3 will be converted to CTEs
    root = action(root, df2)
    with SqlCounter(query_count=6):
        check_result(session, root, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        # if SQL simplifier is enabled, filter and select will be one query,
        # so there are only 2 CTEs
        assert count_number_of_ctes(root.queries["queries"][-1]) == (
            2 if session._sql_simplifier_enabled else 3
        )


@sql_count_checker(query_count=6, union_count=6)
def test_different_df_same_query(session):
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).select("a")
    df2 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).select("a")
    df = df2.union_all(df1)
    check_result(session, df, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(df.queries["queries"][-1]) == 1


def test_same_duplicate_subtree(session):
    """
            root
           /    \
         df3   df3
          |     |
        df2    df2
          |     |
        df1    df1

    Only should df3 be converted to a CTE
    """
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df2 = df1.filter(col("a") == 1)
    df3 = df2.select("b")
    df_result1 = df3.union_all(df3)
    with SqlCounter(query_count=6, union_count=6):
        check_result(session, df_result1, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(df_result1.queries["queries"][-1]) == 1
    """
                              root
                             /    \
                           df5   df6
                        /   |     |   \
                      df3  df3   df4  df4
                       |    |     |    |
                      df2  df2   df2  df2
                       |    |     |    |
                      df1  df1   df1  df1

    df4, df3 and df2 should be converted to CTEs
    """
    df4 = df2.select("a")
    df_result2 = df3.union_all(df3).union_all(df4.union_all(df4))
    with SqlCounter(query_count=6, union_count=18):
        check_result(session, df_result2, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(df_result2.queries["queries"][-1]) == 3


@pytest.mark.parametrize(
    "mode", ["append", "truncate", "overwrite", "errorifexists", "ignore"]
)
def test_save_as_table(session, mode):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    if mode == "append":
        # 1 show query + 1 create table query + 1 insert query
        expected_query_count = 3
    elif mode == "truncate":
        # 1 show query + 1 create table query
        expected_query_count = 2
    else:
        # 1 create table query
        expected_query_count = 1
    with SqlCounter(query_count=expected_query_count, union_count=1):
        with session.query_history() as query_history:
            df.union_all(df).write.save_as_table(
                random_name_for_temp_object(TempObjectType.TABLE),
                table_type="temp",
                mode=mode,
            )
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert count_number_of_ctes(query) == 1
    if mode in ["append", "truncate"]:
        assert sum("show" in q.sql_text for q in query_history.queries) == 1


@sql_count_checker(query_count=1, union_count=1)
def test_create_or_replace_view(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    with session.query_history() as query_history:
        df.union_all(df).create_or_replace_temp_view(
            random_name_for_temp_object(TempObjectType.VIEW)
        )
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert count_number_of_ctes(query) == 1


@sql_count_checker(query_count=4, union_count=3)
def test_table_update_delete_merge(session):
    table_name = random_name_for_temp_object(TempObjectType.VIEW)
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df.write.save_as_table(table_name, table_type="temp")
    source_df = df.union_all(df)
    t = session.table(table_name)

    # update
    with session.query_history() as query_history:
        t.update({"b": 0}, t.a == source_df.a, source_df)
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert count_number_of_ctes(query) == 1

    # delete
    with session.query_history() as query_history:
        t.delete(t.a == source_df.a, source_df)
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert count_number_of_ctes(query) == 1

    # merge
    with session.query_history() as query_history:
        t.merge(
            source_df, t.a == source_df.a, [when_matched().update({"b": source_df.b})]
        )
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert count_number_of_ctes(query) == 1


@sql_count_checker(query_count=1, union_count=1)
def test_copy_into_location(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.union_all(df)
    remote_file_path = f"{session.get_session_stage()}/df.parquet"
    with session.query_history() as query_history:
        df1.write.copy_into_location(
            remote_file_path,
            file_format_type="parquet",
            header=True,
            overwrite=True,
            single=True,
        )
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert count_number_of_ctes(query) == 1


@sql_count_checker(query_count=1, union_count=1)
def test_explain(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    explain_string = df.union_all(df)._explain_string()
    assert "WITH SNOWPARK_TEMP_CTE" in explain_string


def test_sql_simplifier(session):
    if not session._sql_simplifier_enabled:
        pytest.skip("SQL simplifier is not enabled")

    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.filter(col("a") == 1)
    filter_clause = (
        'WHERE ("A" = 1)'
        if session.eliminate_numeric_sql_value_cast_enabled
        else 'WHERE ("A" = 1 :: INT)'
    )

    df2 = df1.select("a", "b")
    df3 = df1.select("a", "b").select("a", "b")
    df4 = df1.union_by_name(df2).union_by_name(df3)
    with SqlCounter(query_count=6, union_count=12):
        check_result(session, df4, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        # after applying sql simplifier, there is only one CTE (df1, df2, df3 have the same query)
        assert count_number_of_ctes(df4.queries["queries"][-1]) == 1
        assert df4.queries["queries"][-1].count(filter_clause) == 1

    df5 = df1.join(df2).join(df3)
    with SqlCounter(query_count=6, join_count=12):
        check_result(session, df5, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        # when joining the dataframe with the same column names, we will add random suffix to column names,
        # so df1, df2 and df3 have 3 different queries, and we can't convert them to a CTE
        # the only CTE is from df
        assert count_number_of_ctes(df5.queries["queries"][-1]) == 1
        assert df5.queries["queries"][-1].count(filter_clause) == 3

    df6 = df1.join(df2, lsuffix="_xxx").join(df3, lsuffix="_yyy")
    with SqlCounter(query_count=6, join_count=12):
        check_result(session, df6, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        # When adding a lsuffix, the columns of right dataframe don't need to be renamed,
        # so we will get a common CTE with filter
        assert count_number_of_ctes(df6.queries["queries"][-1]) == 2
        assert df6.queries["queries"][-1].count(filter_clause) == 2

    df7 = df1.with_column("c", lit(1))
    df8 = df1.with_column("c", lit(1)).with_column("d", lit(1))
    df9 = df1.join(df7, lsuffix="_xxx").join(df8, lsuffix="_yyy")
    with SqlCounter(query_count=6, join_count=12):
        check_result(session, df9, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        # after applying sql simplifier, with_column operations are flattened,
        # so df1, df7 and df8 have different queries, and we can't convert them to a CTE
        # the only CTE is from df
        assert count_number_of_ctes(df9.queries["queries"][-1]) == 1
        assert df9.queries["queries"][-1].count(filter_clause) == 3


@sql_count_checker(query_count=6, union_count=6)
def test_table_function(session):
    df = (
        session.generator(seq1(1), uniform(1, 10, 2), rowcount=150)
        .order_by(seq1(1))
        .limit(3, offset=20)
    )
    df_result = df.union_all(df).select("*")
    check_result(session, df_result, expect_cte_optimized=True)
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


@sql_count_checker(query_count=7, union_count=6)
def test_table(session):
    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).write.save_as_table(
        temp_table_name, table_type="temp"
    )
    df = session.table(temp_table_name).filter(col("a") == 1)
    df_result = df.union_all(df).select("*")
    check_result(session, df_result, expect_cte_optimized=True)
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


@pytest.mark.parametrize(
    "query",
    [
        "select 1 as a, 2 as b",
        "show tables in schema limit 10",
    ],
)
def test_sql(session, query):
    if not session._query_compilation_stage_enabled:
        pytest.skip(
            "CTE query generation without the new query generation doesn't work correctly"
        )

    df = session.sql(query).filter(lit(True))
    df_result = df.union_all(df).select("*")
    expected_query_count = 6
    high_query_count_expected = False
    if "show tables" in query:
        expected_query_count = 12
        high_query_count_expected = True
    with SqlCounter(
        query_count=expected_query_count,
        union_count=6,
        high_count_expected=high_query_count_expected,
        high_count_reason="extra query from show tables",
    ):
        check_result(session, df_result, expect_cte_optimized=True)
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


@pytest.mark.parametrize(
    "action",
    [
        lambda x: x.distinct(),
        lambda x: x.group_by("a").avg("b"),
    ],
)
@sql_count_checker(query_count=7, union_count=6)
def test_aggregate(session, action):
    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).write.save_as_table(
        temp_table_name, table_type="temp"
    )
    df = action(session.table(temp_table_name)).filter(col("a") == 1)
    df_result = df.union_by_name(df)
    check_result(session, df_result, expect_cte_optimized=True)
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="need resources")
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
    df_result = df.union_by_name(df)
    expected_query_count = 18
    if mode == "copy":
        expected_query_count = 24
    with SqlCounter(
        query_count=expected_query_count,
        union_count=6,
        high_count_expected=True,
        high_count_reason="extra query for file operations",
    ):
        check_result(session, df_result, expect_cte_optimized=True)


@sql_count_checker(query_count=6, join_count=15)
def test_join_table_function(session):
    df = session.sql(
        "select 'James' as name, 'address1 address2 address3' as addresses"
    )
    df1 = df.join_table_function("split_to_table", df["addresses"], lit(" "))
    df_result = df1.join(df1.select("name", "addresses"), rsuffix="_y")
    check_result(session, df_result, expect_cte_optimized=True)
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


@sql_count_checker(query_count=7, join_count=9, union_count=6)
def test_pivot_unpivot(session):
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
    df_pivot = session.table(table_name).pivot("month", ["JAN", "FEB"]).sum("amount")
    df_unpivot = session.create_dataframe(
        [(1, "electronics", 100, 200), (2, "clothes", 100, 300)],
        schema=["empid", "dept", "jan", "feb"],
    ).unpivot("sales", "month", ["jan", "feb"])
    df = df_pivot.join(df_unpivot, "empid")
    df_result = df.union_all(df).select("*")
    check_result(session, df_result, expect_cte_optimized=True)
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


@sql_count_checker(query_count=6, union_count=6)
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
    check_result(session, df_result, expect_cte_optimized=True)
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


def test_in_with_subquery_multiple_query(session):
    if session._sql_simplifier_enabled:
        pytest.skip(
            "SNOW-1678419 pre and post actions are not propagated properly for SelectStatement"
        )
    # multiple queries
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df0 = session.create_dataframe([[1], [2], [5], [7]], schema=["a"])
        df = session.create_dataframe(
            [[1, "a", 1, 1], [2, "b", 2, 2], [3, "b", 33, 33], [5, "c", 21, 18]],
            schema=["a", "b", "c", "d"],
        )
        df_filter = df0.filter(col("a") < 3)
        df_in = df.filter(~df["a"].in_(df_filter))
        df_result = df_in.union_all(df_in).select("*")
        with SqlCounter(
            query_count=66,
            high_count_expected=True,
            high_count_reason="multiple queries for both df0 and df",
        ):
            check_result(session, df_result, expect_cte_optimized=True)
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold


def test_select_with_column_expr_alias(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.select("a", "b", (col("a") + col("b")).as_("c"))
    df2 = df1.select("a", "b", "c", (col("a") + col("b") + 1).as_("d"))
    df_result = df2.union_all(df2).select("*")
    with SqlCounter(query_count=6, union_count=6):
        check_result(session, df_result, expect_cte_optimized=True)

    df2 = df.select_expr("a + 1 as a", "b + 1 as b")
    df_result = df2.union_all(df2).select("*")
    with SqlCounter(query_count=6, union_count=6):
        check_result(session, df_result, expect_cte_optimized=True)


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-609328: support caplog in SP regression test"
)
@sql_count_checker(query_count=0)
def test_cte_optimization_enabled_parameter(session, caplog):
    with caplog.at_level(logging.WARNING):
        session.cte_optimization_enabled = True
    assert "cte_optimization_enabled is experimental" in caplog.text


def test_ctc_workloads(session):
    import random

    self_width = "M"
    orders_base_table_fqn = '"SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS"'
    orders_large_base_table_fqn = '"SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."ORDERS"'
    customer_base_table_fqn = '"SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER"'
    lineitem_base_table_fqn = '"SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM"'

    num_add_drop_column = 1
    num_redundant_group_by = 1
    num_self_join = 10
    num_self_join_layers = 1

    def expand_columns_for_df(df, width: str):
        if width == "S":
            return df

        cols = df.columns
        df = df.with_columns(
            [f"{col}__COPY1" for col in cols], [F.col(col) for col in cols]
        )
        if width == "M":
            return df

        df = df.with_columns(
            [f"{col}__COPY2" for col in cols], [F.col(col) for col in cols]
        )
        return df

    def get_orders_base_df():
        df = session.table(orders_base_table_fqn)
        return expand_columns_for_df(df, self_width)

    def get_orders_large_base_df():
        df = session.table(orders_large_base_table_fqn)
        return expand_columns_for_df(df, self_width)

    def get_customer_base_df():
        df = session.table(customer_base_table_fqn)
        return expand_columns_for_df(df, self_width)

    def get_lineitem_base_df():
        df = session.table(lineitem_base_table_fqn)
        return expand_columns_for_df(df, self_width)

    def union_all_with_non_matching_cols(df1, df2):
        left_attr = df1.schema
        right_attr = df2.schema
        left_datatype_by_name = {attr.name: attr.datatype for attr in left_attr}
        right_datatype_by_name = {attr.name: attr.datatype for attr in right_attr}

        left_attr_names = {attr.name for attr in left_attr}
        right_attr_names = {attr.name for attr in right_attr}

        intersect_attr = left_attr_names & right_attr_names
        left_extra = left_attr_names - intersect_attr
        right_extra = right_attr_names - intersect_attr

        left_expand, right_expand = df1, df2
        for v in right_extra:
            left_expand = left_expand.with_column(
                v, F.lit(None).cast(right_datatype_by_name[v])
            )
        left_expand = left_expand.with_column("__L", F.lit(True))

        for v in left_extra:
            right_expand = right_expand.with_column(
                v, F.lit(None).cast(left_datatype_by_name[v])
            )
        right_expand = right_expand.with_column("__L", F.lit(False))

        return left_expand.unionByName(right_expand)

    def pseudo_merge_asof(
        df1,
        df2,
        order_on: List[str],
        partition_by: List[str],
        over: List[str],
        direction: str,
    ):
        df = union_all_with_non_matching_cols(df1, df2)
        if direction == "backward":
            order_on_x = [F.col(x).asc() for x in order_on] + [F.col("__L").asc()]
        else:
            order_on_x = [F.col(x).asc() for x in order_on] + [F.col("__L").desc()]
        partition_spec = Window.partition_by(partition_by).order_by(order_on_x)
        if direction == "backward":
            join_spec = partition_spec.rows_between(
                Window.unboundedPreceding, Window.currentRow
            )
        else:
            join_spec = partition_spec.rows_between(
                Window.currentRow, Window.unboundedFollowing
            )
        cols = [
            F.last_value(over_col, ignore_nulls=True).over(join_spec)
            for over_col in over
        ]

        df = df.with_columns(over, cols)
        return df.filter("__L").drop("__L")

    def pseudo_one_to_many_merge_asof(
        df1,
        df2,
        order_on: str,
        partition_by: List[str],
    ):
        order_on_col_name = "__ORDER_ON_COLUMN_NAME"
        order_on_backward_col_name = order_on_col_name + "__BACKWARD"
        order_on_forward_col_name = order_on_col_name + "__FORWARD"

        df2 = df2.with_column(order_on_col_name, F.col(order_on))
        df = pseudo_merge_asof(
            df1,
            df2.with_column(order_on_backward_col_name, F.col(order_on)),
            order_on=[order_on],
            partition_by=partition_by,
            over=[order_on_backward_col_name],
            direction="backward",
        )
        df = pseudo_merge_asof(
            df,
            df2.with_column(order_on_forward_col_name, F.col(order_on)),
            order_on=[order_on],
            partition_by=partition_by,
            over=[order_on_forward_col_name],
            direction="forward",
        )
        df = df.with_column(
            order_on_col_name,
            F.when(
                F.is_null(order_on_backward_col_name)
                | (
                    F.abs(F.col(order_on) - F.col(order_on_backward_col_name))
                    > F.abs(F.col(order_on) - F.col(order_on_forward_col_name))
                ),
                F.col(order_on_forward_col_name),
            ).otherwise(F.col(order_on_backward_col_name)),
        ).drop(order_on_forward_col_name, order_on_backward_col_name)

        df = df.drop(order_on_col_name)
        return df

    def get_customer_with_filter_and_dropped_cols(customer_df):
        customer_df = customer_df.where(F.col("C_ACCTBAL") < F.lit(100))
        customer_df = customer_df.drop("C_PHONE")
        customer_df = customer_df.distinct()

        original_cols = customer_df.columns
        for col_t in original_cols:
            customer_df = customer_df.with_column(f"{col_t}__NOTNULL", F.col(col_t))

        return customer_df.drop(original_cols)

    def apply_with_column_and_filter_ops(df):
        discount_lower_bound = 0.06
        discount_upper_bound = 0.07

        df = df.with_column(
            "O_ORDERSTATUS",
            F.when(
                (F.col("O_ORDERPRIORITY") == "1-URGENT")
                & (F.col("L_DISCOUNT") > discount_lower_bound),
                "F",
            ).otherwise(F.col("O_ORDERSTATUS")),
        )

        df = df.with_column(
            "EFFECTIVE_DISCOUNT",
            F.when(F.col("L_DISCOUNT") < discount_upper_bound, 3).otherwise(
                F.col("L_DISCOUNT")
            ),
        )

        df = df.where(F.col("L_TAX") > F.col("EFFECTIVE_DISCOUNT"))
        df = df.drop("EFFECTIVE_DISCOUNT")
        return df

    def apply_with_column_and_filter_for_self_union(df, seed: int):
        window_lineitem = Window.partition_by("L_ORDERKEY")
        window_order = Window.partition_by("O_CUSTKEY")

        random.seed(seed)
        date_diff_thresh = random.randint(25, 50)
        random_year = random.randint(5, 7)
        random_month = random.randint(1, 9)
        random_day = random.randint(1, 28)
        expensive_thresh = random.randint(1600, 3000)

        df_filtered = df.with_column(
            "EARLY_ORDER",
            F.datediff("day", F.col("L_SHIPDATE"), F.col("L_COMMITDATE"))
            < date_diff_thresh,
        )
        df_filtered = df_filtered.with_column(
            "MOST_EXPENSIVE_PART", F.max(F.col("L_EXTENDEDPRICE")).over(window_lineitem)
        )
        df_filtered = df_filtered.with_column(
            "MOST_EXPENSIVE_BUY", F.max(F.col("O_TOTALPRICE")).over(window_order)
        )
        df_filtered = df_filtered.with_column(
            "MOST_EXPENSIVE_PART_BY_CUSTOMER",
            F.max(F.col("MOST_EXPENSIVE_PART")).over(window_order),
        )
        df_filtered = df_filtered.where(
            F.col("L_SHIPDATE")
            > F.lit(f"199{random_year}-0{random_month}-{random_day}")
        )
        df_filtered = df_filtered.where(F.col("EARLY_ORDER"))
        df_filtered = df_filtered.where(
            F.col("MOST_EXPENSIVE_PART_BY_CUSTOMER") > expensive_thresh
        )
        df_filtered = df_filtered.drop(
            "EARLY_ORDER",
            "MOST_EXPENSIVE_PART",
            "MOST_EXPENSIVE_BUY",
            "MOST_EXPENSIVE_PART_BY_CUSTOMER",
        )

        return df_filtered

    def apply_add_drop_column(df):
        df = df.with_column("C_CUSTKEY_TO_BE_DROPPED", F.col("C_CUSTKEY").is_not_null())
        df = df.filter(F.col("C_CUSTKEY_TO_BE_DROPPED"))
        df = df.drop("C_CUSTKEY_TO_BE_DROPPED")

        return df

    def get_left_df():
        # base tables
        orders_small_df = get_orders_base_df()
        orders_large_df = get_orders_large_base_df()
        customer_df = get_customer_base_df()
        lineitem_sf1 = get_lineitem_base_df()

        # add a column by calling a temp udf
        @F.udf
        def add_one(x: int) -> int:
            return x + 1

        customer_df = customer_df.with_column(
            "NATIONKEY_PLUS1", add_one(customer_df["C_NATIONKEY"])
        )

        orders_with_customer_df = orders_small_df.join(
            customer_df,
            orders_small_df["O_CUSTKEY"] == customer_df["C_CUSTKEY"],
            how="left",
        )
        orders_with_customer_lineitem_df = orders_with_customer_df.join(
            lineitem_sf1.distinct(),
            orders_small_df["O_ORDERKEY"] == lineitem_sf1["L_ORDERKEY"],
            how="left",
        )

        # perform one_to_many_merge_asof
        return pseudo_one_to_many_merge_asof(
            orders_with_customer_lineitem_df,
            orders_large_df,
            "O_ORDERDATE",
            "O_CUSTKEY",
        )

    def get_right_df():
        customer_df = get_customer_base_df()
        # create a new base with filtering and dropping cols
        customer_with_filter_and_dropped_rows = (
            get_customer_with_filter_and_dropped_cols(customer_df)
        )

        for _ in range(num_redundant_group_by):
            customer_with_filter_and_dropped_rows = (
                customer_with_filter_and_dropped_rows.distinct()
            )
        return customer_with_filter_and_dropped_rows

    def apply_self_join_layers(df, layer: int):
        df_layer_set = df.distinct().where(F.col("LAYER_SEQ") == layer)
        return df.union_all_by_name(df_layer_set)

    def get_df_before_self_union():
        df_left = get_left_df()
        df_right = get_right_df()

        df_res = df_left.join(
            df_right,
            df_left["C_CUSTKEY"] == df_right["C_CUSTKEY__NOTNULL"],
            how="inner",
        )

        for _ in range(num_add_drop_column):
            df_res = apply_add_drop_column(df_res)

        df_res = apply_with_column_and_filter_ops(df_res)
        return df_res

    # def get_df():
    df = get_df_before_self_union()

    df_with_filters = []
    for i in range(num_self_join):
        df_with_filters.append(apply_with_column_and_filter_for_self_union(df, i))

    df = reduce(lambda l, r: l.unionByName(r), df_with_filters)

    df = df.with_column("LAYER_SEQ", F.seq8())
    for i in range(1, num_self_join_layers):
        df = apply_self_join_layers(df, i)

    print(df.queries)
    # return df
