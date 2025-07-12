#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re

import pytest

from snowflake.connector.options import installed_pandas
from snowflake.snowpark import Window
from snowflake.snowpark._internal.analyzer import analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import PlanQueryType
from snowflake.snowpark._internal.utils import (
    TEMP_OBJECT_NAME_PREFIX,
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import (
    avg,
    col,
    lit,
    seq1,
    uniform,
    when_matched,
    to_timestamp,
)
from tests.integ.scala.test_dataframe_reader_suite import get_reader
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import IS_IN_STORED_PROC, IS_IN_STORED_PROC_LOCALFS, TestFiles, Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="CTE is a SQL feature",
        run=False,
    ),
]

binary_operations = [
    ("union", lambda x, y: x.union_all(y)),
    ("union", lambda x, y: x.select("a").union(y.select("a"))),
    ("except", lambda x, y: x.except_(y)),
    ("intersect", lambda x, y: x.select("a").intersect(y.select("a"))),
    ("join", lambda x, y: x.join(y.select("a", "b"), rsuffix="_y")),
    ("join", lambda x, y: x.select("a").join(y, how="outer", rsuffix="_y")),
    ("join", lambda x, y: x.join(y.select("a"), how="left", rsuffix="_y")),
]


WITH = "WITH"


@pytest.fixture(autouse=True)
def setup(request, session):
    is_cte_optimization_enabled = session._cte_optimization_enabled
    is_query_compilation_enabled = session._query_compilation_stage_enabled
    session._query_compilation_stage_enabled = True
    session._cte_optimization_enabled = True
    yield
    session._cte_optimization_enabled = is_cte_optimization_enabled
    session._query_compilation_stage_enabled = is_query_compilation_enabled


def check_result(
    session,
    df,
    expect_cte_optimized: bool,
    query_count=None,
    describe_count=None,
    union_count=None,
    join_count=None,
    cte_union_count=None,
    cte_join_count=None,
    high_query_count_expected=False,
    high_query_count_reason=None,
    describe_count_for_optimized=None,
):
    describe_count_for_optimized = (
        describe_count_for_optimized
        if describe_count_for_optimized is not None
        else describe_count
    )
    df = df.sort(df.columns)
    session._cte_optimization_enabled = False
    with SqlCounter(
        query_count=query_count,
        describe_count=describe_count,
        union_count=union_count,
        join_count=join_count,
        high_count_expected=high_query_count_expected,
        high_count_reason=high_query_count_reason,
    ):
        result = df.collect()
    with SqlCounter(
        query_count=query_count,
        describe_count=describe_count,
        union_count=union_count,
        join_count=join_count,
        high_count_expected=high_query_count_expected,
        high_count_reason=high_query_count_reason,
    ):
        result_count = df.count()
    result_pandas = df.to_pandas() if installed_pandas else None

    session._cte_optimization_enabled = True
    if cte_union_count is None:
        cte_union_count = union_count
    if cte_join_count is None:
        cte_join_count = join_count
    with SqlCounter(
        query_count=query_count,
        describe_count=describe_count_for_optimized,
        union_count=cte_union_count,
        join_count=cte_join_count,
        high_count_expected=high_query_count_expected,
        high_count_reason=high_query_count_reason,
    ):
        cte_result = df.collect()
    with SqlCounter(
        query_count=query_count,
        describe_count=describe_count_for_optimized,
        union_count=cte_union_count,
        join_count=cte_join_count,
        high_count_expected=high_query_count_expected,
        high_count_reason=high_query_count_reason,
        strict=False,
    ):
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
def test_unary(session, action):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df_action = action(df)
    check_result(
        session,
        df_action,
        expect_cte_optimized=False,
        query_count=1,
        describe_count=0,
        union_count=0,
        join_count=0,
    )
    check_result(
        session,
        df_action.union_all(df_action),
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )


@pytest.mark.parametrize("type, action", binary_operations)
def test_binary(session, type, action):
    union_count = 0
    join_count = 0
    if type == "union":
        union_count = 1
    if type == "join":
        join_count = 1

    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    check_result(
        session,
        action(df, df),
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=union_count,
        join_count=join_count,
    )

    df1 = session.create_dataframe([[3, 4], [2, 1]], schema=["a", "b"])
    check_result(
        session,
        action(df, df1),
        expect_cte_optimized=False,
        query_count=1,
        describe_count=0,
        union_count=union_count,
        join_count=join_count,
    )

    # multiple queries
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df2 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold
    df3 = action(df2, df2)
    check_result(
        session,
        df3,
        expect_cte_optimized=True,
        query_count=4,
        describe_count=0,
        union_count=union_count,
        join_count=join_count,
    )
    plan_queries = df3.queries
    # check the number of queries
    assert len(plan_queries["queries"]) == 3
    assert len(plan_queries["post_actions"]) == 1


def test_join_with_alias_dataframe(session):
    expected_describe_count = (
        3
        if (session.reduce_describe_query_enabled and session.sql_simplifier_enabled)
        else 4
    )
    with SqlCounter(
        query_count=2, describe_count=expected_describe_count, join_count=2
    ):
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


def test_join_with_set_operation(session):
    df1 = session.create_dataframe([[1, 2, 3], [4, 5, 6]], "a: int, b: int, c: int")
    df2 = session.create_dataframe([[1, 1], [4, 5]], "a: int, b: int")

    df1 = df1.filter(df1.a > 1)
    df1 = df1.union_all(df1)

    df3 = df1.join(df2, (df1.a == df2.a) & (df1.b == df2.b)).select(
        df2.a.as_("a"), df2.b.as_("b"), df1.c
    )
    df4 = df3.except_(df1)

    check_result(
        session,
        df4,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=2,
        cte_union_count=1,
        join_count=1,
    )


@pytest.mark.parametrize("type, action", binary_operations)
def test_variable_binding_binary(session, type, action):
    df1 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]
    )
    df2 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "c", 3, "d"]
    )
    df3 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]
    )

    union_count = 0
    join_count = 0
    if type == "join":
        join_count = 1
    if type == "union":
        union_count = 1
    check_result(
        session,
        action(df1, df3),
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=union_count,
        join_count=join_count,
    )
    check_result(
        session,
        action(df1, df2),
        expect_cte_optimized=False,
        query_count=1,
        describe_count=0,
        union_count=union_count,
        join_count=join_count,
    )


def test_variable_binding_multiple(session):
    df1 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]
    )
    df2 = session.sql(
        "select $1 as a, $2 as b from values (?, ?), (?, ?)", params=[1, "c", 3, "d"]
    )

    df_res = df1.union(df1).union(df2)
    check_result(
        session,
        df_res,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=2,
        join_count=0,
    )
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
    check_result(
        session,
        df_res,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=3,
        join_count=0,
    )
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
    "type, action",
    [
        ("union", lambda x, y: x.union_all(y)),
        ("join", lambda x, y: x.join(y.select((col("a") + 1).as_("a")))),
    ],
)
def test_number_of_ctes(session, type, action):
    df3 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df2 = df3.filter(col("a") == 1)
    df1 = df2.select((col("a") + 1).as_("a"), "b")

    join_count = 0
    union_count = 0
    if type == "union":
        union_count = 1
    if type == "join":
        join_count = 1

    # only df1 will be converted to a CTE
    root = action(df1, df1)
    check_result(
        session,
        root,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=union_count,
        join_count=join_count,
    )
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(root.queries["queries"][-1]) == 1

    # df1 and df3 will be converted to CTEs
    root = action(root, df3)
    if type == "union":
        union_count += 1
    if type == "join":
        join_count += 1
    check_result(
        session,
        root,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=union_count,
        join_count=join_count,
    )
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(root.queries["queries"][-1]) == 2

    # df1, df2 and df3 will be converted to CTEs
    root = action(root, df2)
    if type == "union":
        union_count += 1
    if type == "join":
        join_count += 1
    check_result(
        session,
        root,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=union_count,
        join_count=join_count,
    )
    with SqlCounter(query_count=0, describe_count=0):
        # if SQL simplifier is enabled, filter and select will be one query,
        # so there are only 2 CTEs
        assert count_number_of_ctes(root.queries["queries"][-1]) == (
            2 if session._sql_simplifier_enabled else 3
        )


def test_different_df_same_query(session):
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).select("a")
    df2 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).select("a")
    df = df2.union_all(df1)
    check_result(
        session,
        df,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )
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
    check_result(
        session,
        df_result1,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )
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
    check_result(
        session,
        df_result2,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=3,
        join_count=0,
    )
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
    if mode == "append":
        assert query.startswith("INSERT  INTO")
    elif mode in ("truncate", "overwrite"):
        assert query.startswith("CREATE  OR  REPLACE  TEMPORARY  TABLE")
    else:
        assert query.startswith("CREATE  TEMPORARY  TABLE")
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
    assert query.startswith("CREATE  OR  REPLACE  TEMPORARY  VIEW")
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
    assert query.startswith("UPDATE")
    assert count_number_of_ctes(query) == 1

    # delete
    with session.query_history() as query_history:
        t.delete(t.a == source_df.a, source_df)
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert query.startswith("DELETE  FROM")
    assert count_number_of_ctes(query) == 1

    # merge
    with session.query_history() as query_history:
        t.merge(
            source_df, t.a == source_df.a, [when_matched().update({"b": source_df.b})]
        )
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert query.startswith("MERGE  INTO")
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
    assert query.startswith("COPY  INTO")
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
    check_result(
        session,
        df4,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=2,
        join_count=0,
    )
    with SqlCounter(query_count=0, describe_count=0):
        # after applying sql simplifier, there is only one CTE (df1, df2, df3 have the same query)
        assert (
            count_number_of_ctes(Utils.normalize_sql(df4.queries["queries"][-1])) == 1
        )
        assert Utils.normalize_sql(df4.queries["queries"][-1]).count(filter_clause) == 1

    df5 = df1.join(df2).join(df3)
    check_result(
        session,
        df5,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=0,
        join_count=2,
    )
    with SqlCounter(query_count=0, describe_count=0):
        # when joining the dataframe with the same column names, we will add random suffix to column names,
        # so df1, df2 and df3 have 3 different queries, and we can't convert them to a CTE
        # the only CTE is from df
        assert (
            count_number_of_ctes(Utils.normalize_sql(df5.queries["queries"][-1])) == 1
        )
        assert Utils.normalize_sql(df5.queries["queries"][-1]).count(filter_clause) == 3

    df6 = df1.join(df2, lsuffix="_xxx").join(df3, lsuffix="_yyy")
    check_result(
        session,
        df6,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=0,
        join_count=2,
        describe_count_for_optimized=1 if session._join_alias_fix else None,
    )
    with SqlCounter(query_count=0, describe_count=0):
        # When adding a lsuffix, the columns of right dataframe don't need to be renamed,
        # so we will get a common CTE with filter
        assert (
            count_number_of_ctes(Utils.normalize_sql(df6.queries["queries"][-1])) == 2
        )
        assert Utils.normalize_sql(df6.queries["queries"][-1]).count(filter_clause) == 2

    df7 = df1.with_column("c", lit(1))
    df8 = df1.with_column("c", lit(1)).with_column("d", lit(1))
    df9 = df1.join(df7, lsuffix="_xxx").join(df8, lsuffix="_yyy")
    check_result(
        session,
        df9,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=0,
        join_count=2,
    )
    with SqlCounter(query_count=0, describe_count=0):
        # after applying sql simplifier, with_column operations are flattened,
        # so df1, df7 and df8 have different queries, and we can't convert them to a CTE
        # the only CTE is from df
        assert (
            count_number_of_ctes(Utils.normalize_sql(df9.queries["queries"][-1])) == 1
        )
        assert Utils.normalize_sql(df9.queries["queries"][-1]).count(filter_clause) == 3


def test_table_function(session):
    df = (
        session.generator(seq1(1), uniform(1, 10, 2), rowcount=150)
        .order_by(seq1(1))
        .limit(3, offset=20)
    )
    df_result = df.union_all(df).select("*")
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


def test_table(session):
    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).write.save_as_table(
        temp_table_name, table_type="temp"
    )
    df = session.table(temp_table_name).filter(col("a") == 1)
    df_result = df.union_all(df).select("*")
    check_result(
        session,
        df_result,
        expect_cte_optimized=False if session.sql_simplifier_enabled else True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )
    if not session.sql_simplifier_enabled:
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
    expected_query_count = 1
    if "show tables" in query:
        expected_query_count = 2
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=expected_query_count,
        describe_count=0,
        union_count=1,
        join_count=0,
    )
    with SqlCounter(query_count=0, describe_count=0):
        assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


def test_sql_non_select(session):
    df1 = session.sql("show tables in schema limit 10")
    df2 = session.sql("show tables in schema limit 10")

    df_result = df1.union(df2).select('"name"').filter(lit(True))

    check_result(
        session,
        df_result,
        # since the two show tables are called in two different dataframe, we
        # won't be able to detect those as common subquery.
        expect_cte_optimized=False,
        query_count=3,
        describe_count=0,
        union_count=1,
        join_count=0,
    )


def test_sql_with(session):
    df1 = session.sql("with t as (select 1 as A) select * from t")
    df2 = session.sql("with t as (select 1 as A) select * from t")

    df_result = df1.union(df2).select("A").filter(lit(True))

    check_result(
        session,
        df_result,
        # with ... select is also treated as a select query
        # see is_sql_select_statement() function
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )


@pytest.mark.parametrize(
    "action",
    [
        lambda x: x.distinct(),
        lambda x: x.group_by("a").avg("b"),
    ],
)
def test_aggregate(session, action):
    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).write.save_as_table(
        temp_table_name, table_type="temp"
    )
    # add limit to add a layer of nesting for distinct()
    base_df = session.table(temp_table_name).limit(10)
    df = action(base_df).filter(col("a") == 1)
    df_result = df.union_by_name(df)
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="need resources")
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_df_reader(session, mode, resources_path):
    """
    Test the following cases for reader:

    1.
                        UNION (invalid)
                ________/    |_________
                |                      |
        SelectFromFileNode        SelectFromFileNode

    2.
                        UNION (invalid)
                ________/    |_________
                |                      |
        WithColumn (invalid)        WithColumn (invalid)
                |                      |
        SelectFromFileNode        SelectFromFileNode

    3.
                            UNION (invalid)
            __________________/    |____________________________
            |                                                  |
        UNION (invalid)                                 UNION (invalid)
         /     |_____________                             ____/    |____________
        |                   |                           |                      |
    WithColumn(invalid)    WithColumn(valid)        WithColumn(invalid)    WithColumn (valid)
        |                     |                         |                       |
    SelectFromFileNode      Filter (valid)          SelectFromFileNode      Filter (valid)
                               |                                                |
                          Select (valid)                                  Select (valid)
    """
    if not session.sql_simplifier_enabled:
        pytest.skip("Skip test for simplifier disabled")
    reader = get_reader(session, mode)
    session_stage = session.get_session_stage()
    test_files = TestFiles(resources_path)
    test_file_on_stage = f"{session_stage}/testCSV.csv"
    Utils.upload_to_stage(
        session, session_stage, test_files.test_file_csv, compress=False
    )
    table_name = Utils.random_table_name()
    session.create_dataframe(
        [[3, "three", 3.3], [4, "four", 4.4]], schema=["a", "b", "c"]
    ).write.save_as_table(table_name, table_type="temp")
    df_reader = (
        reader.option("INFER_SCHEMA", True).csv(test_file_on_stage).to_df("a", "b", "c")
    )
    df_select = session.table(table_name).select("a", "b", "c")

    # Case 1
    df_result = df_reader.union_by_name(df_reader)
    expected_query_count = 4 if mode == "copy" else 3
    check_result(
        session,
        df_result,
        expect_cte_optimized=(mode == "copy"),
        query_count=expected_query_count,
        describe_count=0,
        union_count=1,
        join_count=0,
    )

    # Case 2
    df_with_column1 = df_reader.with_column("a1", col("a") + 1)
    df_result = df_with_column1.union_by_name(df_with_column1)
    expected_query_count = 4 if mode == "copy" else 3
    check_result(
        session,
        df_result,
        expect_cte_optimized=(mode == "copy"),
        query_count=expected_query_count,
        describe_count=0,
        union_count=1,
        join_count=0,
    )

    # Case 3
    df_with_column2 = df_select.filter(col("a") == 3).with_column("a1", col("a") + 1)
    df_union = df_with_column1.union_by_name(df_with_column2)
    df_result = df_union.union_by_name(df_union)
    expected_query_count = 4 if mode == "copy" else 3
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=expected_query_count,
        describe_count=0,
        union_count=3,
        join_count=0,
    )


def test_join_table_function(session):
    df = session.sql(
        "select 'James' as name, 'address1 address2 address3' as addresses"
    )
    df1 = df.join_table_function("split_to_table", df["addresses"], lit(" "))
    df_result = df1.join(df1.select("name", "addresses"), rsuffix="_y")
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=0,
        join_count=3,
        cte_join_count=2,
    )
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


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
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=2,
        cte_join_count=1,
    )
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


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
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )
    assert count_number_of_ctes(df_result.queries["queries"][-1]) == 1


def test_in_with_subquery_multiple_query(session):
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
        check_result(
            session,
            df_result,
            expect_cte_optimized=True,
            query_count=7,
            describe_count=0,
            union_count=1,
            join_count=0,
            high_query_count_expected=True,
            high_query_count_reason="multiple queries for both df0 and df",
        )
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold


def test_select_with_column_expr_alias(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.select("a", "b", (col("a") + col("b")).as_("c"))
    df2 = df1.select("a", "b", "c", (col("a") + col("b") + 1).as_("d"))
    df_result = df2.union_all(df2).select("*")
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )

    df2 = df.select_expr("a + 1 as a", "b + 1 as b")
    df_result = df2.union_all(df2).select("*")
    check_result(
        session,
        df_result,
        expect_cte_optimized=True,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )


def test_time_series_aggregation_grouping(session):
    data = [
        ["2024-02-01 00:00:00", "product_A", "transaction_1", 10],
        ["2024-02-15 00:00:00", "product_A", "transaction_2", 15],
        ["2024-02-15 08:00:00", "product_A", "transaction_3", 7],
        ["2024-02-17 00:00:00", "product_A", "transaction_4", 3],
    ]
    df = session.create_dataframe(data).to_df(
        "TS", "PRODUCT_ID", "TRANSACTION_ID", "QUANTITY"
    )
    df = df.with_column("TS", to_timestamp("TS"))

    res = df.analytics.time_series_agg(
        time_col="TS",
        group_by=["PRODUCT_ID"],
        aggs={"QUANTITY": ["SUM"]},
        windows=["-1D", "-7D"],
    )
    check_result(
        session,
        res,
        expect_cte_optimized=False,
        query_count=1,
        describe_count=0,
        union_count=0,
        join_count=0,
        cte_join_count=0,
    )


def test_table_select_cte(session):
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df.write.save_as_table(table_name, table_type="temp")
    df = session.table(table_name)
    df_result = df.with_column("add_one", col("a") + 1).union(
        df.with_column("add_two", col("a") + 2)
    )
    check_result(
        session,
        df_result,
        expect_cte_optimized=False,
        query_count=1,
        describe_count=0,
        union_count=1,
        join_count=0,
    )


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-609328: support caplog in SP regression test"
)
@sql_count_checker(query_count=0)
def test_cte_optimization_enabled_parameter(session, caplog):
    with caplog.at_level(logging.WARNING):
        session.cte_optimization_enabled = True
    assert "cte_optimization_enabled is experimental" in caplog.text
