#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import re

import pytest

from snowflake.snowpark._internal.analyzer import analyzer
from snowflake.snowpark._internal.utils import (
    TEMP_OBJECT_NAME_PREFIX,
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import col, when_matched
from tests.utils import Utils

WITH = "WITH"


@pytest.fixture(autouse=True)
def setup(session):
    # TODO SNOW-106671: enable cte optimization with sql simplifier
    is_sql_simplifier_enabled = session._sql_simplifier_enabled
    is_cte_optimization_enabled = session._cte_optimization_enabled
    session._sql_simplifier_enabled = False
    session._cte_optimization_enabled = True
    yield
    session._sql_simplifier_enabled = is_sql_simplifier_enabled
    session._cte_optimization_enabled = is_cte_optimization_enabled


def check_result(session, df, expect_cte_optimized):
    session._cte_optimization_enabled = False
    result = df.collect()

    session._cte_optimization_enabled = True
    cte_result = df.collect()

    Utils.check_answer(cte_result, result)
    last_query = df.queries["queries"][-1]
    if expect_cte_optimized:
        assert last_query.startswith(WITH)
        assert last_query.count(WITH) == 1
    else:
        assert last_query.count(WITH) == 0


def count_number_of_ctes(query):
    # a CTE is represented with a pattern `SNOWPARK_TEMP_xxx AS`
    pattern = re.compile(rf"{TEMP_OBJECT_NAME_PREFIX}TABLE_[0-9A-Z]+\sAS")
    return len(pattern.findall(query))


@pytest.mark.parametrize(
    "action",
    [
        lambda x: x.select("a", "b").select("b"),
        lambda x: x.filter(col("a") == 1).select("b"),
        lambda x: x.select("a").filter(col("a") == 1),
    ],
)
def test_no_duplicate_unary(session, action):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    check_result(session, action(df), expect_cte_optimized=False)


@pytest.mark.parametrize(
    "action",
    [
        lambda x, y: x.union_all(y),
        lambda x, y: x.select("a").union_all(y.select("a")),
        lambda x, y: x.except_(y),
        lambda x, y: x.select("a").except_(y.select("a")),
        lambda x, y: x.join(y.select("a", "b"), rsuffix="_y"),
        lambda x, y: x.select("a").join(y, rsuffix="_y"),
        lambda x, y: x.join(y.select("a"), rsuffix="_y"),
    ],
)
def test_binary(session, action):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    check_result(session, action(df, df), expect_cte_optimized=True)

    df1 = session.create_dataframe([[3, 4], [2, 1]], schema=["a", "b"])
    check_result(session, action(df, df1), expect_cte_optimized=False)

    # multiple queries
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df2 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold
    check_result(session, action(df2, df2), expect_cte_optimized=True)


@pytest.mark.parametrize(
    "action",
    [
        lambda x, y: x.union_all(y),
        lambda x, y: x.join(y.select("a")),
    ],
)
def test_number_of_ctes(session, action):
    df3 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df2 = df3.filter(col("a") == 1)
    df1 = df2.select("*")

    # only df1 will be converted to a CTE
    root = action(df1, df1)
    check_result(session, root, expect_cte_optimized=True)
    assert count_number_of_ctes(root.queries["queries"][-1]) == 1

    # df1 and df3 will be converted to CTEs
    root = action(root, df3)
    check_result(session, root, expect_cte_optimized=True)
    assert count_number_of_ctes(root.queries["queries"][-1]) == 2

    # df1, df2 and df3 will be converted to CTEs
    root = action(root, df2)
    check_result(session, root, expect_cte_optimized=True)
    assert count_number_of_ctes(root.queries["queries"][-1]) == 3


def test_different_df_same_query(session):
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).select("a")
    df2 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).select("a")
    df = df2.union_all(df1)
    check_result(session, df, expect_cte_optimized=True)
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
    check_result(session, df_result1, expect_cte_optimized=True)
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
    check_result(session, df_result2, expect_cte_optimized=True)
    assert count_number_of_ctes(df_result2.queries["queries"][-1]) == 3


@pytest.mark.parametrize("mode", ["append", "overwrite", "errorifexists", "ignore"])
def test_save_as_table(session, mode):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    with session.query_history() as query_history:
        df.union_all(df).write.save_as_table(
            random_name_for_temp_object(TempObjectType.TABLE),
            table_type="temp",
            mode=mode,
        )
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert count_number_of_ctes(query) == 1


def test_create_or_replace_view(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    with session.query_history() as query_history:
        df.union_all(df).create_or_replace_temp_view(
            random_name_for_temp_object(TempObjectType.VIEW)
        )
    query = query_history.queries[-1].sql_text
    assert query.count(WITH) == 1
    assert count_number_of_ctes(query) == 1


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


def test_explain(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    explain_string = df.union_all(df)._explain_string()
    assert "WithReference" in explain_string
    assert "WithClause" in explain_string
