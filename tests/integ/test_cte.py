#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import re

import pytest

from snowflake.snowpark._internal.analyzer import analyzer
from snowflake.snowpark._internal.utils import TEMP_OBJECT_NAME_PREFIX
from snowflake.snowpark.functions import col
from tests.utils import Utils

WITH = "WITH"
original_threshold = analyzer.ARRAY_BIND_THRESHOLD


@pytest.fixture(autouse=True)
def cleanup(session):
    yield
    analyzer.ARRAY_BIND_THRESHOLD = original_threshold


@pytest.fixture(params=[False, True])
def has_multi_queries(request):
    return request.param


@pytest.fixture(scope="function")
def df(session, has_multi_queries):
    # TODO SNOW-1020742: Integerate CTE support with sql simplifier
    session.sql_simplifier_enabled = False
    if has_multi_queries:
        analyzer.ARRAY_BIND_THRESHOLD = 2
    else:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold
    return session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])


def is_select_star_from_table_query(query):
    # check whether the query ends with `SELECT * FROM (SNOWPARK_TEMP_TABLE_XXX)`
    pattern = re.compile(
        rf"SELECT\s+\*\s+FROM \({TEMP_OBJECT_NAME_PREFIX}TABLE_[0-9A-Z]+\)$"
    )
    return bool(pattern.search(query))


def check_result(df_result, df_cte_result):
    Utils.check_answer(df_result, df_cte_result)
    last_query = df_cte_result.queries["queries"][-1]
    assert last_query.startswith(WITH)
    assert last_query.count(WITH) == 1

    # move the select sql to CTE
    df_cte_result2 = df_cte_result._as_cte()
    Utils.check_answer(df_result, df_cte_result2)
    last_query = df_cte_result2.queries["queries"][-1]
    assert last_query.startswith(WITH)
    assert last_query.count(WITH) == 1
    assert is_select_star_from_table_query(last_query)


@pytest.mark.parametrize(
    "action",
    [
        lambda x: x,
        lambda x: x.select("a"),
        lambda x: x.select("a", "b"),
        lambda x: x.select("*"),
        lambda x: x.select("a", "b").select("b"),
        lambda x: x.filter(col("a") == 1),
        lambda x: x.filter(col("a") == 1).select("b"),
        lambda x: x.select("a").filter(col("a") == 1),
        lambda x: x.sort("a", ascending=False),
        lambda x: x.filter(col("a") == 1).sort("a"),
        lambda x: x.limit(1),
        lambda x: x.sort("a").limit(1),
        lambda x: x.drop("b"),
        lambda x: x.select("a", "b").drop("b"),
        lambda x: x.agg({"a": "count", "b": "sum"}),
        lambda x: x.group_by("a").min("b"),
    ],
)
def test_basic(session, df, action):
    df_cte = df._as_cte()
    df_result, df_cte_result = action(df), action(df_cte)
    check_result(df_result, df_cte_result)


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
def test_binary(session, df, action):
    df_cte = df._as_cte()
    df_result = action(df, df)
    for df_cte_result in [
        action(df_cte, df),
        action(df, df_cte),
        action(df_cte, df_cte),
    ]:
        check_result(df_result, df_cte_result)
