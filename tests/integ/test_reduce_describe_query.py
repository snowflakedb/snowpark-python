#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark.functions import col

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="Reducing describe queries is not supported in Local Testing",
    ),
]


paramList = [True, False]


@pytest.fixture(params=paramList, autouse=True)
def setup(request, session):
    is_reduce_describe_query_enabled = session.reduce_describe_query_enabled
    session.reduce_describe_query_enabled = request.param
    yield
    session.reduce_describe_query_enabled = is_reduce_describe_query_enabled


@pytest.mark.parametrize(
    "action",
    [
        lambda df: df.filter(col("a") > 2),
        lambda df: df.sort(col("a").desc()),
        lambda df: df.limit(2),
        lambda df: df.filter(col("a") > 2).sort(col("a").desc()).limit(2),
        lambda df: df.sample(0.5),
        lambda df: df.sample(0.5).filter(col("a") > 2),
        lambda df: df.filter(col("a") > 2).sample(0.5),
    ],
)
def test_schema_no_change(session, action):
    df = session.sql("SELECT 1 AS a, 2 as b")
    # df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    attributes = df._plan.attributes
    df = action(df)
    if session.reduce_describe_query_enabled:
        assert df._plan._attributes == attributes
        if df._select_statement:
            assert df._select_statement.from_._snowflake_plan._attributes == attributes
    else:
        assert df._plan._attributes is None
