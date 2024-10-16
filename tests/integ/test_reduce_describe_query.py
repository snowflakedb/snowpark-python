#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import List

import pytest

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark.functions import col
from snowflake.snowpark.session import (
    _PYTHON_SNOWPARK_REDUCE_DESCRIBE_QUERY_ENABLED,
    Session,
)
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import IS_IN_STORED_PROC

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="Reducing describe queries is not supported in Local Testing",
    ),
]


@pytest.fixture(scope="module", autouse=True)
def setup(session):
    is_reduce_describe_query_enabled = session.reduce_describe_query_enabled
    session.reduce_describe_query_enabled = True
    yield
    session.reduce_describe_query_enabled = is_reduce_describe_query_enabled


# TODO SNOW-1728988: add more test cases with select after caching attributes on SelectStatement
# Create from SQL
create_from_sql_funcs = [
    lambda session: session.sql("SELECT 1 AS a, 2 AS b"),
]

# Create from Values
create_from_values_funcs = []

# Create from Table
create_from_table_funcs = [
    lambda session: session.create_dataframe(
        [[1, 2], [3, 4]], schema=["a", "b"]
    ).cache_result(),
]

# Create from SnowflakePlan
create_from_snowflake_plan_funcs = [
    lambda session: session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    .group_by("a")
    .count(),
    lambda session: session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).join(
        session.sql("SELECT 1 AS a, 2 AS b")
    ),
    lambda session: session.create_dataframe(
        [[1, 2], [3, 4]], schema=["a", "b"]
    ).rename({"b": "c"}),
]

metadata_no_change_df_ops = [
    lambda df: df.filter(col("a") > 2),
    lambda df: df.filter((col("a") - 2) > 2),
    lambda df: df.sort(col("a").desc()),
    lambda df: df.sort(-col("a")),
    lambda df: df.limit(2),
    lambda df: df.filter(col("a") > 2).sort(col("a").desc()).limit(2),
    lambda df: df.sample(0.5),
    lambda df: df.sample(0.5).filter(col("a") > 2),
    lambda df: df.filter(col("a") > 2).sample(0.5),
]


def check_attributes_equality(attrs1: List[Attribute], attrs2: List[Attribute]) -> None:
    for attr1, attr2 in zip(attrs1, attrs2):
        assert attr1.name == attr2.name
        assert attr1.datatype == attr2.datatype
        assert attr1.nullable == attr2.nullable


@pytest.mark.parametrize(
    "action",
    metadata_no_change_df_ops,
)
@pytest.mark.parametrize(
    "create_df_func",
    create_from_sql_funcs
    + create_from_values_funcs
    + create_from_table_funcs
    + create_from_snowflake_plan_funcs,
)
def test_metadata_no_change(session, action, create_df_func):
    df = create_df_func(session)
    with SqlCounter(query_count=0, describe_count=1):
        attributes = df._plan.attributes
    df = action(df)
    check_attributes_equality(df._plan._attributes, attributes)
    with SqlCounter(query_count=0, describe_count=0):
        _ = df.schema
        _ = df.columns


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Can't create a session in SP")
def test_reduce_describe_query_enabled_on_session(db_parameters):
    with Session.builder.configs(db_parameters).create() as new_session:
        default_value = new_session.reduce_describe_query_enabled
        new_session.reduce_describe_query_enabled = not default_value
        assert new_session.reduce_describe_query_enabled is not default_value
        new_session.reduce_describe_query_enabled = default_value
        assert new_session.reduce_describe_query_enabled is default_value

        parameters = db_parameters.copy()
        parameters["session_parameters"] = {
            _PYTHON_SNOWPARK_REDUCE_DESCRIBE_QUERY_ENABLED: not default_value
        }
        with Session.builder.configs(parameters).create() as new_session2:
            assert new_session2.reduce_describe_query_enabled is not default_value
