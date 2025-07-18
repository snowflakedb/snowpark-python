#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest
from modin.config import Backend
from modin.tests.pandas.utils import df_equals

from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from tests.integ.utils.sql_counter import sql_count_checker
from tests.parameters import CONNECTION_PARAMETERS
from tests.utils import Utils


Backend.set_active_backends(("Ray", "Pandas", "Python_Test", "Snowflake"))


class MockQueryCompiler:
    def get_backend(self):
        return "UnsupportedBackend"


@pytest.fixture(scope="module")
def snowflake_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "name varchar, age integer, gpa float")
    session.sql(f"insert into {table_name} values ('Alice', 20, 3.5)").collect()
    session.sql(f"insert into {table_name} values ('Bob', 21, 3.8)").collect()
    session.sql(f"insert into {table_name} values ('Charlie', 22, 3.2)").collect()

    try:
        yield table_name
    finally:
        Utils.drop_table(session, table_name)


@pytest.fixture()
def pandas_df():
    pandas_df = native_pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "age": [20, 21, 22],
            "gpa": [3.5, 3.8, 3.2],
        }
    )
    return pandas_df


@sql_count_checker(query_count=1)
def test_unsupported_movement(session, snowflake_table):
    Backend.put("Snowflake")
    snow_df = pd.read_snowflake(snowflake_table)
    mock_qc = MockQueryCompiler()
    move_to_result = snow_df._query_compiler._move_to("UnsupportedBackend")
    move_from_result = SnowflakeQueryCompiler._move_from(mock_qc)
    assert move_to_result is NotImplemented
    assert move_from_result is NotImplemented


@sql_count_checker(query_count=4)
def test_move_to_ray(session, snowflake_table):
    Backend.put("Snowflake")
    snow_df = pd.read_snowflake(snowflake_table)
    result = snow_df._query_compiler._move_to("Ray", enforce_ordering=True)
    result_df = pd.DataFrame(query_compiler=result)
    assert Backend.get() == "Snowflake"
    df_equals(result_df, snow_df)


@pytest.mark.parametrize("enforce_ordering", [True, False])
@sql_count_checker(query_count=3)
def test_move_from_ray(session, pandas_df, enforce_ordering):
    Backend.put("Ray")
    ray_df = pd.DataFrame(pandas_df)
    result = SnowflakeQueryCompiler._move_from(
        ray_df._query_compiler, enforce_ordering=enforce_ordering
    )
    result_df = pd.DataFrame(query_compiler=result)
    assert Backend.get() == "Ray"
    df_equals(result_df, ray_df)


@pytest.mark.parametrize("enforce_ordering", [True, False])
@sql_count_checker(query_count=4)
def test_move_from_ray_multiple_sessions(session, pandas_df, enforce_ordering):
    Backend.put("Ray")
    ray_df = pd.DataFrame(pandas_df)
    result = SnowflakeQueryCompiler._move_from(
        ray_df._query_compiler,
        enforce_ordering=enforce_ordering,
        max_sessions=2,
        connection_params=CONNECTION_PARAMETERS,
    )
    result_df = pd.DataFrame(query_compiler=result)
    assert Backend.get() == "Ray"
    df_equals(result_df, ray_df)
