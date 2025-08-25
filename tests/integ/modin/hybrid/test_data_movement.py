#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys

import numpy as np
import pandas as native_pd
import modin.pandas as pd
import pytest
from modin.config import Backend, context as config_context
from modin.tests.pandas.utils import df_equals

from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_35_0
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from tests.integ.utils.sql_counter import sql_count_checker

Backend.set_active_backends(("Ray", "Pandas", "Python_Test", "Snowflake"))


class MockQueryCompiler:
    def get_backend(self):
        return "UnsupportedBackend"


@pytest.fixture()
def pandas_df():
    np.random.seed(42)
    rows, cols = 5000, 200
    data = {f"col_{i}": np.random.randint(0, 1000, size=rows) for i in range(cols)}
    index = native_pd.Index([f"index_{i}" for i in range(rows)], name="custom_index")
    pandas_df = native_pd.DataFrame(data, index=index)
    return pandas_df


@sql_count_checker(query_count=0)
@pytest.mark.skipif(
    not MODIN_IS_AT_LEAST_0_35_0, reason="Modin 0.35.0+ defines movement interface"
)
def test_unsupported_movement(session, pandas_df):
    with config_context(Backend="Snowflake", AutoSwitchBackend=False):
        snow_df = pd.DataFrame(pandas_df)
        mock_qc = MockQueryCompiler()
        move_to_result = snow_df._query_compiler.move_to("UnsupportedBackend")
        move_from_result = SnowflakeQueryCompiler.move_from(mock_qc)
        assert move_to_result is NotImplemented
        assert move_from_result is NotImplemented


@pytest.mark.skipif(
    sys.version_info.minor >= 12,
    reason="snowflake-ml-python for efficient movement is not installed above python 3.12",
)
@sql_count_checker(query_count=9)
def test_move_to_ray(session, pandas_df):
    with config_context(Backend="Snowflake", AutoSwitchBackend=False):
        snow_df = pd.DataFrame(pandas_df)
        assert snow_df.get_backend() == "Snowflake"
        result = snow_df._query_compiler.move_to("Ray")
        result_df = pd.DataFrame(query_compiler=result)
        assert result_df.get_backend() == "Ray"
        assert Backend.get() == "Snowflake"
        df_equals(result_df, snow_df)


@pytest.mark.skip(reason="SNOW-2276090")
@sql_count_checker(query_count=4)
def test_move_from_ray(session, pandas_df):
    with config_context(Backend="Ray", AutoSwitchBackend=False):
        ray_df = pd.DataFrame(pandas_df)
        result = SnowflakeQueryCompiler.move_from(ray_df._query_compiler)
        result_df = pd.DataFrame(query_compiler=result)
        assert result_df.get_backend() == "Snowflake"
        assert Backend.get() == "Ray"
        df_equals(result_df, ray_df)
