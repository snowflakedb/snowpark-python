#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
@pytest.mark.parametrize(
    "data", [[1, 2, 3, 4, 5], ["une", "deux", "trois", "quatre", "cinq"]]
)
def test_groupby_shift_series(periods, data):
    pandas_df = native_pd.Series(
        data, index=["tuna", "salmon", "catfish", "goldfish", "shark"]
    )
    snow_df = pd.Series(pandas_df)
    eval_snowpark_pandas_result(
        snow_df, pandas_df, lambda df: df.groupby(level=0).shift(periods=periods)
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
@pytest.mark.parametrize("data", [["une", "deux", "trois", "quatre", "cinq"]])
@pytest.mark.parametrize("fill_value", ["sept", "something magical"])
def test_groupby_shift_series_fill_string(periods, data, fill_value):
    pandas_df = native_pd.Series(
        data, index=["tuna", "salmon", "catfish", "goldfish", "shark"]
    )
    snow_df = pd.Series(pandas_df)
    eval_snowpark_pandas_result(
        snow_df,
        pandas_df,
        lambda df: df.groupby(level=0).shift(periods=periods, fill_value=fill_value),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
@pytest.mark.parametrize("data", [[1, 2, 3, 4, 5]])
@pytest.mark.parametrize("fill_value", [10000, -10000])
def test_groupby_shift_series_fill_numeric(periods, data, fill_value):
    pandas_df = native_pd.Series(
        data, index=["tuna", "salmon", "catfish", "goldfish", "shark"]
    )
    snow_df = pd.Series(pandas_df)
    eval_snowpark_pandas_result(
        snow_df,
        pandas_df,
        lambda df: df.groupby(level=0).shift(periods=periods, fill_value=fill_value),
    )
