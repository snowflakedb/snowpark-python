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
@pytest.mark.parametrize(
    "data", [[1, 2, 3, None, 5], [10.01, 20.02, 30.03, 40.04, None]]
)
@pytest.mark.parametrize("func_name", ["cumsum", "cummin", "cummax"])
@pytest.mark.parametrize("dropna", [True, False])
def test_groupby_cumulative_series(data, func_name, dropna):
    pandas_df = native_pd.Series(
        data, index=["tuna", "salmon", "catfish", "tuna", None]
    )
    snow_df = pd.Series(pandas_df)
    eval_snowpark_pandas_result(
        snow_df,
        pandas_df,
        lambda df: getattr(df.groupby(level=0, dropna=dropna), func_name)(),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "data",
    [
        [1, 2, 3, None, 5],
        [10.01, 20.02, 30.03, 40.04, None],
        ["une", None, "trois", "quatre", "cinq"],
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
def test_groupby_cumcount_series(data, dropna, ascending):
    pandas_df = native_pd.Series(
        data, index=["tuna", "salmon", "catfish", "tuna", None]
    )
    snow_df = pd.Series(pandas_df)
    eval_snowpark_pandas_result(
        snow_df,
        pandas_df,
        lambda df: df.groupby(level=0, dropna=dropna).cumcount(ascending=ascending),
    )
