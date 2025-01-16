#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("func", ["backfill", "bfill", "ffill", "pad"])
@sql_count_checker(query_count=1)
def test_df_fill(func):
    native_df = native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
            [3, np.nan, 4, np.nan],
        ],
        columns=list("ABCD"),
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, func)(),
    )


@pytest.mark.parametrize("func", ["backfill", "bfill", "ffill", "pad"])
@sql_count_checker(query_count=1)
def test_df_timedelta_fill(func):
    native_df = native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
            [3, np.nan, 4, np.nan],
        ],
        columns=list("ABCD"),
    ).astype("timedelta64[ns]")
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, func)(),
    )
