#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@sql_count_checker(query_count=1)
def test_df_ffill():
    native_df = native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
        ],
        columns=list("ABCD"),
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.ffill(),
    )


@sql_count_checker(query_count=1)
def test_df_pad():
    native_df = native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
        ],
        columns=list("ABCD"),
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.pad(),
    )
