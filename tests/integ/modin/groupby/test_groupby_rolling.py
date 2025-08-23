#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
import pytest
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.window.utils import (
    agg_func,
)

window = pytest.mark.parametrize("window", [1, 2, 3, 4, 6])
min_periods = pytest.mark.parametrize("min_periods", [None, 1, 2])
center = pytest.mark.parametrize("center", [True, False])


@agg_func
@window
@min_periods
@center
def test_rolling_dataframe(window, min_periods, center, agg_func):
    native_df = native_pd.DataFrame(
        {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]}
    )
    snow_df = pd.DataFrame(native_df)
    if min_periods is not None and min_periods > window:
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.groupby("A").rolling(
                        window=window, min_periods=min_periods, center=center
                    ),
                    agg_func,
                )(numeric_only=True),
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match=f"min_periods {min_periods} must be <= window {window}",
                test_attrs=False,
            )
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.groupby("A").rolling(
                        window=window, min_periods=min_periods, center=center
                    ),
                    agg_func,
                )(numeric_only=True),
                test_attrs=False,
            )


@agg_func
@window
@min_periods
@center
def test_rolling_null_dataframe(window, min_periods, center, agg_func):
    native_df = native_pd.DataFrame(
        {
            "A": [1, 1, np.nan, 2],
            "B": [1, np.nan, 3, 4],
            "C": [np.nan, np.nan, np.nan, np.nan],
        }
    )
    snow_df = pd.DataFrame(native_df)
    if min_periods is not None and min_periods > window:
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.groupby("A").rolling(
                        window=window, min_periods=min_periods, center=center
                    ),
                    agg_func,
                )(numeric_only=True),
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match=f"min_periods {min_periods} must be <= window {window}",
                test_attrs=False,
            )
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.groupby("A").rolling(
                        window=window, min_periods=min_periods, center=center
                    ),
                    agg_func,
                )(numeric_only=True),
                test_attrs=False,
            )


@sql_count_checker(query_count=1)
def test_groupby_rolling_by_mul_cols():
    native_df = native_pd.DataFrame(
        {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]}
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(["A", "B"]).rolling(2).sum(),
        test_attrs=False,
    )
