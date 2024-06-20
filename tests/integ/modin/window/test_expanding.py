#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result

agg_func = pytest.mark.parametrize(
    "agg_func", ["count", "sum", "mean", "var", "std", "min", "max", "sem"]
)
min_periods = pytest.mark.parametrize("min_periods", [None, 0, 1, 2, 10])


@agg_func
@min_periods
@sql_count_checker(query_count=1)
def test_expanding_dataframe(agg_func, min_periods):
    native_df = native_pd.DataFrame(
        {"A": ["h", "e", "l", "l", "o"], "B": [0, -1, 2.5, np.nan, 4]}
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(
            df.expanding(min_periods),
            agg_func,
        )(numeric_only=True),
    )


@agg_func
@min_periods
@sql_count_checker(query_count=1)
def test_expanding_null_dataframe(agg_func, min_periods):
    native_df = native_pd.DataFrame(
        {
            "A": ["h", np.nan, "l", "l", "o"],
            "B": [np.nan, np.nan, np.nan, np.nan, np.nan],
        }
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(
            df.expanding(min_periods),
            agg_func,
        )(numeric_only=True),
    )


@agg_func
@min_periods
@sql_count_checker(query_count=1)
def test_expanding_series(agg_func, min_periods):
    native_series = native_pd.Series([0, -1, 2.5, np.nan, 4])
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda df: getattr(
            df.expanding(min_periods),
            agg_func,
        )(),
    )


@pytest.mark.parametrize("ddof", [-1, 0, 0.5, 1, 2])
def test_expanding_sem_ddof(ddof):
    with SqlCounter(query_count=1):
        native_df = native_pd.DataFrame(
            {"A": ["h", "e", "l", "l", "o"], "B": [0, -1, 2.5, np.nan, 4]}
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.expanding().sem(numeric_only=True, ddof=ddof),
        )
    with SqlCounter(query_count=1):
        native_df = native_pd.DataFrame(
            {
                "A": ["h", np.nan, "l", "l", "o"],
                "B": [np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.expanding(0).sem(numeric_only=True, ddof=ddof),
        )


@sql_count_checker(query_count=1)
def test_expanding_min_periods_default():
    native_df = native_pd.DataFrame(
        {"A": ["h", "e", "l", "l", "o"], "B": [0, -1, 2.5, np.nan, 4]}
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.expanding().min(numeric_only=True),
    )


@sql_count_checker(query_count=0)
def test_expanding_min_periods_negative():
    native_df = native_pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.expanding("invalid_value").min(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="min_periods must be an integer",
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.expanding(-2).min(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="min_periods must be >= 0",
    )


@pytest.mark.parametrize(
    "agg_func, agg_func_kwargs",
    [
        ("median", None),
        ("corr", None),
        ("cov", None),
        ("skew", None),
        ("kurt", None),
        ("apply", "min"),
        ("aggregate", "min"),
        ("quantile", 0.5),
        ("rank", None),
    ],
)
@sql_count_checker(query_count=0)
def test_expanding_aggregation_dataframe_unsupported(agg_func, agg_func_kwargs):
    snow_df = pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    with pytest.raises(NotImplementedError):
        getattr(snow_df.expanding(), agg_func)(agg_func_kwargs)


@pytest.mark.parametrize(
    "agg_func, agg_func_kwargs",
    [
        ("median", None),
        ("corr", None),
        ("cov", None),
        ("skew", None),
        ("kurt", None),
        ("apply", "min"),
        ("aggregate", "min"),
        ("quantile", 0.5),
        ("rank", None),
    ],
)
@sql_count_checker(query_count=0)
def test_expanding_aggregation_series_unsupported(agg_func, agg_func_kwargs):
    snow_df = pd.Series([2, 3, 4, 1], index=["a", "b", "c", "d"])
    with pytest.raises(NotImplementedError):
        getattr(snow_df.expanding(), agg_func)(agg_func_kwargs)
