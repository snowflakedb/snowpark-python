#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    assert_series_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)

agg_func = pytest.mark.parametrize(
    "agg_func", ["count", "sum", "mean", "var", "std", "min", "max", "sem"]
)
window = pytest.mark.parametrize("window", [1, 2, 3, 4, 6])
min_periods = pytest.mark.parametrize("min_periods", [1, 2])
center = pytest.mark.parametrize("center", [True, False])


@agg_func
@window
@min_periods
@center
def test_rolling_dataframe(window, min_periods, center, agg_func):
    native_df = native_pd.DataFrame(
        {"A": ["h", "e", "l", "l", "o"], "B": [0, -1, 2.5, np.nan, 4]}
    )
    snow_df = pd.DataFrame(native_df)
    if min_periods > window:
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.rolling(window=window, min_periods=min_periods, center=center),
                    agg_func,
                )(numeric_only=True),
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match=f"min_periods {min_periods} must be <= window {window}",
            )
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.rolling(window=window, min_periods=min_periods, center=center),
                    agg_func,
                )(numeric_only=True),
            )


@agg_func
@window
@min_periods
@center
def test_rolling_null_dataframe(window, min_periods, center, agg_func):
    native_df = native_pd.DataFrame(
        {
            "A": ["h", np.nan, "l", "l", "o"],
            "B": [np.nan, np.nan, np.nan, np.nan, np.nan],
        }
    )
    snow_df = pd.DataFrame(native_df)
    if min_periods > window:
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.rolling(window=window, min_periods=min_periods, center=center),
                    agg_func,
                )(numeric_only=True),
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match=f"min_periods {min_periods} must be <= window {window}",
            )
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: getattr(
                    df.rolling(window=window, min_periods=min_periods, center=center),
                    agg_func,
                )(numeric_only=True),
            )


@agg_func
@window
@min_periods
@center
def test_rolling_series(window, min_periods, center, agg_func):
    native_series = native_pd.Series([0, -1, 2.5, np.nan, 4])
    snow_series = pd.Series(native_series)
    if min_periods > window:
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_series,
                native_series,
                lambda series: getattr(
                    series.rolling(
                        window=window, min_periods=min_periods, center=center
                    ),
                    agg_func,
                )(),
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match=f"min_periods {min_periods} must be <= window {window}",
            )
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_series,
                native_series,
                lambda series: getattr(
                    series.rolling(
                        window=window, min_periods=min_periods, center=center
                    ),
                    agg_func,
                )(),
            )


@pytest.mark.parametrize("ddof", [-1, 0, 0.5, 1, 2])
@sql_count_checker(query_count=1)
def test_rolling_sem_ddof(ddof):
    native_df = native_pd.DataFrame(
        {"A": ["h", "e", "l", "l", "o"], "B": [0, -1, 2.5, np.nan, 4]}
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.rolling(window=2, min_periods=1).sem(
            numeric_only=True, ddof=ddof
        ),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_rolling_corr_simple():
    native_df = native_pd.DataFrame({"col1": [1, 4, 3]})
    other_native_df = native_pd.DataFrame({"col1": [1, 6, 3]})
    snow_df = pd.DataFrame(native_df)
    other_snow_df = pd.DataFrame(other_native_df)
    snow_df = snow_df.rolling(window=3, min_periods=3).corr(
        other=other_snow_df,
        pairwise=None,
        ddof=1,
        numeric_only=True,
    )
    native_df = native_df.rolling(window=3, min_periods=3).corr(
        other=other_native_df,
        pairwise=None,
        ddof=1,
        numeric_only=True,
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=1, join_count=1)
def test_rolling_corr_multi_cols():
    native_df = native_pd.DataFrame({"col1": [1, 2, 3, 4, 1], "col3": [1, 2, 3, 4, 5]})
    other_native_df = native_pd.DataFrame(
        {"col2": [1, 2, 3, 4, 5], "col3": [1, 2, 3, 7, 6], "col4": [1, 1, 3, 6, 5]}
    )
    snow_df = pd.DataFrame(native_df)
    other_snow_df = pd.DataFrame(other_native_df)
    snow_df = snow_df.rolling(window=3, min_periods=3).corr(
        other=other_snow_df,
        pairwise=None,
        ddof=1,
        numeric_only=True,
    )
    native_df = native_df.rolling(window=3, min_periods=3).corr(
        other=other_native_df,
        pairwise=None,
        ddof=1,
        numeric_only=True,
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=1, join_count=1)
def test_rolling_corr_series():
    native_ser = native_pd.Series([1, 4, 3])
    other_native_ser = native_pd.Series([1, 6, 3])
    snow_ser = pd.Series(native_ser)
    other_snow_ser = pd.Series(other_native_ser)
    snow_df = snow_ser.rolling(window=3, min_periods=3).corr(
        other=other_snow_ser,
        pairwise=None,
        ddof=1,
        numeric_only=True,
    )
    native_df = native_ser.rolling(window=3, min_periods=3).corr(
        other=other_native_ser,
        pairwise=None,
        ddof=1,
        numeric_only=True,
    )
    assert_series_equal(snow_df, native_df)


@sql_count_checker(query_count=1, join_count=1)
def test_rolling_corr_nulls():
    native_df = native_pd.DataFrame(
        {"col1": [1, 2, np.nan, 4, 1], "col3": [1, np.nan, 3, 4, 5]}
    )
    other_native_df = native_pd.DataFrame(
        {
            "col2": [1, 2, 3, 4, 5],
            "col3": [1, np.nan, 3, np.nan, 6],
            "col4": [1, 1, 3, np.nan, 5],
        }
    )
    snow_df = pd.DataFrame(native_df)
    other_snow_df = pd.DataFrame(other_native_df)
    snow_df = snow_df.rolling(window=3, min_periods=3).corr(
        other=other_snow_df,
        pairwise=None,
        ddof=1,
        numeric_only=True,
    )
    native_df = native_df.rolling(window=3, min_periods=3).corr(
        other=other_native_df,
        pairwise=None,
        ddof=1,
        numeric_only=True,
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=0)
def test_rolling_corr_negative():
    native_df = native_pd.DataFrame(
        {"col1": [1, 2, np.nan, 4, 1], "col3": [1, np.nan, 3, 4, 5]}
    )
    other_native_df = native_pd.DataFrame(
        {
            "col2": [1, 2, 3, 4, 5],
            "col3": [1, np.nan, 3, np.nan, 6],
            "col4": [1, 1, 3, np.nan, 5],
        }
    )
    snow_df = pd.DataFrame(native_df)
    other_snow_df = pd.DataFrame(other_native_df)
    with pytest.raises(NotImplementedError):
        snow_df = snow_df.rolling(window=3, min_periods=2).corr(
            other=other_snow_df,
            pairwise=None,
            ddof=1,
            numeric_only=True,
        )
    with pytest.raises(NotImplementedError):
        snow_df = snow_df.rolling(window=3, min_periods=2).corr(
            pairwise=None,
            ddof=1,
            numeric_only=True,
        )
    with pytest.raises(NotImplementedError):
        snow_df = snow_df.rolling(window=3, min_periods=2).corr(
            pairwise=True,
            ddof=1,
            numeric_only=True,
        )
    with pytest.raises(NotImplementedError):
        snow_df = snow_df.rolling(window="a", min_periods=2).corr(
            pairwise=True,
            ddof=1,
            numeric_only=True,
        )


@sql_count_checker(query_count=0)
def test_rolling_window_negative():
    native_df = native_pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.rolling(window=None).sum(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="window must be an integer 0 or greater",
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.rolling(window=-2).sum(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="window must be an integer 0 or greater",
    )


@sql_count_checker(query_count=0)
def test_rolling_min_periods_negative():
    native_df = native_pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.rolling(window=2, min_periods="invalid_value").sum(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="min_periods must be an integer",
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.rolling(window=2, min_periods=-2).sum(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="min_periods must be >= 0",
    )


@sql_count_checker(query_count=0)
def test_rolling_center_negative():
    native_df = native_pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.rolling(window=2, center="invalid_value").sum(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="center must be a boolean",
    )


@sql_count_checker(query_count=0)
def test_rolling_window_unsupported():
    snow_df = pd.DataFrame(
        {"B": [0, 1, 2, np.nan, 4]},
        index=[
            pd.Timestamp("20130101 09:00:00"),
            pd.Timestamp("20130101 09:00:02"),
            pd.Timestamp("20130101 09:00:03"),
            pd.Timestamp("20130101 09:00:05"),
            pd.Timestamp("20130101 09:00:06"),
        ],
    )
    with pytest.raises(NotImplementedError):
        snow_df.rolling(window="2s", min_periods=None).sum()


@pytest.mark.parametrize(
    "function",
    [
        lambda df: df.rolling(2, min_periods=0).sum(),
        lambda df: df.rolling(2, min_periods=None).sum(),
        lambda df: df.rolling(2, win_type="barthann").sum(),
        lambda df: df.rolling(2, on="B").sum(),
        lambda df: df.rolling(2, axis=1).sum(),
        lambda df: df.rolling(2, closed="left").sum(),
        lambda df: df.rolling(2, step=2).sum(),
        lambda df: df.rolling(0, min_periods=0).sum(),
    ],
)
@sql_count_checker(query_count=0)
def test_rolling_params_unsupported(function):
    snow_df = pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    with pytest.raises(NotImplementedError):
        function(snow_df)


@pytest.mark.parametrize(
    "agg_func, agg_func_kwargs",
    [
        ("median", None),
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
def test_rolling_aggregation_unsupported(agg_func, agg_func_kwargs):
    snow_df = pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    with pytest.raises(NotImplementedError):
        getattr(snow_df.rolling(window=2, min_periods=1), agg_func)(agg_func_kwargs)
