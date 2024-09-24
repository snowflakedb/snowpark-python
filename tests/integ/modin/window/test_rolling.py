#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import re

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


@agg_func
@min_periods
@pytest.mark.parametrize("window_time_period", ["2s", "3s", "10s"])
@sql_count_checker(query_count=1)
def test_rolling_time_period(agg_func, min_periods, window_time_period):
    native_df = native_pd.DataFrame(
        {"B": [0, 1, 2, np.nan, 4]},
        index=[
            native_pd.Timestamp("20130101 09:00:00"),
            native_pd.Timestamp("20130101 09:00:02"),
            native_pd.Timestamp("20130101 09:00:03"),
            native_pd.Timestamp("20130101 09:00:04"),
            native_pd.Timestamp("20130101 09:00:06"),
        ],
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(
            df.rolling(window=window_time_period, min_periods=min_periods),
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
def test_rolling_time_window_negative():
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
    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            "'center=True' is not implemented with str window for Rolling.sum"
        ),
    ):
        snow_df.rolling(window="2s", center=True).sum()
    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            "Snowpark pandas 'Rolling' does not yet support negative time 'window' offset"
        ),
    ):
        snow_df.rolling(window="-2s").sum()
    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            "Snowpark pandas does not yet support Rolling with windows that are not strings or integers"
        ),
    ):
        snow_df.rolling(window=pd.to_timedelta("1s")).sum()


@sql_count_checker(query_count=0)
def test_rolling_window_multiindex():
    native_df = native_pd.DataFrame(
        {
            "A": [
                native_pd.to_datetime("2020-01-01"),
                native_pd.to_datetime("2020-01-02"),
                native_pd.to_datetime("2020-01-03"),
            ],
            "B": native_pd.date_range("2020", periods=3),
            "C": [1, 2, 3],
        },
    )
    native_df = native_df.set_index(["A", "B"])

    # pandas throws a ValueError when multiindex used with Rolling, whereas Snowpark pandas
    # throws a NotImplementedError succeeds
    with pytest.raises(ValueError, match="window must be an integer 0 or greater"):
        native_df.rolling("2D").sum()

    snow_df = pd.DataFrame(native_df)
    with pytest.raises(
        ValueError, match="Rolling behavior is undefined when used with a MultiIndex"
    ):
        snow_df.rolling("2D").sum()


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
def test_rolling_aggregation_unsupported(agg_func, agg_func_kwargs):
    snow_df = pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    with pytest.raises(NotImplementedError):
        getattr(snow_df.rolling(window=2, min_periods=1), agg_func)(agg_func_kwargs)
