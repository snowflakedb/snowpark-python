#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
import pytest
from tests.integ.modin.utils import eval_snowpark_pandas_result, create_test_dfs
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

IMPLEMENTED_GROUPBY_RESAMPLE_FUNCTIONS = ["max", "mean", "median", "min", "sum"]
IMPLEMENTED_GROUPBY_RESAMPLE_DATEOFFSET_STRINGS = ["s", "min", "h", "D"]

agg_func = pytest.mark.parametrize("agg_func", IMPLEMENTED_GROUPBY_RESAMPLE_FUNCTIONS)
freq = pytest.mark.parametrize("freq", IMPLEMENTED_GROUPBY_RESAMPLE_DATEOFFSET_STRINGS)
interval = pytest.mark.parametrize("interval", [1, 3, 5, 15])


@freq
@interval
@agg_func
def test_groupby_resample_by_a(freq, interval, agg_func):
    rule = f"{interval}{freq}"
    date_idx = native_pd.to_datetime(
        [
            "2000-01-01 00:00:00",
            "2000-01-01 00:01:00",
            "2000-01-01 00:02:00",
            "2000-01-01 00:06:00",
            "2000-01-01 00:07:00",
        ]
    )

    with SqlCounter(query_count=6):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {"a": [0, 0, 5, 0, 5], "b": [1, 1, 1, 1, 1], "c": [2, 2, 2, 2, 2]},
                index=date_idx,
            ),
            lambda df: getattr(
                df.groupby("a").resample(rule=rule, include_groups=False),
                agg_func,
            )(),
        )


@freq
@interval
@agg_func
def test_groupby_resample_by_a_closed_left(freq, interval, agg_func):
    rule = f"{interval}{freq}"
    date_idx = native_pd.to_datetime(
        [
            "2000-01-01 00:00:00",
            "2000-01-01 00:01:00",
            "2000-01-01 00:02:00",
            "2000-01-01 00:06:00",
            "2000-01-01 00:07:00",
        ]
    )
    with SqlCounter(query_count=6):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {"a": [0, 0, 5, 0, 5], "b": [1, 1, 1, 1, 1], "c": [2, 2, 2, 2, 2]},
                index=date_idx,
            ),
            lambda df: getattr(
                df.groupby("a").resample(
                    rule=rule, closed="left", include_groups=False
                ),
                agg_func,
            )(),
        )


@freq
def test_groupby_resample_by_a_diff_freq(freq):
    idx = native_pd.date_range("1/1/2000", periods=8, freq=f"{freq}")
    pandas_df = native_pd.DataFrame(
        data=8 * [range(3)], index=idx, columns=["a", "b", "c"]
    )
    snow_df = pd.DataFrame(pandas_df)
    rule = f"1{freq}"
    with SqlCounter(query_count=4):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby("a").resample(rule=rule, include_groups=False).sum(),
        )


@freq
@interval
@agg_func
def test_groupby_resample_by_b(freq, interval, agg_func):
    date_idx = native_pd.to_datetime(
        [
            "2000-01-01 00:00:00",
            "2000-01-01 00:01:00",
            "2000-01-01 00:02:00",
            "2000-01-01 00:06:00",
            "2000-01-01 00:07:00",
        ]
    )
    rule = f"{interval}{freq}"
    with SqlCounter(query_count=5):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {"a": [0, 0, 5, 0, 5], "b": [1, 1, 1, 1, 1], "c": [2, 2, 2, 2, 2]},
                index=date_idx,
            ),
            lambda df: getattr(
                df.groupby("b").resample(rule=rule, include_groups=False),
                agg_func,
            )(),
        )


@freq
@interval
@agg_func
def test_groupby_resample_multiple_by_cols(freq, interval, agg_func):
    date_idx = native_pd.to_datetime(
        [
            "2000-01-01 00:00:00",
            "2000-01-01 00:01:00",
            "2000-01-01 00:02:00",
            "2000-01-01 00:06:00",
            "2000-01-01 00:07:00",
        ]
    )
    rule = f"{interval}{freq}"
    with SqlCounter(query_count=6):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {"a": [0, 0, 5, 0, 5], "b": [1, 1, 1, 1, 1], "c": [2, 2, 2, 2, 2]},
                index=date_idx,
            ),
            lambda df: getattr(
                df.groupby(["a", "b"]).resample(rule=rule, include_groups=False),
                agg_func,
            )(),
        )


@pytest.mark.parametrize("freq", ["W", "ME", "YE"])
@sql_count_checker(query_count=0)
def test_groupby_resample_week_to_year_negative(freq):
    idx = pd.date_range("1/1/2000", periods=8, freq="min")
    snow_df = pd.DataFrame(data=8 * [range(3)], index=idx, columns=["a", "b", "c"])
    rule = f"1{freq}"
    with pytest.raises(
        NotImplementedError,
        match=f"Groupby resample with rule offset {rule} is not yet implemented.",
    ):
        snow_df.groupby("a").resample(rule=rule, include_groups=False).sum()


@sql_count_checker(query_count=0)
def test_resample_series_negative():
    date_idx = pd.date_range("1/1/2000", periods=8, freq="min")
    date_idx.names = ["grp_col"]
    snow_ser = pd.Series(data=[0, 1, 1, 4, 4, 5, 0, 1], index=date_idx, name="a")
    with pytest.raises(
        NotImplementedError, match="Series GroupbyResampler is not yet implemented."
    ):
        snow_ser.groupby(by="grp_col").resample(rule="3min", include_groups=False).sum()
