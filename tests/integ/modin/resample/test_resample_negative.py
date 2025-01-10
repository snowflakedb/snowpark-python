#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.resample_utils import (
    NOT_IMPLEMENTED_DATEOFFSET_STRINGS,
    UNSUPPORTED_DATEOFFSET_STRINGS,
)
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("index_col", [["datecol", "B"], ["A", "B"], ["A"]])
@sql_count_checker(query_count=0)
def test_resample_invalid_index_type_negative(index_col):
    snow_df = pd.DataFrame(
        {
            "A": np.random.randn(15),
            "B": np.random.randn(15),
            "datecol": native_pd.date_range("2020-01-01", periods=15, freq="1D"),
        }
    )

    multi_index_error_msg = ", but got an instance of 'MultiIndex'"
    snow_df = snow_df.set_index(index_col)
    with pytest.raises(
        TypeError,
        match=f"Only valid with DatetimeIndex{multi_index_error_msg if len(index_col) > 1 else ''}",
    ):
        snow_df.resample(rule="2D").mean()


@pytest.mark.parametrize("invalid_rule", ["3KQ", {"qweqwe": 123}])
@sql_count_checker(query_count=0)
def test_resample_invalid_rule_negative(invalid_rule):
    snow_df = pd.DataFrame(
        {"A": np.random.randn(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
    )

    with pytest.raises(ValueError, match=f"Invalid frequency: {invalid_rule}"):
        snow_df.resample(rule=invalid_rule).mean()


@pytest.mark.parametrize("freq", UNSUPPORTED_DATEOFFSET_STRINGS)
@sql_count_checker(query_count=0)
def test_resample_unsupported_freq_negative(freq):
    snow_df = pd.DataFrame(
        {"A": np.random.randn(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
    )

    with pytest.raises(
        NotImplementedError,
        match=f"Unsupported frequency: {freq}. Snowpark pandas cannot map {freq} to a Snowflake date or time unit.",
    ):
        snow_df.resample(rule=freq).max().to_pandas()


@pytest.mark.parametrize("freq", NOT_IMPLEMENTED_DATEOFFSET_STRINGS)
@sql_count_checker(query_count=0)
def test_resample_not_yet_implemented_freq(freq):
    snow_df = pd.DataFrame(
        {"A": np.random.randn(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
    )

    with pytest.raises(NotImplementedError):
        snow_df.resample(freq).min().to_pandas()


@sql_count_checker(query_count=0)
def test_resample_not_yet_implemented_closed():
    snow_df = pd.DataFrame(
        {"A": np.random.randn(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
    )

    with pytest.raises(
        NotImplementedError,
        match="resample with rule offset 3ME is only implemented with closed='left'",
    ):
        snow_df.resample("3ME").min().to_pandas()


@pytest.mark.parametrize(
    "func",
    [
        lambda re: re.max(numeric_only=True),
        lambda re: re.min(numeric_only=True),
        lambda re: re.mean(numeric_only=True),
    ],
)
@sql_count_checker(query_count=0)
def test_series_agg_numeric_true_raises(func):
    snow_ser = pd.Series(
        range(15), index=native_pd.date_range("2020-01-01", periods=15, freq="1D")
    )
    resampler = snow_ser.resample("1D")
    msg = "Series Resampler does not implement numeric_only."
    with pytest.raises(NotImplementedError, match=msg):
        func(resampler)


@sql_count_checker(query_count=0)
def test_resample_ffill_limit_negative():
    snow_df = pd.DataFrame(
        {"a": range(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
    )
    with pytest.raises(
        NotImplementedError,
        match="Parameter limit of resample.ffill has not been implemented",
    ):
        snow_df.resample("1D").ffill(limit=10)


@sql_count_checker(query_count=0)
def test_resample_fillna_method_not_implemented():
    snow_df = pd.DataFrame(
        {"a": range(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
    )
    with pytest.raises(
        NotImplementedError,
        match="Method nearest is not implemented for Resampler!",
    ):
        snow_df.resample("1D").fillna(method="nearest")


@sql_count_checker(query_count=0)
def test_resample_fillna_invalid_method():
    native_df = native_pd.DataFrame(
        {"a": range(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.resample("5D").fillna("invalid_method"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=r"Invalid fill method. Expecting pad \(ffill\), backfill \(bfill\) or nearest. Got invalid_method",
    )


@sql_count_checker(query_count=1)
def test_resample_tz_negative():
    snow_df = pd.DataFrame(
        {"a": range(3)},
        index=native_pd.to_datetime(
            [
                "2014-08-01 09:00:00+02:00",
                "2014-08-01 10:00:00+02:00",
                "2014-08-01 11:00:00+02:00",
            ]
        ),
    )
    with pytest.raises(
        TypeError,
        match="Cannot subtract tz-naive and tz-aware datetime-like objects.",
    ):
        snow_df.resample("2D").min()


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="SNOW-1704430")
def test_resample_sum_timedelta_with_duplicate_column_label_changes_type_to_int():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [[pd.Timedelta(1), 2]], columns=["a", "a"], index=[pd.Timestamp(1)]
        ),
        lambda df: df.resample("1s").sum(),
    )
