#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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
from tests.integ.modin.sql_counter import sql_count_checker


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
def test_resample_ffill_negative():
    snow_df = pd.DataFrame(
        {"a": range(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
    )
    with pytest.raises(
        NotImplementedError,
        match="Parameter limit of resample.ffill has not been implemented",
    ):
        snow_df.resample("1D").ffill(limit=10)
