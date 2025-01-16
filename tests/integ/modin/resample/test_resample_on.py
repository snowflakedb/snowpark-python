#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.resample_utils import (
    IMPLEMENTED_AGG_METHODS,
    IMPLEMENTED_DATEOFFSET_STRINGS,
)
from tests.integ.modin.utils import (
    create_test_dfs,
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


def eval_snowpark_pandas_result(*args, **kwargs):
    # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


agg_func = pytest.mark.parametrize(
    "agg_func", list(filter(lambda x: x not in ["indices"], IMPLEMENTED_AGG_METHODS))
)
freq = pytest.mark.parametrize("freq", IMPLEMENTED_DATEOFFSET_STRINGS)


@freq
@agg_func
# One extra query to convert index to native pandas for dataframe constructor
@sql_count_checker(query_count=3, join_count=1)
def test_resample_on(freq, agg_func):
    rule = f"2{freq}"
    # Note that supplying 'on' to Resampler replaces the existing index of the DataFrame with the 'on' column
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "A": np.random.randn(15),
                "B": native_pd.date_range("2020-01-01", periods=15, freq=f"1{freq}"),
            },
            index=native_pd.date_range("2020-10-01", periods=15, freq=f"1{freq}"),
        ),
        lambda df: getattr(df.resample(rule=rule, on="B", closed="left"), agg_func)(),
        check_freq=False,
    )


# One extra query to convert index to native pandas for dataframe constructor
@sql_count_checker(query_count=3, join_count=1)
def test_resample_hashable_on():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "A": np.random.randn(15),
                1: native_pd.date_range("2020-01-01", periods=15, freq="1s"),
            },
            index=native_pd.date_range("2020-10-01", periods=15, freq="1s"),
        ),
        lambda df: df.resample(rule="2s", on=1, closed="left").min(),
        check_freq=False,
    )


@sql_count_checker(query_count=0)
def test_resample_non_datetime_on():
    native_df = native_pd.DataFrame(
        data={
            "A": np.random.randn(15),
            "B": native_pd.date_range("2020-01-01", periods=15, freq="1s"),
        },
        index=native_pd.date_range("2020-10-01", periods=15, freq="1s"),
    )
    snow_df = pd.DataFrame(native_df)
    with pytest.raises(
        TypeError,
        match="Only valid with DatetimeIndex, TimedeltaIndex or PeriodIndex, but got an instance of 'Index'",
    ):
        native_df.resample(rule="2s", on="A").min()
    with pytest.raises(
        TypeError, match="Only valid with DatetimeIndex or TimedeltaIndex"
    ):
        snow_df.resample(rule="2s", on="A").min().to_pandas()


@sql_count_checker(query_count=1)
# One query to get the Modin frame data column pandas labels
def test_resample_invalid_on():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "A": np.random.randn(15),
                "B": native_pd.date_range("2020-01-01", periods=15, freq="1s"),
            },
            index=native_pd.date_range("2020-10-01", periods=15, freq="1s"),
        ),
        lambda df: df.resample(rule="2s", on="invalid", closed="left").min(),
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match="invalid",
    )
