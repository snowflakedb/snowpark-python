#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

freq = pytest.mark.parametrize("freq", ["min", "s", "h", "D"])
interval = pytest.mark.parametrize("interval", [1, 2, 3, 5, 15])


@freq
@interval
@sql_count_checker(query_count=3, join_count=1)
def test_asfreq_no_method(freq, interval):
    rule = f"{interval}{freq}"
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "A": np.random.randn(15),
                "B": native_pd.timedelta_range("1 days", periods=15),
            },
            index=native_pd.date_range("2020-01-01", periods=15, freq=f"1{freq}"),
        ),
        lambda df: df.asfreq(freq=rule),
        check_freq=False,
    )


@sql_count_checker(query_count=3, join_count=1)
def test_asfreq_ffill():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(15)},
            index=native_pd.date_range("2020-01-01", periods=15, freq="1s"),
        ),
        lambda df: df.asfreq(freq="5s", method="ffill"),
        check_freq=False,
    )


@sql_count_checker(query_count=3, join_count=1)
@pytest.mark.parametrize("freq", ["3min", "30S"])
def test_resampler_asfreq(freq):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "A": np.random.randn(15),
                "B": native_pd.timedelta_range("1 days", periods=15),
            },
            index=native_pd.date_range("2020-01-01", periods=15, freq="1min"),
        ),
        lambda df: df.resample(freq).asfreq(),
        check_freq=False,
    )


@sql_count_checker(query_count=0)
def test_asfreq_parameter_negative():
    snow_df = pd.DataFrame(
        {"A": np.random.randn(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1s"),
    )
    with pytest.raises(NotImplementedError):
        snow_df.asfreq(freq="5s", method="ffill", how="end")
    with pytest.raises(NotImplementedError):
        snow_df.asfreq(freq="5s", method="ffill", normalize=True)
    with pytest.raises(NotImplementedError):
        snow_df.asfreq(freq="5s", method="ffill", fill_value=2)
    with pytest.raises(NotImplementedError):
        snow_df.resample("5s").asfreq(fill_value=2)


@sql_count_checker(query_count=0)
def test_asfreq_rule_negative():
    snow_df = pd.DataFrame(
        {"A": np.random.randn(15)},
        index=native_pd.date_range("2020-01-01", periods=15, freq="1ME"),
    )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas `asfreq` does not yet support frequencies week, month, quarter, or year",
    ):
        snow_df.asfreq(freq="3ME")
