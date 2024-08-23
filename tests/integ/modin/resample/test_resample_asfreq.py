#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin._internal.resample_utils import (
    IMPLEMENTED_DATEOFFSET_STRINGS,
)
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result

freq = pytest.mark.parametrize("freq", IMPLEMENTED_DATEOFFSET_STRINGS)
interval = pytest.mark.parametrize("interval", [1, 2, 3, 5, 15])


@freq
@interval
@sql_count_checker(query_count=2, join_count=3)
def test_asfreq_no_method(freq, interval):
    rule = f"{interval}{freq}"
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(15)},
            index=native_pd.date_range("2020-01-01", periods=15, freq=f"1{freq}"),
        ),
        lambda df: df.asfreq(freq=rule),
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=3)
def test_asfreq_ffill():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(15)},
            index=native_pd.date_range("2020-01-01", periods=15, freq="1s"),
        ),
        lambda df: df.asfreq(freq="5s", method="ffill"),
        check_freq=False,
    )


@sql_count_checker(query_count=0)
def test_asfreq_negative():
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
