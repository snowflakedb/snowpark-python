#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin._internal.resample_utils import (
    IMPLEMENTED_DATEOFFSET_STRINGS,
)
from tests.integ.modin.utils import (
    assert_dicts_equal,
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

freq = pytest.mark.parametrize("freq", IMPLEMENTED_DATEOFFSET_STRINGS)
interval = pytest.mark.parametrize("interval", [1, 2, 3, 5, 15])


@freq
@interval
@sql_count_checker(query_count=3)
def test_resample_indices_df(freq, interval):
    rule = f"{interval}{freq}"
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(15)},
            index=native_pd.date_range("2020-01-01", periods=15, freq=f"1{freq}"),
        ),
        lambda df: df.resample(rule=rule, closed="left").indices,
        comparator=assert_dicts_equal,
    )


@freq
@interval
@sql_count_checker(query_count=2)
def test_resample_indices_series(freq, interval):
    rule = f"{interval}{freq}"
    eval_snowpark_pandas_result(
        *create_test_series(
            range(15),
            index=native_pd.date_range("2020-01-01", periods=15, freq=f"1{freq}"),
        ),
        lambda series: series.resample(rule=rule, closed="left").indices,
        comparator=assert_dicts_equal,
    )
