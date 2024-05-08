#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)


@sql_count_checker(query_count=4)
def test_cache_empty_series():
    native_series = native_pd.Series()
    snow_series = pd.Series()
    cached_snow_series = snow_series.cache()
    assert cached_snow_series is snow_series
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_series, native_series
    )

    native_series = native_pd.Series(index=["A", "B", "C"])
    snow_series = pd.Series(native_series)
    cached_snow_series = snow_series.cache()
    assert cached_snow_series is snow_series
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        cached_snow_series, native_series
    )


@sql_count_checker(query_count=3, join_count=1)
def test_cache_dataframe_complex(
    time_index_snowpark_pandas_series, time_index_native_series
):
    snow_series = time_index_snowpark_pandas_series
    native_series = time_index_native_series

    snow_series = snow_series.resample("2H").mean()
    cached_snow_series = snow_series.cache()
    assert snow_series is cached_snow_series
    native_series = native_series.resample("2H").mean()

    cached_snow_series = cached_snow_series.diff()
    native_series = native_series.diff()
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        cached_snow_series, native_series, check_freq=False
    )
