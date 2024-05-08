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


def assert_empty_snowpark_pandas_equals_to_pandas(snow_df, native_df):
    native_snow_df = snow_df.to_pandas()
    assert native_snow_df.empty is native_df.empty is True
    # When columns or index are empty, we have an empty Index, but pandas has a RangeIndex with no elements.
    if len(native_snow_df.columns) == 0:
        assert len(native_df.columns) == 0
    else:
        assert native_snow_df.columns.equals(native_df.columns)
    if len(native_snow_df.index) == 0:
        assert len(native_df.index) == 0
    else:
        assert native_snow_df.index.equals(native_df.index)


@sql_count_checker(query_count=8)
def test_cache_empty_dataframe():
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame()
    cached_snow_df = snow_df.cache()
    assert cached_snow_df is snow_df
    assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(columns=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    cached_snow_df = snow_df.cache()
    assert cached_snow_df is snow_df
    assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(index=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    cached_snow_df = snow_df.cache()
    assert cached_snow_df is snow_df
    assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(columns=["A", "B", "C"], index=[0, 1, 2])
    snow_df = pd.DataFrame(native_df)
    cached_snow_df = snow_df.cache()
    assert cached_snow_df is snow_df
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        cached_snow_df, native_df
    )


@sql_count_checker(query_count=3, join_count=1)
def test_cache_dataframe_complex(
    time_index_string_column_snowpark_pandas_df, time_index_string_column_native_df
):
    snow_df = time_index_string_column_snowpark_pandas_df
    native_df = time_index_string_column_native_df

    snow_df = snow_df.resample("2H").mean()
    cached_snow_df = snow_df.cache()
    assert snow_df is cached_snow_df
    native_df = native_df.resample("2H").mean()

    cached_snow_df = cached_snow_df.set_index("b", drop=False)
    native_df = native_df.set_index("b", drop=False)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        cached_snow_df, native_df, check_freq=False
    )
