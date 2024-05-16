#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


from time import perf_counter

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter
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


def cache_and_return_df(snow_df, inplace):
    """
    Helper method to cache and return a reference to cached DataFrame depending on inplace.
    """
    if not inplace:
        cached_snow_df = snow_df.cache_result()
    else:
        # If inplace=True, there is no return, so we set `cached_snow_df` equal to `snow_df`.
        snow_df.cache_result(inplace=inplace)
        cached_snow_df = snow_df
    return cached_snow_df


@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_empty_dataframe(inplace):
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame()
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    if not inplace:
        # If inplace, then cached_snow_df IS snow_df, so this check
        # is meaningless.
        with SqlCounter(query_count=1):
            assert np.all((cached_snow_df == snow_df).values)
    with SqlCounter(query_count=1):
        assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(columns=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    if not inplace:
        # If inplace, then cached_snow_df IS snow_df, so this check
        # is meaningless.
        with SqlCounter(query_count=1):
            assert np.all((cached_snow_df == snow_df).values)
    with SqlCounter(query_count=1):
        assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(index=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    if not inplace:
        # If inplace, then cached_snow_df IS snow_df, so this check
        # is meaningless.
        with SqlCounter(query_count=1):
            assert np.all((cached_snow_df == snow_df).values)
    with SqlCounter(query_count=1):
        assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(columns=["A", "B", "C"], index=[0, 1, 2])
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    if not inplace:
        # If inplace, then cached_snow_df IS snow_df, so this check
        # is meaningless.
        with SqlCounter(query_count=1):
            assert np.all((cached_snow_df.isnull() == snow_df.isnull()).values)
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            cached_snow_df, native_df
        )


@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_dataframe_complex_correctness(
    time_index_string_column_snowpark_pandas_df,
    time_index_string_column_native_df,
    inplace,
):
    snow_df = time_index_string_column_snowpark_pandas_df
    native_df = time_index_string_column_native_df

    snow_df = snow_df.resample("2H").mean()
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    if not inplace:
        # If inplace, then cached_snow_df IS snow_df, so this check
        # is meaningless.
        with SqlCounter(query_count=1):
            assert np.all((cached_snow_df == snow_df).values)
    native_df = native_df.resample("2H").mean()

    cached_snow_df = cached_snow_df.set_index("b", drop=False)
    native_df = native_df.set_index("b", drop=False)
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            cached_snow_df, native_df, check_freq=False
        )


@pytest.mark.parametrize("inplace", [True, False])
class TestCacheResultReducesLatency:
    def test_cache_result_simple(self, inplace):
        snow_df = pd.concat([pd.DataFrame([range(i, i + 5)]) for i in range(0, 150, 5)])
        native_df = native_pd.DataFrame(np.arange(150).reshape((30, 5)))
        start = perf_counter()
        with SqlCounter(query_count=1):
            snow_df = snow_df.reset_index(drop=True)
            snow_df.to_pandas()
        uncached_time = perf_counter() - start

        cached_snow_df = cache_and_return_df(snow_df, inplace)

        start = perf_counter()
        with SqlCounter(query_count=1):
            cached_snow_df = cached_snow_df.reset_index(drop=True)
            cached_snow_df.to_pandas()
        cached_time = perf_counter() - start

        if not inplace:
            # If inplace, then cached_snow_df IS snow_df, so this check
            # is meaningless.
            with SqlCounter(query_count=1):
                assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                    snow_df, native_df
                )

        with SqlCounter(query_count=1):
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_df, native_df
            )

        assert cached_time < uncached_time
