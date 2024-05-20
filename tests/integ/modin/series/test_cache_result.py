#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


from itertools import chain

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_series,
)


def assert_snowpark_pandas_series_are_equal(snow_series1, snow_series2):
    assert snow_series1.empty is snow_series2.empty
    assert snow_series1.index.equals(snow_series2.index)
    if not snow_series1.empty:
        assert np.all((snow_series1.isna() == snow_series2.isna()).values)


def cache_and_return_series(snow_series, inplace):
    """
    Helper method to cache and return a reference to cached Series depending on inplace.

    Notes
    -----
    If inplace=True, the returned series is a reference to the inputted series.
    """
    if not inplace:
        cached_snow_series = snow_series.cache_result()
    else:
        # If inplace=True, there is no return, so we set `cached_snow_series` equal to `snow_series`.
        cached_snow_series = snow_series
        cached_snow_series.cache_result(inplace=inplace)
    return cached_snow_series


def perform_chained_operations(series, module):
    series = series.reset_index(drop=True)
    return module.concat([series] * 10)


@pytest.fixture(scope="function")
def simple_test_data():
    int_data = list(range(9))
    str_data = ["foo", "bar", "baz", "foo", "bar", "baz", "foo", "bar", "baz"]
    return list(chain.from_iterable(zip(int_data, str_data)))


@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_empty_series(inplace):
    native_series = native_pd.Series()
    snow_series = pd.Series()
    snow_series_copy = snow_series.copy(deep=True)
    with SqlCounter(query_count=6):
        cached_snow_series = cache_and_return_series(snow_series, inplace)
        assert_snowpark_pandas_series_are_equal(cached_snow_series, snow_series_copy)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_series, native_series
        )

    native_series = native_pd.Series(index=["A", "B", "C"])
    snow_series = pd.Series(native_series)
    snow_series_copy = snow_series.copy(deep=True)
    with SqlCounter(query_count=7, join_count=1):
        cached_snow_series = cache_and_return_series(snow_series, inplace)
        assert_snowpark_pandas_series_are_equal(cached_snow_series, snow_series_copy)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            cached_snow_series, native_series
        )


@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_series_complex_correctness(time_index_series_data, inplace):
    series_data, kwargs = time_index_series_data
    snow_series, native_series = create_test_series(series_data, **kwargs)
    snow_series = snow_series.resample("2H").mean()
    snow_series_copy = snow_series.copy(deep=True)
    with SqlCounter(query_count=6):
        cached_snow_series = cache_and_return_series(snow_series, inplace)
        assert_snowpark_pandas_series_are_equal(cached_snow_series, snow_series_copy)
    native_series = native_series.resample("2H").mean()

    with SqlCounter(query_count=1):
        cached_snow_series = cached_snow_series.diff()
        native_series = native_series.diff()
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            cached_snow_series, native_series, check_freq=False
        )


@pytest.mark.parametrize("inplace", [True, False])
class TestCacheResultReducesQueryCount:
    def test_cache_result_simple(self, inplace):
        snow_series = pd.concat(
            [pd.Series(list(range(i, i + 5))) for i in range(0, 50, 5)]
        )
        native_series = native_pd.Series(np.arange(50))
        native_series = native_pd.concat([native_series] * 10)
        native_series.name = 0
        with SqlCounter(query_count=1, union_count=99):
            snow_series = perform_chained_operations(snow_series, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_series, native_series
            )

        with SqlCounter(query_count=1, union_count=9):
            snow_series = pd.concat(
                [pd.Series(list(range(i, i + 5))) for i in range(0, 50, 5)]
            )
            cached_snow_series = cache_and_return_series(snow_series, inplace)

        with SqlCounter(query_count=1, union_count=9):
            cached_snow_series = perform_chained_operations(cached_snow_series, pd)
            cached_snow_series.to_pandas()

        with SqlCounter(query_count=1):
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_series, native_series
            )

    def test_cache_result_post_apply(self, inplace, simple_test_data):
        snow_series = pd.Series(simple_test_data).apply(lambda x: x + x)
        native_series = perform_chained_operations(
            native_pd.Series(simple_test_data).apply(lambda x: x + x), native_pd
        )
        native_series.name = 0
        with SqlCounter(query_count=2, union_count=9):
            repr(snow_series)
            snow_series = perform_chained_operations(snow_series, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_series, native_series
            )

        with SqlCounter(query_count=4):
            snow_series = pd.Series(simple_test_data).apply(lambda x: x + x)
            cached_snow_series = cache_and_return_series(snow_series, inplace)

        with SqlCounter(query_count=2, union_count=9):
            repr(cached_snow_series)
            cached_snow_series = perform_chained_operations(cached_snow_series, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_series,
                native_series,
            )
