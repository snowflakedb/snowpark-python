#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from itertools import chain

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.testing import assert_series_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_series,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


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
    """
    Helper method to simulate an expensive pipeline.

    This method is expensive because the concat generates unions, and the objects being unioned
    may themselves contain unions, joins, or other expensive operations like pivots.
    """
    series = series.reset_index(drop=True)
    return module.concat([series] * 10)


@pytest.fixture(scope="function")
def simple_test_data():
    int_data = list(range(9))
    str_data = ["foo", "bar", "baz", "foo", "bar", "baz", "foo", "bar", "baz"]
    return list(chain.from_iterable(zip(int_data, str_data)))


@pytest.mark.parametrize(
    "init_kwargs", [{}, {"index": ["A", "B", "C"]}], ids=["no_index", "index"]
)
@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_empty_series(init_kwargs, inplace):
    snow_series, native_series = create_test_series(**init_kwargs)
    snow_series_copy = snow_series.copy(deep=True)
    with SqlCounter(query_count=1):
        cached_snow_series = cache_and_return_series(snow_series, inplace)
    with SqlCounter(query_count=2):
        assert_series_equal(
            cached_snow_series.to_pandas(), snow_series_copy.to_pandas()
        )
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_series, native_series
        )


@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_series_complex_correctness(time_index_series_data, inplace):
    series_data, kwargs = time_index_series_data
    snow_series, native_series = create_test_series(series_data, **kwargs)
    snow_series = snow_series.resample("2H").mean()
    snow_series_copy = snow_series.copy(deep=True)
    with SqlCounter(query_count=1):
        cached_snow_series = cache_and_return_series(snow_series, inplace)
    with SqlCounter(query_count=2):
        assert_series_equal(
            cached_snow_series.to_pandas(), snow_series_copy.to_pandas()
        )
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
        native_series = perform_chained_operations(
            native_pd.Series(np.arange(50)), native_pd
        )
        with SqlCounter(query_count=1, union_count=18):
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
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_series, native_series
            )

    def test_cache_result_post_apply(self, inplace, simple_test_data):
        # In this test, the caching doesn't aid in the query counts since
        # the implementation of apply(axis=1) itself contains intermediate
        # result caching.
        native_series = perform_chained_operations(
            native_pd.Series(simple_test_data).apply(lambda x: x + x), native_pd
        )
        with SqlCounter(query_count=5, union_count=9):
            snow_series = pd.Series(simple_test_data).apply(lambda x: x + x)
            repr(snow_series)
            snow_series = perform_chained_operations(snow_series, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_series, native_series
            )

        with SqlCounter(query_count=4):
            snow_series = pd.Series(simple_test_data).apply(lambda x: x + x)
            cached_snow_series = cache_and_return_series(snow_series, inplace)

        with SqlCounter(query_count=1):
            repr(cached_snow_series)
        with SqlCounter(query_count=1, union_count=9):
            cached_snow_series = perform_chained_operations(cached_snow_series, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_series,
                native_series,
            )


@sql_count_checker(query_count=1)
def test_cacheresult_timedelta():
    native_s = native_pd.Series(
        [
            native_pd.Timedelta("1 days"),
            native_pd.Timedelta("2 days"),
            native_pd.Timedelta("3 days"),
        ]
    )
    assert "timedelta64[ns]" == pd.Series(native_s).cache_result().dtype
