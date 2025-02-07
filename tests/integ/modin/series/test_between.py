#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime as dt

import numpy as np
import pandas as native_pd
import pytest

import modin.pandas as pd
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    eval_snowpark_pandas_result,
    create_test_series,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
def test_series_between_default_inclusive():
    eval_snowpark_pandas_result(
        *create_test_series(list(range(0, 10))), lambda ser: ser.between(2, 8)
    )


# tuples of (data, low, high)
BETWEEN_TEST_ARGUMENTS = [
    ([0.8, 1.1, 0.9, 1.2], 0.9, 1.1),
    ([0.8, -1.1, 0.9, 1.2], -1, 1.14),
    # strings are compared lexicographically
    (["quick", "brown", "fox", "Quick", "Brown", "Fox"], "Zeta", "alpha"),
    (["quick", "brown", "fox", "Quick", "Brown", "Fox"], "Delta", "kappa"),
    (
        [
            dt.datetime(2024, 10, 11, 17, 5),
            dt.datetime(2020, 1, 2, 2, 40),
            dt.datetime(1998, 7, 7, 12, 33),
        ],
        dt.datetime(2019, 1, 1, 0, 0),
        dt.datetime(2021, 1, 1, 0, 0),
    ),
]


@pytest.mark.parametrize("data, low, high", BETWEEN_TEST_ARGUMENTS)
@pytest.mark.parametrize("inclusive", ["both", "neither", "left", "right"])
@sql_count_checker(query_count=1)
def test_series_between(data, low, high, inclusive):
    eval_snowpark_pandas_result(
        *create_test_series(data), lambda ser: ser.between(low, high, inclusive)
    )


@pytest.mark.parametrize(
    "data, low, high",
    [
        ([0.8, 1.1, 0.9, 1.2, np.nan], np.nan, 1.1),
        ([0.8, 1.1, 0.9, 1.2, np.nan], np.nan, np.nan),
        ([0.8, 1.1, 0.9, 1.2, np.nan], -1, 1.1),
        ([None, "", "aa", "aaaa"], "", "aaa"),
        ([None, "", "aa", "aaaa"], None, "aaa"),
        ([None, "", "aa", "aaaa"], None, None),
    ],
)
@pytest.mark.parametrize("inclusive", ["both", "neither", "left", "right"])
@sql_count_checker(query_count=1)
def test_series_between_with_nulls(data, low, high, inclusive):
    # In Snowflake SQL, comparisons with NULL will always result in a NULL value in the output.
    # Any comparison with NULL will return NULL, though the conjunction FALSE AND NULL will
    # short-circuit and return FALSE.
    eval_snowpark_pandas_result(
        *create_test_series(data),
        lambda ser: ser.between(low, high, inclusive).astype(bool),
    )


@pytest.mark.parametrize("data, low, high", BETWEEN_TEST_ARGUMENTS)
@sql_count_checker(query_count=1)
def test_series_between_flip_left_right(data, low, high):
    # When left/right are out of order, comparisons are still performed (high >= low is not enforced)
    eval_snowpark_pandas_result(
        *create_test_series(data), lambda ser: ser.between(high, low)
    )


@pytest.mark.parametrize(
    "data, low, high",
    [
        ([1, 2, 3, 4], [0, 1, 2, 3], [1.1, 1.9, 3.1, 3.9]),
        (["a", "b", "aa", "aaa"], ["", "a", "ccc", "aaaa"], ["c", "bb", "aaa", "d"]),
    ],
)
@pytest.mark.parametrize("inclusive", ["both", "neither", "left", "right"])
@sql_count_checker(query_count=1, join_count=3)
def test_series_between_series(data, low, high, inclusive):
    eval_snowpark_pandas_result(
        *create_test_series(data),
        lambda ser: ser.between(
            pd.Series(high) if isinstance(ser, pd.Series) else native_pd.Series(high),
            pd.Series(low) if isinstance(ser, pd.Series) else native_pd.Series(low),
            inclusive,
        ),
    )


@sql_count_checker(query_count=1, join_count=3)
def test_series_between_series_different_dimensions():
    # When attempting to compare with low/high of different lengths, Snowflake will leave NULLs
    # but pandas will raise an error.
    data = [1.1]
    low = [1, 2]
    high = [1, 2, 3]
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        pd.Series(data).between(low, high),
        native_pd.Series([False]),
    )
    with pytest.raises(
        ValueError, match="Can only compare identically-labeled Series objects"
    ):
        native_pd.Series(data).between(native_pd.Series(low), native_pd.Series(high))


@sql_count_checker(query_count=0)
def test_series_between_invalid_comparison():
    with pytest.raises(
        TypeError, match="Invalid comparison between dtype=int64 and str"
    ):
        native_pd.Series([1]).between("a", "b")
    with pytest.raises(
        SnowparkSQLException, match="Numeric value 'a' is not recognized"
    ):
        pd.Series([1]).between("a", "b").to_pandas()
