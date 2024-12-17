#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime as dt

import pandas as native_pd
import pytest

import modin.pandas as pd
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import eval_snowpark_pandas_result, create_test_series
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
    (["", "aa", "aaaa"], "", "aaa"),
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


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("data, low, high", BETWEEN_TEST_ARGUMENTS)
@pytest.mark.parametrize("inclusive", ["both", "neither", "left", "right"])
def test_series_between(data, low, high, inclusive):
    eval_snowpark_pandas_result(
        *create_test_series(data), lambda ser: ser.between(low, high, inclusive)
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("data, low, high", BETWEEN_TEST_ARGUMENTS)
def test_series_between_flip_left_right(data, low, high):
    # When left/right are out of order, comparisons are still performed (high >= low is not enforced)
    eval_snowpark_pandas_result(
        *create_test_series(data), lambda ser: ser.between(high, low)
    )


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
