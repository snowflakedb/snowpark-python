#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "values", [[], [1], [3, 2], [1, 3, 2], [1, 2, 2], [1, np.nan, 3]]
)
@sql_count_checker(query_count=1)
def test_monotonic_increasing_numbers(values):
    assert (
        pd.Index(values).is_monotonic_increasing
        == native_pd.Index(values).is_monotonic_increasing
    )


@pytest.mark.parametrize(
    "values", [[], [3], [1, 2], [3, 1, 2], [2, 2, 1], [3, np.nan, 1]]
)
@sql_count_checker(query_count=1)
def test_monotonic_decreasing_numbers(values):
    assert (
        pd.Index(values).is_monotonic_decreasing
        == native_pd.Index(values).is_monotonic_decreasing
    )


@pytest.mark.parametrize(
    "values", [["a", "b", "c"], ["c", "b", "a"], ["a", "c", "b"], ["ca", "cab", "cat"]]
)
@sql_count_checker(query_count=1)
def test_monotonic_increasing_str(values):
    assert (
        pd.Index(values).is_monotonic_increasing
        == native_pd.Index(values).is_monotonic_increasing
    )


@pytest.mark.parametrize(
    "values", [["c", "b", "a"], ["a", "b", "c"], ["c", "a", "b"], ["cat", "cab", "ca"]]
)
@sql_count_checker(query_count=1)
def test_monotonic_decreasing_str(values):
    assert (
        pd.Index(values).is_monotonic_decreasing
        == native_pd.Index(values).is_monotonic_decreasing
    )


@pytest.mark.parametrize(
    "values",
    [
        native_pd.date_range(start="1/1/2018", end="1/03/2018").values,
        native_pd.date_range(start="1/1/2018", end="1/03/2018").values[::-1],
        native_pd.date_range(start="1/1/2018", end="1/03/2018").values[[0, 2, 1]],
        [
            native_pd.Timestamp("2018-01-01 00:00:00"),
            native_pd.NaT,
            native_pd.Timestamp("2018-01-01 01:20:00"),
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_monotonic_increasing_dates(values):
    assert (
        pd.DatetimeIndex(values).is_monotonic_increasing
        == native_pd.DatetimeIndex(values).is_monotonic_increasing
    )


@pytest.mark.parametrize(
    "values",
    [
        native_pd.date_range(start="1/1/2018", end="1/03/2018").values[::-1],
        native_pd.date_range(start="1/1/2018", end="1/03/2018").values,
        native_pd.date_range(start="1/1/2018", end="1/03/2018").values[[2, 0, 1]],
        [
            native_pd.Timestamp("2018-01-01 01:20:00"),
            native_pd.NaT,
            native_pd.Timestamp("2018-01-01 00:00:00"),
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_monotonic_decreasing_dates(values):
    assert (
        pd.DatetimeIndex(values).is_monotonic_decreasing
        == native_pd.DatetimeIndex(values).is_monotonic_decreasing
    )
