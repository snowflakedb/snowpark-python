#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import SqlCounter


@pytest.mark.parametrize(
    "native_series",
    [
        native_pd.Series([None, None]),
        native_pd.Series(),
        native_pd.Series([None, 1, None]),
        native_pd.Series([np.nan, 3, np.nan]),
        native_pd.Series([1, 2, 3], index=[10, 11, 12]),
        native_pd.Series([1, 2, 3], index=[13, 10, 15]),
        native_pd.Series([5, 6, 7, 8], index=["i", "am", "iron", "man"]),
        native_pd.Series([None, None, 2], index=[None, 1, 2]),
        native_pd.Series([None, None, 2], index=[None, None, None]),
        pytest.param(
            native_pd.Series([None, None, pd.Timedelta(2)], index=[None, 1, 2]),
            id="timedelta",
        ),
    ],
)
def test_first_and_last_valid_index_series(native_series):
    snow_series = pd.Series(native_series)
    with SqlCounter(query_count=1):
        assert native_series.first_valid_index() == snow_series.first_valid_index()
    with SqlCounter(query_count=1):
        assert native_series.last_valid_index() == snow_series.last_valid_index()


def test_first_and_last_valid_none_float64index_series():
    native_series = native_pd.Series([None, 2, None], index=[None, None, 3])
    snow_series = pd.Series(native_series)
    # Resulting value for valid index should be np.nan since Float64Index
    # Convert to str and compare since nan != nan
    with SqlCounter(query_count=1):
        assert str(native_series.first_valid_index()) == str(
            snow_series.first_valid_index()
        )
    with SqlCounter(query_count=1):
        assert str(native_series.last_valid_index()) == str(
            snow_series.last_valid_index()
        )


@pytest.mark.parametrize(
    "index",
    [
        [
            np.array(["bar", "bar", "baz", "baz", "foo", "foo", "qux", "qux"]),
            np.array(["one", "two", "one", "two", "one", "two", "one", "two"]),
        ],
        [
            [None] * 8,
            [None] * 8,
        ],
        [
            [None, 1, 2, 3, 4, 5, 6, None],
            [5] * 8,
        ],
        [
            np.array(["None", "bar", "baz", "baz", "foo", "foo", "qux", "None"]),
            np.array(["None", "two", "one", "two", "one", "two", "one", "None"]),
        ],
    ],
)
def test_first_and_last_valid_multiindex_series(index):
    native_series = native_pd.Series(np.random.randn(8), index=index)
    snow_series = pd.DataFrame(native_series)
    # Should return a tuple in Multiindex case
    with SqlCounter(query_count=1):
        assert native_series.first_valid_index() == snow_series.first_valid_index()
    with SqlCounter(query_count=1):
        assert native_series.last_valid_index() == snow_series.last_valid_index()


@pytest.mark.parametrize("data", [[10, 11, 12], [None, 11, None]])
def test_first_and_last_valid_none_float64_multiindex_series(data):
    arrays = [
        [None, 3.0, None],
        [None, 5.0, None],
    ]
    native_series = native_pd.Series(data, index=arrays)
    snow_series = pd.Series(native_series)
    with SqlCounter(query_count=1):
        assert native_series.first_valid_index() == snow_series.first_valid_index()
    with SqlCounter(query_count=1):
        assert native_series.first_valid_index() == snow_series.first_valid_index()


def test_first_and_last_valid_nested_multiindex_series():
    arrays = [
        ["bar", "bar", "baz", "baz", "foo", "foo", "qux", "qux"],
        ["one", "two", "one", "two", "one", "two", "one", "two"],
    ]
    tuples = list(zip(*arrays))
    index = native_pd.MultiIndex.from_tuples(tuples, names=["first", "second"])
    native_series = native_pd.Series(np.random.randn(6), index=index[:6])
    snow_series = pd.Series(native_series)
    with SqlCounter(query_count=1):
        assert native_series.first_valid_index() == snow_series.first_valid_index()
    with SqlCounter(query_count=1):
        assert native_series.last_valid_index() == snow_series.last_valid_index()
