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
    "native_dataframe",
    [
        native_pd.DataFrame([None, None]),
        native_pd.DataFrame(),
        native_pd.DataFrame([None, 1, None]),
        native_pd.DataFrame([np.nan, 3, np.nan]),
        native_pd.DataFrame(
            {"A": [None, 1, 2, None], "B": [3, 2, 1, None]}, index=[10, 11, 12, 13]
        ),
        native_pd.DataFrame(
            {"A": [None, 1, 2, None], "B": [3, 2, 1, None]}, index=[13, 12, 11, 15]
        ),
        native_pd.DataFrame([5, 6, 7, 8], index=["i", "am", "iron", "man"]),
        native_pd.DataFrame([None, None, 2], index=[None, 1, 2]),
        native_pd.DataFrame([None, None, 2], index=[None, None, None]),
    ],
)
def test_first_and_last_valid_index_dataframe(native_dataframe):
    snow_dataframe = pd.DataFrame(native_dataframe)
    with SqlCounter(query_count=1):
        assert (
            native_dataframe.first_valid_index() == snow_dataframe.first_valid_index()
        )
    with SqlCounter(query_count=1):
        assert native_dataframe.last_valid_index() == snow_dataframe.last_valid_index()


def test_first_and_last_valid_none_float64index_dataframe():
    native_dataframe = native_pd.DataFrame([None, 2, None], index=[None, None, 3])
    snow_dataframe = pd.DataFrame(native_dataframe)
    # Resulting value for valid index should be np.nan since Float64Index
    # Convert to str and compare since nan != nan
    with SqlCounter(query_count=1):
        assert str(native_dataframe.first_valid_index()) == str(
            snow_dataframe.first_valid_index()
        )
    with SqlCounter(query_count=1):
        assert str(native_dataframe.last_valid_index()) == str(
            snow_dataframe.last_valid_index()
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
def test_first_and_last_valid_multiindex_dataframe(index):
    native_dataframe = native_pd.DataFrame(np.random.randn(8, 4), index=index)
    snow_dataframe = pd.DataFrame(native_dataframe)
    # Should return a tuple in Multiindex case
    with SqlCounter(query_count=1):
        assert (
            native_dataframe.first_valid_index() == snow_dataframe.first_valid_index()
        )
    with SqlCounter(query_count=1):
        assert native_dataframe.last_valid_index() == snow_dataframe.last_valid_index()


@pytest.mark.parametrize(
    "data",
    [
        [[10, 11, 12], [5, 6, 7], [8, 9, 10]],
        [[None, None, None], [None, None, 7], [None, None, None]],
    ],
)
def test_first_and_last_valid_none_float64_multiindex_dataframe(data):
    arrays = [
        [None, 3.0, None],
        [None, 5.0, None],
    ]
    native_dataframe = native_pd.DataFrame(data, index=arrays)
    snow_dataframe = pd.DataFrame(native_dataframe)
    with SqlCounter(query_count=1):
        assert (
            native_dataframe.first_valid_index() == snow_dataframe.first_valid_index()
        )
    with SqlCounter(query_count=1):
        assert (
            native_dataframe.first_valid_index() == snow_dataframe.first_valid_index()
        )


def test_first_and_last_valid_nested_multiindex_series():
    arrays = [
        ["bar", "bar", "baz", "baz", "foo", "foo", "qux", "qux"],
        ["one", "two", "one", "two", "one", "two", "one", "two"],
    ]
    tuples = list(zip(*arrays))
    index = native_pd.MultiIndex.from_tuples(tuples, names=["first", "second"])
    native_dataframe = native_pd.DataFrame(
        np.random.randn(6, 6), index=index[:6], columns=index[:6]
    )
    snow_dataframe = pd.DataFrame(native_dataframe)
    with SqlCounter(query_count=1):
        assert (
            native_dataframe.first_valid_index() == snow_dataframe.first_valid_index()
        )
    with SqlCounter(query_count=1):
        assert native_dataframe.last_valid_index() == snow_dataframe.last_valid_index()
