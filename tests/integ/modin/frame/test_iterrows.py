#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# To generate seeded random data.
rng = np.random.default_rng(12345)


def assert_iterators_equal(snowpark_iterator, native_iterator):
    # We can't use zip() because the iterators may have a different number of elements. Iterate through all the elements
    # in the native iterator and check that they match corresponding elements in the Snowpark iterator.
    for native_result in native_iterator:
        snowpark_result = next(snowpark_iterator)
        # Compare the index.
        assert snowpark_result[0] == native_result[0]
        # Compare the series.
        assert_series_equal(snowpark_result[1], native_result[1], check_dtype=False)

    with pytest.raises(StopIteration):
        # Check that the Snowpark iterator is also empty and therefore not longer than the native iterator.
        next(snowpark_iterator)


@pytest.mark.parametrize(
    "native_df",
    [
        # default index df
        native_pd.DataFrame([[1, 2, 3, 4], [11, 12, 13, 14], [111, 112, 113, 114]]),
        # non-default index df
        native_pd.DataFrame(
            [[1, 1.5], [2, 2.5], [3, 7.8], [4, 4], [5, 8.9]],
            columns=["int", "float"],
            index=["one", "two", "three", "four", "five"],
        ),
        # repeated column name df
        native_pd.DataFrame(
            [
                ("falcon", "bird", 389.0),
                ("parrot", "bird", 24.0),
                ("lion", "mammal", 80.5),
                ("monkey", "mammal", np.nan),
            ],
            columns=["name", "name", "max_speed"],
        ),
        # empty df
        native_pd.DataFrame([]),
        native_pd.DataFrame({"ts": native_pd.timedelta_range(10, periods=4)}),
    ],
)
def test_df_iterrows(native_df):
    # Test that the tuple returned is correct: (index, Series).
    snowpark_df = pd.DataFrame(native_df)
    # One query is used to retrieve each row - each query has 4 JOIN operations performed due to iloc.
    with SqlCounter(query_count=len(native_df)):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            lambda df: df.iterrows(),
            comparator=assert_iterators_equal,
        )


@sql_count_checker(query_count=7, union_count=7)
def test_df_iterrows_mixed_types(default_index_native_df):
    # Same test as above on bigger df with mixed types.
    # One query is used to retrieve each row - each query has 4 JOIN operations performed due to iloc.
    native_df = default_index_native_df
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.iterrows(),
        comparator=assert_iterators_equal,
    )


@sql_count_checker(query_count=6, union_count=6)
def test_df_iterrows_multindex_df():
    # Create df with a MultiIndex index.
    # One query is used to retrieve each row - each query has 4 JOIN operations performed due to iloc.
    arrays = [
        np.array(["bar", "bar", "baz", "baz", "foo", "foo"]),
        np.array(["one", "two", "one", "two", "one", "two"]),
    ]
    native_df = native_pd.DataFrame(rng.random(size=(6, 4)), index=arrays)
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.iterrows(),
        comparator=assert_iterators_equal,
    )
