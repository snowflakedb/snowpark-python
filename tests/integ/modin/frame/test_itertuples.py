#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.snow_partition_iterator import (
    PARTITION_SIZE,
)
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import running_on_public_ci

# To generate seeded random data.
rng = np.random.default_rng(12345)

ITERTUPLES_DF_DATA = [
    # default index df
    native_pd.DataFrame([[1, 2, 3, 4], [11, 12, 13, 14], [111, 112, 113, 114]]),
    # non-default index df
    native_pd.DataFrame(
        [[1, 1.5], [2, 2.5], [3, 7.8]],
        columns=["int", "float"],
        index=["one", "two", "three"],
    ),
    # repeated column name df
    native_pd.DataFrame(
        [
            ("falcon", "bird", 389.0),
            ("monkey", "mammal", -100),
        ],
        columns=["name", "name", "max_speed"],
    ),
    # column name with spaces df
    native_pd.DataFrame([[1, 1.5], [2, 2.5], [3, 7.8]], columns=["i nt", "flo at"]),
    # empty df
    native_pd.DataFrame([]),
    native_pd.DataFrame({"ts": native_pd.timedelta_range(10, periods=10)}),
]


def assert_iterators_equal(snowpark_iterator, native_iterator):
    # Iterators should have the same number of elements.
    snowpark_result = list(snowpark_iterator)
    native_result = list(native_iterator)
    assert snowpark_result == native_result


@sql_count_checker(query_count=1, join_count=0)
@pytest.mark.parametrize("native_df", ITERTUPLES_DF_DATA)
def test_df_itertuples(native_df):
    # Test that the namedtuple returned is correct.
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.itertuples(),
        comparator=assert_iterators_equal,
    )


@sql_count_checker(query_count=1, join_count=0)
@pytest.mark.parametrize("native_df", ITERTUPLES_DF_DATA)
@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("name", ["AptlyNamedTuple", None])
def test_df_itertuples_index_and_name(native_df, index, name):
    # Test that the namedtuple returned is correct.
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.itertuples(),
        comparator=assert_iterators_equal,
    )


@sql_count_checker(query_count=1, join_count=0)
def test_df_itertuples_multindex_df():
    # Create df with a MultiIndex index.
    arrays = [
        np.array(["bar", "bar", "baz", "baz", "foo", "foo", "qux", "qux"]),
        np.array(["one", "two", "one", "two", "one", "two", "one", "two"]),
    ]
    native_df = native_pd.DataFrame(rng.random(size=(8, 4)), index=arrays)
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.itertuples(),
        comparator=assert_iterators_equal,
    )


@sql_count_checker(query_count=1, join_count=0)
@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("name", ["AptlyNamedTuple", None])
def test_df_itertuples_multindex_df_index_and_name(index, name):
    # Create df with a MultiIndex index.
    arrays = [
        np.array(["bar", "bar", "baz", "baz", "foo", "foo", "qux", "qux"]),
        np.array(["one", "two", "one", "two", "one", "two", "one", "two"]),
    ]
    native_df = native_pd.DataFrame(rng.random(size=(8, 4)), index=arrays)
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.itertuples(),
        comparator=assert_iterators_equal,
    )


@sql_count_checker(query_count=1, join_count=0)
@pytest.mark.xfail(
    raises=AssertionError,
    strict=True,
    reason="Native pandas retains the tuple value while Snowpark pandas converts it to list.",
)
def test_df_itertuples_tuple_data_negative():
    native_df = native_pd.DataFrame(
        {
            "F": [(1,), (2,), (3,)],
        }
    )
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.itertuples(),
        comparator=assert_iterators_equal,
    )


@sql_count_checker(query_count=1, join_count=0)
@pytest.mark.xfail(
    raises=AssertionError,
    strict=True,
    reason="Native pandas retains the np.nan value while Snowpark pandas converts it to None.",
)
def test_df_itertuples_nan_data_negative():
    native_df = native_pd.DataFrame({"A": [np.nan]})
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.itertuples(),
        comparator=assert_iterators_equal,
    )


@pytest.mark.parametrize("size", [10000, PARTITION_SIZE, PARTITION_SIZE * 2])
@pytest.mark.skipif(running_on_public_ci(), reason="slow test")
def test_df_itertuples_large_df(size):
    data = rng.integers(low=-1500, high=1500, size=size)
    native_df = native_pd.DataFrame(data)
    snowpark_df = pd.DataFrame(native_df)
    query_count = (np.floor(size / PARTITION_SIZE) + 1) * 4
    with SqlCounter(
        query_count=query_count,
        join_count=0,
        high_count_expected=True,
        high_count_reason="DataFrame spans multiple iteration partitions, each of which requires 4 queries",
    ):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            lambda df: df.itertuples(),
            comparator=assert_iterators_equal,
        )
