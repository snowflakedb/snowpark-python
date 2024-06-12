#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_index_equal, assert_series_equal

NATIVE_INDEX_TEST_DATA = [
    native_pd.Index([], dtype="object"),
    native_pd.Index([1, 2, 3]),
    native_pd.Index(["a", "b", 1, 2]),
]


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_astype(native_index):
    snow_index = pd.Index(native_index)
    snow_index = snow_index.astype("object")
    assert_index_equal(snow_index, native_index.astype("object"))


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_copy(native_index):
    snow_index = pd.Index(native_index)
    snow_index2 = snow_index.copy()
    assert_index_equal(snow_index, snow_index2)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_drop(native_index):
    snow_index = pd.Index(native_index)
    if not native_index.empty:
        labels = [native_index[0]]
        assert_index_equal(snow_index.drop(labels), native_index.drop(labels))


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_equals(native_index):
    snow_index = pd.Index(native_index)
    index_check = pd.Index([1, 2, 3, 4])
    assert not snow_index.equals(index_check)
    assert index_check.equals(pd.Index([1, 2, 3, 4]))

    new_index = snow_index.copy()
    assert snow_index.equals(new_index)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_value_counts(native_index):
    snow_index = pd.Index(native_index)
    assert_series_equal(snow_index.value_counts(), native_index.value_counts())


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_sort_values(native_index):
    snow_index = pd.Index(native_index)
    if not (1 in native_index and "a" in native_index):
        assert_index_equal(snow_index.sort_values(), native_index.sort_values())
        native_tup = native_index.sort_values(return_indexer=True)
        snow_tup = snow_index.sort_values(return_indexer=True)
        assert_index_equal(native_tup[0], snow_tup[0])
        assert np.array_equal(native_tup[1], snow_tup[1])


@sql_count_checker(query_count=0)
def test_index_union():
    idx1 = pd.Index([1, 2, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    union = idx1.union(idx2)
    assert_index_equal(union, pd.Index([1, 2, 3, 4, 5, 6], dtype="int64"))
    idx1 = pd.Index(["a", "b", "c", "d"])
    idx2 = pd.Index([1, 2, 3, 4])
    union = idx1.union(idx2)
    assert_index_equal(
        union, pd.Index(["a", "b", "c", "d", 1, 2, 3, 4], dtype="object")
    )


@sql_count_checker(query_count=0)
def test_index_difference():
    idx1 = pd.Index([2, 1, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    diff = idx1.difference(idx2)
    assert_index_equal(diff, pd.Index([1, 2], dtype="int64"))


@sql_count_checker(query_count=0)
def test_index_intersection():
    idx1 = pd.Index([2, 1, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    diff = idx1.intersection(idx2)
    assert_index_equal(diff, pd.Index([3, 4], dtype="int64"))


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_get_level_values(native_index):
    snow_index = pd.Index(native_index)
    assert_index_equal(snow_index.get_level_values(0), snow_index)


@sql_count_checker(query_count=0)
def test_slice_indexer():
    idx = pd.Index(list("abcd"))
    s = idx.slice_indexer(start="a", end="c")
    assert s != slice(1, 3, None)
    s = idx.slice_indexer(start="b", end="c")
    assert s == slice(1, 3, None)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_summary(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index._summary() == native_index._summary()
