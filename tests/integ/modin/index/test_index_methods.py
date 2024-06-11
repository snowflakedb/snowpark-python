#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
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
