#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.core.dtypes.common import is_numeric_dtype

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_index_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

SINGLE_TYPE_NATIVE_INDEX_TEST_DATA = [
    native_pd.Index(
        [1, 2, 3, 2, 3, 5, 6, 7, 8, 4, 4, 5, 6, 7, 1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 7]
    ),
    native_pd.Index([1.1, 2.2, 1.0, 1, 1.1, 2.2, 1, 1, 1, 2, 2, 2, 2.2]),
    native_pd.Index(
        [1.1, np.nan, 2.2, 1.0, 1, 1.1, 2.2, 1, np.nan, 1, 1, 2, 2, 2, np.nan, 2.2]
    ),
    native_pd.Index([1, 3, 1, 1, 1, 3, 1, 1, 1, 2, 2, 2, 3]),
    native_pd.Index(
        [True, False, True, False, True, False, True, False, True, True], dtype=bool
    ),
    native_pd.Index(
        [
            "a",
            "b",
            "c",
            "b",
            "c",
            "e",
            "f",
            "g",
            "h",
            "d",
            "d",
            "e",
            "f",
            "g",
            "a",
            "b",
            "a",
            "b",
            "c",
            "d",
            "c",
            "d",
            "e",
            "f",
            "g",
        ]
    ),
    native_pd.Index(
        [
            "a",
            "b",
            "c",
            "b",
            "c",
            None,
            "e",
            "f",
            "g",
            "h",
            "d",
            "d",
            None,
            "e",
            "f",
            "g",
            "a",
            "b",
            "a",
            "b",
            "c",
            "d",
            "c",
            "d",
            "e",
            "f",
            "g",
            None,
        ]
    ),
]


@pytest.mark.parametrize("native_index", SINGLE_TYPE_NATIVE_INDEX_TEST_DATA)
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@sql_count_checker(query_count=1)
def test_index_sort_values(native_index, ascending, na_position):
    # In this test, `return_indexer` is False: only an Index is returned.
    snow_index = pd.Index(native_index)
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda idx: idx.sort_values(
            return_indexer=False, ascending=ascending, na_position=na_position
        ),
    )


@pytest.mark.parametrize(
    "native_index",
    SINGLE_TYPE_NATIVE_INDEX_TEST_DATA[:-1]
    # last index ignored since np.argsort cannot compare str and None values.
)
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@sql_count_checker(query_count=2)
def test_index_sort_values_return_indexer(native_index, ascending, na_position):
    # In this test, `return_indexer` is True: a tuple of the result Index
    # and a numpy ndarray of the indexer is returned.
    snow_index = pd.Index(native_index)
    snow_res = snow_index.sort_values(
        return_indexer=True, ascending=ascending, na_position=na_position
    )
    native_res = native_index.sort_values(
        return_indexer=True, ascending=ascending, na_position=na_position
    )

    # Compare the index part of the results.
    assert_index_equal(snow_res[0], native_res[0])

    # Pandas performs quicksort to generate the indexer. Currently, Snowpark pandas does not support quicksort;
    # instead, stable sort is performed. Therefore, the results are not guaranteed to match.
    # Therefore, verify that it matches `np.argsort(kind=stable)`.

    array = native_index
    if not ascending:
        # When `ascending` is False, the Index is sorted in descending order. There is no such direct option for
        # `np.argsort`: it is improvised by negating all the values in the Index before sorting.
        if is_numeric_dtype(array):
            array = ~array if array.dtype == bool else -array
        else:
            # In case the type isn't numeric (dtype is str), turn them into ordinals.
            array = [-ord(c) if c else None for c in array]

    if na_position == "first" and is_numeric_dtype(array):
        # np.argsort has no option of placing NaN values at the start of the result. Therefore, mask the index
        # and convert NaN values to -inf.
        array = np.nan_to_num(array, nan=float("-inf"))

    assert np.array_equal(snow_res[1], np.argsort(array, kind="stable"))


@sql_count_checker(query_count=0)
def test_index_sort_values_key_raises_not_implemented_error():
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas sort_index API doesn't yet support 'key' parameter",
    ):
        snow_index = pd.Index(["A", "b", "C", "d"])
        snow_index.sort_values(key=lambda x: x.str.lower())
