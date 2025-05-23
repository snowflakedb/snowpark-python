#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import assert_index_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def assert_reindex_result_equal(
    snow_idx: pd.Index, native_idx: native_pd.Index, func: Callable
) -> None:
    snow_result = func(snow_idx)
    native_result = func(native_idx)
    assert_index_equal(snow_result[0], native_result[0])
    np.testing.assert_array_equal(snow_result[1], native_result[1])


@sql_count_checker(query_count=0)
def test_invalid_limit_parameter():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    eval_snowpark_pandas_result(
        snow_idx,
        native_idx,
        lambda idx: idx.reindex(list("CAB"), limit=1),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="limit argument only valid if doing pad, backfill or nearest reindexing",
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=2, join_count=2)
def test_basic_reorder():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    assert_reindex_result_equal(
        snow_idx, native_idx, lambda idx: idx.reindex(list("CAB"))
    )


@sql_count_checker(query_count=2, join_count=2)
def test_basic_add_elements():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    assert_reindex_result_equal(
        snow_idx, native_idx, lambda idx: idx.reindex(list("CABDEF"))
    )


@sql_count_checker(query_count=2, join_count=2)
def test_basic_remove_elements():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    assert_reindex_result_equal(
        snow_idx, native_idx, lambda idx: idx.reindex(list("CA"))
    )


@sql_count_checker(query_count=2, join_count=2)
def test_basic_add_remove_elements():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    assert_reindex_result_equal(
        snow_idx, native_idx, lambda idx: idx.reindex(list("CADEFG"))
    )


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
def test_fill_method(method, limit):
    native_idx = native_pd.Index(list("ACE"))
    snow_idx = pd.Index(native_idx)
    assert_reindex_result_equal(
        snow_idx,
        native_idx,
        lambda idx: idx.reindex(list("ABCDEFGH"), method=method, limit=limit),
    )


@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
def test_fill_method_non_monotonic_new_index(method, limit):
    # The negative is because pandas does not support `limit` parameter
    # if the index and target are not both monotonic, while we do.
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    if limit is not None:
        with SqlCounter(query_count=1, join_count=1):
            method_str = {"bfill": "backfill", "ffill": "pad"}.get(method, method)
            eval_snowpark_pandas_result(
                snow_idx,
                native_idx,
                lambda idx: idx.reindex(list("CEBFGA"), method=method, limit=limit),
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match=f"limit argument for '{method_str}' method only well-defined if index and target are monotonic",
                assert_exception_equal=True,
            )
    else:
        with SqlCounter(query_count=2, join_count=2):
            assert_reindex_result_equal(
                snow_idx,
                native_idx,
                lambda idx: idx.reindex(list("CADEFG"), method=method, limit=limit),
            )


@sql_count_checker(query_count=2, join_count=2)
def test_ordered_index_unordered_new_index():
    native_idx = native_pd.Index([5, 6, 8])
    snow_idx = pd.Index(native_idx)
    assert_reindex_result_equal(
        snow_idx, native_idx, lambda idx: idx.reindex([6, 8, 7], method="ffill")
    )


@pytest.mark.parametrize("limit", [None, 1, 2, 100])
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_datetime_with_fill(limit, method):
    with SqlCounter(query_count=2 if limit is None else 3, join_count=2):
        native_date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
        snow_date_index = pd.date_range("1/1/2010", periods=6, freq="D")
        assert_reindex_result_equal(
            snow_date_index,
            native_date_index,
            lambda idx: idx.reindex(
                native_pd.date_range("12/29/2009", periods=10, freq="D"),
                method=method,
                limit=limit,
            ),
        )


@sql_count_checker(query_count=2, join_count=2)
def test_non_overlapping_index():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    assert_reindex_result_equal(
        snow_idx, native_idx, lambda idx: idx.reindex(list("EFG"))
    )


@sql_count_checker(query_count=2, join_count=2)
def test_non_overlapping_datetime_index():
    native_date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
    snow_date_index = pd.date_range("1/1/2010", periods=6, freq="D")
    assert_reindex_result_equal(
        snow_date_index,
        native_date_index,
        lambda idx: idx.reindex(
            native_pd.date_range("12/29/2008", periods=10, freq="D")
        ),
    )


@sql_count_checker(query_count=0)
def test_non_overlapping_different_types_index_negative_SNOW_1622502():
    date_index = pd.date_range("1/1/2010", periods=6, freq="D")

    with pytest.raises(
        SnowparkSQLException,
        match='Failed to cast variant value "A" to TIMESTAMP_NTZ',
    ):
        date_index.reindex(list("ABC"))


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize(
    "new_index, result_index, indices",
    [
        (list("ABC"), list("AABC"), np.array([0, 2, 1, -1])),
        (list("ABCC"), list("AABCC"), np.array([0, 2, 1, -1, -1])),
        (list("ABBC"), list("AABBC"), np.array([0, 2, 1, 1, -1])),
        (list("AABC"), list("AAAABC"), np.array([0, 2, 0, 2, 1, -1])),
    ],
)
def test_duplicate_values(new_index, result_index, indices):
    native_idx = native_pd.Index(list("ABA"))
    snow_idx = pd.Index(native_idx)

    with pytest.raises(
        ValueError, match="cannot reindex on an axis with duplicate labels"
    ):
        native_idx.reindex(new_index)
    snow_result = snow_idx.reindex(new_index)
    assert_index_equal(snow_result[0], native_pd.Index(result_index))
    np.testing.assert_array_equal(snow_result[1], indices)


@sql_count_checker(query_count=0)
def test_tuple_values_negative():
    native_idx = native_pd.MultiIndex.from_tuples(list(zip(range(4), range(4))))
    snow_idx = pd.Index(native_idx)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not support `reindex` with tuple-like Index values.",
    ):
        snow_idx.reindex([1, 2, 3])


@sql_count_checker(query_count=2, join_count=2)
def test_non_monotonic_index_no_overlapping_negative():
    native_idx = native_pd.Index(["car", "bike", "train"])
    snow_idx = pd.Index(native_idx)

    with pytest.raises(
        ValueError, match="index must be monotonic increasing or decreasing"
    ):
        native_idx.reindex(["a"], method="ffill")

    snow_result = snow_idx.reindex(["a"], method="ffill")
    assert_index_equal(snow_result[0], native_pd.Index(["a"]))
    np.testing.assert_array_equal(snow_result[1], np.array([-1]))


@sql_count_checker(query_count=1, join_count=1)
def test_non_monotonic_index():
    native_idx = native_pd.Index(["car", "bike", "train"])
    snow_idx = pd.Index(native_idx)

    eval_snowpark_pandas_result(
        snow_idx,
        native_idx,
        lambda idx: idx.reindex(["car", "cat"], method="ffill"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="index must be monotonic increasing or decreasing",
        assert_exception_equal=True,
    )
