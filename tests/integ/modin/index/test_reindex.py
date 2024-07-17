#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import assert_index_equal


def assert_reindex_result_equal(
    snow_result: tuple[pd.Index, np.ndarray], native_result: tuple[pd.Index, np.ndarray]
) -> None:
    assert_index_equal(snow_result[0], native_result[0])
    np.testing.assert_array_equal(snow_result[1], native_result[1])


def assert_reindex_exceptions_match(
    snow_idx: pd.Index,
    native_idx: native_pd.Index,
    func: Callable,
    excp_type: Exception,
    excp_match: str,
) -> None:
    with pytest.raises(excp_type, match=excp_match):
        func(native_idx)

    with pytest.raises(excp_type, match=excp_match):
        func(snow_idx)


def create_datetime_index(snow_series: pd.Series) -> pd.Index:
    from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    modin_frame = snow_series._query_compiler._modin_frame
    return pd.Index(
        SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=modin_frame.ordered_dataframe,
                data_column_pandas_index_names=modin_frame.data_column_pandas_index_names,
                data_column_pandas_labels=[],
                data_column_snowflake_quoted_identifiers=[],
                index_column_pandas_labels=[None],
                index_column_snowflake_quoted_identifiers=modin_frame.data_column_snowflake_quoted_identifiers,
            )
        )
    )


@sql_count_checker(query_count=0)
def test_reindex_invalid_limit_parameter():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    assert_reindex_exceptions_match(
        snow_idx,
        native_idx,
        lambda idx: idx.reindex(list("CAB"), limit=1),
        ValueError,
        "limit argument only valid if doing pad, backfill or nearest reindexing",
    )


@sql_count_checker(query_count=2, join_count=2)
def test_reindex_index_basic_reorder():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    native_result = native_idx.reindex(list("CAB"))
    snow_result = snow_idx.reindex(list("CAB"))
    assert_reindex_result_equal(snow_result, native_result)


@sql_count_checker(query_count=2, join_count=2)
def test_reindex_index_basic_add_elements():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    native_result = native_idx.reindex(list("CABDEF"))
    snow_result = snow_idx.reindex(list("CABDEF"))
    assert_reindex_result_equal(snow_result, native_result)


@sql_count_checker(query_count=2, join_count=2)
def test_reindex_index_basic_remove_elements():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    native_result = native_idx.reindex(list("CA"))
    snow_result = snow_idx.reindex(list("CA"))
    assert_reindex_result_equal(snow_result, native_result)


@sql_count_checker(query_count=2, join_count=2)
def test_reindex_index_basic_add_remove_elements():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    native_result = native_idx.reindex(list("CADEFG"))
    snow_result = snow_idx.reindex(list("CADEFG"))
    assert_reindex_result_equal(snow_result, native_result)


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
def test_reindex_index_fill_method(method, limit):
    native_idx = native_pd.Index(list("ACE"))
    snow_idx = pd.Index(native_idx)
    native_result = native_idx.reindex(list("ABCDEFGH"), method=method, limit=limit)
    snow_result = snow_idx.reindex(list("ABCDEFGH"), method=method, limit=limit)
    assert_reindex_result_equal(snow_result, native_result)


@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
def test_reindex_index_fill_method_non_monotonic_new_index(method, limit):
    # The negative is because pandas does not support `limit` parameter
    # if the index and target are not both monotonic, while we do.
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    if limit is not None:
        with SqlCounter(query_count=1, join_count=1):
            method_str = {"bfill": "backfill", "ffill": "pad"}.get(method, method)
            assert_reindex_exceptions_match(
                snow_idx,
                native_idx,
                lambda idx: idx.reindex(list("CEBFGA"), method=method, limit=limit),
                ValueError,
                f"limit argument for '{method_str}' method only well-defined if index and target are monotonic",
            )
    else:
        with SqlCounter(query_count=2, join_count=2):
            native_result = native_idx.reindex(
                list("CADEFG"), method=method, limit=limit
            )
            snow_result = snow_idx.reindex(list("CADEFG"), method=method, limit=limit)
            assert_reindex_result_equal(snow_result, native_result)


@sql_count_checker(query_count=2, join_count=2)
def test_reindex_index_ordered_index_unordered_new_index():
    native_idx = native_pd.Index([5, 6, 8])
    snow_idx = pd.Index(native_idx)
    native_result = native_idx.reindex([6, 8, 7], method="ffill")
    snow_result = snow_idx.reindex([6, 8, 7], method="ffill")
    assert_reindex_result_equal(snow_result, native_result)


@pytest.mark.parametrize("limit", [None, 1, 2, 100])
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_reindex_index_datetime_with_fill(limit, method):
    query_count = 3 if limit else 2
    join_count = 2
    with SqlCounter(query_count=query_count, join_count=join_count):
        native_date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
        snow_date_index = create_datetime_index(
            pd.date_range("1/1/2010", periods=6, freq="D")
        )
        native_result = native_date_index.reindex(
            native_pd.date_range("12/29/2009", periods=10, freq="D"),
            method=method,
            limit=limit,
        )
        snow_result = snow_date_index.reindex(
            pd.date_range("12/29/2009", periods=10, freq="D"),
            method=method,
            limit=limit,
        )
        assert_reindex_result_equal(snow_result, native_result)


@sql_count_checker(query_count=2, join_count=2)
def test_reindex_index_non_overlapping_index():
    native_idx = native_pd.Index(list("ABC"))
    snow_idx = pd.Index(native_idx)
    native_result = native_idx.reindex(list("EFG"))
    snow_result = snow_idx.reindex(list("EFG"))
    assert_reindex_result_equal(snow_result, native_result)


@sql_count_checker(query_count=2, join_count=2)
def test_reindex_index_non_overlapping_datetime_index():
    native_date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
    snow_date_index = create_datetime_index(
        pd.date_range("1/1/2010", periods=6, freq="D")
    )
    native_result = native_date_index.reindex(
        native_pd.date_range("12/29/2008", periods=10, freq="D")
    )
    snow_result = snow_date_index.reindex(
        pd.date_range("12/29/2008", periods=10, freq="D")
    )
    assert_reindex_result_equal(snow_result, native_result)


@sql_count_checker(query_count=0)
def test_reindex_index_non_overlapping_different_types_index_negative():
    date_index = create_datetime_index(pd.date_range("1/1/2010", periods=6, freq="D"))

    with pytest.raises(SnowparkSQLException, match=".*Timestamp 'A' is not recognized"):
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
def test_reindex_index_duplicate_values(new_index, result_index, indices):
    native_idx = native_pd.Index(list("ABA"))
    snow_idx = pd.Index(native_idx)

    with pytest.raises(
        ValueError, match="cannot reindex on an axis with duplicate labels"
    ):
        native_idx.reindex(new_index)
    snow_result = snow_idx.reindex(new_index)
    assert_reindex_result_equal(snow_result, (native_pd.Index(result_index), indices))


@sql_count_checker(query_count=0)
def test_reindex_multiindex_negative():
    native_idx = native_pd.MultiIndex.from_tuples(list(zip(range(4), range(4))))
    snow_idx = pd.Index(native_idx)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support `reindex` with MultiIndex",
    ):
        snow_idx.reindex([1, 2, 3])


@sql_count_checker(query_count=2, join_count=2)
def test_reindex_non_monotonic_index_no_overlapping_negative():
    native_idx = native_pd.Index(["car", "bike", "train"])
    snow_idx = pd.Index(native_idx)

    with pytest.raises(
        ValueError, match="index must be monotonic increasing or decreasing"
    ):
        native_idx.reindex(["a"], method="ffill")

    snow_result = snow_idx.reindex(["a"], method="ffill")
    assert_reindex_result_equal(snow_result, (native_pd.Index(["a"]), np.array([-1])))


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_non_monotonic_index():
    native_idx = native_pd.Index(["car", "bike", "train"])
    snow_idx = pd.Index(native_idx)

    assert_reindex_exceptions_match(
        snow_idx,
        native_idx,
        lambda idx: idx.reindex(["car", "cat"], method="ffill"),
        ValueError,
        "index must be monotonic increasing or decreasing",
    )
