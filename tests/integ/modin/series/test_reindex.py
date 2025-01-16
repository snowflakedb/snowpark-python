#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_reindex_invalid_limit_parameter():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(index=list("CAB"), fill_value=1, limit=1),
        expect_exception=True,
        expect_exception_match="limit argument only valid if doing pad, backfill or nearest reindexing",
        expect_exception_type=ValueError,
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_invalid_axis_parameter_ignored():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(axis=1, index=list("CAB")),
    )


@sql_count_checker(query_count=0)
def test_reindex_invalid_labels_parameter():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(labels=list("CAB")),
        expect_exception=True,
        expect_exception_match=re.escape("got an unexpected keyword argument 'labels'"),
        expect_exception_type=TypeError,
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=0)
def test_reindex_index_passed_twice():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(list("CAB"), index=list("CAB")),
        expect_exception=True,
        expect_exception_match=re.escape("got multiple values for argument 'index'"),
        expect_exception_type=TypeError,
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=0)
def test_reindex_multiple_args_passed():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(list("CAB"), index=list("CAB")),
        expect_exception=True,
        expect_exception_match=re.escape("got multiple values for argument 'index'"),
        expect_exception_type=TypeError,
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_basic_reorder():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.reindex(index=list("CAB"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_basic_add_elements():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.reindex(index=list("CABDEF"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_basic_remove_elements():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.reindex(index=list("CA"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_basic_add_remove_elements():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.reindex(index=list("CADEFG"))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_fill_value():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(index=list("CADEFG"), fill_value=-1.0),
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
def test_reindex_index_fill_method(method, limit):
    native_series = native_pd.Series([0, 1, 2], index=list("ACE"))
    snow_series = pd.Series(native_series)

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(
            index=list("ABCDEFGH"), method=method, limit=limit
        ),
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
def test_reindex_index_fill_method_pandas_negative(method, limit):
    # The negative is because pandas does not support `limit` parameter
    # if the index and target are not both monotonic, while we do.
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    if limit is not None:
        method_str = {"bfill": "backfill", "ffill": "pad"}.get(method, method)
        with pytest.raises(
            ValueError,
            match=f"limit argument for '{method_str}' method only well-defined if index and target are monotonic",
        ):
            native_series.reindex(index=list("CEBFGA"), method=method, limit=limit)

    def perform_reindex(series):
        if isinstance(series, pd.Series):
            return series.reindex(index=list("CADEFG"), method=method, limit=limit)
        else:
            return series.reindex(
                index=list("ACDEFG"), method=method, limit=limit
            ).reindex(list("CADEFG"))

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        perform_reindex,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_ordered_index_unordered_new_index():
    ordered_native_Series = native_pd.Series([5, 6, 8], index=[5, 6, 8])
    ordered_snow_series = pd.Series(ordered_native_Series)
    eval_snowpark_pandas_result(
        ordered_snow_series,
        ordered_native_Series,
        lambda series: series.reindex(index=[6, 8, 7], method="ffill"),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_fill_value_with_old_na_values():
    native_series = native_pd.Series([1, np.nan, 3], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(index=list("CEBFGA"), fill_value=-1),
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
@pytest.mark.xfail(reason="Cannot qualify window in fillna.")
def test_reindex_index_fill_method_with_old_na_values(limit, method):
    # Say there are NA values in the data before reindex. reindex is called with
    # ffill as the method, and there is a new NA value in the row following the
    # row with the pre-existing NA value. In this case, the ffilled value should
    # be an NA value (rather than looking to previous rows for a non-NA value).
    # To support this, we would need to have `ignore_nulls=False`, but we would
    # also need to qualify the window for values that were in the original Series
    # as otherwise, if we have multiple new index values that have NA values, we would
    # pick these NA values instead of finding a non-NA value from the original Series.
    native_series = native_pd.Series([1, np.nan, 3], index=list("ACE"))
    snow_series = pd.Series(native_series)

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(
            index=list("ABCDEFGH"), method=method, limit=limit
        ),
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_reindex_index_fill_method_with_old_na_values_pandas_negative(limit, method):
    native_series = native_pd.Series([1, np.nan, 3], index=list("ABC"))
    snow_series = pd.Series(native_series)
    if limit is not None:
        method_str = {"bfill": "backfill", "ffill": "pad"}.get(method, method)
        with pytest.raises(
            ValueError,
            match=f"limit argument for '{method_str}' method only well-defined if index and target are monotonic",
        ):
            native_series.reindex(index=list("CEBFGA"), method=method, limit=limit)

    def perform_reindex(series):
        if isinstance(series, pd.Series):
            return series.reindex(index=list("CEBFGA"), method=method, limit=limit)
        else:
            return series.reindex(
                index=list("ABCEFG"), method=method, limit=limit
            ).reindex(list("CEBFGA"))

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        perform_reindex,
    )


@sql_count_checker(query_count=1, join_count=2)
@pytest.mark.parametrize("limit", [None, 1, 2, 100])
@pytest.mark.parametrize("method", ["bfill", "backfill", "pad", "ffill"])
def test_reindex_index_datetime_with_fill(limit, method):
    date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
    native_series = native_pd.Series(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )
    date_index = pd.date_range("1/1/2010", periods=6, freq="D")
    snow_series = pd.Series(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )

    def perform_reindex(series):
        if isinstance(series, pd.Series):
            return series.reindex(
                pd.date_range("12/29/2009", periods=10, freq="D"),
                method=method,
                limit=limit,
            )
        else:
            return series.reindex(
                native_pd.date_range("12/29/2009", periods=10, freq="D"),
                method=method,
                limit=limit,
            )

    eval_snowpark_pandas_result(
        snow_series, native_series, perform_reindex, check_freq=False
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_index_non_overlapping_index():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.reindex(index=list("EFG"))
    )


@sql_count_checker(query_count=1, join_count=2)
def test_reindex_index_non_overlapping_datetime_index():
    date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
    native_series = native_pd.Series(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )
    date_index = pd.date_range("1/1/2010", periods=6, freq="D")
    snow_series = pd.Series(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )

    def perform_reindex(series):
        if isinstance(series, pd.Series):
            return series.reindex(
                pd.date_range("12/29/2008", periods=10, freq="D"),
            )
        else:
            return series.reindex(
                native_pd.date_range("12/29/2008", periods=10, freq="D"),
            )

    eval_snowpark_pandas_result(
        snow_series, native_series, perform_reindex, check_freq=False
    )


@sql_count_checker(query_count=1, join_count=2)
def test_reindex_index_non_overlapping_different_types_index():
    date_index = native_pd.date_range("1/1/2010", periods=6, freq="D")
    native_series = native_pd.Series(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )
    date_index = pd.date_range("1/1/2010", periods=6, freq="D")
    snow_series = pd.Series(
        {"prices": [100, 101, np.nan, 100, 89, 88]}, index=date_index
    )

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(list("ABC")),
        check_freq=False,
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize(
    "new_index", [list("ABC"), list("ABCC"), list("ABBC"), list("AABC")]
)
def test_reindex_index_duplicate_values(new_index):
    native_series = native_pd.Series([0, 1, 2], index=list("ABA"))
    snow_series = pd.Series(native_series)

    with pytest.raises(
        ValueError, match="cannot reindex on an axis with duplicate labels"
    ):
        native_series.reindex(index=new_index)

    series_to_concat = []
    for row in new_index:
        if row not in list("AB"):
            series_to_concat.append(native_pd.Series([np.nan], index=[row]))
        else:
            value = native_series.loc[row]
            if not isinstance(value, native_pd.Series):
                value = native_pd.Series([value], index=[row])
            series_to_concat.append(value)
    result_native_series = native_pd.concat(series_to_concat)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_series.reindex(index=new_index), result_native_series
    )


@sql_count_checker(query_count=0)
def test_reindex_multiindex_negative():
    snow_series = pd.Series(
        [0, 1, 2], index=native_pd.MultiIndex.from_tuples([(1, 1), (2, 2), (3, 3)])
    )

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support `reindex` with MultiIndex",
    ):
        snow_series.reindex(index=[1, 2, 3])


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_with_index_name():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"), name="test")
    snow_series = pd.Series(native_series)
    index_with_name = native_pd.Index(list("CAB"), name="weewoo")
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_series.reindex(index=index_with_name),
        native_series.reindex(index=index_with_name),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_with_index_name_and_series_index_name():
    native_series = native_pd.Series(
        [0, 1, 2], index=native_pd.Index(list("ABC"), name="AAAAA"), name="test"
    )
    snow_series = pd.Series(native_series)
    index_with_name = native_pd.Index(list("CAB"), name="weewoo")
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_series.reindex(index=index_with_name),
        native_series.reindex(index=index_with_name),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_reindex_with_lazy_index():
    native_series = native_pd.Series([0, 1, 2], index=list("ABC"))
    snow_series = pd.Series(native_series)
    native_idx = native_pd.Index(list("CAB"))
    lazy_idx = pd.Index(native_idx)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.reindex(
            index=native_idx if isinstance(series, native_pd.Series) else lazy_idx
        ),
    )
