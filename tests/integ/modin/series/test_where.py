#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=2, join_count=1)
def test_series_where_with_cond_series():
    data = range(5)
    snow_ser = pd.Series(data)
    native_ser = native_pd.Series(data)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.where(ser > 0))

    cond = [True, False]
    cond_snow_ser = pd.Series(cond)
    cond_native_ser = native_pd.Series(cond)
    other = 99

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.where(cond_snow_ser, other)
        if isinstance(ser, pd.Series)
        else ser.where(cond_native_ser, other),
    )


@sql_count_checker(query_count=2, join_count=2)
def test_series_where_with_cond_and_other_series():
    data = range(5)
    snow_ser = pd.Series(data)
    native_ser = native_pd.Series(data)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.where(ser > 0))

    cond = [True, False]
    cond_snow_ser = pd.Series(cond)
    cond_native_ser = native_pd.Series(cond)

    other = [123.45, 54.321]
    other_snow_ser = pd.Series(other)
    other_native_ser = native_pd.Series(other)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.where(cond_snow_ser, other_snow_ser)
        if isinstance(ser, pd.Series)
        else ser.where(cond_native_ser, other_native_ser),
    )


@pytest.mark.xfail(
    reason="SNOW-914228: Do not currently handle duplicates in index correctly"
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_where_duplicate_labels():
    data = [1, 2, 3, 4, 5]
    index = ["a", "b", "c", "b", "a"]

    snow_ser = pd.Series(data=data, index=index)
    native_ser = native_pd.Series(data=data, index=index)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.where(ser > 3))


@sql_count_checker(query_count=1)
def test_series_where_multiindex():
    data = [1, 2, 3, 4, 5]
    index = [("a", "x"), ("b", "y"), ("c", "z"), ("d", "u"), ("e", "v")]

    snow_ser = pd.Series(data=data, index=index)
    native_ser = native_pd.Series(data=data, index=index)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.where(ser > 3))


@pytest.mark.xfail(
    reason="SNOW-914228: Do not currently handle duplicates in index correctly"
)
@sql_count_checker(query_count=8, join_count=1, fallback_count=1, sproc_count=1)
def test_series_where_index_no_names():
    data = [1, 2, 3, 4, 5]
    index = [None, None, None, None, None]

    snow_ser = pd.Series(data=data, index=index)
    native_ser = native_pd.Series(data=data, index=index)

    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: ser.where(ser > 3, -ser)
    )


@sql_count_checker(query_count=2, join_count=2)
def test_series_where_with_np_array_cond():
    data = [1, 2]
    cond = np.array([True, False])

    snow_ser = pd.Series(data=data)
    native_ser = native_pd.Series(data=data)

    eval_snowpark_pandas_result(snow_ser, native_ser, lambda df: df.where(cond))


@sql_count_checker(query_count=1, join_count=3)
def test_series_where_with_series_cond_single_index_different_names():
    data = [1, 2, 3]
    cond = [False, True, False]

    snow_ser = pd.Series(data, index=pd.Index(["a", "b", "c"], name="Y"))
    native_ser = native_pd.Series(
        data, index=native_pd.Index(["a", "b", "c"], name="Y")
    )

    cond_snow_ser = pd.Series(cond, index=pd.Index(["a", "b", "c"], name="X"))
    cond_native_ser = native_pd.Series(
        cond, index=native_pd.Index(["a", "b", "c"], name="X")
    )

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.where(cond_snow_ser, 99.9)
        if isinstance(ser, pd.Series)
        else ser.where(cond_native_ser, 99.9),
        comparator=assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    )


@sql_count_checker(query_count=1, join_count=3)
def test_series_where_with_duplicated_index_aligned():
    data = [1, 2, 3]
    cond = [False, True, False]
    index = pd.Index(["a", "a", "c"], name="index")
    native_index = native_pd.Index(["a", "a", "c"], name="index")

    snow_ser = pd.Series(data, index=index)
    native_ser = native_pd.Series(data, index=native_index)

    cond_snow_ser = pd.Series(cond, index=index)
    cond_native_ser = native_pd.Series(cond, index=native_index)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.where(cond_snow_ser, 99)
        if isinstance(ser, pd.Series)
        else ser.where(cond_native_ser, 99),
    )


@sql_count_checker(query_count=1)
def test_series_where_with_lambda_cond():
    data = [1, 6, 7, 4]
    index = native_pd.Index(["a", "b", "c", "d"])

    native_ser = native_pd.Series(data, index=index)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.where(lambda x: x >= 6, 99),
    )


@sql_count_checker(query_count=0)
def test_series_where_with_lambda_cond_returns_singleton_should_fail():
    data = [1, 6, 7, 4]
    index = native_pd.Index(["a", "b", "c", "d"])
    native_ser = native_pd.Series(data, index=index)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.where(lambda x: True, 99),
        assert_exception_equal=True,
        expect_exception=True,
        expect_exception_match="Array conditional must be same shape as self",
        expect_exception_type=ValueError,
    )


@pytest.mark.parametrize(
    "other, sql_count, join_count",
    [(lambda x: -x.iloc[0], 4, 6), (lambda x: x**2, 3, 6)],
)
def test_series_where_with_lambda_other(other, sql_count, join_count):
    # High join count due to creatinga  Series with non-Snowpark pandas data
    # and a Snowpark pandas Index.
    data = [1, 6, 7, 4]
    index = pd.Index(["a", "b", "c", "d"])

    snow_ser = pd.Series(data, index=index)
    native_ser = native_pd.Series(data, index=index)

    with SqlCounter(query_count=sql_count, join_count=join_count):
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            lambda ser: ser.where([True, False, True, False], other),
        )


@pytest.mark.parametrize("cond", [1, [1]], ids=["scalar_cond", "scalar_cond_in_list"])
def test_series_where_with_scalar_cond(cond):
    native_ser = native_pd.Series([1, 2, 3])
    snow_ser = pd.Series(native_ser)

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            lambda ser: ser.where(cond, 1),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match="Array conditional must be same shape as self",
            assert_exception_equal=True,
        )


@sql_count_checker(query_count=1, join_count=1)
def test_series_where_series_cond_unmatched_index():
    data = [1, 2, 3, 4]
    index1 = [0, 1, 2, 3]
    index2 = [4, 5, 6, 7]

    snow_ser = pd.Series(data, index=index1)
    snow_cond = pd.Series([True, False, True, False], index=index2)

    native_ser = native_pd.Series(data, index=index1)
    native_cond = native_pd.Series([True, False, True, False], index=index2)

    def perform_where(series):
        if isinstance(series, pd.Series):
            return series.where(snow_cond, -1)
        else:
            return series.where(native_cond, -1)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        perform_where,
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("index", ["matched_index", "unmatched_index"])
def test_series_where_short_series_cond(index):
    data = [1, 2, 3, 4]
    if index != "matched_index":
        index = [7, 8, 9]
    else:
        index = None

    snow_ser = pd.Series(data)
    snow_cond = pd.Series([True, False, True], index=index)

    native_ser = native_pd.Series(data)
    native_cond = native_pd.Series([True, False, True], index=index)

    def perform_where(series):
        if isinstance(series, pd.Series):
            return series.where(snow_cond, -1)
        else:
            return series.where(native_cond, -1)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        perform_where,
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("index", ["matched_index", "unmatched_index"])
def test_series_where_long_series_cond(index):
    data = [1, 2, 3, 4]
    if index != "matched_index":
        index = [7, 8, 9, 10, 11]
    else:
        index = None

    snow_ser = pd.Series(data)
    snow_cond = pd.Series([True, False, True, False, True], index=index)

    native_ser = native_pd.Series(data)
    native_cond = native_pd.Series([True, False, True, False, True], index=index)

    def perform_where(series):
        if isinstance(series, pd.Series):
            return series.where(snow_cond, -1)
        else:
            return series.where(native_cond, -1)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        perform_where,
    )
