#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_series_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_basic_series_axis0(join):
    native_ser = native_pd.Series([1, 2, 3])
    native_other_ser = native_pd.Series([60, 70, 80, 90, 100, np.nan])
    native_left, native_right = native_ser.align(
        native_other_ser,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    ser = pd.Series(native_ser)
    other_ser = pd.Series(native_other_ser)
    left, right = ser.align(other_ser, join=join, axis=0)
    assert_series_equal(left, native_left)
    assert_series_equal(right, native_right)


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_series_with_nulls_axis0(join):
    native_ser = native_pd.Series([np.nan, np.nan, np.nan])
    native_other_ser = native_pd.Series([60, 70, 80, 90, 100, np.nan])
    native_left, native_right = native_ser.align(
        native_other_ser,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    ser = pd.Series(native_ser)
    other_ser = pd.Series(native_other_ser)
    left, right = ser.align(other_ser, join=join, axis=0)
    assert_series_equal(left, native_left)
    assert_series_equal(right, native_right)


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_basic_series_reorder_index_axis0(join):
    native_ser = native_pd.Series([1, 2, 3], index=["Z", "V", "W"])
    native_other_ser = native_pd.Series(
        [
            60,
            70,
            80,
            90,
        ],
        index=["G", "H", "M", "A"],
    )
    native_left, native_right = native_ser.align(
        native_other_ser,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    ser = pd.Series(native_ser)
    other_ser = pd.Series(native_other_ser)
    left, right = ser.align(other_ser, join=join, axis=0)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(left, native_left)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(right, native_right)


@sql_count_checker(query_count=0)
def test_align_series_axis_None_negative():
    ser = pd.Series([1, 2, 3])
    other_ser = pd.Series([60, 70, 80, 90, 100, np.nan])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'axis=None'",
    ):
        left, right = ser.align(other_ser, join="outer", axis=None)


@sql_count_checker(query_count=0)
def test_align_series_fill_value_negative():
    ser = pd.Series([1, 2, 3])
    other_ser = pd.Series([60, 70, 80, 90, 100, np.nan])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'fill_value'",
    ):
        left, right = ser.align(other_ser, join="outer", axis=0, fill_value="empty")


@sql_count_checker(query_count=0)
def test_align_series_axis_1_negative():
    ser = pd.Series([1, 2, 3])
    other_ser = pd.Series([60, 70, 80, 90, 100, np.nan])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'axis=1'",
    ):
        left, right = ser.align(other_ser, join="outer", axis=1)


@sql_count_checker(query_count=0)
def test_align_series_copy_negative():
    ser = pd.Series([1, 2, 3])
    other_ser = pd.Series([60, 70, 80, 90, 100, np.nan])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'copy=False'",
    ):
        left, right = ser.align(other_ser, join="outer", copy=False)


@sql_count_checker(query_count=0)
def test_align_series_invalid_axis_negative():
    ser = pd.Series([1, 2, 3])
    other_ser = pd.Series([60, 70, 80, 90, 100, np.nan])
    axis = 2
    with pytest.raises(
        ValueError,
        match=f"No axis named {axis} for object type Series",
    ):
        left, right = ser.align(other_ser, join="outer", axis=axis)


@sql_count_checker(query_count=0)
def test_align_series_deprecated_negative():
    ser = pd.Series([1, 2, 3])
    other_ser = pd.Series([60, 70, 80, 90, 100, np.nan])
    for method in ["backfill", "bfill", "pad", "ffill"]:
        with pytest.raises(
            NotImplementedError,
            match="The 'method', 'limit', and 'fill_axis' keywords in Series.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead.",
        ):
            left, right = ser.align(other_ser, join="outer", method=method)
    with pytest.raises(
        NotImplementedError,
        match="The 'method', 'limit', and 'fill_axis' keywords in Series.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead.",
    ):
        left, right = ser.align(other_ser, join="outer", limit=5)
    with pytest.raises(
        NotImplementedError,
        match="The 'method', 'limit', and 'fill_axis' keywords in Series.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead.",
    ):
        left, right = ser.align(other_ser, join="outer", fill_axis=1)
    with pytest.raises(
        NotImplementedError,
        match="The 'broadcast_axis' keyword in Series.align is deprecated and will be removed in a future version.",
    ):
        left, right = ser.align(other_ser, join="outer", broadcast_axis=0)
