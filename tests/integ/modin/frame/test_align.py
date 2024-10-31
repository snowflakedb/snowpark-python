#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_basic_axis0(join):
    native_df = native_pd.DataFrame(
        [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
    )
    native_other_df = native_pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
    )
    native_left, native_right = native_df.align(
        native_other_df,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    df = pd.DataFrame(native_df)
    other_df = pd.DataFrame(native_other_df)
    left, right = df.align(other_df, join=join, axis=0)
    assert_frame_equal(left, native_left)
    assert_frame_equal(right, native_right)


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_basic_reorder_axis0(join):
    native_df = native_pd.DataFrame(
        [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"], index=["R", "L"]
    )
    native_other_df = native_pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
        index=["A", "B", "C"],
    )
    native_left, native_right = native_df.align(
        native_other_df,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    df = pd.DataFrame(native_df)
    other_df = pd.DataFrame(native_other_df)
    left, right = df.align(other_df, join=join, axis=0)
    assert_frame_equal(left, native_left)
    assert_frame_equal(right, native_right)


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_basic_diff_index_axis0(join):
    native_df = native_pd.DataFrame(
        [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"], index=[10, 20]
    )
    native_other_df = native_pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
        index=["one", "two", "three"],
    )
    native_left, native_right = native_df.align(
        native_other_df,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    df = pd.DataFrame(native_df)
    other_df = pd.DataFrame(native_other_df)
    left, right = df.align(other_df, join=join, axis=0)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(left, native_left)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(right, native_right)


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_basic_with_nulls_axis0(join):
    native_df = native_pd.DataFrame(
        [[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
    )
    native_other_df = native_pd.DataFrame(
        [[10, 20, 30, np.nan], [60, np.nan, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
    )
    native_left, native_right = native_df.align(
        native_other_df,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    df = pd.DataFrame(native_df)
    other_df = pd.DataFrame(native_other_df)
    left, right = df.align(other_df, join=join, axis=0)
    assert_frame_equal(left, native_left)
    assert_frame_equal(right, native_right)


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_basic_with_all_null_row_axis0(join):
    native_df = native_pd.DataFrame(
        [[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
    )
    native_other_df = native_pd.DataFrame(
        [[10, 20, 30, np.nan], [60, np.nan, 80, 90], [np.nan, np.nan, np.nan, np.nan]],
        columns=["A", "B", "C", "D"],
    )
    native_left, native_right = native_df.align(
        native_other_df,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    df = pd.DataFrame(native_df)
    other_df = pd.DataFrame(native_other_df)
    left, right = df.align(other_df, join=join, axis=0)
    assert_frame_equal(left, native_left)
    assert_frame_equal(right, native_right)


@sql_count_checker(query_count=0)
def test_align_frame_with_nulls_axis_None_negative():
    df = pd.DataFrame([[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"])
    other_df = pd.DataFrame(
        [[10, 20, 30, np.nan], [60, np.nan, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
    )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'axis=None'",
    ):
        left, right = df.align(other_df, join="outer", axis=None)


@sql_count_checker(query_count=0)
def test_align_frame_fill_value_negative():
    df = pd.DataFrame([[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"])
    other_df = pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
    )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'fill_value'",
    ):
        left, right = df.align(other_df, join="outer", axis=0, fill_value="empty")


@sql_count_checker(query_count=0)
def test_align_frame_axis1_negative():
    df = pd.DataFrame(
        [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"], index=[1, 2]
    )
    other_df = pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
        index=[2, 3, 4],
    )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'axis=1'",
    ):
        left, right = df.align(other_df, join="outer", axis=1)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("level", [0, 1])
def test_level_negative(level):
    df = pd.DataFrame(
        [[1], [2]],
        index=pd.MultiIndex.from_tuples(
            [("foo", "bah", "ack"), ("bar", "bas", "bar")], names=["a", "b", "c"]
        ),
        columns=["num"],
    )
    other_df = pd.DataFrame(
        [[2], [3]],
        index=pd.Series(["foo", "bah"], name="a"),
        columns=["num"],
    )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'level'",
    ):
        left, right = df.align(other_df, join="outer", axis=0, level=0)


@sql_count_checker(query_count=0)
def test_multiindex_negative():
    df = pd.DataFrame(
        [[1], [2]],
        index=pd.MultiIndex.from_tuples(
            [("foo", "bah", "ack"), ("bar", "bas", "bar")], names=["a", "b", "c"]
        ),
        columns=["num"],
    )
    other_df = pd.DataFrame(
        [[2], [3]],
        index=pd.Series(["foo", "bah"], name="a"),
        columns=["num"],
    )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support `align` with MultiIndex",
    ):
        left, right = df.align(other_df, join="outer", axis=0)


@sql_count_checker(query_count=0)
def test_align_frame_copy_negative():
    df = pd.DataFrame([[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"])
    other_df = pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
    )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'align' method doesn't support 'copy=False'",
    ):
        left, right = df.align(other_df, join="outer", axis=0, copy=False)


@sql_count_checker(query_count=0)
def test_align_frame_invalid_axis_negative():
    df = pd.DataFrame([[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"])
    other_df = pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
    )
    axis = 2
    with pytest.raises(
        ValueError,
        match=f"No axis named {axis} for object type DataFrame",
    ):
        left, right = df.align(other_df, join="outer", axis=axis)


@sql_count_checker(query_count=0)
def test_align_frame_deprecated_negative():
    df = pd.DataFrame([[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"])
    other_df = pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
    )
    for method in ["backfill", "bfill", "pad", "ffill"]:
        with pytest.raises(
            NotImplementedError,
            match="The 'method', 'limit', and 'fill_axis' keywords in DataFrame.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead.",
        ):
            left, right = df.align(other_df, join="outer", method=method)
    with pytest.raises(
        NotImplementedError,
        match="The 'method', 'limit', and 'fill_axis' keywords in DataFrame.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead.",
    ):
        left, right = df.align(other_df, join="outer", limit=5)
    with pytest.raises(
        NotImplementedError,
        match="The 'method', 'limit', and 'fill_axis' keywords in DataFrame.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead.",
    ):
        left, right = df.align(other_df, join="outer", fill_axis=1)
    with pytest.raises(
        NotImplementedError,
        match="The 'broadcast_axis' keyword in DataFrame.align is deprecated and will be removed in a future version.",
    ):
        left, right = df.align(other_df, join="outer", broadcast_axis=0)
