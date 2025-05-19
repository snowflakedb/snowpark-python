#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def eval_dataframe_align_result(
    native_df: native_pd.DataFrame,
    native_other_df: native_pd.DataFrame,
    check_dtype: bool,
    **kwargs,
) -> None:
    snow_df = pd.DataFrame(native_df)
    snow_other_df = pd.DataFrame(native_other_df)

    native_left, native_right = native_df.align(native_other_df, **kwargs)
    left, right = snow_df.align(snow_other_df, **kwargs)
    if check_dtype:
        assert_frame_equal(left, native_left)
        assert_frame_equal(right, native_right)
    else:
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(left, native_left)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(right, native_right)


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count, window_count",
    [(0, 2, 10), (1, 0, 0), (None, 2, 10)],
)
def test_align_basic(join, axis, join_count, window_count):
    with SqlCounter(query_count=2, join_count=join_count, window_count=window_count):
        native_df = native_pd.DataFrame(
            [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
        )
        native_other_df = native_pd.DataFrame(
            [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
            columns=["A", "B", "C", "D"],
        )
        eval_dataframe_align_result(
            native_df, native_other_df, check_dtype=True, join=join, axis=axis
        )


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_duplicate_idx(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame(
            [
                [1, 2, 3],
                [6, 7, 8],
                [9, 10, 11],
                [4, 6, 2],
            ],
            columns=["A", "B", "C"],
            index=["one", "two", "three", "two"],
        )
        native_other_df = native_pd.DataFrame(
            [
                [10, 20, 30, 40, 50],
                [60, 70, 80, 90, 100],
                [60, 70, 80, 90, 100],
            ],
            columns=["A", "B", "B", "C", "D"],
            index=["one", "two", "two"],
        )
        eval_dataframe_align_result(
            native_df, native_other_df, check_dtype=True, join=join, axis=axis
        )


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_basic_reorder(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame(
            [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"], index=["R", "L"]
        )
        native_other_df = native_pd.DataFrame(
            [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
            columns=["A", "B", "C", "D"],
            index=["A", "B", "C"],
        )
        eval_dataframe_align_result(
            native_df, native_other_df, check_dtype=True, join=join, axis=axis
        )


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_basic_diff_index(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame(
            [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"], index=[10, 20]
        )
        native_other_df = native_pd.DataFrame(
            [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
            columns=["A", "B", "C", "D"],
            index=["one", "two", "three"],
        )
        eval_dataframe_align_result(
            native_df, native_other_df, check_dtype=False, join=join, axis=axis
        )


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_basic_with_nulls(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame(
            [[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
        )
        native_other_df = native_pd.DataFrame(
            [[10, 20, 30, np.nan], [60, np.nan, 80, 90], [600, 700, 800, 900]],
            columns=["A", "B", "C", "D"],
        )
        eval_dataframe_align_result(
            native_df, native_other_df, check_dtype=True, join=join, axis=axis
        )


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_basic_with_all_null_row(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame(
            [[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
        )
        native_other_df = native_pd.DataFrame(
            [
                [10, 20, 30, np.nan],
                [60, np.nan, 80, 90],
                [np.nan, np.nan, np.nan, np.nan],
            ],
            columns=["A", "B", "C", "D"],
        )
        eval_dataframe_align_result(
            native_df, native_other_df, check_dtype=True, join=join, axis=axis
        )


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_basic_with_empty_df(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame(
            [[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
        )
        native_other_df = native_pd.DataFrame()

        native_left, native_right = native_df.align(
            native_other_df,
            join=join,
            axis=axis,
            limit=None,
            fill_axis=0,
            broadcast_axis=None,
        )
        df = pd.DataFrame(native_df)
        other_df = pd.DataFrame()
        left, right = df.align(other_df, join=join, axis=axis)
        # Need check_column_type=False here since the `inferred_type` of empty columns differs
        # from native pandas (Snowpark pandas gives "empty" vs. native pandas "integer")
        assert_frame_equal(left, native_left, check_column_type=False)
        assert_frame_equal(right, native_right, check_column_type=False)


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_basic_with_zero_cols(join, axis, join_count):
    with SqlCounter(query_count=3, join_count=join_count):
        native_df = native_pd.DataFrame(
            [[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
        )
        native_other_df = native_pd.DataFrame(index=pd.Index([1, 2]))

        native_left, native_right = native_df.align(
            native_other_df,
            join=join,
            axis=axis,
        )
        df = pd.DataFrame(native_df)
        other_df = pd.DataFrame(native_other_df)
        left, right = df.align(other_df, join=join, axis=axis)
        # Need check_column_type=False here since the `inferred_type` of empty columns differs
        # from native pandas (Snowpark pandas gives "empty" vs. native pandas "integer")
        assert_frame_equal(left, native_left, check_column_type=False)
        assert_frame_equal(right, native_right, check_column_type=False)


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_basic_with_zero_rows(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame(
            [[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
        )
        native_other_df = native_pd.DataFrame(columns=["A", "B", "C"])

        eval_dataframe_align_result(
            native_df, native_other_df, check_dtype=True, join=join, axis=axis
        )


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_basic_with_both_empty_dfs(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame()
        native_other_df = native_pd.DataFrame()

        native_left, native_right = native_df.align(
            native_other_df,
            join=join,
            axis=axis,
            limit=None,
            fill_axis=0,
            broadcast_axis=None,
        )
        df = pd.DataFrame(native_df)
        other_df = pd.DataFrame(native_other_df)
        left, right = df.align(other_df, join=join, axis=axis)
        # Need check_column_type=False here since the `inferred_type` of empty columns differs
        # from native pandas (Snowpark pandas gives "empty" vs. native pandas "integer")
        assert_frame_equal(left, native_left, check_column_type=False)
        assert_frame_equal(right, native_right, check_column_type=False)


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize(
    "axis, join_count",
    [(0, 2), (1, 0), (None, 2)],
)
def test_align_overlapping_duplicate_cols(join, axis, join_count):
    with SqlCounter(query_count=2, join_count=join_count):
        native_df = native_pd.DataFrame(
            [[1, 2, 3, 4, 4, 5, 6], [6, 7, 8, 7, 9, 10, 11]],
            columns=["D", "B", "C", "A", "B", "A", "E"],
        )
        native_other_df = native_pd.DataFrame(
            [
                [10, 20, 30, 40, 50, 45, 55],
                [60, 70, 80, 90, 100, 320, 444],
                [600, 700, 800, 900, 1000, 750, 999],
            ],
            columns=["A", "B", "B", "C", "D", "A", "A"],
        )
        native_left, native_right = native_df.align(
            native_other_df,
            join=join,
            axis=axis,
            limit=None,
            fill_axis=0,
            broadcast_axis=None,
        )
        df = pd.DataFrame(native_df)
        other_df = pd.DataFrame(native_other_df)
        left, right = df.align(other_df, join=join, axis=axis)
        assert_frame_equal(left, native_left)
        assert_frame_equal(right, native_right)


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_frame_with_series(join):
    native_df = native_pd.DataFrame(
        [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
    )
    native_other_df = native_pd.DataFrame(
        [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
        columns=["A", "B", "C", "D"],
    )
    eval_dataframe_align_result(
        native_df, native_other_df, check_dtype=True, join=join, axis=0
    )


@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
@pytest.mark.parametrize("enable_sql_simplifier", [True, False])
def test_align_basic_axis0_on_row_position_columns(
    session, join, enable_sql_simplifier
):
    session.sql_simplifier_enabled = enable_sql_simplifier
    expected_window_count = 16
    if not session.sql_simplifier_enabled:
        expected_window_count = 18

    num_cols = 3
    select_data = [f'{i} as "{i}"' for i in range(num_cols)]
    query = f"select {', '.join(select_data)}"

    df1 = pd.read_snowflake(query)
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    df1 = df1.sort_values(df1.columns.to_list())

    df2 = pd.read_snowflake(query)
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    df2 = df2.sort_values(df2.columns.to_list())

    native_df1 = df1.to_pandas()
    native_df2 = df2.to_pandas()

    with SqlCounter(query_count=2, join_count=2, window_count=expected_window_count):
        native_left, native_right = native_df1.align(
            native_df2,
            join=join,
            axis=0,
        )

        left, right = df1.align(df2, join=join, axis=0)

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
    eval_dataframe_align_result(
        native_df, native_other_df, check_dtype=True, join=join, axis=0
    )


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
    eval_dataframe_align_result(
        native_df, native_other_df, check_dtype=False, join=join, axis=0
    )


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_basic_with_nulls_axis0(join):
    native_df = native_pd.DataFrame(
        [[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"]
    )
    native_ser = native_pd.Series([60, 70, 80, 90, 100, 60])
    native_left, native_right = native_df.align(
        native_ser,
        join=join,
        axis=0,
        limit=None,
        fill_axis=0,
        broadcast_axis=None,
    )
    df = pd.DataFrame(native_df)
    other_ser = pd.Series(native_ser)
    left, right = df.align(
        other_ser, join=join, axis=0, limit=None, fill_axis=0, broadcast_axis=None
    )
    assert_snowpark_pandas_equal_to_pandas(left, native_left)
    assert_snowpark_pandas_equal_to_pandas(right, native_right)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("join", ["outer", "inner", "left", "right"])
def test_align_frame_with_series_axis_negative(join):
    df = pd.DataFrame([[1, 2, np.nan, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"])
    other_ser = pd.Series([60, 70, 80, 90, 100, 60])
    with pytest.raises(
        ValueError,
        match="Must specify axis=0 or 1",
    ):
        df.align(other_ser, join=join, axis=None)
    with pytest.raises(
        NotImplementedError,
        match="The Snowpark pandas DataFrame.align with Series other does not support axis=1.",
    ):
        df.align(other_ser, join=join, axis=1)


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
        df.align(other_df, join="outer", axis=0, fill_value="empty")


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("level", [0, 1])
@pytest.mark.parametrize("axis", [0, 1, None])
def test_level_negative(level, axis):
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
        df.align(other_df, join="outer", axis=axis, level=0)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("axis", [0, 1, None])
def test_multiindex_negative(axis):
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
        df.align(other_df, join="outer", axis=axis)


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
        df.align(other_df, join="outer", axis=0, copy=False)


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
        df.align(other_df, join="outer", axis=axis)


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
            df.align(other_df, join="outer", method=method)
    with pytest.raises(
        NotImplementedError,
        match="The 'method', 'limit', and 'fill_axis' keywords in DataFrame.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead.",
    ):
        df.align(other_df, join="outer", limit=5)
    with pytest.raises(
        NotImplementedError,
        match="The 'method', 'limit', and 'fill_axis' keywords in DataFrame.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead.",
    ):
        df.align(other_df, join="outer", fill_axis=1)
    with pytest.raises(
        NotImplementedError,
        match="The 'broadcast_axis' keyword in DataFrame.align is deprecated and will be removed in a future version.",
    ):
        df.align(other_df, join="outer", broadcast_axis=0)
