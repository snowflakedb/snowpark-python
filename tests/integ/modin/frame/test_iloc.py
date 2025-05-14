#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import itertools
import random
import re
from collections.abc import Iterable
from typing import Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from modin.pandas import DataFrame, Series
from modin.pandas.indexing import is_range_like
from pandas._libs.lib import is_bool, is_list_like, is_scalar
from pandas.errors import IndexingError

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.frame.test_head_tail import eval_result_and_query_with_no_join
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import running_on_public_ci
from tests.utils import RUNNING_ON_GH

# default_index_snowpark_pandas_df and default_index_native_df have size of axis_len x axis_len
AXIS_LEN = 7
test_inputs_for_no_scalar_output = [
    ([0], 1, 2),
    (
        [
            2,
            0,
            1,
            1,
            1.0,
            AXIS_LEN - 1.0,
            AXIS_LEN - 0.1,
            AXIS_LEN - 0.9,
            -AXIS_LEN + 0.1,
            -AXIS_LEN + 0.9,
        ],
        1,
        2,
    ),
    ([], 1, 2),
    (np.array([-AXIS_LEN, AXIS_LEN - 1], dtype=np.int64), 1, 2),
    (np.array([-AXIS_LEN, AXIS_LEN - 1], dtype=np.int64), 1, 2),
    (np.array([-AXIS_LEN, AXIS_LEN - 1], dtype=np.int64), 1, 2),
    (np.array([-AXIS_LEN, AXIS_LEN - 1], dtype=np.float16), 1, 2),
    (np.array([-AXIS_LEN, AXIS_LEN - 1], dtype=np.float32), 1, 2),
    (np.array([-AXIS_LEN, AXIS_LEN - 1], dtype=np.float64), 1, 2),
    (
        np.array(
            [
                2,
                0,
                1,
                1,
                1.0,
                AXIS_LEN - 1.0,
                AXIS_LEN - 0.1,
                AXIS_LEN - 0.9,
                -AXIS_LEN + 0.1,
                -AXIS_LEN + 0.9,
            ]
        ),
        1,
        2,
    ),
    (native_pd.Index([0, 2, AXIS_LEN - 1]), 1, 2),
    (native_pd.RangeIndex(1, AXIS_LEN - 1), 1, 0),
    ([True, True, False, False, False, True, True], 1, 1),
    (native_pd.Index([True, True, False, False, False, True, True]), 1, 1),
    (Ellipsis, 1, 0),
    (slice(0, AXIS_LEN + 1, 2), 1, 0),
    (slice(-AXIS_LEN - 2, 0, -1), 1, 0),
    (slice(-1, 0, -1), 1, 0),
]

test_inputs_for_range = [
    slice(None),
    slice(2, AXIS_LEN - 1),
    slice(0, 10),
    slice(-AXIS_LEN - 1, -2),
    slice(2, 3, 1),
    slice(-1, 3, 1),
    slice(3, 3, 1),
    slice(-AXIS_LEN, AXIS_LEN + 1, 1),
    slice(None, 5, None),
    range(3),
]

test_inputs_for_range2 = list(
    slice(res[0], res[1], res[2]) for res in itertools.permutations([None, 0, -1, 1], 3)
)

test_negative_bound_list_input = [([-AXIS_LEN - 0.9], 1, 2)]
test_int_inputs = [
    (0, 1, 0),
    (AXIS_LEN - 1, 1, 0),
    (-AXIS_LEN, 1, 0),
]
test_inputs_on_df_for_dataframe_output = (
    test_int_inputs + test_inputs_for_no_scalar_output
)

snowpark_pandas_input_keys = [
    ("Index", 2),
    ("Series", 2),
    ("Series[positive_int]", 2),
    ("Series_all_positive_int", 2),
    ("RangeIndex", 0),
    ("Index[bool]", 1),
    ("emptyFloatSeries", 2),
    ("multi_index_Series", 2),
]

# Snowflake type checking will fail if the item values aren't type compatible, so we normalize to int to stay compatible.
setitem_key_val_pair = [
    (([3, 1, 2], [0, 1]), np.random.randint(100, size=(3, 7))),
    (([-3, -1, -2], [1, 0]), np.random.randint(100, size=(3, 7))),
    ((slice(None), [1]), np.random.randint(100, size=(7, 3))),
    (([3, 1, 2], [0, 1]), np.random.randint(100, size=7)),
    ((slice(None), [3, 3]), ["991", "992", "993"]),
    (([3, 1, 2], [1]), -1.5),
    (((slice(None), [1]), [3, 1, 2]), -2.5),
    ((slice(None), slice(0, 1, 1)), np.random.randint(100, size=(7, 3))),
    (
        ([True, False, False, True, False, False, True], [0, 1]),
        np.random.randint(100, size=(3, 7)),
    ),
    ((3, 0), np.random.randint(7)),
]

# Test list-like key + all types of invalid val and other types of key + an invalid val
setitem_key_negative_val_pair = [
    ([3, 1, 2], [["991"]] * 7),
    ([3, 1, 2], ["991"]),
    ([3, 1, 2], [["991"] * 3 for _ in range(7)]),
    (slice(0, 6, 2), [["991"]] * 7),
    ([True, False, False, True, False, False, True], [["991"]] * 7),
]

setitem_snowpark_pandas_input_key = ["series", "series_broadcast", "df_broadcast"]
negative_setitem_snowpark_pandas_input_key = ["7x1", "1x1", "7x3"]

TEST_ITEMS_DATA_2X2 = {"foo": [-999, -998], "bar": [-997, -996]}


@pytest.mark.parametrize(
    "key, expected_query_count, expected_join_count",
    test_negative_bound_list_input + test_inputs_on_df_for_dataframe_output,
)
def test_df_row_return_dataframe(
    key,
    expected_query_count,
    expected_join_count,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: df.iloc[key],
        )


@pytest.mark.parametrize(
    "key, expected_query_count, not_used",
    test_inputs_on_df_for_dataframe_output,
)
def test_df_iloc_get_dataframe(
    key,
    expected_query_count,
    not_used,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    with SqlCounter(query_count=expected_query_count, join_count=0):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: df.iloc[slice(None), key],
        )


@pytest.mark.parametrize("key", test_inputs_for_range)
@sql_count_checker(query_count=1)
def test_df_iloc_get_dataframe_with_range(
    key, default_index_snowpark_pandas_df, default_index_native_df
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.iloc[slice(None), key],
        comparator=eval_result_and_query_with_no_join,
    )


# TODO: SNOW-916739 some of those cases should be optimized to skip joins
@pytest.mark.parametrize("key", test_inputs_for_range2)
def test_df_iloc_get_dataframe_with_range2(
    key, default_index_snowpark_pandas_df, default_index_native_df
):
    if key.step == 0:
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                default_index_snowpark_pandas_df,
                default_index_native_df,
                lambda df: df.iloc[slice(None), key],
                expect_exception=True,
                expect_exception_match="slice step cannot be zero",
                expect_exception_type=ValueError,
            )
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                default_index_snowpark_pandas_df,
                default_index_native_df,
                lambda df: df.iloc[slice(None), key],
            )


@pytest.mark.parametrize("row_key", [slice(None), [1, 0]])
@pytest.mark.parametrize("col_key", [[0], [0, 0]])
def test_df_iloc_get_dataframe_after_sort(row_key, col_key):
    with SqlCounter(query_count=1, join_count=2 if isinstance(row_key, list) else 0):
        native_df = native_pd.DataFrame({"A": [1, 2]})
        # Generate a dataframe where ordering column is also a data column.
        snow_df = pd.DataFrame(native_df)

        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.iloc[row_key, col_key],
        )


@pytest.mark.parametrize(
    "col_key, expected_cols",
    [
        (-1337, 0),
        (1337, 0),
        ([True, True, False, True], 3),
        ([True, False, True, False, False, False, False, False, True], 2),
    ],
)
def test_df_iloc_get_col_key_out_of_bounds(
    col_key, expected_cols, default_index_snowpark_pandas_df
):
    key = (pd.Series([0, 2, 4]), col_key)
    df = default_index_snowpark_pandas_df
    result = df.iloc[key]
    with SqlCounter(query_count=1, join_count=2):
        assert result.shape == (3, expected_cols)


@pytest.mark.parametrize(
    "key, expected_join_count",
    snowpark_pandas_input_keys,
)
def test_df_iloc_get_row_input_snowpark_pandas_return_dataframe(
    key,
    expected_join_count,
    iloc_snowpark_pandas_input_map,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    expected_query_count = 2
    if key == "Index" or key == "Index[bool]":
        # extra query to convert to native pandas
        expected_query_count = 3
    elif key == "RangeIndex":
        expected_query_count = 1
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: df.iloc[
                try_convert_index_to_native(iloc_snowpark_pandas_input_map[key])
            ],
        )


@pytest.mark.parametrize(
    "key, not_used_for_test",
    snowpark_pandas_input_keys,
)
def test_df_iloc_get_col_input_snowpark_pandas_return_dataframe(
    key,
    not_used_for_test,
    iloc_snowpark_pandas_input_map,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    def eval_func(df):
        label = iloc_snowpark_pandas_input_map[key]

        # convert to native pandas because iloc_snowpandas_input_map[key] holds SnowPandas objects
        if not isinstance(df, DataFrame) and isinstance(
            label, (Series, DataFrame, pd.Index)
        ):
            label = label.to_pandas()

        return df.iloc[slice(None), label]

    expected_query_count = 3
    if key == "RangeIndex":
        expected_query_count = 1

    with SqlCounter(query_count=expected_query_count, join_count=0):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df, default_index_native_df, eval_func
        )


@pytest.mark.parametrize(
    "key",
    [
        (-AXIS_LEN, 1),
        (..., 1, 2),  # leading ellipsis should be stripped
    ],
)
@sql_count_checker(query_count=1)
def test_df_iloc_get_scalar(
    key, default_index_snowpark_pandas_df, default_index_native_df
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.iloc[key],
        comparator=lambda x, y: type(x) is type(y) and x == y,
    )


@sql_count_checker(query_count=5, join_count=4)
def test_df_iloc_get_callable(
    default_index_snowpark_pandas_df, default_index_native_df
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.iloc[lambda x: try_convert_index_to_native(x.index) % 2 == 0],
    )

    def test_func(df: DataFrame) -> Series:
        return df["A"] - 1

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.iloc[test_func],
    )

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.iloc[
            lambda x: try_convert_index_to_native(x.index) % 2 == 0, [2, 3]
        ],
    )


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.set_index("B", inplace=False),
        lambda df: df.sort_values("C", inplace=False),
    ],
    ids=["non-default_index", "sort_on_non-index"],
)
@sql_count_checker(query_count=1, join_count=2)
def test_df_iloc_get_indexed_and_sorted(func, default_index_native_df):
    # This is to test ordering columns, index and row_position column are set correctly after an iloc that
    # takes part of df column and row wise and in a different order.
    native_df = func(default_index_native_df)
    snow_df = DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.iloc[([3, 1], [2, 0])],
    )


@pytest.mark.parametrize(
    "key",
    [
        [],
        ([], []),
        (slice(None), []),
        (slice(None), slice(None)),
    ],
)
def test_df_iloc_get_empty_key(
    key,
    empty_snowpark_pandas_df,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            empty_snowpark_pandas_df,
            native_pd.DataFrame(),
            lambda df: df.iloc[key],
            comparator=assert_snowpark_pandas_equal_to_pandas,
            # Need check_column_type=False since the `inferred_type` of empty columns differs
            # from native pandas (Snowpark pandas gives "empty" vs. native pandas "integer")
            check_column_type=False,
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: df.iloc[key],
            check_column_type=False,
        )


@sql_count_checker(query_count=1)
def test_df_iloc_get_empty(empty_snowpark_pandas_df):
    _ = empty_snowpark_pandas_df.iloc[0]


@sql_count_checker(query_count=1)
def test_df_iloc_get_diff2native(
    default_index_snowpark_pandas_df, default_index_native_df
):
    # Native pandas raises error on df.iloc[..., np.array([0, 1])] because it calls tuple.count(Ellipsis) which raises
    # `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()` from
    # numpy. This is a common error when not calling elementwise equality and seems like an unintended error.
    # Thus df.iloc[..., np.array([0, 1])] is valid in snowpark pandas
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.iloc[..., np.array([0, 1])]
        if isinstance(df, DataFrame)
        else df.iloc[..., [0, 1]],
    )


@sql_count_checker(query_count=2, join_count=4)
def test_df_iloc_get_with_conflict():
    # index and data columns have conflict in get_by_col
    df = DataFrame({"A": [0, 1]}, index=native_pd.Index([2, 3], name="A")).rename(
        columns={"A": "__A__"}
    )
    native_df = native_pd.DataFrame(
        {"A": [0, 1]}, index=native_pd.Index([2, 3], name="A")
    ).rename(columns={"A": "__A__"})
    eval_snowpark_pandas_result(df, native_df, lambda df: df.iloc[[0], [0, 0]])

    # df's index and key's ordering column have conflict in get_by_col
    df = DataFrame(
        {"A": [0, 1]}, index=native_pd.Index([2, 3], name="__row_position__")
    )
    native_df = native_pd.DataFrame(
        {"A": [0, 1]}, index=native_pd.Index([2, 3], name="__row_position__")
    )
    eval_snowpark_pandas_result(df, native_df, lambda df: df.iloc[[0], [0, 0]])


@sql_count_checker(query_count=1, join_count=2)
def test_df_iloc_get_sort_on_index():
    df = DataFrame({"A": [0, 1]}).sort_values("A")
    native_df = native_pd.DataFrame({"A": [0, 1]}).sort_values("A")
    eval_snowpark_pandas_result(df, native_df, lambda df: df.iloc[[0], [0]])


@pytest.mark.parametrize(
    "row_key, col_key, item_value",
    [
        ([3, 1], [0], 123),
        ([3, 1, 2], [0], 987),
        ([3, 2, 1], [0], [[123], [234], [345]]),
        ([5, 6], [1], 99.99),
        ([3, 6, 5], [1], 99.99),
        ([5, 3, 6], [1], [[11.11], [22.22], [33.33]]),
        ([4, 2], [1, 0], [[234, 345], [456, 567]]),
        ([6, 4, 2], [1, 0], 234),
        ([2, 4, 6], [1, 0], [[234, 345], [456, 567], [678, 789]]),
        ([5, 3], [2], True),
        ([5, 3, 1], [2], [True, True, True]),
        ([1, 5, 3], [2], [False, False, False]),
        ([3, 1], [3], "x"),
        ([5, 3, 1], [3], "w"),
        ([1, 5, 3], [3], [["x"], ["y"], ["z"]]),
        ([4, 2], [4], "2022-12-31"),
        ([4, 3, 1], [4], "2022-12-31"),
        ([5, 3, 4], [4], [["2022-12-31"], ["2022-12-30"], ["]2022-12-29"]]),
    ],
)
def test_df_iloc_set_different_item_compatible_types(
    row_key,
    col_key,
    item_value,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    def operation(df):
        df.iloc[row_key, col_key] = item_value

    expected_join_count = 3 if isinstance(item_value, list) else 2
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            operation,
            inplace=True,
        )


# These test cases succeed in pandas but fail in Snowpark pandas because of snowflake type system.
@pytest.mark.parametrize(
    "item_value",
    [
        123,
        99.99,
        True,
        "abc",
        (99,),
        [[88]],
    ],
)
@pytest.mark.parametrize("col_key", [5, 6])
@sql_count_checker(query_count=0)
def test_df_iloc_set_different_incompatible_types(
    col_key, item_value, default_index_snowpark_pandas_df
):
    row_key = [3, 5, 1]
    df = default_index_snowpark_pandas_df

    with pytest.raises(SnowparkSQLException):
        df.iloc[row_key, col_key] = item_value
        df.to_pandas()


@pytest.mark.parametrize("key, val", setitem_key_negative_val_pair)
@sql_count_checker(query_count=0)
def test_df_iloc_set_negative(
    key, val, default_index_snowpark_pandas_df, default_index_native_df
):
    def operation(df):
        df.iloc[key] = val

    # This fails on the Snowpark pandas side because of type system incompatibility in setting item value to the column
    # in snowflake.
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        operation,
        inplace=True,
        expect_exception=True,
        assert_exception_equal=False,
    )


@pytest.mark.parametrize(
    "key",
    setitem_snowpark_pandas_input_key,
)
@sql_count_checker(query_count=0)
def test_df_iloc_set_snowpark_pandas_input_negative_incompatible_types(
    key,
    iloc_setitem_snowpark_pandas_pair_map,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    df = default_index_snowpark_pandas_df

    with pytest.raises(SnowparkSQLException):
        df.iloc[
            iloc_setitem_snowpark_pandas_pair_map[key][0][0]
        ] = iloc_setitem_snowpark_pandas_pair_map[key][0][1]
        df.to_pandas()


@sql_count_checker(query_count=1, join_count=3)
def test_df_iloc_set_with_duplicates_single_row_item():
    data = {"A": [1, 2, 3, 4], "B": [3, 4, 5, 2]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    def helper(df):
        # The logic in pandas is here to overwrite first element at [0, 0] with 99,
        # then [0, 0] with 97, then [1, 0] with 96.
        # By removing duplicates via taking the last_value in a group, this behavior can be simulated.
        # see indexing_utils.py::set_frame_2d_positional for details.
        df.iloc[[0, 0, 1], 0] = [99, 97, 96]

    eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)


@sql_count_checker(query_count=1, join_count=3)
def test_df_iloc_set_with_duplicates_2d():
    data = {"A": [1, 2, 3, 4], "B": [3, 4, 5, 2]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    item_data = np.array([[99, 97, 96], [20, 27, 26], [71, 72, 73]])

    snow_item = pd.DataFrame(item_data.T)
    native_item = native_pd.DataFrame(item_data.T)

    def helper(df):
        # similar as for single_row case, but this time with 2d assignment incl. duplicates
        df.iloc[[0, 0, 1], [0, 1, 0]] = (
            snow_item if isinstance(df, pd.DataFrame) else native_item
        )

    eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)


@sql_count_checker(query_count=1, join_count=0)
def test_df_iloc_get_with_numpy_types():
    data = {
        "A": [np.int8(3)],
        "B": [np.int16(4)],
        "C": [np.int32(5)],
        "D": [np.int64(6)],
        "E": [np.float32(2.718)],
        "F": [np.float64(3.14519)],
        "G": [np.bool_(False)],
        "H": [np.datetime64("2010-01-01T00:00:00", "25s")],
        "I": [np.half(3.14159)],
    }
    native_df = native_pd.DataFrame(
        data,
        index=native_pd.Index(["foo"]),
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.iloc[0, 0 : len(data)],
        inplace=True,
    )


# TODO: SNOW-955975 Add MultiIndex tests for columns. Add more .iloc[] tests for row and col.
# Types of input to test with get iloc.
KEY_TYPES = ["list", "series", "index", "ndarray", "index with name"]
ILOC_GET_KEY_AXIS = ["row", "col"]


@pytest.mark.parametrize(
    "key",
    [
        [],
        [True] * 7,
        [False] * 7,
        [random.choice([True, False]) for _ in range(7)],
        # length mismatch
        [random.choice([True, False]) for _ in range(random.randint(1, 7))],
        [random.choice([True, False]) for _ in range(random.randint(8, 20))],
    ],
)
@pytest.mark.parametrize("key_type", KEY_TYPES)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
def test_df_iloc_get_key_bool(
    key,
    key_type,
    axis,
    default_index_snowpark_pandas_df,
    default_index_native_df,
    multiindex_native,
    native_df_with_multiindex_columns,
):
    # Check whether df.iloc[key] and df.iloc[:, key] works on given df with:
    # - boolean list      - boolean Index (with and without name)
    # - boolean Series    - np.ndarray
    def iloc_helper(df):
        # Note:
        # 1. boolean series key is not implemented in pandas, so we use list key to test it
        # 2. if key length does not match with df, Snowpark will only select the row position the key contains; while
        # pandas will raise error, so we first truncate the df for pandas and then compare the result
        _df, _key = df, key
        if isinstance(df, native_pd.DataFrame):
            # If native pandas Series, truncate the series and key.
            if axis == "row":
                _df = _df[: len(_key)]
                _key = _key[: len(_df)]
            else:
                _df = _df.iloc[:, : len(_key)]
                _key = _key[: _df.shape[1]]

        # Convert key to the required type.
        if key_type == "index":
            _key = (
                pd.Index(_key, dtype=bool)
                if isinstance(_df, pd.DataFrame)
                else native_pd.Index(_key, dtype=bool)
            )
        elif key_type == "ndarray":
            _key = np.array(_key)
        elif key_type == "index with name":
            _key = (
                pd.Index(_key, name="some name", dtype=bool)
                if isinstance(_df, pd.DataFrame)
                else native_pd.Index(_key, name="some name", dtype=bool)
            )
        elif key_type == "series" and isinstance(_df, pd.DataFrame):
            # Native pandas does not support iloc with Snowpark Series.
            _key = pd.Series(_key, dtype=bool)

        return _df.iloc[_key] if axis == "row" else _df.iloc[:, _key]

    # One extra query for index conversion to series to set item
    query_count = (
        2 if ("index" in key_type or key_type == "series") and axis == "col" else 1
    )
    if axis == "row":
        if key == [] and key_type in ["list", "ndarray"]:
            expected_join_count = 2
        else:
            expected_join_count = 1
    else:
        expected_join_count = 0

    # test df with default index
    with SqlCounter(query_count=query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            iloc_helper,
        )

    # test df with non-default index
    with SqlCounter(query_count=query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df.set_index("D"),
            default_index_native_df.set_index("D"),
            iloc_helper,
        )

    # test df with MultiIndex on index
    # Index dtype is different between Snowpark and native pandas if key produces empty df.
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=False,  # some tests don't match index type with pandas
        )

    # test df with MultiIndex on columns
    is_row = True if axis == "row" else False
    snowpark_df_with_multiindex_columns = pd.DataFrame(
        native_df_with_multiindex_columns
    )
    with SqlCounter(query_count=query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snowpark_df_with_multiindex_columns,
            native_df_with_multiindex_columns,
            iloc_helper,
            check_index_type=False,
            check_column_type=is_row,
        )

    # test df with MultiIndex on both index and columns
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=False,
            check_column_type=is_row,
        )


@pytest.mark.parametrize(
    "key",
    [
        [],
        [random.choice([True, False]) for _ in range(random.randint(0, 100))],
        [random.choice([True, False]) for _ in range(random.randint(1000, 2000))],
    ],
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow test")
def test_df_iloc_get_key_bool_series_with_1k_shape(key, native_df_1k_1k):
    def iloc_helper(df):
        # Note:
        # 1. boolean series key is not implemented in pandas, so we use list key to test it
        # 2. if key length does not match with df, Snowpark will only select the row position the key contains; while
        # pandas will raise error, so we first truncate the df for pandas and then compare the result
        return (
            df.iloc[pd.Series(key, dtype="bool")]
            if isinstance(df, pd.DataFrame)
            else df.iloc[: len(key)].iloc[key[: len(df)]]
        )

    # 4 queries includes 3 queries to prepare the temp table for df, including create,
    # insert, drop the temp table (3) and one select query.
    # 7 queries add extra 3 queries to prepare the temp table for key.
    query_count = 7 if len(key) >= 300 else 4
    _test_df_iloc_with_1k_shape(native_df_1k_1k, iloc_helper, query_count, 1)


def _test_df_iloc_with_1k_shape(
    native_df_1k_1k,
    iloc_helper,
    expected_query_count,
    expected_join_count,
    high_count_reason=None,
):
    df_1k_1k = pd.DataFrame(native_df_1k_1k)
    high_count_expected = high_count_reason is not None

    # test df with default index
    with SqlCounter(
        query_count=expected_query_count,
        join_count=expected_join_count,
        high_count_expected=high_count_expected,
        high_count_reason=high_count_reason,
    ):
        eval_snowpark_pandas_result(
            df_1k_1k,
            native_df_1k_1k,
            iloc_helper,
        )

    # test df with non-default index
    native_df_1k_1k_non_default_index = native_df_1k_1k.set_index("c0")
    df_1k_1k_non_default_index = pd.DataFrame(native_df_1k_1k_non_default_index)
    with SqlCounter(
        query_count=expected_query_count,
        join_count=expected_join_count,
        high_count_expected=high_count_expected,
        high_count_reason=high_count_reason,
    ):
        eval_snowpark_pandas_result(
            df_1k_1k_non_default_index,
            native_df_1k_1k_non_default_index,
            iloc_helper,
        )

    # test df 1 col with default index
    native_df_1k_1 = native_df_1k_1k[["c0"]]
    df_1k_1 = pd.DataFrame(native_df_1k_1)
    with SqlCounter(
        query_count=expected_query_count,
        join_count=expected_join_count,
        high_count_expected=high_count_expected,
        high_count_reason=high_count_reason,
    ):
        eval_snowpark_pandas_result(
            df_1k_1,
            native_df_1k_1,
            iloc_helper,
        )
    native_df_1k_1_non_default_index = native_df_1k_1k[["c0", "c1"]].set_index("c0")
    df_1k_1_non_default_index = pd.DataFrame(native_df_1k_1_non_default_index)

    # test df 1 col with non-default index
    with SqlCounter(
        query_count=expected_query_count,
        join_count=expected_join_count,
        high_count_expected=high_count_expected,
        high_count_reason=high_count_reason,
    ):
        eval_snowpark_pandas_result(
            df_1k_1_non_default_index,
            native_df_1k_1_non_default_index,
            iloc_helper,
        )


@pytest.mark.parametrize(
    "key",
    [
        [],
        [0],
        [-1],
        # unsorted with duplicates
        [2, 3, 1, 3, 2, 1],
        [random.choice(range(-20, 20)) for _ in range(random.randint(1, 20))],
        # implicitly support float
        [-0.1, -1.9, 2.1, 2.6],
        [random.uniform(-20, 20) for _ in range(random.randint(1, 20))],
    ],
)
@pytest.mark.parametrize("key_type", KEY_TYPES)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
def test_df_iloc_get_key_numeric(
    key,
    key_type,
    axis,
    default_index_snowpark_pandas_df,
    default_index_native_df,
    multiindex_native,
    native_df_with_multiindex_columns,
):
    # Check whether df.iloc[key] and df.iloc[:, key] works on a given df with:
    # - numeric list      - numeric Index (with and without name)
    # - numeric Series    - np.ndarray
    def iloc_helper(df):
        if isinstance(df, native_pd.DataFrame):
            # If native pandas DataFrame, remove out-of-bound values to avoid errors and compare.
            # pandas bug: only in the case of column indexing, pandas does not allow integer keys of value less
            # than -len(df). Convert anything between -7 and -6 to -6 for non-default index and keys between -8 and -7
            # to -7. For example, in a df with 7 rows and 7 cols, df.iloc[:, [-7.1]] produces an error but
            # df.iloc[:, [-7.0]] is fine. Both should round to -7.
            _key = []
            num_elements = num_cols if axis == "col" else 7  # num_rows is always 7
            lower_bound, upper_bound = -num_elements - 1, num_elements
            for k in key:
                if lower_bound < k < upper_bound:
                    _key.append(k if k >= (lower_bound + 1) else (lower_bound + 1))
        else:
            _key = key

        # Convert key to the required type.
        if key_type == "index":
            _key = (
                pd.Index(_key, dtype=float)
                if isinstance(df, pd.DataFrame)
                else native_pd.Index(_key, dtype=float)
            )
        elif key_type == "ndarray":
            _key = np.array(_key)
        elif key_type == "index with name":
            _key = (
                pd.Index(_key, name="some name", dtype=float)
                if isinstance(df, pd.DataFrame)
                else native_pd.Index(_key, name="some name", dtype=float)
            )
        elif key_type == "series" and isinstance(df, pd.DataFrame):
            # Native pandas does not support iloc with Snowpark Series.
            _key = pd.Series(_key, dtype=float if len(key) == 0 else None)

        return df.iloc[_key] if axis == "row" else df.iloc[:, _key]

    # one extra query for index conversion to series to set item
    query_count = (
        2 if ("index" in key_type or key_type == "series") and axis == "col" else 1
    )
    join_count = 2 if axis == "row" else 0

    # test df with default index
    num_cols = 7
    with SqlCounter(query_count=query_count, join_count=join_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            iloc_helper,
        )

    # test df with non-default index
    num_cols = 6  # set_index() makes the number of columns 6
    with SqlCounter(query_count=query_count, join_count=join_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df.set_index("D"),
            default_index_native_df.set_index("D"),
            iloc_helper,
        )

    # test df with MultiIndex
    # Index dtype is different between Snowpark and native pandas if key produces empty df.
    is_row = True if axis == "row" else False
    num_cols = 7
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=query_count, join_count=join_count):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=False,  # some tests don't match index type with pandas
        )

    # test df with MultiIndex on columns
    snowpark_df_with_multiindex_columns = pd.DataFrame(
        native_df_with_multiindex_columns
    )
    with SqlCounter(query_count=query_count, join_count=join_count):
        eval_snowpark_pandas_result(
            snowpark_df_with_multiindex_columns,
            native_df_with_multiindex_columns,
            iloc_helper,
            check_index_type=False,
            check_column_type=is_row,
        )

    # test df with MultiIndex on both index and columns
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=query_count, join_count=join_count):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=False,
            check_column_type=is_row,
        )


def test_df_iloc_get_key_int_series_with_name(default_index_snowpark_pandas_df):
    # assume the name of the key won't be populated into iloc result
    series_key_wo_name = pd.Series([1, 2, 3])
    series_key_with_name = pd.Series([1, 2, 3], name="series_with_name")
    with SqlCounter(query_count=2, join_count=4):
        assert_frame_equal(
            default_index_snowpark_pandas_df.iloc[series_key_with_name],
            default_index_snowpark_pandas_df.iloc[series_key_wo_name],
        )

    # For each series, the first query turns the series key into a list, the second query runs .iloc[].
    with SqlCounter(query_count=4, join_count=0):
        assert_frame_equal(
            default_index_snowpark_pandas_df.iloc[(slice(None), series_key_with_name)],
            default_index_snowpark_pandas_df.iloc[(slice(None), series_key_wo_name)],
        )


@pytest.mark.parametrize(
    "key",
    [
        [],
        # int key with short size (i.e., generated by inline sql)
        [random.randint(-1500, 1500) for _ in range(random.randint(0, 100))],
        # float key with large size (i.e., generated by temp table)
        [random.uniform(-1500, 1500) for _ in range(random.randint(1000, 1500))],
    ],
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow test")
def test_df_iloc_get_key_int_series_with_1k_shape(key, native_df_1k_1k):
    def iloc_helper(df):
        # similarly, remove out-of-bound values, so we can avoid error and compare
        return (
            df.iloc[pd.Series(key, dtype=float if len(key) == 0 else None)]
            if isinstance(df, pd.DataFrame)
            else df.iloc[[k for k in key if -1001 < k < 1000]]
        )

    # 4 queries includes 3 queries to prepare the temp table for df, including create,
    # insert, drop the temp table (3) and one select query.
    # 7 queries add extra 3 queries to prepare the temp table for key.
    query_count = 4 if len(key) < 300 else 7
    _test_df_iloc_with_1k_shape(native_df_1k_1k, iloc_helper, query_count, 2)


ILOC_GET_INT_SCALAR_KEYS = [0, -3, 4, -7, 6, -6, -8, 7, 52879115, -9028751]


@pytest.mark.parametrize("key", ILOC_GET_INT_SCALAR_KEYS)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
def test_df_iloc_get_key_scalar(
    key,
    axis,
    default_index_snowpark_pandas_df,
    default_index_native_df,
    multiindex_native,
    native_df_with_multiindex_columns,
):
    # Use test_attrs=False in all of these eval functions because iloc_helper may return a new empty native series

    # Check whether DataFrame.iloc[key] and DataFrame.iloc[:, key] works with integer scalar keys.
    def iloc_helper(df):
        # use [] instead of out-of-bounds values
        num_elements = num_cols if axis == "col" else 7  # num_rows is always 7
        lower_bound, upper_bound = -num_elements - 1, num_elements
        if isinstance(df, pd.DataFrame) or lower_bound < key < upper_bound:
            return df.iloc[key] if axis == "row" else df.iloc[:, key]
        else:
            return native_pd.Series([]) if axis == "row" else df.iloc[:, []]

    def determine_query_count():
        # Multiple queries because of squeeze() - in range is 2, out-of-bounds is 1.
        if axis == "col":
            num_queries = 1
        else:
            if not -8 < key < 7:  # key is out of bound
                num_queries = 2
            else:
                num_queries = 1
        return num_queries

    query_count = determine_query_count()
    # test df with default index
    num_cols = 7
    with SqlCounter(query_count=query_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            iloc_helper,
            test_attrs=False,
        )

    # test df with non-default index
    num_cols = 6  # set_index() makes the number of columns 6
    with SqlCounter(query_count=query_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df.set_index("D"),
            default_index_native_df.set_index("D"),
            iloc_helper,
            test_attrs=False,
        )

    query_count = determine_query_count()
    # test df with MultiIndex
    # Index dtype is different between Snowpark and native pandas if key produces empty df.
    num_cols = 7
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=query_count):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=False,  # some tests don't match index type with pandas
            test_attrs=False,
        )

    # test df with MultiIndex on columns
    snowpark_df_with_multiindex_columns = pd.DataFrame(
        native_df_with_multiindex_columns
    )
    in_range = True if (-8 < key < 7) else False
    with SqlCounter(query_count=query_count):
        if axis == "row" or in_range:  # series result
            eval_snowpark_pandas_result(
                snowpark_df_with_multiindex_columns,
                native_df_with_multiindex_columns,
                iloc_helper,
                check_index_type=False,
                test_attrs=False,
            )
        else:
            eval_snowpark_pandas_result(  # df result
                snowpark_df_with_multiindex_columns,
                native_df_with_multiindex_columns,
                iloc_helper,
                check_index_type=False,
                check_column_type=False,
                test_attrs=False,
            )

    # test df with MultiIndex on both index and columns
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=query_count):
        if axis == "row" or in_range:  # series result
            eval_snowpark_pandas_result(
                snowpark_df,
                native_df,
                iloc_helper,
                check_index_type=False,
                test_attrs=False,
            )
        else:  # df result
            eval_snowpark_pandas_result(
                snowpark_df,
                native_df,
                iloc_helper,
                check_index_type=False,
                check_column_type=False,
                test_attrs=False,
            )


@pytest.mark.parametrize("key", [-7.2, 6.5, -120.3, 23.9])
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
@sql_count_checker(query_count=0, join_count=0)
def test_df_iloc_get_with_float_scalar_negative(
    key, axis, default_index_snowpark_pandas_df, default_index_native_df
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.iloc[key] if axis == "row" else df.iloc[:, key],
        expect_exception=True,
        assert_exception_equal=False,
    )


@pytest.mark.parametrize(
    "key, error_msg, except_type",
    [
        (
            native_pd.Series(["native", "pandas", "series", "of", "strings", ""]),
            re.escape(
                "<class 'pandas.core.series.Series'> is not supported as 'value' argument. "
                + "Please convert this to Snowpark pandas objects by calling "
                + "modin.pandas.Series()/DataFrame()"
            ),
            TypeError,
        ),
        (
            native_pd.DataFrame({"A": [1, 2, 3, "hi"], "B": [0.9, -10, -5 / 6, "bye"]}),
            re.escape(
                "<class 'pandas.core.frame.DataFrame'> is not supported as 'value' argument. "
                + "Please convert this to Snowpark pandas objects by calling "
                + "modin.pandas.Series()/DataFrame()"
            ),
            TypeError,
        ),
        (
            (Ellipsis, [Ellipsis, 1]),
            re.escape(".iloc requires numeric indexers, got [Ellipsis, 1]"),
            IndexError,
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_df_iloc_get_native_pd_key_raises_type_error_negative(
    key, error_msg, default_index_snowpark_pandas_df, except_type
):
    # Check whether invalid keys passed in raise TypeError. Native pandas objects cannot be used as keys.
    with pytest.raises(except_type, match=error_msg):
        _ = default_index_snowpark_pandas_df.iloc[key]


@pytest.mark.parametrize(
    "key, error_msg",
    [
        (((1, 3), 0), "Too many indexers"),
        ((1, 1, 1), "Too many indexers"),
        (((0, 1), (0, 1)), "Too many indexers"),
        ((..., ...), "indexer may only contain one '...' entry"),
        ((..., ..., ...), "indexer may only contain one '...' entry"),
    ],
)
@sql_count_checker(query_count=0)
def test_df_iloc_get_key_raises_indexing_error_negative(
    key, error_msg, default_index_snowpark_pandas_df
):
    # Check whether invalid keys passed in raise IndexError. Raised when tuples or Categorical objects
    # are used as row/col objects, too many ellipses used, more than two values inside a tuple key.
    with pytest.raises(IndexingError, match=error_msg):
        _ = default_index_snowpark_pandas_df.iloc[key]


@pytest.mark.parametrize(
    "key",
    [
        slice(0, 0.1),  # stop is not an int
        slice(1, 2, 3.5),  # step is not an int
        slice(1.1, 2.1, 3),  # start and stop are not ints
    ],
)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
@sql_count_checker(query_count=0)
def test_df_iloc_get_invalid_slice_key_negative(
    key, axis, default_index_snowpark_pandas_df
):
    # TypeError raised when non-integer scalars used as start, stop, or step in slice.
    error_msg = "cannot do positional indexing with these indexers"
    with pytest.raises(TypeError, match=error_msg):
        _ = (
            default_index_snowpark_pandas_df.iloc[key]
            if axis == "row"
            else default_index_snowpark_pandas_df.iloc[:, key]
        )


@pytest.mark.parametrize(
    "key",
    [
        None,
        True,
        False,
        -3.14,
        22 / 7,
        np.nan,
        np.array(["this", "is", "an", "ndarray!"]),
        native_pd.Index(["index", "of", "strings"]),
        native_pd.Index([]),
        native_pd.Index([], dtype=str),
        "string",
        "test",
        ["list", "of", "strings"],
        np.array([1.2, None, "hi"]),
    ],
)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
def test_df_iloc_get_non_numeric_key_negative(
    key, axis, default_index_snowpark_pandas_df
):
    # Check whether invalid non-numeric keys passed in raise TypeError. list-like objects need to be numeric, scalar
    # keys can only be integers. Native pandas Series and DataFrames are invalid inputs.

    if isinstance(key, native_pd.Index):
        key = pd.Index(key)
    with SqlCounter(query_count=2 if isinstance(key, pd.Index) else 0):
        # General case fails with TypeError.
        error_msg = re.escape(f".iloc requires numeric indexers, got {key}")
        with pytest.raises(IndexError, match=error_msg):
            _ = (
                default_index_snowpark_pandas_df.iloc[key]
                if axis == "row"
                else default_index_snowpark_pandas_df.iloc[:, key]
            )


@sql_count_checker(query_count=0)
def test_df_iloc_get_non_numeric_key_categorical_negative(
    default_index_snowpark_pandas_df,
):
    key = (12, native_pd.Categorical([1, 3, 5]))
    error_msg = re.escape(f".iloc requires numeric indexers, got {key[1]}")
    with pytest.raises(IndexError, match=error_msg):
        _ = default_index_snowpark_pandas_df.iloc[key]


@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
@sql_count_checker(query_count=0)
def test_df_iloc_get_key_snowpark_df_input_negative(
    axis, default_index_snowpark_pandas_df
):
    # Verify that Snowpark DataFrame is invalid input.
    key = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    error_msg = "DataFrame indexer is not allowed for .iloc\nConsider using .loc for automatic alignment."
    with pytest.raises(IndexError, match=error_msg):
        _ = (
            default_index_snowpark_pandas_df.iloc[key]
            if axis == "row"
            else default_index_snowpark_pandas_df.iloc[:, key]
        )


@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
@sql_count_checker(query_count=1)
def test_df_iloc_get_key_snowpark_empty_str_series_input_negative(
    axis, default_index_snowpark_pandas_df
):
    # 3 out of the 4 queries are because of raising TypeError in _validate_iloc_keys_are_numeric. 4th query is to close
    # the session.
    # Verify that Empty Snowpark Series of string type is invalid input.
    key = pd.Series([], dtype=str)
    error_msg = re.escape(
        ".iloc requires numeric indexers, got Series([], dtype: object"
    )
    with pytest.raises(IndexError, match=error_msg):
        _ = (
            default_index_snowpark_pandas_df.iloc[key]
            if axis == "row"
            else default_index_snowpark_pandas_df.iloc[:, key]
        )


@pytest.mark.parametrize(
    "key",
    [
        native_pd.Categorical((1, 3, -1)),
        (native_pd.Categorical([1, 3, 4]), [0]),
    ],
)
@sql_count_checker(query_count=0)
def test_df_iloc_get_key_raises_not_implemented_error_negative(
    key, default_index_snowpark_pandas_df
):
    # Verify that Categorical types raises NotImplementedError.
    error_msg = re.escape("pandas type category is not implemented")
    with pytest.raises(NotImplementedError, match=error_msg):
        _ = default_index_snowpark_pandas_df.iloc[key]


TEST_DATA_FOR_ILOC_GET_SLICE = [0, -10, -2, -1, None, 1, 2, 10]


@pytest.mark.parametrize("start", TEST_DATA_FOR_ILOC_GET_SLICE)
@pytest.mark.parametrize("stop", TEST_DATA_FOR_ILOC_GET_SLICE)
@pytest.mark.parametrize("step", TEST_DATA_FOR_ILOC_GET_SLICE[1:])
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
@pytest.mark.skipif(running_on_public_ci(), reason="large number of tests")
def test_df_iloc_get_slice(
    start,
    stop,
    step,
    axis,
    default_index_native_df,
    multiindex_native,
    native_df_with_multiindex_columns,
):
    def iloc_helper(df):
        return (
            df.iloc[slice(start, stop, step)]
            if axis == "row"
            else df.iloc[:, slice(start, stop, step)]
        )

    default_index_snowpark_pandas_df = pd.DataFrame(default_index_native_df)

    # test df with default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            iloc_helper,
        )

    # test df with non-default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df.set_index("D"),
            default_index_native_df.set_index("D"),
            iloc_helper,
        )

    # test df with MultiIndex
    # Index dtype is different between Snowpark and native pandas if key produces empty df.
    is_row = True if axis == "row" else False
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=not is_row,
            check_column_type=is_row,
        )

    # test df with MultiIndex on columns
    # Index class is different when the column type is MultiIndex.
    snowpark_df_with_multiindex_columns = pd.DataFrame(
        native_df_with_multiindex_columns
    )
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snowpark_df_with_multiindex_columns,
            native_df_with_multiindex_columns,
            iloc_helper,
            check_index_type=False,
            check_column_type=is_row,
        )

    # test df with MultiIndex on both index and columns
    # Index class is different when the column type is MultiIndex.
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=False,
            check_column_type=is_row,
        )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
def test_df_iloc_get_slice_with_invalid_step_negative(axis):
    snowpark_df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    with pytest.raises(ValueError, match="slice step cannot be zero"):
        _ = (
            snowpark_df.iloc[slice(None, None, 0)]
            if axis == "row"
            else snowpark_df.iloc[:, slice(None, None, 0)]
        )


@sql_count_checker(query_count=0, join_count=0)
@pytest.mark.parametrize(
    "slice_key",
    [
        slice("a", None, None),
        slice(None, "b", None),
        slice(None, None, "c"),
    ],
)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
@sql_count_checker(query_count=0)
def test_df_iloc_get_slice_with_non_integer_parameters_negative(slice_key, axis):
    snowpark_df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    with pytest.raises(
        TypeError, match="cannot do positional indexing with these indexers"
    ):
        _ = (
            snowpark_df.iloc[slice_key]
            if axis == "row"
            else snowpark_df.iloc[:, slice_key]
        )


@pytest.mark.parametrize(
    "range_key",
    [
        range(1, 4, 2),  # start < stop, step > 0
        range(1, 4, -2),  # start < stop, step < 0
        range(-1, -4, 2),  # start > stop, step > 0
        range(-1, -4, -2),  # start > stop, step < 0
        range(3, -1, 4),
        range(5, 1, -36897),
        # start = step
        range(3, -1, 4),  # step > 0
        range(100, 100, 1245),  # step > 0
        range(-100, -100, -3),  # step < 0
        range(-100, -100, -36897),  # step < 0
        range(2, 1, -2),
    ],
)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
def test_df_iloc_get_range(
    range_key,
    axis,
    default_index_native_df,
    multiindex_native,
    native_df_with_multiindex_columns,
):
    def iloc_helper(df):
        return df.iloc[range_key] if axis == "row" else df.iloc[:, range_key]

    default_index_snowpark_pandas_df = pd.DataFrame(default_index_native_df)

    # test df with default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            iloc_helper,
        )

    # test df with non-default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df.set_index("D"),
            default_index_native_df.set_index("D"),
            iloc_helper,
        )

    # test df with MultiIndex
    # Index dtype is different between Snowpark and native pandas if key produces empty df.
    # Index class is different when the column type is MultiIndex.
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    is_row = True if axis == "row" else False
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=not is_row,
            check_column_type=is_row,
        )

    # test df with MultiIndex on columns
    snowpark_df_with_multiindex_columns = pd.DataFrame(
        native_df_with_multiindex_columns
    )
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snowpark_df_with_multiindex_columns,
            native_df_with_multiindex_columns,
            iloc_helper,
            check_index_type=False,
            check_column_type=is_row,
        )

    # test df with MultiIndex on both index and columns
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            iloc_helper,
            check_index_type=False,
            check_column_type=is_row,
        )


@pytest.mark.parametrize(
    "range_key",
    [
        # pandas fails when:
        # 1. start >= num_rows and step < 0 and stop < 0
        # 2. start < -num_rows and step > 0 and stop > 0
        # num_rows = 7
        range(7, -1, -1),
        range(1000, -500, -31556),
        range(-8, 1, 1),
        range(-119085, 1805, 15792),
    ],
)
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
def test_df_iloc_get_range_deviating_behavior(range_key, axis, default_index_native_df):
    def iloc_helper(df):
        if isinstance(df, pd.DataFrame):
            return df.iloc[range_key] if axis == "row" else df.iloc[:, range_key]

        # Convert range key to slice key for comparison since pandas fails with given ranges.
        start, stop, step = range_key.start, range_key.stop, range_key.step
        if (start > stop and step > 0) or (start < stop and step < 0):
            slice_key = slice(0, 0, 1)
        else:
            slice_key = slice(start, stop, step)
        return df.iloc[slice_key] if axis == "row" else df.iloc[:, slice_key]

    default_index_snowpark_pandas_df = pd.DataFrame(default_index_native_df)

    with pytest.raises(IndexError, match="positional indexers are out-of-bounds"):
        _ = (
            default_index_native_df.iloc[range_key]
            if axis == "row"
            else default_index_native_df.iloc[:, range_key]
        )

    # test df with default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            iloc_helper,
        )

    # test df with non-default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df.set_index("D"),
            default_index_native_df.set_index("D"),
            iloc_helper,
        )


@pytest.mark.parametrize(
    "row_pos, col_pos, item_values",
    [
        # Test row and column positions in order, item single column projection
        (
            [1, 2, 3],
            [0, 2, 3],
            [91, 92, 93],
        ),
        # Test row and column positions in reverse order, item single column projection
        (
            [3, 2, 1],
            [3, 2, 0],
            [91, 92, 93],
        ),
        # Test row and column positions out-of-order, full item values
        ([1, 0, 3], [3, 1, 2], [[91, 92, 93], [94, 95, 96], [97, 98, 99]]),
        # Test row and column positions out-of-order, full item values including nulls
        ([0, 3, 2], [2, 0, 3], [[None, 92, 93], [94, None, 96], [97, 98, None]]),
        # Test row and column positions out-of-order, item single row projection
        ([1, 2, 3], [0, 2, 3], [[91, 92, 93]]),
        # Test row and column positions in-order, item multiple row single column
        ([1, 2, 3], [3, 2, 0], [[91], [92], [93]]),
        # Test row and column positions out-of-order, item single row projection all None values
        ([2, 1, 3], [1, 0, 3], [[None, None, None]]),
        # Test row and column positions in & out-of-order, item multiple row single column
        ([1, 2, 0], [3, 1, 2], [[91], [92], [93]]),
        # Test row and column positions out-of-order, item single row with None
        ([2, 1, 3], [2, 3, 0], [[91, None, 93]]),
        # Test with duplicate row positions
        ([1, 2, 3], [1, 2, 2], [91, 92, 93]),
        # Test with duplicate row and column positions
        ([1, 2, 2], [3, 1, 1], [91, 92, 93]),
        # Test with multiple duplicate row and column positions
        ([2, 2, 0, 0], [2, 1], [91, 92, 93, 94]),
        # Test with duplicate row and multiple column positions
        ([1, 2, 0, 3], [3, 3, 2, 2], [91, 92, 93, 94]),
        # Test row bool indexer and column non-bool indexer
        (
            [False, True, True, False],
            [1, 2],
            [99, 101],
        ),
        # Test row non-bool indexer and column bool indexer
        (
            [1, 2],
            [False, True, False, True],
            [99, 101],
        ),
        # Test row and column pool indexer, item single row projection
        (
            [False, True, False, False],
            [True, True, True, True],
            [[99, 101, 102, 103]],
        ),
        # Test row and column pool indexer, item single column projection
        (
            [True, True, True, True],
            [False, True, False, False],
            [99, 101, 102, 103],
        ),
    ],
)
def test_df_iloc_set_with_series_row_key_df_item_match_pandas(
    numeric_test_data_4x4,
    row_pos,
    col_pos,
    item_values,
):
    expected_query_count = 2
    expected_join_count = (
        2
        if isinstance(row_pos, list) and all(isinstance(i, bool) for i in row_pos)
        else 3
    )

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        helper_test_iloc_set_with_row_and_col_pos(
            numeric_test_data_4x4,
            row_pos,
            row_pos,
            col_pos,
            col_pos,
            item_values,
            item_values,
            wrap_item="df",
            wrap_row="series",
            wrap_col="series",
        )


@pytest.mark.parametrize("start", TEST_DATA_FOR_ILOC_GET_SLICE)
@pytest.mark.parametrize("stop", TEST_DATA_FOR_ILOC_GET_SLICE)
@pytest.mark.parametrize("step", TEST_DATA_FOR_ILOC_GET_SLICE[1:])
def test_df_iloc_set_with_row_key_slice_range(numeric_test_data_4x4, start, stop, step):
    item_values = [99, 98, 97]
    row_key = slice(start, stop, step)
    col_key = [0, 1, 2]

    with SqlCounter(query_count=1, join_count=3):
        helper_test_iloc_set_with_row_and_col_pos(
            numeric_test_data_4x4,
            row_key,
            row_key,
            col_key,
            col_key,
            item_values,
            item_values,
            wrap_item="na",
            wrap_row="na",
            wrap_col="na",
        )


@pytest.mark.parametrize(
    "row_pos, col_pos, item_values",
    [
        # Test single index row position
        (
            [3],
            [3, 2],
            91,
        ),
        # Test single negative index row position
        (
            [-2],
            [3, 2],
            98,
        ),
        # Test single index row position
        (
            [3],
            [3, 2],
            [91, 92],
        ),
        # Test single index col position
        (
            [3, 2],
            [3],
            94,
        ),
        # Test single index col position
        (
            [3, 2],
            [-2],
            95,
        ),
        # Test 2-size index row positions
        (
            [3, 2],
            [3, 2],
            [91, 92],
        ),
        # Test 3-size index row positions
        (
            [3, 2, 1],
            [3, 2, 0],
            [91, 92, 93],
        ),
        # Test 3-size index row positions
        (
            [3, -2, -1],
            [3, -2, 0],
            [91, 92, 93],
        ),
    ],
)
@pytest.mark.parametrize(
    "list_convert",
    [
        lambda l: list(l),
        lambda l: np.array(l),
        lambda l: native_pd.Index([(tuple(t) if is_list_like(t) else t) for t in l]),
    ],
)
def test_df_iloc_set_with_row_key_list(
    numeric_test_data_4x4, row_pos, col_pos, item_values, list_convert
):
    row_pos = list_convert(row_pos)
    if isinstance(row_pos, native_pd.Index):
        snow_row_pos = pd.Index(row_pos)
    else:
        snow_row_pos = row_pos

    # 1 extra query for iter
    expected_query_count = 2 if isinstance(snow_row_pos, pd.Index) else 1
    expected_join_count = 2 if isinstance(item_values, int) else 3

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        helper_test_iloc_set_with_row_and_col_pos(
            numeric_test_data_4x4,
            snow_row_pos,
            row_pos,
            col_pos,
            col_pos,
            item_values,
            item_values,
            wrap_item="na",
            wrap_row="index",
            wrap_col="index",
        )


@pytest.mark.parametrize(
    "row_pos, col_pos",
    [
        ([3, 2], [3, 2]),
        ([1, 2, 3], [1, 2, 3]),
        ([3, 2, 1], [3, 2, 0]),
        ([3, 1, 2], [1, 3, 2]),
        ([0, 2, 3], [2, 0, 3]),
        ([2, 3, 0], [3, 2, 1]),
    ],
)
@pytest.mark.parametrize(
    "item_values",
    [
        [99, 100, 101],
        123,
    ],
)
def test_df_iloc_set_with_row_tuple_key_list(
    numeric_test_data_4x4, row_pos, col_pos, item_values
):
    if isinstance(item_values, list) and len(item_values) > len(row_pos):
        item_values = item_values[: len(row_pos)]

    expected_query_count = 1
    expected_join_count = 2 if isinstance(item_values, int) else 3

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        helper_test_iloc_set_with_row_and_col_pos(
            numeric_test_data_4x4,
            row_pos,
            row_pos,
            col_pos,
            col_pos,
            item_values,
            item_values,
            wrap_item="na",
            wrap_row="tuple",
            wrap_col="tuple",
        )


@pytest.mark.parametrize(
    "row_pos, col_pos, item_values, native_item",
    [
        # Snowpark pandas will only set co-ordinate (3,3) in this example rather than both (3,2) and (3,3) to
        # keep the behavior and semantics consistent.
        (
            [3],
            [3, 2],
            91,
            [91, 16],
        ),
        # Snowpark pandas will only set co-ordinate (3,3) in this example rather than both (3,2) and (3,3) to
        # keep the behavior and semantics consistent.
        (
            [3],
            [3, 2],
            [91, 92],
            [91, 16],
        ),
    ],
)
def test_df_iloc_set_with_row_tuple_key_list_different_behavior(
    numeric_test_data_4x4, row_pos, col_pos, item_values, native_item
):
    snow_df = pd.DataFrame(numeric_test_data_4x4)
    snow_row_key = tuple(row_pos)
    snow_col_key = tuple(col_pos)
    snow_df.iloc[snow_row_key, snow_col_key] = item_values

    native_df = native_pd.DataFrame(numeric_test_data_4x4)
    native_df.iloc[row_pos, col_pos] = native_item

    expected_join_count = 3 if isinstance(item_values, list) else 2
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df,
        )


@pytest.mark.parametrize(
    "row_pos, wrap_row, col_pos, wrap_col",
    [
        ([1, 2, 3], "df", [1, 2, 3], "df"),
        ([3, 2, 1], "df", [3, 2, 1], "df"),
        ([2, 3, 0], "df", [3, 2, 1], "df"),
        ([3, 1, 2], "df", [1, 3, 2], "tuple"),
        ([1, 2, 0], "df", [1, 0, 2], "na"),
        ([0, 2, 3], "tuple", [2, 0, 3], "df"),
        ([0, 2, 1], "na", [1, 3, 0], "df"),
    ],
)
def test_df_iloc_set_with_row_df_col_df_key_set_coordinates(
    numeric_test_data_4x4,
    row_pos,
    wrap_row,
    col_pos,
    wrap_col,
):
    item_values = 123

    expected_query_count = 1 if wrap_col == "tuple" or wrap_col == "na" else 2
    expected_join_count = 2

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        helper_test_iloc_set_with_row_and_col_pos(
            numeric_test_data_4x4,
            row_pos,
            row_pos,
            col_pos,
            col_pos,
            item_values,
            item_values,
            wrap_item="na",
            wrap_row=wrap_row,
            wrap_col=wrap_col,
        )


@pytest.mark.parametrize(
    "row_pos, col_pos, snow_item, native_item",
    [
        # Test with extra item values ignored (last row)
        (
            [1, 0, 3],
            [3, 0, 2],
            [
                [91, 92, 93, 100],
                [94, 95, 96, 101],
                [97, 98, 99, 102],
                [103, 104, 105, 106],  # ignored
            ],
            [[91, 92, 93], [94, 95, 96], [97, 98, 99]],
        ),
        # Test row and col positions with item value None
        (
            [1, 2, 3],
            [0, 2, 3],
            [None],
            [[None] * 3] * 3,
        ),  # native pandas will raise index error with key and item shape mismatch
        # Test row and col positions with last item value row projected [94, 95, 96]
        (
            [1, 3, 2],
            [2, 0, 3],
            [[91, 92, 93], [94, 95, 96]],
            [[91, 92, 93], [94, 95, 96], [94, 95, 96]],
        ),
        # Test row and col positions with last item column projected [[92], [95], [98]]
        (
            [3, 1, 2],
            [1, 2, 0],
            [[91, 92], [94, 95], [97, 98]],
            [[91, 92, 92], [94, 95, 95], [97, 98, 98]],
        ),
        # Test with bool indexer all row and col positions, projecting row and column wise
        (
            [True, True, True, True],
            [True, True, True, True],
            [91, 92, 93],
            [[91, 91, 91, 91], [92, 92, 92, 92], [93, 93, 93, 93], [93, 93, 93, 93]],
        ),
        # Test with shorter row and col bool indexer and larger item values (ignore 93)
        (
            ([True, True], [True, True, False, False]),
            ([True, True], [True, True, False, False]),
            [91, 92, 93],
            [91, 92],
        ),
        # Another variation, test with shorter row and col bool indexer and larger item values (ignore 93, 94)
        (
            ([True, False, True], [True, False, True, False]),
            ([True, False, True], [True, False, True, False]),
            [91, 92, 93, 94],
            [91, 92],
        ),
        # Test with larger row and col bool indexer and shorter item values (fewer than True bool indexes)
        (
            ([False, True, False, True, True, False], [False, True, False, True]),
            ([False, True, True, True, True, False], [False, True, True, True]),
            [91, 92],
            [91, 92],
        ),
        # Test with empty items does not change the frame
        (
            [3, 2, 1],
            [3, 2, 0],
            [],
            [[17, 16, 14], [14, 13, 11], [11, 10, 8]],
        ),
    ],
)
def test_df_iloc_set_with_row_key_series_rhs_dataframe_mismatch_pandas(
    numeric_test_data_4x4, row_pos, col_pos, snow_item, native_item
):
    expected_query_count = 2
    row_pos_values = row_pos[0] if isinstance(row_pos, tuple) else row_pos
    expected_join_count = 2 if all(isinstance(i, bool) for i in row_pos_values) else 3

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        helper_test_iloc_set_with_row_and_col_pos(
            numeric_test_data_4x4,
            row_pos[0] if isinstance(row_pos, tuple) else row_pos,
            row_pos[1] if isinstance(row_pos, tuple) else row_pos,
            col_pos[0] if isinstance(col_pos, tuple) else col_pos,
            col_pos[1] if isinstance(col_pos, tuple) else col_pos,
            snow_item,
            native_item,
            wrap_item="df",
            wrap_row="series",
            wrap_col="series",
        )


@pytest.mark.parametrize(
    "row_pos, col_pos, item_value",
    [
        # Test empty column positions and scalar
        (
            [1, 2, 3],
            [],
            99,
        ),
        # Test empty row positions and scalar
        (
            [],
            [3, 2, 1],
            98,
        ),
        # Test single cell with scalar
        ([2], [1], 97),
        # Test 2x2 locations with scalar
        (
            [3, 1],
            [2, 0],
            96,
        ),
        # Test 3x3 locations with scalar
        (
            [2, 1, 3],
            [3, 0, 2],
            0,
        ),
        # Test 3x3 locations with single (list) scalar
        (
            [2, 1, 3],
            [3, 0, 2],
            [234],
        ),
        # Test 3x3 locations with single (tuple) scalar
        (
            [2, 1, 3],
            [3, 0, 2],
            (123,),
        ),
        # Test 3x3 locations with (numpy) scalar
        (
            [2, 1, 3],
            [3, 0, 2],
            np.int_(-123),
        ),
        # Test 3x3 locations with single (numpy array) scalar
        (
            [2, 1, 3],
            [3, 0, 2],
            np.array([np.int_(-123)]),
        ),
        # Test 3x3 locations with single (index) scalar
        (
            [2, 1, 3],
            [3, 0, 2],
            native_pd.Index([456]),
        ),
        # Test locations with 1d item (list) values
        (
            [3, 1, 2],
            [0, 3, 1],
            [99, 98, 97],
        ),
        # Test locations with 1d item (tuple) values
        (
            [3, 1, 2],
            [0, 3, 1],
            (99, 98, 97),
        ),
        # Test locations with 1d item (numpy array) values
        (
            [3, 1, 2],
            [0, 3, 1],
            np.array([99, 98, 97], np.int32),
        ),
        # Test locations with 1d item (index) values
        (
            [3, 1, 2],
            [0, 3, 1],
            native_pd.Index([99, 98, 97]),
        ),
        # Test 3x3 locations with 2d item (list) values
        (
            [2, 1, 0],
            [1, 0, 2],
            [[55, 56, 57], [58, 59, 60], [61, 62, 63]],
        ),
        # Test 3x3 locations with 2d item (tuple) values
        (
            [2, 1, 0],
            [1, 0, 2],
            ((55, 56, 55), (58, 59, 60), (61, 62, 63)),
        ),
        # Test 3x3 locations with 2d item (numpy array) values
        (
            [2, 1, 0],
            [1, 0, 2],
            np.array([[55, 56, 57], [58, 59, 60], [61, 62, 63]], np.int32),
        ),
        # Test 3x3 locations with 2d item (index) values
        (
            [2, 1, 0],
            [1, 0, 2],
            native_pd.Index([(55, 56, 57), (58, 59, 60), (61, 62, 63)]),
        ),
        # Test 3x3 locations with scalar 0
        (
            [2, 1, 3],
            [3, 0, 2],
            0,
        ),
        # Test boolean indexer with single list scalar
        (
            [True, False, True, False],
            [False, True, False, True],
            [-99],
        ),
    ],
)
def test_df_iloc_set_with_row_key_series_rhs_scalar(
    numeric_test_data_4x4, row_pos, col_pos, item_value
):
    expected_query_count = 2
    if isinstance(item_value, Iterable):
        if len(item_value) > 1:
            expected_join_count = 3
        elif isinstance(item_value, list) and item_value[0] < 0:
            expected_join_count = 1
        else:
            expected_join_count = 2
    else:
        expected_join_count = 2

    if isinstance(item_value, native_pd.Index):
        snow_item_value = pd.Index(item_value)
        # extra query for tolist
        expected_query_count = 3
    else:
        snow_item_value = item_value

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        helper_test_iloc_set_with_row_and_col_pos(
            numeric_test_data_4x4,
            row_pos,
            row_pos,
            col_pos,
            col_pos,
            snow_item_value,
            item_value,
            wrap_item="na",
            wrap_row="series",
            wrap_col="series",
        )


def helper_test_iloc_set_with_row_and_col_pos(
    data,
    snow_row_pos,
    native_row_pos,
    snow_col_pos,
    native_col_pos,
    snow_item,
    native_item,
    wrap_item="df",
    wrap_row="series",
    wrap_col="series",
):
    """
    Helper method to test iloc with various row, column key and item values.

    Parameters:
    -----------
    data: data passed directly into dataframe
    snow_row_pos: tuple of snow values or single row position values as a list.
    native_row_pos: tuple of native values or single row position values as a list.
    snow_col_pos: tuple of snow values or single column position values as a list.
    native_col_pos: tuple of native values or single column position values as a list.
    snow_item: Snowpark pandas values to set
    native_item: Native pandas values to set (may be same as snow_item)
    wrap_item: the rhs item should be wrapped as dataframe (if set to "df") or not.
    wrap_row: wrap the row key as a dataframe ("df"), series ("series"), tuple ("tuple") or leave same.
    wrap_col: wrap the column key as a dataframe ("df"), series ("series"), tuple ("tuple") or leave same.
    """
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    snow_row_key, native_row_key = wrap_key_as_expected_type(
        wrap_row, snow_row_pos, native_row_pos
    )
    snow_col_key, native_col_key = wrap_key_as_expected_type(
        wrap_col, snow_col_pos, native_col_pos
    )

    def perform_iloc(df):
        if isinstance(df, pd.DataFrame):
            df.iloc[snow_row_key, snow_col_key] = (
                snow_item
                if is_scalar(snow_item) or not wrap_item == "df"
                else pd.DataFrame(snow_item)
            )
        else:
            df.iloc[native_row_key, native_col_key] = (
                native_item
                if is_scalar(native_item) or not wrap_item == "df"
                else native_pd.DataFrame(native_item)
            )

    eval_snowpark_pandas_result(snow_df, native_df, perform_iloc, inplace=True)


def wrap_key_as_expected_type(wrap_type, snow_pos, native_pos):
    if wrap_type == "series":
        snow_key = pd.Series(snow_pos)
        native_key = native_pd.Series(native_pos)
    elif wrap_type == "df":
        snow_key = pd.DataFrame(snow_pos)
        native_key = native_pd.DataFrame(native_pos)
    elif wrap_type == "tuple":
        snow_key = tuple(snow_pos)
        native_key = tuple(native_pos)
    else:
        snow_key = snow_pos
        native_key = native_pos

    if is_list_like(snow_pos) and len(snow_pos) == 0:
        # An empty list defaults to float which then fails, so we convert to int in that case.
        snow_key = snow_key.astype("int")

    return snow_key, native_key


@pytest.mark.parametrize(
    "row_key, col_key",
    [
        ([], []),
        ([1, 2], []),
        ([], [2, 1]),
    ],
)
@pytest.mark.parametrize(
    "item",
    [
        [100],
        [],
    ],
)
def test_df_iloc_set_with_row_key_series_empty_keys(
    numeric_test_data_4x4, row_key, col_key, item
):
    native_df = native_pd.DataFrame(numeric_test_data_4x4)

    snow_df_1 = pd.DataFrame(numeric_test_data_4x4)
    item_df_1 = pd.DataFrame(item)
    with SqlCounter(query_count=2, join_count=3):
        snow_df_1.iloc[
            pd.Series(row_key).astype("int"), pd.Series(col_key).astype("int")
        ] = item_df_1
        eval_snowpark_pandas_result(snow_df_1, native_df, lambda df: df)

    snow_df_2 = pd.DataFrame(numeric_test_data_4x4)
    item_df_2 = pd.Series(item)
    with SqlCounter(query_count=2, join_count=3):
        snow_df_2.iloc[
            pd.Series(row_key).astype("int"), pd.Series(col_key).astype("int")
        ] = item_df_2
        eval_snowpark_pandas_result(snow_df_2, native_df, lambda df: df)


@pytest.mark.parametrize(
    "row_key, col_key, item",
    [
        ([], [], []),
        ([1, 2], [], []),
        ([], [1, 2], []),
        ([0], [0], [100]),
        ([1], [1], [99]),
        ([2, 1], [0, 1], [99, 100]),
        ([1, 2], [1, 0], [[99, 100], [98, 101]]),
    ],
)
@sql_count_checker(query_count=1, join_count=0)
def test_df_iloc_set_with_empty_df(row_key, col_key, item):
    snow_df = pd.DataFrame([])

    snow_row_key = pd.Series(row_key).astype("int")
    snow_col_key = pd.Series(col_key).astype("int")

    # Note that pandas fails for most of these cases since the row and col key are out of bounds
    # when the shape is (0,0) but we succeed as a no-op operation since we don't do any upfront
    # validation.
    snow_df.iloc[snow_row_key, snow_col_key] = pd.DataFrame(item)

    assert snow_df.empty


@pytest.mark.parametrize(
    "row_key, col_key, item",
    [
        ([1, 0], [0, 1], [[11, 22], [33, 44]]),
        ([0, 1], [1], [55]),
        ([0], [0], [100]),
        ([1], [1], [99]),
    ],
)
def test_df_iloc_set_with_none_df(row_key, col_key, item):
    data = [[None, None], [None, None]]

    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    snow_row_key = pd.Series(row_key)
    native_row_key = native_pd.Series(row_key)

    snow_col_key = pd.Series(col_key)
    native_col_key = native_pd.Series(col_key)

    def perform_iloc(df):
        if isinstance(df, pd.DataFrame):
            df.iloc[snow_row_key, snow_col_key] = pd.DataFrame(item)
        else:
            df.iloc[native_row_key, native_col_key] = native_pd.DataFrame(item)

    with SqlCounter(query_count=2, join_count=3):
        eval_snowpark_pandas_result(snow_df, native_df, perform_iloc, inplace=True)


@pytest.mark.parametrize(
    "row_key, col_key, item",
    [
        ([0], [0], [99]),
        ([1], [1], [True]),
        ([2], [2], [101.101]),
        ([3], [3], ["a"]),
        ([0, 1, 2, 3], [0], [96, 97, 98, 99]),
        ([0, 1, 2, 3], [1], [True, True, False, False]),
        ([0, 1, 2, 3], [1], [999, True, 101, False]),
        ([0, 1, 2, 3], [2], [12.34, 34.56, 56.78, 78.9]),
        ([0, 1, 2, 3], [3], ["a", "b", "c", "d"]),
        ([0, 1, 2, 3], [0, 1, 2, 3], [[99, True, 101.101, "a"]]),
        ([1, 2], [1, 2], [[True, 9.9], [False, 10.9]]),
    ],
)
def test_df_iloc_set_with_types_df(row_key, col_key, item):
    mixed_data = {
        "A": [1, 2, 3, 4],
        "B": [True, False, True, False],
        "C": [1.5, 3.14159, 99.99, 123.45],
        "D": ["x", "y", "z", "w"],
    }

    snow_df = pd.DataFrame(mixed_data)
    native_df = native_pd.DataFrame(mixed_data)

    snow_row_key = pd.Series(row_key)
    native_row_key = native_pd.Series(row_key)

    snow_col_key = pd.Series(col_key)
    native_col_key = native_pd.Series(col_key)

    def perform_iloc(df):
        if isinstance(df, pd.DataFrame):
            df.iloc[snow_row_key, snow_col_key] = pd.DataFrame(item)
        else:
            df.iloc[native_row_key, native_col_key] = native_pd.DataFrame(item)

    with SqlCounter(query_count=2, join_count=3):
        eval_snowpark_pandas_result(snow_df, native_df, perform_iloc, inplace=True)


@pytest.mark.parametrize(
    "row_key, col_key",
    [
        ([-5, -8], [-4, -1]),
        ([4, 6], [-2, 8]),
        ([10, 11], [6, -3]),
        ([-10, 5], [9, 7]),
        ([False, False, False, False, False], [False, False, False, False, False]),
    ],
)
def test_df_iloc_set_with_row_key_series_out_of_bounds_keys(
    numeric_test_data_4x4, row_key, col_key
):
    native_df = native_pd.DataFrame(numeric_test_data_4x4)

    expected_join_count = 2 if all(isinstance(i, bool) for i in row_key) else 3

    snow_df_1 = pd.DataFrame(numeric_test_data_4x4)
    item_df_1 = pd.DataFrame([100])
    with SqlCounter(query_count=2, join_count=expected_join_count):
        snow_df_1.iloc[pd.Series(row_key), pd.Series(col_key)] = item_df_1
        eval_snowpark_pandas_result(snow_df_1, native_df, lambda df: df)

    snow_df_2 = pd.DataFrame(numeric_test_data_4x4)
    item_df_2 = pd.Series([100])
    with SqlCounter(query_count=2, join_count=expected_join_count):
        snow_df_2.iloc[pd.Series(row_key), pd.Series(col_key)] = item_df_2
        eval_snowpark_pandas_result(snow_df_2, native_df, lambda df: df)


@pytest.mark.parametrize(
    "row_pos, col_pos",
    [
        ([1.1, 2], [1, 2]),
        ([1, 2], [0.4, 2.7]),
        ([1.3, 2.8], [2.3, 1.1]),
        ([-1.1, 2], [1, -2]),
        ([-1, -2], [-0.4, -2.7]),
        ([-1.3, -2.8], [-2.3, -1.1]),
    ],
)
@sql_count_checker(query_count=0)
def test_df_iloc_set_with_row_key_series_float_keys(
    numeric_test_data_4x4, row_pos, col_pos
):
    snow_df = pd.DataFrame(numeric_test_data_4x4)
    snow_row_key = pd.Series(row_pos)
    snow_col_key = pd.Series(col_pos)
    item_df = pd.DataFrame([99, 101])

    with pytest.raises(
        IndexError,
        match=r"arrays used as indices must be of integer \(or boolean\) type",
    ):
        snow_df.iloc[snow_row_key, snow_col_key] = item_df


@pytest.mark.parametrize(
    "row_pos, wrap_row, col_pos, wrap_col",
    [
        (1.2, "na", 0, "na"),
        (0, "na", 2.5, "na"),
        (np.nan, "na", 2, "na"),
        (0, "na", np.nan, "na"),
        ([1.1, 2], "na", [1, 2], "na"),
        ([2, 1.1], "tuple", [0, 1], "na"),
        ([1.1, 2], "series", [1, 0], "na"),
        ([1, 2], "na", [1, 2.3], "na"),
        ([2, 1], "na", [0, 2.5], "tuple"),
        ([1, 0], "na", [2, 2.9], "series"),
        (np.array([1.3, 2.8], dtype=float), "na", [2, 1], "na"),
        ([2, 1], "na", np.array([1.3, 2.8], dtype=float), "na"),
        (["x", 2], "na", [1, 2], "na"),
    ],
)
@sql_count_checker(query_count=0)
def test_df_iloc_set_with_row_key_constant_float_keys_negative(
    numeric_test_data_4x4,
    row_pos,
    wrap_row,
    col_pos,
    wrap_col,
):
    snow_df = pd.DataFrame(numeric_test_data_4x4)
    native_df = native_pd.DataFrame(numeric_test_data_4x4)

    snow_row_key, native_row_key = wrap_key_as_expected_type(wrap_row, row_pos, row_pos)
    snow_col_key, native_col_key = wrap_key_as_expected_type(wrap_row, col_pos, col_pos)

    item_values = [99, 101]
    if is_scalar(row_pos) or is_scalar(col_pos):
        item_values = item_values[0]

    def perform_iloc(df):
        if isinstance(df, pd.DataFrame):
            df.iloc[snow_row_key, snow_col_key] = item_values
        else:
            df.iloc[native_row_key, native_col_key] = item_values
        return df

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        perform_iloc,
        inplace=True,
        expect_exception=True,
        expect_exception_type=IndexError,
        expect_exception_match=re.escape(
            "arrays used as indices must be of integer (or boolean) type"
        ),
        # Snowpark pandas and pandas both raise an exception, but message is sometimes different for Snowpark pandas.
        assert_exception_equal=False,
    )


@pytest.mark.parametrize(
    "row_key, col_key, item, expected_results, expect_exception",
    [
        ([0], [0], [True], {"A": [True, True, True, True]}, False),
        ([1], [1], [0], None, False),
        ([1], [1], [99], {"B": [True, True, True, False]}, False),
        ([2], [2], ["a"], None, True),
        ([3], [3], [101.101], None, True),
        (
            [0, 1, 2, 3],
            [0],
            [123.4, "x", 99],
            {"A": [123.4, "x", 99, 99]},
            False,
        ),
        ([0, 1, 2, 3], [1], [999, True, 101, False], None, False),
        (
            [0, 1, 2, 3],
            [2],
            [56.78, 78],
            {"C": [56.78, 78.0, 78.0, 78.0]},
            False,
        ),
        (
            [0, 1, 2, 3],
            [3],
            ["a", "b", False, "d"],
            {"D": ["a", "b", "false", "d"]},
            False,
        ),
        ([0, 1, 2, 3], [0, 1, 2, 3], [[99, True, "x", "a"]], None, True),
        (
            [1, 2],
            [1, 2],
            [[True], [False]],
            {"B": [True, True, False, False], "C": [True, True, False, True]},
            False,
        ),
    ],
)
def test_df_iloc_set_with_mixed_types_fail(
    row_key, col_key, item, expected_results, expect_exception
):
    # Some of these tests fail because of snowflake type system differences compared to pandas.  In some cases
    # they succeed but yield different results from pandas due to type system differences.
    mixed_data = {
        "A": [1, 2, 3, 4],
        "B": [True, False, True, False],
        "C": [1.5, 3.14159, 99.99, 123.45],
        "D": ["x", "y", "z", "w"],
    }

    snow_df = pd.DataFrame(mixed_data)
    snow_row_key = pd.Series(row_key)
    snow_col_key = pd.Series(col_key)

    if expect_exception:
        with SqlCounter(query_count=1, join_count=0):
            with pytest.raises(SnowparkSQLException):
                snow_df.iloc[snow_row_key, snow_col_key] = pd.DataFrame(item)
                snow_df.to_pandas()
    else:
        if expected_results is not None:
            native_mixed_data = mixed_data.copy()
            native_mixed_data.update(expected_results)
            native_df = native_pd.DataFrame(native_mixed_data)
        else:
            native_df = native_pd.DataFrame(mixed_data)
            native_row_key = native_pd.Series(row_key)
            native_col_key = native_pd.Series(col_key)
            native_df.iloc[native_row_key, native_col_key] = native_pd.DataFrame(item)

        def perform_iloc(df):
            if isinstance(df, pd.DataFrame):
                df.iloc[snow_row_key, snow_col_key] = pd.DataFrame(item)

        with SqlCounter(query_count=2, join_count=3):
            eval_snowpark_pandas_result(snow_df, native_df, perform_iloc, inplace=True)


@pytest.mark.parametrize(
    "row_key, row_key_index",
    [
        [1, None],
        [[3, 0], None],
        [[1, 2], [("A",), ("B",)]],
        [[2, 1], [("A", 1), ("B", 2)]],
    ],
)
@pytest.mark.parametrize(
    "col_key, col_key_index",
    [
        [2, None],
        [[2, 1], None],
        [[1, 2], [("X",), ("Y",)]],
        [[2, 1], [("X", 11), ("Y", 21)]],
    ],
)
@pytest.mark.parametrize(
    "item_values, item_index, item_columns, expected_join_count",
    [
        [999, None, None, 2],
        [TEST_ITEMS_DATA_2X2, None, None, 3],
        [TEST_ITEMS_DATA_2X2, [("r", 20), ("s", 25)], None, 5],
        [TEST_ITEMS_DATA_2X2, [("r", 20), ("s", 25)], [("e", 5), ("f", 6)], 5],
        [TEST_ITEMS_DATA_2X2, None, [("e", 5), ("f", 6)], 3],
    ],
)
def test_df_iloc_set_with_multiindex(
    row_key,
    row_key_index,
    col_key,
    col_key_index,
    item_values,
    item_index,
    item_columns,
    expected_join_count,
):
    df_data = [
        [1, 2, 3, 4, 5],
        [10, 11, 12, 13, 14],
        [99, 101, 98, 102, 97],
        [55, 56, 57, 58, 59],
        [-5, -6, -7, -8, -9],
    ]

    col_index = pd.MultiIndex.from_tuples(
        [("a", 1), ("b", 2), ("a", 2), ("b", 2), ("c", 3)]
    )

    row_index = pd.MultiIndex.from_tuples(
        [("x", 99), ("y", 11), ("x", 11), ("y", 99), ("z", -12)]
    )

    snow_df = pd.DataFrame(df_data, index=row_index, columns=col_index)
    native_df = native_pd.DataFrame(df_data, index=row_index, columns=col_index)

    # If one of the row_key or col_key is not a list, then we'll shorten the item_values to expected shape/length
    # for the iloc set operation.
    if not (is_list_like(row_key) and is_list_like(col_key)):
        max_key_len = 1
        if is_list_like(row_key):
            max_key_len = len(row_key)
        if is_list_like(col_key):
            max_key_len = len(col_key)
        if isinstance(item_values, list):
            item_values = item_values[0:max_key_len]
        elif isinstance(item_values, dict):
            item_values = list(item_values.values())[0][:max_key_len]
        if item_index:
            item_index = item_index[:max_key_len]
        if item_columns:
            item_columns = item_columns[:max_key_len]

    if isinstance(item_values, list):
        snow_items = pd.Series(item_values)
        native_items = native_pd.Series(item_values)
    elif isinstance(item_values, dict):
        snow_items = pd.DataFrame(item_values)
        native_items = native_pd.DataFrame(item_values)
    else:
        snow_items = item_values
        native_items = item_values

    if item_index:
        snow_items.index = pd.MultiIndex.from_tuples(item_index)
        native_items.index = pd.MultiIndex.from_tuples(item_index)
    if item_columns:
        snow_items.columns = pd.MultiIndex.from_tuples(item_columns)
        native_items.columns = pd.MultiIndex.from_tuples(item_columns)

    if row_key_index:
        # Using native pandas index since row_key[2] is a MultiIndex object.
        snow_row_key = pd.Series(row_key, index=native_pd.Index(row_key_index))
        native_row_key = native_pd.Series(row_key, index=native_pd.Index(row_key_index))
    else:
        snow_row_key = row_key
        native_row_key = row_key

    if col_key_index:
        # Using native pandas index since col_key[2] is a MultiIndex object.
        snow_col_key = pd.Series(col_key, index=native_pd.Index(col_key_index))
        native_col_key = native_pd.Series(col_key, index=native_pd.Index(col_key_index))
    else:
        snow_col_key = col_key
        native_col_key = col_key

    def helper_iloc(df):
        if isinstance(df, native_pd.DataFrame):
            df.iloc[native_row_key, native_col_key] = native_items
        else:
            df.iloc[snow_row_key, snow_col_key] = snow_items

    expected_query_count = 1
    if isinstance(snow_col_key, pd.Series):
        expected_query_count += 1

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(snow_df, native_df, helper_iloc, inplace=True)


@pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
@pytest.mark.parametrize("axis", ILOC_GET_KEY_AXIS)
def test_df_iloc_get_series_with_multiindex(
    axis,
    default_index_native_df,
    default_index_snowpark_pandas_df,
    multiindex_native,
    native_df_with_multiindex_columns,
):
    def run_multiindex_test(
        _ser: pd.Series, _native_ser: native_pd.Series, query_count, join_count
    ) -> None:
        def iloc_helper(df: Union[pd.DataFrame, native_pd.DataFrame]) -> None:
            series = _ser if isinstance(df, pd.DataFrame) else _native_ser
            return df.iloc[series] if axis == "row" else df.iloc[:, series]

        # test df with default index
        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                default_index_snowpark_pandas_df,
                default_index_native_df,
                iloc_helper,
            )
        # test df with non-default index
        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                default_index_snowpark_pandas_df.set_index("D"),
                default_index_native_df.set_index("D"),
                iloc_helper,
            )

        # test df with MultiIndex
        # Index dtype is different between Snowpark and native pandas if key produces empty df.
        native_df = default_index_native_df.set_index(multiindex_native)
        snowpark_df = pd.DataFrame(native_df)
        is_row = True if axis == "row" else False
        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                snowpark_df,
                native_df,
                iloc_helper,
                check_index_type=not is_row,
                check_column_type=is_row,
            )

        # test df with MultiIndex on columns
        snowpark_df_with_multiindex_columns = pd.DataFrame(
            native_df_with_multiindex_columns
        )
        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                snowpark_df_with_multiindex_columns,
                native_df_with_multiindex_columns,
                iloc_helper,
                check_index_type=False,
                check_column_type=is_row,
            )

        # test df with MultiIndex on both index and columns
        native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
        snowpark_df = pd.DataFrame(native_df)
        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                snowpark_df,
                native_df,
                iloc_helper,
                check_index_type=not is_row,
                check_column_type=is_row,
            )

    # For a Series row key, the key is joined with the df to derive the iloc results. For column keys, a select
    # statement is used instead of a join.
    join_count = 2 if axis == "row" else 0
    query_count = 1 if axis == "row" else 2

    # Evaluate with MultiIndex created from tuples.
    arrays = [
        ["bar", "bar", "baz", "baz"],
        ["one", "two", "one", "two"],
    ]
    tuples = list(zip(*arrays))
    index = native_pd.MultiIndex.from_tuples(tuples, names=["first", "second"])
    native_ser = native_pd.Series([2, 3, 4, 5], index=index)
    ser = pd.Series([2, 3, 4, 5], index=index)
    run_multiindex_test(ser, native_ser, query_count, join_count)

    # Evaluate with MultiIndex created from product.
    iterables = [["bar", "baz", "foo"], [22, 23]]
    index = native_pd.MultiIndex.from_product(iterables, names=[2, "second"])
    ser = pd.Series([0, 1, 2, 3, 4, 5], index=index)
    native_ser = native_pd.Series([0, 1, 2, 3, 4, 5], index=index)
    run_multiindex_test(ser, native_ser, query_count, join_count)

    # Evaluate with MultiIndex created from a DataFrame.
    dataframe = native_pd.DataFrame(
        [["bar", "one"]],
        columns=["first", "second"],
    )
    index = native_pd.MultiIndex.from_frame(dataframe)
    ser = pd.Series([4], index=index)
    native_ser = native_pd.Series([4], index=index)
    run_multiindex_test(ser, native_ser, query_count, join_count)

    # Evaluate with MultiIndex created from an empty DataFrame.
    dataframe = native_pd.DataFrame([], columns=["first", "second"])
    index = native_pd.MultiIndex.from_frame(dataframe)
    ser = pd.Series([], index=index, dtype=int)
    native_ser = native_pd.Series([], index=index, dtype=int)
    query_count = query_count if axis == "row" else 2
    run_multiindex_test(ser, native_ser, query_count, join_count)


@sql_count_checker(query_count=0, join_count=0)
def test_df_iloc_get_multiindex_key_negative(
    default_index_snowpark_pandas_df, multiindex_native
):
    err_msg = "key of type MultiIndex cannot be used with iloc"
    with pytest.raises(TypeError, match=err_msg):
        _ = default_index_snowpark_pandas_df.iloc[multiindex_native]
    with pytest.raises(TypeError, match=err_msg):
        _ = default_index_snowpark_pandas_df.iloc[:, multiindex_native]


TEST_DATA_FOR_ILOC_GET_COMBINATIONS = [
    slice(6, 1, -2),
    range(2, 6, 3),
    [],
    # numeric list-like
    [random.choice(range(-7, 7)) for _ in range(7)],
    np.array([random.choice(range(-7, 7)) for _ in range(7)]),
    native_pd.Index([random.choice(range(-7, 7)) for _ in range(7)], name="some name"),
    native_pd.Series([random.choice(range(-7, 7)) for _ in range(7)]),
    # boolean list-like
    [random.choice([True, False]) for _ in range(7)],
    np.array([random.choice([True, False]) for _ in range(7)]),
    native_pd.Index([random.choice([True, False]) for _ in range(7)]),
    native_pd.Series(
        [random.choice([True, False]) for _ in range(7)], name="different name"
    ),
]


@pytest.mark.parametrize("row", TEST_DATA_FOR_ILOC_GET_COMBINATIONS)
@pytest.mark.parametrize("col", TEST_DATA_FOR_ILOC_GET_COMBINATIONS)
@pytest.mark.parametrize("is_tuple", [True, False])
def test_df_iloc_get_numeric_combinations_of_row_and_col(
    row, col, is_tuple, default_index_snowpark_pandas_df, default_index_native_df
):
    def iloc_helper(df):
        # Convert row and column keys into the appropriate format for testing and call .iloc get.
        _row, _col = row, col
        if isinstance(df, pd.DataFrame):
            if isinstance(row, native_pd.Series):
                _row = pd.Series(row, dtype=float if len(row) == 0 else None)
            if isinstance(col, native_pd.Series):
                _col = pd.Series(col, dtype=float if len(col) == 0 else None)
        else:
            # Convert boolean list-like data into corresponding numeric data since pandas does not support iloc get
            # with boolean type across rows and columns.
            if is_list_like(row) and len(row) > 0 and is_bool(row[0]):
                _row = [idx for idx, element in enumerate(row) if element]
            if is_list_like(col) and len(col) > 0 and is_bool(col[0]):
                _col = [idx for idx, element in enumerate(col) if element]
        return df.iloc[(_row, _col)] if is_tuple else df.iloc[_row, _col]

    def determine_query_and_join_count():
        # Initialize count values; query_count = row_count + col_count.
        query_count = 1  # base query count
        # All list-like row keys are treated like Series keys; a join is performed between the df and
        # key. For slice and range keys, a filter is used on the df instead.
        join_count = 2
        if is_scalar(row):
            join_count = 0
        elif not isinstance(row, list) or len(row) > 0:
            if is_range_like(row) or isinstance(row, slice):
                join_count = 0
            elif all(isinstance(i, bool) or isinstance(i, np.bool_) for i in row):
                join_count = 1

        query_count += 1 if isinstance(col, native_pd.Series) else 0
        return query_count, join_count

    qc, jc = determine_query_and_join_count()
    # Test different combinations of rows and columns.
    with SqlCounter(query_count=qc, join_count=jc):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df, default_index_native_df, iloc_helper
        )


@pytest.mark.parametrize(
    "row,col",
    [
        (-4, native_pd.Series([False, False, False, False, False, False, True])),
        (0, native_pd.Series([False, False, False, False, False, False, True])),
        (1, native_pd.Series([False, False, False, False, False, True, True])),
    ],
)
@sql_count_checker(query_count=2, union_count=1)
def test_df_iloc_get_array_col(
    row,
    col,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    # This test ensures that when iloc would return a 1xN frame, where the only columns are
    # internally represented by the Snowpark ARRAY type, no errors occur.
    # Previously, a call to df.squeeze() led to a call of df.transpose(), which would fail
    # when operating on a df with an ARRAY column.
    # This test is separated from get_numeric_combinations_of_row_and_col to avoid adding
    # too many permutations.
    def iloc_helper(df):
        # Convert row and column keys into the appropriate format for testing and call .iloc get.
        _row, _col = row, col
        if isinstance(df, pd.DataFrame):
            if isinstance(row, native_pd.Series):
                _row = pd.Series(row)
            if isinstance(col, native_pd.Series):
                _col = pd.Series(col)
        else:
            # Convert boolean list-like data into corresponding numeric data since pandas does not support iloc get
            # with boolean type across rows and columns.
            if is_list_like(row) and len(row) > 0 and is_bool(row[0]):
                _row = [idx for idx, element in enumerate(row) if element]
            if is_list_like(col) and len(col) > 0 and is_bool(col[0]):
                _col = [idx for idx, element in enumerate(col) if element]
        return df.iloc[_row, _col]

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df, default_index_native_df, iloc_helper
    )


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.iloc[lambda x: [1, 3]],
        lambda df: df.iloc[lambda x: [1, 3], :],
        lambda df: df.iloc[lambda x: [1, 3], lambda x: 0],
        lambda df: df.iloc[lambda x: [1, 3], lambda x: [0]],
        lambda df: df.iloc[[1, 3], lambda x: 0],
        lambda df: df.iloc[[1, 3], lambda x: [0]],
        lambda df: df.iloc[lambda x: [1, 3], 0],
        lambda df: df.iloc[lambda x: [1, 3], [0]],
    ],
)
def test_df_iloc_get_callable2(
    default_index_snowpark_pandas_df,
    default_index_native_df,
    multiindex_native,
    native_df_with_multiindex_columns,
    func,
):
    def run_test(snowpark_df, native_df):
        with SqlCounter(query_count=1, join_count=2):
            eval_snowpark_pandas_result(
                snowpark_df, native_df, func, check_index_type=False
            )

    # test df with default index
    run_test(default_index_snowpark_pandas_df, default_index_native_df)

    # test df with non-default index
    run_test(
        default_index_snowpark_pandas_df.set_index("D"),
        default_index_native_df.set_index("D"),
    )

    # test df with MultiIndex
    # Index dtype is different between Snowpark and native pandas if key produces empty df.
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    run_test(snowpark_df, native_df)

    # test df with MultiIndex on columns
    snowpark_df_with_multiindex_columns = pd.DataFrame(
        native_df_with_multiindex_columns
    )
    run_test(snowpark_df_with_multiindex_columns, native_df_with_multiindex_columns)

    # test df with MultiIndex on both index and columns
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    run_test(snowpark_df, native_df)


# TODO: SNOW-962607 - unskip tests below when bug is fixed.
@pytest.mark.skip("BUG: SNOW-962607 - test below fails due to transpose on array")
def test_df_iloc_get_scalar_row_boolean_col(
    default_index_snowpark_pandas_df, default_index_native_df
):
    # df.iloc[-4, [False, False, False, False, False, False, True]] fails due to transpose on an array.
    # Skip the test if row == -4 and col == [False, False, False, False, False, False, True].
    row = -4
    col = [False, False, False, False, False, False, True]
    # test df with default index
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: df.iloc[row, col],
        )


@pytest.mark.skip(
    "BUG: SNOW-962607 - test below can fail due to transpose on array -- flaky test"
)
@pytest.mark.parametrize("key1", [-4])
@pytest.mark.parametrize("key2", TEST_DATA_FOR_ILOC_GET_COMBINATIONS)
@pytest.mark.parametrize("is_tuple", [True, False])
def test_df_iloc_get_scalar_and_any_key(
    key1, key2, is_tuple, default_index_snowpark_pandas_df, default_index_native_df
):
    def iloc_helper(df):
        # Convert row and column keys into the appropriate format for testing and call .iloc get.
        _row, _col = row, col
        if isinstance(df, pd.DataFrame):
            if isinstance(row, native_pd.Series):
                _row = pd.Series(row)
            elif isinstance(col, native_pd.Series):
                _col = pd.Series(col)
        else:
            # Convert boolean list-like data into corresponding numeric data since pandas does not support iloc get
            # with boolean type across rows and columns.
            if is_list_like(row) and len(row) > 0 and is_bool(row[0]):
                _row = [idx for idx, element in enumerate(row) if element]
            elif is_list_like(col) and len(col) > 0 and is_bool(col[0]):
                _col = [idx for idx, element in enumerate(col) if element]
        return df.iloc[(_row, _col)] if is_tuple else df.iloc[_row, _col]

    def determine_query_and_join_count():
        # Initialize count values; query_count = row_count + col_count.
        if is_scalar(row) and isinstance(col, list) and not col:  # df.iloc[-4. []]
            # out of the 11 queries, 9 are from squeeze(), 5 of which generated by creation of SnowflakeQueryCompiler
            # during transpose operation. The remaining 2 queries are generated during comparison of results.
            # 1 join comes from squeeze, 4 joins from transpose in squeeze, and the remaining from results assertion.
            return 11, 7
        if is_scalar(row):
            query_count, join_count = 3, 3
        else:
            query_count = 1  # base counts
            # All scalar and list-like row keys are treated like Series keys; a join is performed between the df and
            # key. For slice and range keys, a filter is used on the df instead.
            join_count = 0 if (is_range_like(row) or isinstance(row, slice)) else 1
        query_count += 1 if isinstance(col, native_pd.Series) else 0
        return query_count, join_count

    row, col = key1, key2
    qc, jc = determine_query_and_join_count()
    with SqlCounter(query_count=qc, join_count=jc):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            iloc_helper,
        )

    row, col = key2, key1
    qc, jc = determine_query_and_join_count()
    with SqlCounter(query_count=qc, join_count=jc):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            iloc_helper,
        )


@pytest.mark.parametrize("row", ILOC_GET_INT_SCALAR_KEYS)
@pytest.mark.parametrize("col", ILOC_GET_INT_SCALAR_KEYS)
@pytest.mark.parametrize("is_tuple", [True, False])
def test_df_iloc_get_scalar_row_and_scalar_col(
    row,
    col,
    is_tuple,
    default_index_snowpark_pandas_df,
    default_index_native_df,
    multiindex_native,
    native_df_with_multiindex_columns,
):
    def run_test(snowpark_df, native_df):
        col_lower_bound, col_upper_bound = -num_cols - 1, num_cols

        def iloc_helper(df):
            return df.iloc[(row, col)] if is_tuple else df.iloc[row, col]

        row_in_range = True if -8 < row < 7 else False
        col_in_range = True if col_lower_bound < col < col_upper_bound else False
        if row_in_range and col_in_range:
            # scalar value is returned
            with SqlCounter(query_count=1):
                snowpark_res = (
                    snowpark_df.iloc[(row, col)]
                    if is_tuple
                    else snowpark_df.iloc[row, col]
                )
                native_res = native_df.iloc[row, col]
                if is_scalar(snowpark_res):
                    assert snowpark_res == native_res
                else:
                    for idx, val in enumerate(snowpark_res):
                        assert val == native_res[idx]
        else:
            with SqlCounter(query_count=1):
                with pytest.raises(IndexError):
                    iloc_helper(native_df)
                assert len(iloc_helper(snowpark_df)) == 0

    # test df with default index
    num_cols = 7
    run_test(default_index_snowpark_pandas_df, default_index_native_df)

    # test df with non-default index
    # When iloc is used on columns with non-default index: setting a column to index reduces the total number of
    # columns by 1, therefore the valid range for indices in native pandas needs to be trimmed by 1.
    num_cols = 6
    run_test(
        default_index_snowpark_pandas_df.set_index("D"),
        default_index_native_df.set_index("D"),
    )

    # test df with MultiIndex
    # Index dtype is different between Snowpark and native pandas if key produces empty df.
    num_cols = 7
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    run_test(snowpark_df, native_df)

    # test df with MultiIndex on columns
    snowpark_df_with_multiindex_columns = pd.DataFrame(
        native_df_with_multiindex_columns
    )
    run_test(snowpark_df_with_multiindex_columns, native_df_with_multiindex_columns)

    # test df with MultiIndex on both index and columns
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    run_test(snowpark_df, native_df)


@sql_count_checker(query_count=1, join_count=3)
def test_df_iloc_set_ffill_na_values_negative():
    native_df = native_pd.DataFrame({"a": [1, 2, 3], "b": [3, 4, 5]})
    snow_df = pd.DataFrame(native_df)
    values = [[1, 2], [None, 4]]
    ffilled_values = [[1, 2], [None, 4], [1, 4]]

    def iloc_helper(df):
        if isinstance(df, native_pd.DataFrame):
            # Ideally, we would want to propagate the NA value in
            # column 0, row 1, but our ffill algorithm does not support
            # that. In that case, we could write this:
            # df.iloc[[0, 1, 2]] = [values[0], values[1], values[1]]
            # Instead, we ffill the last *non-NA* value, so we use:
            df.iloc[[0, 1, 2]] = ffilled_values
        else:
            df.iloc[[0, 1, 2]] = values

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        iloc_helper,
        inplace=True,
    )


@sql_count_checker(query_count=0)
def test_raise_set_cell_with_list_like_value_error():
    s = pd.Series([[1, 2], [3, 4]])
    with pytest.raises(NotImplementedError):
        s.iloc[0] = [0, 0]
    with pytest.raises(NotImplementedError):
        s.to_frame().iloc[0, 0] = [0, 0]


@sql_count_checker(query_count=1, join_count=3)
@pytest.mark.parametrize("index", [list("ABC"), [0, 1, 2]])
def test_df_iloc_set_row_from_series(index):
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=list("ABC"))
    snow_df = pd.DataFrame(native_df)

    def ilocset(df):
        series = (
            pd.Series([1, 4, 9], index=index)
            if isinstance(df, pd.DataFrame)
            else native_pd.Series([1, 4, 9], index=index)
        )
        df.iloc[1] = series
        return df

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        ilocset,
    )


@sql_count_checker(query_count=1, join_count=3)
@pytest.mark.parametrize("index", [[3, 4, 5], [0, 1, 2], list("ABC")])
@pytest.mark.parametrize("columns", [None, list("ABC")])
def test_df_iloc_full_set_row_from_series(columns, index):
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=columns)
    snow_df = pd.DataFrame(native_df)

    def ilocset(df):
        series = (
            pd.Series([1, 4, 9], index=index)
            if isinstance(df, pd.DataFrame)
            else native_pd.Series([1, 4, 9], index=index)
        )
        df.iloc[:] = series
        return df

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        ilocset,
    )


@pytest.mark.parametrize(
    "ops",
    [
        lambda df: df.head(),
        lambda df: df.iloc[1:100],
        lambda df: df.iloc[1000:100:-1],
    ],
)
@sql_count_checker(query_count=4)
def test_df_iloc_efficient_sql(session, ops):
    df = DataFrame({"a": [1] * 10000})
    with session.query_history() as query_listener:
        ops(df).to_pandas()
    eval_query = query_listener.queries[
        -2
    ].sql_text.lower()  # query before drop temp table
    # check no row count is in the sql query
    assert "count" not in eval_query
    # check orderBy is after limit in the sql query
    assert eval_query.index("limit") < eval_query.index("order by")


@pytest.mark.parametrize(
    "ops",
    [
        lambda df: df.iloc[0],
        lambda df: df.iloc[100],
    ],
)
@sql_count_checker(query_count=6, union_count=1)
def test_df_iloc_scalar_efficient_sql(session, ops):
    df = DataFrame({"a": [1] * 10000})
    with session.query_history() as query_listener:
        ops(df).to_pandas()
    eval_query = query_listener.queries[
        -3
    ].sql_text.lower()  # query before drop temp table and transpose
    # check no row count is in the sql query
    assert "count" not in eval_query
    # check limit is used in the sql query
    assert "limit" in eval_query
