#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import random
import re
from typing import Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from modin.pandas import Series
from pandas._libs.lib import is_scalar
from pandas.errors import IndexingError

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.frame.test_iloc import snowpark_pandas_input_keys
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# default_index_native_series and default_index_snowpark_pandas_series have size axis_len x 1
axis_len = 7
setitem_key_val_pair = [
    ([3, 1, 2], ["991", "992", "993"]),
    ([3, 1, 2], "991"),
]
TEST_ITEMS_DATA_2X1 = [-999, -998]


@sql_count_checker(query_count=1)
def test_non_default_index_series_iloc():
    indexed_native_series = native_pd.Series([1, 2, 3, 4, 5])
    indexed_native_series.index = ["index1", "index2", "index3", "index4", "index5"]
    indexed_snow_series = Series(indexed_native_series)
    eval_snowpark_pandas_result(
        indexed_snow_series,
        indexed_native_series,
        lambda ser: ser.iloc[1:3],
    )


@pytest.mark.parametrize(
    "key, expected_join_count",
    snowpark_pandas_input_keys,
)
def test_series_iloc_snowpark_pandas_input_return_dataframe(
    key,
    expected_join_count,
    iloc_snowpark_pandas_input_map,
    default_index_snowpark_pandas_series,
    default_index_native_series,
):
    expected_query_count = 3 if key.startswith("Index") else 1 if "Index" in key else 2
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_series,
            default_index_native_series,
            lambda ser: ser.iloc[
                try_convert_index_to_native(iloc_snowpark_pandas_input_map[key])
            ],
        )


@sql_count_checker(query_count=1)
def test_diff2native(default_index_snowpark_pandas_series, default_index_native_series):
    assert (
        default_index_snowpark_pandas_series.iloc[..., 3]
        == default_index_native_series.iloc[3]
    )


@pytest.mark.parametrize(
    "key, val",
    setitem_key_val_pair,
)
def test_series_iloc_setitem(
    key,
    val,
    default_index_native_int_snowpark_pandas_series,
    default_index_native_int_series,
):
    def operation(ser):
        ser.iloc[key] = val
        # Based on snowflake type results, the result becomes 'str' type so we normalize to float for comparison.
        return ser.astype("float")

    expected_join_count = 3 if isinstance(val, list) else 2
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            default_index_native_int_snowpark_pandas_series,
            default_index_native_int_series,
            operation,
        )


@pytest.mark.parametrize(
    "key, val",
    [
        (3, 9),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_setitem_scalar(
    key, val, default_index_snowpark_pandas_series, default_index_native_series
):
    def operation(ser):
        ser.iloc[key] = val

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        operation,
        inplace=True,
    )


@sql_count_checker(query_count=1, join_count=3)
def test_iloc_setitem_snowpark_pandas_input(
    default_index_snowpark_pandas_series, default_index_native_series
):
    item = Series([7, 8, 9])
    native_item = native_pd.Series([7, 8, 9])

    def operation(ser):
        if isinstance(ser, Series):
            ser.iloc[[0, 1, 2]] = item
        else:
            ser.iloc[[0, 1, 2]] = native_item

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        operation,
        inplace=True,
    )


# Types of input to test with get iloc.
KEY_TYPES = ["list", "series", "index", "ndarray", "index with name"]


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
def test_series_iloc_get_key_bool(
    key, key_type, default_index_native_int_series, multiindex_native_int_series
):
    # Check whether Series.iloc[key] works on given Series with:
    # - boolean list      - boolean Index (with and without name)
    # - boolean Series    - np.ndarray
    def iloc_helper(ser):
        # Note:
        # 1. boolean series key is not implemented in pandas, so we use list key to test it
        # 2. if key length does not match with ser, Snowpark will only select the row position the key contains; while
        # pandas will raise error, so we first truncate the Series for pandas and then compare the result
        if isinstance(ser, native_pd.Series):
            # If native pandas Series, truncate the series and key.
            _ser = ser[: len(key)]
            _key = key[: len(_ser)]
        else:
            _key, _ser = key, ser

        # Convert key to the required type.
        if key_type == "index":
            _key = (
                pd.Index(_key, dtype=bool)
                if isinstance(_ser, pd.Series)
                else native_pd.Index(_key, dtype=bool)
            )
        elif key_type == "ndarray":
            _key = np.array(_key)
        elif key_type == "index with name":
            _key = (
                pd.Index(_key, name="some name", dtype=bool)
                if isinstance(_ser, pd.Series)
                else native_pd.Index(_key, name="some name", dtype=bool)
            )
        elif key_type == "series" and isinstance(_ser, pd.Series):
            # Native pandas does not support iloc with Snowpark Series.
            _key = pd.Series(_key, dtype=bool)

        return _ser.iloc[_key]

    expected_join_count = 1

    expected_query_count = 1
    if key == [] and key_type in ["list", "ndarray"]:
        expected_join_count += 1

    default_index_int_series = pd.Series(default_index_native_int_series)
    # test ser with non-default index
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            default_index_int_series,
            default_index_native_int_series,
            iloc_helper,
        )

    native_int_series_with_non_default_index = default_index_native_int_series.reindex()
    int_series_with_non_default_index = pd.Series(
        native_int_series_with_non_default_index
    )
    # test ser with non default index
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            int_series_with_non_default_index,
            native_int_series_with_non_default_index,
            iloc_helper,
        )

    # test ser with MultiIndex
    int_series_with_multiindex = pd.Series(multiindex_native_int_series)
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            int_series_with_multiindex,
            multiindex_native_int_series,
            iloc_helper,
            check_index_type=False,
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
def test_series_iloc_get_key_numeric(
    key, key_type, default_index_native_int_series, multiindex_native_int_series
):
    # Check whether Series.iloc[key] works on a given Series with:
    # - numeric list      - numeric Index (with and without name)
    # - numeric Series    - np.ndarray
    def iloc_helper(ser):
        if isinstance(ser, native_pd.Series):
            # If native pandas Series, remove out-of-bound values to avoid errors and compare.
            _key = [k for k in key if -8 < k < 7]
        else:
            _key = key

        # Convert key to the required type.
        if key_type == "index":
            _key = (
                pd.Index(_key) if isinstance(ser, pd.Series) else native_pd.Index(_key)
            )
        elif key_type == "ndarray":
            _key = np.array(_key)
        elif key_type == "index with name":
            _key = (
                pd.Index(_key, name="some name")
                if isinstance(ser, pd.Series)
                else native_pd.Index(_key, name="some name")
            )
        elif key_type == "series" and isinstance(ser, pd.Series):
            # Native pandas does not support iloc with Snowpark Series.
            _key = pd.Series(_key, dtype=float if len(key) == 0 else None)

        return ser.iloc[_key]

    if key == [] and "index" in key_type:
        # There should be 0 queries in this case
        with SqlCounter(query_count=0):
            pass
        # Index objects have dtype object when empty
        return

    default_index_int_series = pd.Series(default_index_native_int_series)
    # test ser with default index
    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(
            default_index_int_series,
            default_index_native_int_series,
            iloc_helper,
        )

    native_int_series_with_non_default_index = default_index_native_int_series.reindex()
    int_series_with_non_default_index = pd.Series(
        native_int_series_with_non_default_index
    )
    # test ser with non default index
    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(
            int_series_with_non_default_index,
            native_int_series_with_non_default_index,
            iloc_helper,
        )

    # test ser with MultiIndex
    # Index dtype is different between Snowpark and native pandas if key produces empty df.
    int_series_with_multiindex = pd.Series(multiindex_native_int_series)
    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(
            int_series_with_multiindex,
            multiindex_native_int_series,
            iloc_helper,
            check_index_type=False,
        )


@pytest.mark.parametrize("key", [0, -3, 4, -7, 6, -8, 7, 52879115, -9028751])
def test_series_iloc_get_key_scalar(
    key, default_index_native_int_series, multiindex_native_int_series
):
    # Check whether Series.iloc[key] works with integer scalar keys.
    def iloc_helper(ser):
        if isinstance(ser, pd.Series) or -8 < key < 7:
            return ser.iloc[key]
        else:
            return []  # Snowpark pandas return [] where key is out of bound

    default_index_int_series = pd.Series(default_index_native_int_series)

    # Two queries are run - one for getting count(), one for displaying results.
    # One join is performed for each query.

    # test ser with default index
    with SqlCounter(query_count=1):
        snowpark_res = iloc_helper(default_index_int_series)
        native_res = iloc_helper(default_index_native_int_series)
        assert snowpark_res == native_res

    # test ser with non-default index
    native_int_series_with_non_default_index = default_index_native_int_series.reindex()
    int_series_with_non_default_index = pd.Series(
        native_int_series_with_non_default_index
    )
    with SqlCounter(query_count=1):
        snowpark_res = iloc_helper(int_series_with_non_default_index)
        native_res = iloc_helper(native_int_series_with_non_default_index)
        assert snowpark_res == native_res

    # test ser with MultiIndex
    int_series_with_multiindex = pd.Series(multiindex_native_int_series)
    with SqlCounter(query_count=1):
        snowpark_res = iloc_helper(int_series_with_multiindex)
        native_res = iloc_helper(multiindex_native_int_series)
        assert snowpark_res == native_res


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("key", [-7.2, 6.5, -120.3, 23.9])
def test_series_iloc_get_with_float_scalar_negative(
    key, default_index_snowpark_pandas_series, default_index_native_series
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        lambda ser: ser.iloc[key],
        expect_exception=True,
        assert_exception_equal=False,
    )


@pytest.mark.parametrize(
    "key, error_msg",
    [
        (
            native_pd.Series(["native", "pandas", "series", "of", "strings", ""]),
            re.escape(
                "<class 'pandas.core.series.Series'> is not supported as 'value' argument. "
                + "Please convert this to Snowpark pandas objects by calling "
                + "modin.pandas.Series()/DataFrame()"
            ),
        ),
        (
            native_pd.DataFrame({"A": [1, 2, 3, "hi"], "B": [0.9, -10, -5 / 6, "bye"]}),
            re.escape(
                "<class 'pandas.core.frame.DataFrame'> is not supported as 'value' argument. "
                + "Please convert this to Snowpark pandas objects by calling "
                + "modin.pandas.Series()/DataFrame()"
            ),
        ),
        ([..., 0], re.escape("Object of type ellipsis is not JSON serializable")),
        ((..., [..., 1]), "Object of type ellipsis is not JSON serializable"),
    ],
)
@sql_count_checker(query_count=0)
def test_series_iloc_get_ellipses_and_native_pd_key_raises_type_error_negative(
    key, error_msg, default_index_native_int_series
):
    # Check whether invalid keys passed in raise TypeError.
    # Native pandas objects cannot be used as keys, ellipses should not be present in row/col objects.
    snowpark_index_int_series = pd.Series(default_index_native_int_series)
    with pytest.raises(TypeError, match=error_msg):
        _ = snowpark_index_int_series.iloc[key]


@pytest.mark.parametrize(
    "key, error_msg",
    [
        (((1, 3), 0), "Too many indexers"),
        ((1, 1, 1), "Too many indexers"),
        (((0, 1), (0, 1)), "Too many indexers"),
        ((..., ...), "indexer may only contain one '...' entry"),
        ((..., ..., ...), "indexer may only contain one '...' entry"),
        ((12, native_pd.Categorical([1, 3, 4])), "Too many indexers"),
        ((native_pd.Categorical([1, 3, 4]), [0]), "Too many indexers"),
    ],
)
@sql_count_checker(query_count=0)
def test_series_iloc_get_key_raises_indexing_error_negative(
    key, error_msg, default_index_native_int_series
):
    # Check whether invalid keys passed in raise IndexError. Raised when tuples or Categorical objects
    # are used as row/col objects, too many ellipses used, more than two values inside a tuple key.
    snowpark_index_int_series = pd.Series(default_index_native_int_series)
    with pytest.raises(IndexingError, match=error_msg):
        _ = snowpark_index_int_series.iloc[key]


@pytest.mark.parametrize(
    "key",
    [
        slice(0, 0.1),  # stop is not an int
        slice(1, 2, 3.5),  # step is not an int
        slice(1.1, 2.1, 3),  # start and stop are not ints
    ],
)
@sql_count_checker(query_count=0)
def test_series_iloc_get_invalid_slice_key_negative(
    key, default_index_native_int_series
):
    # TypeError raised when non-integer scalars used as start, stop, or step in slice.
    snowpark_index_int_series = pd.Series(default_index_native_int_series)
    error_msg = "cannot do positional indexing with these indexers"
    with pytest.raises(TypeError, match=error_msg):
        _ = snowpark_index_int_series.iloc[key]


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
def test_series_iloc_get_non_numeric_key_negative(key, default_index_native_int_series):
    # Check whether invalid non-numeric keys passed in raise TypeError.
    # list-like objects need to be numeric, scalar keys can only be integers.
    # Native pandas Series and DataFrames are invalid inputs.
    if isinstance(key, native_pd.Index):
        key = pd.Index(key)
    snowpark_index_int_series = pd.Series(default_index_native_int_series)
    error_msg = re.escape(f".iloc requires numeric indexers, got {key}")
    # 1 extra queries for repr
    with SqlCounter(query_count=1 if isinstance(key, pd.Index) else 0):
        with pytest.raises(IndexError, match=error_msg):
            _ = snowpark_index_int_series.iloc[key]


@sql_count_checker(query_count=0)
def test_series_iloc_get_key_snowpark_df_input_negative(
    default_index_native_int_series,
):
    # Verify that Snowpark DataFrame is invalid input.
    snowpark_index_int_series = pd.Series(default_index_native_int_series)
    key = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    error_msg = "DataFrame indexer is not allowed for .iloc\nConsider using .loc for automatic alignment."
    with pytest.raises(IndexError, match=error_msg):
        _ = snowpark_index_int_series.iloc[key]


@sql_count_checker(query_count=1)
def test_series_iloc_get_key_snowpark_empty_str_series_input_negative(
    default_index_native_int_series,
):
    # Verify that Empty Snowpark Series of string type is invalid input.
    snowpark_index_int_series = pd.Series(default_index_native_int_series)
    key = pd.Series([], dtype=str)
    error_msg = re.escape(
        ".iloc requires numeric indexers, got Series([], dtype: object)"
    )
    with pytest.raises(IndexError, match=error_msg):
        _ = snowpark_index_int_series.iloc[key]


@pytest.mark.parametrize(
    "key",
    [
        native_pd.Categorical((1, 3, -1)),
    ],
)
@sql_count_checker(query_count=0)
def test_series_iloc_get_key_raises_not_implemented_error_negative(
    key, default_index_native_int_series
):
    # Verify that Categorical types raises NotImplementedError.
    snowpark_index_int_series = pd.Series(default_index_native_int_series)
    error_msg = re.escape("pandas type category is not implemented")
    with pytest.raises(NotImplementedError, match=error_msg):
        _ = snowpark_index_int_series.iloc[key]


@sql_count_checker(query_count=1)
def test_series_iloc_get_empty(empty_snowpark_pandas_series):
    _ = empty_snowpark_pandas_series.iloc[0]


TEST_DATA_FOR_ILOC_GET_SLICE = [0, -10, -2, -1, None, 1, 2, 10]


@pytest.mark.parametrize("start", TEST_DATA_FOR_ILOC_GET_SLICE)
@pytest.mark.parametrize("stop", TEST_DATA_FOR_ILOC_GET_SLICE)
@pytest.mark.parametrize("step", TEST_DATA_FOR_ILOC_GET_SLICE[1:])
@sql_count_checker(query_count=2)
def test_series_iloc_get_slice(default_index_native_int_series, start, stop, step):
    default_index_int_series = pd.Series(default_index_native_int_series)
    # test ser with default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_int_series,
            default_index_native_int_series,
            lambda ser: ser.iloc[slice(start, stop, step)],
        )

    native_int_series_with_non_default_index = default_index_native_int_series.reindex()
    int_series_with_non_default_index = pd.Series(
        native_int_series_with_non_default_index
    )
    # test ser with non default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            int_series_with_non_default_index,
            native_int_series_with_non_default_index,
            lambda ser: ser.iloc[slice(start, stop, step)],
        )


@sql_count_checker(query_count=0)
def test_series_iloc_get_slice_with_invalid_step_negative():
    snowpark_ser = pd.Series([1, 2, 3, 4])
    with pytest.raises(ValueError, match="slice step cannot be zero."):
        _ = snowpark_ser.iloc[slice(None, None, 0)]


@sql_count_checker(query_count=0, join_count=0)
@pytest.mark.parametrize(
    "slice_key",
    [
        slice("a", None, None),
        slice(None, "b", None),
        slice(None, None, "c"),
    ],
)
@sql_count_checker(query_count=0)
def test_series_iloc_get_slice_with_non_integer_parameters_negative(slice_key):
    snowpark_ser = pd.Series([1, 2, 3, 4])
    with pytest.raises(
        TypeError, match="cannot do positional indexing with these indexers"
    ):
        _ = snowpark_ser.iloc[slice_key]


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
@sql_count_checker(query_count=2)
def test_series_iloc_get_range(default_index_native_int_series, range_key):
    default_index_int_series = pd.Series(default_index_native_int_series)

    # test ser with default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_int_series,
            default_index_native_int_series,
            lambda ser: ser.iloc[range_key],
        )

    native_int_series_with_non_default_index = default_index_native_int_series.reindex()
    int_series_with_non_default_index = pd.Series(
        native_int_series_with_non_default_index
    )
    # test ser with non default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            int_series_with_non_default_index,
            native_int_series_with_non_default_index,
            lambda ser: ser.iloc[range_key],
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
@sql_count_checker(query_count=2)
def test_series_iloc_get_range_deviating_behavior(
    default_index_native_int_series, range_key
):
    def iloc_helper(ser):
        if isinstance(ser, pd.Series):
            return ser.iloc[range_key]
        # Convert range key to slice key for comparison since pandas fails with given ranges.
        start, stop, step = range_key.start, range_key.stop, range_key.step
        if (start > stop and step > 0) or (start < stop and step < 0):
            slice_key = slice(0, 0, 1)
        else:
            slice_key = slice(start, stop, step)
        return ser.iloc[slice_key]

    default_index_int_series = pd.Series(default_index_native_int_series)

    with pytest.raises(IndexError, match="positional indexers are out-of-bounds"):
        _ = default_index_native_int_series.iloc[range_key]

    # test ser with default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            default_index_int_series,
            default_index_native_int_series,
            iloc_helper,
        )

    native_int_series_with_non_default_index = default_index_native_int_series.reindex()
    int_series_with_non_default_index = pd.Series(
        native_int_series_with_non_default_index
    )
    # test ser with non default index
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            int_series_with_non_default_index,
            native_int_series_with_non_default_index,
            iloc_helper,
        )


@pytest.mark.parametrize(
    "row_pos, col_pos, item_values",
    [
        ([1, 2, 3], [], [91]),
        ([1, 2, 3], [0, 2, 3], [91, 92, 93]),
        (
            [3, 2, 1],
            [3, 2, 0],
            [91, 92, 93],
        ),
        ([3, 2, 1], [0, 2, 3], [91, 92, 93]),
        ([1, 3, 0], [3, 2, 0], [91]),
        ([False, True, True, True], [True, False, True, True], [91, 92, 93]),
    ],
)
def test_iloc_with_row_key_series_rhs_series(
    numeric_test_data_4x4, row_pos, col_pos, item_values
):
    snow_df = pd.DataFrame(numeric_test_data_4x4)
    native_df = native_pd.DataFrame(numeric_test_data_4x4)

    snow_row_key = pd.Series(row_pos)
    native_row_key = native_pd.Series(row_pos)

    snow_col_key = pd.Series(col_pos)
    if len(col_pos) == 0:
        # An empty list defaults to float which is not supported here.
        snow_col_key = snow_col_key.astype("int")
    native_col_key = native_pd.Series(col_pos)

    snow_values_df = pd.Series(item_values)
    native_values_df = native_pd.Series(item_values)

    def perform_iloc(df):
        if isinstance(df, pd.DataFrame):
            df.iloc[snow_row_key, snow_col_key] = snow_values_df
        else:
            df.iloc[native_row_key, native_col_key] = native_values_df

    expected_query_count = 2
    expected_join_count = 2 if all(isinstance(i, bool) for i in row_pos) else 3
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(snow_df, native_df, perform_iloc, inplace=True)


@pytest.mark.parametrize(
    "row_pos, col_pos, item_values, native_values",
    [
        ([1, 2, 3], [0, 2, 3], [91, 92], [91, 92, 92]),
        (
            [3, 2, 1],
            [3, 2, 0],
            [],
            [[None, None, None], [None, None, None], [None, None, None]],
        ),
        ([3, 1, 2], [2, 0, 3], [None], [None, None, None]),
        ([2, 3, 1], [3, 1, 2], [91, 92], [91, 92, 92]),
        ([False, True, True, True], [True, False, True, True], [91, 92], [91, 92, 92]),
    ],
)
def test_iloc_with_row_key_series_rhs_series_no_shape_check(
    numeric_test_data_4x4, row_pos, col_pos, item_values, native_values
):
    snow_df = pd.DataFrame(numeric_test_data_4x4)
    native_df = native_pd.DataFrame(numeric_test_data_4x4)

    snow_row_key = pd.Series(row_pos)
    native_row_key = native_pd.Series(row_pos)

    snow_col_key = pd.Series(col_pos)
    native_col_key = native_pd.Series(col_pos)

    def perform_iloc(df):
        if isinstance(df, pd.DataFrame):
            df.iloc[snow_row_key, snow_col_key] = pd.Series(item_values)
        else:
            if native_values:
                # This means the equivalent pandas operation we expect to fail, but we'll use a different
                # item value that should match what Snowpark pandas produces.
                if isinstance(native_values[0], list):
                    native_values_df = native_pd.DataFrame(native_values)
                else:
                    native_values_df = native_pd.Series(native_values)
            else:
                native_values_df = native_pd.Series(item_values)
            df.iloc[native_row_key, native_col_key] = native_values_df

    expected_query_count = 2
    expected_join_count = 2 if all(isinstance(i, bool) for i in row_pos) else 3
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
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
    "item_values, item_index, expected_join_count",
    [
        [999, None, 2],
        [TEST_ITEMS_DATA_2X1, None, 3],
        [TEST_ITEMS_DATA_2X1, [("r",), ("s",)], 4],
        [TEST_ITEMS_DATA_2X1, [("r", 20), ("s", 25)], 5],
    ],
)
def test_df_iloc_set_with_multiindex(
    row_key, row_key_index, item_values, item_index, expected_join_count
):
    ser_data = [10, 11, 12, 13, 14]
    row_index = pd.MultiIndex.from_tuples(
        [("x", 99), ("y", 11), ("x", 11), ("y", 99), ("z", -12)]
    )

    snow_ser = pd.Series(ser_data, index=row_index)
    native_ser = native_pd.Series(ser_data, index=row_index)

    if is_scalar(row_key):
        if isinstance(item_values, list):
            item_values = item_values[:1]
        if item_index:
            item_index = item_index[:1]

    if isinstance(item_values, list):
        snow_items = pd.Series(item_values)
        native_items = native_pd.Series(item_values)
    else:
        snow_items = item_values
        native_items = item_values

    if item_index:
        snow_items.index = pd.MultiIndex.from_tuples(item_index)
        native_items.index = pd.MultiIndex.from_tuples(item_index)

    if row_key_index:
        # Using native pandas index since row_key[2] is a MultiIndex object.
        snow_row_key = pd.Series(row_key, index=native_pd.Index(row_key_index))
        native_row_key = native_pd.Series(row_key, index=native_pd.Index(row_key_index))
    else:
        snow_row_key = row_key
        native_row_key = row_key

    def helper_iloc(ser):
        if isinstance(ser, native_pd.Series):
            ser.iloc[native_row_key] = native_items
        else:
            ser.iloc[snow_row_key] = snow_items

    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(snow_ser, native_ser, helper_iloc, inplace=True)


def test_series_iloc_get_series_with_multiindex(
    default_index_native_int_series, multiindex_native_int_series
):
    def run_multiindex_test(_ser: pd.Series, _native_ser: native_pd.Series) -> None:
        def iloc_helper(series: Union[pd.Series, native_pd.Series]) -> None:
            return (
                series.iloc[_ser]
                if isinstance(series, pd.Series)
                else series.iloc[_native_ser]
            )

        # test ser with default index
        with SqlCounter(query_count=1, join_count=2):
            eval_snowpark_pandas_result(
                default_index_int_series,
                default_index_native_int_series,
                iloc_helper,
            )

        # test ser with non default index
        with SqlCounter(query_count=1, join_count=2):
            eval_snowpark_pandas_result(
                int_series_with_non_default_index,
                native_int_series_with_non_default_index,
                iloc_helper,
            )

        # test ser with MultiIndex
        with SqlCounter(query_count=1, join_count=2):
            eval_snowpark_pandas_result(
                int_series_with_multiindex,
                multiindex_native_int_series,
                iloc_helper,
                check_index_type=False,
            )

    # Create series for running tests on.
    default_index_int_series = pd.Series(default_index_native_int_series)
    native_int_series_with_non_default_index = default_index_native_int_series.reindex()
    int_series_with_non_default_index = pd.Series(
        native_int_series_with_non_default_index
    )
    int_series_with_multiindex = pd.Series(multiindex_native_int_series)

    # Evaluate with MultiIndex created from tuples.
    arrays = [
        ["bar", "bar", "baz", "baz"],
        ["one", "two", "one", "two"],
    ]
    tuples = list(zip(*arrays))
    index = native_pd.MultiIndex.from_tuples(tuples, names=["first", "second"])
    native_ser = native_pd.Series([2, 3, 4, 5], index=index)
    ser = pd.Series([2, 3, 4, 5], index=index)
    run_multiindex_test(ser, native_ser)

    # Evaluate with MultiIndex created from product.
    iterables = [["bar", "baz", "foo"], [22, 23]]
    index = native_pd.MultiIndex.from_product(iterables, names=[2, "second"])
    ser = pd.Series([0, 1, 2, 3, 4, 5], index=index)
    native_ser = native_pd.Series([0, 1, 2, 3, 4, 5], index=index)
    run_multiindex_test(ser, native_ser)

    # Evaluate with MultiIndex created from a DataFrame.
    dataframe = native_pd.DataFrame(
        [["bar", "one"]],
        columns=["first", "second"],
    )
    index = native_pd.MultiIndex.from_frame(dataframe)
    ser = pd.Series([4], index=index)
    native_ser = native_pd.Series([4], index=index)
    run_multiindex_test(ser, native_ser)

    # Evaluate with MultiIndex created from an empty DataFrame.
    dataframe = native_pd.DataFrame([], columns=["first", "second"])
    index = native_pd.MultiIndex.from_frame(dataframe)
    ser = pd.Series([], index=index, dtype=int)
    native_ser = native_pd.Series([], index=index, dtype=int)
    run_multiindex_test(ser, native_ser)


@sql_count_checker(query_count=0, join_count=0)
def test_iloc_get_multiindex_key_negative(
    default_index_native_int_series, multiindex_native
):
    with pytest.raises(
        TypeError, match="key of type MultiIndex cannot be used with iloc"
    ):
        snowpark_series = pd.Series(default_index_native_int_series)
        _ = snowpark_series.iloc[multiindex_native]


def test_series_iloc_get_key_bool_series_from_itself():
    # The join in the following iloc operation is a self-join on row position column,
    # so it will be skipped and no join query is issued.
    series = pd.Series([4, 3, 3, 1])
    with SqlCounter(query_count=1, join_count=0):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            series.iloc[series == 3],
            native_pd.Series([3, 3], index=[1, 2]),
        )

    series = pd.Series([True, False, False, True])
    with SqlCounter(query_count=1, join_count=0):
        assert_snowpark_pandas_equal_to_pandas(
            series.iloc[series],
            native_pd.Series([True, True], index=[0, 3]),
        )


@pytest.mark.xfail(reason="TODO: SNOW-991872 support set cell to array values")
def test_series_iloc_set_scalar_key_with_list_value_negative():
    series = pd.Series([[1], [2], [3]])
    native_s = native_pd.Series([[1], [2], [3]])

    def helper(series):
        series.iloc[0] = [4, 5, 6]

    eval_snowpark_pandas_result(series, native_s, helper, inplace=True)


@pytest.mark.parametrize(
    "ops",
    [
        lambda df: df.head(),
        lambda df: df.iloc[1:100],
        lambda df: df.iloc[1000:100:-1],
        lambda df: df.iloc[0],
        lambda df: df.iloc[100],
    ],
)
@sql_count_checker(query_count=1)
def test_iloc_efficient_sql(session, ops):
    df = pd.Series({"a": [1] * 10000})
    with session.query_history() as query_listener:
        res = ops(df)
        if isinstance(res, pd.Series):
            res._to_pandas()
    eval_query = query_listener.queries[-1].sql_text.lower()
    # check no row count
    assert "count" not in eval_query
    # check orderBy is after limit in the sql query
    assert "count" not in eval_query
    assert eval_query.index("limit") < eval_query.index("order by")
