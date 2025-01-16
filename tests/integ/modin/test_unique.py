#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_values_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


def _make_nan_interleaved_float_series():
    ser = native_pd.Series([1.2345] * 100)
    ser[::2] = np.nan
    return ser


TEST_UNIQUE_SERIES_DATA = [
    native_pd.Series([]),
    native_pd.Series([2, 1, 3, 3]),
    native_pd.Series([1, 1, 1, 1]),
    native_pd.Series([12.0, 11.999999, 11.999999]),
    native_pd.Series(["A", "A", "C", "C", "A"]),
    native_pd.Series([None, "A", None, "B"]),
    native_pd.Series([None] * 100),
    _make_nan_interleaved_float_series(),
    native_pd.Series(["A", 12, 56, "A"]),  # heterogeneous data
]


class TestUniqueUserDefinedClass:
    # Class used to create a list of user defined objects for negative testing.
    def __init__(self, int_param, float_param, str_param) -> None:
        self.int_param = int_param
        self.float_param = float_param
        self.str_param = str_param


# Tests based on Series.unique() tests.
@pytest.mark.parametrize("native_series", TEST_UNIQUE_SERIES_DATA)
@sql_count_checker(query_count=1)
def test_unique_series(native_series: native_pd.Series):
    # Check whether pd.unique works with pd.Series as input.
    snowpark_series = pd.Series(native_series)
    snowpark_unique = pd.unique(snowpark_series)
    native_unique = native_pd.unique(native_series)
    assert_values_equal(snowpark_unique, native_unique)


@pytest.mark.parametrize("native_series", TEST_UNIQUE_SERIES_DATA)
@sql_count_checker(query_count=1)
def test_unique_series_reordered(native_series: native_pd.Series):
    # Check whether pd.unique works with a reordered series as input to verify that the order of the elements returned
    # by unique is the same as the order of appearance.
    snowpark_series = pd.Series(native_series)
    snowpark_unique = pd.unique(snowpark_series.iloc[::-1])
    native_unique = native_pd.unique(native_series.iloc[::-1])
    assert_values_equal(snowpark_unique, native_unique)


@pytest.mark.parametrize(
    "index",
    [
        native_pd.Index([]),
        native_pd.Index([2, 1, 3, 3]),
        native_pd.Index([1, 1, 1, 1]),
        native_pd.Index([12.0, 11.999999, 11.999999]),
        native_pd.Index(["A", "A", "C", "C", "A"]),
        native_pd.Index([None, "A", None, "B"]),
        native_pd.Index([None] * 100),
        native_pd.Index(["A", 12, 56, "A"]),  # heterogeneous data
    ],
)
@sql_count_checker(query_count=1)
def test_unique_index(index):
    # Check whether pd.unique works with pd.Index as input.
    eval_snowpark_pandas_result(
        pd, native_pd, lambda lib: lib.unique(index), comparator=assert_values_equal
    )


@pytest.mark.parametrize(
    "input_data",
    [
        np.array(list("baabc"), dtype="O"),
        np.array(["this", "is", "a", "big", "big", "big", "list"]),
        np.array([12, 13, 12, 13, 13, 42960, 1245]),
    ],
)
@sql_count_checker(query_count=1)
def test_unique_ndarray(input_data: np.array):
    # Check whether pd.unique works with np.ndarray as input.
    eval_snowpark_pandas_result(
        pd,
        native_pd,
        lambda lib: lib.unique(input_data),
        comparator=assert_values_equal,
    )


@pytest.mark.parametrize(
    "timestamp_index",
    [
        native_pd.Index(
            [
                native_pd.Timestamp("20160101", tz="US/Eastern"),
                native_pd.Timestamp("20160101", tz="US/Eastern"),
                native_pd.Timestamp("20160201", tz="US/Eastern"),
                native_pd.Timestamp("20990101", tz="US/Eastern"),
            ]
        )
    ],
)
@sql_count_checker(query_count=1)
def test_unique_timestamp_index(timestamp_index):
    # Check whether pd.unique works with a pd.Index of timestamps as input.
    snowpark_unique = pd.unique(timestamp_index)
    native_unique = native_pd.unique(timestamp_index)

    # pd.unique(index) seems to return an array most of the time, but
    # returns a datetime index in this case. It's supposed to return
    # an index though:
    # https://github.com/pandas-dev/pandas/issues/57043
    # TODO(SNOW-1019312): Make snowpark_pandas.unique(index) always
    # return an index.
    assert_values_equal(
        native_unique,
        native_pd.Index(
            [
                native_pd.Timestamp("20160101", tz="US/Eastern"),
                native_pd.Timestamp("20160201", tz="US/Eastern"),
                native_pd.Timestamp("20990101", tz="US/Eastern"),
            ]
        ),
    )

    assert_values_equal(snowpark_unique, np.array(native_unique))


@pytest.mark.parametrize(
    "input_data",
    [
        [1, 1, 1, 2, 2, 3],
        [1.0, -1.0, 0, -1.0, 0],
        ["foo", "bar", "baz", "foobar", "baz"],
        [None, 1, 0.9, "0.8"],
        [None] * 10,
        [0.9] * 100,
        [],
    ],
)
@sql_count_checker(query_count=1)
def test_unique_list(input_data: list):
    # Check whether pd.unique works with a list as input.
    eval_snowpark_pandas_result(
        pd,
        native_pd,
        lambda lib: lib.unique(input_data),
        comparator=assert_values_equal,
    )


@pytest.mark.parametrize(
    "input_data",
    [
        tuple([1, 2, -1, -2, -2, 0]),
        ("first", "second", "third", "third"),
        (1.0, -9.8, -0.006, 11.999999, -9.8),
        tuple([None, None]),
        tuple(),
    ],
)
@sql_count_checker(query_count=1)
def test_unique_tuple(input_data: tuple):
    # Check whether pd.unique works with a tuple as input.
    eval_snowpark_pandas_result(
        pd,
        native_pd,
        lambda lib: lib.unique(input_data),
        comparator=assert_values_equal,
    )


@pytest.mark.parametrize(
    "input_data",
    [
        bytearray(b"This string is a future bytearray!"),
        bytearray(b""),
    ],
)
@sql_count_checker(query_count=1)
def test_unique_byte_array(input_data: bytearray):
    # Check whether pd.unique works with a bytearray as input.
    eval_snowpark_pandas_result(
        pd,
        native_pd,
        lambda lib: lib.unique(input_data),
        comparator=assert_values_equal,
    )


@sql_count_checker(query_count=1)
def test_unique_list_of_tuples():
    input = [("a", "b"), ("b", "a"), ("a", "c"), ("b", "a")]
    # Native pandas returns a ndarray with tuples in it while Snowpark pandas returns a ndarray with lists.
    # Native pandas returns a ndarray of shape (3,) - trying to construct it from np.array() directly yields a ndarray
    # of shape (3, 1), therefore construct it from a native pandas series.
    native_expected = native_pd.Series([("a", "b"), ("b", "a"), ("a", "c")]).to_numpy()
    assert_values_equal(native_pd.unique(input), native_expected)

    # Snowpark pandas returns a ndarray of shape (3,) - trying to construct it from np.array([[11], [12], [100]])
    # yields a ndarray of shape (3, 1).
    snowpark_expected = native_pd.Series(
        [["a", "b"], ["b", "a"], ["a", "c"]]
    ).to_numpy()
    assert_values_equal(pd.unique(input), snowpark_expected)


@sql_count_checker(query_count=1)
def test_unique_list_of_lists():
    input = [[11], [12], [12], [12], [100]]
    # Native pandas raises TypeError when given a list of lists.
    err_msg = "unhashable type: 'list'"
    with pytest.raises(TypeError, match=err_msg):
        native_pd.unique(input)

    # Snowpark pandas returns a ndarray of shape (3,) - trying to construct it from np.array([[11], [12], [100]])
    # yields a ndarray of shape (3, 1).
    expected = native_pd.Series([[11], [12], [100]]).to_numpy()
    assert_values_equal(pd.unique(input), expected)


@pytest.mark.parametrize(
    "input_data",
    [{"first": None, "second": None, "third": []}, None, 12, "", "this is a string!"],
)
@sql_count_checker(query_count=0)
def test_unique_non_list_like_object_negative(input_data):
    with pytest.raises(
        TypeError, match="Only list-like objects can be used with unique"
    ):
        assert_values_equal(pd.unique(input_data), "placeholder")


@pytest.mark.parametrize(
    "input_data, dtype",
    [
        (
            [
                TestUniqueUserDefinedClass(1, 0.9, "0.8"),
                TestUniqueUserDefinedClass(1, 0.9, "0.8"),
                TestUniqueUserDefinedClass(None, None, None),
            ],
            "TestUniqueUserDefinedClass",
        ),
        ([{1, 2, 3, 4, 5, 6}, {1, 2, 3, 4, 5, 6}, {1, 2, 3, 4, 5, 6}], "set"),
        (
            [
                np.array([12, 13, 12, 13, 13, 42960, 1245]),
                np.array([12, 13, 12, 13, 13, 42960, 1245]),
                np.array([12, 13, 12, 14, 15, 42960, 1245]),
            ],
            "ndarray",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_unique_list_of_invalid_objects_negative(input_data, dtype):
    with pytest.raises(
        TypeError, match=f"Object of type {dtype} is not JSON serializable"
    ):
        assert_values_equal(pd.unique(input_data), "placeholder")
