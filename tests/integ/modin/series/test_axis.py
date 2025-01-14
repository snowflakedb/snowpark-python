#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# Tests for Series.set_axis()
# ---------------------------

# Valid data
# ----------
# Format: series, axis, labels, number of SQL queries on copy, number of JOINs.
# This data covers the positive cases for Series.set_axis().
# "index", "rows", and 0 are valid axis values and are tested on different Series objects with valid labels.
# The valid labels include strings with quotation marks, None values, numbers, Index, and MultiIndex objects.
# Some of the Series and Index inputs have "name".
TEST_DATA_FOR_SERIES_SET_AXIS = [
    [
        native_pd.Series({"A": [1, 2, 3], 5 / 6: [4, 5, 6]}),
        "index",
        [None] * 2,
        1,
        1,
    ],
    [
        native_pd.Series(
            {
                "fibonacci": [0, 1, 2, 3, 5, 8],
                "squares": [0, 1, 4, 9, 16, 25],
                "primes": [2, 3, 5, 7, 11, 13],
            },
            name="special numbers series",
        ),
        "index",
        ["iccanobif", "serauqs", "semirp"],
        1,
        1,
    ],
    [
        native_pd.Series(
            {
                "fibonacci": [0, 1, 2, 3, 5, 8],
                "squares": [0, 1, 4, 9, 16, 25],
                "primes": [2, 3, 5, 7, 11, 13],
            },
            name="special numbers series",
        ),
        "index",
        native_pd.Series(["iccanobif", "serauqs", "semirp"], name="reverse names"),
        1,
        1,
    ],
    [
        native_pd.Series(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "'C'": [4, 5, 6],
                '"D"': [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        0,
        native_pd.Index([99, 999, 9999, 99999, 999999]),
        1,
        1,
    ],
    [
        native_pd.Series(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "'C'": [4, 5, 6],
                '"D"': [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        0,
        native_pd.Index([99, 999, 9999, 99999, 999999], name="index with name"),
        1,
        1,
    ],
    [
        native_pd.Series(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "'C'": [4, 5, 6],
                '"D"': [7, 8, 9],
                "E": [-1, -2, -3],
            },
            name="series with name",
        ),
        0,
        native_pd.Index([99, 999, 9999, 99999, 999999], name="index with name"),
        1,
        1,
    ],
    [  # Index is a MultiIndex from tuples.
        native_pd.Series({"A": [1, 2, 3], -2515 / 135: [4, 5, 6]}),
        "index",
        native_pd.MultiIndex.from_tuples(
            [("r0", "rA"), ("r1", "rB")], names=["Courses", "Fee"]
        ),
        1,
        2,
    ],
    [  # Index is a MultiIndex from arrays.
        native_pd.Series(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "'C'": [4, 5, 6],
                '"D"': [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        0,
        native_pd.MultiIndex.from_arrays(
            [
                ["genmaicha", "peppermint", "jasmine", "spice", "earl grey"],
                [5, 2.5, 6, 1, 3.5],
                ["mild", "none", "mild", "high", "high"],
            ],
            names=("tea", "steep time", "caffeine"),
        ),
        1,
        3,
    ],
    [  # Index is a MultiIndex from a DataFrame.
        native_pd.Series(
            {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9], "D": [10, 11, 12]}
        ),
        "rows",
        native_pd.MultiIndex.from_frame(
            native_pd.DataFrame(
                [["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]],
                columns=["a", "b"],
            ),
        ),
        1,
        2,
    ],
    [  # Index is a MultiIndex from a product.
        native_pd.Series({1: [1], 2: [2], 3: [3], 4: [4], 5: [5], 6: [6]}),
        0,
        native_pd.MultiIndex.from_product(
            [[0, 1, 2], ["green", "purple"]], names=["number", "color"]
        ),
        1,
        2,
    ],
    [
        native_pd.Series({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        "index",
        {1: 1, 2: 2},
        1,
        1,
    ],
    [
        native_pd.Series({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        "rows",
        {1, 2},
        1,
        1,
    ],
]


# Invalid data which raises ValueError
# ------------------------------------
# Format: series, axis, labels, and error message.
# - This data is for Series.set_axis() with invalid axis values.
# - Invalid axis values here consist of: None values, numbers other than 0,
#   strings other than "index" and "rows", and column values like "columns" and 1.
TEST_DATA_FOR_SERIES_SET_AXIS_RAISES_VALUE_ERROR = [
    [
        native_pd.Series({"A": [1, 2, 3], "B": [4, 5, 6]}),
        "indexes",  # invalid axis
        native_pd.Index(["a", "b", "c"]),
    ],
    [
        native_pd.Series(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "C": [4, 5, 6],
                "D": [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        -1111.1111,  # invalid axis
        {99, 999, 9999, 99999, 999999},
    ],
    [
        native_pd.Series({"A": [1, 2, 3], "B": [None] * 3}),
        4000000,  # invalid axis
        ["None"] * 3,
    ],
    [
        native_pd.Series({"A": [3.14, 1.414, 1.732], "B": [9.8, 1.0, 0]}),
        None,  # invalid axis
        {1: 1, 2: 2, 3: 3},
    ],
    [
        native_pd.Series(
            {
                "A": ["foo", "bar", 3],
                "B": [4, "baz", 6],
                "C": [4, "baz", "foo"],
                "D": [4, 5, None],
            }
        ),
        -97 / 23,  # invalid axis
        native_pd.Series(["index"] * 4),
    ],
    [
        native_pd.Series(
            {"A": ["a", "b", "c", "d", "e"], "B": ["e", "d", "c", "b", "a"]}
        ),
        "0",  # invalid axis
        native_pd.Index([11, 111]),
    ],
    [
        native_pd.Series({"foo": [None], "bar": [None], "baz": [None]}),
        -0.00001,  # invalid axis
        native_pd.Index([None] * 3),
    ],
    [  # Index is a MultiIndex from tuples.
        native_pd.Series({"A": [1, 2, 3], -2515 / 135: [4, 5, 6]}),
        0.000001,  # invalid axis
        native_pd.MultiIndex.from_tuples(
            [("r0", "rA"), ("r1", "rB")], names=["Courses", "Fee"]
        ),
    ],
    [  # Labels is a MultiIndex from a DataFrame.
        native_pd.Series(
            {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9], "D": [10, 11, 12]}
        ),
        1,  # invalid axis
        native_pd.MultiIndex.from_frame(
            native_pd.DataFrame(
                [["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]],
                columns=["a", "b"],
            ),
        ),
    ],
    [
        native_pd.Series({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        "columns",  # invalid axis; valid axis for DataFrames
        None,  # invalid labels
    ],
]


# Invalid data which raises ValueError different from native pandas
# -----------------------------------------------------------------
# Format: series, axis, labels, and error message.
# - This data is for Series.set_axis() with invalid labels.
# - Invalid labels consist of too many or too few labels provided.
TEST_DATA_FOR_SERIES_SET_AXIS_RAISES_VALUE_ERROR_DIFF_ERROR_MSG = [
    [
        native_pd.Series({"A": [1, 2, 3], "B": [4, 5, 6]}),
        "index",
        ["a", "b", "" "c" "", "d"],  # too many labels
        "Length mismatch: Expected 2 rows, received array of length 4",
    ],
    [
        native_pd.Series({"foo": [None], "bar": [None], "baz": [None]}),
        0,
        native_pd.Index([None]),  # too few labels
        "Length mismatch: Expected 3 rows, received array of length 1",
    ],
    [  # Labels is a MultiIndex from arrays.
        native_pd.Series(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "'C'": [4, 5, 6],
                '"D"': [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        "rows",
        native_pd.MultiIndex.from_arrays(
            [[], [], []],
            names=("tea", "steep time", "caffeine"),
        ),  # too few labels
        "Length mismatch: Expected 5 rows, received array of length 0",
    ],
    [  # Labels is a MultiIndex from a product.
        native_pd.Series({1: [1], 2: [2], 3: [3], 4: [4], 5: [5], 6: [6]}),
        "index",
        native_pd.MultiIndex.from_product(
            [[0], ["green", "purple", "pink", "brown", "orange"]],
            names=["number", "color"],
        ),  # too many labels
        "Length mismatch: Expected 6 rows, received array of length 5",
    ],
]


# Invalid data which raises TypeError different from native pandas
# ----------------------------------------------------------------
# Format: series, axis, labels, and error message.
# - This data is for Series.set_axis() with invalid axis values.
# - This includes using list, Index, Series, DataFrame, and MultiIndex objects
# - for axis or passing in a scalar value for labels
# - Data below raises TypeError.
TEST_DATA_FOR_SERIES_SET_AXIS_RAISES_TYPE_ERROR = [
    [
        native_pd.Series({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        "rows",
        1,  # invalid labels type
        re.escape(
            "Index(...) must be called with a collection of some kind, 1 was passed"
        ),
    ],
    [
        native_pd.Series({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        "rows",  # invalid axis; valid only for DataFrames
        ...,  # invalid labels type
        re.escape(
            "Index(...) must be called with a collection of some kind, Ellipsis was passed"
        ),
    ],
    [
        native_pd.Series({"A": [1, 2, 3], "B": [None] * 3}),
        [],  # invalid axis
        [None] * 2,
        "list is not a valid type for axis.",
    ],
    [
        native_pd.Series({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        native_pd.Series({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),  # invalid axis
        ["I", "II"],
        "Series is not a valid type for axis.",
    ],
    [
        native_pd.Series(
            {
                "A": ["foo", "bar", 3],
                "B": [4, "baz", 6],
                "C": [4, "baz", "foo"],
                "D": [4, 5, None],
            }
        ),
        native_pd.DataFrame(
            {
                "A": ["foo", "bar", 3],
                "B": [4, "baz", 6],
                "C": [4, "baz", "foo"],
                "D": [4, 5, None],
            }
        ),  # invalid axis
        ["index"] * 4,
        "DataFrame is not a valid type for axis.",
    ],
    [
        native_pd.Series(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "C": [4, 5, 6],
                "D": [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        native_pd.Index([11, 111]),  # invalid axis
        [99, 999, 9999, 99999, 999999],
        "Index is not a valid type for axis.",
    ],
    [
        native_pd.Series({"A": [1, 2, 3], "B": [None] * 3}),
        native_pd.MultiIndex.from_tuples([], names=["Courses", "Fee"]),  # invalid axis
        ["None"] * 3,
        "MultiIndex is not a valid type for axis.",
    ],
    [
        native_pd.Series({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        native_pd.MultiIndex.from_frame(
            native_pd.DataFrame(
                [["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]],
                columns=["a", "b"],
            ),
        ),  # invalid axis
        ["I", "II"],
        "MultiIndex is not a valid type for axis.",
    ],
    [
        native_pd.Series({"A": [], "B": []}),
        "index",
        None,  # invalid labels
        "None is not a valid value for the parameter 'labels'.",
    ],
]


# Behavior tests for DataFrame.set_axis().
@pytest.mark.parametrize(
    "native_series, axis, labels, num_queries, num_joins", TEST_DATA_FOR_SERIES_SET_AXIS
)
def test_set_axis_series_copy(native_series, axis, labels, num_queries, num_joins):
    # Create a copy, perform set_axis on copy, return copy.
    # Similar to native pandas when copy=True.
    snowpark_series = pd.Series(native_series)
    native_res = native_series.set_axis(labels, axis=axis, copy=True)

    with SqlCounter(query_count=num_queries, join_count=num_joins):
        snowpark_res = snowpark_series.set_axis(labels, axis=axis)
        # Results should be the same for Snowpark and Native pandas.
        assert_snowpark_pandas_equal_to_pandas(snowpark_res, native_res)

        # Should return the copy on which set_axis() was performed.
        assert snowpark_res is not None

        # Results should be different from the Series on which set_axis() was performed.
        with pytest.raises(AssertionError):
            assert_snowpark_pandas_equal_to_pandas(snowpark_res, snowpark_series)


# Invalid input tests for Series.set_axis().
@pytest.mark.parametrize(
    "ser, axis, labels",
    TEST_DATA_FOR_SERIES_SET_AXIS_RAISES_VALUE_ERROR,
)
@sql_count_checker(query_count=0)
def test_set_axis_series_raises_value_error(ser, axis, labels):
    # Should raise a ValueError if invalid scalar axis is provided.
    # Error messages match with native pandas error messages.
    eval_snowpark_pandas_result(
        pd.Series(ser),
        ser,
        lambda _ser: _ser.set_axis(labels, axis=axis),
        expect_exception=True,
        expect_exception_type=ValueError,
    )


@pytest.mark.parametrize(
    "ser, axis, labels, error_msg",
    TEST_DATA_FOR_SERIES_SET_AXIS_RAISES_TYPE_ERROR,
)
@sql_count_checker(query_count=0)
def test_set_axis_series_raises_type_error(ser, axis, labels, error_msg):
    # Should raise a TypeError if invalid axis is provided of an
    # unexpected type/object or labels is a scalar value.
    with pytest.raises(TypeError, match=error_msg):
        pd.Series(ser).set_axis(labels, axis=axis)


@sql_count_checker(query_count=1, join_count=1)
def test_series_set_axis_copy_true(caplog):
    # Test that warning is raised when copy argument is used.
    series = native_pd.Series([1.25])

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        native_res = series.set_axis(["hello"], axis=0, copy=True)
        snowpark_res = pd.Series(series).set_axis(["hello"], axis=0, copy=True)
        assert_snowpark_pandas_equal_to_pandas(snowpark_res, native_res)
        assert "keyword is unused and is ignored." in caplog.text
