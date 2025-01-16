#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from tests.integ.modin.utils import (
    VALID_PANDAS_LABELS,
    assert_index_equal,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def assert_axes_result_equal(snow_res, pd_res):
    assert len(snow_res) == len(pd_res)
    # exact is set to False so that we only compare the values within the index,
    # not the dtype or class of the Index type.
    # Unlike pandas, Snowpark pandas never uses RangeIndex objects, and instead will use
    # the generic Index object, such as `Index([0, 1, 2], dtype="int8")` in place of
    # `RangeIndex(start=0, stop=3, step=1)`.
    # Furthermore, the Snowflake query result uses smaller dtypes when possible; that is,
    # a 128-element index such as `pd.Series([0] * 128).index` will have dtype int8, while
    # a 129-element index such as `pd.Series([0] * 129).index` will have dtype int16.
    assert_index_equal(snow_res[0], pd_res[0], exact=False)
    assert_index_equal(snow_res[1], pd_res[1], exact=False)


test_dfs = [
    native_pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),  # set column names with dict
    native_pd.DataFrame([(1, 3), (2, 4)]),  # without column names
    native_pd.DataFrame(
        [(1, 3), (2, 4)], columns=["a", "b"]
    ),  # set column names using columns=
    native_pd.DataFrame([(1, 3), (2, 4)], columns=["a", "b"]).set_index(
        "a"
    ),  # with index
    native_pd.DataFrame(
        [(1, 3), (2, 4)], columns=[("A", "a"), ("B", "b")]
    ),  # column names are tuples
]


@pytest.mark.parametrize("test_df", test_dfs)
@sql_count_checker(query_count=1)
def test_axes(test_df):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_df),
        test_df,
        lambda df: df.axes,
        comparator=assert_axes_result_equal,
    )


@pytest.mark.parametrize("test_df", test_dfs)
@sql_count_checker(query_count=1)
def test_index(test_df):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_df),
        test_df,
        lambda df: df.index,
        comparator=assert_index_equal,
        # exact is set to False so that we only compare the values within the index,
        # not the dtype or class of the Index type.
        # Unlike pandas, Snowpark pandas never uses RangeIndex objects, and instead will use
        # the generic Index object, such as `Index([0, 1, 2], dtype="int8")` in place of
        # `RangeIndex(start=0, stop=3, step=1)`.
        # Furthermore, the Snowflake query result uses smaller dtypes when possible; that is,
        # a 128-element index such as `pd.Series([0] * 128).index` will have dtype int8, while
        # a 129-element index such as `pd.Series([0] * 129).index` will have dtype int16.
        exact=False,
    )


@pytest.mark.parametrize("test_df", test_dfs)
@sql_count_checker(query_count=3, join_count=3)
def test_set_and_assign_index(test_df):
    def assign_index(df, keys):
        df.index = keys
        return df.index

    def set_index(df, keys):
        df.set_index(keys)
        return df.index

    new_native_index = native_pd.Index(np.random.rand(len(test_df)))
    new_index = pd.Index(new_native_index)
    eval_snowpark_pandas_result(
        pd.DataFrame(test_df),
        test_df,
        lambda df: assign_index(df, new_native_index)
        if isinstance(df, native_pd.DataFrame)
        else assign_index(df, new_index),
        comparator=assert_index_equal,
    )

    eval_snowpark_pandas_result(
        pd.DataFrame(test_df),
        test_df,
        lambda df: set_index(df, df.columns[0]),
        comparator=assert_index_equal,
    )

    new_mi = pd.MultiIndex.from_arrays(
        [np.random.rand(len(test_df)), np.random.rand(len(test_df))],
        names=["mi1", "mi2"],
    )
    eval_snowpark_pandas_result(
        pd.DataFrame(test_df),
        test_df,
        lambda df: assign_index(df, new_mi),
        comparator=assert_index_equal,
    )


@pytest.mark.parametrize("test_df", test_dfs)
@sql_count_checker(query_count=0)
def test_columns(test_df):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_df),
        test_df,
        lambda df: df.columns,
        comparator=assert_index_equal,
    )


def set_columns_func(df, labels):
    df.columns = labels
    return df.columns


@pytest.mark.parametrize(
    "columns",
    [
        ["a", "b"],
        [1.3, 2],
        native_pd.Index([1.3, 2]),
        [None, int],
        [(42, "test"), (1, 2, 3)],
        native_pd.Index(["a", "b"]),
        [("A",), ("B",)],
        [("A", "a", 1), ("B", "b", 1)],
        [["A", "a"], ["B", "b"]],
        native_pd.MultiIndex.from_tuples([("A", "a"), ("B", "b")]),
    ],
)
@sql_count_checker(query_count=0)
def test_set_columns(columns):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_dfs[0].copy()),
        test_dfs[0].copy(),
        lambda df: set_columns_func(df, columns),
        comparator=assert_index_equal,
    )


@pytest.mark.parametrize("col_name", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=0)
def test_set_columns_valid_names(col_name):
    test_df = native_pd.DataFrame({col_name: [1, 2, 3]})
    # test valid column labels in from_pandas
    # SNOW-823379 need multiindex as df.columns support for tuple column name
    if type(col_name) is not tuple:
        eval_snowpark_pandas_result(
            pd.DataFrame(test_df),
            test_df,
            lambda df: df.columns,
            comparator=assert_index_equal,
        )
    # test set valid column labels
    test_df = native_pd.DataFrame([1, 2, 3])
    eval_snowpark_pandas_result(
        pd.DataFrame(test_df),
        test_df,
        lambda df: set_columns_func(df, labels=[col_name]),
        comparator=assert_index_equal,
    )


@pytest.mark.parametrize(
    "columns, error_type, error_msg",
    [
        (
            "a",
            TypeError,
            "must be called with a collection of some kind, 'a' was passed",
        ),
        (
            ["a"],
            ValueError,
            "Length mismatch: Expected axis has 2 elements, new values have 1 elements",
        ),
        (
            native_pd.Index(["a"]),
            ValueError,
            "Length mismatch: Expected axis has 2 elements, new values have 1 elements",
        ),
        (
            ["a", "b", "c"],
            ValueError,
            "Length mismatch: Expected axis has 2 elements, new values have 3 elements",
        ),
        (
            [["A", "a", 1], ["B", "b", 1]],
            ValueError,
            "Length mismatch: Expected axis has 2 elements, new values have 3 elements",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_set_columns_negative(columns, error_type, error_msg):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_dfs[0]),
        test_dfs[0],
        lambda df: set_columns_func(df, labels=columns),
        comparator=assert_index_equal,
        expect_exception=True,
        expect_exception_type=error_type,
        expect_exception_match=error_msg,
    )


@pytest.mark.parametrize("index_name", VALID_PANDAS_LABELS)
# one query to convert the modin index to pandas index to set columns
@sql_count_checker(query_count=1)
def test_set_columns_index_name(index_name):
    snow_columns = pd.Index(["a", "b"], name=index_name)
    native_columns = native_pd.Index(["a", "b"], name=index_name)
    eval_snowpark_pandas_result(
        pd.DataFrame(test_dfs[0]),
        test_dfs[0],
        lambda df: set_columns_func(
            df, labels=snow_columns if isinstance(df, pd.DataFrame) else native_columns
        ),
        comparator=assert_index_equal,
    )


@sql_count_checker(query_count=1)
def test_duplicate_labels_assignment():
    # Duplicate data labels
    snow_df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    snow_df.columns = ["a", "a", "A"]
    assert snow_df.columns.tolist() == ["a", "a", "A"]

    # Duplicate between index and data label
    snow_df = pd.DataFrame(
        {"b": [1, 2]}, index=native_pd.RangeIndex(start=4, stop=6, step=1, name="a")
    )
    snow_df.columns = ["a"]
    assert snow_df.columns.tolist() == ["a"]
    assert snow_df.index.name == "a"

    # Duplicate index labels
    snow_df = pd.DataFrame(
        {"z": [1, 2]},
        index=pd.MultiIndex.from_arrays([["u", "v"], ["x", "y"]], names=("a", "a")),
    )
    assert snow_df.index.names == ["a", "a"]


# Valid data
# ----------
# This data covers the happy cases for DataFrame.set_axis().
# "index", "rows", "columns", 0, and 1 are valid axis values and are tested on different DataFrame objects
# with valid labels. The valid labels include strings with different quotation marks, None values, numbers,
# Index, and MultiIndex objects.
# Format: df, axis, labels, and number of SQL queries for set_axis() that creates a copy
# number of queries for set_axis() on self = number of queries for set_axis() on copy
# increased query counts to convert to
TEST_DATA_FOR_DF_SET_AXIS = [
    # Set rows.
    # TODO: uncomment test case when SNOW-933782 is fixed.
    # [
    #     native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}),
    #     0,
    #     native_pd.DataFrame(["'a'", "" "b" "", "c"]),
    #     5,
    # ],
    [
        native_pd.DataFrame({"A": [3.14, 1.414, 1.732], "B": [9.8, 1.0, 0]}),
        "rows",
        [None] * 3,
        3,
        2,
    ],
    [  # Labels is a MultiIndex from tuples.
        native_pd.DataFrame({"A": [1, 2, 3], -2515 / 135: [4, 5, 6]}),
        "index",
        native_pd.MultiIndex.from_tuples(
            [("r0", "rA", "rR"), ("r1", "rB", "rS"), ("r2", "rC", "rT")],
            names=["Courses", "Fee", "Random"],
        ),
        3,
        6,
    ],
    [
        native_pd.DataFrame({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        0,
        {1: "c", 2: "b", 3: "a"},
        3,
        2,
    ],
    [
        native_pd.DataFrame(
            index=native_pd.MultiIndex.from_tuples(
                [("r0", "rA"), ("r1", "rB")], names=["Courses", "Fee"]
            ),
            columns=native_pd.MultiIndex.from_tuples(
                [
                    ("Gasoline", "Toyota"),
                    ("Gasoline", "Ford"),
                    ("Electric", "Tesla"),
                    ("Electric", "Nio"),
                ]
            ),
            data=[[100, 300, 900, 400], [200, 500, 300, 600]],
        ),
        0,
        ['"row 1"', "row 2"],
        3,
        2,
    ],
    [
        native_pd.DataFrame(
            index=native_pd.MultiIndex.from_arrays(
                [[None] * 10, [None] * 10, [None] * 10], names=(None, None, None)
            ),
            columns=native_pd.MultiIndex.from_tuples([(None, None)] * 5),
            data=[[None] * 5] * 10,
        ),
        "rows",
        list(range(10)),
        3,
        2,
    ],
    [
        native_pd.DataFrame(
            index=native_pd.MultiIndex.from_frame(
                native_pd.DataFrame(
                    [
                        ["HI", "Temp"],
                        ["HI", "Precip"],
                        ["NJ", "Temp"],
                        ["NJ", "Precip"],
                    ],
                    columns=["a", "b"],
                )
            ),
            columns=["column0", "column2", "column4", "column6"],
            data=[
                [0, 1, 2, 3],
                [4, 5, 6, 7],
                [8, 9, 10, 11],
                [12, 13, 14, 15],
            ],
        ),
        "index",
        native_pd.MultiIndex.from_product(
            [["NJ", "CA"], ["temp", "precip"]], names=["number", "color"]
        ),
        3,
        4,
    ],
    # Set columns.
    [
        native_pd.DataFrame(
            {
                "A": ["foo", "bar", 3],
                "B": [4, "" "baz" "", 6],
                "C": [4, "baz", "foo"],
                "D": [4, 5, None],
            }
        ),
        "columns",
        ["index"] * 4,
        3,
        6,
    ],
    [
        native_pd.DataFrame(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "" "C" "": [4, 5, 6],
                "'D'": [7, 8, 9],
                -0.0123: [-1, -2, -3],
            }
        ),
        1,
        {99, 999, 9999, 99999, 999999},
        3,
        6,
    ],
    [
        native_pd.DataFrame({1: [1, 11, 111], 2: [2, 22, 222], 9: [9, 99, 999]}),
        1,
        native_pd.Index(["0", "00", "000"]),
        3,
        6,
    ],
    [  # Labels is a MultiIndex from arrays.
        native_pd.DataFrame(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "'C'": [4, 5, 6],
                '"D"': [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        "columns",
        native_pd.MultiIndex.from_arrays(
            [
                ["genmaicha", "peppermint", "jasmine", "spice", "earl grey"],
                [5, 2.5, 6, 1, 3.5],
                ["mild", "none", "mild", "high", "high"],
            ],
            names=("tea", "steep time", "caffeine"),
        ),
        3,
        6,
    ],
    [
        native_pd.DataFrame(
            index=list(range(3)),
            columns=native_pd.MultiIndex.from_product(
                [[0, 1, 2], ["green", "purple"]], names=["number", "color"]
            ),
            data=[
                ["cyan", "pink", "blue", "brown", "violet", "gold"],
                ["brown", "violet", "gold", "silver", "white", "grey"],
                ["silver", "white", "grey", "cyan", "pink", "blue"],
            ],
        ),
        1,
        list(range(6)),
        3,
        6,
    ],
    [
        native_pd.DataFrame(
            index=native_pd.MultiIndex.from_tuples(
                [("r0", "rA"), ("r1", "rB")], names=["Courses", "Fee"]
            ),
            columns=native_pd.MultiIndex.from_tuples(
                [
                    ("Gasoline", "Toyota"),
                    ("Gasoline", "Ford"),
                    ("Electric", "Tesla"),
                    ("Electric", "Nio"),
                ]
            ),
            data=[[100, 300, 900, 400], [200, 500, 300, 600]],
        ),
        "columns",
        native_pd.MultiIndex.from_arrays(
            [
                ["genmaicha", "spice", "green", "earl grey"],
                ["mild", "none", "high", "moderate"],
            ],
            names=("tea", "caffeine"),
        ),
        3,
        6,
    ],
]


# Invalid data which raises ValueError identical to native pandas
# ---------------------------------------------------------------
# Format: df, invalid axis, and invalid labels.
# - This data cover the negative case for DataFrame.set_axis() with invalid axis and labels.
# - Invalid axis values here consist of strings other than "index", "rows", and "columns",
#   numbers other than 0 and 1, empty lists, None values.
# - Invalid labels here consist of: passing None, too many values, too few values, empty list,
# - Index, and MultiIndex objects as invalid labels for row-like axis.
TEST_DATA_FOR_DF_SET_AXIS_RAISES_VALUE_ERROR = [
    # invalid axis values.
    [
        native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}),
        "indexes",  # invalid axis
        ["a", "b", "c"],
    ],
    [
        native_pd.DataFrame({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        "column",  # invalid axis
        ["I", "II"],
    ],
    [
        native_pd.DataFrame(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "C": [4, 5, 6],
                "D": [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        -1,  # invalid axis
        {99, 999, 9999, 99999, 999999},
    ],
    [
        native_pd.DataFrame({"A": [3.14, 1.414, 1.732], -0.9: [9.8, 1.0, 0]}),
        None,  # invalid axis
        [0] * 3,
    ],
    [
        native_pd.DataFrame({-3.14: [3.14, 0, None], "B": [9.8, 1.0, 0]}),
        0.000001,  # invalid axis
        [0] * 3,
    ],
    [
        native_pd.DataFrame(
            {"A": ["a", "b", "c", "d", "e"], "B": ["e", "d", "c", "b", "a"]}
        ),
        -0.000001,  # invalid axis
        native_pd.Index([11, 111, 1111, 11111, 111111]),
    ],
    [  # Index is a MultiIndex from tuples.
        native_pd.DataFrame({"A": [1, 2, 3], -2515 / 135: [4, 5, 6]}),
        -9999999999,  # invalid axis
        native_pd.MultiIndex.from_tuples(
            [("r0", "rA", "rR"), ("r1", "rB", "rS"), ("r2", "rC", "rT")],
            names=["Courses", "Fee", "Random"],
        ),
    ],
    [  # Labels is a MultiIndex from arrays.
        native_pd.DataFrame(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "'C'": [4, 5, 6],
                '"D"': [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        99999999,  # invalid axis
        native_pd.MultiIndex.from_arrays(
            [
                ["genmaicha", "peppermint", "jasmine", "spice", "earl grey"],
                [5, 2.5, 6, 1, 3.5],
                ["mild", "none", "mild", "high", "high"],
            ],
            names=("tea", "steep time", "caffeine"),
        ),
    ],
    # invalid column labels.
    [
        native_pd.DataFrame(
            {
                "A": ["foo", "bar", 3],
                "B": [4, "baz", 6],
                "C": [4, "baz", "foo"],
                "D": [4, 5, None],
            }
        ),
        1,
        ["'index'"] * 5,  # too many labels
    ],
    [
        native_pd.DataFrame({1: [1, 11, 111], 2: [2, 22, 222], 9: [9, 99, 999]}),
        "columns",
        native_pd.Index([]),  # too few labels
    ],
    [  # Labels is a MultiIndex from arrays.
        native_pd.DataFrame(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "'C'": [4, 5, 6],
                '"D"': [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        "columns",
        native_pd.MultiIndex.from_arrays(
            [
                [],
                [],
                [],
            ],
            names=("tea", "steep time", "caffeine"),
        ),  # too few labels
    ],
    [  # Labels is a MultiIndex from a product.
        native_pd.DataFrame({1: [1], 2: [2], 3: [3], 4: [4], 5: [5], 6: [6]}),
        1,
        native_pd.MultiIndex.from_product(
            [[0, 1, 2], ["green", "purple", "pink", "blue"]], names=["number", "color"]
        ),  # too many labels
    ],
]


# Invalid data which raises TypeError different from native pandas
# ----------------------------------------------------------------
# Format: df, invalid axis, invalid labels, and error message.
# - Invalid axis as list, Series, DataFrame, Index, and MultiIndex objects (Causes TypeError).
# - Scalar values as labels.
TEST_DATA_FOR_DF_SET_AXIS_RAISES_TYPE_ERROR = [
    [
        native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}),
        [],  # invalid axis
        ["a", "b", "c"],
        "list is not a valid type for axis.",
    ],
    [
        native_pd.DataFrame({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        [0],  # invalid axis
        ["I", "II"],
        "list is not a valid type for axis.",
    ],
    [
        native_pd.DataFrame(
            {
                "A": ["foo", "bar", 3],
                "B": [4, "baz", 6],
                "C": [4, "baz", "foo"],
                "D": [4, 5, None],
            }
        ),
        native_pd.Index([]),  # invalid axis
        ["index"] * 4,
        "Index is not a valid type for axis.",
    ],
    [
        native_pd.DataFrame(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "C": [4, 5, 6],
                "D": [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),
        native_pd.DataFrame(
            {
                "A": [None] * 3,
                "B": [4, 5, 6],
                "C": [4, 5, 6],
                "D": [7, 8, 9],
                "E": [-1, -2, -3],
            }
        ),  # invalid axis
        [99, 999, 9999, 99999, 999999],
        "DataFrame is not a valid type for axis.",
    ],
    [
        native_pd.DataFrame(
            {"echo": ["echo", "echo"], "not an echo": [". . .", ". . ."]}
        ),
        native_pd.Series(["0"]),  # invalid axis
        native_pd.Index([11, 111]),
        "Series is not a valid type for axis.",
    ],
    [  # Labels is a MultiIndex from tuples.
        native_pd.DataFrame({"A": [1, 2, 3], -2515 / 135: [4, 5, 6]}),
        native_pd.MultiIndex.from_tuples([], names=["Courses", "Fee"]),  # invalid axis
        native_pd.MultiIndex.from_tuples(
            [("r0", "rA", "rR"), ("r1", "rB", "rS"), ("r2", "rC", "rT")],
            names=["Courses", "Fee", "Random"],
        ),
        "MultiIndex is not a valid type for axis.",
    ],
    [  # Labels is a MultiIndex from a product.
        native_pd.DataFrame({1: [1], 2: [2], 3: [3], 4: [4], 5: [5], 6: [6]}),
        native_pd.MultiIndex.from_product(
            [[0, 1, 2], ["green", "purple"]], names=["number", "color"]
        ),  # invalid axis
        native_pd.MultiIndex.from_product(
            [[0, 1, 2], ["green", "purple"]], names=["number", "color"]
        ),
        "MultiIndex is not a valid type for axis.",
    ],
    # labels are None.
    [
        native_pd.DataFrame({"A": [], "B": []}),
        "index",
        None,  # invalid labels,
        "None is not a valid value for the parameter 'labels'.",
    ],
    [
        native_pd.DataFrame({1 / 2: ["foo", "bar", 3], "B": [4, "baz", 6]}),
        "columns",
        None,  # invalid labels,
        "None is not a valid value for the parameter 'labels'.",
    ],
    [
        native_pd.DataFrame({"A": ["foo", "bar", 3], "B": [4, "baz", 6]}),
        "rows",
        1,  # invalid labels type
        re.escape(
            "Index(...) must be called with a collection of some kind, 1 was passed"
        ),
    ],
    [
        native_pd.DataFrame(
            {
                "A": ["foo", "bar", 3],
                "B": [4, "baz", 6],
                "C": [4, "baz", "foo"],
                "D": [4, 5, None],
            }
        ),
        0,
        ...,  # invalid labels type
        re.escape(
            "Index(...) must be called with a collection of some kind, Ellipsis was passed"
        ),
    ],
]


@pytest.mark.parametrize(
    "native_df, axis, labels, num_queries, num_joins", TEST_DATA_FOR_DF_SET_AXIS
)
def test_set_axis_df_copy(native_df, axis, labels, num_queries, num_joins):
    # Create a copy, perform set_axis on copy, return copy.
    # Similar to native pandas when copy=True.
    snowpark_df = pd.DataFrame(native_df)
    native_res = native_df.set_axis(labels, axis=axis, copy=True)
    labels = try_convert_index_to_native(labels)
    if axis in ["columns", 1]:
        num_joins = 0

    with SqlCounter(query_count=num_queries, join_count=num_joins):
        snowpark_res = snowpark_df.set_axis(labels, axis=axis)

        # Should return the copy on which set_axis() was performed.
        assert snowpark_res is not None

        # Results should be the same for Snowpark and Native pandas.
        assert_axes_result_equal(snowpark_res.axes, native_res.axes)

        # Results should be different from the DataFrame on which set_axis() was performed.
        with pytest.raises(AssertionError):
            assert_axes_result_equal(snowpark_res.axes, snowpark_df.axes)


# Invalid input tests for DataFrame.set_axis().
@pytest.mark.parametrize(
    "native_df, axis, labels", TEST_DATA_FOR_DF_SET_AXIS_RAISES_VALUE_ERROR
)
@sql_count_checker(query_count=0)
def test_set_axis_df_raises_value_error(native_df, axis, labels):
    # Should raise a ValueError if invalid axis is provided of the expected type
    # or if the labels for column-like axis are invalid.
    # The error messages match native pandas.
    snowpark_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df.set_axis(labels, axis=axis),
        expect_exception=True,
        expect_exception_type=ValueError,
    )


@pytest.mark.parametrize(
    "native_df, axis, labels, error_msg", TEST_DATA_FOR_DF_SET_AXIS_RAISES_TYPE_ERROR
)
@sql_count_checker(query_count=0)
def test_set_axis_df_raises_type_error_diff_error_msg(
    native_df, axis, labels, error_msg
):
    # Should raise a TypeError if invalid axis is provided of an unexpected type/object
    # or labels passed in are None. The error messages do not match native pandas.
    with pytest.raises(TypeError, match=error_msg):
        pd.DataFrame(native_df).set_axis(labels, axis=axis)


@sql_count_checker(query_count=1, join_count=1)
def test_df_set_axis_copy_true(caplog):
    # Test that warning is raised when copy argument is used.
    native_df = native_pd.DataFrame({"A": [1.25], "B": [3]})
    snowpark_df = pd.DataFrame(native_df)

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            lambda df: df.set_axis(["hello"], axis=0, copy=True),
        )
        assert "keyword is unused and is ignored." in caplog.text


@sql_count_checker(query_count=1)
def test_df_set_axis_copy_false(caplog):
    # Test that warning is raised when copy argument is used.
    native_df = native_pd.DataFrame({"A": [1.25], "B": [3]})
    snowpark_df = pd.DataFrame(native_df)

    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        eval_snowpark_pandas_result(
            snowpark_df, native_df, lambda df: df.set_axis([12, 35], axis=1, copy=False)
        )
        assert "keyword is unused and is ignored." in caplog.text


def test_df_set_axis_with_quoted_index():
    # reported as bug in https://snowflakecomputing.atlassian.net/browse/SNOW-933782
    data = {"A": [1, 2, 3], "B": [4, 5, 6]}
    labels = ["'a'", '" "b" "', '""c""']

    helper = lambda df: df.set_axis(labels, axis=0)  # noqa: E731

    # check first that operation result is the same
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(snow_df, native_df, helper)

    # then, explicitly compare axes
    with SqlCounter(query_count=0):
        ans = helper(snow_df)

    native_ans = helper(native_df)

    with SqlCounter(query_count=1):
        assert_axes_result_equal(ans.axes, native_ans.axes)

    assert list(native_ans.index) == labels
    # extra query for tolist
    with SqlCounter(query_count=2):
        assert list(ans.index) == labels
