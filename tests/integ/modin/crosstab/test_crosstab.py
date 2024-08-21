#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)


@sql_count_checker(query_count=3)
def test_basic_crosstab():
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = np.array(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        dtype=object,
    )
    c = np.array(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        dtype=object,
    )
    native_df = native_pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])
    snow_df = pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=5, join_count=13)
def test_basic_crosstab_with_series_objs_full_overlap():
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = native_pd.Series(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
    )
    c = native_pd.Series(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
    )
    native_df = native_pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])
    snow_df = pd.crosstab(
        a, [pd.Series(b), pd.Series(c)], rownames=["a"], colnames=["b", "c"]
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=5, join_count=13)
def test_basic_crosstab_with_series_objs_some_overlap():
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = native_pd.Series(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        index=list(range(len(a))),
    )
    c = native_pd.Series(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        index=-1 * np.array(list(range(len(a)))),
    )

    # All columns have to be the same length (if NumPy arrays are present, then
    # pandas errors if they do not match the length of the other Series after
    # they are joined (i.e. filtered so that their indices are the same)). In
    # this test, we truncate the numpy column so that the lengths are correct.
    def eval_func(args_list):
        a, b, c = args_list
        if isinstance(b, native_pd.Series):
            return native_pd.crosstab(
                a[:1], [b, c], rownames=["a"], colnames=["b", "c"]
            )
        else:
            return pd.crosstab(a[:1], [b, c], rownames=["a"], colnames=["b", "c"])

    native_args = [a, b, c]
    snow_args = [a, pd.Series(b), pd.Series(c)]
    eval_snowpark_pandas_result(
        snow_args,
        native_args,
        eval_func,
    )


@sql_count_checker(query_count=2, join_count=1)
def test_basic_crosstab_with_series_objs_some_overlap_error():
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = native_pd.Series(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        index=list(range(len(a))),
    )
    c = native_pd.Series(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        index=-1 * np.array(list(range(len(a)))),
    )

    # All columns have to be the same length (if NumPy arrays are present, then
    # pandas errors if they do not match the length of the other Series after
    # they are joined (i.e. filtered so that their indices are the same))
    def eval_func(args_list):
        a, b, c = args_list
        if isinstance(b, native_pd.Series):
            return native_pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])
        else:
            return pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])

    native_args = [a, b, c]
    snow_args = [a, pd.Series(b), pd.Series(c)]
    eval_snowpark_pandas_result(
        snow_args,
        native_args,
        eval_func,
        expect_exception=True,
        expect_exception_match=re.escape(
            "Length mismatch: Expected 11 rows, received array of length 1"
        ),
        expect_exception_type=ValueError,
        assert_exception_equal=False,  # Our error message is a little different.
    )


@sql_count_checker(query_count=2, join_count=1)
def test_basic_crosstab_with_series_objs_no_overlap_error():
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = native_pd.Series(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        index=list(range(len(a))),
    )
    c = native_pd.Series(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        index=-1 - np.array(list(range(len(a)))),
    )

    # All columns have to be the same length (if NumPy arrays are present, then
    # pandas errors if they do not match the length of the other Series after
    # they are joined (i.e. filtered so that their indices are the same))
    def eval_func(args_list):
        a, b, c = args_list
        if isinstance(b, native_pd.Series):
            return native_pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])
        else:
            return pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])

    native_args = [a, b, c]
    snow_args = [a, pd.Series(b), pd.Series(c)]
    eval_snowpark_pandas_result(
        snow_args,
        native_args,
        eval_func,
        expect_exception=True,
        expect_exception_match=re.escape(
            "Length mismatch: Expected 11 rows, received array of length 0"
        ),
        expect_exception_type=ValueError,
        assert_exception_equal=False,  # Our error message is a little different.
    )


@sql_count_checker(query_count=6, join_count=3)
def test_basic_crosstab_with_df_and_series_objs_pandas_errors():
    a = native_pd.Series(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = native_pd.DataFrame(
        {
            "0": [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            "1": [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
        }
    )
    # pandas expects only Series objects, or DataFrames that have only a single column, while
    # we support accepting DataFrames with multiple columns.
    with pytest.raises(
        AssertionError, match="arrays and names must have the same length"
    ):
        native_pd.crosstab(a, b, rownames=["a"], colnames=["b", "c"])

    def eval_func(args_list):
        a, b = args_list
        if isinstance(a, native_pd.Series):
            return native_pd.crosstab(
                a, [b[c] for c in b.columns], rownames=["a"], colnames=["b", "c"]
            )
        else:
            return pd.crosstab(a, b, rownames=["a"], colnames=["b", "c"])

    native_args = [a, b]
    snow_args = [pd.Series(a), pd.DataFrame(b)]
    eval_snowpark_pandas_result(
        snow_args,
        native_args,
        eval_func,
    )


@sql_count_checker(query_count=3, join_count=3, union_count=3)
def test_margins():
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = np.array(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        dtype=object,
    )
    c = np.array(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        dtype=object,
    )
    native_df = native_pd.crosstab(
        a,
        [b, c],
        rownames=["a"],
        colnames=["b", "c"],
        margins=True,
        margins_name="MARGINS_NAME",
    )
    snow_df = pd.crosstab(
        a,
        [b, c],
        rownames=["a"],
        colnames=["b", "c"],
        margins=True,
        margins_name="MARGINS_NAME",
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@pytest.mark.parametrize("normalize", [0, 1, True, "all", "index", "columns"])
def test_normalize(normalize):
    query_count = 4 if normalize not in (0, "index") else 3
    join_count = 0 if normalize not in (0, "index") else 3
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = np.array(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        dtype=object,
    )
    c = np.array(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        dtype=object,
    )
    with SqlCounter(query_count=query_count, join_count=join_count):
        native_df = native_pd.crosstab(
            a, [b, c], rownames=["a"], colnames=["b", "c"], normalize=normalize
        )
        snow_df = pd.crosstab(
            a, [b, c], rownames=["a"], colnames=["b", "c"], normalize=normalize
        )
        assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=5, join_count=11, union_count=8)
@pytest.mark.parametrize("normalize", [0, 1, True, "all", "index", "columns"])
def test_normalize_and_margins(normalize):
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = np.array(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        dtype=object,
    )
    c = np.array(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        dtype=object,
    )
    native_df = native_pd.crosstab(
        a,
        [b, c],
        rownames=["a"],
        colnames=["b", "c"],
        normalize=normalize,
        margins=True,
    )
    snow_df = pd.crosstab(
        a,
        [b, c],
        rownames=["a"],
        colnames=["b", "c"],
        normalize=normalize,
        margins=True,
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=3, join_count=6)
@pytest.mark.parametrize("aggfunc", ["mean", "sum"])
def test_values(aggfunc):
    native_df = native_pd.DataFrame(
        {
            "species": ["dog", "cat", "dog", "dog", "cat", "cat", "dog", "cat"],
            "favorite_food": [
                "chicken",
                "fish",
                "fish",
                "beef",
                "chicken",
                "beef",
                "fish",
                "beef",
            ],
            "age": [7, 2, 8, 5, 9, 3, 6, 1],
        }
    )
    native_df_result = native_pd.crosstab(
        native_df["species"].values,
        native_df["favorite_food"].values,
        values=native_df["age"].values,
        aggfunc=aggfunc,
    )
    snow_df = pd.crosstab(
        native_df["species"].values,
        native_df["favorite_food"].values,
        values=native_df["age"].values,
        aggfunc=aggfunc,
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df_result)


@sql_count_checker(
    query_count=9,
    join_count=10,
)
@pytest.mark.parametrize("aggfunc", ["mean", "sum"])
def test_values_series_like(aggfunc):
    native_df = native_pd.DataFrame(
        {
            "species": ["dog", "cat", "dog", "dog", "cat", "cat", "dog", "cat"],
            "favorite_food": [
                "chicken",
                "fish",
                "fish",
                "beef",
                "chicken",
                "beef",
                "fish",
                "beef",
            ],
            "age": [7, 2, 8, 5, 9, 3, 6, 1],
        }
    )
    snow_df = pd.DataFrame(native_df)
    native_df = native_pd.crosstab(
        native_df["species"],
        native_df["favorite_food"],
        values=native_df["age"],
        aggfunc=aggfunc,
    )
    snow_df = pd.crosstab(
        snow_df["species"],
        snow_df["favorite_food"],
        values=snow_df["age"],
        aggfunc=aggfunc,
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)
