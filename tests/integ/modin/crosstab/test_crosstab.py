#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas


@sql_count_checker(query_count=4, join_count=1)
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


@sql_count_checker(query_count=4, join_count=4, union_count=3)
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


@sql_count_checker(query_count=5, join_count=1)
@pytest.mark.parametrize("normalize", [0, 1, True, "all", "index", "columns"])
def test_normalize(normalize):
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
        a, [b, c], rownames=["a"], colnames=["b", "c"], normalize=normalize
    )
    snow_df = pd.crosstab(
        a, [b, c], rownames=["a"], colnames=["b", "c"], normalize=normalize
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=6, join_count=12, union_count=8)
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


@sql_count_checker(query_count=4, join_count=7)
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


@sql_count_checker(query_count=4, join_count=7)
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
