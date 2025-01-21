#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

TEST_RANK_DATA = [
    (
        {
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b"],
            "a": [2, 4, 2, 3, 5, 1, 2, 4, 1],
        },
        None,
    ),
    (
        {
            "group": ["a", np.nan, "a", "a", "a", np.nan, "b", np.nan, "b"],
            "a": [2, 4, 2, 3, 5, 1, 2, 4, 1],
        },
        None,
    ),
    (
        {
            "group": [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ],
            "a": [2, 4, 2, 3, 5, 1, 2, 4, 1],
        },
        None,
    ),
    (
        {
            "group": [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ],
            "a": [np.nan, 4, 2, 3, 5, np.nan, 2, 4, np.nan],
        },
        None,
    ),
    (
        {
            "group": [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ],
            "a": [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ],
        },
        None,
    ),
    (
        {
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b", "b"],
            "a": [2, 4, 2, 3, 5, 1, 2, 4, 1, 5],
            "b": [2, 4, 2, np.nan, 5, 1, 2, np.nan, 1, 5],
        },
        None,
    ),
    (
        {
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b", "b"],
            "a": [2, 4, 2, 3, 5, 1, 2, 4, 1, 5],
            "b": [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ],
        },
        None,
    ),
    (
        {
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b", "b"],
            "a": [
                "cat",
                "penguin",
                "dog",
                "spider",
                "snake",
                "dog",
                "bear",
                "dog",
                "cat",
                "snake",
            ],
        },
        None,
    ),
    (
        {"group": ["a", "a", "b", "a"], "Value": [4, -2, 4, 8]},
        native_pd.MultiIndex.from_arrays(
            [["A", "A", "B", "B"], [1, 2, 1, 2]], names=["Letter", "Number"]
        ),
    ),
]

TEST_RANK_DATA_MUL = [
    (
        {
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b", "b"],
            "a": [2, 1, 2, 3, 3, 1, 2, 0, 1, 0],
            "b": [2, 4, 5, 5, 5, 1, 2, 8, 1, 1],
        },
        None,
    ),
    (
        {
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b", "b"],
            "a": [2, 4, 2, 3, 5, 1, 2, 4, 1, 5],
            "b": [2, 4, 2, np.nan, 5, 1, 2, np.nan, 1, 5],
        },
        None,
    ),
    (
        {
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b", "b"],
            "a": [2, 4, 2, 3, 5, 1, 2, 4, 1, 5],
            "b": [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ],
        },
        None,
    ),
    (
        {
            "group": ["a", "a", np.nan, "a", "a", np.nan, "b", np.nan, "b"],
            "a": [2, 4, 2, 3, 5, 1, 2, 4, 1],
            "b": [2, 4, 2, 3, 5, 4, 2, 4, 1],
        },
        None,
    ),
    (
        {
            "group": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "a": [2, 4, 2, 3, 5, 1],
            "b": [2, 4, 5, 2, 5, 1],
        },
        None,
    ),
    (
        {
            "group": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "a": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "b": [2, 4, 5, 5, 5, 1],
        },
        None,
    ),
    (
        {"group": ["a", "a", "b", "a"], "a": [4, -2, 4, 8], "b": [4, 2, 6, 8]},
        native_pd.MultiIndex.from_arrays(
            [["A", "A", "B", "B"], [1, 2, 1, 2]], names=["Letter", "Number"]
        ),
    ),
]


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("data, index", TEST_RANK_DATA)
@pytest.mark.parametrize(
    "method",
    ["min", "dense", "first", "max", "average"],
)
@pytest.mark.parametrize(
    "ascending",
    [True, False],
)
@pytest.mark.parametrize(
    "na_option",
    ["keep", "top", "bottom"],
)
@pytest.mark.parametrize(
    "dropna",
    [True, False],
)
# test df. groupby rank with all method, na_option, ascending parameter combinations
def test_df_groupby_rank(data, index, method, ascending, na_option, dropna):
    snow_df = pd.DataFrame(data, index=index)
    native_df = native_pd.DataFrame(data, index=index)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby("group", dropna=dropna).rank(
            method=method, na_option=na_option, ascending=ascending
        ),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("data, index", TEST_RANK_DATA)
@pytest.mark.parametrize(
    "method",
    ["min", "dense", "first", "max", "average"],
)
@pytest.mark.parametrize(
    "ascending",
    [True, False],
)
@pytest.mark.parametrize(
    "na_option",
    ["keep", "top", "bottom"],
)
@pytest.mark.parametrize(
    "dropna",
    [True, False],
)
# test df percentile rank
def test_df_rank_pct(data, index, method, ascending, na_option, dropna):
    snow_df = (
        pd.DataFrame(data, index=index)
        .groupby("group", dropna=dropna)
        .rank(method=method, ascending=ascending, na_option=na_option, pct=True)
    )
    native_df = (
        native_pd.DataFrame(data, index=index)
        .groupby("group", dropna=dropna)
        .rank(method=method, ascending=ascending, na_option=na_option, pct=True)
    )
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64(snow_df, native_df)


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("data, index", TEST_RANK_DATA_MUL)
@pytest.mark.parametrize(
    "method",
    ["min", "dense", "first", "max", "average"],
)
@pytest.mark.parametrize(
    "ascending",
    [True, False],
)
@pytest.mark.parametrize(
    "na_option",
    ["keep", "top", "bottom"],
)
# test df. groupby rank with multiple groupby columns and all method, na_option, ascending parameter combinations
def test_df_groupby_rank_by_list(data, index, method, ascending, na_option):
    snow_df = pd.DataFrame(data, index=index)
    native_df = native_pd.DataFrame(data, index=index)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(["group", "a"]).rank(
            method=method, na_option=na_option, ascending=ascending
        ),
    )


@pytest.mark.parametrize(
    "by, level, axis, error",
    [
        # by and level are set
        ("group", 0, 0, NotImplementedError),
        # non-zero level
        (None, -1, 0, NotImplementedError),
        # non-zero axis
        ("group", 0, 1, NotImplementedError),
    ],
)
@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "method",
    ["min", "dense", "first", "max", "average"],
)
@pytest.mark.parametrize(
    "ascending",
    [True, False],
)
@pytest.mark.parametrize(
    "na_option",
    ["keep", "top", "bottom"],
)
@pytest.mark.parametrize(
    "pct",
    [True, False],
)
def test_groupby_rank_negative(
    by, level, axis, error, method, ascending, na_option, pct
):
    pandas_df = native_pd.DataFrame(
        {
            "group": [2, 2, 4, 5, 2, 4],
            "value": [2, 4, 2, 3, 5, 1],
        }
    )
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(error):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(
                by=by,
                level=level,
                axis=axis,
                dropna=True,
                as_index=True,
                sort=True,
                group_keys=True,
            ).rank(method=method, ascending=ascending, na_option=na_option, pct=pct),
        )
