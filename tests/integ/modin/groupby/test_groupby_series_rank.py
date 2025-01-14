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
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b"],
            "a": [np.nan, 4, 2, 3, 5, np.nan, 2, 4, np.nan],
        },
        None,
    ),
    (
        {
            "group": ["a", "a", "a", "a", "a", "b", "b", "b", "b"],
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
]


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
@sql_count_checker(query_count=1)
# test df. groupby rank with all method, na_option, ascending, dropna parameter combinations
def test_series_groupby_rank(data, index, method, ascending, na_option, dropna):
    snow_df = pd.DataFrame(data, index=index)
    native_df = native_pd.DataFrame(data, index=index)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby("group", dropna=dropna)["a"].rank(
            method=method, na_option=na_option, ascending=ascending
        ),
    )


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
@sql_count_checker(query_count=1)
# test df percentile rank
def test_series_rank_pct(data, index, method, ascending, na_option, dropna):
    snow_df = (
        pd.DataFrame(data, index=index)
        .groupby("group", dropna=dropna)["a"]
        .rank(method=method, ascending=ascending, na_option=na_option, pct=True)
    )
    native_df = (
        native_pd.DataFrame(data, index=index)
        .groupby("group", dropna=dropna)["a"]
        .rank(method=method, ascending=ascending, na_option=na_option, pct=True)
    )
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64(snow_df, native_df)


@pytest.mark.parametrize(
    "by, level, axis, error",
    [
        # by and level are set
        ("group", 0, 0, NotImplementedError),
        # non-zero level
        (None, -1, 0, NotImplementedError),
        # non-zero axis
        ("group", 0, 1, ValueError),
    ],
)
@sql_count_checker(query_count=0)
def test_groupby_ser_rank_negative(by, level, axis, error):
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
            )["value"].rank(ascending=True),
        )
