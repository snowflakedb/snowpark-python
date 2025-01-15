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
    ({"a": [1, 2, 2, 2, 3, 3, 3]}, None),
    ({"a": [4, -2, 4, 8, 3]}, None),
    ({"Animal": ["cat", "penguin", "dog", "spider", "snake", "dog", "bear"]}, None),
    ({"a": [1, 2, np.nan, 2, 3, np.nan, 3]}, None),
    ({"a": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]}, None),
    (
        {"Value": [4, -2, 4, 8]},
        native_pd.MultiIndex.from_product(
            [["A", "B"], ["C", "D"]], names=["Index1", "Index2"]
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
# test Series.rank with all method, na_option, ascending parameter combinations
def test_series_rank(data, index, method, ascending, na_option):
    snow_series = pd.Series(data, index=index)
    native_series = native_pd.Series(data, index=index)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda df: df.rank(method=method, ascending=ascending, na_option=na_option),
    )


@sql_count_checker(query_count=1)
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
# test Series.rank numeric_only
def test_series_rank_numeric_only(method, ascending, na_option):
    test_rank_data = {
        "Animal": ["cat", "penguin", "dog", "spider", "snake", "dog", "bear"],
    }
    snow_series = pd.DataFrame(test_rank_data)
    native_series = native_pd.DataFrame(test_rank_data)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda df: df.rank(
            method=method, ascending=ascending, na_option=na_option, numeric_only=True
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
# test Series percentile rank
def test_df_rank_pct(data, index, method, ascending, na_option):
    snow_df = pd.DataFrame(data, index=index).rank(
        method=method, ascending=ascending, na_option=na_option, pct=True
    )
    native_df = native_pd.DataFrame(data, index=index).rank(
        method=method, ascending=ascending, na_option=na_option, pct=True
    )
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64(snow_df, native_df)
