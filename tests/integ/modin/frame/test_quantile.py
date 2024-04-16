#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result

TEST_QUANTILE_DATA = {
    "dates": [
        pd.NaT,
        pd.Timestamp("1940-04-25"),
        pd.Timestamp("2000-10-10"),
        pd.Timestamp("2020-12-31"),
    ],
    "a": [0.1, -10.0, 100, 9.2],
    "b": [-5, -2, -1, 0],
    "c": [89, np.nan, -540, 0.1],
    "d": [0, 0, 0, 0],
}

TEST_QUANTILES = [
    0.1,
    [0.1, 0.2, 0.8],
    [0.2, 0.8, 0.1],  # output will not be sorted by quantile
]


@pytest.mark.parametrize("q", TEST_QUANTILES)
@pytest.mark.parametrize("interpolation", ["linear", "nearest"])
def test_quantile_basic(q, interpolation):
    snow_df = pd.DataFrame(TEST_QUANTILE_DATA)
    native_df = native_pd.DataFrame(TEST_QUANTILE_DATA)
    expected_query_count = 2 if isinstance(q, list) else 0

    with SqlCounter(query_count=1, union_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.quantile(q, numeric_only=True),
        )


@sql_count_checker(query_count=1)
def test_quantile_empty_args():
    # by default, returns the median (q=0.5)
    snow_df = pd.DataFrame(TEST_QUANTILE_DATA)
    native_df = native_pd.DataFrame(TEST_QUANTILE_DATA)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.quantile(numeric_only=True),
    )


@pytest.mark.parametrize("q", TEST_QUANTILES)
@sql_count_checker(query_count=1)
def test_quantile_empty_df(q):
    # df.quantile() where df is empty should still have the correct columns
    snow_df = pd.DataFrame([], columns=["c", "b", "a"], dtype=int)
    native_df = native_pd.DataFrame([], columns=["c", "b", "a"], dtype=int)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.quantile())


@pytest.mark.parametrize("q", TEST_QUANTILES)
@sql_count_checker(query_count=1)
def test_quantile_nones(q):
    snow_df = pd.DataFrame([None] * 4, dtype=float)
    native_df = native_pd.DataFrame([None] * 4, dtype=float)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.quantile())


@pytest.mark.parametrize(
    "q",
    [
        native_pd.Index([0.1, 0.2, 0.3, 0.8]),
        np.array([0.1, 0.2, 0.3, 0.8]),
    ],
)
@sql_count_checker(query_count=1)
def expected_query_count(q):
    snow_df = pd.DataFrame(TEST_QUANTILE_DATA)
    native_df = native_pd.DataFrame(TEST_QUANTILE_DATA)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.quantile(q, numeric_only=True),
    )


@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize("interpolation", ["lower", "higher", "midpoint"])
@pytest.mark.parametrize("method", ["table"])
@sql_count_checker(query_count=0)
def test_quantile_unsupported_args_negative(axis, interpolation, method):
    snow_df = pd.DataFrame(TEST_QUANTILE_DATA)
    with pytest.raises(NotImplementedError):
        snow_df.quantile(
            axis=axis, interpolation=interpolation, method=method, numeric_only=True
        ),


@sql_count_checker(query_count=0)
def test_quantile_datetime_negative():
    # Snowflake PERCENTILE_* functions do not operate on datetimes, so it should fail
    snow_df = pd.DataFrame(TEST_QUANTILE_DATA)
    with pytest.raises(NotImplementedError):
        snow_df.quantile(numeric_only=False)
