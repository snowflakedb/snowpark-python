#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime as dt

import modin.pandas as pd
import numpy as np
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_series_equal,
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    # For numeric data, 5 UNIONs occur because we concat 6 query compilers together:
    # count, mean, std, min, quantiles (0.25, 0.5, 0.75), max
    # For object data, 5 UNIONs occur:
    # - 2 to concat 3 query compilers (count, unique, top/freq)
    # - 1 to NULL-pad the Series to avoid special-case handling top/freq calculation for empty Series
    # - 2 from a UNION ALL + replicated subquery when transposing the top/freq calculation
    "data",
    [
        [1, 10, -1, 20],
        [None, 1.0, 0.8, 0.9, 0, 0.1, 0.2, None],
        # If there are multiple modes, return the first
        ["k", "j", "j", "k"],
        ["y", "y", "y", "z"],
        # Empty series is object by default
        [],
        # Heterogeneous data is considered non-numeric
        [1.1, 2.2, "hello", None],
    ],
)
def test_describe(data):
    with SqlCounter(
        query_count=1,
        union_count=4 if not data or any(isinstance(x, str) for x in data) else 5,
    ):
        eval_snowpark_pandas_result(
            *create_test_series(data), lambda ser: ser.describe()
        )


@pytest.mark.parametrize(
    "percentiles",
    [
        # We concat count, std, mean, min, max, and 1 QC for all percentiles
        # median is automatically added if it is not present already
        [0.1, 0.2, 0.33, 0.432],
        [],
        [0.1],
        [0.5],
    ],
)
@sql_count_checker(query_count=1, union_count=5)
def test_describe_percentiles(percentiles):
    eval_snowpark_pandas_result(
        *create_test_series(list(range(10))), lambda ser: ser.describe(percentiles)
    )


# The include and exclude parameters are completely ignored for Series objects, even if
# they would not be well-formed parameters for Dataframe.describe
@pytest.mark.parametrize(
    "include, exclude",
    [
        (None, None),
        (1, 2),  # Even non-dtype arguments are ignored
        (int, None),
        (
            str,
            None,
        ),  # Specifying string dtypes (instead of object) is invalid for dataframes
        (
            "all",
            [int],
        ),  # Specifying non-None exclude with include="all" is invalid for dataframes
    ],
)
@sql_count_checker(query_count=1, union_count=4)
def test_describe_ignore_include_exclude(include, exclude):
    data = [f"data{i}" for i in range(10)]
    eval_snowpark_pandas_result(
        *create_test_series(data),
        lambda ser: ser.describe(include=include, exclude=exclude),
    )


@pytest.mark.parametrize(
    "data",
    [
        [
            pd.NaT,
            pd.Timestamp("1940-04-25 00:00:01"),
            pd.Timestamp("2000-10-10 20:20:20"),
            pd.Timestamp("2020-12-31 10:00:05"),
        ],
        [
            dt.datetime(year=1900, month=1, day=1, hour=3, minute=4, second=5),
            dt.datetime(year=1940, month=4, day=25, hour=0, minute=0, second=1),
            dt.datetime(year=2000, month=10, day=10, hour=20, minute=20, second=20),
            dt.datetime(year=2020, month=12, day=31, hour=10, minute=0, second=5),
        ],
    ],
)
# Datetime Series have 4 UNIONs for 5 computed statistics.
# (count, mean, min, quantiles, max)
@sql_count_checker(query_count=1, union_count=4)
def test_describe_timestamps(data):
    def timestamp_describe_comparator(snow_res, native_res):
        # atol/rtol arguments of asserters doesn't work for datetimes
        # Snowflake computed mean is very slightly different from pandas
        # (1987-05-13 18:06:48.66666668 vs. 1987-05-13 18:06:48.666000)
        # Perform exact comparison on other rows, and check the delta between means is small
        snow_to_pandas = snow_res.to_pandas()
        # assert_series_equal and assert_allclose are used here instead of assert_snowpark_pandas*
        # helpers so we only call to_pandas() a single time
        assert_series_equal(snow_to_pandas.drop(["mean"]), native_res.drop(["mean"]))
        np.testing.assert_allclose(
            snow_to_pandas.loc["mean"].timestamp(),
            native_res.loc["mean"].timestamp(),
        )

    eval_snowpark_pandas_result(
        *create_test_series(data),
        lambda ser: ser.describe(),
        comparator=timestamp_describe_comparator,
    )


@pytest.mark.parametrize(
    "index",
    [
        pytest.param(None, id="default_index"),
        pytest.param(["one", "two", "three", "four", "five", "six"], id="flat_index"),
        pytest.param(
            [
                np.array(["bar", "bar", "baz", "baz", "foo", "foo"]),
                np.array(["one", "two", "one", "two", "one", "two"]),
            ],
            id="2D_index",
        ),
    ],
)
@pytest.mark.parametrize(
    "data",
    [
        [-1, -3, 1, 14, 0, 100],
        [3.1, 4.1, 5.9, 2.6, 5.3, np.nan],
        [f"data{i}" for i in range(6)],
    ],
    ids=["ints", "floats", "objects"],
)
def test_describe_multiindex(data, index):
    with SqlCounter(
        query_count=1,
        union_count=4 if not data or any(isinstance(x, str) for x in data) else 5,
    ):
        eval_snowpark_pandas_result(
            *create_test_series(data, index=index), lambda ser: ser.describe()
        )


@sql_count_checker(query_count=0)
@pytest.mark.xfail(
    strict=True,
    raises=NotImplementedError,
    reason="requires concat(), which we cannot do with Timedelta.",
)
def test_timedelta(timedelta_native_df):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            timedelta_native_df,
        ),
        lambda df: df["A"].describe(),
    )
