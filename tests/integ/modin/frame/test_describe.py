#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime as dt

import modin.pandas as pd
import numpy as np
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_series_equal,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import TestFiles


@pytest.mark.parametrize(
    "data",
    [
        {"a": [1, 2, np.nan], "b": [4, 5, 6]},
        {"a": [], "b": []},  # Empty columns are numeric by default
        [[None, -2.4, -3], [4.0, 5.1, -6.7], [7, None, None], [None, None, None]],
        {"a": [1, 2, np.nan]},
    ],
)
# In general, 7 UNIONs occur because we concat 8 query compilers together:
# count, mean, std, min, 0.25, 0.5, 0.75, max
# However, for 1-column frames, we compute all the quantiles in a single query compiler, lowering the
# union count to 5 UNIONs for 6 query compilers
def test_describe_numeric_only(data):
    with SqlCounter(query_count=1, union_count=5 if len(data) == 1 else 7):
        eval_snowpark_pandas_result(*create_test_dfs(data), lambda df: df.describe())


@pytest.mark.parametrize(
    "data, expected_union_count",
    # 2 UNIONs occur to concatenate 3 query compilers together for count, unique, top/freq;
    # The following subquery computes top/freq:
    # - N-1 UNIONs occur for an N-column frame when computing top/freq for each column;
    # - N UNIONs to NULL-pad columns to handle empty frames (1 single UNION ALL operation, but
    #   referenced multiple times as a subquery)
    # The Snowpark pandas implementation of transpose copies this subquery to ensure there is at least one
    # row, and additionally performs one more UNION ALL to include this row.
    # In total, we thus have 2 + 2 * (N - 1 + N) + 1 = 4N + 1 UNIONs for an N-column frame.
    [
        # If there are multiple modes, return the value that appears first
        ({"a": ["k", "j", "j", "k"], "b": ["y", "y", "y", "z"]}, 5),
        # Empty columns are numeric by default (df constructor must explicitly specify object dtype)
        ({"a": [], "b": []}, 5),
        # Heterogeneous data is considered non-numeric
        ({2: ["string", 0, None], -1: [1.1, 2.2, "hello"], 0: [None, None, None]}, 6),
        (
            [
                [None, "quick", None],
                ["fox", "quick", "lazy"],
                ["dog", "dog", "lazy"],
                [None, None, None],
            ],
            6,
        ),
    ],
)
def test_describe_obj_only(data, expected_union_count):
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(
            *create_test_dfs(data, dtype="O"), lambda df: df.describe(include="all")
        )


@pytest.mark.parametrize(
    "dtype, expected_union_count", [(int, 7), (float, 7), (object, 5)]
)
def test_describe_empty_rows(dtype, expected_union_count):
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(
            *create_test_dfs({}, dtype=dtype, columns=["a", "b"]),
            lambda df: df.describe(include="all"),
        )


@sql_count_checker(query_count=0)
def test_describe_empty_cols():
    eval_snowpark_pandas_result(
        *create_test_dfs({}),
        lambda df: df.describe(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Cannot describe a DataFrame without columns",
        assert_exception_equal=True,
    )


@pytest.mark.parametrize(
    "include, exclude, expected_exception, expected_union_count",
    [
        # Neither of include/exclude are specified -- only take numeric columns
        (None, None, None, 7),
        # *** Only include is specified ***
        # Since the result has both numeric and object values, we have
        # 9 UNIONs to concatenate the 10 statistics QCs together
        # (count, unique, top/freq, mean, std, min, 0.25, 0.5, 0.75, max), with
        # 4K-1 UNIONs to compute top/freq for K object-dtype columns (see comment on
        # test_describe_obj_only for reasoning).
        # Since we have K=2 object columns, the result is 9 + (4 * 2 - 1) = 16 UNIONs.
        ([int, object], None, None, 12),
        (np.number, [], None, 7),
        # Including only datetimes has 7 statistics since std is not computed.
        # Since there is only 1 column, all quantiles are computed in a single QC.
        # (count, mean, min, [0.25, 0.5, 0.75], max)
        (np.datetime64, [], None, 4),
        ([int, np.datetime64], [], None, 7),
        # *** Only exclude is specified ***
        (None, [int, object], None, 7),
        # np.datetime64 is not a subtype of np.number, and should still be included
        # There's only one column, so quantiles are computed together.
        (None, [object, "number"], None, 4),
        # Error if all columns get excluded
        (None, [object, "number", np.datetime64], ValueError, 0),
        # When include is "all", exclude must be unspecified
        ("all", ["O"], ValueError, 0),
        ("all", [], ValueError, 0),
        # include and exclude cannot directly overlap
        ([int, "O"], [float, "O"], ValueError, 0),
        # Like select_dtypes, a dtype in include/exclude can be a subtype of a dtype in the other
        ([int, "O"], [float, np.number, np.datetime64], None, 5),
        ("O", None, None, 5),
    ],
)
def test_describe_include_exclude(
    include, exclude, expected_exception, expected_union_count
):
    # When include=None and exclude=None, object columns are dropped
    data = {
        "int_data_1": [-5, -3, 0, 1, 2],
        "float_data_1": [np.nan, 1, 0.3, 200, np.nan],
        "int_data_2": [10, 9, -1, 3, 40],
        "float_data_2": [np.nan, np.nan, np.nan, np.nan, np.nan],
        "object_data_1": ["this", "data", "is", "an", "object"],
        "object_data_2": ["this", "too", "is", "an", "object"],
        "datetime_data": [
            dt.datetime(year=1900, month=1, day=1, hour=3, minute=4, second=5),
            dt.datetime(year=1940, month=4, day=25, hour=0, minute=0, second=1),
            dt.datetime(year=2000, month=10, day=10, hour=20, minute=20, second=20),
            dt.datetime(year=2020, month=12, day=31, hour=10, minute=0, second=5),
            dt.datetime(year=2024, month=2, day=28, hour=19, minute=30, second=50),
        ],
    }
    with SqlCounter(
        query_count=1 if expected_exception is None else 0,
        union_count=expected_union_count,
    ):
        eval_snowpark_pandas_result(
            *create_test_dfs(data),
            lambda df: df.describe(include=include, exclude=exclude),
            expect_exception=expected_exception is not None,
            expect_exception_type=expected_exception,
            assert_exception_equal=expected_exception is not None,
        )


@pytest.mark.parametrize(
    "include, exclude, expected_exception",
    [
        # If all columns are object, then include=None + exclude=None will still include them
        (None, None, None),
        ([int, object], None, None),
        # Error if no columns are selected
        (np.number, [], ValueError),
        (None, [object], ValueError),
    ],
)
def test_describe_include_exclude_obj_only(include, exclude, expected_exception):
    data = {
        "object_data_1": ["this", "data", "is", "an", "object"],
        "object_data_2": ["this", "too", "is", "an", "object"],
    }
    with SqlCounter(
        query_count=1 if expected_exception is None else 0,
        union_count=5 if expected_exception is None else 0,
    ):
        eval_snowpark_pandas_result(
            *create_test_dfs(data),
            lambda df: df.describe(include=include, exclude=exclude),
            expect_exception=expected_exception is not None,
            expect_exception_type=expected_exception,
            assert_exception_equal=expected_exception is not None,
        )


@pytest.mark.parametrize(
    "percentiles, expected_union_count",
    [
        # We concat count, std, mean, min, max, and 1 QC for each percentile
        # median is automatically added if it is not present already
        ([0.1, 0.2, 0.33, 0.432], 9),
        ([], 5),
        ([0.1], 6),
        ([0.5], 5),
    ],
)
def test_describe_percentiles(percentiles, expected_union_count):
    data = {"a": [1, 2, np.nan], "b": [4, 5, 6]}
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(
            *create_test_dfs(data), lambda df: df.describe(percentiles)
        )


# Datetime Series have 6 UNIONs for 7 computed statistics.
# (count, mean, min, 0.25, 0.5, 0.75, max)
@sql_count_checker(query_count=1, union_count=6)
def test_describe_timestamps():
    data = {
        "timestamps": [
            pd.NaT,
            pd.Timestamp("1940-04-25 00:00:01"),
            pd.Timestamp("2000-10-10 20:20:20"),
            pd.Timestamp("2020-12-31 10:00:05"),
        ],
        "datetimes": [
            dt.datetime(year=1900, month=1, day=1, hour=3, minute=4, second=5),
            dt.datetime(year=1940, month=4, day=25, hour=0, minute=0, second=1),
            dt.datetime(year=2000, month=10, day=10, hour=20, minute=20, second=20),
            dt.datetime(year=2020, month=12, day=31, hour=10, minute=0, second=5),
        ],
    }

    def timestamp_describe_comparator(snow_res, native_res):
        # atol/rtol arguments of asserters doesn't work for datetimes
        # Snowflake computed mean is very slightly different from pandas
        # (1987-05-13 18:06:48.66666668 vs. 1987-05-13 18:06:48.666000)
        # Perform exact comparison on other rows, and check the delta between means is small
        snow_to_pandas = snow_res.to_pandas()
        # assert_frame_equal and assert_series_equal are used here instead of assert_snowpark_pandas*
        # helpers so we only call to_pandas() a single time
        assert_frame_equal(snow_to_pandas.drop(["mean"]), native_res.drop(["mean"]))
        assert_series_equal(
            snow_to_pandas.loc["mean"].apply(lambda e: e.timestamp()),
            native_res.loc["mean"].apply(lambda e: e.timestamp()),
            check_exact=False,
        )

    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.describe(),
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
    "columns",
    [
        pytest.param(None, id="default_columns"),
        pytest.param(["one", "two", "three", "four", "five", "six"], id="flat_columns"),
        pytest.param(
            [
                np.array(["bar", "bar", "baz", "baz", "foo", "foo"]),
                np.array(["one", "two", "one", "two", "one", "two"]),
            ],
            id="2D_columns",
        ),
    ],
)
@pytest.mark.parametrize(
    "include, expected_union_count",
    # Don't need to test all permutations of include/exclude with MultiIndex -- this is covered by
    # tests for select_dtypes, as well as other tests in this file
    [
        ("all", 12),
        (np.number, 7),
        (object, 5),
    ],
)
def test_describe_multiindex(index, columns, include, expected_union_count):
    ints = [-1, -3, 1, 14, 0, 100]
    floats = [3.1, 4.1, 5.9, 2.6, 5.3, np.nan]
    objects = [f"data{i}" for i in range(6)]
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(
            # Use two columns of each datatype to make sure all matched columns are selected
            # Need to call list(zip) because otherwise the call to pd.DataFrame will consume the zip
            # before it is passed to native_pd.DataFrame
            *create_test_dfs(
                list(zip(ints, floats, objects, ints, floats, objects)),
                index=index,
                columns=columns,
            ),
            lambda df: df.describe(include=include),
        )


@pytest.mark.parametrize(
    "include, exclude, expected_union_count",
    [
        (None, None, 7),
        ("all", None, 11),
        (np.number, None, 7),
        (None, float, 9),
        (object, None, 4),
        (None, object, 7),
        (int, float, 5),
        (float, int, 5),
    ],
)
def test_describe_duplicate_columns(include, exclude, expected_union_count):
    # Tests describing a 3-column frame where one column is int, one is float, and one is object,
    # and all columns have the same name
    data = [[1, np.nan, "string"], [2, -4.1, "string"]]
    columns = ["col"] * 3
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(
            *create_test_dfs(data, columns=columns),
            lambda df: df.describe(include=include, exclude=exclude),
        )


def test_describe_duplicate_columns_mixed():
    # Test that describing a frame where there are multiple columns (including ones with numeric data
    # but `object` dtype) that share the same label is correct.
    data = [[5, 0, 1.0], [6, 3, 4.0]]

    def helper(df):
        # Convert first column to `object` dtype
        df = df.astype({0: object})
        df.columns = ["a"] * 3
        return df.describe()

    with SqlCounter(query_count=1, union_count=7):
        eval_snowpark_pandas_result(*create_test_dfs(data), lambda df: helper(df))


@sql_count_checker(
    query_count=3,
    union_count=8,
)
# SNOW-1320296 - pd.concat SQL Compilation ambigious __row_position__ issue
def test_describe_object_file(resources_path):
    test_files = TestFiles(resources_path)
    df = pd.read_csv(test_files.test_concat_file1_csv)
    native_df = df.to_pandas()
    eval_snowpark_pandas_result(df, native_df, lambda x: x.describe(include="O"))


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
        lambda df: df.describe(),
    )
