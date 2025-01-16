#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime as dt
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

SELECT_DTYPES_DATA = {
    "ints": [1, 2, 3, 4],
    "floats": [1.0, np.nan, 3.0, 4.0],
    "pd timestamps": [
        pd.NaT,
        pd.Timestamp("1940-04-25"),
        pd.Timestamp("2000-10-10"),
        pd.Timestamp("2020-12-31"),
    ],
    "np datetimes": [
        np.datetime64("1900-01-01"),
        np.datetime64("1940-04-25"),
        np.datetime64("2000-10-10"),
        np.datetime64("2020-12-31"),
    ],
    "python datetimes": [
        dt.datetime(year=1940, month=4, day=25, hour=1),
        dt.datetime(year=2040, month=10, day=19),
        dt.datetime(year=2020, month=12, day=5, hour=1, second=4),
        dt.datetime(year=2000, month=3, day=7, hour=1, minute=10),
    ],
    "variant": [{}, {"k": "v"}, [], "s"],
    "strings (object dtype)": ["one", "two", "three", "ten"],
    "also ints": [5, 6, 7, 8],
    # Snowpandas ignores the explicit np.int32 constructor, and instead will always return a column
    # with int64 dtype (see TypeMapper::to_snowflake in type_utils.py).
    "explicit int32": list(map(np.int32, [9, 10, 11, 12])),
    "explicit int64": list(map(np.int64, [13, 14, 15, 16])),
    "bools": [True, False, True, True],
    "timedelta": [pd.Timedelta(1), pd.Timedelta(2), pd.Timedelta(3), pd.Timedelta(4)],
}

INCLUDE_EXCLUDE_OPTIONS = [
    (["number"], ["float"]),  # Select only non-float numeric columns
    (np.number, None),  # Select only numeric columns
    ([], "O"),  # Select only numeric columns
    ("object", []),  # Select non-numeric columns
    ([], "number"),  # Select non-numeric columns
    ([int], None),
    (float, None),
    ([np.datetime64, bool], None),
    (None, [np.datetime64, bool]),
    # exclude takes precedence when subtype relation causes overlaps
    ([np.datetime64, bool], object),
    (object, [np.datetime64, bool]),
    (int, np.number),
    (np.number, int),
    param("timedelta64[ns]", None, id="include_timedelta64_ns"),
    param("timedelta64", None, id="include_timedelta64"),
    param(None, "timedelta64[ns]", id="exclude_timedelta64_ns"),
    param(None, "timedelta64", id="exclude_timedelta64"),
]


@pytest.mark.parametrize("include, exclude", INCLUDE_EXCLUDE_OPTIONS)
@sql_count_checker(query_count=1)
def test_select_dtypes_basic(include, exclude):
    snow_df = pd.DataFrame(SELECT_DTYPES_DATA)
    native_df = native_pd.DataFrame(SELECT_DTYPES_DATA)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.select_dtypes(include, exclude),
    )


# Snowpandas coerces all int columns to int64, and will thus produce different results from pandas
# when trying to select columns declared as sized ints.
# This test is in place to track any potential changes to this behavior in Snowpandas.
@pytest.mark.parametrize(
    "include, snow_result",
    [
        # With int32, Snowpandas will miss the column that was explicitly specified as int32
        [np.int32, native_pd.DataFrame(columns=[], index=[0, 1, 2, 3])],
        # With int64, Snowpandas will include all int columns, including the column that was
        # explicitly specified as int32
        [np.int64, native_pd.DataFrame(SELECT_DTYPES_DATA).select_dtypes(int)],
    ],
)
@sql_count_checker(query_count=1)
def test_select_dtypes_sized_int_negative(include, snow_result):
    snow_df = pd.DataFrame(SELECT_DTYPES_DATA)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df.select_dtypes(include=include), snow_result
    )


@pytest.mark.parametrize("include, exclude", INCLUDE_EXCLUDE_OPTIONS)
@sql_count_checker(query_count=1)
def test_select_dtypes_empty_frame(include, exclude):
    snow_df = pd.DataFrame([])
    native_df = native_pd.DataFrame([])
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.select_dtypes(include, exclude),
        check_column_type=False,
    )


@pytest.mark.parametrize("include, exclude", INCLUDE_EXCLUDE_OPTIONS)
@sql_count_checker(query_count=1)
def test_select_dtypes_duplicate_col_names(include, exclude):
    # 3x3 dataframe with 3 columns of int, float, and obj, all with the same name
    data = [
        [1, 1.1, "a"],
        [2, 2.1, "b"],
        [3, 3.1, "c"],
    ]
    columns = ["col"] * 3
    snow_df = pd.DataFrame(data, columns=columns)
    native_df = native_pd.DataFrame(data, columns=columns)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.select_dtypes(include, exclude),
    )


@pytest.mark.parametrize(
    "include, exclude, exc, exc_match",
    [
        ([], [], ValueError, "at least one of include or exclude must be nonempty"),
        (None, None, ValueError, "at least one of include or exclude must be nonempty"),
        # python `int` is equivalent to any np.int dtype, but it is fine for an type in
        # `include` to be a strict subtype of a type in `exclude` or vice versa
        (int, int, ValueError, "include and exclude overlap"),
        ([int], ["O", int], ValueError, "include and exclude overlap"),
        (["O", int], [int], ValueError, "include and exclude overlap"),
        (int, np.int32, ValueError, "include and exclude overlap"),
        (int, np.int64, ValueError, "include and exclude overlap"),
        ("datetime", np.datetime64, ValueError, "include and exclude overlap"),
        ("O", object, ValueError, "include and exclude overlap"),
        # string dtypes are prohibited by pandas
        (str, None, TypeError, "string dtypes are not allowed, use 'object' instead"),
        (None, str, TypeError, "string dtypes are not allowed, use 'object' instead"),
        (
            "timedelta64[s]",
            None,
            ValueError,
            "'timedelta64[s]' is too specific of a frequency, try passing 'timedelta64'",
        ),
        (
            None,
            "timedelta64[s]",
            ValueError,
            "'timedelta64[s]' is too specific of a frequency, try passing 'timedelta64'",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_select_dtypes_invalid_args(include, exclude, exc, exc_match):
    snow_df = pd.DataFrame(SELECT_DTYPES_DATA)
    native_df = native_pd.DataFrame(SELECT_DTYPES_DATA)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.select_dtypes(include, exclude),
        expect_exception=True,
        expect_exception_type=exc,
        expect_exception_match=re.escape(exc_match),
        assert_exception_equal=True,
    )
