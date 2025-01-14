#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import warnings

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas.errors import SettingWithCopyWarning

from tests.integ.modin.utils import (
    assert_series_equal,
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
def test_create_timedelta_column_from_pandas_timedelta():
    pandas_df = native_pd.DataFrame(
        {"timedelta_column": [native_pd.Timedelta(nanoseconds=1)], "int_column": [3]}
    )
    snow_df = pd.DataFrame(pandas_df)
    eval_snowpark_pandas_result(snow_df, pandas_df, lambda df: df)


@sql_count_checker(query_count=1)
def test_create_timedelta_series_from_pandas_timedelta():
    eval_snowpark_pandas_result(
        *create_test_series(
            [
                native_pd.Timedelta(
                    weeks=5,
                    days=27,
                    hours=23,
                    minutes=59,
                    seconds=59,
                    milliseconds=999,
                    microseconds=999,
                    nanoseconds=1,
                ),
                None,
            ]
        ),
        lambda s: s
    )


@sql_count_checker(query_count=1)
def test_create_timedelta_column_from_datetime_timedelta():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"timedelta_column": [datetime.timedelta(days=1)], "int_column": [3]}
        ),
        lambda df: df
    )


@sql_count_checker(query_count=0)
def test_timedelta_dataframe_dtypes():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "timedelta_column": [native_pd.Timedelta(nanoseconds=1)],
                "int_column": [3],
            }
        ),
        lambda df: df.dtypes,
        comparator=assert_series_equal
    )


@sql_count_checker(query_count=0)
def test_timedelta_series_dtypes():
    eval_snowpark_pandas_result(
        *create_test_series([native_pd.Timedelta(1)]),
        lambda s: s.dtype,
        comparator=lambda snow_type, pandas_type: snow_type == pandas_type
    )


@sql_count_checker(query_count=1)
def test_timedelta_precision_insufficient_with_nulls():
    # Storing this timedelta requires more than 15 digits of precision
    timedelta = pd.Timedelta(days=105, nanoseconds=1)
    eval_snowpark_pandas_result(
        pd, native_pd, lambda lib: lib.Series([None, timedelta])
    )


@sql_count_checker(query_count=0)
def test_timedelta_not_supported():
    df = pd.DataFrame(
        {
            "a": ["one", "two", "three"],
            "b": ["abc", "pqr", "xyz"],
            "dt": [
                pd.Timedelta("1 days"),
                pd.Timedelta("2 days"),
                pd.Timedelta("3 days"),
            ],
        }
    )
    with pytest.raises(
        NotImplementedError,
        match="SnowflakeQueryCompiler::groupby_groups is not yet implemented for Timedelta Type",
    ):
        df.groupby("a").groups()


@sql_count_checker(query_count=1)
def test_aggregation_does_not_print_internal_warning_SNOW_1664064():
    with warnings.catch_warnings():
        warnings.simplefilter(category=SettingWithCopyWarning, action="error")
        pd.Series(pd.Timedelta(1)).max()
