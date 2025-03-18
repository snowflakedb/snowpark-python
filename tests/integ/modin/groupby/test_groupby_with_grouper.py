#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
#

"""
Tests for groupby operations that involve `pd.Grouper` objects, including resampling operations.

To avoid blowing up runtime too exponentially, these tests cover a mixture of groupby operations
and arguments rather than every possible operation-argument combination.
"""

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.exceptions import SnowparkSQLException
import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker, SqlCounter
from tests.integ.modin.utils import (
    eval_snowpark_pandas_result,
    create_test_series,
    create_test_dfs,
)

DT_SORTED_INDEX_LEVEL = 0
DT_UNSORTED_INDEX_LEVEL = 1
STRING_INDEX_LEVEL = 2
NUMERIC_INDEX_LEVEL = 3


@pytest.fixture
def dfs_with_datetime_cols_and_mi() -> tuple[pd.DataFrame, native_pd.DataFrame]:
    # Returns a native DF with datetime columns and a 2-level multi-index.
    rng = np.random.default_rng(0)
    row_count = 8
    # Use a mixture of frequencies and start dates
    dts_sorted_index = native_pd.date_range(
        start="2000-01-01 00:45:50", periods=row_count, freq="45s"
    ).to_numpy()
    dts_unsorted_index = native_pd.date_range(
        start="2030-11-19 16:39:00", periods=row_count, freq="1ME"
    ).to_numpy()
    rng.shuffle(dts_unsorted_index)
    dts_sorted_data = native_pd.date_range(
        start="1800-05-22 08:30:21", periods=row_count, freq="1h"
    ).to_numpy()
    dts_unsorted_data = native_pd.date_range(
        start="2013-10-08 22:33:00", periods=row_count, freq="2min"
    ).to_numpy()
    rng.shuffle(dts_unsorted_data)
    data = {
        "datetimes_sorted_index": dts_sorted_index,
        "datetimes_unsorted_index": dts_unsorted_index,
        "string_index": [f"s_index_{i % 2}" for i in range(row_count)],
        "numeric_index": [i % 3 for i in range(row_count)],
        "datetimes_sorted_data": dts_sorted_data,
        "datetimes_unsorted_data": dts_unsorted_data,
        "string_data": [f"s_data_{i % 4}" for i in range(row_count)],
        "numeric_data": [100 + (i % 5) for i in range(row_count)],
    }
    idx_list = [
        "datetimes_sorted_index",
        "datetimes_unsorted_index",
        "string_index",
        "numeric_index",
    ]
    return pd.DataFrame(data).set_index(idx_list), native_pd.DataFrame(data).set_index(
        idx_list
    )


@pytest.mark.parametrize(
    "by",
    [
        # Standalone grouper object with label
        pd.Grouper(key="string_data"),
        # Standalone grouper object with level
        pd.Grouper(level=0),
        # List containing mix of grouper and string labels
        [
            pd.Grouper(level=1),
            "string_data",
            "numeric_data",
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_groupby_nunique_with_grouper(by, dfs_with_datetime_cols_and_mi):
    eval_snowpark_pandas_result(
        *dfs_with_datetime_cols_and_mi, lambda df: df.groupby(by).nunique()
    )


@sql_count_checker(query_count=2, join_count=3)
def test_groupby_count_implicit_grouper():
    # If a Grouper is specified without a label or level but has a freq, it implicitly refers to
    # the frame's index column.
    dts = native_pd.date_range(start="2000-10-01 23:00:00", freq="5min", periods=10)
    eval_snowpark_pandas_result(
        *create_test_dfs(list(range(10)), index=dts),
        lambda df: df.groupby(pd.Grouper(freq="120s")).count(),
        test_attrs=False,
        check_freq=False,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_groupby_head_no_resample():
    # native pandas does NOT resample when calling groupby_head or tail.
    dts = native_pd.date_range(start="2000-10-01 23:00:00", freq="5min", periods=10)
    eval_snowpark_pandas_result(
        *create_test_dfs(list(range(10)), index=dts),
        lambda df: df.groupby(pd.Grouper(freq="120s")).head(),
        test_attrs=False,
        check_freq=False,
    )


@pytest.mark.parametrize(
    "by",
    [
        [pd.Grouper(level=DT_SORTED_INDEX_LEVEL, freq="90s")],
        [
            pd.Grouper(level=DT_UNSORTED_INDEX_LEVEL, freq="90s"),
            pd.Grouper(level=DT_SORTED_INDEX_LEVEL, freq="200s", origin="start"),
        ],
    ],
)
def test_groupby_agg_level_resample(by, dfs_with_datetime_cols_and_mi):
    # One join is incurred for every resampled column.
    # One additional query is incurred for every resample column to determine the start/end dates for binning.
    with SqlCounter(query_count=1 + len(by), join_count=len(by)):
        eval_snowpark_pandas_result(
            *dfs_with_datetime_cols_and_mi,
            lambda df: df.groupby(by).agg("count"),
            test_attrs=False,
            check_freq=False,
        )


# TODO: test groupby_apply, since it seems like some timestamp columns get converted to string


@pytest.mark.parametrize(
    "by",
    [
        [pd.Grouper(key="datetimes_sorted_data", freq="1ME")],
        [
            pd.Grouper(key="datetimes_sorted_data", freq="1ME"),
            pd.Grouper(key="datetimes_unsorted_data", freq="93s"),
        ],
    ],
)
def test_groupby_count_named_resample(by, dfs_with_datetime_cols_and_mi):
    with SqlCounter(query_count=1 + len(by), join_count=len(by)):
        # Need to reset_index because resampling with a named key is only valid for DatetimeIndex, not MultiIndex
        eval_snowpark_pandas_result(
            *dfs_with_datetime_cols_and_mi,
            lambda df: df.reset_index()
            .set_index("datetimes_unsorted_index")
            .groupby(by)
            .count(),
            test_attrs=False,
            check_freq=False,
        )


@pytest.mark.parametrize("freq", ["45s", "3min", "4min", "8min", "17min"])
@pytest.mark.parametrize(
    "origin",
    [
        None,
        "start_day",
        "start",
    ],
)
def test_groupby_series_datetime_sum(freq, origin):
    dates = native_pd.date_range(
        "2000-10-01 23:00:00", "2000-10-01 23:16:00", freq="4min"
    )
    grouper_kwargs = {"freq": freq}
    if origin is not None:
        grouper_kwargs["origin"] = origin
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            *create_test_series(np.arange(len(dates)), index=dates),
            lambda ser: ser.groupby(pd.Grouper(**grouper_kwargs)).sum(),
            check_freq=False,
        )


@sql_count_checker(query_count=0)
def test_groupby_invalid_resample(dfs_with_datetime_cols_and_mi):
    # Resamples cannot be performed on non-datetime columns.
    snow_df, native_df = dfs_with_datetime_cols_and_mi
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Only valid with DatetimeIndex, TimedeltaIndex or PeriodIndex, but got an instance of 'Index'"
        ),
    ):
        native_df.groupby(pd.Grouper(key="numeric_data", freq="120s")).count()
    with pytest.raises(
        SnowparkSQLException,
        match=re.escape(
            "Function DATE_TRUNC does not support NUMBER(38,0) argument type"
        ),
    ):
        snow_df.groupby(pd.Grouper(key="numeric_data", freq="120s")).count()


@sql_count_checker(query_count=0, join_count=0)
def test_groupby_double_resample_unsupported(dfs_with_datetime_cols_and_mi):
    snow_df, native_df = dfs_with_datetime_cols_and_mi
    grouper = [
        pd.Grouper(key="datetimes_sorted_data", freq="10s"),
        pd.Grouper(key="datetimes_sorted_data", freq="20s"),
    ]
    native_df.groupby(grouper).count()  # no error
    # Resampling the same column twice is currently unsupported.
    with pytest.raises(
        NotImplementedError,
        match="Resampling the same column multiple times is not yet supported in Snowpark pandas.",
    ):
        snow_df.groupby(grouper).count()
