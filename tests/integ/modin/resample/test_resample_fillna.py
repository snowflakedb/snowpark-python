#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import pytest

from tests.integ.modin.utils import (
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

interval = pytest.mark.parametrize("interval", [1, 2, 3, 5, 15])
agg_func = pytest.mark.parametrize("agg_func", ["ffill", "bfill"])


# One extra query to convert index to native pandas for dataframe constructor
@interval
@agg_func
@sql_count_checker(query_count=3, join_count=1)
def test_resample_fill(interval, agg_func):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01 1:00:00",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
            "2020-01-09",
            "2020-02-08",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "a": range(len(datecol)),
                "b": range(len(datecol) - 1, -1, -1),
                "c": native_pd.timedelta_range("1 days", periods=len(datecol)),
            },
            index=datecol,
        ),
        lambda df: getattr(df.resample(rule=f"{interval}D"), agg_func)(),
        check_freq=False,
    )


@interval
@agg_func
@sql_count_checker(query_count=2, join_count=1)
def test_resample_fill_ser(interval, agg_func):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01 1:00:00",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
            "2020-01-09",
            "2020-02-08",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_series({"a": range(len(datecol))}, index=datecol),
        lambda df: getattr(df.resample(rule=f"{interval}D"), agg_func)(),
        check_freq=False,
    )


@interval
@agg_func
@sql_count_checker(query_count=3, join_count=1)
def test_resample_ffill_one_gap(interval, agg_func):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"a": range(len(datecol)), "b": range(len(datecol) - 1, -1, -1)},
            index=datecol,
        ),
        lambda df: getattr(df.resample(rule=f"{interval}D"), agg_func)(),
        check_freq=False,
    )


@agg_func
@sql_count_checker(query_count=2, join_count=1)
def resample_ffill_ser_one_gap(agg_func):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_series({"a": range(len(datecol))}, index=datecol),
        lambda df: getattr(df.resample(rule=f"{interval}D"), agg_func)(),
        check_freq=False,
    )


@interval
@agg_func
@sql_count_checker(query_count=3, join_count=1)
def test_resample_ffill_missing_in_middle(interval, agg_func):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03 1:00:00",
            "2020-01-03 2:00:00",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"a": range(len(datecol)), "b": range(len(datecol) - 1, -1, -1)},
            index=datecol,
        ),
        lambda df: getattr(df.resample(rule=f"{interval}D"), agg_func)(),
        check_freq=False,
    )


@interval
@agg_func
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ffill_ser_missing_in_middle(interval, agg_func):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03 1:00:00",
            "2020-01-03 2:00:00",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_series({"a": range(len(datecol))}, index=datecol),
        lambda df: getattr(df.resample(rule=f"{interval}D"), agg_func)(),
        check_freq=False,
    )


@interval
@agg_func
@sql_count_checker(query_count=3, join_count=1)
def test_resample_ffill_ffilled_with_none(interval, agg_func):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03 2:00:00",
            "2020-01-03 1:00:00",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
            "2020-01-10",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"a": [1, 2, None, 4, 5, 7, None, 8]}, index=datecol),
        lambda df: getattr(df.resample(rule=f"{interval}D"), agg_func)(),
        check_freq=False,
    )


@interval
@agg_func
@sql_count_checker(query_count=3, join_count=1)
def test_resample_ffill_large_gaps(interval, agg_func):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-07-07",
            "2021-01-08",
            "2021-01-10",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"a": [1, None, 4, 5, 7, None, 8]}, index=datecol),
        lambda df: getattr(df.resample(rule=f"{interval}D"), agg_func)(),
        check_freq=False,
    )


@interval
@pytest.mark.parametrize("method", ["ffill", "pad", "backfill", "bfill"])
@sql_count_checker(query_count=3, join_count=1)
def test_resample_fillna(interval, method):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-07-07",
            "2021-01-08",
            "2021-01-10",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"a": [1, None, 4, 5, 7, None, 8]}, index=datecol),
        lambda df: df.resample(rule=f"{interval}D").fillna(method=method),
        check_freq=False,
    )
