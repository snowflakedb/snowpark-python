#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
from pandas import Timestamp

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_series_equal,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

TIME_DATA1 = {
    "CREATED_AT": ["2018-8-26 15:09:02", "2018-8-25 11:10:07", "2018-8-27 12:05:00"],
    "REPORTING_DATE": [
        "2018-9-26 12:00:00",
        "2018-10-26 12:00:00",
        "2018-10-26 12:00:00",
    ],
    "OPEN_DATE": ["2018-10-26 12:00:00", "2018-10-26 12:00:00", "2018-10-26 12:00:00"],
    "CLOSED_DATE": ["2018-12-26", "2018-12-28", "2018-12-29"],
}


@sql_count_checker(query_count=1)
def test_insert_datetime_difference_in_days():
    data = TIME_DATA1
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    native_df["month_lag"] = (
        (
            native_pd.to_datetime(native_df["CREATED_AT"], format="%Y-%m-%d %H:%M:%S")
            - native_pd.to_datetime(
                native_df["REPORTING_DATE"], format="%Y-%m-%d", errors="coerce"
            )
        )
        / np.timedelta64(1, "D")
    ).round()
    snow_df["month_lag"] = (
        (
            pd.to_datetime(snow_df["CREATED_AT"], format="%Y-%m-%d %H:%M:%S")
            - pd.to_datetime(
                snow_df["REPORTING_DATE"], format="%Y-%m-%d", errors="coerce"
            )
        )
        / np.timedelta64(1, "D")
    ).round()
    assert_series_equal(snow_df["month_lag"], native_df["month_lag"])


@sql_count_checker(query_count=1)
def test_insert_datetime_difference():
    data = {
        "bl_start_ts": [Timestamp("2017-03-01T12")],
        "green_light_ts": [Timestamp("2017-01-07T12")],
    }
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.assign(
            green_light_response_time=(df["bl_start_ts"] - df["green_light_ts"])
        ),
    )


@sql_count_checker(query_count=1)
def test_diff_timestamp_column_to_get_timedelta():
    data = {
        "Country": ["A", "B", "C", "D", "E"],
        "Agreement Signing Date": [
            pd.Timestamp("2017-01-07"),
            pd.Timestamp("2017-03-07"),
            pd.Timestamp("2017-06-07"),
            pd.Timestamp("2017-04-06"),
            pd.Timestamp("2017-08-09"),
        ],
    }
    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.set_index("Country")
        .diff()
        .rename(columns={"Agreement Signing Date": "DiffDaysPrevAggrement"}),
    )
