#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.fixture(scope="function")
def left_right_native_df():
    df1 = native_pd.DataFrame(
        {
            "a": [1, 5, 10],
            "left_val": ["a", "b", "c"],
            "another_col": [10, 12, 14],
        },
    )
    df2 = native_pd.DataFrame(
        {
            "a": [1, 2, 3, 6, 7],
            "right_val": [1, 2, 3, 6, 7],
            "another_col_right": [0, np.nan, 3, 6, 8],
        },
    )
    return df1, df2


@pytest.fixture(scope="function")
def left_right_native_df_switch_column_order():
    df1 = native_pd.DataFrame(
        {
            "left_val": ["a", "b", "c"],
            "a": [1, 5, 10],
            "another_col": [10, 12, 14],
        },
    )
    df2 = native_pd.DataFrame(
        {
            "right_val": [1, 2, 3, 6, 7],
            "another_col_right": [0, np.nan, 3, 6, 8],
            "a": [1, 2, 3, 6, 7],
        },
    )
    return df1, df2


@pytest.fixture(scope="function")
def left_right_native_df_left_right_by():
    df1 = native_pd.DataFrame(
        {
            "a": [1, 5, 10],
            "left_val": ["a", "b", "c"],
            "another_col": [10, 12, 14],
        },
    )
    df2 = native_pd.DataFrame(
        {
            "right_val": [1, 2, 3, 6, 7],
            "another_col_right": [0, np.nan, 3, 6, 8],
            "b": [1, 2, 3, 6, 7],
        },
    )
    return df1, df2


@pytest.fixture(scope="function")
def left_right_timestamp_data():
    quotes = native_pd.DataFrame(
        {
            "time": [
                pd.Timestamp("2016-05-25 13:30:00.023"),
                pd.Timestamp("2016-05-25 13:30:00.023"),
                pd.Timestamp("2016-05-25 13:30:00.030"),
                pd.Timestamp("2016-05-25 13:30:00.041"),
                pd.Timestamp("2016-05-25 13:30:00.048"),
                pd.Timestamp("2016-05-25 13:30:00.049"),
                pd.Timestamp("2016-05-25 13:30:00.072"),
                pd.Timestamp("2016-05-25 13:30:00.075"),
            ],
            "bid": [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01],
            "ask": [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03],
        }
    )
    trades = native_pd.DataFrame(
        {
            "time": [
                pd.Timestamp("2016-05-25 13:30:00.023"),
                pd.Timestamp("2016-05-25 13:30:00.038"),
                pd.Timestamp("2016-05-25 13:30:00.048"),
                pd.Timestamp("2016-05-25 13:30:00.048"),
                pd.Timestamp("2016-05-25 13:30:00.048"),
            ],
            "price": [51.95, 51.95, 720.77, 720.92, 98.0],
            "quantity": [75, 155, 100, 100, 100],
        }
    )
    return quotes, trades


allow_exact_matches = pytest.mark.parametrize("allow_exact_matches", [True, False])
direction = pytest.mark.parametrize("direction", ["backward", "forward"])


@allow_exact_matches
@direction
@sql_count_checker(query_count=1, join_count=1)
def test_merge_asof_on(left_right_native_df, allow_exact_matches, direction):
    left_native_df, right_native_df = left_right_native_df
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(
        left_native_df,
        right_native_df,
        on="a",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    snow_output = pd.merge_asof(
        left_snow_df,
        right_snow_df,
        on="a",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    eval_snowpark_pandas_result(snow_output, native_output, lambda df: df)


@allow_exact_matches
@direction
@sql_count_checker(query_count=1, join_count=1)
def test_merge_asof_on_switch_column_order(
    left_right_native_df_switch_column_order, allow_exact_matches, direction
):
    left_native_df, right_native_df = left_right_native_df_switch_column_order
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(
        left_native_df,
        right_native_df,
        on="a",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    snow_output = pd.merge_asof(
        left_snow_df,
        right_snow_df,
        on="a",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    eval_snowpark_pandas_result(snow_output, native_output, lambda df: df)


@allow_exact_matches
@direction
def test_merge_asof_left_right_on(
    left_right_native_df_left_right_by, allow_exact_matches, direction
):
    left_native_df, right_native_df = left_right_native_df_left_right_by
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(
        left_native_df,
        right_native_df,
        left_on="a",
        right_on="b",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    snow_output = pd.merge_asof(
        left_snow_df,
        right_snow_df,
        left_on="a",
        right_on="b",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    eval_snowpark_pandas_result(snow_output, native_output, lambda df: df)


def test_merge_asof_timestamps(left_right_timestamp_data):
    left_native_df, right_native_df = left_right_timestamp_data
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(left_native_df, right_native_df, on="time")
    snow_output = pd.merge_asof(left_snow_df, right_snow_df, on="time")
    eval_snowpark_pandas_result(snow_output, native_output, lambda df: df)
